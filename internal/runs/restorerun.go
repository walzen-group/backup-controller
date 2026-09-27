package runs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/served"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// RestoreRunReconciler runs RestoreRuns. A RestoreRun puts volumes and
// databases back to the state they were in at a chosen moment.
//
// A volume restores in place. Once no pod mounts the claim, the run creates
// its own restore Job (internal/restorejob), which mounts the claim and runs
// restic restore of the selected snapshot by its full ID, and the Job's
// terminal conditions say how it ended. A database restores by being created
// again. The run deletes the Cluster, and when Flux or tofu creates it again,
// the bootstrap webhook makes the new Cluster recover to the run's moment.
// With spec.into set, a volume restores into a new claim the run creates, and
// the app's own claims and databases are left alone. The same restore Job,
// mounting that claim, writes the selected snapshot into it by its full ID,
// from the backups of spec.claim or from spec.repository.
//
// The run stops a mover once the item's end is in its status, when the run
// ends and when it is deleted. It suspends a restore Job and waits until no
// pod of it can still write before it gives the app back, releases the
// item's Leases or finishes (rule X2, see stopJobs).
type RestoreRunReconciler struct {
	client.Client

	// Reader reads straight from the API server, without the informer cache.
	// The run reads claims, VolumeRestores, Secrets, ObjectStores, Clusters,
	// ReplicationDestinations, Deployments, StatefulSets, pods and Jobs through
	// it.
	Reader client.Reader

	// Snapshots lists the snapshots in a restic repository. plan uses it to
	// check that each volume has a snapshot the run's moment reaches.
	Snapshots restic.Lister

	// Prober lists a database's base backups in its object store. plan uses it
	// to check that each database has a base backup the run's moment reaches.
	Prober bootstrap.Prober

	// Recorder writes an event on the run each time its Ready reason changes.
	Recorder events.EventRecorder

	// RestoreImage is the image the run's restore Jobs run restic in, from
	// the controller's required --restore-image flag: the image VolSync runs
	// its restic mover in, so a restore runs the restic that wrote the
	// backup.
	RestoreImage string

	// Now returns the current time. Tests replace it so they can move time
	// forward without sleeping.
	Now func() time.Time

	// schemas caches the check that the installed CRD of the run's kind
	// declares every field the controller writes (see crdOutdated).
	schemas schemaCache
}

// SetupWithManager registers the reconciler with mgr so it runs for every
// RestoreRun. It sets Now to time.Now when the caller left it unset.
func (r *RestoreRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	r.serve()
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.RestoreRun{}).
		Named("restorerun").
		Complete(r)
}

// Reconcile moves the RestoreRun the request names one step further, and
// requeues until the run has finished.
//
// A new run starts in plan, which checks that every item has a backup in reach
// before anything is changed, or in planIntoNewClaim when spec.into is set. An
// error that either of them returns for a retry goes through planFailed,
// which reports it on the Ready condition and ends the run once spec.timeout
// has passed since its creation. A run past its checks continues in work, or in
// restoreIntoEmptyClaim for an into restore. Before any of these, Reconcile
// adds the run's finalizer. A run being deleted gets its changes put back by
// finalize, and a finished run is deleted once spec.ttlSecondsAfterFinished
// has passed.
//
// A new run is first checked against the installed RestoreRun CRD (see
// schemaCache.crdOutdated), and a run whose CRD lacks a field the controller
// writes ends with reason CRDOutdated before anything is planned.
// Whenever the Ready reason changes during a reconcile, Reconcile records an
// event on the run.
//
// While VolSync serves its kinds only at a version other than v1alpha1, a
// pass that reads, creates or deletes a VolSync object gets an error naming
// the kind and v1alpha1, which it returns for a retry (see serve). The run
// changes nothing on that error, and an app it has already stopped stays
// stopped until v1alpha1 is served again. VolSync is upgraded after the
// controller, so a supported cluster never gets there.
//
// A run that recorded status.ending has decided to end, and every later pass
// only finishes it with that reason and message (see finish), also one that
// waits for a stopped mover or retries a failed restart.
func (r *RestoreRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.RestoreRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Restore") }()
	if !run.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, run)
	}
	if run.Status.Phase.Finished() {
		return expire(ctx, r.Client, run, run.Spec.TTLSecondsAfterFinished, run.Status.CompletedAt, r.Now())
	}
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		controllerutil.AddFinalizer(run, Finalizer)
		if err := r.Update(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer to RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
		}
	}
	if ending := run.Status.Ending; ending != nil {
		return r.finish(ctx, run, ending.Reason, ending.Message)
	}

	if run.Status.Phase == "" {
		return r.start(ctx, run)
	}
	if run.Spec.Into != "" {
		return r.restoreIntoEmptyClaim(ctx, run)
	}
	return r.work(ctx, run)
}

// start makes the first pass over a new run: it checks the installed
// RestoreRun CRD, then plans the run.
//
// Parameters:
//   - run is the RestoreRun with an empty phase and its finalizer in place.
//
// It returns what plan or planIntoNewClaim returns, or what finish returns
// for a run whose CRD lacks a field the controller writes. An error that
// either check returns for a retry goes through planFailed.
//
// The CRD check comes first for both plans: the old RestoreRun CRD drops the
// items' clusterUID and snapshotTime, which the restore relies on. A run
// with spec.into set is planned by planIntoNewClaim, and any other run by
// plan.
func (r *RestoreRunReconciler) start(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	message, err := r.schemas.crdOutdated(ctx, r.Reader, restoreRunsCRD, "RestoreRun", backupv1alpha1.RestoreRun{},
		"the run would lose the Cluster UIDs and snapshot times it records to check its own work")
	if err != nil {
		return ctrl.Result{}, r.planFailed(ctx, run, err)
	}
	if message != "" {
		return r.finish(ctx, run, backupv1alpha1.ReasonCRDOutdated, message)
	}
	plan := r.plan
	if run.Spec.Into != "" {
		plan = r.planIntoNewClaim
	}
	result, err := plan(ctx, run)
	if err != nil {
		return ctrl.Result{}, r.planFailed(ctx, run, err)
	}
	return result, nil
}

// planFailed handles an error that plan or planIntoNewClaim returned for a
// retry, and returns the error the reconcile hands back.
//
// A run whose checks keep failing never records status.startedAt, so overdue
// never fires for it. Once the deadline from checksOverdue has passed,
// planFailed ends the run as Failed with reason TimedOut and the error in the
// message. Until then it sets Ready to False with reason Retrying and the
// error as the message, so `kubectl get` shows why the run has not started,
// and returns the error for a retry. It writes the status only when it
// differs from the stored one (see writeChangedStatus), because every write
// starts another reconcile.
//
// An error from a pass that had already given the run a phase, such as a lost
// status write, is returned unchanged.
func (r *RestoreRunReconciler) planFailed(ctx context.Context, run *backupv1alpha1.RestoreRun, err error) error {
	if run.Status.Phase != "" {
		return err
	}
	if deadline, over := r.checksOverdue(run); over {
		// Nothing was created before the checks passed, so there is no
		// restore Job to stop and no mover to wait for.
		_, finishErr := r.finish(ctx, run, backupv1alpha1.ReasonTimedOut, checksTimedOut(deadline, err.Error()))
		return finishErr
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRetrying, err.Error())
	if werr := r.writeChangedStatus(ctx, run); werr != nil {
		return errors.Join(err, werr)
	}
	return err
}

// checksOverdue returns the deadline of a run that has not passed its checks,
// its creation plus spec.timeout, and reports whether the run has worked past
// it. Such a run has no status.startedAt, so overdue never fires for it. A run
// with no timeout, or with no creation time yet, is never overdue.
func (r *RestoreRunReconciler) checksOverdue(run *backupv1alpha1.RestoreRun) (time.Time, bool) {
	if run.Spec.Timeout == nil || run.CreationTimestamp.IsZero() {
		return time.Time{}, false
	}
	deadline := run.CreationTimestamp.Add(run.Spec.Timeout.Duration)
	return deadline, !r.Now().Before(deadline)
}

// checksTimedOut returns the Ready message of a run that ends TimedOut
// before it passed its checks: the deadline from checksOverdue, and why the
// checks had not passed, which is the text given in why.
func checksTimedOut(deadline time.Time, why string) string {
	return fmt.Sprintf("the run had not passed its checks by %s: %s", deadline.UTC().Format(time.RFC3339), why)
}

// writeChangedStatus writes the run's status unless the stored run already
// holds that exact status, so a pass that waits and changed nothing starts
// no other reconcile.
//
// Parameters:
//   - run is the RestoreRun as this pass computed it, status included.
//
// It returns nil when the write was skipped or went through, and the error
// of the read or of the write otherwise.
//
// It reads the stored run with the uncached Reader and skips the write only
// when the whole stored status, items and ending included, equals the
// computed one. Any difference writes the status, so whatever the pass
// changed in it before it waits reaches the API server in that pass. The
// resourceVersion does not count: a pass whose copy of the run is older than
// the stored run, and whose status is already stored, has nothing to write,
// and a write with the older resourceVersion would only fail with a
// conflict.
func (r *RestoreRunReconciler) writeChangedStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	stored := &backupv1alpha1.RestoreRun{}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
		return fmt.Errorf("get RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	if equality.Semantic.DeepEqual(stored.Status, run.Status) {
		return nil
	}
	return r.writeStatus(ctx, run)
}

// waitAtChecks holds a run that has not passed its checks while a backup of
// a repository it restores from is in progress, and returns the result the
// reconcile hands back. The run selects its snapshot once that backup has
// finished, so after the backup's restic forget and its retime.
//
// Parameters:
//   - busy is otherMover's message naming the backup, which becomes the
//     Ready message.
//
// The run keeps its empty phase, because Reconcile plans a run whose phase
// is empty. waitAtChecks sets Ready to False with reason SourceBusy and the
// message, writes the status when it differs from the stored one (see
// writeChangedStatus), and requeues after pollInterval. Once the deadline
// from checksOverdue has passed, it ends the run as Failed with reason
// TimedOut and the message instead. A failed read or write of the status
// comes back as an error.
func (r *RestoreRunReconciler) waitAtChecks(ctx context.Context, run *backupv1alpha1.RestoreRun, busy string) (ctrl.Result, error) {
	if deadline, over := r.checksOverdue(run); over {
		return r.finish(ctx, run, backupv1alpha1.ReasonTimedOut, checksTimedOut(deadline, busy))
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonSourceBusy, busy)
	return after(pollInterval, r.writeChangedStatus(ctx, run))
}

// target parses the run's spec.restoreAsOf. It returns nil when the field is
// unset, which means the newest backup, and an error when the value is not an
// RFC 3339 time.
func target(run *backupv1alpha1.RestoreRun) (*time.Time, error) {
	if run.Spec.RestoreAsOf == nil {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, *run.Spec.RestoreAsOf)
	if err != nil {
		return nil, fmt.Errorf("restoreAsOf %q is not an RFC 3339 time", *run.Spec.RestoreAsOf)
	}
	return &t, nil
}

// plan checks, before the run changes anything, that a backup reaches the
// run's moment for every item of an in-place run, and starts the run when
// every item passes.
//
// Parameters:
//   - run is the RestoreRun with an empty phase and no spec.into. plan
//     records its items, the selected snapshots and base backups, and
//     status.syncedTo in its status.
//
// It returns the result of the pass. A run whose checks passed is moved to
// Running with status.startedAt set and requeued after a second; a run that
// ends here returns what finish returns. plan returns an error, and writes
// nothing, when listing the snapshots or the base backups fails, and when a
// read fails in a way a retry may fix, such as a timeout from the API
// server; Reconcile hands such an error to planFailed.
//
// For a volume, the check selects the snapshot the restore would use and
// records its full ID, its short ID and its time on the item (see
// checkVolume and recordSnapshot). For a database, it selects the base
// backup the recovery would start from and records its ID (see
// checkDatabase). An item with nothing in reach (see selectSnapshot) is
// marked Failed with the reason. If any item fails, plan
// marks every other Pending item Skipped and ends the run as Failed with
// reason NoBackupInReach, naming each item it cannot reach. Nothing has been
// deleted or overwritten at that point.
//
// A backup's mover runs restic forget after it saves, and a quiesced
// BackupRun retimes its snapshot after the mover, so a snapshot selected
// during a backup may be gone by the time the restore's mover starts. Before
// it checks a volume, plan therefore looks for a backup of the claim or its
// repository in progress (see volumeBackedUp), and while there is one it
// waits with reason SourceBusy and leaves the run unplanned (see
// waitAtChecks), until spec.timeout from the run's creation ends it TimedOut.
//
// With spec.syncDatabaseToVolume set, each volume may only select a snapshot
// tagged quiesced, and all the selected snapshots must carry the same time.
// plan records that time in status.syncedTo and checks each database against
// it in place of spec.restoreAsOf.
//
// A spec that can't work ends the run as Failed with reason Invalid: a
// restoreAsOf that doesn't parse, a spec.quiesce entry the namespace does not
// hold, a synced run with no claim to take the moment from, or any refusal
// from items, such as a Cluster that another unfinished RestoreRun is
// restoring.
//
// While clusterWebhookBlind reports that the API server no longer serves
// Cluster at the version the bootstrap webhook's rules name, a run with a
// Pending Cluster item ends Failed with reason ClusterVersionUnsupported
// before anything is stopped or deleted: each such item fails with the
// message, and every other Pending item is Skipped.
func (r *RestoreRunReconciler) plan(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	items, err := r.items(ctx, run)
	if asRunRefusal(err) {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	at, err := target(run)
	if err != nil {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if _, err := namedTargets(ctx, r.Reader, run.Namespace, run.Spec.Quiesce); err != nil {
		if !isQuiesceSpecError(err) {
			return ctrl.Result{}, err
		}
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}

	// A Cluster the run would delete comes back empty while the webhook
	// can't see its creation, so the run ends before it changes anything.
	if blind := clusterWebhookBlind(r.RESTMapper()); len(failBlindClusters(items, blind)) > 0 {
		log.FromContext(ctx).Error(errors.New(blind.message()), "deleting no Cluster: the bootstrap webhook would not see it created again",
			"namespace", run.Namespace, "name", run.Name)
		for i := range items {
			if items[i].Phase == backupv1alpha1.ItemPending {
				items[i].Phase = backupv1alpha1.ItemSkipped
				items[i].Message = "left alone because the run deletes no Cluster the bootstrap webhook would not see created again"
			}
		}
		run.Status.Items = items
		return r.finish(ctx, run, backupv1alpha1.ReasonClusterVersionUnsupported, blind.message())
	}

	// A synced run takes its databases' moment from the volumes' quiesced
	// snapshots. The items method lists the claims before the Clusters, so
	// the moment is known by the time the first Cluster is checked.
	sync := run.Spec.SyncDatabaseToVolume
	var synced *time.Time
	var unreachable []string
	for i := range items {
		item := &items[i]
		if item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		// Each item's check sets its own reason and error, so an item never
		// fails with the refusal of the item before it.
		var reason string
		var err error
		switch item.Kind {
		case "PersistentVolumeClaim":
			// The snapshot is selected once no backup of the repository is in
			// progress, so after that backup's forget and retime.
			busy, busyErr := r.volumeBackedUp(ctx, run, item.Name)
			if busyErr != nil {
				return ctrl.Result{}, busyErr
			}
			if busy != "" {
				return r.waitAtChecks(ctx, run, busy)
			}
			var snapshot restic.Snapshot
			snapshot, reason, err = r.checkVolume(ctx, run, item.Name, at, sync)
			recordSnapshot(item, snapshot)
			if sync && reason == "" && err == nil {
				switch {
				case synced == nil:
					moment := snapshot.Time.UTC()
					synced = &moment
				case !snapshot.Time.Equal(*synced):
					reason = fmt.Sprintf("its quiesced snapshot %s is from %s and another volume's is from %s; a synced restore needs one moment for every volume",
						snapshot.ShortID(), snapshot.Time.UTC().Format(time.RFC3339), synced.Format(time.RFC3339))
				}
			}
		case "Cluster":
			moment := at
			if sync {
				moment = synced
			}
			if sync && synced == nil {
				reason = "no volume selected a quiesced snapshot, so there is no moment to recover the database to"
				break
			}
			item.BaseBackup, reason, err = r.checkDatabase(ctx, run.Namespace, item.Name, moment)
		}
		// A refusal fails the item with its reason; a reason still given as
		// a string fails it with none.
		if err != nil && !failRestoreItem(item, err) {
			return ctrl.Result{}, err
		}
		if reason != "" {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, reason
		}
		if item.Phase == backupv1alpha1.ItemFailed {
			unreachable = append(unreachable, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
		}
	}

	run.Status.Items = items
	if len(unreachable) > 0 {
		// Nothing has been deleted or overwritten yet. The other items are
		// left as they were, and the run reports every item it cannot reach.
		for i := range run.Status.Items {
			if run.Status.Items[i].Phase == backupv1alpha1.ItemPending {
				run.Status.Items[i].Phase = backupv1alpha1.ItemSkipped
				run.Status.Items[i].Message = "left alone because another item has no backup the run's moment reaches"
			}
		}
		return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, strings.Join(unreachable, "; "))
	}
	if sync {
		if synced == nil {
			return r.finish(ctx, run, backupv1alpha1.ReasonInvalid,
				fmt.Sprintf("syncDatabaseToVolume needs a claim marked %s: \"true\" in this namespace to take the moment from", backupv1alpha1.AnnotationEnabled))
		}
		run.Status.SyncedTo = &metav1.Time{Time: *synced}
	}

	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = &now
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "restoring")
	return after(time.Second, r.writeStatus(ctx, run))
}

// items returns one Pending item for each claim and Cluster the run's spec
// names.
//
// Parameters:
//   - run is the RestoreRun being planned. Its spec.claim, spec.repository,
//     spec.database and spec.all choose the items, and its namespace is where
//     they are looked up.
//
// It returns the items, claims first. spec.claim names one claim and
// spec.database names one Cluster. A run with neither takes every claim and
// every Cluster in the namespace marked backup.wlz.li/enabled: "true". A
// Cluster that archives nowhere has no backup to restore, so its item starts
// out Skipped. So does a Cluster that opts out of the bootstrap webhook, or
// whose owner declares its own bootstrap method (see leftAlone).
//
// It returns an *invalidSpecError, which plan turns into reason Invalid: when
// spec.repository is set and spec.claim is not, since a restore in place
// needs a claim to write into, and the refusal sends the user to spec.into
// with a name no claim has, or to spec.claim to overwrite an existing claim
// in place; when nothing in the namespace is marked; and when spec.database
// names a Cluster that opts out of the bootstrap webhook or declares its own
// bootstrap. It returns the *refusalError of clustersRestoredElsewhere, which
// plan turns into reason Invalid as well, when another unfinished RestoreRun
// is restoring a Cluster the run would restore.
// A failed read comes back as a plain error, and the caller retries.
func (r *RestoreRunReconciler) items(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	pending := func(kind, name string) backupv1alpha1.RestoreItem {
		return backupv1alpha1.RestoreItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}
	switch {
	case run.Spec.Claim != "":
		return []backupv1alpha1.RestoreItem{pending("PersistentVolumeClaim", run.Spec.Claim)}, nil
	case run.Spec.Repository != "":
		return nil, invalidSpec("spec.repository alone restores into a new claim, so spec.into is required, and it must name a claim that does not exist yet. " +
			"To overwrite an existing claim from this repository, set spec.claim to it as well; the run then restores it in place once no pod mounts it.")
	case run.Spec.Database != "":
		cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Spec.Database)
		if err != nil {
			return nil, err
		}
		if found {
			if why := leftAlone(cluster); why != "" {
				return nil, invalidSpec("Cluster %s: %s", run.Spec.Database, why)
			}
		}
		items := []backupv1alpha1.RestoreItem{pending("Cluster", run.Spec.Database)}
		if err := r.clustersRestoredElsewhere(ctx, run, items); err != nil {
			return nil, err
		}
		return items, nil
	}

	claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
	if err != nil {
		return nil, err
	}
	clusters, err := enabledClusters(ctx, r.Reader, r.RESTMapper(), run.Namespace)
	if err != nil {
		return nil, err
	}
	var items []backupv1alpha1.RestoreItem
	for _, claim := range claims {
		items = append(items, pending("PersistentVolumeClaim", claim.Name))
	}
	for i := range clusters {
		item := pending("Cluster", clusters[i].GetName())
		if why := leftAlone(&clusters[i]); why != "" {
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, why
		} else if _, _, archives := bootstrap.Archiver(&clusters[i]); !archives {
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster archives nowhere, so it has no backup to restore"
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, invalidSpec("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
	}
	if err := r.clustersRestoredElsewhere(ctx, run, items); err != nil {
		return nil, err
	}
	return items, nil
}

// clustersRestoredElsewhere refuses a run that would restore a Cluster
// another unfinished RestoreRun is restoring.
//
// Parameters:
//   - run is the run being planned. A RestoreRun with its UID is skipped.
//   - items are the run's items as planned. Only a Cluster item that is
//     Pending counts, since the run leaves a Skipped one alone.
//
// It returns a *refusalError with reason ClusterRestoredElsewhere, naming
// the other run and the Cluster, when another RestoreRun in the namespace
// that has not finished holds an item for one of those Clusters in phase
// Pending, Deleted or Recovering, and nil otherwise.
// A failed list of the RestoreRuns comes back as a plain error, and the
// caller retries.
//
// Two runs that both delete one Cluster race for its recovery: the webhook
// recovers it for the first run it lists, and the other run fails. Refused
// at its checks, the second run deletes nothing. Two runs that planned in the
// same instant both pass that check, so restoreDatabase asks again right
// before it marks a Pending item Deleted. The refusal names the other run,
// so the user knows which run to wait for.
func (r *RestoreRunReconciler) clustersRestoredElsewhere(ctx context.Context, run *backupv1alpha1.RestoreRun, items []backupv1alpha1.RestoreItem) error {
	var clusters []string
	for _, item := range items {
		if item.Kind == "Cluster" && item.Phase == backupv1alpha1.ItemPending {
			clusters = append(clusters, item.Name)
		}
	}
	if len(clusters) == 0 {
		return nil
	}
	restores := &backupv1alpha1.RestoreRunList{}
	if err := r.Reader.List(ctx, restores, client.InNamespace(run.Namespace)); err != nil {
		return fmt.Errorf("list RestoreRuns in %s: %w", run.Namespace, err)
	}
	for i := range restores.Items {
		other := &restores.Items[i]
		if other.UID == run.UID || other.Status.Phase.Finished() {
			continue
		}
		for _, item := range other.Status.Items {
			if item.Kind != "Cluster" || !slices.Contains(clusters, item.Name) {
				continue
			}
			switch item.Phase {
			case backupv1alpha1.ItemPending, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
				return refuse(backupv1alpha1.ItemReasonClusterRestoredElsewhere, "RestoreRun %s is restoring Cluster %s. Create this RestoreRun again once that run has finished", other.Name, item.Name)
			default:
				// An item in any other phase has let go of the Cluster or
				// never deleted it, so it holds no recovery.
			}
		}
	}
	return nil
}

// checkVolume finds the snapshot that a restore of the claim named claimName
// would select.
//
// Parameters:
//   - at is the moment to restore to, or nil for the newest snapshot.
//   - quiescedOnly limits the choice to snapshots tagged quiesced. plan sets
//     it on a run with spec.syncDatabaseToVolume.
//
// It returns the snapshot, or a reason when there is none. When
// repositoryFor refuses the claim, because the claim or its VolumeRestore is
// missing, it returns that *refusalError, and plan fails the item with it
// (see failRestoreItem). It returns a plain error when listing the
// repository fails, and when a read fails for any other reason.
//
// This is the only place a missing snapshot is caught. VolSync restores
// nothing and still reports success when no snapshot matches.
func (r *RestoreRunReconciler) checkVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, string, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		return restic.Snapshot{}, "", err
	}
	return r.selectSnapshot(ctx, run, settings.Secret, at, quiescedOnly)
}

// volumeBackedUp returns otherMover's message naming a backup of the claim
// named claimName, or of the repository it restores from, that is in
// progress, and "" when there is none.
//
// It reads the repository the way checkVolume does. When repositoryFor
// refuses the claim, it returns "", so that checkVolume returns the refusal
// and plan fails the item with it. Any other failed read comes back as an
// error.
func (r *RestoreRunReconciler) volumeBackedUp(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string) (string, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		if _, refused := asItemFailure(err); refused {
			return "", nil
		}
		return "", err
	}
	return otherMover(ctx, r.Reader, run.Namespace, claimName, settings.Secret, backupMover)
}

// selectSnapshot returns the snapshot the run would restore, picked from
// restoreAsOf and previous.
//
// Parameters:
//   - secretName names the restic repository Secret in the run's namespace.
//   - at is the moment to restore to. selectSnapshot takes the newest
//     snapshot at or before it, or the newest of all when it is nil.
//   - quiescedOnly passes over every snapshot not tagged quiesced.
//
// Only a snapshot with the layout VolSync's backup mover gives one, host
// volsync and paths exactly /data, is a candidate (see restic.MoverLayout):
// the restore writes a snapshot's files at the root of the claim, and a
// snapshot of another layout would put them in another directory. When
// spec.previous is set, selectSnapshot then steps that many candidates
// further back. It relies on the lister returning the snapshots oldest
// first, and snapshots of one time in the order of their IDs, so a tie
// resolves the same way on every pass and previous reaches each snapshot of
// it.
//
// Every restore restores the selected snapshot by its full ID, so two
// snapshots in one second, or with the same time, are each restored as
// selected.
//
// It returns a reason, and no error, when the Secret doesn't exist, when no
// snapshot is a candidate (naming the snapshots it passed over, see
// noCandidate), when none is at or before the moment, and when
// spec.previous reaches past the oldest candidate. It returns an error when
// the Secret can't be read for another reason and when listing the
// repository fails.
func (r *RestoreRunReconciler) selectSnapshot(ctx context.Context, run *backupv1alpha1.RestoreRun, secretName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, string, error) {
	all, reason, err := r.listRepository(ctx, run, secretName)
	if reason != "" || err != nil {
		return restic.Snapshot{}, reason, err
	}
	snapshots := slices.DeleteFunc(slices.Clone(all), func(s restic.Snapshot) bool {
		return !restic.MoverLayout(s) || quiescedOnly && !slices.Contains(s.Tags, restic.QuiescedTag)
	})
	if len(snapshots) == 0 {
		return restic.Snapshot{}, noCandidate(all, quiescedOnly), nil
	}

	index := len(snapshots) - 1
	if at != nil {
		found, ok := restic.AtOrBefore(snapshots, *at)
		if !ok {
			return restic.Snapshot{}, fmt.Sprintf("no snapshot at or before %s; the oldest, %s, is from %s",
				at.UTC().Format(time.RFC3339), snapshots[0].ShortID(), snapshots[0].Time.UTC().Format(time.RFC3339)), nil
		}
		index = slices.IndexFunc(snapshots, func(s restic.Snapshot) bool { return s.ID == found.ID })
	}
	if run.Spec.Previous != nil {
		index -= int(*run.Spec.Previous)
		if index < 0 {
			return restic.Snapshot{}, fmt.Sprintf("previous %d reaches past the oldest snapshot", *run.Spec.Previous), nil
		}
	}
	return snapshots[index], "", nil
}

// noCandidate returns the reason of a run whose repository holds no
// snapshot selectSnapshot may restore.
//
// Parameters:
//   - all is every snapshot in the repository, oldest first.
//   - quiescedOnly is selectSnapshot's: the run takes only snapshots tagged
//     quiesced.
//
// It says the repository is empty; or that none of its snapshots has the
// mover's layout, naming the newest ones with their hosts and paths (see
// passedOver); or that none of those is tagged quiesced.
func noCandidate(all []restic.Snapshot, quiescedOnly bool) string {
	if len(all) == 0 {
		return "the repository holds no snapshot"
	}
	if slices.ContainsFunc(all, restic.MoverLayout) && quiescedOnly {
		return fmt.Sprintf("the repository holds no snapshot tagged %s; only a BackupRun that stopped the workloads writes one", restic.QuiescedTag)
	}
	return fmt.Sprintf("the repository holds %d snapshots, none written by a VolSync mover (host volsync, paths [/data]): %s",
		len(all), passedOver(all))
}

// passedOverShown is how many snapshots the reason of a repository with no
// snapshot of the mover's layout names at most, so a large repository still
// gives an item message of a few lines.
const passedOverShown = 5

// passedOver names the newest snapshots of a repository that has none of
// the mover's layout, for noCandidate.
//
// Parameters:
//   - all is every snapshot in the repository, oldest first, and not empty.
//
// It returns the newest passedOverShown snapshots, newest first, each with
// its short ID, host and paths, and a count of the older ones it leaves out.
func passedOver(all []restic.Snapshot) string {
	shown := all[max(0, len(all)-passedOverShown):]
	names := make([]string, 0, len(shown)+1)
	for i := len(shown) - 1; i >= 0; i-- {
		s := shown[i]
		names = append(names, fmt.Sprintf("%s (host %s, paths %v)", s.ShortID(), s.Hostname, s.Paths))
	}
	if older := len(all) - len(shown); older > 0 {
		names = append(names, fmt.Sprintf("and %d older", older))
	}
	return strings.Join(names, ", ")
}

// recordSnapshot records on a volume item the snapshot the checks selected:
// its full ID, which the restore Job restores, its short ID for display,
// and its time.
//
// Parameters:
//   - item is the volume item, updated in place.
//   - snapshot is the selected snapshot, or the zero Snapshot when the
//     checks selected none, which records nothing but an empty short ID.
func recordSnapshot(item *backupv1alpha1.RestoreItem, snapshot restic.Snapshot) {
	item.Snapshot, item.SnapshotID = snapshot.ShortID(), snapshot.ID
	if !snapshot.Time.IsZero() {
		item.SnapshotTime = &metav1.Time{Time: snapshot.Time}
	}
}

// repositorySnapshots lists every snapshot in a run's restic repository,
// oldest first.
//
// Parameters:
//   - run is the RestoreRun; the Secret is read in its namespace.
//   - secretName names the repository Secret.
//
// It returns the snapshots. It returns a *refusalError with reason
// RepositorySecretMissing when the Secret doesn't exist, and a plain error
// when the Secret can't be read for another reason or listing the
// repository fails.
func (r *RestoreRunReconciler) repositorySnapshots(ctx context.Context, run *backupv1alpha1.RestoreRun, secretName string) ([]restic.Snapshot, error) {
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: secretName}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("read repository Secret %s: %w", secretName, err)
		}
		return nil, refuse(backupv1alpha1.ItemReasonRepositorySecretMissing, "no repository Secret %s in this namespace", secretName)
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return nil, fmt.Errorf("list the snapshots in %s: %w", secretName, err)
	}
	return snapshots, nil
}

// listRepository lists every snapshot in a run's restic repository, oldest
// first, for the checks that still take a reason as a string.
//
// Parameters:
//   - run is the RestoreRun; the Secret is read in its namespace.
//   - secretName names the repository Secret.
//
// It returns the snapshots. It returns the refusal's text as the reason,
// and no error, when the Secret doesn't exist. It returns an error when the
// Secret can't be read for another reason and when listing the repository
// fails.
//
// It turns the typed refusal of repositorySnapshots back into a string,
// which selectSnapshot still returns as its reason.
func (r *RestoreRunReconciler) listRepository(ctx context.Context, run *backupv1alpha1.RestoreRun, secretName string) ([]restic.Snapshot, string, error) {
	snapshots, err := r.repositorySnapshots(ctx, run, secretName)
	var refused *refusalError
	if errors.As(err, &refused) {
		return nil, refused.Error(), nil
	}
	return snapshots, "", err
}

// nothingWritten returns the end of the message of a restore that failed
// before its mover existed: nothing was written to the claim named claim,
// and a new RestoreRun selects again.
func nothingWritten(claim string) string {
	return fmt.Sprintf(". Nothing was written to claim %s. Create a new RestoreRun to select again", claim)
}

// checkDatabase finds the base backup that a recovery of one Cluster would
// start from. The Cluster is the one its namespace and name arguments give.
// The base backup is the newest one that finished at or before the time given
// in at, or the newest of all when no time is given.
//
// It returns the base backup's ID, or a reason when there is none. It also
// returns a reason when the Cluster is missing, archives nowhere, or names an
// object store that is missing or incomplete. A store with no completed base
// backup is a reason too, because deleting the Cluster would bring it back
// empty. It returns an error when the Cluster can't be read, when a read of
// the store or its Secrets fails in a way a retry may fix, and when listing
// the base backups fails.
func (r *RestoreRunReconciler) checkDatabase(ctx context.Context, namespace, name string, at *time.Time) (string, string, error) {
	cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), namespace, name)
	if err != nil {
		return "", "", err
	}
	if !found {
		return "", fmt.Sprintf("no Cluster %s in this namespace", name), nil
	}
	store, serverName, archives := bootstrap.Archiver(cluster)
	if !archives {
		return "", "the Cluster archives nowhere, so it has no backup to restore", nil
	}
	location, err := bootstrap.ResolveLocation(ctx, r.Reader, r.RESTMapper(), namespace, store, serverName)
	if err != nil {
		if retryable(err) {
			return "", "", err
		}
		return "", err.Error(), nil
	}
	backups, err := r.Prober.BaseBackups(ctx, location)
	if err != nil {
		return "", "", fmt.Errorf("list the base backups of %s: %w", name, err)
	}
	if len(backups) == 0 {
		return "", fmt.Sprintf("%s/%s holds no completed base backup; deleting the Cluster would bring it back empty", location.Bucket, location.BasePrefix()), nil
	}
	if at == nil {
		return backups[len(backups)-1].ID, "", nil
	}
	backup, ok := bootstrap.AtOrBefore(backups, *at)
	if !ok {
		return "", fmt.Sprintf("no base backup finished by %s; the oldest, %s, finished at %s",
			at.UTC().Format(time.RFC3339), backups[0].ID, backups[0].End.UTC().Format(time.RFC3339)), nil
	}
	return backup.ID, "", nil
}

// work makes one pass over an in-place run that plan has checked, and
// returns when to look again.
//
// Parameters:
//   - run is the RestoreRun, in phase Running or Waiting with its items
//     planned. work updates its status in place and writes it.
//
// It returns a result that requeues the run while it has work left, and an
// empty result once the run has ended (see finish). A failed read, write or
// delete comes back as an error, and controller-runtime retries the pass.
//
// A run past spec.timeout with an item still unfinished ends with reason
// TimedOut (see timeOut), and a message that names the SourceBusy wait it
// was in. A run whose items have all finished only waits for its stopped
// movers to go before it gives the app back, so a deadline that passes
// during that wait leaves it waiting, and it ends as its items say. With
// spec.quiesce set, the first passes call quiesce to stop the listed
// workloads, and later passes restore nothing until every pod of those
// workloads is gone. work then moves each volume item a step further (see
// restoreVolume), and stops the restore Job of each item that has finished
// once its end is in the status (see stopJobs). The databases wait
// until every volume item is done; when a volume restore failed, the
// databases still Pending are skipped and left running, and otherwise each
// is moved a step further (see restoreDatabase).
//
// While a mover the run stopped is not gone yet, the run waits with reason
// WaitingForShutdown and gives nothing back (rule X2). After a Cluster is
// deleted, work also waits with reason WaitingForShutdown until the old
// Cluster's instance pods and PVCs are gone (see instanceLeft). Only then does
// it start the stopped workloads again and resume the Kustomizations it
// suspended, and it waits with reason WaitingForRecreate, whose message asks
// for the Cluster to be created again. The run finishes once every item is
// Succeeded, Failed or Skipped: Failed when an item failed, Failed with
// reason NoBackupInReach when every item was Skipped (see nothingRestored),
// and Succeeded otherwise. At the start of each pass work releases the Leases
// of the items that finished in an earlier pass and whose movers are gone:
// an item that still names a restore Job has one Stop has not reported
// stopped (see restoreItemDone).
//
// While clusterWebhookBlind reports that the bootstrap webhook would not see
// a Cluster created again, work deletes no Cluster: it fails every Pending
// Cluster item (see failBlindClusters), a run that ends in the pass that
// failed such an item ends with reason ClusterVersionUnsupported (see
// failedReason), and a run that waits for a deleted Cluster waits with that
// reason and the message from recreateMessage.
func (r *RestoreRunReconciler) work(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	// A run whose items have all finished only waits to give back what it
	// holds, and that wait is bounded by its movers (rule X2), so it ends as
	// its items say.
	if deadline, over := r.overdue(run); over && !restoreDone(run.Status.Items) {
		return r.timeOut(ctx, run, timedOutMessage(deadline, run.Status.Conditions))
	}
	// The Leases of an item that finished in an earlier pass go now, so a
	// backup of that claim need not wait for the rest of the run. An item
	// whose stopped restore Job may still write keeps its Lease: another
	// run's mover must not start on the claim or the repository meanwhile
	// (rule X2, see restoreItemDone).
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(name string) bool {
		return restoreItemDone(run, name)
	}); err != nil {
		return ctrl.Result{}, err
	}
	// The quiesce Leases go once the stored status shows every workload back,
	// since the run then never touches them again. Best effort: a Lease left
	// behind is stale under holderLive's rule and the next run takes it over.
	if durablyRestarted(run) {
		if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
			log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the run goes on",
				"namespace", run.Namespace, "name", run.Name)
		}
	}

	if len(run.Spec.Quiesce) > 0 && run.Status.QuiescedAt == nil {
		return r.quiesce(ctx, run)
	}
	if stopped(run) && anyRestorePending(run.Status.Items) {
		targets, err := namedTargets(ctx, r.Reader, run.Namespace, run.Spec.Quiesce)
		if err != nil {
			if !isQuiesceSpecError(err) {
				return ctrl.Result{}, err
			}
			return r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
		}
		gone, pod, err := podsGone(ctx, r.Reader, run.Namespace, targets)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			return after(2*time.Second, r.waitFor(ctx, run, backupv1alpha1.ReasonRunning,
				fmt.Sprintf("waiting for pod %s to stop before anything is restored", pod)))
		}
	}

	waitReason, waitMessage := "", ""
	volumesDone, volumesFailed := true, false
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != "PersistentVolumeClaim" {
			continue
		}
		reason, message, err := r.restoreVolume(ctx, run, i, item)
		if err != nil {
			return ctrl.Result{}, err
		}
		if reason != "" {
			waitReason, waitMessage = reason, message
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning:
			volumesDone = false
		case backupv1alpha1.ItemFailed:
			volumesFailed = true
		default:
			// A volume item in any other phase has finished without a
			// failure, so it changes neither flag.
		}
	}

	// A finished volume item's phase goes into the status before its restore
	// Job is stopped. The Job's conditions are the only record of how the
	// restore ended. Deleted first, a lost status write or a crash would
	// leave a Running item whose Job is gone, and no later pass could tell
	// how it ended. A later pass stops the Job of any finished item that
	// still names one (see stopJobs). The status is written only when it
	// differs from the stored one: a pass that waits for the same stopped
	// Job as the pass before has its items stored already, and a write
	// would only start another reconcile.
	var stopping jobList
	if finishedWithMover(run.Status.Items) {
		if err := r.writeChangedStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		left, err := r.stopJobs(ctx, run, finished)
		if err != nil {
			return ctrl.Result{}, err
		}
		stopping = left
	}

	// A Cluster the webhook would not see created again comes back empty,
	// so no Cluster is deleted while that holds (see clusterWebhookBlind).
	blind := clusterWebhookBlind(r.RESTMapper())
	blindFailed := failBlindClusters(run.Status.Items, blind)
	if len(blindFailed) > 0 {
		log.FromContext(ctx).Error(errors.New(blind.message()), "deleting no Cluster: the bootstrap webhook would not see it created again",
			"namespace", run.Namespace, "name", run.Name)
	}

	var recreate, shuttingDown []string
	if volumesDone {
		for i := range run.Status.Items {
			item := &run.Status.Items[i]
			if item.Kind != "Cluster" {
				continue
			}
			if volumesFailed && item.Phase == backupv1alpha1.ItemPending {
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, "left running because a volume restore failed"
				continue
			}
			if err := r.restoreDatabase(ctx, run, item); err != nil {
				return ctrl.Result{}, err
			}
			if item.Phase == backupv1alpha1.ItemDeleted {
				left, err := instanceLeft(ctx, r.Reader, run.Namespace, item.Name)
				if err != nil {
					return ctrl.Result{}, err
				}
				if left != "" {
					shuttingDown = append(shuttingDown, left)
				} else {
					recreate = append(recreate, item.Name)
				}
			}
		}
	}

	// A restore Job the run stopped may still write into a claim or the
	// repository, so the run gives nothing back and releases no Lease it
	// still holds until no pod of that Job can write (rule X2, see
	// stopJobs). Only a finished item counts here: the Job of an item that
	// is still Pending or Running is doing the restore, and belongs there.
	if len(stopping) > 0 {
		return r.waitForStopped(ctx, run, stopping.message())
	}

	// A database comes back only when its owner creates it again, and a
	// Kustomization this run suspended creates nothing. So the app is given
	// back once every volume is restored and every database is deleted, down
	// to its last instance pod and PVC. A run whose status shows the restart
	// done starts nothing again, so it never scales up a workload another run
	// has stopped since.
	if volumesDone && !anyRestorePending(run.Status.Items) && len(shuttingDown) == 0 && stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}

	if restoreDone(run.Status.Items) {
		if failed := restoreFailures(run.Status.Items); failed != "" {
			return r.finish(ctx, run, failedReason(blindFailed), failed)
		}
		if skipped := nothingRestored(run.Status.Items); skipped != "" {
			return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, skipped)
		}
		return r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, "every item holds the restored data")
	}

	switch {
	case len(shuttingDown) > 0:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonShutdown,
			fmt.Sprintf("waiting for %s of the deleted Cluster to be gone before anything creates it again", strings.Join(shuttingDown, ", "))))
	case len(recreate) > 0 && blind.blind():
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonClusterVersionUnsupported,
			blind.recreateMessage(recreate)))
	case len(recreate) > 0:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonRecreate,
			fmt.Sprintf("recreate %s to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it", strings.Join(recreate, ", "))))
	case waitReason != "":
		return after(pollInterval, r.waitFor(ctx, run, waitReason, waitMessage))
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "restoring")
	return after(pollInterval, r.writeStatus(ctx, run))
}

// restoreVolume moves one in-place volume restore a step further, through
// the item's restore Job.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - index is the item's position in status.items. It goes into the name of
//     the item's restore Job (see jobName).
//   - item is the volume item, which restoreVolume updates in place.
//
// It returns a Ready reason and message while a Pending item waits, and
// empty strings otherwise, and an error, which leaves the item as it was,
// when an API call fails for a reason a retry may fix.
//
// A Pending item starts its restore Job once nothing holds it back (see
// startJob). A Running item follows its Job to its end (see followJob), and
// its Job, created suspended, is resumed once the item's Job name and UID
// are in the stored status (see resumeJob): the pass read them from there,
// and a pass with an item still Running does not end the run. The run
// stops the Job once the item's end is in the status (see stopJobs).
func (r *RestoreRunReconciler) restoreVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (reason, message string, err error) {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		return r.startJob(ctx, run, index, item)
	case backupv1alpha1.ItemRunning:
		seen, err := r.followJob(ctx, run, item)
		if done, err := settled(item, err); done || seen.unresumed == nil {
			return "", "", err
		}
		_, err = settled(item, r.resumeJob(ctx, *item, seen.unresumed))
		return "", "", err
	default:
		// An item in any other phase has finished, so there is nothing
		// left to restore.
		return "", "", nil
	}
}

// startRefusal checks whether the item's claim or repository settings refuse
// a Pending in-place restore, and reads those settings when they don't.
// restoreVolume calls it before it takes the Leases, and quiesce calls it in
// its pre-check, so an item that cannot start fails before the app is
// stopped for it.
//
// Parameters:
//   - run is the asking run; its namespace is read, and spec.repository and
//     spec.moverSecurityContext are used the way repositoryFor uses them.
//   - claimName names the item's claim, which is also the item's name.
//
// It returns the settings from repositoryFor when the restore can go on. A
// claim that is being deleted (reason ClaimDeleting), and a refusal from
// repositoryFor (a claim that is gone, or a VolumeRestore that is missing
// when the run names no repository), come back as a *refusalError that
// says nothing was written to the claim (see nothingWrittenTo); the caller
// fails the item with it through failRestoreItem. A claim being deleted is
// refused because the mover would write into a claim that is about to go,
// and the scheduler does not place a pod whose claim is being deleted
// (podHasPVCs, in the PreFilter of the volumebinding plugin). A failed read
// comes back as a plain error, and the caller leaves the item Pending.
func (r *RestoreRunReconciler) startRefusal(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string) (restoreSettings, error) {
	claim := &corev1.PersistentVolumeClaim{}
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim)
	switch {
	case apierrors.IsNotFound(err):
		// repositoryFor refuses the claim that is gone with its message.
	case err != nil:
		return restoreSettings{}, fmt.Errorf("get PersistentVolumeClaim %s/%s: %w", run.Namespace, claimName, err)
	case claim.DeletionTimestamp != nil:
		return restoreSettings{}, nothingWrittenTo(claimName,
			refuse(backupv1alpha1.ItemReasonClaimDeleting, "claim %s is being deleted", claimName))
	}
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	return settings, nothingWrittenTo(claimName, err)
}

// inPlaceClaimLost checks that the claim an in-place item restored is
// still the claim the run checked and took its Lease on.
//
// Parameters:
//   - run is the RestoreRun. Its claim Leases are the ones labelled with its
//     UID.
//   - claimName names the claim, which is also the item's name.
//
// It returns nil while that claim is there, and a *refusalError with reason
// ClaimLost when the claim is gone, is being deleted, or is another claim.
// A run that holds no claim Lease for the item can't tell which claim the
// mover wrote into, and gets that refusal too, since the item must not
// succeed without that evidence. A failed read of the claim or the Leases
// comes back as a plain error, and the caller leaves the item as it was.
//
// The claim the run checked is the one it took its claim Lease on right
// before it created the item's restore Job: the Lease is named after that
// claim's UID (see claimLeaseName) and lists the item. A claim whose UID is
// not among the run's claim Leases for the item was created after that
// create, so it replaced the claim the run checked. The Job mounts the claim
// by name, so a pod of it that started after the replacement may have
// written into the new claim, and the message asks for that claim's data to
// be checked.
//
// A claim deleted while its mover's pod mounts it stays, Terminating, until
// the pod is gone (pvc-protection), so the mover can complete into a claim
// that is about to go.
func (r *RestoreRunReconciler) inPlaceClaimLost(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string) error {
	leases := &coordinationv1.LeaseList{}
	if err := r.Reader.List(ctx, leases, client.InNamespace(run.Namespace), client.MatchingLabels{labelLeaseHolderUID: string(run.UID)}); err != nil {
		return fmt.Errorf("list the Leases of RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	prefix := claimLeaseName("")
	var leased []string
	for _, lease := range leases.Items {
		if strings.HasPrefix(lease.Name, prefix) && slices.Contains(leaseItems(&lease), claimName) {
			leased = append(leased, strings.TrimPrefix(lease.Name, prefix))
		}
	}
	if len(leased) == 0 {
		return refuse(backupv1alpha1.ItemReasonClaimLost, "the run holds no claim Lease for claim %s, so it can't tell whether the mover wrote into the claim that is there now. "+
			"Check the claim's data, and create a new RestoreRun to restore it", claimName)
	}

	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return refuse(backupv1alpha1.ItemReasonClaimLost, "claim %s was deleted while the mover wrote into it, and the restored data went with it", claimName)
		}
		return fmt.Errorf("get claim %s/%s: %w", run.Namespace, claimName, err)
	}
	if claim.DeletionTimestamp != nil {
		return refuse(backupv1alpha1.ItemReasonClaimLost, "claim %s was deleted while the mover wrote into it, and the restored data goes with it once the claim is released", claimName)
	}
	if !slices.Contains(leased, string(claim.UID)) {
		return refuse(backupv1alpha1.ItemReasonClaimLost, "claim %[1]s was replaced while the mover wrote into it: the claim there now (UID %[2]s) is not the one the run checked "+
			"and took its Lease on (UID %[3]s). The mover mounts claim %[1]s by name, so it may have written into it; check its data, "+
			"and create a new RestoreRun to restore it", claimName, claim.UID, strings.Join(leased, ", "))
	}
	return nil
}

// restoreDatabase moves one database item a step further: it deletes the
// Cluster of a Pending item, and follows a Deleted or Recovering item until
// the Cluster is recovered.
//
// Parameters:
//   - run is the RestoreRun the item belongs to. Its name is the mark the
//     bootstrap webhook sets on a Cluster it recovers for the run, and its
//     status is written before a delete.
//   - item is the Cluster item, which restoreDatabase updates in place.
//
// It returns an error when a read of the Cluster, the list of the
// RestoreRuns, the status write or the delete fails, and the caller retries.
// It returns nil otherwise, with the item's new phase and message set.
//
// A Pending item whose Cluster opts out of the bootstrap webhook, or whose
// owner declares its own bootstrap method (see leftAlone), moves to Skipped,
// and the run never deletes that Cluster. Right before it marks any other
// Pending item Deleted, restoreDatabase checks again that no other unfinished
// run is restoring the Cluster (see clustersRestoredElsewhere), because two
// runs that planned in the same instant both passed the check at their plan.
// When another run holds the Cluster, the item fails with the refusal,
// naming that run, and the run deletes nothing.
//
// The item is marked Deleted, and the status written, before the Cluster is
// deleted. The bootstrap webhook recovers a Cluster only for a run whose item
// says Deleted, so the mark has to be in place before anything can create the
// Cluster again. The same write records the old Cluster's UID in
// status.items[].clusterUID. A run that found no Cluster to delete leaves its
// item Deleted without a UID.
//
// A Deleted item waits while no Cluster of its name exists, or while one is
// being deleted, and otherwise sorts the live Cluster into one of three kinds:
//
//   - The old Cluster, whose UID is the recorded one: the delete failed, or
//     the controller stopped after the mark was written. The run deletes it
//     again, or Skips the item when the Cluster opted out or declared its own
//     bootstrap since. The webhook's backup.wlz.li/restore-run annotation
//     stays on a recovered Cluster for good, so it says nothing on the old
//     Cluster.
//   - This run's recovery: another UID and the annotation naming this run.
//     The item moves to Recovering.
//   - Any other Cluster: one created again empty by the owner's choice, with
//     its own bootstrap, archiving nowhere, recovered for another run, or not
//     seen by the webhook. The item fails with a message saying so (see
//     notRecovered), and the run leaves the Cluster alone. It did not create
//     that Cluster, and deleting it would only loop as Flux creates it again.
//     An item without a recorded UID has no old Cluster, so any live Cluster
//     that is not the run's recovery lands here.
//
// A Recovering item succeeds once the Cluster reports the healthy phase and
// still carries the run's mark, and fails when the recovered Cluster is
// deleted or replaced by one without the mark.
func (r *RestoreRunReconciler) restoreDatabase(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) error {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		if err != nil {
			return err
		}
		if found {
			if why := leftAlone(cluster); why != "" {
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, why
				return nil
			}
		}
		// Two runs that planned in the same instant both passed this check
		// at their plan. Checked again right before the mark, the second
		// run finds the first one's item Pending or Deleted and deletes
		// nothing; of two runs that get here together, each finds the other
		// Pending and neither deletes.
		err = r.clustersRestoredElsewhere(ctx, run, []backupv1alpha1.RestoreItem{*item})
		if failRestoreItem(item, nothingDeleted(err)) {
			return nil
		}
		if err != nil {
			return err
		}
		if found {
			item.ClusterUID = cluster.GetUID()
		}
		item.Phase = backupv1alpha1.ItemDeleted
		if err := r.writeStatus(ctx, run); err != nil {
			return err
		}
		if !found {
			return nil
		}
		return r.deleteCluster(ctx, cluster)

	case backupv1alpha1.ItemDeleted:
		cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		switch {
		case err != nil:
			return err
		case !found || cluster.GetDeletionTimestamp() != nil:
			return nil
		case item.ClusterUID != "" && cluster.GetUID() == item.ClusterUID:
			// The old Cluster, which the earlier delete did not reach. Its
			// annotations say nothing about this run.
			if why := leftAlone(cluster); why != "" {
				// Marked to be left alone before the delete reached it.
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, why
				return nil
			}
			return r.deleteCluster(ctx, cluster)
		case cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun] == run.Name:
			item.Phase = backupv1alpha1.ItemRecovering
		default:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, notRecovered(item, cluster)
		}

	case backupv1alpha1.ItemRecovering:
		cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		switch {
		case err != nil:
			return err
		case !found || cluster.GetDeletionTimestamp() != nil:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, "the recovered Cluster was deleted"
		case cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun] != run.Name:
			// The webhook recovers a Cluster only for a run whose item says
			// Deleted, so one created again now carries no mark of this run.
			item.Phase, item.Message = backupv1alpha1.ItemFailed, "the recovered Cluster was replaced by one this run did not recover"
		case clusterPhase(cluster) == healthyPhase:
			item.Phase = backupv1alpha1.ItemSucceeded
		}
	default:
		// An item in any other phase has finished, so there is nothing
		// left to restore.
	}
	return nil
}

// notRecovered explains why a live Cluster that a Deleted item finds is not
// the run's recovery, for the item's message. The caller has already ruled
// out the old Cluster (the recorded UID) and the run's own recovery (the
// backup.wlz.li/restore-run annotation naming the run).
//
// Parameters:
//   - item is the run's Deleted database item. An empty item.ClusterUID
//     means the run can't tell the old Cluster from a new one.
//   - cluster is the live Cluster of the item's name.
//
// With a recorded UID, a Cluster that opted out of the bootstrap webhook or
// declares its own bootstrap came back without a recovery, and the message
// says nothing was restored and how to recover it. Any other Cluster gets
// "came back without this run's recovery" with the reason: it archives
// nowhere, another RestoreRun recovered it, or the webhook did not mark it.
// Without a recorded UID the message gives the same reason and says the run
// can't tell whether this is the Cluster it meant to delete. Every message
// says the run leaves the Cluster alone.
func notRecovered(item *backupv1alpha1.RestoreItem, cluster *unstructured.Unstructured) string {
	name, uid := cluster.GetName(), cluster.GetUID()
	optedOut := cluster.GetAnnotations()[bootstrap.OptOutAnnotation] == bootstrap.OptOutValue
	method := bootstrap.OwnerBootstrap(cluster)

	var why string
	switch other := cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun]; {
	case optedOut:
		why = fmt.Sprintf("it carries %s: %s", bootstrap.OptOutAnnotation, bootstrap.OptOutValue)
	case method != "":
		why = fmt.Sprintf("it declares its own spec.bootstrap.%s", method)
	case !archives(cluster):
		why = "it archives nowhere"
	case other != "":
		why = fmt.Sprintf("RestoreRun %s recovered it", other)
	default:
		why = "the bootstrap webhook did not mark it"
	}

	if item.ClusterUID == "" {
		return fmt.Sprintf("Cluster %s (UID %s) is not this run's recovery: %s. The item holds no clusterUID, "+
			"because the run found no Cluster at its start, so it can't tell the old Cluster from a new one, "+
			"and it leaves this one alone. Create a new RestoreRun to restore it", name, uid, why)
	}
	switch {
	case optedOut:
		return fmt.Sprintf("Cluster %s came back carrying %s: %s after this run deleted it, so it started empty and nothing was restored. "+
			"Remove the annotation from its manifest and create a new RestoreRun to recover it. The run leaves the Cluster alone",
			name, bootstrap.OptOutAnnotation, bootstrap.OptOutValue)
	case method != "":
		return fmt.Sprintf("Cluster %s came back declaring spec.bootstrap.%s after this run deleted it, so it started from that bootstrap and nothing was restored. "+
			"Remove spec.bootstrap.%s from its manifest and create a new RestoreRun to recover it. The run leaves the Cluster alone",
			name, method, method)
	}
	return fmt.Sprintf("Cluster %s (UID %s) came back without this run's recovery: %s. The run does not delete a Cluster it did not recover",
		name, uid, why)
}

// archives reports whether a Cluster archives its WAL through the
// barman-cloud plugin (see bootstrap.Archiver). The webhook admits a Cluster
// that archives nowhere unchanged, so such a Cluster is never a recovery.
func archives(cluster *unstructured.Unstructured) bool {
	_, _, found := bootstrap.Archiver(cluster)
	return found
}

// deleteCluster deletes the Cluster the run read. The delete carries the
// Cluster's UID as a precondition, so it never reaches a Cluster of the same
// name created since the read. A Cluster that is already gone is not an
// error. The delete goes out at the version the Cluster was read at, which
// getCluster looked up; a delete the API server refuses because it has
// stopped serving that version since is an error (see served.VersionGone),
// never a Cluster that is gone.
func (r *RestoreRunReconciler) deleteCluster(ctx context.Context, cluster *unstructured.Unstructured) error {
	uid := cluster.GetUID()
	err := served.VersionGone(r.RESTMapper(), cluster.GroupVersionKind(), r.Delete(ctx, cluster, client.Preconditions{UID: &uid}))
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Cluster %s/%s: %w", cluster.GetNamespace(), cluster.GetName(), err)
	}
	return nil
}

// planIntoNewClaim checks an into restore before it creates anything, and
// starts the run when the check passes.
//
// Parameters:
//   - run is the RestoreRun with an empty phase and spec.into set. It
//     restores from the backups of spec.claim, or from spec.repository.
//
// It returns the result of the pass: a run whose check passed is moved to
// Running, with status.startedAt set and its single item Running on the
// selected snapshot, and requeued after a second; a run that ends here
// returns what finish returns; a run that waits for a backup returns what
// waitAtChecks returns. A failed read, and a failed listing of the
// repository, come back as an error for a retry (see planFailed).
//
// The check selects the snapshot the restore would use and records its full
// ID, short ID and time on the item (see recordSnapshot); the restore Job
// that restoreIntoEmptyClaim creates restores exactly that full ID. The
// item names no Job yet. Before it selects the snapshot, planIntoNewClaim
// waits, as plan does, while a backup of the source claim or the repository
// is in progress (see otherMover and waitAtChecks).
//
// A spec that can't work ends the run as Failed with reason Invalid: a claim
// named spec.into that exists and that the run did not create (see
// notCreatedByRun); for a restore from spec.claim, a VolumeRestore of that
// name, which describes the backups of a claim of that name; a source claim
// or VolumeRestore that is missing; a restoreAsOf that doesn't parse; or a
// restore from spec.repository alone without spec.intoSize. The run writes
// only into a claim it creates itself, so it never overwrites or takes over
// one it finds. A run with no snapshot in reach, or whose snapshot
// selectSnapshot refuses, ends with reason NoBackupInReach.
func (r *RestoreRunReconciler) planIntoNewClaim(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	run.Status.Target = run.Spec.Into
	err := r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, "claim")
	if err == nil && run.Spec.Claim != "" {
		err = r.intoTaken(ctx, run, &backupv1alpha1.VolumeRestore{}, "VolumeRestore")
	}
	if asRunRefusal(err) {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if asRunRefusal(err) {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if settings.Capacity == nil && run.Spec.IntoSize == nil {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid,
			"spec.intoSize is required when spec.repository names the source, because there is no source claim to copy a size from")
	}
	at, err := target(run)
	if err != nil {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	// The snapshot is selected once no backup of the repository is in
	// progress, so after that backup's forget and retime.
	busy, err := otherMover(ctx, r.Reader, run.Namespace, run.Spec.Claim, settings.Secret, backupMover)
	if err != nil {
		return ctrl.Result{}, err
	}
	if busy != "" {
		return r.waitAtChecks(ctx, run, busy)
	}
	snapshot, reason, err := r.selectSnapshot(ctx, run, settings.Secret, at, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	if reason != "" {
		return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, reason)
	}
	item := backupv1alpha1.RestoreItem{
		Kind: backupv1alpha1.ItemKindClaim, Name: run.Spec.Into, Phase: backupv1alpha1.ItemRunning,
	}
	recordSnapshot(&item, snapshot)
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = &now
	run.Status.Items = []backupv1alpha1.RestoreItem{item}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("restoring into claim %s", run.Spec.Into))
	return after(time.Second, r.writeStatus(ctx, run))
}

// intoTimedOut returns the message of an into restore that ran past its
// deadline.
//
// Parameters:
//   - into is the claim the run restores into, spec.into, which the message
//     names.
//   - deadline is the run's deadline as overdue returns it, which the
//     message gives.
//
// timeOut puts the message on the run and on its item, and records it in
// status.ending, which a later pass ends the run with.
func intoTimedOut(into string, deadline time.Time) string {
	return fmt.Sprintf("claim %s had not been restored by %s", into, deadline.Format(time.RFC3339))
}

// intoTaken reads the object named spec.into into object, and refuses it
// when it exists and the run did not create it.
//
// Parameters:
//   - object is an empty claim or VolumeRestore, of the kind to read.
//   - kind is "claim" or "VolumeRestore", for the message.
//
// It returns nil when the object does not exist or the run controls it, and
// the *refusalError from notCreatedByRun, with reason IntoClaimTaken,
// otherwise. The read goes through the uncached Reader, so an object
// created a moment ago is seen. A failed read comes back as a plain error,
// and the caller retries.
func (r *RestoreRunReconciler) intoTaken(ctx context.Context, run *backupv1alpha1.RestoreRun, object client.Object, kind string) error {
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	if err := r.Reader.Get(ctx, key, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get %s %s: %w", kind, key, err)
	}
	return notCreatedByRun(run, kind, object)
}

// createOwned creates object, which carries the run's controller reference,
// and makes sure the object of that name is one the run controls.
//
// Parameters:
//   - object is the claim to create.
//   - existing is an empty object of the same kind, which the stored object
//     is read into when the create finds one.
//   - kind is "claim", for the message.
//
// It returns nil once an object of that name exists that the run controls,
// and the *refusalError from notCreatedByRun, with reason IntoClaimTaken,
// for any other; nothing is written to that one. A failed create or read
// comes back as a plain error, and the caller retries.
//
// A create that finds the name taken reads the stored object. The run's own,
// left by a pass whose create went through but whose answer was lost, is
// fine.
func (r *RestoreRunReconciler) createOwned(ctx context.Context, run *backupv1alpha1.RestoreRun, object, existing client.Object, kind string) error {
	err := r.Create(ctx, object)
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s %s/%s: %w", kind, object.GetNamespace(), object.GetName(), err)
	}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(object), existing); err != nil {
		return fmt.Errorf("get %s %s/%s: %w", kind, object.GetNamespace(), object.GetName(), err)
	}
	return notCreatedByRun(run, kind, existing)
}

// claimLost checks that the claim an into restore writes into is still the
// run's own.
//
// Parameters:
//   - run is the RestoreRun. spec.into names the claim, and the claim is the
//     run's own when the run is its controller (see notCreatedByRun).
//
// It returns nil while the run's own claim is there, and a *refusalError
// with reason ClaimLost when the claim is gone, is being deleted, or is
// controlled by something other than the run. A failed read that is not
// NotFound comes back as a plain error, and the caller leaves the item as it
// was.
//
// An into restore checks it on every pass once its restore Job exists (see
// claimLostError), so a Job that finished never counts as a restore into a
// claim that is no longer the run's. inPlaceClaimLost does the same for an
// in-place item.
func (r *RestoreRunReconciler) claimLost(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	err := r.Reader.Get(ctx, key, claim)
	switch {
	case apierrors.IsNotFound(err):
		// A claim that is gone is lost.
	case err != nil:
		return fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	case claim.DeletionTimestamp == nil && metav1.IsControlledBy(claim, run):
		return nil
	}
	return refuse(backupv1alpha1.ItemReasonClaimLost, "claim %s was deleted (or replaced) while the mover wrote into it", run.Spec.Into)
}

// abort ends a run early as Failed. It fails every item that has not
// finished with the given message (see failRemainingItems), then calls
// finish, which records the ending, stops the run's movers and gives the
// app back.
//
// Parameters:
//   - reason is the Ready reason the run ends with: ReasonInvalid when it
//     refuses a Kustomization it would have to suspend, and ReasonFailed
//     when it hit an error it cannot get past, such as a workload it could
//     not stop or one that was deleted.
//   - message is the Ready message, and the start of each unfinished item's
//     message.
//
// It returns what finish returns: an empty result once the run has ended, or
// the wait for a stopped mover, which finish reports (rule X2).
//
// Before it fails anything, abort reads the restore Job of each unfinished
// volume item, a lost create's included (see settleJobs), so an item whose
// Job has ended records how, and an item whose Job still waits for its pod
// adds why to its message. A failed read comes back as an error for a
// retry. The items it fails record no reason; the run's ending says why.
// The Ready message also carries the note of each Cluster the run left
// deleted (see leftDeletedNotes). A run past its deadline ends through
// timeOut instead.
func (r *RestoreRunReconciler) abort(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) (ctrl.Result, error) {
	waits, err := r.settleJobs(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	failRemainingItems(run, message, "")
	addWaits(run, waits)
	return r.finish(ctx, run, reason, leftDeletedNotes(run.Status.Items, message))
}

// timeOut ends a run whose deadline has passed, with reason TimedOut.
//
// Parameters:
//   - run is the RestoreRun past its deadline, with no ending recorded yet.
//   - message is the Ready message: timedOutMessage's for an in-place run,
//     which names the SourceBusy wait the run was in, and intoTimedOut's for
//     an into restore.
//
// It returns what finish returns: an empty result once the run has ended,
// the wait for a stopped mover, or the error of a release that failed, for
// a retry. A failed read of a restore Job comes back as an error too.
//
// First the restore Job of each unfinished volume item is read, a lost
// create's included (see settleJobs), so a restore that completed just as
// the deadline passed records Succeeded. Every item still unfinished then
// fails with reason TimedOut (see failRemainingItems), and finish records
// the Ready message, with the note of each Cluster the run left deleted, in
// status.ending. Every status
// write from then on carries the ending, and a later pass, such as one
// after the wait for a stopped mover replaced the SourceBusy condition,
// ends with it as recorded.
func (r *RestoreRunReconciler) timeOut(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) (ctrl.Result, error) {
	waits, err := r.settleJobs(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	failRemainingItems(run, message, backupv1alpha1.ItemReasonTimedOut)
	addWaits(run, waits)
	return r.finish(ctx, run, backupv1alpha1.ReasonTimedOut, leftDeletedNotes(run.Status.Items, message))
}

// failRemainingItems fails every item of a run that ends early and has not
// finished, each with a message that starts with the run's.
//
// Parameters:
//   - run is the RestoreRun that ends. Its items are changed in place, and
//     the caller writes the status.
//   - message is the run's Ready message.
//   - reason is the reason each failed item records: ItemReasonTimedOut
//     from timeOut, and none from abort, whose run's ending says why.
//
// A Pending, Running or Recovering item gets the message as it is. A
// Cluster item in phase Deleted records status.items[].clusterLeftDeleted,
// and its message adds the note from clusterLeftDeleted: the run deleted
// that Cluster, and the webhook recovers its next creation without the run.
// An item in any other phase has already ended and keeps how it ended.
func failRemainingItems(run *backupv1alpha1.RestoreRun, message string, reason backupv1alpha1.ItemReason) {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		switch item.Phase {
		case backupv1alpha1.ItemDeleted:
			item.ClusterLeftDeleted = true
			failRestoreItem(item, refuse(reason, "%s", message+". "+clusterLeftDeleted(item.Name)))
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning, backupv1alpha1.ItemRecovering:
			failRestoreItem(item, refuse(reason, "%s", message))
		default:
			// An item in any other phase has already ended, and keeps its
			// phase and message.
		}
	}
}

// leftDeletedNotes returns the Ready message of a run that ends early: the
// given message, then the note from clusterLeftDeleted for each item that
// records status.items[].clusterLeftDeleted.
//
// Parameters:
//   - items are the run's items, after failRemainingItems.
//   - message is the message the run ends with.
func leftDeletedNotes(items []backupv1alpha1.RestoreItem, message string) string {
	for _, item := range items {
		if item.ClusterLeftDeleted {
			message += ". " + clusterLeftDeleted(item.Name)
		}
	}
	return message
}

// leftDeleted reports whether an item is a Cluster the run deleted and has
// not seen created again: one still in phase Deleted, or one a run that
// ended early failed and marked with status.items[].clusterLeftDeleted.
//
// Parameters:
//   - item is one of the run's items, as its status records it.
func leftDeleted(item backupv1alpha1.RestoreItem) bool {
	return item.Kind == "Cluster" && (item.Phase == backupv1alpha1.ItemDeleted || item.ClusterLeftDeleted)
}

// clusterLeftDeleted returns the note for a Cluster the run deleted and
// ended without: no run waits for the Cluster any more, so the bootstrap
// webhook recovers its next creation to the end of its archive, or to the
// time in the Cluster's own backup.wlz.li/restore-as-of annotation (see
// bootstrap.Decider.Handle, whose waitingRun passes over a finished run and
// a run being deleted).
//
// Parameters:
//   - cluster is the name of the Cluster, which the note names.
func clusterLeftDeleted(cluster string) string {
	return fmt.Sprintf("Cluster %s was deleted, and the run ended before it was created again. No run waits for it now, "+
		"so when Flux or tofu creates it, the bootstrap webhook recovers it to the end of its archive, or to the time in its own %s annotation; "+
		"the moment this run chose no longer applies", cluster, backupv1alpha1.AnnotationRestoreAsOf)
}

// quiesce stops the workloads spec.quiesce lists, before the run restores
// anything.
//
// Parameters:
//   - run is the RestoreRun, Running with its items planned and no
//     status.quiescedAt yet. quiesce records its plan and the stop in the
//     run's status.
//
// It returns a result that requeues the run: after pollInterval while it
// waits, after a second or two once the stop is recorded. A run it ends
// returns what finish or abort returns. A failed read of an entry or a
// Kustomization, a failed Lease call and a failed status write come back as
// an error, and the pass is retried with nothing stopped that the status
// does not record.
//
// Before it records a plan, with nothing stopped, quiesce fails a Pending
// volume item that restoreVolume would refuse before its restore Job exists
// (see startRefusal) or whose repository Secret is gone, with the message
// restoreVolume gives. It then waits with reason SourceBusy while a backup
// holds one of the run's claims or repositories (see backupHeldElsewhere),
// while another run is in the way (see waitingOn), and while another run
// holds the namespace's quiesce Lease (see acquireQuiesceLease). A run
// left with no Pending item, by that or because the plan Skipped every item,
// stops nothing: quiesce records status.quiescedAt and status.restartedAt at
// the same moment, and work then finishes the run. A spec.quiesce entry the
// namespace does not hold ends the run as Failed with reason Invalid before
// anything is stopped, and a Kustomization that also applies workloads of
// another namespace aborts it with reason Invalid (see planStop).
//
// quiesce then records the plan from planStop in status.quiesced and
// status.suspendedKustomizations, with each workload's replica count, and
// writes the status. Only then does applyStop suspend the Kustomizations and
// scale the workloads to zero. A pass that finds a plan in the status reuses
// it, so a retry after a lost status write still gives back the counts the
// workloads had before the run touched them. Before it stops from such a
// plan, quiesce reads the run again through the uncached Reader (see
// stopOwed), and stops nothing when the stored run has recorded the stop,
// given the app back or ended. quiesce then writes status.quiescedAt. After
// a failed stop, it narrows the plan with appliedPart to what is stopped
// now, and aborts the run with reason Failed, which starts those workloads
// again and resumes the Kustomizations, the same as a BackupRun does.
func (r *RestoreRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if len(run.Status.Quiesced) == 0 {
		targets, err := namedTargets(ctx, r.Reader, run.Namespace, run.Spec.Quiesce)
		if err != nil {
			if !isQuiesceSpecError(err) {
				return ctrl.Result{}, err
			}
			return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
		}
		// A backup of one of the run's claims or repositories that is in
		// progress would make the restore wait with the app down, from
		// restoreVolume on. The run waits here instead, with the workloads
		// still running. restoreVolume keeps its own check: this one is
		// advisory, and the check at the mover object is the one that
		// counts. An item restoreVolume would refuse before its restore
		// Job exists (see startRefusal), and one whose repository
		// Secret is gone, fail now with the message restoreVolume gives, so
		// the app is not stopped for a restore that cannot start.
		for i := range run.Status.Items {
			item := &run.Status.Items[i]
			if item.Kind != "PersistentVolumeClaim" || item.Phase != backupv1alpha1.ItemPending {
				continue
			}
			_, err := r.startRefusal(ctx, run, item.Name)
			if failRestoreItem(item, err) {
				continue
			}
			if err != nil {
				return ctrl.Result{}, err
			}
			held, err := r.backupHeldElsewhere(ctx, run, item.Name)
			if failRestoreItem(item, nothingWrittenTo(item.Name, err)) {
				continue
			}
			if err != nil {
				return ctrl.Result{}, err
			}
			if held != "" {
				return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, held))
			}
		}
		// With no item left to restore once the checks above failed the
		// rest, or every item Skipped at the plan, there is nothing to stop.
		// The run records the stop and the restart at the same moment, and
		// work then finishes it.
		if !anyRestorePending(run.Status.Items) {
			now := metav1.NewTime(r.Now())
			run.Status.QuiescedAt, run.Status.RestartedAt = &now, &now
			return after(time.Second, r.writeStatus(ctx, run))
		}
		// A restore that waits for the Cluster it deleted keeps this run
		// waiting with nothing stopped and no Lease held (see waitingOn).
		waiting, err := waitingOn(ctx, r.Reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if waiting != "" {
			return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, waiting))
		}
		// The namespace's quiesce Lease lets one run at a time stop its
		// workloads. It is taken before the plan and held until the stored
		// status shows the workloads back, so a second run waits here with
		// the app running rather than recording the count the first stopped
		// it at.
		busy, err := acquireQuiesceLease(ctx, r.Client, r.Reader, run, "RestoreRun")
		if err != nil {
			return ctrl.Result{}, err
		}
		if busy != "" {
			return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, busy))
		}

		// A Kustomization that also applies workloads of another namespace
		// is refused before anything is stopped (see planStop).
		stop, suspend, err := planStop(ctx, r.Reader, r.RESTMapper(), run.Namespace, targets)
		if asRunRefusal(err) {
			return r.abort(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		run.Status.Quiesced, run.Status.SuspendedKustomizations = stop, suspend
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// The plan came from the run as the pass read it, and that copy may
		// lag behind a pass that has since stopped the app, given it back
		// and ended the run. The stored run decides (see stopOwed); a run
		// that no longer owes the stop changes nothing and looks again.
		owed, err := stopOwed(ctx, r.Reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !owed {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}
	stopErr := applyStop(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	if stopErr != nil {
		run.Status.Quiesced, run.Status.SuspendedKustomizations = appliedPart(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	}
	now := metav1.NewTime(r.Now())
	run.Status.QuiescedAt = &now
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if stopErr != nil {
		return r.abort(ctx, run, backupv1alpha1.ReasonFailed, stopErr.Error())
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// backupHeldElsewhere returns a message naming the run that holds the claim
// or its repository, or "" when neither is held. A restore calls it before it
// stops any workload, so that it waits with the app running where
// restoreVolume would wait with the app down.
//
// Parameters:
//   - run is the asking run; its namespace and UID are read. spec.repository
//     and spec.moverSecurityContext are used the way repositoryFor uses them.
//   - claimName names the claim the item restores.
//
// A refusal from repositoryFor, for a claim or a VolumeRestore that is gone,
// gives "": quiesce has failed such an item with startRefusal just before,
// and restoreVolume fails an item that became one since. A repository
// Secret that does not exist comes back as the refusal leaseNamesFor gives,
// and quiesce fails the item with it (see failRestoreItem) before anything is
// stopped. Any other failed read comes back as an error, and the pass
// retries with nothing stopped.
//
// The check is advisory. A run that starts its mover between this read and
// the stop still goes first under the Leases and otherMover, which run right
// before the mover object is written.
func (r *RestoreRunReconciler) backupHeldElsewhere(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string) (string, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		if _, refused := asItemFailure(err); refused {
			return "", nil
		}
		return "", err
	}
	backing, err := otherMover(ctx, r.Reader, run.Namespace, claimName, settings.Secret, backupMover)
	if err != nil {
		return "", err
	}
	if backing != "" {
		return backing, nil
	}
	return leaseHeldElsewhere(ctx, r.Reader, run, run.Namespace, claimName, settings.Secret)
}

// restart gives the stopped workloads their replicas back, resumes the
// Kustomizations the run suspended, and records status.restartedAt. The
// caller writes the status.
//
// Parameters:
//   - run is the RestoreRun whose status shows it holds the app stopped (see
//     stopped).
//
// The run's copy may come from an informer cache that lags behind the run's
// own last writes, so restart first reads the stored run again (see
// readStop). When the stored run shows the restart done, restart starts
// nothing and returns nil: another run may have stopped the app since. A
// failed read, and a failed restart, come back as the error.
func (r *RestoreRunReconciler) restart(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if _, err := readStop(ctx, r.Reader, run); err != nil {
		return err
	}
	if !stopped(run) {
		return nil
	}
	if err := restartWorkloads(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
		return err
	}
	now := metav1.NewTime(r.Now())
	run.Status.RestartedAt = &now
	return nil
}

// stopped reports whether the run has stopped workloads and not yet started
// them again. A run that recorded its plan to stop them counts as having
// stopped them, even before status.quiescedAt is set, because the pass that
// wrote the plan may have stopped them and then lost its status write.
func stopped(run *backupv1alpha1.RestoreRun) bool {
	return (run.Status.QuiescedAt != nil || len(run.Status.Quiesced) > 0) && run.Status.RestartedAt == nil
}

// anyRestorePending reports whether any item is still Pending, which means
// the run has not started it yet.
func anyRestorePending(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending {
			return true
		}
	}
	return false
}

// finish ends the run and gives back what it holds.
//
// Parameters:
//   - run is the RestoreRun to end. Its status is written with the end.
//   - reason is the Ready reason the run ends with. ReasonSucceeded ends it
//     Succeeded, and any other reason ends it Failed.
//   - message is the Ready message the run ends with.
//
// A run that already recorded status.ending ends with that one, and reason
// and message are not used.
//
// It returns an empty result once the run has ended. While a stopped mover
// is not gone yet, it returns the wait from waitForStopped, and the run stays
// unfinished. A step that fails is reported through releaseFailed, whose
// error it returns for a retry; a failed status write comes back as it is.
//
// finish first records reason and message in status.ending and writes the
// status, with the items the caller failed, before it stops anything. Every
// status write from then on carries the ending, and the next pass finds it
// and calls finish with it (see Reconcile), so the run ends as it decided
// to, whatever it waited for when it decided. A stop that completes in the
// same pass therefore cannot lose the items' ends with a lost final write:
// the next pass would otherwise find an item Running on a Job that is gone
// and fail it as a Job deleted before it finished.
//
// The steps run in this order, and each one runs only once the one before it
// went through:
//
//  1. It stops the run's movers (see stopJobs): it suspends each restore
//     Job and waits until no pod of it can still write (rule X2), so no
//     mover writes into a claim or the repository once the app is back or
//     another run takes over.
//  2. It gives back the workloads the run stopped and resumes the
//     Kustomizations it suspended, and records status.restartedAt.
//  3. It releases the run's claim and repository Leases (see releaseLeases).
//  4. It writes the end: the phase, the Ready condition and
//     status.completedAt.
//  5. It releases the namespace's quiesce Lease, which the stored status now
//     shows is no longer needed.
//  6. It removes the run's finalizer.
//
// A run never reaches finish with a Cluster item still in phase Deleted:
// such an item is not finished, so work ends the run only through abort or
// timeOut, which fail the item with the note from clusterLeftDeleted first
// (see failRemainingItems).
func (r *RestoreRunReconciler) finish(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) (ctrl.Result, error) {
	ending := backupv1alpha1.RunEnding{Reason: reason, Message: message}
	if run.Status.Ending != nil {
		ending = *run.Status.Ending
	} else {
		run.Status.Ending = ending.DeepCopy()
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	left, err := r.stopJobs(ctx, run, anyItem)
	if err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, err)
	}
	if len(left) > 0 {
		return r.waitForStopped(ctx, run, left.message())
	}
	// The app is given back before the Leases go: a run that could not start
	// the workloads keeps the claim and the repository to itself until it
	// can, so no other run's mover starts on them meanwhile.
	if stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return ctrl.Result{}, r.releaseFailed(ctx, run, err)
		}
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, leaseReleaseError(run, err))
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	if ending.Reason != backupv1alpha1.ReasonSucceeded {
		run.Status.Phase = backupv1alpha1.RunPhaseFailed
	}
	run.Status.CompletedAt = &now
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, ending.Reason, ending.Message)
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	// The stored status now shows the workloads back, so the quiesce Leases
	// may go, before the finalizer: another run then takes the namespace over
	// at once. Best effort, as in work.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the run goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return ctrl.Result{}, dropFinalizer(ctx, r.Client, run)
}

// finalize gives back what a run holds when the run is deleted, and then
// removes its finalizer so the deletion can complete.
//
// Parameters:
//   - run is the RestoreRun being deleted. It may be finished or unfinished.
//
// It returns an empty result once the finalizer is gone, or when the run
// holds none. While a stopped mover is not gone yet, it returns the wait
// from waitForStopped. A step that fails is reported through releaseFailed,
// whose error it returns for a retry.
//
// The steps run in this order, and each one runs only once the one before it
// went through: it stops the run's movers and waits until none can still
// write (rule X2, see stopJobs), gives the stopped workloads back and
// resumes the suspended Kustomizations, releases the claim and repository
// Leases, drops the finalizer, and last releases the namespace's quiesce
// Lease. Without
// finalize, a run deleted while its mover writes would leave a restore
// running against a claim with nothing tracking it. The run keeps its
// finalizer and its Leases while it waits or retries: a Lease released
// before the finalizer is dropped would let another run take the claim over
// while this run repeats its restart.
//
// Right before it drops the finalizer, finalize records a Warning event with
// reason ClusterLeftDeleted, with the note from clusterLeftDeleted, for
// each Cluster item still in phase Deleted and each one that records
// status.items[].clusterLeftDeleted, which a run that timed out or aborted
// set when it failed the item (see leftDeleted). The run is about to go,
// so the event is the only place that note can appear. It reads the
// Cluster first: one its owner has created again, with a UID other than the
// item's clusterUID, gets no event, since the note speaks of a creation
// still to come. A failed read comes back as an error, and the finalizer
// stays until a retry gets past it.
func (r *RestoreRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return ctrl.Result{}, nil
	}
	left, err := r.stopJobs(ctx, run, anyItem)
	if err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, err)
	}
	if len(left) > 0 {
		return r.waitForStopped(ctx, run, left.message())
	}
	// The app is given back before the Leases go, as in finish.
	if stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return ctrl.Result{}, r.releaseFailed(ctx, run, err)
		}
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, leaseReleaseError(run, err))
	}
	if r.Recorder != nil {
		for _, item := range run.Status.Items {
			if !leftDeleted(item) {
				continue
			}
			// A Cluster its owner has created again is back, and the note
			// about its next creation would be wrong.
			cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !found || cluster.GetUID() == item.ClusterUID {
				r.Recorder.Eventf(run, nil, corev1.EventTypeWarning, "ClusterLeftDeleted", "Restore", "%s", fitNote(clusterLeftDeleted(item.Name)))
			}
		}
	}
	if err := dropFinalizer(ctx, r.Client, run); err != nil {
		return ctrl.Result{}, err
	}
	// The quiesce Lease goes after dropFinalizer. finalize does not store the
	// restart in the run's status, so when the drop fails, the next pass
	// restarts the app again. Released before that, the Lease would let
	// another run stop the app in between, and the repeated restart would
	// undo that stop. The release is best effort: a Lease left behind is
	// stale under holderLive's rule.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the deletion goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return ctrl.Result{}, nil
}

// waitForStopped reports that a run waits for something it stopped to go
// before it gives the app back, releases its Leases, or finishes (rule X2
// and designs/restorerun.md D1).
//
// Parameters:
//   - run is the RestoreRun that waits, with the status this pass computed.
//     Its status is written when it differs from the stored one.
//   - message is the Ready message that says what the run waits for: a
//     mover it stopped (see jobList.message).
//
// It returns a result that looks again after pollInterval, and the error of
// the status read or write, if any.
//
// It moves an unfinished run to Waiting and sets the Ready condition to
// False with reason WaitingForShutdown and the message. A finished run that
// finalize holds keeps the phase it finished with, which is the record of how
// its restore went. The status is written only when the whole status
// differs from the stored one (see writeChangedStatus), since every write
// starts another reconcile. So an ending that finish recorded, or items
// that work changed, reach the API server in the pass that changed them,
// even when the run waits for the same mover as before. The run keeps its
// finalizer and its Leases meanwhile, so nothing takes the claim or the
// repository over while the mover may still write.
func (r *RestoreRunReconciler) waitForStopped(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) (ctrl.Result, error) {
	if !run.Status.Phase.Finished() {
		run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonShutdown, message)
	return after(pollInterval, r.writeChangedStatus(ctx, run))
}

// overdue returns the run's deadline, status.startedAt plus spec.timeout, and
// reports whether the run has worked past it. A run that has not started, or
// has no timeout, is never overdue.
func (r *RestoreRunReconciler) overdue(run *backupv1alpha1.RestoreRun) (time.Time, bool) {
	if run.Status.StartedAt == nil || run.Spec.Timeout == nil {
		return time.Time{}, false
	}
	deadline := run.Status.StartedAt.Add(run.Spec.Timeout.Duration)
	return deadline, !r.Now().Before(deadline)
}

// waitFor moves the run to Waiting, sets its Ready condition to False with
// reason and message, and writes the status.
func (r *RestoreRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

// waitForBackup keeps an into restore from creating anything while a backup
// of its claim or its repository is in progress.
//
// Parameters:
//   - claim is the name of the claim the run takes a Lease on: the source
//     claim, or the new claim for a restore from spec.repository alone.
//   - secret is the name of the repository Secret.
//
// The run calls it right before it creates its first object. It first takes
// the Leases of the claim and the repository for the run's item (see
// acquireLeases), then looks for a backup's mover object (see otherMover),
// which catches a backup started before the controller took Leases.
//
// It returns true when the run has to wait. It has then moved the run to
// Waiting with reason SourceBusy and a message naming the run that holds a
// Lease or the BackupRun. A repository Secret that does not exist comes back
// as the refusal acquireLeases gives, and the caller ends the run. A failed
// read, write or status write comes back as an error, which the caller
// retries.
func (r *RestoreRunReconciler) waitForBackup(ctx context.Context, run *backupv1alpha1.RestoreRun, claim, secret string) (bool, error) {
	busy, err := acquireLeases(ctx, r.Client, r.Reader, leaseHolder{kind: "RestoreRun", run: run, item: run.Status.Items[0].Name},
		run.Namespace, claim, secret)
	if err != nil {
		return false, err
	}
	if busy != "" {
		return true, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, busy)
	}
	backing, err := otherMover(ctx, r.Reader, run.Namespace, claim, secret, backupMover)
	if err != nil || backing == "" {
		return false, err
	}
	return true, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, backing)
}

// writeStatus writes the run's status subresource.
func (r *RestoreRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set RestoreRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// claimHolder returns the name of a pod that mounts a claim.
//
// Parameters:
//   - c lists the pods. The run passes its uncached Reader: without
//     spec.quiesce this check is the only thing that keeps a second writer
//     off the volume, and an informer cache that has not yet seen a new pod
//     would report the claim free.
//   - namespace is the run's namespace, which holds the claim and its pods.
//   - claim is the name of the claim an in-place restore is about to write
//     into.
//
// It returns the name of the first pod in the namespace, in the order the
// list gives, whose volumes name the claim, and "" when none does. A pod in
// phase Succeeded or Failed has no container left and doesn't count. A
// failed list comes back as an error.
func claimHolder(ctx context.Context, c client.Reader, namespace, claim string) (string, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
				return pod.Name, nil
			}
		}
	}
	return "", nil
}

// restoreDone reports whether every item is Succeeded, Failed or Skipped, so
// none has anything more to do.
func restoreDone(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if !finished(item) {
			return false
		}
	}
	return true
}

// restoreFailures returns one line per failed item, naming its kind, its name
// and its message, joined with "; ". It returns an empty string when no item
// failed.
func restoreFailures(items []backupv1alpha1.RestoreItem) string {
	var failed []string
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			failed = append(failed, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
		}
	}
	return strings.Join(failed, "; ")
}

// nothingRestored returns the Ready message of a run none of whose items
// succeeded, and "" when at least one did.
//
// Parameters:
//   - items are the run's finished items. The caller has ruled out a failed
//     one, so an item that did not succeed was Skipped.
//
// The message says nothing was restored and gives each item's kind, name and
// message, joined with "; ", so the user sees why each one was left alone.
// A run that restored nothing must not end Succeeded.
func nothingRestored(items []backupv1alpha1.RestoreItem) string {
	var skipped []string
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemSucceeded {
			return ""
		}
		skipped = append(skipped, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
	}
	return "nothing was restored: " + strings.Join(skipped, "; ")
}

// leftAlone says why a run must not restore a Cluster, and returns an empty
// string when it may. The run never deletes a Cluster whose next creation it
// can't turn into its recovery:
//
//   - A Cluster carrying backup.wlz.li/bootstrap: initdb opts out of the
//     bootstrap webhook. The webhook lets it through empty and never marks it
//     as a run's recovery, so a run that deleted it would see it come back
//     empty and delete it again until the timeout.
//   - A Cluster whose owner declares a bootstrap method, such as
//     pg_basebackup or a recovery of their own (see bootstrap.OwnerBootstrap),
//     comes back with that method. The webhook refuses it while the run waits,
//     so the database would stay down until the timeout and nothing would be
//     restored.
//
// The run asks this of a Cluster it has not deleted: at its checks, at a
// Pending item, and of the old Cluster (the recorded UID) that a Deleted item
// finds still there. A Cluster created again after the delete is never
// Skipped, since it came back without the restore; restoreDatabase fails
// that item (see notRecovered). The returned text is the item's message.
func leftAlone(cluster *unstructured.Unstructured) string {
	if cluster.GetAnnotations()[bootstrap.OptOutAnnotation] == bootstrap.OptOutValue {
		return fmt.Sprintf("the Cluster carries %s: %s, which asks for an empty database, so the run leaves it alone",
			bootstrap.OptOutAnnotation, bootstrap.OptOutValue)
	}
	if method := bootstrap.OwnerBootstrap(cluster); method != "" {
		return fmt.Sprintf("the Cluster declares its own spec.bootstrap.%s, so the run leaves it alone", method)
	}
	return ""
}

// anyItem chooses every item, for stopJobs from finish and finalize.
func anyItem(backupv1alpha1.RestoreItem) bool { return true }

// finished reports whether an item is Succeeded, Failed or Skipped, so its
// restore Job has no more work to do.
func finished(item backupv1alpha1.RestoreItem) bool {
	switch item.Phase {
	case backupv1alpha1.ItemSucceeded, backupv1alpha1.ItemFailed, backupv1alpha1.ItemSkipped:
		return true
	case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
		return false
	}
	return false
}

// restoreItemDone reports whether the run's claim item named name has
// finished (see finished) and names no restore Job, or the run has no such
// item. An item keeps its Job's UID until Stop reports the Job stopped (see
// stopJobs), so until then the Job may still write, and the item keeps its
// Leases. Only a claim item counts, as in holderLive: a Cluster item takes
// no Lease, and one of the same name says nothing about the claim.
func restoreItemDone(run *backupv1alpha1.RestoreRun, name string) bool {
	for _, item := range run.Status.Items {
		if item.Kind == backupv1alpha1.ItemKindClaim && item.Name == name {
			return finished(item) && item.JobUID == ""
		}
	}
	return true
}

// finishedWithMover reports whether any finished item still records the UID
// of its restore Job, so that Job is not stopped yet.
func finishedWithMover(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if finished(item) && item.JobUID != "" {
			return true
		}
	}
	return false
}

// releaseFailed reports on the run that it could not stop one of its movers,
// give the app back or release what it holds, and hands the error back.
//
// Parameters:
//   - run is the RestoreRun that failed. Its Ready condition is set and its
//     status written.
//   - err is the error from finish or finalize: the *releaseError of a mover
//     the run could not stop or of a Lease it could not read or release, or
//     the *restartError of a restart that failed.
//
// It returns err, so the reconcile runs again with controller-runtime's
// backoff.
//
// Only finish and finalize call it, once the run has stopped restoring, so
// the plan it hands releaseFailure says the run is not working, and the
// advice may say the run can be deleted.
//
// The Ready condition takes the reason and message from releaseFailure. The
// reason is RestartFailed while the run still holds workloads stopped (see
// stopped), whatever step failed, and ReleaseFailed when a release step
// failed once the app is back. Announce turns either reason into a Warning
// event. The status write is best effort: a write that fails is made again
// by the next pass that fails. The run never gives up. A run that finished
// while it still held a claim, a repository or the app would lose the only
// record of what it has to put back.
func (r *RestoreRunReconciler) releaseFailed(ctx context.Context, run *backupv1alpha1.RestoreRun, err error) error {
	reason, message := releaseFailure(err, releasePlan{
		stopped: run.Status.Quiesced, suspended: run.Status.SuspendedKustomizations,
		kind: "RestoreRun", deleting: !run.DeletionTimestamp.IsZero(), appDown: stopped(run),
	})
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	_ = r.writeStatus(ctx, run)
	return err
}
