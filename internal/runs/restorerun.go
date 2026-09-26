package runs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/populator"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// RestoreRunReconciler runs RestoreRuns. A RestoreRun puts volumes and
// databases back to the state they were in at a chosen moment.
//
// A volume restores in place. Once no pod mounts the claim, the run creates a
// ReplicationDestination in Direct mode, whose mover mounts the claim and
// writes the selected snapshot into it. A database restores by being created
// again. The run deletes the Cluster, and when Flux or tofu creates it again,
// the bootstrap webhook makes the new Cluster recover to the run's moment.
// With spec.into set, a volume restores into a new claim through the ordinary
// VolumeRestore populator path, and the app's own claims and databases are
// left alone. A restore from spec.repository alone has no source claim to
// take a node from, so its mover writes into the new claim directly.
type RestoreRunReconciler struct {
	client.Client

	// Reader reads straight from the API server, without the informer cache.
	// The run reads claims, VolumeRestores, Secrets, ObjectStores, Clusters,
	// ReplicationDestinations, Deployments, StatefulSets and pods through it.
	Reader client.Reader

	// Snapshots lists the snapshots in a restic repository. plan uses it to
	// check that each volume has a snapshot the run's moment reaches.
	Snapshots restic.Lister

	// Prober lists a database's base backups in its object store. plan uses it
	// to check that each database has a base backup the run's moment reaches.
	Prober bootstrap.Prober

	// Recorder writes an event on the run each time its Ready reason changes.
	Recorder events.EventRecorder

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
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.RestoreRun{}).
		Named("restorerun").
		Complete(r)
}

// Reconcile moves the RestoreRun that req names one step further, and requeues
// until the run has finished.
//
// A new run starts in plan, which checks that every item has a backup in reach
// before anything is changed, or in planIntoNewClaim when spec.into is set. An
// error either returns for a retry goes through planFailed, which reports it
// on the Ready condition and ends the run once spec.timeout has passed since
// its creation. A run past its checks continues in work, or in
// reconcileIntoNewClaim for an into restore. Before any of these, Reconcile
// adds the run's finalizer. A run being deleted gets its changes put back by
// finalize, and a finished run is deleted once spec.ttlSecondsAfterFinished
// has passed.
//
// A new run is first checked against the installed RestoreRun CRD (see
// schemaCache.crdOutdated), and a run whose CRD lacks a field the controller
// writes ends with reason CRDOutdated before anything is planned.
// Whenever the Ready reason changes during a reconcile, Reconcile records an
// event on the run.
func (r *RestoreRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.RestoreRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Restore") }()
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, run)
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

	if run.Status.Phase == "" {
		// The check comes first for both plans: the old RestoreRun CRD drops
		// the items' clusterUID and snapshotTime, which the restore relies on.
		message, err := r.schemas.crdOutdated(ctx, r.Reader, restoreRunsCRD, "RestoreRun", backupv1alpha1.RestoreRun{},
			"the run would lose the Cluster UIDs and snapshot times it records to check its own work")
		if err != nil {
			return ctrl.Result{}, r.planFailed(ctx, run, err)
		}
		if message != "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonCRDOutdated, message)
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
	if run.Spec.Into != "" {
		return r.reconcileIntoNewClaim(ctx, run)
	}
	return r.work(ctx, run)
}

// planFailed handles an error that plan or planIntoNewClaim returned for a
// retry, and returns the error the reconcile hands back.
//
// A run whose checks keep failing never records status.startedAt, so overdue
// never fires for it. planFailed therefore counts spec.timeout from the run's
// creation. Once that has passed, it ends the run as Failed with reason
// TimedOut and the error in the message. Until then it sets Ready to False with
// reason Retrying and the error as the message, so `kubectl get` shows why the
// run has not started, and returns the error for a retry. It writes the
// condition only when it changed, because every write starts another
// reconcile.
//
// An error from a pass that had already given the run a phase, such as a lost
// status write, is returned unchanged.
func (r *RestoreRunReconciler) planFailed(ctx context.Context, run *backupv1alpha1.RestoreRun, err error) error {
	if run.Status.Phase != "" {
		return err
	}
	if run.Spec.Timeout != nil && !run.CreationTimestamp.IsZero() {
		deadline := run.CreationTimestamp.Add(run.Spec.Timeout.Duration)
		if !r.Now().Before(deadline) {
			return r.finish(ctx, run, backupv1alpha1.ReasonTimedOut,
				fmt.Sprintf("the run had not passed its checks by %s: %v", deadline.UTC().Format(time.RFC3339), err))
		}
	}
	before := meta.FindStatusCondition(run.Status.Conditions, backupv1alpha1.ConditionReady)
	unchanged := before != nil && before.Status == metav1.ConditionFalse &&
		before.Reason == backupv1alpha1.ReasonRetrying && before.Message == err.Error()
	if unchanged {
		return err
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRetrying, err.Error())
	if werr := r.writeStatus(ctx, run); werr != nil {
		return errors.Join(err, werr)
	}
	return err
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

// plan lists the run's items and checks, before it changes anything, that a
// backup reaches the run's moment for every one of them. When every item
// passes, plan moves the run to Running and records status.startedAt.
//
// For a volume, the check selects the snapshot the restore would use and
// records its ID on the item. For a database, it selects the base backup the
// recovery would start from and records its ID. An item with nothing in reach
// is marked Failed with the reason. If any item fails, plan marks every other
// Pending item Skipped and ends the run as Failed with reason NoBackupInReach,
// naming each item it cannot reach. Nothing has been deleted or overwritten
// at that point.
//
// With spec.syncDatabaseToVolume set, each volume may only select a snapshot
// tagged quiesced, and all the selected snapshots must carry the same time.
// plan records that time in status.syncedTo and checks each database against
// it in place of spec.restoreAsOf.
//
// A spec that can't work ends the run as Failed with reason Invalid: a
// restoreAsOf that doesn't parse, a spec.quiesce entry the namespace does not
// hold, or a synced run with no claim to take the moment from. plan returns
// an error, and writes nothing, when listing the snapshots or the base
// backups fails, and when a read fails in a way a retry may fix, such as a
// timeout from the API server. Reconcile hands such an error to planFailed.
func (r *RestoreRunReconciler) plan(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	items, err := r.items(ctx, run)
	if err != nil {
		if !isRefusal(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	at, err := target(run)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if _, err := namedTargets(ctx, r.Reader, run.Namespace, run.Spec.Quiesce); err != nil {
		if !isQuiesceSpecError(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
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
		var reason string
		switch item.Kind {
		case "PersistentVolumeClaim":
			var snapshot restic.Snapshot
			snapshot, reason, err = r.checkVolume(ctx, run, item.Name, at, sync)
			item.Snapshot = snapshot.ShortID()
			if !snapshot.Time.IsZero() {
				item.SnapshotTime = &metav1.Time{Time: snapshot.Time}
			}
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
		if err != nil {
			return ctrl.Result{}, err
		}
		if reason != "" {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, reason
			unreachable = append(unreachable, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, reason))
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
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, strings.Join(unreachable, "; "))
	}
	if sync {
		if synced == nil {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid,
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

// items returns one Pending item for each thing the run's spec names.
// spec.claim names one claim and spec.database names one Cluster. A run with
// neither takes every claim and every Cluster in the namespace marked
// backup.wlz.li/enabled: "true", claims first. A Cluster that archives nowhere
// has no backup to restore, so its item starts out Skipped. So does a Cluster
// that opts out of the bootstrap webhook, or whose owner declares its own
// bootstrap method (see leftAlone).
//
// items returns a refusal when spec.repository is set and spec.claim is not,
// because a restore in place needs a claim to write into; the refusal sends
// the user to spec.into with a name no claim has, or to spec.claim to
// overwrite an existing claim in place. It also returns a refusal when
// nothing in the namespace is marked, and when spec.database names a Cluster
// that opts out of the bootstrap webhook. A failed read comes back as a plain
// error.
func (r *RestoreRunReconciler) items(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	pending := func(kind, name string) backupv1alpha1.RestoreItem {
		return backupv1alpha1.RestoreItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}
	switch {
	case run.Spec.Claim != "":
		return []backupv1alpha1.RestoreItem{pending("PersistentVolumeClaim", run.Spec.Claim)}, nil
	case run.Spec.Repository != "":
		return nil, refuse("spec.repository alone restores into a new claim, so spec.into is required, and it must name a claim that does not exist yet. " +
			"To overwrite an existing claim from this repository, set spec.claim to it as well; the run then restores it in place once no pod mounts it.")
	case run.Spec.Database != "":
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, run.Spec.Database)
		if err != nil {
			return nil, err
		}
		if found {
			if why := leftAlone(cluster); why != "" {
				return nil, refuse("Cluster %s: %s", run.Spec.Database, why)
			}
		}
		return []backupv1alpha1.RestoreItem{pending("Cluster", run.Spec.Database)}, nil
	}

	claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
	if err != nil {
		return nil, err
	}
	clusters, err := enabledClusters(ctx, r.Reader, run.Namespace)
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
		return nil, refuse("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
	}
	return items, nil
}

// checkVolume finds the snapshot that a restore of the claim named claimName
// would select.
//
// Parameters:
//   - at is the moment to restore to, or nil for the newest snapshot.
//   - quiescedOnly limits the choice to snapshots tagged quiesced. plan sets
//     it on a run with spec.syncDatabaseToVolume.
//
// It returns the snapshot, or a reason when there is none. It also returns a
// reason when repositoryFor refuses the claim, because the claim or its
// VolumeRestore is missing. It returns an error when listing the repository
// fails, and when a read fails for any other reason.
//
// This is the only place a missing snapshot is caught. VolSync restores
// nothing and still reports success when no snapshot matches.
func (r *RestoreRunReconciler) checkVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, string, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		if !isRefusal(err) {
			return restic.Snapshot{}, "", err
		}
		return restic.Snapshot{}, err.Error(), nil
	}
	return r.selectSnapshot(ctx, run, settings.Secret, at, quiescedOnly)
}

// selectSnapshot returns the snapshot the run would restore, picked the way
// the VolSync mover picks it from restoreAsOf and previous.
//
// Parameters:
//   - secretName names the restic repository Secret in the run's namespace.
//   - at is the moment to restore to. selectSnapshot takes the newest
//     snapshot at or before it, or the newest of all when it is nil.
//   - quiescedOnly passes over every snapshot not tagged quiesced.
//
// When spec.previous is set, selectSnapshot then steps that many snapshots
// further back. It relies on the lister returning the snapshots oldest first.
//
// It returns a reason, and no error, when the Secret doesn't exist, when no
// snapshot qualifies, when none is at or before the moment, and when
// spec.previous reaches past the oldest snapshot. It returns an error when
// the Secret can't be read for another reason and when listing the repository
// fails.
func (r *RestoreRunReconciler) selectSnapshot(ctx context.Context, run *backupv1alpha1.RestoreRun, secretName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, string, error) {
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: secretName}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return restic.Snapshot{}, "", fmt.Errorf("read repository Secret %s: %w", secretName, err)
		}
		return restic.Snapshot{}, fmt.Sprintf("no repository Secret %s in this namespace", secretName), nil
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return restic.Snapshot{}, "", fmt.Errorf("list the snapshots in %s: %w", secretName, err)
	}
	if quiescedOnly {
		snapshots = slices.DeleteFunc(slices.Clone(snapshots), func(s restic.Snapshot) bool { return !slices.Contains(s.Tags, restic.QuiescedTag) })
		if len(snapshots) == 0 {
			return restic.Snapshot{}, fmt.Sprintf("the repository holds no snapshot tagged %s; only a BackupRun that stopped the workloads writes one", restic.QuiescedTag), nil
		}
	}
	if len(snapshots) == 0 {
		return restic.Snapshot{}, "the repository holds no snapshot", nil
	}

	index := len(snapshots) - 1
	if at != nil {
		found, ok := restic.AtOrBefore(snapshots, *at)
		if !ok {
			return restic.Snapshot{}, fmt.Sprintf("no snapshot at or before %s; the oldest, %s, is from %s",
				at.UTC().Format(time.RFC3339), snapshots[0].ShortID(), snapshots[0].Time.UTC().Format(time.RFC3339)), nil
		}
		for i, s := range snapshots {
			if s.ID == found.ID {
				index = i
			}
		}
	}
	if run.Spec.Previous != nil {
		index -= int(*run.Spec.Previous)
		if index < 0 {
			return restic.Snapshot{}, fmt.Sprintf("previous %d reaches past the oldest snapshot", *run.Spec.Previous), nil
		}
	}
	return snapshots[index], "", nil
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
	cluster, found, err := getCluster(ctx, r.Reader, namespace, name)
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
	location, err := bootstrap.ResolveLocation(ctx, r.Reader, namespace, store, serverName)
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

// work makes one pass over a run that plan has checked, and returns when to
// look again. It restores the volumes first, then deletes the databases and
// follows them until they are recovered.
//
// With spec.quiesce set, the first pass calls quiesce to stop the listed
// workloads, and later passes restore nothing until every pod of those
// workloads is gone. The databases wait until every volume item is done. When
// a volume restore failed, the databases still Pending are skipped and left
// running.
//
// After a Cluster is deleted, work waits with reason WaitingForShutdown until
// the old Cluster's instance pods and PVCs are gone. Only then does it start
// the stopped workloads again and resume the Kustomizations it suspended. It
// then waits with reason WaitingForRecreate, whose message asks for the
// Cluster to be created again. The run finishes once every item is
// Succeeded, Failed or Skipped. A run past spec.timeout is aborted.
func (r *RestoreRunReconciler) work(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if deadline, over := r.overdue(run); over {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonTimedOut, fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339)))
	}
	// The Leases of an item that finished in an earlier pass go now, so a
	// backup of that claim need not wait for the rest of the run.
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(name string) bool { return restoreItemDone(run, name) }); err != nil {
		return ctrl.Result{}, err
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
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
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
		}
	}

	// A finished volume item's phase goes into the status before its
	// ReplicationDestination is deleted. The destination's lastManualSync is
	// the only record that the mover completed the run's trigger. Deleted
	// first, a lost status write or a crash would leave a Running item whose
	// destination is gone, and no later pass could tell how it ended. A later
	// pass deletes the destination of any finished item that still names one,
	// and a destination that is already gone counts as deleted.
	if finishedWithDestination(run.Status.Items) {
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		if err := r.removeDestinations(ctx, run, finished); err != nil {
			return ctrl.Result{}, err
		}
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

	// A database comes back only when its owner creates it again, and a
	// Kustomization this run suspended creates nothing. So the app is given
	// back once every volume is restored and every database is deleted, down
	// to its last instance pod and PVC.
	if stopped(run) && volumesDone && !anyRestorePending(run.Status.Items) && len(shuttingDown) == 0 {
		if err := r.restart(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}

	if restoreDone(run.Status.Items) {
		if failed := restoreFailures(run.Status.Items); failed != "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, failed)
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, "every item holds the restored data")
	}

	switch {
	case len(shuttingDown) > 0:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonShutdown,
			fmt.Sprintf("waiting for %s of the deleted Cluster to be gone before anything creates it again", strings.Join(shuttingDown, ", "))))
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

// restoreVolume moves one in-place volume restore a step further.
//
// Parameters:
//   - index is the item's position in status.items. It goes into the name of
//     the item's ReplicationDestination.
//   - item is the volume item, which restoreVolume updates in place.
//
// A Pending item waits until no pod mounts the claim. restoreVolume then
// creates the item's ReplicationDestination and moves the item to Running.
// A Running item succeeds once the destination has completed the run's
// trigger, or fails with the mover's logs. It also fails when its destination
// is gone. restoreVolume leaves the destination in place, and work deletes it
// once the item's end is in the status. A claim that is gone, or whose
// VolumeRestore is missing, fails the item. So does a destination with the
// item's name that the run did not create (see ownsDestination), found at
// the create or on a later pass; the item then names no destination, so
// nothing the run does afterwards touches it.
//
// It returns reason ClaimInUse and a message naming the pod while a pod
// mounts the claim, and reason SourceBusy and a message naming the other run
// while another run holds the Lease of the claim or its repository (see
// acquireLeases) or a backup of either is in progress (see
// otherMover); the item stays Pending in both cases. It returns empty strings
// otherwise. It returns an error, and leaves the item as it was, when an API
// call fails for any other reason.
func (r *RestoreRunReconciler) restoreVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (string, string, error) {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		holder, err := claimHolder(ctx, r.Reader, run.Namespace, item.Name)
		if err != nil {
			return "", "", fmt.Errorf("look for a pod holding claim %s: %w", item.Name, err)
		}
		if holder != "" {
			return backupv1alpha1.ReasonClaimInUse,
				fmt.Sprintf("claim %s is mounted by pod %s; stop the workload and this restore starts on its own", item.Name, holder), nil
		}
		settings, err := repositoryFor(ctx, r.Reader, run.Namespace, item.Name, run.Spec.Repository, run.Spec.MoverSecurityContext)
		if err != nil {
			if !isRefusal(err) {
				return "", "", err
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return "", "", nil
		}
		// The Leases make the restore and a backup of the claim or its
		// repository exclusive: of two runs that get here in the same
		// instant, only one creates each Lease.
		busy, err := acquireLeases(ctx, r.Client, r.Reader, leaseHolder{kind: "RestoreRun", run: run, item: item.Name},
			run.Namespace, item.Name, settings.Secret)
		if err != nil {
			return "", "", err
		}
		if busy != "" {
			return backupv1alpha1.ReasonSourceBusy, busy, nil
		}
		// A backup of the claim or its repository that already has its
		// trigger on a ReplicationSource goes first, as one started before
		// the controller took Leases does. The check runs right before the
		// create, so a backup that wrote its trigger after the checks is
		// still seen.
		backing, err := otherMover(ctx, r.Reader, run.Namespace, item.Name, settings.Secret, backupMover)
		if err != nil {
			return "", "", err
		}
		if backing != "" {
			return backupv1alpha1.ReasonSourceBusy, backing, nil
		}
		name := destinationName(run.UID, index)
		refusal, err := r.createDestination(ctx, run, directDestination(run, *item, settings, name))
		if err != nil {
			return "", "", err
		}
		if refusal != "" {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, refusal
			return "", "", nil
		}
		item.Phase, item.Destination = backupv1alpha1.ItemRunning, name

	case backupv1alpha1.ItemRunning:
		destination := &volsyncv1alpha1.ReplicationDestination{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Destination}, destination); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", "", fmt.Errorf("get ReplicationDestination %s: %w", item.Destination, err)
			}
			// The run records an item's end before it deletes the
			// destination, so something else deleted this one.
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the ReplicationDestination %s was deleted before its mover finished", item.Destination)
			return "", "", nil
		}
		if !ownsDestination(run, destination) {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, replacedDestination(item.Destination)
			item.Destination = ""
			return "", "", nil
		}
		if reason, failed := failedMover(destination); failed {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, reason
		} else if destination.Status != nil && destination.Status.LastManualSync == string(run.UID) {
			item.Phase = backupv1alpha1.ItemSucceeded
		}
	}
	return "", "", nil
}

// restoreDatabase moves one database restore a step further. A Pending item
// has its Cluster deleted. A Deleted item waits until Flux or tofu creates the
// Cluster again and the bootstrap webhook marks it as this run's recovery,
// which moves the item to Recovering. A Recovering item succeeds once the
// Cluster is healthy, and fails if the recovered Cluster is deleted.
//
// A Cluster that opts out of the bootstrap webhook, or whose owner declares
// its own bootstrap method (see leftAlone), moves the item to Skipped, and the
// run never deletes it. That holds for the old Cluster before the delete
// reaches it and for a Cluster created again.
//
// The item is marked Deleted, and the status written, before the Cluster is
// deleted. The bootstrap webhook recovers a Cluster only for a run whose item
// says Deleted, so the mark has to be in place before anything can create the
// Cluster again. The same write records the old Cluster's UID in
// status.items[].clusterUID.
//
// A Deleted item tells the old Cluster from a new one by that UID. The
// webhook's backup.wlz.li/restore-run annotation stays on a recovered Cluster
// for good, so a run created again under the same name finds it on the old
// Cluster too. When the delete failed, or the controller stopped after the
// mark was written, the Cluster still there carries the recorded UID, and the
// run deletes it again. Only a Cluster with another UID and the annotation
// naming this run counts as the recovery.
func (r *RestoreRunReconciler) restoreDatabase(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) error {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		if err != nil {
			return err
		}
		if found {
			if why := leftAlone(cluster); why != "" {
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, why
				return nil
			}
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
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		switch {
		case err != nil:
			return err
		case !found || cluster.GetDeletionTimestamp() != nil:
			return nil
		case leftAlone(cluster) != "":
			// Created again by the owner's choice, empty or with its own
			// bootstrap, or marked that way before the delete reached it.
			// Deleting it again would only loop.
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, leftAlone(cluster)
		case item.ClusterUID != "" && cluster.GetUID() == item.ClusterUID:
			// The old Cluster, which the earlier delete did not reach. Its
			// annotations say nothing about this run.
			return r.deleteCluster(ctx, cluster)
		case cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun] == run.Name:
			item.Phase = backupv1alpha1.ItemRecovering
		default:
			// The webhook writes backup.wlz.li/restore-run on every Cluster it
			// recovers for a run whose item says Deleted. A live Cluster
			// without it was not created through this run.
			return r.deleteCluster(ctx, cluster)
		}

	case backupv1alpha1.ItemRecovering:
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		switch {
		case err != nil:
			return err
		case !found:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, "the recovered Cluster was deleted"
		case clusterPhase(cluster) == healthyPhase:
			item.Phase = backupv1alpha1.ItemSucceeded
		}
	}
	return nil
}

// deleteCluster deletes the Cluster the run read. The delete carries the
// Cluster's UID as a precondition, so it never reaches a Cluster of the same
// name created since the read. A Cluster that is already gone is not an
// error.
func (r *RestoreRunReconciler) deleteCluster(ctx context.Context, cluster client.Object) error {
	uid := cluster.GetUID()
	if err := r.Delete(ctx, cluster, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Cluster %s/%s: %w", cluster.GetNamespace(), cluster.GetName(), err)
	}
	return nil
}

// planIntoNewClaim checks an into restore before it creates anything. It
// selects the snapshot the restore would use, records it on the run's single
// item, and moves the run to Running with status.startedAt set. The check has
// to come first, because the populator would otherwise bind an empty claim and
// report success. For a restore from spec.repository alone, the item also
// names the ReplicationDestination that restoreIntoEmptyClaim creates.
//
// A spec that can't work ends the run as Failed with reason Invalid: a claim
// named spec.into that exists and that the run did not create (for a restore
// through the populator, a VolumeRestore of that name as well; see
// notCreatedByRun), a source claim or VolumeRestore that is missing, a
// restoreAsOf that doesn't parse, or a restore from spec.repository alone
// without spec.intoSize. The run writes only into a claim it creates itself,
// so it never overwrites or takes over one it finds. A run with no
// snapshot in reach ends with reason NoBackupInReach. Any other failed read,
// and a failed listing of the repository, is returned for a retry.
func (r *RestoreRunReconciler) planIntoNewClaim(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	run.Status.Target = run.Spec.Into
	refusal, err := r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, "claim")
	if err == nil && refusal == "" && run.Spec.Claim != "" {
		refusal, err = r.intoTaken(ctx, run, &backupv1alpha1.VolumeRestore{}, "VolumeRestore")
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, refusal)
	}
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		if !isRefusal(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if settings.Capacity == nil && run.Spec.IntoSize == nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid,
			"spec.intoSize is required when spec.repository names the source, because there is no source claim to copy a size from")
	}
	at, err := target(run)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	snapshot, reason, err := r.selectSnapshot(ctx, run, settings.Secret, at, false)
	if err != nil {
		return ctrl.Result{}, err
	}
	if reason != "" {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, reason)
	}
	item := backupv1alpha1.RestoreItem{
		Kind: "PersistentVolumeClaim", Name: run.Spec.Into, Phase: backupv1alpha1.ItemRunning,
		Snapshot: snapshot.ShortID(), SnapshotTime: &metav1.Time{Time: snapshot.Time},
	}
	if run.Spec.Claim == "" {
		item.Destination = destinationName(run.UID, 0)
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = &now
	run.Status.Items = []backupv1alpha1.RestoreItem{item}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("restoring into claim %s", run.Spec.Into))
	return after(time.Second, r.writeStatus(ctx, run))
}

// reconcileIntoNewClaim restores a volume into the new claim that spec.into
// names, and leaves the app's claim alone. planIntoNewClaim has checked the
// run before this runs. A restore from spec.repository alone goes to
// restoreIntoEmptyClaim.
//
// With a source claim, it creates a VolumeRestore carrying the time of the
// snapshot the checks selected (see pointInTimeRestore) and a claim whose data
// source names it, so the ordinary populator path does the work. The claim
// takes the source claim's node (see scratchClaim). The run succeeds once the
// new claim is Bound, and is aborted with reason TimedOut when the claim has
// not bound by spec.timeout. The deadline is checked before anything is
// created, so a create the API server keeps refusing can't hold the run past
// it. A source claim deleted before the new claim was created ends the run as
// Failed. While a backup of the source claim or its repository is in progress
// (see otherMover), the run waits with reason SourceBusy before it creates
// anything.
//
// The run writes only through a VolumeRestore and into a claim it created:
// a Bound claim counts as restored only when the run controls it. A claim
// or VolumeRestore named spec.into that the run did not create (see
// notCreatedByRun), found at the create or on any later pass, aborts the run
// with reason Failed. Such an object was created after the checks, which
// refuse one that already existed.
func (r *RestoreRunReconciler) reconcileIntoNewClaim(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if run.Spec.Claim == "" {
		return r.restoreIntoEmptyClaim(ctx, run)
	}
	bound := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	readErr := r.Reader.Get(ctx, key, bound)
	if readErr == nil {
		if refusal := notCreatedByRun(run, "claim", bound); refusal != "" {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, refusal)
		}
	}
	if readErr == nil && bound.Status.Phase == corev1.ClaimBound {
		for i := range run.Status.Items {
			run.Status.Items[i].Phase = backupv1alpha1.ItemSucceeded
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded,
			fmt.Sprintf("claim %s is bound and holds the restored data", run.Spec.Into))
	}
	if deadline, over := r.overdue(run); over {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonTimedOut, fmt.Sprintf("claim %s had not bound by %s", run.Spec.Into, deadline.Format(time.RFC3339)))
	}
	switch {
	case readErr == nil:
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	case !apierrors.IsNotFound(readErr):
		return ctrl.Result{}, fmt.Errorf("get PersistentVolumeClaim %s: %w", key, readErr)
	}

	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		if !isRefusal(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
	}
	if waiting, err := r.waitForBackup(ctx, run, run.Spec.Claim, settings.Secret); waiting || err != nil {
		return after(pollInterval, err)
	}
	vr := pointInTimeRestore(run, run.Status.Items[0], settings)
	refusal, err := r.createOwned(ctx, run, vr, &backupv1alpha1.VolumeRestore{}, "VolumeRestore")
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, refusal)
	}
	refusal, err = r.createOwned(ctx, run, scratchClaim(run, settings, vr.Name), &corev1.PersistentVolumeClaim{}, "claim")
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, refusal)
	}
	return after(pollInterval, r.resume(ctx, run))
}

// restoreIntoEmptyClaim restores a repository that no claim in the namespace
// backs up to, which spec.repository names, into the new claim spec.into
// names.
//
// The populator path can't do this on a WaitForFirstConsumer class, which is
// every class on the walzen cluster. The populator library fills a claim only
// once volume.kubernetes.io/selected-node is set on it, and only the scheduler
// sets that, when it places a pod that uses the claim. With no source claim
// there is no node to copy, and picking one here would have to repeat the
// scheduler's checks of topology, capacity and taints. So the run creates a
// plain claim of spec.intoSize, with no data source, and a
// ReplicationDestination whose mover writes the selected snapshot into it
// (see directDestination). The mover pod is the claim's first consumer, so
// the scheduler places the claim where the mover runs, the way VolSync places
// a destination claim it creates itself.
//
// The run succeeds once the destination has completed the run's trigger, and
// fails with the mover's logs when the mover fails. It records the item's end
// in the status before finish deletes the destination, for the reason work
// gives. It is aborted with reason TimedOut when the restore has not finished
// by spec.timeout, and the deadline is checked before anything is created.
// While a backup of the repository is in progress (see otherMover), the run
// waits with reason SourceBusy before it creates anything.
//
// The run writes only into a claim it created and only through a
// destination it created. A claim named spec.into that the run did not
// create (see notCreatedByRun), found before the create or at it, aborts the
// run with reason Failed before any destination exists; the checks refuse
// one that existed before them. A destination with the item's name that the
// run did not create (see ownsDestination) aborts it too, and is left alone.
// Once the destination exists, every pass reads the claim again, and a claim
// that is gone, being deleted or replaced fails the run (see claimLost), so
// a mover that completed never counts as a restore into a claim that is no
// longer the run's.
func (r *RestoreRunReconciler) restoreIntoEmptyClaim(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	item := &run.Status.Items[0]
	done := fmt.Sprintf("claim %s holds the restored data", run.Spec.Into)
	if item.Phase == backupv1alpha1.ItemSucceeded {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, done)
	}

	destination := &volsyncv1alpha1.ReplicationDestination{}
	readErr := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Destination}, destination)
	if readErr == nil {
		if !ownsDestination(run, destination) {
			message := takenDestination(item.Destination)
			item.Destination = ""
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, message)
		}
		lost, err := r.claimLost(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if lost != "" {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, lost
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, restoreFailures(run.Status.Items))
		}
		if reason, failed := failedMover(destination); failed {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, reason
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, restoreFailures(run.Status.Items))
		}
		if destination.Status != nil && destination.Status.LastManualSync == string(run.UID) {
			item.Phase = backupv1alpha1.ItemSucceeded
			if err := r.writeStatus(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, done)
		}
	}
	if deadline, over := r.overdue(run); over {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonTimedOut, fmt.Sprintf("claim %s had not been restored by %s", run.Spec.Into, deadline.Format(time.RFC3339)))
	}
	switch {
	case readErr == nil:
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	case !apierrors.IsNotFound(readErr):
		return ctrl.Result{}, fmt.Errorf("get ReplicationDestination %s: %w", item.Destination, readErr)
	}

	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, "", run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		return ctrl.Result{}, err
	}
	// A claim that appeared since the checks ends the run before it waits
	// for anything: it is not the run's to write into.
	refusal, err := r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, "claim")
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, refusal)
	}
	if waiting, err := r.waitForBackup(ctx, run, run.Spec.Into, settings.Secret); waiting || err != nil {
		return after(pollInterval, err)
	}
	// The destination is created only once the claim is known to be the
	// run's: its mover deletes every file the snapshot lacks.
	refusal, err = r.createOwned(ctx, run, scratchClaim(run, settings, ""), &corev1.PersistentVolumeClaim{}, "claim")
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, refusal)
	}
	refusal, err = r.createDestination(ctx, run, directDestination(run, *item, settings, item.Destination))
	if err != nil {
		return ctrl.Result{}, err
	}
	if refusal != "" {
		item.Destination = ""
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, refusal)
	}
	return after(pollInterval, r.resume(ctx, run))
}

// intoTaken reads the object named spec.into into object, and returns the
// refusal from notCreatedByRun when it exists and the run did not create it.
// It returns "" when the object does not exist or the run controls it.
//
// Parameters:
//   - object is an empty claim or VolumeRestore, of the kind to read.
//   - kind is "claim" or "VolumeRestore", for the message.
//
// The read goes through the uncached Reader, so an object created a moment
// ago is seen. A failed read comes back as an error, and the caller retries.
func (r *RestoreRunReconciler) intoTaken(ctx context.Context, run *backupv1alpha1.RestoreRun, object client.Object, kind string) (string, error) {
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	if err := r.Reader.Get(ctx, key, object); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get %s %s: %w", kind, key, err)
	}
	return notCreatedByRun(run, kind, object), nil
}

// createOwned creates object, which carries the run's controller reference,
// and returns "" once an object of that name exists that the run controls.
//
// Parameters:
//   - object is the claim or VolumeRestore to create.
//   - existing is an empty object of the same kind, which the stored object
//     is read into when the create finds one.
//   - kind is "claim" or "VolumeRestore", for the message.
//
// A create that finds the name taken reads the stored object. The run's own,
// left by a pass whose create went through but whose answer was lost, is
// fine; any other gives the refusal from notCreatedByRun, and nothing is
// written to it. A failed create or read comes back as an error, and the
// caller retries.
func (r *RestoreRunReconciler) createOwned(ctx context.Context, run *backupv1alpha1.RestoreRun, object, existing client.Object, kind string) (string, error) {
	err := r.Create(ctx, object)
	if err == nil {
		return "", nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create %s %s/%s: %w", kind, object.GetNamespace(), object.GetName(), err)
	}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(object), existing); err != nil {
		return "", fmt.Errorf("get %s %s/%s: %w", kind, object.GetNamespace(), object.GetName(), err)
	}
	return notCreatedByRun(run, kind, existing), nil
}

// createDestination creates the run's ReplicationDestination, and returns
// "" once a destination of that name exists that the run owns (see
// ownsDestination). A destination of that name with another trigger belongs
// to something else; createDestination returns a refusal naming it, and the
// caller must not record it on the item, because finish deletes the
// destination an item names. A failed create or read comes back as an error.
func (r *RestoreRunReconciler) createDestination(ctx context.Context, run *backupv1alpha1.RestoreRun, destination *volsyncv1alpha1.ReplicationDestination) (string, error) {
	err := r.Create(ctx, destination)
	if err == nil {
		return "", nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create ReplicationDestination %s: %w", destination.Name, err)
	}
	stored := &volsyncv1alpha1.ReplicationDestination{}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(destination), stored); err != nil {
		return "", fmt.Errorf("get ReplicationDestination %s: %w", destination.Name, err)
	}
	if !ownsDestination(run, stored) {
		return takenDestination(destination.Name), nil
	}
	return "", nil
}

// takenDestination returns the message for a ReplicationDestination with
// the name the run gives its own that the run did not create.
func takenDestination(name string) string {
	return fmt.Sprintf("ReplicationDestination %s already exists and does not carry this run's trigger; nothing was written", name)
}

// replacedDestination returns the message for an item whose
// ReplicationDestination was replaced, while the item ran, by one the run
// did not create.
func replacedDestination(name string) string {
	return fmt.Sprintf("ReplicationDestination %s was replaced by one that does not carry this run's trigger; the run leaves it alone", name)
}

// claimLost returns a message when the claim spec.into names is gone, is
// being deleted, or is not the one the run created (see notCreatedByRun),
// and "" while the run's own claim is there. An into restore from a
// repository checks it on every pass after its destination exists, so a
// mover that finished never counts as a restore into a claim that is no
// longer the run's. A failed read comes back as an error.
func (r *RestoreRunReconciler) claimLost(ctx context.Context, run *backupv1alpha1.RestoreRun) (string, error) {
	lost := fmt.Sprintf("claim %s was deleted (or replaced) while the mover wrote into it", run.Spec.Into)
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	if err := r.Reader.Get(ctx, key, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return lost, nil
		}
		return "", fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	}
	if claim.DeletionTimestamp != nil || !metav1.IsControlledBy(claim, run) {
		return lost, nil
	}
	return "", nil
}

// abort ends a run early as Failed. It marks every item that has not finished
// as Failed with the given message, then calls finish, which deletes the run's
// ReplicationDestinations and starts any workload the run stopped.
//
// Parameters:
//   - reason is the Ready reason the run ends with: ReasonTimedOut when the
//     run ran past spec.timeout, ReasonFailed when it hit an error it can't
//     get past, such as a workload it couldn't stop or one that was deleted.
//   - message is the Ready message, and each unfinished item's message.
func (r *RestoreRunReconciler) abort(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
	return r.finish(ctx, run, reason, message)
}

// quiesce stops the workloads spec.quiesce lists, before the run does
// anything else.
//
// It first records the plan from planStop in status.quiesced and
// status.suspendedKustomizations, with each workload's replica count, and
// writes the status. Only then does applyStop suspend the Kustomizations and
// scale the workloads to zero. A pass that finds a plan in the status reuses
// it, so a retry after a lost status write still gives back the counts the
// workloads had before the run touched them. quiesce then writes
// status.quiescedAt. After a failed stop, it narrows the plan with
// appliedPart to what is stopped now, and aborts the run with reason Failed,
// which starts those workloads again and resumes the Kustomizations, the same as a
// BackupRun. A spec.quiesce entry the namespace does not hold ends the run as
// Failed with reason Invalid before anything is stopped, and a failed read of
// an entry or a Kustomization is returned for a retry.
func (r *RestoreRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if len(run.Status.Quiesced) == 0 {
		targets, err := namedTargets(ctx, r.Reader, run.Namespace, run.Spec.Quiesce)
		if err != nil {
			if !isQuiesceSpecError(err) {
				return ctrl.Result{}, err
			}
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
		}
		stop, suspend, err := planStop(ctx, r.Reader, targets)
		if err != nil {
			return ctrl.Result{}, err
		}
		run.Status.Quiesced, run.Status.SuspendedKustomizations = stop, suspend
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	stopErr := applyStop(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	if stopErr != nil {
		run.Status.Quiesced, run.Status.SuspendedKustomizations = appliedPart(ctx, r.Reader, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	}
	now := metav1.NewTime(r.Now())
	run.Status.QuiescedAt = &now
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if stopErr != nil {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, stopErr.Error())
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// restart gives the stopped workloads their replicas back, resumes the
// Kustomizations the run suspended, and records status.restartedAt. The
// caller writes the status.
func (r *RestoreRunReconciler) restart(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
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

// finish ends the run. It deletes the ReplicationDestinations the run
// created, releases the run's Leases (see releaseLeases), starts any
// workload it still holds stopped, and records the
// terminal phase: Succeeded when reason is ReasonSucceeded and Failed for any
// other reason. It sets the Ready condition to reason and message, records
// status.completedAt, and removes the finalizer. For an into restore that
// did not succeed, it first releases the VolumeRestore with
// releaseVolumeRestore.
func (r *RestoreRunReconciler) finish(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	if reason != backupv1alpha1.ReasonSucceeded {
		if err := r.releaseVolumeRestore(ctx, run); err != nil {
			return err
		}
	}
	if err := r.removeDestinations(ctx, run, anyItem); err != nil {
		return err
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return err
	}
	if stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return err
		}
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	if reason != backupv1alpha1.ReasonSucceeded {
		run.Status.Phase = backupv1alpha1.RunPhaseFailed
	}
	run.Status.CompletedAt = &now
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, reason, message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	return dropFinalizer(ctx, r.Client, run)
}

// finalize runs when the run is deleted before it finished. It deletes the
// run's ReplicationDestinations, releases the run's Leases (see
// releaseLeases), starts any workload the run still holds
// stopped, releases an into restore's VolumeRestore with
// releaseVolumeRestore, and removes the finalizer so the deletion can
// complete. Without it, a run deleted while its mover writes would leave a
// restore running against a claim with nothing tracking it.
func (r *RestoreRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.removeDestinations(ctx, run, anyItem); err != nil {
		return err
	}
	if err := r.releaseVolumeRestore(ctx, run); err != nil {
		return err
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return err
	}
	if stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return err
		}
	}
	return dropFinalizer(ctx, r.Client, run)
}

// removeDestinations deletes the ReplicationDestination that each item
// chosen by the function given in which still names, and clears the item's
// destination. finish and finalize pass anyItem, and work passes finished.
//
// It reads each destination first and deletes it only when the run owns it
// (see ownsDestination); one with another trigger is left alone, and the
// item's destination is cleared all the same. The delete carries the UID it
// read as a precondition, so a destination created under the name since the
// read is never deleted. A destination that is already gone is not an error.
// A failed read or delete, and a delete whose precondition failed, come back
// as an error, and the next pass reads the destination again.
func (r *RestoreRunReconciler) removeDestinations(ctx context.Context, run *backupv1alpha1.RestoreRun, which func(backupv1alpha1.RestoreItem) bool) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Destination == "" || !which(*item) {
			continue
		}
		destination := &volsyncv1alpha1.ReplicationDestination{}
		key := types.NamespacedName{Namespace: run.Namespace, Name: item.Destination}
		if err := r.Reader.Get(ctx, key, destination); err != nil {
			if !apierrors.IsNotFound(err) {
				return fmt.Errorf("get ReplicationDestination %s: %w", item.Destination, err)
			}
			item.Destination = ""
			continue
		}
		if ownsDestination(run, destination) {
			uid := destination.UID
			if err := r.Delete(ctx, destination, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("delete ReplicationDestination %s: %w", item.Destination, err)
			}
		}
		item.Destination = ""
	}
	return nil
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
// of the claim or the repository Secret secret is in progress. It is called
// right before the run creates its first object. It first takes the Leases of
// the claim and the repository for the run's item (see acquireLeases), then
// looks for a backup's mover object (see otherMover), which catches a backup
// started before the controller took Leases.
//
// It returns true when the run has to wait. It has then moved the run to
// Waiting with reason SourceBusy and a message naming the run that holds a
// Lease or the BackupRun. A failed read, write or status write comes back as
// an error, which the caller retries.
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

// resume moves an into restore that waited back to Running once it has
// created its objects, and writes the status. A run that did not wait is
// left as it is, and nothing is written.
func (r *RestoreRunReconciler) resume(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting {
		return nil
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("restoring into claim %s", run.Spec.Into))
	return r.writeStatus(ctx, run)
}

// writeStatus writes the run's status subresource.
func (r *RestoreRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set RestoreRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// claimHolder returns the name of a pod in the namespace that mounts the
// claim, or an empty string when none does. Pods that have Succeeded or
// Failed don't count.
//
// The run reads the pods through its uncached Reader. Without spec.quiesce
// this check is the only thing that keeps a second writer off the volume, and
// an informer cache that has not yet seen a new pod would report the claim
// free.
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

// destinationName returns the name of the ReplicationDestination that
// restores one item of a run: restore-, the first eight characters of the
// run's UID, a dash, and the item's position in status.items. The name is
// kept short because VolSync names its Job and pod labels after it.
func destinationName(uid types.UID, index int) string {
	short := string(uid)
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("restore-%s-%d", short, index)
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

// failedMover reports whether the destination's latest mover failed, and
// returns the mover's logs, which hold restic's error.
func failedMover(destination *volsyncv1alpha1.ReplicationDestination) (string, bool) {
	if destination.Status == nil || destination.Status.LatestMoverStatus == nil {
		return "", false
	}
	mover := destination.Status.LatestMoverStatus
	return mover.Logs, mover.Result == volsyncv1alpha1.MoverResultFailed
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
// The returned text is the item's message.
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

// anyItem chooses every item, for removeDestinations.
func anyItem(backupv1alpha1.RestoreItem) bool { return true }

// finished reports whether an item is Succeeded, Failed or Skipped, so its
// ReplicationDestination has no more work to do.
func finished(item backupv1alpha1.RestoreItem) bool {
	switch item.Phase {
	case backupv1alpha1.ItemSucceeded, backupv1alpha1.ItemFailed, backupv1alpha1.ItemSkipped:
		return true
	}
	return false
}

// restoreItemDone reports whether the run's item named name has finished (see
// finished), or the run has no such item.
func restoreItemDone(run *backupv1alpha1.RestoreRun, name string) bool {
	for _, item := range run.Status.Items {
		if item.Name == name {
			return finished(item)
		}
	}
	return true
}

// finishedWithDestination reports whether any finished item still names a
// ReplicationDestination.
func finishedWithDestination(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if finished(item) && item.Destination != "" {
			return true
		}
	}
	return false
}

// populatorClaimFinalizer is the end of the finalizer name the volume populator
// library puts on a claim while it fills it. The name starts with the
// populator's prefix, which the binary sets.
const populatorClaimFinalizer = "/populate-target-protection"

// releaseVolumeRestore takes populator.Finalizer off the VolumeRestore an into
// restore created, when the populator never took that VolumeRestore on. finish
// calls it for an into restore that did not succeed, and finalize for one
// deleted before it finished.
//
// pointInTimeRestore creates the VolumeRestore with the finalizer, and the
// populator's Cleanup removes it once the claim is filled or deleted. The
// library calls Cleanup only for a claim it has started on, so a claim it
// never reached, such as one still waiting for a node, would leave the
// VolumeRestore hanging in deletion after the run is gone.
//
// It leaves the finalizer while the VolumeRestore's status lists a claim, or
// while the scratch claim carries the library's finalizer, because the
// populator is then at work and its Cleanup will remove it. If the populator
// starts after the release, its Populate adds the finalizer again. A
// VolumeRestore the run does not control is left alone. It returns an error
// when a read or the write fails.
func (r *RestoreRunReconciler) releaseVolumeRestore(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if run.Spec.Into == "" {
		return nil
	}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	vr := &backupv1alpha1.VolumeRestore{}
	if err := r.Reader.Get(ctx, key, vr); err != nil {
		return client.IgnoreNotFound(err)
	}
	if !metav1.IsControlledBy(vr, run) || !controllerutil.ContainsFinalizer(vr, populator.Finalizer) || len(vr.Status.Claims) > 0 {
		return nil
	}
	claim := &corev1.PersistentVolumeClaim{}
	err := r.Reader.Get(ctx, key, claim)
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	}
	if err == nil && slices.ContainsFunc(claim.Finalizers, func(f string) bool { return strings.HasSuffix(f, populatorClaimFinalizer) }) {
		return nil
	}
	controllerutil.RemoveFinalizer(vr, populator.Finalizer)
	if err := r.Update(ctx, vr); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("remove finalizer %s from VolumeRestore %s: %w", populator.Finalizer, key, err)
	}
	return nil
}
