package runs

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
// left alone.
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
// A run with spec.into set goes to reconcileIntoNewClaim. Any other run starts
// in plan, which checks that every item has a backup in reach before anything
// is changed, and then continues in work. Before either, Reconcile adds the
// run's finalizer. A run being deleted gets its changes put back by finalize,
// and a finished run is deleted once spec.ttlSecondsAfterFinished has passed.
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

	if run.Spec.Into != "" {
		return r.reconcileIntoNewClaim(ctx, run)
	}
	if run.Status.Phase == "" {
		return r.plan(ctx, run)
	}
	return r.work(ctx, run)
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
// backups fails.
func (r *RestoreRunReconciler) plan(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	items, err := r.items(ctx, run)
	if err != nil {
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
// has no backup to restore, so its item starts out Skipped.
//
// items returns an error when spec.repository is set and spec.claim is not,
// because a restore in place needs a claim to write into, and when nothing in
// the namespace is marked.
func (r *RestoreRunReconciler) items(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	pending := func(kind, name string) backupv1alpha1.RestoreItem {
		return backupv1alpha1.RestoreItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}
	switch {
	case run.Spec.Claim != "":
		return []backupv1alpha1.RestoreItem{pending("PersistentVolumeClaim", run.Spec.Claim)}, nil
	case run.Spec.Repository != "":
		return nil, fmt.Errorf("spec.into is required when spec.repository names the source, because there is no claim to restore in place")
	case run.Spec.Database != "":
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
		if _, _, archives := bootstrap.Archiver(&clusters[i]); !archives {
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster archives nowhere, so it has no backup to restore"
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
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
// reason when the claim's repository can't be resolved. It returns an error
// only when listing the repository fails.
//
// This is the only place a missing snapshot is caught. VolSync restores
// nothing and still reports success when no snapshot matches.
func (r *RestoreRunReconciler) checkVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, string, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
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
// It returns a reason, and no error, when the Secret can't be read, when no
// snapshot qualifies, when none is at or before the moment, and when
// spec.previous reaches past the oldest snapshot. It returns an error when
// listing the repository fails.
func (r *RestoreRunReconciler) selectSnapshot(ctx context.Context, run *backupv1alpha1.RestoreRun, secretName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, string, error) {
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: secretName}, secret); err != nil {
		return restic.Snapshot{}, fmt.Sprintf("read repository Secret %s: %v", secretName, err), nil
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
// object store that can't be resolved. A store with no completed base backup
// is a reason too, because deleting the Cluster would bring it back empty. It
// returns an error when the Cluster can't be read or listing the base backups
// fails.
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
				left, err := instanceLeft(ctx, r.Client, run.Namespace, item.Name)
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
// trigger, or fails with the mover's logs. Either way, restoreVolume then
// deletes the destination. A claim whose repository can't be resolved fails
// the item.
//
// It returns reason ClaimInUse and a message naming the pod while a pod
// mounts the claim, and empty strings otherwise. It returns an error when an
// API call fails.
func (r *RestoreRunReconciler) restoreVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (string, string, error) {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		holder, err := claimHolder(ctx, r.Client, run.Namespace, item.Name)
		if err != nil {
			return "", "", fmt.Errorf("look for a pod holding claim %s: %w", item.Name, err)
		}
		if holder != "" {
			return backupv1alpha1.ReasonClaimInUse,
				fmt.Sprintf("claim %s is mounted by pod %s; stop the workload and this restore starts on its own", item.Name, holder), nil
		}
		settings, err := repositoryFor(ctx, r.Reader, run.Namespace, item.Name, run.Spec.Repository, run.Spec.MoverSecurityContext)
		if err != nil {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return "", "", nil
		}
		name := destinationName(run.UID, index)
		if err := r.Create(ctx, directDestination(run, item.Name, settings, name)); err != nil && !apierrors.IsAlreadyExists(err) {
			return "", "", fmt.Errorf("create ReplicationDestination %s: %w", name, err)
		}
		item.Phase, item.Destination = backupv1alpha1.ItemRunning, name

	case backupv1alpha1.ItemRunning:
		destination := &volsyncv1alpha1.ReplicationDestination{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Destination}, destination); err != nil {
			return "", "", fmt.Errorf("get ReplicationDestination %s: %w", item.Destination, err)
		}
		if reason, failed := failedMover(destination); failed {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, reason
		} else if destination.Status != nil && destination.Status.LastManualSync == string(run.UID) {
			item.Phase = backupv1alpha1.ItemSucceeded
		} else {
			return "", "", nil
		}
		if err := r.Delete(ctx, destination); err != nil && !apierrors.IsNotFound(err) {
			return "", "", fmt.Errorf("delete ReplicationDestination %s: %w", item.Destination, err)
		}
		item.Destination = ""
	}
	return "", "", nil
}

// restoreDatabase moves one database restore a step further. A Pending item
// has its Cluster deleted. A Deleted item waits until Flux or tofu creates the
// Cluster again and the bootstrap webhook marks it as this run's recovery,
// which moves the item to Recovering. A Recovering item succeeds once the
// Cluster is healthy, and fails if the recovered Cluster is deleted.
//
// The item is marked Deleted, and the status written, before the Cluster is
// deleted. The bootstrap webhook recovers a Cluster only for a run whose item
// says Deleted, so the mark has to be in place before anything can create the
// Cluster again.
func (r *RestoreRunReconciler) restoreDatabase(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) error {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		item.Phase = backupv1alpha1.ItemDeleted
		if err := r.writeStatus(ctx, run); err != nil {
			return err
		}
		return r.deleteCluster(ctx, run.Namespace, item.Name)

	case backupv1alpha1.ItemDeleted:
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		switch {
		case err != nil:
			return err
		case !found || cluster.GetDeletionTimestamp() != nil:
			return nil
		case cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun] == run.Name:
			item.Phase = backupv1alpha1.ItemRecovering
		default:
			// The webhook writes backup.wlz.li/restore-run on every Cluster it
			// recovers for a run whose item says Deleted. A live Cluster
			// without it is one the earlier delete did not reach.
			return r.deleteCluster(ctx, run.Namespace, item.Name)
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

// deleteCluster deletes a Cluster, given by its namespace and name. A Cluster
// that is already gone is not an error.
func (r *RestoreRunReconciler) deleteCluster(ctx context.Context, namespace, name string) error {
	cluster, found, err := getCluster(ctx, r.Reader, namespace, name)
	if err != nil || !found {
		return err
	}
	if err := r.Delete(ctx, cluster); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Cluster %s/%s: %w", namespace, name, err)
	}
	return nil
}

// reconcileIntoNewClaim restores a volume into the new claim that spec.into
// names, and leaves the app's claim alone.
//
// It creates a VolumeRestore carrying spec.restoreAsOf and a claim whose data
// source names it, so the ordinary populator path does the work. On the first
// pass it checks that a snapshot is in reach, because the populator would
// otherwise bind an empty claim and report success. The run succeeds once the
// new claim is Bound, and is aborted when the claim has not bound by
// spec.timeout.
func (r *RestoreRunReconciler) reconcileIntoNewClaim(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	run.Status.Target = run.Spec.Into
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}

	if run.Status.Phase == "" {
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
		now := metav1.NewTime(r.Now())
		run.Status.Phase = backupv1alpha1.RunPhaseRunning
		run.Status.StartedAt = &now
		run.Status.Items = []backupv1alpha1.RestoreItem{{
			Kind: "PersistentVolumeClaim", Name: run.Spec.Into, Phase: backupv1alpha1.ItemRunning, Snapshot: snapshot.ShortID(),
		}}
		backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
			fmt.Sprintf("restoring into claim %s", run.Spec.Into))
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}

	vr := pointInTimeRestore(run, settings)
	if err := r.Create(ctx, vr); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("create VolumeRestore %s/%s: %w", vr.Namespace, vr.Name, err)
	}
	claim := scratchClaim(run, settings, vr.Name)
	if err := r.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("create PersistentVolumeClaim %s/%s: %w", claim.Namespace, claim.Name, err)
	}

	bound := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	if err := r.Reader.Get(ctx, key, bound); err != nil {
		return ctrl.Result{}, fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	}
	if bound.Status.Phase == corev1.ClaimBound {
		for i := range run.Status.Items {
			run.Status.Items[i].Phase = backupv1alpha1.ItemSucceeded
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded,
			fmt.Sprintf("claim %s is bound and holds the restored data", run.Spec.Into))
	}
	if deadline, over := r.overdue(run); over {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonTimedOut, fmt.Sprintf("claim %s had not bound by %s", run.Spec.Into, deadline.Format(time.RFC3339)))
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
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
// created, starts any workload it still holds stopped, and records the
// terminal phase: Succeeded when reason is ReasonSucceeded and Failed for any
// other reason. It sets the Ready condition to reason and message, records
// status.completedAt, and removes the finalizer.
func (r *RestoreRunReconciler) finish(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	if err := r.removeDestinations(ctx, run); err != nil {
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
// run's ReplicationDestinations, starts any workload the run still holds
// stopped, and removes the finalizer so the deletion can complete. Without
// it, a run deleted while its mover writes would leave a restore running
// against a claim with nothing tracking it.
func (r *RestoreRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.removeDestinations(ctx, run); err != nil {
		return err
	}
	if stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return err
		}
	}
	return dropFinalizer(ctx, r.Client, run)
}

// removeDestinations deletes the ReplicationDestination each item still
// names, and clears the item's destination. A destination that is already
// gone is not an error.
func (r *RestoreRunReconciler) removeDestinations(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Destination == "" {
			continue
		}
		destination := &volsyncv1alpha1.ReplicationDestination{
			ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: item.Destination},
		}
		if err := r.Delete(ctx, destination); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ReplicationDestination %s: %w", item.Destination, err)
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
		switch item.Phase {
		case backupv1alpha1.ItemSucceeded, backupv1alpha1.ItemFailed, backupv1alpha1.ItemSkipped:
		default:
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
