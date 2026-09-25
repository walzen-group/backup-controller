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

// RestoreRunReconciler restores volumes and databases to a chosen moment.
//
// A volume restores in place through a Direct-mode ReplicationDestination
// mounting the claim, once nothing else mounts it. A database restores by
// being created again: the run deletes its Cluster, and the bootstrap webhook
// recovers the Cluster Flux or tofu creates next to the run's moment. With
// Into set, a volume restores into a new claim instead, through the ordinary
// populator path, and nothing of the app's is touched.
type RestoreRunReconciler struct {
	client.Client

	// Reader reads without the informer cache: Secrets, ObjectStores and
	// Clusters.
	Reader client.Reader

	// Snapshots lists a repository's snapshots, for the check that a volume
	// has one the run's moment reaches.
	Snapshots restic.Lister

	// Prober lists a database's base backups, for the same check.
	Prober bootstrap.Prober

	// Recorder writes an event on the run each time its Ready reason changes.
	Recorder events.EventRecorder

	// Now is the clock, injected so tests can move time without sleeping.
	Now func() time.Time
}

// SetupWithManager registers the reconciler.
func (r *RestoreRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.RestoreRun{}).
		Named("restorerun").
		Complete(r)
}

// Reconcile drives one RestoreRun from submitted to finished.
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

// target parses the run's moment, nil for the newest backup.
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

// plan lists the run's items and checks, before it touches anything, that a
// backup reaches the run's moment for every one of them.
func (r *RestoreRunReconciler) plan(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	items, err := r.items(ctx, run)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	at, err := target(run)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}

	// A synced run takes its databases' moment from the volumes' quiesced
	// snapshots. items lists the claims before the Clusters, so the moment is
	// known by the time the first Cluster is checked.
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
		// Nothing has been deleted or overwritten yet, so the other items stay
		// as they were and the run reports every item it cannot reach.
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

// items lists what the run's spec names.
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

// checkVolume finds the snapshot a restore of the claim would select, and
// returns a reason when there is none. VolSync itself restores nothing and
// reports success when no snapshot matches, so this is the only place the
// missing snapshot is caught.
func (r *RestoreRunReconciler) checkVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, string, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		return restic.Snapshot{}, err.Error(), nil
	}
	return r.selectSnapshot(ctx, run, settings.Secret, at, quiescedOnly)
}

// selectSnapshot returns the snapshot the run would restore from the
// repository Secret, or a reason when there is none. quiescedOnly passes over
// every snapshot not tagged quiesced.
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

// checkDatabase finds the base backup a recovery of the Cluster would start
// from, and returns a reason when there is none.
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

// work restores the volumes, then deletes the databases and follows them
// until they are recovered.
func (r *RestoreRunReconciler) work(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if deadline, over := r.overdue(run); over {
		return ctrl.Result{}, r.abort(ctx, run, fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339)))
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

	var recreate []string
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
				recreate = append(recreate, item.Name)
			}
		}
	}

	if restoreDone(run.Status.Items) {
		if failed := restoreFailures(run.Status.Items); failed != "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, failed)
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, "every item holds the restored data")
	}

	switch {
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

// restoreVolume advances one in-place volume restore, and returns a waiting
// reason while a pod still mounts the claim.
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

// restoreDatabase advances one database restore: it deletes the Cluster, then
// follows the Cluster Flux or tofu creates again until it is healthy.
//
// The item is marked Deleted before the Cluster is deleted. The bootstrap
// webhook recovers a Cluster only for a run whose item says Deleted, so the
// mark has to be in place before anything can create the Cluster again.
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
			// The webhook annotates every Cluster it recovers for a run whose
			// item says Deleted, so a live Cluster without the annotation is
			// the one the delete above did not reach.
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

// reconcileIntoNewClaim fills a second claim and leaves the app's alone.
//
// It writes a VolumeRestore carrying the chosen point in time and a claim
// naming it, so the ordinary populator path does the work. It first checks
// that a snapshot is in reach, because the populator would otherwise bind an
// empty claim and report success.
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
		return ctrl.Result{}, r.abort(ctx, run, fmt.Sprintf("claim %s had not bound by %s", run.Spec.Into, deadline.Format(time.RFC3339)))
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

// abort ends a run early: every unfinished item fails and every destination
// the run created is removed.
func (r *RestoreRunReconciler) abort(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
	return r.finish(ctx, run, backupv1alpha1.ReasonTimedOut, message)
}

// finish removes the destinations the run created and records the terminal
// phase.
func (r *RestoreRunReconciler) finish(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	if err := r.removeDestinations(ctx, run); err != nil {
		return err
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

// finalize removes the destinations on the way to deletion. A run deleted
// while its mover writes would otherwise leave a restore running against a
// claim with nothing tracking it.
func (r *RestoreRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.removeDestinations(ctx, run); err != nil {
		return err
	}
	return dropFinalizer(ctx, r.Client, run)
}

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

func (r *RestoreRunReconciler) overdue(run *backupv1alpha1.RestoreRun) (time.Time, bool) {
	if run.Status.StartedAt == nil || run.Spec.Timeout == nil {
		return time.Time{}, false
	}
	deadline := run.Status.StartedAt.Add(run.Spec.Timeout.Duration)
	return deadline, !r.Now().Before(deadline)
}

func (r *RestoreRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

func (r *RestoreRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set RestoreRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// claimHolder returns the name of a pod mounting the claim, empty when none is.
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

// destinationName is the ReplicationDestination restoring the run's index-th
// item. It is short: VolSync names its Job and pod labels after it.
func destinationName(uid types.UID, index int) string {
	short := string(uid)
	if len(short) > 8 {
		short = short[:8]
	}
	return fmt.Sprintf("restore-%s-%d", short, index)
}

// restoreDone reports whether every item reached a terminal phase.
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

// restoreFailures names the failed items, empty when none failed.
func restoreFailures(items []backupv1alpha1.RestoreItem) string {
	var failed []string
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			failed = append(failed, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
		}
	}
	return strings.Join(failed, "; ")
}

// failedMover reports a destination whose mover gave up, with restic's message.
func failedMover(destination *volsyncv1alpha1.ReplicationDestination) (string, bool) {
	if destination.Status == nil || destination.Status.LatestMoverStatus == nil {
		return "", false
	}
	mover := destination.Status.LatestMoverStatus
	return mover.Logs, mover.Result == volsyncv1alpha1.MoverResultFailed
}
