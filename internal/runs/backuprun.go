package runs

import (
	"context"
	"errors"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// backupRunKind is the kind the Workload's owner reference names.
var backupRunKind = backupv1alpha1.GroupVersion.WithKind("BackupRun")

// BackupRunReconciler runs one backup: of a volume, of a database, or of a
// whole namespace.
type BackupRunReconciler struct {
	client.Client

	// Reader reads without the informer cache: Secrets, Workloads, LocalQueues
	// and Kustomizations, which the controller has no reason to watch.
	Reader client.Reader

	// Snapshots lists a repository's snapshots, for the time restic stamped on
	// the one a mover saved.
	Snapshots restic.Lister

	// Now is the clock, injected so tests can move time without sleeping.
	Now func() time.Time
}

// SetupWithManager registers the reconciler.
func (r *BackupRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.BackupRun{}).
		Named("backuprun").
		Complete(r)
}

// Reconcile drives one BackupRun from submitted to finished.
//
// The run moves through four steps: it lists its items, waits for the queue to
// admit it, stops the workloads marked for quiesce while it starts every
// item, and collects each item's result. Each step reads the state the last
// one recorded, so a reconcile that runs twice does the same thing twice.
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, run)
	}
	if run.Status.Phase.Finished() {
		return expire(ctx, r.Client, run, run.Spec.TTLSecondsAfterFinished, run.Status.CompletedAt, r.Now())
	}
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		controllerutil.AddFinalizer(run, Finalizer)
		if err := r.Update(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer to BackupRun %s/%s: %w", run.Namespace, run.Name, err)
		}
	}

	switch run.Status.Phase {
	case "":
		return r.plan(ctx, run)
	case backupv1alpha1.RunPhaseQueued:
		return r.admit(ctx, run)
	default:
		return r.work(ctx, run)
	}
}

// plan resolves what the run backs up and records one item for each.
func (r *BackupRunReconciler) plan(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	items, err := r.items(ctx, run)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	run.Status.Items = items
	run.Status.Phase = backupv1alpha1.RunPhaseQueued
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonQueued,
		"waiting for the backup queue to admit the run")
	return after(time.Second, r.writeStatus(ctx, run))
}

// items lists what the run's spec names. Everything it backs up has to be
// marked backup.wlz.li/enabled, so a run and a schedule cover the same set.
func (r *BackupRunReconciler) items(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	pending := func(kind, name string) backupv1alpha1.BackupItem {
		return backupv1alpha1.BackupItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}

	switch {
	case run.Spec.Source != "":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}, claim); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("no claim %s in this namespace", run.Spec.Source)
			}
			return nil, fmt.Errorf("get claim %s: %w", run.Spec.Source, err)
		}
		if !backupv1alpha1.Enabled(claim.Annotations) {
			return nil, fmt.Errorf("claim %s is not marked %s: \"true\"", claim.Name, backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("ReplicationSource", claim.Name)}, nil

	case run.Spec.Database != "":
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, run.Spec.Database)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("no Cluster %s in this namespace", run.Spec.Database)
		}
		if !backupv1alpha1.Enabled(cluster.GetAnnotations()) {
			return nil, fmt.Errorf("the Cluster %s is not marked %s: \"true\"", cluster.GetName(), backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("Cluster", cluster.GetName())}, nil

	default:
		claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
		if err != nil {
			return nil, err
		}
		clusters, err := enabledClusters(ctx, r.Reader, run.Namespace)
		if err != nil {
			return nil, err
		}
		var items []backupv1alpha1.BackupItem
		for _, claim := range claims {
			items = append(items, pending("ReplicationSource", claim.Name))
		}
		for _, cluster := range clusters {
			items = append(items, pending("Cluster", cluster.GetName()))
		}
		if len(items) == 0 {
			return nil, fmt.Errorf("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
		}
		return items, nil
	}
}

// admit waits for the namespace's queue to admit the run as one Workload. A
// namespace with no LocalQueue starts at once.
func (r *BackupRunReconciler) admit(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	queue, err := localQueue(ctx, r.Reader, run.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if queue != "" {
		workload, err := ensureWorkload(ctx, r.Client, run, backupRunKind, queue)
		if err != nil {
			return ctrl.Result{}, err
		}
		if run.Status.Workload != workload.GetName() {
			run.Status.Workload = workload.GetName()
			if err := r.writeStatus(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
		}
		if !admitted(workload) {
			return ctrl.Result{RequeueAfter: pollInterval}, nil
		}
		if err := markPodsReady(ctx, r.Client, workload, metav1.NewTime(r.Now())); err != nil {
			return ctrl.Result{}, err
		}
	}

	// A namespace timeout that does not parse fails the run before it starts
	// anything, since no deadline could be kept.
	if _, err := timeoutFor(ctx, r.Reader, run); err != nil {
		var bad invalidSetting
		if errors.As(err, &bad) {
			return ctrl.Result{}, r.abort(ctx, run, bad.Error())
		}
		return ctrl.Result{}, err
	}

	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = newTime(metav1.NewTime(r.Now()))
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "backing up")
	return after(time.Second, r.writeStatus(ctx, run))
}

// work stops the quiesced workloads, starts every item, restarts the
// workloads once every clone is cut, and collects the results.
func (r *BackupRunReconciler) work(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	now := metav1.NewTime(r.Now())
	deadline, over, err := r.overdue(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if over {
		return ctrl.Result{}, r.abort(ctx, run, fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339)))
	}

	if run.Spec.All && run.Status.QuiescedAt == nil {
		return r.quiesce(ctx, run, now)
	}

	if run.Spec.All && run.Status.RestartedAt == nil && anyPending(run.Status.Items) {
		targets, err := quiesceTargets(ctx, r.Reader, run.Namespace)
		if err != nil {
			return ctrl.Result{}, err
		}
		gone, pod, err := podsGone(ctx, r.Reader, run.Namespace, targets)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			return after(2*time.Second, r.waitFor(ctx, run, backupv1alpha1.ReasonRunning,
				fmt.Sprintf("waiting for pod %s to stop before the clones are cut", pod)))
		}
	}

	waiting := ""
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		if message := r.startItem(ctx, run, item); message != "" {
			waiting = message
		}
	}

	if run.Spec.All && run.Status.RestartedAt == nil && r.clonesCut(ctx, run) {
		if err := restartWorkloads(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
			return ctrl.Result{}, err
		}
		run.Status.RestartedAt = newTime(now)
	}

	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase == backupv1alpha1.ItemRunning {
			r.collectItem(ctx, run, item)
		}
	}

	if allDone(run.Status.Items) && (!run.Spec.All || run.Status.RestartedAt != nil) {
		failed := failures(run.Status.Items)
		if failed == "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, summary(run.Status.Items))
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, failed)
	}

	if waiting != "" {
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, waiting))
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "backing up")
	interval := pollInterval
	if run.Spec.All && run.Status.RestartedAt == nil {
		// The workloads are down until every clone is cut, so this wait
		// looks more often than the others.
		interval = 2 * time.Second
	}
	return after(interval, r.writeStatus(ctx, run))
}

// quiesce stops the marked workloads and records what it changed before
// anything else happens, so a failure halfway is still undone by the
// finalizer.
func (r *BackupRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.BackupRun, now metav1.Time) (ctrl.Result, error) {
	// A volume still busy with another run's backup would keep the stopped
	// workloads down for as long as that backup takes, so the run waits with
	// the workloads running.
	for _, item := range run.Status.Items {
		if item.Kind != "ReplicationSource" {
			continue
		}
		source := &volsyncv1alpha1.ReplicationSource{}
		err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source)
		if err == nil && busy(source) && manualTag(source) != TriggerFor(run.UID) {
			return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy,
				fmt.Sprintf("ReplicationSource %s is still completing another run's backup", item.Name)))
		}
	}

	targets, err := quiesceTargets(ctx, r.Reader, run.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	stopped, suspended, stopErr := stopWorkloads(ctx, r.Client, r.Reader, targets)
	run.Status.Quiesced = stopped
	run.Status.SuspendedKustomizations = suspended
	run.Status.QuiescedAt = newTime(now)
	if len(targets) == 0 {
		run.Status.RestartedAt = newTime(now)
	}
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if stopErr != nil {
		return ctrl.Result{}, r.abort(ctx, run, stopErr.Error())
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// startItem triggers one item and returns a waiting message when the item has
// to wait for another run.
func (r *BackupRunReconciler) startItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) string {
	switch item.Kind {
	case "ReplicationSource":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, claim); err != nil {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("get claim %s: %v", item.Name, err)
			return ""
		}
		tag := TriggerFor(run.UID)
		_, err := ensureSource(ctx, r.Client, r.Reader, claim, tag)
		if errors.Is(err, errSourceBusy) {
			return fmt.Sprintf("ReplicationSource %s is still completing another run's backup", item.Name)
		}
		if err != nil {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return ""
		}
		item.Phase, item.Trigger = backupv1alpha1.ItemRunning, tag

	case "Cluster":
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		switch {
		case err != nil:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
		case !found:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the Cluster %s no longer exists", item.Name)
		case hibernated(cluster):
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"
		default:
			name, err := ensureBackup(ctx, r.Client, run.Namespace, item.Name, run.UID)
			if err != nil {
				item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
				return ""
			}
			item.Phase, item.Backup = backupv1alpha1.ItemRunning, name
		}
	}
	return ""
}

// clonesCut reports whether every volume's clone exists, which is the moment
// the stopped workloads can start again: VolSync uploads from the clone. A
// source that already completed the tag has cut its clone too, which covers a
// clone that came and went between two looks.
func (r *BackupRunReconciler) clonesCut(ctx context.Context, run *backupv1alpha1.BackupRun) bool {
	for _, item := range run.Status.Items {
		if item.Kind != "ReplicationSource" {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			return false
		case backupv1alpha1.ItemRunning:
		default:
			continue
		}
		source := &volsyncv1alpha1.ReplicationSource{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err == nil && lastManual(source) == item.Trigger {
			continue
		}
		clone := &corev1.PersistentVolumeClaim{}
		key := types.NamespacedName{Namespace: run.Namespace, Name: "volsync-" + item.Name + "-src"}
		if err := r.Reader.Get(ctx, key, clone); err != nil || clone.Status.Phase != corev1.ClaimBound {
			return false
		}
	}
	return true
}

// collectItem records a started item's result once it has one.
func (r *BackupRunReconciler) collectItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	switch item.Kind {
	case "ReplicationSource":
		source := &volsyncv1alpha1.ReplicationSource{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
			return
		}
		if lastManual(source) != item.Trigger {
			return
		}
		if source.Status.LatestMoverStatus != nil && source.Status.LatestMoverStatus.Result == volsyncv1alpha1.MoverResultFailed {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, source.Status.LatestMoverStatus.Logs
			return
		}
		snapshot, empty := moverOutcome(source)
		item.Phase = backupv1alpha1.ItemSucceeded
		item.Empty = empty
		if empty {
			item.Message = "the volume held no files, so VolSync took no snapshot"
			return
		}
		item.Snapshot = snapshot
		if at, err := r.snapshotTime(ctx, run.Namespace, item.Name, snapshot); err != nil {
			item.Message = fmt.Sprintf("the snapshot's time could not be read: %v", err)
		} else {
			item.SnapshotTime = at
		}

	case "Cluster":
		done, ok, message, err := backupResult(ctx, r.Reader, run.Namespace, item.Backup)
		switch {
		case err != nil || !done:
		case ok:
			item.Phase = backupv1alpha1.ItemSucceeded
		default:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
}

// snapshotTime reads the time restic stamped on the snapshot a mover saved.
func (r *BackupRunReconciler) snapshotTime(ctx context.Context, namespace, claimName, short string) (*metav1.Time, error) {
	if short == "" || r.Snapshots == nil {
		return nil, fmt.Errorf("the mover logged no snapshot")
	}
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claimName}, claim); err != nil {
		return nil, err
	}
	vr, err := volumeRestoreFor(ctx, r.Reader, claim)
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: vr.Spec.Repository}, secret); err != nil {
		return nil, fmt.Errorf("get Secret %s: %w", vr.Spec.Repository, err)
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return nil, err
	}
	found, ok := restic.ByShortID(snapshots, short)
	if !ok {
		return nil, fmt.Errorf("the repository holds no snapshot %s", short)
	}
	return newTime(metav1.NewTime(found.Time)), nil
}

// abort ends a run early: it restarts what it stopped, marks every unfinished
// item failed, and gives the queue its slot back.
func (r *BackupRunReconciler) abort(ctx context.Context, run *backupv1alpha1.BackupRun, message string) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
	return r.finish(ctx, run, backupv1alpha1.ReasonFailed, message)
}

// finish restarts any workload still stopped, removes the Workload, and
// records the terminal phase.
func (r *BackupRunReconciler) finish(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	if err := r.release(ctx, run); err != nil {
		return err
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	if reason != backupv1alpha1.ReasonSucceeded {
		run.Status.Phase = backupv1alpha1.RunPhaseFailed
	}
	run.Status.CompletedAt = &now
	run.Status.Workload = ""
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, reason, message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	return dropFinalizer(ctx, r.Client, run)
}

// release puts back what the run changed on the cluster.
func (r *BackupRunReconciler) release(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if run.Status.QuiescedAt != nil && run.Status.RestartedAt == nil {
		if err := restartWorkloads(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
			return err
		}
		run.Status.RestartedAt = newTime(metav1.NewTime(r.Now()))
	}
	return deleteWorkload(ctx, r.Client, run.Namespace, run.UID)
}

// finalize puts back what a run deleted mid-flight changed.
func (r *BackupRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.release(ctx, run); err != nil {
		return err
	}
	return dropFinalizer(ctx, r.Client, run)
}

// overdue reports whether the run has worked past its timeout, which
// timeoutFor resolves on every check, so a namespace annotation changed during
// a run moves its deadline.
func (r *BackupRunReconciler) overdue(ctx context.Context, run *backupv1alpha1.BackupRun) (time.Time, bool, error) {
	if run.Status.StartedAt == nil {
		return time.Time{}, false, nil
	}
	timeout, err := timeoutFor(ctx, r.Reader, run)
	if err != nil {
		return time.Time{}, false, err
	}
	deadline := run.Status.StartedAt.Add(timeout)
	return deadline, !r.Now().Before(deadline), nil
}

func (r *BackupRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

func (r *BackupRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set BackupRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// anyPending reports whether an item has not been started.
func anyPending(items []backupv1alpha1.BackupItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending {
			return true
		}
	}
	return false
}

// allDone reports whether every item reached a terminal phase.
func allDone(items []backupv1alpha1.BackupItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning {
			return false
		}
	}
	return true
}

// failures names the failed items, empty when none failed.
func failures(items []backupv1alpha1.BackupItem) string {
	message := ""
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			if message != "" {
				message += "; "
			}
			message += fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message)
		}
	}
	return message
}

// summary says what a successful run captured.
func summary(items []backupv1alpha1.BackupItem) string {
	message := ""
	for _, item := range items {
		if message != "" {
			message += "; "
		}
		switch {
		case item.Phase == backupv1alpha1.ItemSkipped:
			message += fmt.Sprintf("%s %s skipped: %s", item.Kind, item.Name, item.Message)
		case item.Snapshot != "":
			message += fmt.Sprintf("%s %s saved snapshot %s", item.Kind, item.Name, item.Snapshot)
		case item.Empty:
			message += fmt.Sprintf("%s %s was empty", item.Kind, item.Name)
		case item.Backup != "":
			message += fmt.Sprintf("%s %s completed Backup %s", item.Kind, item.Name, item.Backup)
		default:
			message += fmt.Sprintf("%s %s succeeded", item.Kind, item.Name)
		}
	}
	return message
}

// dropFinalizer removes the run's finalizer once nothing of it is left.
func dropFinalizer(ctx context.Context, c client.Client, run client.Object) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(run, Finalizer)
	if err := c.Update(ctx, run); err != nil {
		return fmt.Errorf("remove the finalizer from %s: %w", run.GetName(), err)
	}
	return nil
}

// expire deletes a finished run once its TTL has passed, and requeues until
// then. A run with no TTL is kept.
func expire(ctx context.Context, c client.Client, run client.Object, ttl *int32, completed *metav1.Time, now time.Time) (ctrl.Result, error) {
	if ttl == nil || completed == nil {
		return ctrl.Result{}, nil
	}
	deadline := completed.Add(time.Duration(*ttl) * time.Second)
	if remaining := deadline.Sub(now); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	if err := c.Delete(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete the expired %s: %w", run.GetName(), err)
	}
	return ctrl.Result{}, nil
}
