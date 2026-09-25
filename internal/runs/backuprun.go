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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// backupRunKind is the group, version and kind that the owner reference on a
// BackupRun's Kueue Workload names.
var backupRunKind = backupv1alpha1.GroupVersion.WithKind("BackupRun")

// BackupRunReconciler runs BackupRuns. A BackupRun backs up one volume, one
// database, or every volume and database in its namespace that is marked
// backup.wlz.li/enabled.
type BackupRunReconciler struct {
	client.Client

	// Reader reads straight from the API server, without the informer cache.
	// The run reads claims, ReplicationSources, Clusters, Secrets, pods,
	// LocalQueues and Kustomizations through it. The controller has no reason
	// to watch any of these kinds.
	Reader client.Reader

	// Snapshots lists the snapshots in a restic repository. The run uses it to
	// read the time restic stamped on the snapshot a mover saved.
	Snapshots restic.Lister

	// Retimer rewrites a snapshot with a new time and a tag. A quiesced run
	// uses it to move each volume's snapshot to the moment the run started the
	// workloads again, and to tag it quiesced.
	Retimer restic.Retimer

	// Recorder writes an event on the run each time its Ready reason changes.
	Recorder events.EventRecorder

	// Now returns the current time. Tests replace it so they can move time
	// forward without sleeping.
	Now func() time.Time
}

// SetupWithManager registers the reconciler with mgr so it runs for every
// BackupRun. It sets Now to time.Now when the caller left it unset.
func (r *BackupRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.BackupRun{}).
		Named("backuprun").
		Complete(r)
}

// Reconcile moves the BackupRun that req names one step further, and requeues
// until the run has finished.
//
// A run goes through three stages, chosen by status.phase. With no phase, plan
// lists the items to back up. In Queued, admit waits for the namespace's
// LocalQueue to admit the run. From then on, work stops the workloads marked
// for quiesce (on a run with spec.all set), starts every item, starts the
// workloads again once the clones are cut, and collects each item's result.
// Each stage acts only on what the last one wrote to the status, so a
// reconcile that runs twice from the same status does the same thing twice.
//
// Before any stage, Reconcile adds the run's finalizer. A run being deleted
// gets its changes put back by finalize, and a finished run is deleted once
// spec.ttlSecondsAfterFinished has passed. Whenever the Ready reason changes
// during a reconcile, Reconcile records an event on the run.
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Backup") }()
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

// plan records one Pending item for each thing the run backs up and moves the
// run to Queued. When items returns a refusal, plan ends the run as Failed
// with reason Invalid and the refusal as the message. Any other error, such as
// a timeout from the API server, is returned so the reconcile runs again.
func (r *BackupRunReconciler) plan(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	items, err := r.items(ctx, run)
	if err != nil {
		if !isRefusal(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	run.Status.Items = items
	run.Status.Phase = backupv1alpha1.RunPhaseQueued
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonQueued,
		"waiting for the backup queue to admit the run")
	return after(time.Second, r.writeStatus(ctx, run))
}

// items returns one Pending item for each thing the run's spec names.
// spec.source names one claim, spec.database names one Cluster, and a run
// with neither takes every claim and Cluster in the namespace. A claim's item
// has kind ReplicationSource, after the VolSync object that backs it up.
//
// Everything the run backs up has to be marked backup.wlz.li/enabled: "true",
// so a run and a schedule cover the same set. items returns a refusal when a
// named claim or Cluster is missing or not marked, and when nothing in the
// namespace is marked. A cluster without the CloudNativePG CRDs holds no
// Cluster. Any other failed read comes back as a plain error.
func (r *BackupRunReconciler) items(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	pending := func(kind, name string) backupv1alpha1.BackupItem {
		return backupv1alpha1.BackupItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}

	switch {
	case run.Spec.Source != "":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}, claim); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, refuse("no claim %s in this namespace", run.Spec.Source)
			}
			return nil, fmt.Errorf("get claim %s: %w", run.Spec.Source, err)
		}
		if !backupv1alpha1.Enabled(claim.Annotations) {
			return nil, refuse("claim %s is not marked %s: \"true\"", claim.Name, backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("ReplicationSource", claim.Name)}, nil

	case run.Spec.Database != "":
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, run.Spec.Database)
		if err != nil {
			if meta.IsNoMatchError(err) {
				return nil, refuse("no Cluster %s in this namespace; the cluster has no CloudNativePG CRDs", run.Spec.Database)
			}
			return nil, err
		}
		if !found {
			return nil, refuse("no Cluster %s in this namespace", run.Spec.Database)
		}
		if !backupv1alpha1.Enabled(cluster.GetAnnotations()) {
			return nil, refuse("the Cluster %s is not marked %s: \"true\"", cluster.GetName(), backupv1alpha1.AnnotationEnabled)
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
			return nil, refuse("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
		}
		return items, nil
	}
}

// admit waits for the namespace's LocalQueue to admit the run, then moves the
// run to Running and records status.startedAt. A namespace with no LocalQueue
// starts the run at once.
//
// The run goes through Kueue as one Workload, which counts as one pod against
// the queue's quota. Once Kueue admits it, admit marks the Workload PodsReady,
// so that Kueue's waitForPodsReady does not evict it. While the Workload
// waits, admit requeues after pollInterval.
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

	// A backup.wlz.li/timeout on the namespace that does not parse fails the
	// run here, before it stops or starts anything, because the run would
	// have no deadline to keep.
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

// work makes one pass over a run that the queue has admitted, and returns when
// to look again.
//
// On a run with spec.all set, the first pass calls quiesce to stop the
// workloads marked backup.wlz.li/quiesce and does nothing else. Later passes
// start no item until every pod of those workloads is gone. work then starts
// every Pending item. Once every volume's clone is cut, it records the time
// of the pass in status.restartedAt with status.restartPending set, and
// writes the status. It then scales the workloads back up, resumes the
// Kustomizations it suspended, and clears status.restartPending. A pass that
// finds status.restartPending set repeats the restart and keeps the recorded
// moment. Times in the status are whole seconds. Last, it collects the result
// of every Running item.
//
// The run finishes once every item is done and, on a run with spec.all set,
// the workloads are running again. It finishes Succeeded when no item failed
// and Failed otherwise. A run past its timeout is aborted. While the
// workloads are stopped, work looks again every two seconds; otherwise it
// waits pollInterval.
func (r *BackupRunReconciler) work(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	// The status keeps times in whole seconds. A snapshot moved in this pass
	// must carry the restartedAt that later passes read back.
	now := metav1.NewTime(r.Now()).Rfc3339Copy()
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
		message, err := r.startItem(ctx, run, item)
		if err != nil {
			return ctrl.Result{}, err
		}
		if message != "" {
			waiting = message
		}
	}

	if run.Spec.All && run.Status.RestartedAt == nil && r.clonesCut(ctx, run) {
		// The moment is written before the workloads start, so a pass that
		// starts them and then loses its status write is retried with it.
		run.Status.RestartedAt, run.Status.RestartPending = newTime(now), true
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if run.Status.RestartPending {
		if err := restartWorkloads(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
			return ctrl.Result{}, err
		}
		run.Status.RestartPending = false
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
		// The app stays down until every clone is cut, so the run looks
		// more often here than in its other waits.
		interval = 2 * time.Second
	}
	return after(interval, r.writeStatus(ctx, run))
}

// quiesce stops the workloads in the run's namespace that are marked
// backup.wlz.li/quiesce: "true", before the run does anything else. It stores
// its now argument, the time of this pass, in status.quiescedAt.
//
// It first records the plan from planStop in status.quiesced and
// status.suspendedKustomizations, with each workload's replica count, and
// writes the status. Only then does applyStop suspend the Kustomizations and
// scale the workloads to zero. A pass that finds a plan in the status reuses
// it, so a retry after a lost status write still gives back the counts the
// workloads had before the run touched them. quiesce then writes
// status.quiescedAt. After a failed stop, it narrows the plan with
// appliedPart to what is stopped now, and aborts the run, which puts that
// back. When no workload is marked, quiesce sets status.restartedAt to
// the same moment, because there is nothing to start again.
func (r *BackupRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.BackupRun, now metav1.Time) (ctrl.Result, error) {
	if len(run.Status.Quiesced) == 0 {
		// A volume still busy with another run's backup would keep the
		// stopped workloads down for as long as that backup takes. The run
		// waits with the workloads still running.
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
		if len(targets) == 0 {
			run.Status.QuiescedAt, run.Status.RestartedAt = newTime(now), newTime(now)
			return after(time.Second, r.writeStatus(ctx, run))
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
	run.Status.QuiescedAt = newTime(now)
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if stopErr != nil {
		return ctrl.Result{}, r.abort(ctx, run, stopErr.Error())
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// startItem starts the backup of one Pending item and sets the item's phase.
//
// For a volume, it writes the claim's ReplicationSource with the run's manual
// trigger tag and moves the item to Running. For a database, it creates a
// CloudNativePG Backup and moves the item to Running, or skips the item when
// the Cluster is hibernated. When the claim or the Cluster is gone, or the
// claim's settings are refused, startItem marks the item Failed with the
// reason in its message. The same goes for a Backup the API server rejects as
// invalid.
//
// It returns a message for the run's Ready condition when the item has to
// wait, which happens when the volume's ReplicationSource is still completing
// another run's backup. The item then stays Pending. Otherwise it returns an
// empty string. Any other failed read or write, such as a timeout from the
// API server, comes back as an error with the item left Pending, and the
// caller returns it so the reconcile runs again.
func (r *BackupRunReconciler) startItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) (string, error) {
	switch item.Kind {
	case "ReplicationSource":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, claim); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", fmt.Errorf("get claim %s: %w", item.Name, err)
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the claim %s no longer exists", item.Name)
			return "", nil
		}
		tag := TriggerFor(run.UID)
		_, err := ensureSource(ctx, r.Client, r.Reader, claim, tag)
		if errors.Is(err, errSourceBusy) {
			return fmt.Sprintf("ReplicationSource %s is still completing another run's backup", item.Name), nil
		}
		if err != nil {
			if !isRefusal(err) {
				return "", err
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return "", nil
		}
		item.Phase, item.Trigger = backupv1alpha1.ItemRunning, tag

	case "Cluster":
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		switch {
		case err != nil:
			return "", err
		case !found:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the Cluster %s no longer exists", item.Name)
		case hibernated(cluster):
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"
		default:
			name, err := ensureBackup(ctx, r.Client, run.Namespace, item.Name, run.UID)
			if err != nil {
				if !apierrors.IsInvalid(err) {
					return "", err
				}
				item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
				return "", nil
			}
			item.Phase, item.Backup = backupv1alpha1.ItemRunning, name
		}
	}
	return "", nil
}

// clonesCut reports whether VolSync has cut the clone of every volume the run
// backs up. That is the moment the stopped workloads can start again, because
// VolSync uploads from the clone and no longer reads the app's volume.
//
// A clone counts as cut when the claim volsync-<claim>-src is Bound, is not
// being deleted, and was created after status.quiescedAt. VolSync marks a sync
// done before its cleanup deletes the clone, so the clone of the previous
// sync can still be there when this run starts, and it holds the data from
// before the app stopped. This run writes its trigger at least one pass after
// quiescedAt, so its own clone is always newer.
//
// A ReplicationSource that has already completed the run's trigger tag has
// cut its clone too. That check catches a clone VolSync created and deleted
// between two passes. A Pending volume item means no clone yet, and items
// that failed or were skipped are left out.
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
		if !clone.DeletionTimestamp.IsZero() || run.Status.QuiescedAt == nil || !clone.CreationTimestamp.After(run.Status.QuiescedAt.Time) {
			return false
		}
	}
	return true
}

// collectItem records the result of a Running item once it has one, and
// leaves the item Running until then.
//
// A volume item is done when its ReplicationSource has completed the run's
// trigger tag. VolSync completes a tag only after a mover Job succeeds. A Job
// that fails leaves the tag open: VolSync writes the logs into
// status.latestMoverStatus, deletes the Job and starts another, for as long
// as the tag stays. So while the tag is open, a Failed result that
// moverFailed places in this run's sync fails the item with the mover's
// logs, and collectItem deletes the source to stop the retries. The next run
// writes a fresh source, where the dead tag would have kept it waiting with
// reason SourceBusy. When the delete fails, the item's message says so. A
// volume with no files succeeds with Empty set, since
// VolSync takes no snapshot of it. Otherwise the item records the snapshot ID
// the mover logged and the time restic stamped on it. On a quiesced run, the
// snapshot is first moved to status.restartedAt and tagged quiesced, and the
// item stays Running until that rewrite succeeds.
//
// A database item follows the phase of its CloudNativePG Backup.
func (r *BackupRunReconciler) collectItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	switch item.Kind {
	case "ReplicationSource":
		source := &volsyncv1alpha1.ReplicationSource{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
			return
		}
		if lastManual(source) != item.Trigger {
			if logs, failed := moverFailed(source, item.Trigger, run.Status.StartedAt); failed {
				// VolSync retries a failed Job forever and keeps the trigger
				// until one succeeds. Deleting the source stops the retries,
				// and the next run writes a fresh one.
				item.Phase, item.Message = backupv1alpha1.ItemFailed, logs
				if err := r.Delete(ctx, source); err != nil && !apierrors.IsNotFound(err) {
					item.Message += fmt.Sprintf("\nthe ReplicationSource could not be deleted, so VolSync keeps retrying it and later runs wait for it: %v", err)
				}
			}
			return
		}
		if source.Status.LatestMoverStatus != nil && source.Status.LatestMoverStatus.Result == volsyncv1alpha1.MoverResultFailed {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, source.Status.LatestMoverStatus.Logs
			return
		}
		snapshot, empty := moverOutcome(source)
		if empty {
			item.Phase, item.Empty = backupv1alpha1.ItemSucceeded, true
			item.Message = "the volume held no files, so VolSync took no snapshot"
			return
		}
		if quiesced(run) {
			// The item stays Running until the rewrite goes through. That way
			// the run never reports success for a snapshot that a synced
			// restore cannot use.
			moved, err := r.retime(ctx, run.Namespace, item.Name, snapshot, run.Status.RestartedAt.Time)
			if err != nil {
				item.Message = fmt.Sprintf("snapshot %s is saved and waits to be moved to %s and tagged %s: %v",
					snapshot, run.Status.RestartedAt.UTC().Format(time.RFC3339), restic.QuiescedTag, err)
				return
			}
			item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
			item.Snapshot, item.SnapshotTime = moved.ShortID(), newTime(metav1.NewTime(moved.Time))
			return
		}
		item.Phase, item.Snapshot = backupv1alpha1.ItemSucceeded, snapshot
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

// quiesced reports whether the run stopped at least one workload and has
// started the workloads again. Only such a run has a moment when nothing wrote
// to the volumes or the databases, which is what its snapshots are moved to.
func quiesced(run *backupv1alpha1.BackupRun) bool {
	return run.Spec.All && len(run.Status.Quiesced) > 0 && run.Status.RestartedAt != nil
}

// retime moves the snapshot a mover saved to a new time and tags it quiesced,
// so a RestoreRun with syncDatabaseToVolume can find it.
//
// Parameters:
//   - namespace and claimName name the claim that was backed up. retime reads
//     the repository Secret through the claim's VolumeRestore.
//   - short is the snapshot ID the mover logged, eight hex characters or
//     more. It is empty when the mover logged no snapshot.
//   - at is the time the snapshot should carry. collectItem passes the run's
//     status.restartedAt.
//
// It returns the rewritten snapshot, which has a new ID. It returns an error
// when the mover logged no snapshot, when the reconciler has no Retimer, when
// the Secret can't be read, and when the rewrite fails. A *restic.LockedError
// means another process holds a lock on the repository, and the caller tries
// again on its next pass.
func (r *BackupRunReconciler) retime(ctx context.Context, namespace, claimName, short string, at time.Time) (restic.Snapshot, error) {
	if short == "" || r.Retimer == nil {
		return restic.Snapshot{}, fmt.Errorf("the mover logged no snapshot")
	}
	secret, err := r.repositorySecret(ctx, namespace, claimName)
	if err != nil {
		return restic.Snapshot{}, err
	}
	return r.Retimer.Retime(ctx, secret, short, at, restic.QuiescedTag)
}

// repositorySecret reads the Secret holding the restic repository settings
// for the claim named claimName. It finds the Secret's name in
// spec.repository of the claim's VolumeRestore.
func (r *BackupRunReconciler) repositorySecret(ctx context.Context, namespace, claimName string) (*corev1.Secret, error) {
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
	return secret, nil
}

// snapshotTime reads the time restic stamped on a snapshot a mover saved. It
// lists the repository of the claim named claimName and looks for the
// snapshot whose ID starts with the prefix in short. It returns an error when
// that prefix is empty, when the reconciler has no Snapshots lister, and when
// the repository holds no such snapshot.
func (r *BackupRunReconciler) snapshotTime(ctx context.Context, namespace, claimName, short string) (*metav1.Time, error) {
	if short == "" || r.Snapshots == nil {
		return nil, fmt.Errorf("the mover logged no snapshot")
	}
	secret, err := r.repositorySecret(ctx, namespace, claimName)
	if err != nil {
		return nil, err
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

// abort ends a run early as Failed. It marks every Pending or Running item as
// Failed with the given message, then calls finish, which starts the stopped
// workloads again and deletes the run's Workload so the queue gets its slot
// back.
func (r *BackupRunReconciler) abort(ctx context.Context, run *backupv1alpha1.BackupRun, message string) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
	return r.finish(ctx, run, backupv1alpha1.ReasonFailed, message)
}

// finish ends the run. It starts any workload the run still holds stopped,
// deletes the run's Workload, and records the terminal phase: Succeeded when
// reason is ReasonSucceeded and Failed for any other reason. It sets the
// Ready condition to reason and message, records status.completedAt, and
// removes the finalizer.
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

// release puts back what the run changed in the cluster. When the run stopped
// workloads and has not started them again, release scales them back up,
// resumes the Kustomizations it suspended, and records status.restartedAt. A
// run that recorded its plan to stop them counts as having stopped them, even
// before status.quiescedAt is set, because the pass that wrote the plan may
// have stopped them and then lost its status write. release then deletes the
// run's Workload.
//
// A run with status.restartPending set has chosen its restart moment and may
// not have started the workloads yet. release starts them and keeps that
// moment.
func (r *BackupRunReconciler) release(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	holding := (run.Status.QuiescedAt != nil || len(run.Status.Quiesced) > 0) && run.Status.RestartedAt == nil
	if holding || run.Status.RestartPending {
		if err := restartWorkloads(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
			return err
		}
		if run.Status.RestartedAt == nil {
			run.Status.RestartedAt = newTime(metav1.NewTime(r.Now()).Rfc3339Copy())
		}
		run.Status.RestartPending = false
	}
	return deleteWorkload(ctx, r.Client, run.Namespace, run.UID)
}

// finalize puts back what a run changed when the run is deleted before it
// finished, then removes the finalizer so the deletion can complete.
func (r *BackupRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.release(ctx, run); err != nil {
		return err
	}
	return dropFinalizer(ctx, r.Client, run)
}

// overdue returns the run's deadline and reports whether the run has worked
// past it. The deadline is status.startedAt plus the timeout, and a run that
// has not started is never overdue.
//
// timeoutFor resolves the timeout again on every check, so a change to the
// namespace's backup.wlz.li/timeout during a run moves the run's deadline.
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

// waitFor moves the run to Waiting, sets its Ready condition to False with
// reason and message, and writes the status.
func (r *BackupRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

// writeStatus writes the run's status subresource.
func (r *BackupRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set BackupRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// anyPending reports whether any item is still Pending, which means the run
// has not started it yet.
func anyPending(items []backupv1alpha1.BackupItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending {
			return true
		}
	}
	return false
}

// allDone reports whether every item has left Pending and Running, so none
// has anything more to do.
func allDone(items []backupv1alpha1.BackupItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning {
			return false
		}
	}
	return true
}

// failures returns one line per failed item, naming its kind, its name and
// its message, joined with "; ". It returns an empty string when no item
// failed.
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

// summary returns the Ready message for a run that succeeded. It says for
// each item what it captured: the snapshot a volume saved, the Backup a
// database completed, or why the item was skipped or had nothing to save.
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

// dropFinalizer removes the run's finalizer, if it still has one. Callers
// call it last, once the run has put back everything it changed. Both
// BackupRuns and RestoreRuns use it.
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

// expire deletes a finished run once its time to live has passed, and
// requeues the run for that moment until then. Both BackupRuns and
// RestoreRuns use it.
//
// Parameters:
//   - run is the finished BackupRun or RestoreRun.
//   - ttl is the run's spec.ttlSecondsAfterFinished. When it is nil, the run
//     is kept for good.
//   - completed is the run's status.completedAt, which the time to live
//     counts from.
//   - now is the reconciler's current time.
//
// A run that is already gone when expire deletes it is not an error.
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
