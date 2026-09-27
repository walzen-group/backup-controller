package runs

import (
	"context"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// find the snapshot a volume's sync wrote, with the time restic stamped
	// on it.
	Snapshots SnapshotLister

	// Retimer rewrites a snapshot with a new time and a tag. A quiesced run
	// uses it to move each volume's snapshot to the moment the run started the
	// workloads again, and to tag it quiesced.
	Retimer SnapshotRetimer

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
// BackupRun. It sets Now to time.Now when the caller left it unset.
func (r *BackupRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	r.serve()
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
//
// An unfinished run with no spec.database that is not being deleted first
// goes through volsyncUnsupported. While VolSync serves its kinds only at a
// version other than v1alpha1, the run ends through endForVolSync with
// reason VolSyncUnsupported, which gives the app back: that needs no
// VolSync object. A run being deleted goes to finalize as usual.
//
// A run that recorded status.ending has decided to end, and every later pass
// only finishes it with that reason and message (see finish), also one that
// retries a failed restart after the run timed out.
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Backup") }()
	// A run that may touch VolSync objects can't go on while VolSync serves
	// its kinds only at a version this controller has no Go types for, and
	// ends. A database-only run needs no VolSync object and goes on.
	if !run.Status.Phase.Finished() && run.DeletionTimestamp.IsZero() && run.Spec.Database == "" {
		if unservedErr := volsyncSourceUnserved(r.RESTMapper()); unservedErr != nil {
			return ctrl.Result{}, r.endForVolSync(ctx, run, unservedErr)
		}
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
	if ending := run.Status.Ending; ending != nil {
		return ctrl.Result{}, r.finish(ctx, run, ending.Reason, ending.Message)
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
