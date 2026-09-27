package runs

import (
	"context"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
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

	// Paused is true when the controller runs with --pause. A new run then
	// waits with reason Paused and starts no work (see runOps.pause).
	Paused bool

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
	r.serve()
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.BackupRun{}).
		Named("backuprun").
		Complete(r)
}

// Reconcile moves the BackupRun that req names one step further (see
// runOps.pass), and requeues until the run has finished. Whenever the Ready
// reason changes during a reconcile, Reconcile records an event on the run.
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Backup") }()
	return r.ops().pass(ctx, fieldsOf(run), passSteps{
		paused: r.Paused, isNew: backupRunNew(run), ttl: run.Spec.TTLSecondsAfterFinished,
		finalize: func(ctx context.Context) (ctrl.Result, error) { return ctrl.Result{}, r.finalize(ctx, run) },
		finish: func(ctx context.Context, reason, message string) (ctrl.Result, error) {
			return ctrl.Result{}, r.finish(ctx, run, reason, message)
		},
		work: func(ctx context.Context) (ctrl.Result, error) { return r.step(ctx, run) },
	})
}

// step moves an unfinished BackupRun with no ending one stage further.
//
// A run goes through three stages, chosen by status.phase. With no phase, plan
// lists the items to back up. In Queued, admit waits for the namespace's
// LocalQueue to admit the run. From then on, work stops the workloads marked
// for quiesce (on a run with spec.all set), starts every item, starts the
// workloads again once the clones are cut, and collects each item's result.
// Each stage acts only on what the last one wrote to the status, so a
// reconcile that runs twice from the same status does the same thing twice.
//
// A run with no spec.database first goes through volsyncSourceUnserved.
// While VolSync serves its kinds only at a version other than v1alpha1, the
// run ends through endForVolSync with reason VolSyncUnsupported, which gives
// the app back: that needs no VolSync object. A database-only run needs no
// VolSync object and goes on.
func (r *BackupRunReconciler) step(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	if run.Spec.Database == "" {
		if unservedErr := volsyncSourceUnserved(r.RESTMapper()); unservedErr != nil {
			return ctrl.Result{}, r.endForVolSync(ctx, run, unservedErr)
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
	return r.ops().waitFor(ctx, fieldsOf(run), reason, message)
}

// writeStatus writes the run's status subresource.
func (r *BackupRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	return r.ops().writeStatus(ctx, fieldsOf(run))
}
