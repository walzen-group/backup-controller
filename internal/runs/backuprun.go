package runs

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Finalizer holds a run open until the controller has removed whatever it put
// on the cluster.
//
// Without it, deleting a BackupRun mid-flight would leave its manual tag on the
// source, and a source holding a spent tag reports healthy while taking no
// further backups. That failure is silent and permanent, which is the whole
// reason these objects exist, so it must not be reachable by deleting one.
const Finalizer = "backup.wlz.li/run-cleanup"

// pollInterval is how often a waiting run looks again. The mover's own progress
// is not watched, so this is the resolution of every wait in this package.
const pollInterval = 15 * time.Second

// BackupRunReconciler runs one backup on demand.
type BackupRunReconciler struct {
	client.Client

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
// The whole state machine is a comparison of two fields on the source, the
// manual tag it carries and the tag it last completed, against the tag this run
// owns. Every branch is idempotent, so a reconcile that runs twice on the same
// state does the same thing twice with the same result.
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, run)
	}

	if run.Status.Phase.Finished() {
		return r.expire(ctx, run)
	}

	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		controllerutil.AddFinalizer(run, Finalizer)
		if err := r.Update(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer to BackupRun %s/%s: %w", run.Namespace, run.Name, err)
		}
	}

	source := &volsyncv1alpha1.ReplicationSource{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}
	if err := r.Get(ctx, key, source); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, r.fail(ctx, run, backupv1alpha1.ReasonInvalid,
				fmt.Sprintf("no ReplicationSource %s in this namespace", run.Spec.Source))
		}
		return ctrl.Result{}, fmt.Errorf("get ReplicationSource %s: %w", key, err)
	}

	tag := TriggerFor(run.UID)
	held := manualTag(source)

	switch {
	case held == "" || held != tag:
		// A tag belonging to something else is left alone. Writing over it
		// would take a backup the other holder never sees finish, and would
		// leave that holder unable to clear a field it no longer owns.
		if held != "" {
			return ctrl.Result{}, r.fail(ctx, run, backupv1alpha1.ReasonTriggerHeld, triggerHeldBy(source))
		}
		return ctrl.Result{RequeueAfter: pollInterval}, r.start(ctx, run, source, tag)

	case source.Status.LastManualSync != tag:
		if expired, deadline := r.timedOut(run); expired {
			if err := clearManualTrigger(ctx, r.Client, source); err != nil {
				return ctrl.Result{}, fmt.Errorf("clear the trigger on ReplicationSource %s: %w", key, err)
			}
			return ctrl.Result{}, r.fail(ctx, run, backupv1alpha1.ReasonTimedOut,
				fmt.Sprintf("the mover had not finished by %s", deadline.Format(time.RFC3339)))
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil

	default:
		return ctrl.Result{}, r.succeed(ctx, run, source)
	}
}

// start writes the tag and records that the run is under way.
func (r *BackupRunReconciler) start(ctx context.Context, run *backupv1alpha1.BackupRun, source *volsyncv1alpha1.ReplicationSource, tag string) error {
	if err := setManualTrigger(ctx, r.Client, source, tag); err != nil {
		return fmt.Errorf("set the trigger on ReplicationSource %s/%s: %w", source.Namespace, source.Name, err)
	}

	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.Trigger = tag
	if run.Status.StartedAt == nil {
		run.Status.StartedAt = &now
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("waiting for ReplicationSource %s/%s to complete %s", source.Namespace, source.Name, tag))
	return r.writeStatus(ctx, run)
}

// succeed clears the tag and records the snapshot the repository now holds.
func (r *BackupRunReconciler) succeed(ctx context.Context, run *backupv1alpha1.BackupRun, source *volsyncv1alpha1.ReplicationSource) error {
	if err := clearManualTrigger(ctx, r.Client, source); err != nil {
		return fmt.Errorf("clear the trigger on ReplicationSource %s/%s: %w", source.Namespace, source.Name, err)
	}

	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	run.Status.CompletedAt = &now
	run.Status.SnapshotTime = source.Status.LastSyncTime
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, backupv1alpha1.ReasonSucceeded,
		fmt.Sprintf("ReplicationSource %s/%s completed %s", source.Namespace, source.Name, run.Status.Trigger))
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	return r.release(ctx, run)
}

// fail records a terminal failure. Callers clear anything they created first.
func (r *BackupRunReconciler) fail(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseFailed
	run.Status.CompletedAt = &now
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, reason, message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	return r.release(ctx, run)
}

// finalize clears the tag on the way to deletion, so a run deleted while it
// works takes its trigger with it.
func (r *BackupRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}

	if run.Status.Trigger != "" {
		source := &volsyncv1alpha1.ReplicationSource{}
		key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}
		switch err := r.Get(ctx, key, source); {
		case apierrors.IsNotFound(err):
			// The source is gone, so there is no tag left to clear.
		case err != nil:
			return fmt.Errorf("get ReplicationSource %s: %w", key, err)
		case manualTag(source) == run.Status.Trigger:
			if err := clearManualTrigger(ctx, r.Client, source); err != nil {
				return fmt.Errorf("clear the trigger on ReplicationSource %s: %w", key, err)
			}
		}
	}

	return r.release(ctx, run)
}

// release drops the finalizer once nothing of this run is left on the cluster.
func (r *BackupRunReconciler) release(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(run, Finalizer)
	if err := r.Update(ctx, run); err != nil {
		return fmt.Errorf("remove the finalizer from BackupRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// expire deletes a finished run once its TTL has passed, and requeues until
// then. A run with no TTL is kept, which is the default: whether the backup
// before a restore actually finished is worth being able to answer later.
func (r *BackupRunReconciler) expire(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	if run.Spec.TTLSecondsAfterFinished == nil || run.Status.CompletedAt == nil {
		return ctrl.Result{}, nil
	}

	deadline := run.Status.CompletedAt.Add(time.Duration(*run.Spec.TTLSecondsAfterFinished) * time.Second)
	if remaining := deadline.Sub(r.Now()); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	if err := r.Delete(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete the expired BackupRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	return ctrl.Result{}, nil
}

// timedOut reports whether the mover has run past the run's deadline.
func (r *BackupRunReconciler) timedOut(run *backupv1alpha1.BackupRun) (bool, time.Time) {
	if run.Status.StartedAt == nil || run.Spec.Timeout == nil {
		return false, time.Time{}
	}
	deadline := run.Status.StartedAt.Add(run.Spec.Timeout.Duration)
	return !r.Now().Before(deadline), deadline
}

func (r *BackupRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set BackupRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}
