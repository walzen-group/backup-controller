package runs

import (
	"context"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// FieldOwner is the field manager name, backupv1alpha1.FieldManager, of
// the run controllers. The quiesce package sends the same name with its
// writes to workloads and Kustomizations.
const FieldOwner = client.FieldOwner(backupv1alpha1.FieldManager)

// Finalizer keeps a deleted run in place until the controller has put back
// whatever the run changed: a stopped workload, a suspended Kustomization, or
// a restore Job it created.
const Finalizer = "backup.wlz.li/run-cleanup"

// pollInterval is how long a waiting run waits before it checks again. The
// controller doesn't watch the movers' progress, so this interval sets how
// soon a run notices that a mover has finished.
const pollInterval = 10 * time.Second

// after returns a result that requeues the run once the duration d has
// passed. When err is set, it returns only err, with an empty result, because
// controller-runtime ignores a requeue that comes with an error and logs a
// warning about the pair.
func after(d time.Duration, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: d}, nil
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

// runFields points at the fields that a BackupRun and a RestoreRun both
// have, so that one function handles a run of either kind. fieldsOf makes
// it.
type runFields struct {
	// Object is the run itself.
	client.Object

	// kind is backupv1alpha1.KindBackupRun or KindRestoreRun.
	kind string

	phase       *backupv1alpha1.RunPhase
	conditions  *[]metav1.Condition
	resumedAt   **metav1.Time
	completedAt **metav1.Time
	quiescedAt  **metav1.Time
	restartedAt **metav1.Time
	quiesced    *[]backupv1alpha1.QuiescedWorkload
	suspended   *[]string
	ending      **backupv1alpha1.RunEnding

	// restartPending is status.restartPending of a BackupRun. A RestoreRun
	// has no such field, and its restartPending points at a false that
	// nothing reads back.
	restartPending *bool
}

// fieldsOf returns the shared fields of run, which is a *BackupRun or a
// *RestoreRun. For any other object it returns runFields with only Object
// set.
func fieldsOf(run client.Object) runFields {
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		s := &r.Status
		return runFields{Object: r, kind: backupv1alpha1.KindBackupRun, phase: &s.Phase, conditions: &s.Conditions,
			resumedAt: &s.ResumedAt, completedAt: &s.CompletedAt, quiescedAt: &s.QuiescedAt, restartedAt: &s.RestartedAt,
			quiesced: &s.Quiesced, suspended: &s.SuspendedKustomizations, ending: &s.Ending, restartPending: &s.RestartPending}
	case *backupv1alpha1.RestoreRun:
		s := &r.Status
		return runFields{Object: r, kind: backupv1alpha1.KindRestoreRun, phase: &s.Phase, conditions: &s.Conditions,
			resumedAt: &s.ResumedAt, completedAt: &s.CompletedAt, quiescedAt: &s.QuiescedAt, restartedAt: &s.RestartedAt,
			quiesced: &s.Quiesced, suspended: &s.SuspendedKustomizations, ending: &s.Ending, restartPending: new(bool)}
	}
	return runFields{Object: run}
}

// emptyLike returns a new, empty run of the same kind as run, for a read
// that must not keep a field the stored run leaves out.
func emptyLike(run client.Object) client.Object {
	if _, ok := run.(*backupv1alpha1.RestoreRun); ok {
		return &backupv1alpha1.RestoreRun{}
	}
	return &backupv1alpha1.BackupRun{}
}

// runOps holds what the steps both reconcilers share need: the client that
// writes, the uncached Reader and the clock.
type runOps struct {
	c      client.Client
	reader client.Reader
	now    func() time.Time
}

// moment returns the time of the pass in whole seconds, as a status field
// stores it.
func (o runOps) moment() *metav1.Time {
	return newTime(metav1.NewTime(o.now()).Rfc3339Copy())
}

// writeStatus writes the run's status subresource.
func (o runOps) writeStatus(ctx context.Context, f runFields) error {
	if err := o.c.Status().Update(ctx, f.Object); err != nil {
		return fmt.Errorf("set %s %s/%s status: %w", f.kind, f.GetNamespace(), f.GetName(), err)
	}
	return nil
}

// waitFor moves the run to Waiting, sets its Ready condition to False with
// reason and message, and writes the status.
func (o runOps) waitFor(ctx context.Context, f runFields, reason, message string) error {
	*f.phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(f.conditions, f.GetGeneration(), metav1.ConditionFalse, reason, message)
	return o.writeStatus(ctx, f)
}

// waitOn ends a pass that has to wait for what a hold names, or that failed
// before it.
//
// Parameters:
//   - f is the run. Its status is written when it waits.
//   - h is the hold of the check. waitOn reads it only when err is nil.
//   - err is the error of the check, or nil.
//
// It returns done false when err is nil and h holds nothing. Otherwise it
// returns done true with err, or with the wait: the run is Waiting with the
// hold's reason and text (see waitFor), and looks again after pollInterval.
func (o runOps) waitOn(ctx context.Context, f runFields, h hold, err error) (done bool, result ctrl.Result, _ error) {
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if !h.held() {
		return false, ctrl.Result{}, nil
	}
	result, err = after(pollInterval, o.waitFor(ctx, f, h.readyReason(), h.text))
	return true, result, err
}

// end records the end of a run that has given back what it held, and lets
// the run go.
//
// Parameters:
//   - f is the run, with status.ending recorded. It ends Succeeded when the
//     ending's reason is Succeeded, and Failed for any other reason.
//
// It returns the error of the status write or of the finalizer's removal.
//
// It sets the phase, status.completedAt and the Ready condition from the
// ending, and writes the status. The stored status then shows the workloads
// back, so the namespace's quiesce Lease may go, and it goes before the
// finalizer: another run then takes the namespace over at once, and a run
// whose finalizer is gone while it holds the Lease would keep other runs out
// until the Lease reads as stale. The release is best effort: a Lease left
// behind is stale under holderLive's rule. Last, end removes the finalizer.
func (o runOps) end(ctx context.Context, f runFields) error {
	ending := **f.ending
	*f.phase = backupv1alpha1.RunPhaseSucceeded
	if ending.Reason != backupv1alpha1.ReasonSucceeded {
		*f.phase = backupv1alpha1.RunPhaseFailed
	}
	*f.completedAt = newTime(metav1.NewTime(o.now()))
	backupv1alpha1.SetReady(f.conditions, f.GetGeneration(), metav1.ConditionTrue, ending.Reason, ending.Message)
	if err := o.writeStatus(ctx, f); err != nil {
		return err
	}
	releaseQuiesceLeases(ctx, o.c, o.reader, f.Object)
	return dropFinalizer(ctx, o.c, f.Object)
}

// finalized lets a deleted run go once it has given back what it held: it
// removes the finalizer, then releases the namespace's quiesce Lease.
//
// The Lease goes after the finalizer, not before. A run being deleted does
// not store its restart in its status, so when the removal fails, the next
// pass restarts the app again. Released before that, the Lease would let
// another run stop the app in between, and the repeated restart would undo
// that stop. The release is best effort: a Lease left behind is stale under
// holderLive's rule.
func (o runOps) finalized(ctx context.Context, f runFields) error {
	if err := dropFinalizer(ctx, o.c, f.Object); err != nil {
		return err
	}
	releaseQuiesceLeases(ctx, o.c, o.reader, f.Object)
	return nil
}

// releaseFailed reports on the run that it could not put back what it
// changed or release what it holds, and returns err, so the reconcile runs
// again with controller-runtime's backoff.
//
// Parameters:
//   - f is the run. Its Ready condition is set and its status written.
//   - err is the error of the restart or of the release step.
//   - plan says what the run is doing: working, appDown and scheduled.
//     releaseFailed fills in the workloads, the Kustomizations, the kind and
//     the deletion from the run.
//
// The Ready condition takes the reason and the message from releaseFailure.
// Announce turns either reason into a Warning event. The status write is
// best effort: a write that fails is made again by the next pass that
// fails. The run never gives up. A run that finished while it still owed a
// restart would lose the only record of the replicas the app had.
func (o runOps) releaseFailed(ctx context.Context, f runFields, err error, plan releasePlan) error {
	plan.stopped, plan.suspended, plan.kind = *f.quiesced, *f.suspended, f.kind
	plan.deleting = !f.GetDeletionTimestamp().IsZero()
	reason, message := releaseFailure(err, plan)
	backupv1alpha1.SetReady(f.conditions, f.GetGeneration(), metav1.ConditionFalse, reason, message)
	_ = o.writeStatus(ctx, f)
	return err
}

// passSteps are the steps of a pass that differ between a BackupRun and a
// RestoreRun.
type passSteps struct {
	// paused is true when the controller runs with --pause.
	paused bool
	// isNew is true when the run has started no work (see backupRunNew and
	// restoreRunNew).
	isNew bool
	// ttl is the run's spec.ttlSecondsAfterFinished.
	ttl *int32
	// finalize gives back what a deleted run holds and lets it go.
	finalize func(ctx context.Context) (ctrl.Result, error)
	// finish ends the run with the reason and the message.
	finish func(ctx context.Context, reason, message string) (ctrl.Result, error)
	// work moves an unfinished run with no ending one step further.
	work func(ctx context.Context) (ctrl.Result, error)
}

// pass moves a run one step further, the same way for both kinds of run.
//
// Parameters:
//   - f is the run as the pass read it.
//   - steps are the steps that differ between the two kinds.
//
// It returns what the step it takes returns.
//
// A new run waits while the controller runs with --pause, and a run that
// waited records the end of the pause (see runOps.pause). A run being
// deleted gets its changes put back by steps.finalize, and a finished run is
// deleted once its time to live has passed (see expire). Any other run gets
// its finalizer first. A run that recorded status.ending has decided to end,
// and every later pass only finishes it with that reason and message
// (steps.finish), also one that waits for a stopped mover or retries a
// failed restart. Any other run goes on in steps.work.
func (o runOps) pass(ctx context.Context, f runFields, steps passSteps) (ctrl.Result, error) {
	if held, err := o.pause(ctx, f, steps.paused, steps.isNew); held || err != nil {
		return ctrl.Result{}, err
	}
	if !f.GetDeletionTimestamp().IsZero() {
		return steps.finalize(ctx)
	}
	if f.phase.Finished() {
		return expire(ctx, o.c, f.Object, steps.ttl, *f.completedAt, o.now())
	}
	if controllerutil.AddFinalizer(f.Object, Finalizer) {
		if err := o.c.Update(ctx, f.Object); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer to %s %s/%s: %w", f.kind, f.GetNamespace(), f.GetName(), err)
		}
	}
	if ending := *f.ending; ending != nil {
		return steps.finish(ctx, ending.Reason, ending.Message)
	}
	return steps.work(ctx)
}
