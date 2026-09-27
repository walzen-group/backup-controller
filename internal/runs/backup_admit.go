package runs

import (
	"context"
	"errors"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
)

// admit waits for the namespace's LocalQueue to admit the run, then moves the
// run to Running and records status.startedAt. A namespace with no LocalQueue
// starts the run at once.
//
// The run goes through Kueue as one Workload, which counts as one pod against
// the queue's quota. Once Kueue admits it, admit marks the Workload PodsReady,
// so that Kueue's waitForPodsReady does not evict it. While the Workload
// waits, admit requeues after pollInterval, and a run Kueue does not admit
// within its timeout from its creation fails (see awaitAdmission).
func (r *BackupRunReconciler) admit(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	if done, result, err := r.admitThroughKueue(ctx, run); done {
		return result, err
	}
	if done, err := r.endIfSettingInvalid(ctx, run); done {
		return ctrl.Result{}, err
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = newTime(metav1.NewTime(r.Now()))
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "backing up")
	return after(time.Second, r.writeStatus(ctx, run))
}

// admitThroughKueue makes sure that the namespace's LocalQueue has admitted
// the run, if the namespace has a LocalQueue.
//
// Parameters:
//   - run is the Queued BackupRun. Its status.workload is set and written
//     when it does not name the run's Workload.
//
// It returns done false when the run can go on: the namespace has no
// LocalQueue, or Kueue admitted the Workload and admitThroughKueue marked it
// PodsReady. It returns done true, with the result and the error the pass
// returns, when the Workload waits (see awaitAdmission) or a call fails.
func (r *BackupRunReconciler) admitThroughKueue(ctx context.Context, run *backupv1alpha1.BackupRun) (done bool, result ctrl.Result, err error) {
	queue, err := kueue.LocalQueue(ctx, r.Reader, r.RESTMapper(), run.Namespace)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if queue == "" {
		return false, ctrl.Result{}, nil
	}
	workload, err := kueue.EnsureWorkload(ctx, r.Client, run, backupRunKind, queue)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if run.Status.Workload != workload.GetName() {
		run.Status.Workload = workload.GetName()
		if err := r.writeStatus(ctx, run); err != nil {
			return true, ctrl.Result{}, err
		}
	}
	if !kueue.Admitted(workload) {
		result, err := r.awaitAdmission(ctx, run, workload, queue)
		return true, result, err
	}
	if err := kueue.MarkPodsReady(ctx, r.Client, workload, metav1.NewTime(r.Now())); err != nil {
		return true, ctrl.Result{}, err
	}
	return false, ctrl.Result{}, nil
}

// endIfSettingInvalid ends the run when a namespace setting it needs does
// not parse, before the run stops or starts anything.
//
// Parameters:
//   - run is the admitted BackupRun.
//
// It returns done true when the pass must stop here: the run ended, or a
// read failed. The error is the error of the read or of abort.
//
// A backup.wlz.li/timeout that does not parse ends the run, because the
// run has no deadline to keep. On a run that stops workloads, a
// backup.wlz.li/max-quiesce that does not parse ends the run too, because
// it has no limit on how long the workloads stay down.
func (r *BackupRunReconciler) endIfSettingInvalid(ctx context.Context, run *backupv1alpha1.BackupRun) (done bool, err error) {
	if _, err := timeoutFor(ctx, r.Reader, run); err != nil {
		return true, r.abortOnInvalidSetting(ctx, run, err)
	}
	if !run.Spec.All {
		return false, nil
	}
	if _, err := maxQuiesceFor(ctx, r.Reader, run.Namespace); err != nil {
		return true, r.abortOnInvalidSetting(ctx, run, err)
	}
	return false, nil
}

// abortOnInvalidSetting ends the run as Failed when a namespace setting does
// not parse.
//
// Parameters:
//   - run is the BackupRun that reads the setting.
//   - err is the error of the read of the setting. It is not nil.
//
// When err is an invalidSettingError, it aborts the run with reason Failed
// and the error's text as the message, and returns the error of abort. It
// returns any other err as it is, for a retry.
func (r *BackupRunReconciler) abortOnInvalidSetting(ctx context.Context, run *backupv1alpha1.BackupRun, err error) error {
	var bad invalidSettingError
	if errors.As(err, &bad) {
		return r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
	}
	return err
}

// awaitAdmission keeps a run whose Workload Kueue has not admitted yet
// Queued, and fails it once it has waited longer than its timeout.
//
// Parameters:
//   - run is the Queued BackupRun.
//   - workload is the run's Workload, as kueue.EnsureWorkload read or created it.
//   - queue is the LocalQueue the Workload waits in.
//
// A run with no status.startedAt is never overdue (see overdue), and a
// namespace's schedule starts no run while one is unfinished, so a run that
// is never admitted would stop the namespace's backups for good without a
// word. That happens when a Kueue release renames the Workload's queueName
// field or its Admitted condition, and when the queue has no quota to give.
// The wait is bounded by the run's timeout (spec.timeout, the namespace's
// backup.wlz.li/timeout, or 6h), counted from the run's creation. Past it,
// the run ends Failed with a message that names Kueue, the Workload and the
// LocalQueue. Its items record the reason TimedOut (see endTimedOut), and
// finish deletes the Workload. Until then it returns a result that looks
// again after pollInterval. It returns an error when the timeout can't be
// read or ending the run fails.
func (r *BackupRunReconciler) awaitAdmission(ctx context.Context, run *backupv1alpha1.BackupRun, workload *unstructured.Unstructured, queue string) (ctrl.Result, error) {
	timeout, err := timeoutFor(ctx, r.Reader, run)
	if err != nil {
		return ctrl.Result{}, r.abortOnInvalidSetting(ctx, run, err)
	}
	deadline := run.CreationTimestamp.Add(timeout)
	if run.CreationTimestamp.IsZero() || r.Now().Before(deadline) {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	message := fmt.Sprintf("Kueue did not admit the run's Workload %s/%s in LocalQueue %s within the run's timeout of %s from its creation: "+
		"the Workload's status.conditions hold no Admitted condition with status True. Check the LocalQueue and its ClusterQueue; "+
		"if the queue has quota to give, a Kueue release may have changed the Workload's fields (see docs/compatibility.md)",
		workload.GetNamespace(), workload.GetName(), queue, timeout)
	return ctrl.Result{}, r.endTimedOut(ctx, run, message)
}
