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
	queue, err := kueue.LocalQueue(ctx, r.Reader, r.RESTMapper(), run.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if queue != "" {
		workload, err := kueue.EnsureWorkload(ctx, r.Client, run, backupRunKind, queue)
		if err != nil {
			return ctrl.Result{}, err
		}
		if run.Status.Workload != workload.GetName() {
			run.Status.Workload = workload.GetName()
			if err := r.writeStatus(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
		}
		if !kueue.Admitted(workload) {
			return r.awaitAdmission(ctx, run, workload, queue)
		}
		if err := kueue.MarkPodsReady(ctx, r.Client, workload, metav1.NewTime(r.Now())); err != nil {
			return ctrl.Result{}, err
		}
	}

	// A backup.wlz.li/timeout on the namespace that does not parse fails the
	// run here, before it stops or starts anything, because the run would
	// have no deadline to keep.
	if _, err := timeoutFor(ctx, r.Reader, run); err != nil {
		var bad invalidSettingError
		if errors.As(err, &bad) {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
		}
		return ctrl.Result{}, err
	}
	// The same goes for a backup.wlz.li/max-quiesce that does not parse on a
	// run that stops workloads: it would have no limit on how long they stay
	// down.
	if run.Spec.All {
		if _, err := maxQuiesceFor(ctx, r.Reader, run.Namespace); err != nil {
			var bad invalidSettingError
			if errors.As(err, &bad) {
				return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
			}
			return ctrl.Result{}, err
		}
	}

	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = newTime(metav1.NewTime(r.Now()))
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "backing up")
	return after(time.Second, r.writeStatus(ctx, run))
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
// LocalQueue, and finish deletes the Workload. Until then it returns a
// result that looks again after pollInterval. It returns an error when the
// timeout can't be read or ending the run fails.
func (r *BackupRunReconciler) awaitAdmission(ctx context.Context, run *backupv1alpha1.BackupRun, workload *unstructured.Unstructured, queue string) (ctrl.Result, error) {
	timeout, err := timeoutFor(ctx, r.Reader, run)
	if err != nil {
		var bad invalidSettingError
		if errors.As(err, &bad) {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
		}
		return ctrl.Result{}, err
	}
	deadline := run.CreationTimestamp.Add(timeout)
	if run.CreationTimestamp.IsZero() || r.Now().Before(deadline) {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	message := fmt.Sprintf("Kueue did not admit the run's Workload %s/%s in LocalQueue %s within the run's timeout of %s from its creation: "+
		"the Workload's status.conditions hold no Admitted condition with status True. Check the LocalQueue and its ClusterQueue; "+
		"if the queue has quota to give, a Kueue release may have changed the Workload's fields (see docs/compatibility.md)",
		workload.GetNamespace(), workload.GetName(), queue, timeout)
	return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, message)
}
