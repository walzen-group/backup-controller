package runs

import (
	"context"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// pausedMessage is the Ready message of a new run that waits for the end of
// the pause.
const pausedMessage = "the controller runs with --pause; this run starts when the controller runs without it"

// backupRunNew reports whether a BackupRun has started no work.
//
// Parameters:
//   - run is the BackupRun that the pass read.
//
// It returns true when the run is not deleted, has no ending recorded, and
// either has no phase or is Queued without a Kueue Workload. Such a run has
// taken no Lease, stopped no workload and created no object. The Workload
// is the first object that admit creates, so a Queued run that names one is
// in progress.
func backupRunNew(run *backupv1alpha1.BackupRun) bool {
	if !run.DeletionTimestamp.IsZero() || run.Status.Ending != nil {
		return false
	}
	if run.Status.StartedAt != nil || len(run.Status.Quiesced) > 0 {
		return false
	}
	switch run.Status.Phase {
	case "":
		return true
	case backupv1alpha1.RunPhaseQueued:
		return run.Status.Workload == ""
	default:
		return false
	}
}

// restoreRunNew reports whether a RestoreRun has started no work.
//
// Parameters:
//   - run is the RestoreRun that the pass read.
//
// It returns true when the run is not deleted, has no ending recorded, has
// no phase, and records no start and no stopped workload. The plan sets the
// phase in the same status write that records the first item it starts, so
// a run with no phase has created no restore Job, claim or Lease.
func restoreRunNew(run *backupv1alpha1.RestoreRun) bool {
	return run.DeletionTimestamp.IsZero() && run.Status.Ending == nil && run.Status.Phase == "" &&
		run.Status.StartedAt == nil && len(run.Status.Quiesced) == 0
}

// holdNew keeps a new run waiting while the controller runs with --pause.
//
// Parameters:
//   - c writes the run's status.
//   - run is the new run. The caller checked it with backupRunNew or
//     restoreRunNew.
//   - conditions is the run's condition list, which holdNew changes.
//
// It returns the error of the status write. holdNew writes the status only
// when the Ready condition changes to reason Paused, so a later pass writes
// nothing. The run keeps its phase and changes no other object.
func holdNew(ctx context.Context, c client.Client, run client.Object, conditions *[]metav1.Condition) error {
	ready := meta.FindStatusCondition(*conditions, backupv1alpha1.ConditionReady)
	if ready != nil && ready.Reason == backupv1alpha1.ReasonPaused && ready.Status == metav1.ConditionFalse &&
		ready.ObservedGeneration == run.GetGeneration() {
		return nil
	}
	backupv1alpha1.SetReady(conditions, run.GetGeneration(), metav1.ConditionFalse, backupv1alpha1.ReasonPaused, pausedMessage)
	return c.Status().Update(ctx, run)
}

// holdForPause keeps a new BackupRun waiting with reason Paused (see
// holdNew), and returns the error of the status write with the run named.
func (r *BackupRunReconciler) holdForPause(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if err := holdNew(ctx, r.Client, run, &run.Status.Conditions); err != nil {
		return fmt.Errorf("set BackupRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// holdForPause keeps a new RestoreRun waiting with reason Paused (see
// holdNew), and returns the error of the status write with the run named.
func (r *RestoreRunReconciler) holdForPause(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if err := holdNew(ctx, r.Client, run, &run.Status.Conditions); err != nil {
		return fmt.Errorf("set RestoreRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}
