package runs

import (
	"context"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// pause keeps a new run waiting while the controller runs with --pause, and
// records the end of the pause on a run that waited.
//
// Parameters:
//   - f is the run that the pass read.
//   - paused is true when the controller runs with --pause.
//   - isNew is true when the run has started no work (see backupRunNew and
//     restoreRunNew).
//
// It returns held true when the pass ends here, and the error of the status
// write.
//
// A new run under --pause waits with reason Paused. pause writes the status
// only when the Ready condition changes to reason Paused, so a later pass
// writes nothing, and the run keeps its phase and changes no other object.
// When the Ready condition has reason Paused and the run has no
// status.resumedAt, and the run may go on, pause sets resumedAt to the time
// of the pass and writes the status. The timeout of the run then counts from
// resumedAt (see clockStart), so the time the run waited Paused does not
// count.
func (o runOps) pause(ctx context.Context, f runFields, paused, isNew bool) (held bool, err error) {
	ready := meta.FindStatusCondition(*f.conditions, backupv1alpha1.ConditionReady)
	waited := ready != nil && ready.Reason == backupv1alpha1.ReasonPaused
	switch {
	case paused && isNew:
		if waited && ready.Status == metav1.ConditionFalse && ready.ObservedGeneration == f.GetGeneration() {
			return true, nil
		}
		backupv1alpha1.SetReady(f.conditions, f.GetGeneration(), metav1.ConditionFalse, backupv1alpha1.ReasonPaused, pausedMessage)
		return true, o.writeStatus(ctx, f)
	case waited && *f.resumedAt == nil:
		*f.resumedAt = newTime(metav1.NewTime(o.now()))
		return false, o.writeStatus(ctx, f)
	}
	return false, nil
}

// clockStart returns the time from which the timeout of a run that has no
// status.startedAt counts: status.resumedAt when the run waited Paused (see
// runOps.pause), else the creation of the run. A zero result means that the
// run has no creation time yet.
func clockStart(created metav1.Time, resumedAt *metav1.Time) time.Time {
	if resumedAt != nil {
		return resumedAt.Time
	}
	return created.Time
}

// startWord names the start of the timeout of a run that has no
// status.startedAt, for a message: "the end of the pause" when resumedAt is
// set, else "its creation" (see clockStart).
func startWord(resumedAt *metav1.Time) string {
	if resumedAt != nil {
		return "the end of the pause"
	}
	return "its creation"
}
