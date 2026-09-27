package runs

import (
	"context"
	"errors"
	"slices"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// abort ends a run early as Failed. It fails every Pending or Running item
// with the given message (see failUnfinished), then calls finish, which
// records the ending, starts the stopped workloads again and deletes the
// run's Workload so the queue gets its slot back.
//
// Parameters:
//   - reason is the Ready reason the run ends with: ReasonFailed for a run
//     that hit something it can't get past, ReasonInvalid for a run whose
//     targets it refuses, and ReasonVolSyncUnsupported for a VolSync this
//     controller cannot use.
//   - message is the Ready message, and the start of each unfinished item's
//     message.
//
// The items it fails record the reason RunEnded. The run's ending says why.
// A run past its deadline, or past its timeout in the Kueue queue, ends
// through endTimedOut instead.
func (r *BackupRunReconciler) abort(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	r.failUnfinished(ctx, run, message, func(item *backupv1alpha1.BackupItem, text string) {
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonRunEnded, "%s", text))
	})
	return r.finish(ctx, run, reason, message)
}

// timeOut ends a run whose deadline has passed, as Failed.
//
// Parameters:
//   - run is the BackupRun past its deadline, with no ending recorded yet.
//   - deadline is the run's deadline as overdue returns it.
//
// It returns what finish returns: nil once the run has ended, or the error
// of a restart or release that failed, for a retry.
//
// The message is timedOutMessage's, which names the SourceBusy wait the run
// was in. Every Pending or Running item fails with reason TimedOut and that
// message (see failUnfinished), and finish records the message in
// status.ending in the same status write. A later pass, such as one that
// retries a failed restart after releaseFailed replaced the SourceBusy
// condition, ends with the recorded ending and builds no message again.
func (r *BackupRunReconciler) timeOut(ctx context.Context, run *backupv1alpha1.BackupRun, deadline time.Time) error {
	return r.endTimedOut(ctx, run, timedOutMessage(deadline, run.Status.Conditions))
}

// endTimedOut ends a run whose timeout ran out, as Failed.
//
// Parameters:
//   - run is the BackupRun whose timeout ran out.
//   - message is the Ready message, and the start of each unfinished item's
//     message.
//
// It returns what finish returns.
//
// Every Pending or Running item fails with reason TimedOut (see
// failUnfinished). timeOut and awaitAdmission call it, so a run gives its
// items the same reason for each timeout.
func (r *BackupRunReconciler) endTimedOut(ctx context.Context, run *backupv1alpha1.BackupRun, message string) error {
	r.failUnfinished(ctx, run, message, func(item *backupv1alpha1.BackupItem, text string) {
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonTimedOut, "%s", text))
	})
	return r.finish(ctx, run, backupv1alpha1.ReasonFailed, message)
}

// failUnfinished fails every Pending or Running item of a run that ends
// early, each with a message that starts with the run's.
//
// Parameters:
//   - run is the BackupRun that ends. Its items are changed in place, and
//     the caller writes the status.
//   - message is the run's Ready message.
//   - fail sets an item Failed with its message; the caller decides which
//     reason the item records.
//
// A Pending item whose last start attempt failed adds "; last error: " and
// status.items[].lastStartError to the message. A Running volume item adds
// the snapshot it recorded and did not move, or what data a snapshot of the
// sync VolSync goes on with holds, and a Running database item names a
// Backup phase CloudNativePG 1.30 does not have (see runningNote).
func (r *BackupRunReconciler) failUnfinished(ctx context.Context, run *backupv1alpha1.BackupRun, message string,
	fail func(item *backupv1alpha1.BackupItem, text string)) {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		text := message
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			text += startErrorNote(*item, "; last error: ")
		case backupv1alpha1.ItemRunning:
			if note := r.runningNote(ctx, run, *item); note != "" {
				text += ". " + note
			}
		default:
			// An item that has ended keeps how it ended.
			continue
		}
		fail(item, text)
	}
}

// startErrorNote returns what the message of a Pending item that fails adds
// for the item's last failed start.
//
// Parameters:
//   - item is the Pending item that fails.
//   - separator goes before the error, so the note reads on from the
//     caller's message.
//
// It returns the separator and status.items[].lastStartError, or "" when
// the item's last start attempt did not fail. The text goes into the item's
// message and nothing else; no decision reads it.
func startErrorNote(item backupv1alpha1.BackupItem, separator string) string {
	if item.LastStartError == "" {
		return ""
	}
	return separator + item.LastStartError
}

// finish ends the run. It records the ending, starts any workload the run
// still holds stopped, deletes the run's Workload, and records the terminal
// phase: Succeeded when the ending's reason is ReasonSucceeded and Failed
// for any other reason. It sets the Ready condition to the ending's reason
// and message, records status.completedAt, and removes the finalizer.
//
// Parameters:
//   - reason and message are the Ready reason and message the run ends
//     with. A run that already recorded status.ending ends with that one,
//     and these are not used.
//
// It returns nil once the run has ended, the error of a release that
// failed, or the error of a status write.
//
// finish records reason and message in status.ending before release, so
// every status write from then on carries them, and the items the caller
// failed go in the same write. When release fails, releaseFailed reports
// the failure on the run with reason RestartFailed or ReleaseFailed and
// writes the status, ending included, and the error it returns makes the
// reconcile run again. That pass finds status.ending and calls finish with
// it (see Reconcile), so the run ends as it decided to, whatever it waited
// for when it decided. The run stays unfinished until release succeeds.
func (r *BackupRunReconciler) finish(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	if run.Status.Ending == nil {
		run.Status.Ending = &backupv1alpha1.RunEnding{Reason: reason, Message: message}
	}
	ending := *run.Status.Ending
	if err := r.release(ctx, run); err != nil {
		return r.releaseFailed(ctx, run, err, false)
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	if ending.Reason != backupv1alpha1.ReasonSucceeded {
		run.Status.Phase = backupv1alpha1.RunPhaseFailed
	}
	run.Status.CompletedAt = &now
	run.Status.Workload = ""
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, ending.Reason, ending.Message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	// The stored status now shows the workloads back, so the quiesce Leases
	// may go, and must go before the finalizer: a run whose finalizer is
	// dropped while it still holds them keeps other runs out of the
	// namespace until the Lease's holder reads as gone. Best effort, as in
	// work: a Lease left behind is stale under holderLive's rule.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the run goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return dropFinalizer(ctx, r.Client, run)
}

// release puts back what the run changed in the cluster. When the run stopped
// workloads and has not started them again, release scales them back up,
// resumes the Kustomizations it suspended, and records status.restartedAt. A
// run that recorded its plan to stop them counts as having stopped them, even
// before status.quiescedAt is set, because the pass that wrote the plan may
// have stopped them and then lost its status write. release then releases
// every Lease the run holds (see releaseLeases) and deletes the run's
// Workload.
//
// A run with status.restartPending set has chosen its restart moment and may
// not have started the workloads yet. release starts them and keeps that
// moment. A run whose status shows the restart done starts nothing again, so
// it never scales up a workload another run has stopped since. release
// decides this on the run as stored: it reads the run again through the
// uncached Reader first (see readStop), since the informer cache can lag
// behind the pass that did the restart.
//
// The Leases are released even when the restart fails: the run's items are
// done, and otherMover still keeps another run's mover off a claim whose
// mover runs. The Workload is deleted only once the restart and the Leases
// went through, so the run keeps its place in the queue while it still owes
// the app its replicas.
//
// It returns nil when everything is put back. A failed restart comes back as
// a *quiesce.RestartError from quiesce.Restart, which names the workload or
// Kustomization; a failed Lease release or Workload delete as a
// *releaseError that names what it could not delete. When both the restart
// and the Lease release fail, it returns both, joined with errors.Join. A
// failed read of the stored run comes back as it is, with nothing started.
// Every step is safe to repeat.
func (r *BackupRunReconciler) release(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if _, err := readStop(ctx, r.Reader, run); err != nil {
		return err
	}
	var restartErr error
	holding := (run.Status.QuiescedAt != nil || len(run.Status.Quiesced) > 0) && run.Status.RestartedAt == nil
	if holding || run.Status.RestartPending {
		restartErr = quiesce.Restart(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
		if restartErr == nil {
			if run.Status.RestartedAt == nil {
				run.Status.RestartedAt = newTime(metav1.NewTime(r.Now()).Rfc3339Copy())
			}
			run.Status.RestartPending = false
		}
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return errors.Join(restartErr, leaseReleaseError(run, err))
	}
	if restartErr != nil {
		return restartErr
	}
	if err := kueue.DeleteWorkload(ctx, r.Client, run.Namespace, run.UID); err != nil {
		return &releaseError{
			action: "delete its Kueue Workload " + kueue.WorkloadName(run.UID) + ", which holds the run's place in the queue",
			advice: "Fix the cause, or delete the Workload yourself; either way the run then finishes by itself.",
			err:    err,
		}
	}
	return nil
}

// backupItemDone reports whether the run's items with the given name, a
// volume and a Cluster item, have finished: each is neither Pending nor Running, or the run
// has no such item. A Lease names its items by name alone. Thus a claim Lease
// stays while a Cluster item with the claim's name runs, until the run
// finishes at the latest. Another run can still take that Lease over (see
// holderLive).
func backupItemDone(run *backupv1alpha1.BackupRun, name string) bool {
	return !slices.ContainsFunc(run.Status.Items, func(item backupv1alpha1.BackupItem) bool {
		return item.Name == name && backupItemOpen(item)
	})
}

// finalize puts back what a run changed when the run is deleted before it
// finished, then removes the finalizer so the deletion can complete. When
// release fails, it reports the failure through releaseFailed and keeps the
// finalizer, so the deletion waits until the run has put everything back.
func (r *BackupRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.release(ctx, run); err != nil {
		return r.releaseFailed(ctx, run, err, false)
	}
	if err := dropFinalizer(ctx, r.Client, run); err != nil {
		return err
	}
	// After dropFinalizer, and not before: finalize writes no status, so a
	// release that happened while the drop still failed would let another run
	// take the Lease and then watch this run repeat its restart on the retry.
	// Best effort: a Lease left behind is stale under holderLive's rule.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the deletion goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return nil
}

// releaseFailed reports on the run that it could not put back what it
// changed, and returns err so the reconcile runs again with
// controller-runtime's backoff.
//
// Parameters:
//   - err is the error from release, or the *quiesce.RestartError of the restart
//     after the clones are cut.
//   - working is true when work calls it for that restart, while the run is
//     still backing up, and false when finish or finalize call it because
//     release failed.
//
// It sets the Ready condition to the reason and message from releaseFailure,
// which names what the run could not do and gives advice that fits, and
// writes the status. Announce turns either reason into a Warning event. The
// status write is best effort: a write that fails is made again by the next
// pass that fails.
//
// The run never gives up. A run that finished while it still owed a restart
// would lose the only record of the replicas the app had, and the next
// namespace run would record the stopped workload's 0 as the count to give
// back.
func (r *BackupRunReconciler) releaseFailed(ctx context.Context, run *backupv1alpha1.BackupRun, err error, working bool) error {
	reason, message := releaseFailure(err, releasePlan{
		stopped: run.Status.Quiesced, suspended: run.Status.SuspendedKustomizations,
		kind: "BackupRun", working: working, deleting: !run.DeletionTimestamp.IsZero(), scheduled: run.Spec.All,
	})
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	_ = r.writeStatus(ctx, run)
	return err
}
