package runs

import (
	"context"
	"errors"
	"slices"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
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
	r.failUnfinished(ctx, run, message, backupv1alpha1.ItemReasonRunEnded)
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
	r.failUnfinished(ctx, run, message, backupv1alpha1.ItemReasonTimedOut)
	return r.finish(ctx, run, backupv1alpha1.ReasonFailed, message)
}

// failUnfinished fails every Pending or Running item of a run that ends
// early, each with a message that starts with the run's.
//
// Parameters:
//   - run is the BackupRun that ends. Its items are changed in place, and
//     the caller writes the status.
//   - message is the run's Ready message.
//   - reason is the reason each failed item records.
//
// A Pending item whose last start attempt failed adds "; last error: " and
// status.items[].lastStartError to the message. A Running volume item adds
// the snapshot it recorded and did not move, or what data a snapshot of the
// sync VolSync goes on with holds, and a Running database item names a
// Backup phase CloudNativePG 1.30 does not have (see runningNote).
func (r *BackupRunReconciler) failUnfinished(ctx context.Context, run *backupv1alpha1.BackupRun, message string, reason backupv1alpha1.ItemReason) {
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
		failBackupItem(item, refuse(reason, "%s", text))
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

// finish ends the run. It records the ending, gives back what the run holds
// (see release), and records the end (see runOps.end): the terminal phase,
// Succeeded when the ending's reason is ReasonSucceeded and Failed for any
// other reason, the Ready condition, status.completedAt, and the removal of
// the finalizer.
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
	if err := r.release(ctx, run); err != nil {
		return r.releaseFailed(ctx, run, err, false)
	}
	run.Status.Workload = ""
	return r.ops().end(ctx, fieldsOf(run))
}

// release puts back what the run changed in the cluster: it gives the app
// back (see runOps.restart), releases every Lease the run holds (see
// releaseLeases) and deletes the run's Workload.
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
// and the Lease release fail, it returns both, joined with errors.Join.
// Every step is safe to repeat.
func (r *BackupRunReconciler) release(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	restartErr := r.ops().restart(ctx, fieldsOf(run))
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
// finished, then lets it go (see runOps.finalized). When release fails, it
// reports the failure through releaseFailed and keeps the finalizer, so the
// deletion waits until the run has put everything back.
func (r *BackupRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.release(ctx, run); err != nil {
		return r.releaseFailed(ctx, run, err, false)
	}
	return r.ops().finalized(ctx, fieldsOf(run))
}

// releaseFailed reports on the run that it could not put back what it
// changed (see runOps.releaseFailed), and returns err.
//
// Parameters:
//   - err is the error from release, or the *quiesce.RestartError of the
//     restart after the clones are cut.
//   - working is true when work calls it for that restart, while the run is
//     still backing up, and false when finish or finalize call it because
//     release failed.
//
// A run with spec.all set holds the namespace's schedule, and the message
// says so.
func (r *BackupRunReconciler) releaseFailed(ctx context.Context, run *backupv1alpha1.BackupRun, err error, working bool) error {
	return r.ops().releaseFailed(ctx, fieldsOf(run), err, releasePlan{working: working, scheduled: run.Spec.All})
}
