package runs

import (
	"context"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// The code in this file runs an into restore (spec.into) that
// planIntoNewClaim has checked: it creates the new claim and the restore Job
// that fills it, and follows the Job to its end, through the same restore
// Job code as an in-place item (restore_jobs.go and restore_volume.go).

// restoreIntoEmptyClaim makes one pass over an into restore that
// planIntoNewClaim has checked.
//
// Parameters:
//   - run is the RestoreRun, Running or Waiting, whose single item names the
//     claim spec.into and records the full ID of the snapshot the checks
//     selected. The source is the backups of the claim spec.claim names, or
//     the repository spec.repository names.
//
// It returns a result that requeues the run while the restore goes on, and
// what finish, abort or timeOut returns once the run ends. A failed read,
// create or status write comes back as an error, and the pass is retried.
//
// An item that has ended only finishes the run: an earlier pass recorded
// its end and lost the write that would have carried the run's. An item
// that records a restore Job's UID is followed to its end (see
// followIntoJob), and one that records none gets its Job (see
// startIntoJob). The item's UID decides which, so the run never creates a
// second Job for an item that had one: a Job that is gone fails the item,
// and the run stops by the recorded UID whatever still runs of it.
func (r *RestoreRunReconciler) restoreIntoEmptyClaim(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	item := &run.Status.Items[0]
	switch {
	case finished(*item):
		return r.finishInto(ctx, run)
	case item.JobUID == "":
		return r.startIntoJob(ctx, run, item)
	}
	return r.followIntoJob(ctx, run, item)
}

// startIntoJob gives an into restore's item its restore Job: it takes over
// the Job of a pass whose status write was lost, or creates the claim and
// the Job.
//
// Parameters:
//   - run is the RestoreRun.
//   - item is its single item, which records no Job UID. startIntoJob
//     updates it in place.
//
// It returns what the pass returns (see restoreIntoEmptyClaim).
//
// A Job the run created under the item's name (see jobOfRun) comes from a
// pass that ran every check and lost the status write that recorded it,
// and the item takes it over when it restores the recorded full ID (see
// takeOverJob). The status write that follows records the Job's name and
// UID together. A Job with another ID, or one the run did not create,
// fails the item, and the run ends Failed. A run past its deadline before
// it has a Job ends with reason TimedOut. Otherwise the checks right before
// the create run (see intoChecks): a refusal fails the item with its
// reason and ends the run Failed, and a backup in progress makes the pass
// requeue. Then the claim and the Job are created (see createInto).
func (r *RestoreRunReconciler) startIntoJob(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (ctrl.Result, error) {
	taken, err := r.takeOverJob(ctx, run, 0, item)
	if done, err := settled(item, err); done {
		return r.endInto(ctx, run, err)
	}
	if taken {
		return after(pollInterval, r.resume(ctx, run))
	}
	if deadline, over := r.overdue(run); over {
		return r.timeOut(ctx, run, intoTimedOut(run.Spec.Into, deadline))
	}
	settings, waiting, err := r.intoChecks(ctx, run)
	if done, err := settled(item, err); done {
		return r.endInto(ctx, run, err)
	}
	if waiting {
		return after(pollInterval, nil)
	}
	return r.createInto(ctx, run, item, settings)
}

// intoChecks runs the checks an into restore makes right before it creates
// anything, and takes the Leases.
//
// Parameters:
//   - run is the RestoreRun, with no restore Job yet.
//
// It returns the repository and mover settings, and whether the run waits.
// It returns true, with the run moved to Waiting with reason SourceBusy,
// while a backup of the source claim or the repository is in progress (see
// waitForBackup); the caller creates nothing in that pass. It returns a
// *refusalError, which the caller fails the item with (see settled), for a
// source claim, VolumeRestore or repository Secret that is gone, each
// saying nothing was written to the claim spec.into names, and for a claim
// of that name the run did not create, with reason IntoClaimTaken (see
// intoTaken). A failed read or write comes back as a plain error for a
// retry.
//
// The Leases and the wait cover the source claim when there is one, as the
// checks did, and the new claim otherwise. repositoryFor never refuses the
// spec here: the CRD's CEL rule "into needs claim or repository" keeps a
// spec that names neither out of the API server, on create and on every
// update.
func (r *RestoreRunReconciler) intoChecks(ctx context.Context, run *backupv1alpha1.RestoreRun) (restoreSettings, bool, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		return settings, false, nothingWrittenTo(run.Spec.Into, err)
	}
	// A claim that appeared since the checks ends the run before it waits
	// for anything: it is not the run's to write into.
	if err := r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, "claim"); err != nil {
		return settings, false, err
	}
	leased := run.Spec.Claim
	if leased == "" {
		leased = run.Spec.Into
	}
	waiting, err := r.waitForBackup(ctx, run, leased, settings.Secret)
	return settings, waiting, nothingWrittenTo(run.Spec.Into, err)
}

// createInto lists the repository again, then creates an into restore's
// claim and restore Job.
//
// Parameters:
//   - run is the RestoreRun, past intoChecks.
//   - item is its single item, which createInto updates in place.
//   - settings are the repository and mover settings from intoChecks.
//
// It returns what the pass returns (see restoreIntoEmptyClaim).
//
// The recorded full ID must still be in the repository (see
// recheckJobSnapshot); a refusal fails the item with its reason, says
// nothing was written to the claim, and ends the run Failed. The claim is
// a plain one with no data source, so no populator takes part: it carries
// the run's controller reference, the source claim's size and class, or
// spec.intoSize, and the node the source claim's volume is on (see
// scratchClaim). A claim of that name the run did not create fails the
// item with reason IntoClaimTaken and ends the run Failed (see createOwned).
// The Job, created only once the claim is known to be the run's since it
// restores with --delete, mounts the claim by name and is its first
// consumer, so on a WaitForFirstConsumer class the scheduler places the
// volume where the Job's pod runs when no node was copied (see createJob).
// The status write that follows records the Job's name and UID together.
func (r *RestoreRunReconciler) createInto(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, settings restoreSettings) (ctrl.Result, error) {
	err := nothingWrittenTo(run.Spec.Into, r.recheckJobSnapshot(ctx, run, *item, settings.Secret))
	if done, err := settled(item, err); done {
		return r.endInto(ctx, run, err)
	}
	if done, err := settled(item, r.createOwned(ctx, run, scratchClaim(run, settings), &corev1.PersistentVolumeClaim{}, "claim")); done {
		return r.endInto(ctx, run, err)
	}
	if done, err := settled(item, r.createJob(ctx, run, 0, item, settings)); done {
		return r.endInto(ctx, run, err)
	}
	return after(pollInterval, r.resume(ctx, run))
}

// followIntoJob reads an into restore's restore Job and the claim it
// writes, and ends the run once the item has ended.
//
// Parameters:
//   - run is the RestoreRun.
//   - item is its single item, Running and naming its Job and the Job's
//     UID. followIntoJob updates it in place.
//
// It returns what the pass returns (see restoreIntoEmptyClaim).
//
// The Job's terminal conditions decide the item's end, and a Job that is
// gone or replaced fails it (see followJob). While the Job runs, every pass
// also checks that the claim is still the run's (see claimLostError), so a
// claim deleted or replaced mid-restore fails the item with reason
// ClaimLost and the run stops the Job. A run past its deadline ends with
// reason TimedOut. Otherwise the item's message shows why the Job's pod
// waits, if it does, and the status is written when it changed.
func (r *RestoreRunReconciler) followIntoJob(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (ctrl.Result, error) {
	_, err := r.followJob(ctx, run, item)
	if err == nil && !finished(*item) {
		err = r.claimLostError(ctx, run, *item)
	}
	if done, err := settled(item, err); done || finished(*item) {
		return r.endInto(ctx, run, err)
	}
	if deadline, over := r.overdue(run); over {
		return r.timeOut(ctx, run, intoTimedOut(run.Spec.Into, deadline))
	}
	return after(pollInterval, r.writeChangedStatus(ctx, run))
}

// endInto ends an into restore whose item has ended, or returns the error
// of the step that did not end it.
//
// Parameters:
//   - run is the RestoreRun, whose item has ended when err is nil.
//   - err is the error of the step that ended it, from settled. When it is
//     not nil the item has not ended, and endInto returns it for a retry.
//
// It returns what finishInto returns. finish writes the item's end with the
// run's ending before it stops the Job, so a lost write leaves the item as
// it was with its Job recorded: the retry reads the same Job and ends the
// item again, instead of taking the stopped Job for one that was deleted
// before it finished.
func (r *RestoreRunReconciler) endInto(ctx context.Context, run *backupv1alpha1.RestoreRun, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.finishInto(ctx, run)
}

// finishInto ends an into restore whose item has ended: Succeeded when the
// item succeeded, and Failed with the item's message otherwise.
//
// Parameters:
//   - run is the RestoreRun, whose item has ended.
//
// It returns what finish returns.
func (r *RestoreRunReconciler) finishInto(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if run.Status.Items[0].Phase == backupv1alpha1.ItemSucceeded {
		return r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, fmt.Sprintf("claim %s holds the restored data", run.Spec.Into))
	}
	return r.finish(ctx, run, backupv1alpha1.ReasonFailed, restoreFailures(run.Status.Items))
}

// resume writes the status of an into restore once its item names its
// restore Job, and moves a run that waited back to Running.
//
// Parameters:
//   - run is the RestoreRun, whose item records the Job's name and UID.
//
// It returns the error of the status write. The write records the Job's
// name and UID together; a lost write leaves the Job to the next pass's
// takeover (see takeOverJob).
func (r *RestoreRunReconciler) resume(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if run.Status.Phase == backupv1alpha1.RunPhaseWaiting {
		run.Status.Phase = backupv1alpha1.RunPhaseRunning
		backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
			fmt.Sprintf("restoring into claim %s", run.Spec.Into))
	}
	return r.writeStatus(ctx, run)
}
