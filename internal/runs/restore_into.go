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
// the create run, and the claim and the Job are created (see createInto).
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
	settings, result, err := r.intoChecks(ctx, run)
	if settings == nil {
		return result, err
	}
	return r.createInto(ctx, run, item, *settings)
}

// intoChecks runs the checks an into restore makes right before it creates
// anything, and takes the Leases.
//
// Parameters:
//   - run is the RestoreRun, with no restore Job yet.
//
// It returns the repository and mover settings when the restore may go on.
// It returns nil settings, with the result and error the pass returns, when
// it may not: a source claim, VolumeRestore or repository Secret that is
// gone, or a claim named spec.into the run did not create, aborts the run
// with reason Failed; a backup of the source claim or the repository in
// progress makes the run wait with reason SourceBusy (see waitForBackup); a
// failed read comes back as an error for a retry.
//
// The Leases and the wait cover the source claim when there is one, as the
// checks did, and the new claim otherwise.
func (r *RestoreRunReconciler) intoChecks(ctx context.Context, run *backupv1alpha1.RestoreRun) (*restoreSettings, ctrl.Result, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if asRunRefusal(err) {
		result, err := r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
		return nil, result, err
	}
	if err != nil {
		return nil, ctrl.Result{}, err
	}
	// A claim that appeared since the checks ends the run before it waits
	// for anything: it is not the run's to write into.
	refusal, err := r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, "claim")
	if err != nil || refusal != "" {
		result, err := r.abortOn(ctx, run, refusal, err)
		return nil, result, err
	}
	leased := run.Spec.Claim
	if leased == "" {
		leased = run.Spec.Into
	}
	waiting, err := r.waitForBackup(ctx, run, leased, settings.Secret)
	if asRunRefusal(err) {
		result, err := r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error()+nothingWritten(run.Spec.Into))
		return nil, result, err
	}
	if waiting || err != nil {
		result, err := after(pollInterval, err)
		return nil, result, err
	}
	return &settings, ctrl.Result{}, nil
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
// scratchClaim). A claim of that name the run did not create aborts the run
// with reason Failed (see createOwned). The Job, created only once the
// claim is known to be the run's since it restores with --delete, mounts
// the claim by name and is its first consumer, so on a
// WaitForFirstConsumer class the scheduler places the volume where the
// Job's pod runs when no node was copied (see createJob). The status write
// that follows records the Job's name and UID together.
func (r *RestoreRunReconciler) createInto(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, settings restoreSettings) (ctrl.Result, error) {
	err := nothingWrittenTo(run.Spec.Into, r.recheckJobSnapshot(ctx, run, *item, settings.Secret))
	if done, err := settled(item, err); done {
		return r.endInto(ctx, run, err)
	}
	refusal, err := r.createOwned(ctx, run, scratchClaim(run, settings), &corev1.PersistentVolumeClaim{}, "claim")
	if err != nil || refusal != "" {
		return r.abortOn(ctx, run, refusal, err)
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

// endInto records an into restore's item end and ends the run.
//
// Parameters:
//   - run is the RestoreRun, whose item has ended.
//   - err is the error of the step that ended it, from settled. When it is
//     not nil the item has not ended, and endInto returns it for a retry.
//
// It returns what finishInto returns, or the error of the status write.
//
// The item's end goes into the status before finish stops the Job. A lost
// write then leaves the item as it was with its Job recorded, so the retry
// reads the same Job and ends the item again, instead of taking the stopped
// Job for one that was deleted before it finished.
func (r *RestoreRunReconciler) endInto(ctx context.Context, run *backupv1alpha1.RestoreRun, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	if err := r.writeStatus(ctx, run); err != nil {
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

// abortOn aborts an into restore on a refusal, or returns a failed read for
// a retry.
//
// Parameters:
//   - run is the RestoreRun.
//   - refusal is the message of a refusal from intoTaken or createOwned, or
//     empty.
//   - err is the error of the same call, or nil.
//
// It returns err when it is not nil, and otherwise what abort returns with
// reason Failed and the refusal as the message.
func (r *RestoreRunReconciler) abortOn(ctx context.Context, run *backupv1alpha1.RestoreRun, refusal string, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return r.abort(ctx, run, backupv1alpha1.ReasonFailed, refusal)
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
