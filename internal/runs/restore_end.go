package runs

import (
	"context"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// abort ends a run early as Failed. It fails every item that has not
// finished with the given message (see failRemainingItems), then calls
// finish, which records the ending, stops the run's movers and gives the
// app back.
//
// Parameters:
//   - reason is the Ready reason the run ends with: ReasonInvalid when it
//     refuses a Kustomization it would have to suspend, and ReasonFailed
//     when it hit an error it cannot get past, such as a workload it could
//     not stop or one that was deleted.
//   - message is the Ready message, and the start of each unfinished item's
//     message.
//
// It returns what finish returns: an empty result once the run has ended, or
// the wait for a stopped mover, which finish reports (rule X2).
//
// Before it fails anything, abort reads the restore Job of each unfinished
// volume item, a lost create's included (see settleJobs), so an item whose
// Job has ended records how, and an item whose Job still waits for its pod
// adds why to its message. A failed read comes back as an error for a
// retry. The items it fails record no reason; the run's ending says why.
// The Ready message also carries the note of each Cluster the run left
// deleted (see leftDeletedNotes). A run past its deadline ends through
// timeOut instead.
func (r *RestoreRunReconciler) abort(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) (ctrl.Result, error) {
	waits, err := r.settleJobs(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	failRemainingItems(run, message, "")
	addWaits(run, waits)
	return r.finish(ctx, run, reason, leftDeletedNotes(run.Status.Items, message))
}

// timeOut ends a run whose deadline has passed, with reason TimedOut.
//
// Parameters:
//   - run is the RestoreRun past its deadline, with no ending recorded yet.
//   - message is the Ready message: timedOutMessage's for an in-place run,
//     and intoTimedOut's for an into restore, each naming the SourceBusy or
//     VolSyncUnsupported wait the run was in.
//
// It returns what finish returns: an empty result once the run has ended,
// the wait for a stopped mover, or the error of a release that failed, for
// a retry. A failed read of a restore Job comes back as an error too.
//
// First the restore Job of each unfinished volume item is read, a lost
// create's included (see settleJobs), so a restore that completed just as
// the deadline passed records Succeeded. Every item still unfinished then
// fails with reason TimedOut (see failRemainingItems), and finish records
// the Ready message, with the note of each Cluster the run left deleted, in
// status.ending. Every status
// write from then on carries the ending, and a later pass, such as one
// after the wait for a stopped mover replaced the SourceBusy condition,
// ends with it as recorded.
func (r *RestoreRunReconciler) timeOut(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) (ctrl.Result, error) {
	waits, err := r.settleJobs(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	failRemainingItems(run, message, backupv1alpha1.ItemReasonTimedOut)
	addWaits(run, waits)
	return r.finish(ctx, run, backupv1alpha1.ReasonTimedOut, leftDeletedNotes(run.Status.Items, message))
}

// failRemainingItems fails every item of a run that ends early and has not
// finished, each with a message that starts with the run's.
//
// Parameters:
//   - run is the RestoreRun that ends. Its items are changed in place, and
//     the caller writes the status.
//   - message is the run's Ready message.
//   - reason is the reason each failed item records: ItemReasonTimedOut
//     from timeOut, and none from abort, whose run's ending says why.
//
// A Pending, Running or Recovering item gets the message as it is. A
// Cluster item in phase Deleted records status.items[].clusterLeftDeleted,
// and its message adds the note from clusterLeftDeleted: the run deleted
// that Cluster, and the webhook recovers its next creation without the run.
// An item in any other phase has already ended and keeps how it ended.
func failRemainingItems(run *backupv1alpha1.RestoreRun, message string, reason backupv1alpha1.ItemReason) {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		switch item.Phase {
		case backupv1alpha1.ItemDeleted:
			item.ClusterLeftDeleted = true
			failRestoreItem(item, refuse(reason, "%s", message+". "+clusterLeftDeleted(item.Name)))
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning, backupv1alpha1.ItemRecovering:
			failRestoreItem(item, refuse(reason, "%s", message))
		default:
			// An item in any other phase has already ended, and keeps its
			// phase and message.
		}
	}
}

// leftDeletedNotes returns the Ready message of a run that ends early: the
// given message, then the note from clusterLeftDeleted for each item that
// records status.items[].clusterLeftDeleted.
//
// Parameters:
//   - items are the run's items, after failRemainingItems.
//   - message is the message the run ends with.
func leftDeletedNotes(items []backupv1alpha1.RestoreItem, message string) string {
	for _, item := range items {
		if item.ClusterLeftDeleted {
			message += ". " + clusterLeftDeleted(item.Name)
		}
	}
	return message
}

// leftDeleted reports whether an item is a Cluster the run deleted and has
// not seen created again: one still in phase Deleted, or one a run that
// ended early failed and marked with status.items[].clusterLeftDeleted.
//
// Parameters:
//   - item is one of the run's items, as its status records it.
func leftDeleted(item backupv1alpha1.RestoreItem) bool {
	return item.Kind == "Cluster" && (item.Phase == backupv1alpha1.ItemDeleted || item.ClusterLeftDeleted)
}

// finish ends the run and gives back what it holds.
//
// Parameters:
//   - run is the RestoreRun to end. Its status is written with the end.
//   - reason is the Ready reason the run ends with. ReasonSucceeded ends it
//     Succeeded, and any other reason ends it Failed.
//   - message is the Ready message the run ends with.
//
// A run that already recorded status.ending ends with that one, and reason
// and message are not used.
//
// It returns an empty result once the run has ended. While a stopped mover
// is not gone yet, it returns the wait from waitForStopped, and the run stays
// unfinished. A step that fails is reported through releaseFailed, whose
// error it returns for a retry; a failed status write comes back as it is.
//
// finish first records reason and message in status.ending and writes the
// status, with the items the caller failed, before it stops anything. Every
// status write from then on carries the ending, and the next pass finds it
// and calls finish with it (see Reconcile), so the run ends as it decided
// to, whatever it waited for when it decided. A stop that completes in the
// same pass therefore cannot lose the items' ends with a lost final write:
// the next pass would otherwise find an item Running on a Job that is gone
// and fail it as a Job deleted before it finished.
//
// The steps run in this order, and each one runs only once the one before it
// went through:
//
//  1. It stops the run's movers (see stopJobs): it suspends each restore
//     Job and waits until no pod of it can still write (rule X2), so no
//     mover writes into a claim or the repository once the app is back or
//     another run takes over.
//  2. It gives back the workloads the run stopped and resumes the
//     Kustomizations it suspended, and records status.restartedAt.
//  3. It releases the run's claim and repository Leases (see releaseLeases).
//  4. It writes the end: the phase, the Ready condition and
//     status.completedAt.
//  5. It releases the namespace's quiesce Lease, which the stored status now
//     shows is no longer needed.
//  6. It removes the run's finalizer.
//
// A run never reaches finish with a Cluster item still in phase Deleted:
// such an item is not finished, so work ends the run only through abort or
// timeOut, which fail the item with the note from clusterLeftDeleted first
// (see failRemainingItems).
func (r *RestoreRunReconciler) finish(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) (ctrl.Result, error) {
	ending := backupv1alpha1.RunEnding{Reason: reason, Message: message}
	if run.Status.Ending != nil {
		ending = *run.Status.Ending
	} else {
		run.Status.Ending = ending.DeepCopy()
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	left, err := r.stopJobs(ctx, run, anyItem)
	if err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, err)
	}
	if len(left) > 0 {
		return r.waitForStopped(ctx, run, left.message())
	}
	// The app is given back before the Leases go: a run that could not start
	// the workloads keeps the claim and the repository to itself until it
	// can, so no other run's mover starts on them meanwhile.
	if stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return ctrl.Result{}, r.releaseFailed(ctx, run, err)
		}
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, leaseReleaseError(run, err))
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	if ending.Reason != backupv1alpha1.ReasonSucceeded {
		run.Status.Phase = backupv1alpha1.RunPhaseFailed
	}
	run.Status.CompletedAt = &now
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, ending.Reason, ending.Message)
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	// The stored status now shows the workloads back, so the quiesce Leases
	// may go, before the finalizer: another run then takes the namespace over
	// at once. Best effort, as in work.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the run goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return ctrl.Result{}, dropFinalizer(ctx, r.Client, run)
}

// finalize gives back what a run holds when the run is deleted, and then
// removes its finalizer so the deletion can complete.
//
// Parameters:
//   - run is the RestoreRun being deleted. It may be finished or unfinished.
//
// It returns an empty result once the finalizer is gone, or when the run
// holds none. While a stopped mover is not gone yet, it returns the wait
// from waitForStopped. A step that fails is reported through releaseFailed,
// whose error it returns for a retry.
//
// The steps run in this order, and each one runs only once the one before it
// went through: it stops the run's movers and waits until none can still
// write (rule X2, see stopJobs), gives the stopped workloads back and
// resumes the suspended Kustomizations, releases the claim and repository
// Leases, drops the finalizer, and last releases the namespace's quiesce
// Lease. Without
// finalize, a run deleted while its mover writes would leave a restore
// running against a claim with nothing tracking it. The run keeps its
// finalizer and its Leases while it waits or retries: a Lease released
// before the finalizer is dropped would let another run take the claim over
// while this run repeats its restart.
//
// Right before it drops the finalizer, finalize records a Warning event with
// reason ClusterLeftDeleted, with the note from clusterLeftDeleted, for
// each Cluster item still in phase Deleted and each one that records
// status.items[].clusterLeftDeleted, which a run that timed out or aborted
// set when it failed the item (see leftDeleted). The run is about to go,
// so the event is the only place that note can appear. It reads the
// Cluster first: one its owner has created again, with a UID other than the
// item's clusterUID, gets no event, since the note speaks of a creation
// still to come. A failed read comes back as an error, and the finalizer
// stays until a retry gets past it.
func (r *RestoreRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return ctrl.Result{}, nil
	}
	left, err := r.stopJobs(ctx, run, anyItem)
	if err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, err)
	}
	if len(left) > 0 {
		return r.waitForStopped(ctx, run, left.message())
	}
	// The app is given back before the Leases go, as in finish.
	if stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return ctrl.Result{}, r.releaseFailed(ctx, run, err)
		}
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return ctrl.Result{}, r.releaseFailed(ctx, run, leaseReleaseError(run, err))
	}
	if r.Recorder != nil {
		for _, item := range run.Status.Items {
			if !leftDeleted(item) {
				continue
			}
			// A Cluster its owner has created again is back, and the note
			// about its next creation would be wrong.
			cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			if !found || cluster.GetUID() == item.ClusterUID {
				r.Recorder.Eventf(run, nil, corev1.EventTypeWarning, "ClusterLeftDeleted", "Restore", "%s", fitNote(clusterLeftDeleted(item.Name)))
			}
		}
	}
	if err := dropFinalizer(ctx, r.Client, run); err != nil {
		return ctrl.Result{}, err
	}
	// The quiesce Lease goes after dropFinalizer. finalize does not store the
	// restart in the run's status, so when the drop fails, the next pass
	// restarts the app again. Released before that, the Lease would let
	// another run stop the app in between, and the repeated restart would
	// undo that stop. The release is best effort: a Lease left behind is
	// stale under holderLive's rule.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the deletion goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return ctrl.Result{}, nil
}

// releaseFailed reports on the run that it could not stop one of its movers,
// give the app back or release what it holds, and hands the error back.
//
// Parameters:
//   - run is the RestoreRun that failed. Its Ready condition is set and its
//     status written.
//   - err is the error from finish or finalize: the *releaseError of a mover
//     the run could not stop or of a Lease it could not read or release, or
//     the *quiesce.RestartError of a restart that failed.
//
// It returns err, so the reconcile runs again with controller-runtime's
// backoff.
//
// Only finish and finalize call it, once the run has stopped restoring, so
// the plan it hands releaseFailure says the run is not working, and the
// advice may say the run can be deleted.
//
// The Ready condition takes the reason and message from releaseFailure. The
// reason is RestartFailed while the run still holds workloads stopped (see
// stopped), whatever step failed, and ReleaseFailed when a release step
// failed once the app is back. Announce turns either reason into a Warning
// event. The status write is best effort: a write that fails is made again
// by the next pass that fails. The run never gives up. A run that finished
// while it still held a claim, a repository or the app would lose the only
// record of what it has to put back.
func (r *RestoreRunReconciler) releaseFailed(ctx context.Context, run *backupv1alpha1.RestoreRun, err error) error {
	reason, message := releaseFailure(err, releasePlan{
		stopped: run.Status.Quiesced, suspended: run.Status.SuspendedKustomizations,
		kind: "RestoreRun", deleting: !run.DeletionTimestamp.IsZero(), appDown: stopped(run),
	})
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	_ = r.writeStatus(ctx, run)
	return err
}
