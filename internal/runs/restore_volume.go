package runs

import (
	"context"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The code in this file moves a volume item a step further on its restore
// Job: it starts an in-place item's Job once nothing holds the item back,
// and records how the Job of an in-place or an into item ended.

// settled reports whether a step of a volume restore ends the pass.
//
// Parameters:
//   - item is the volume item. It is failed in place when err is one that
//     fails an item (see failRestoreItem).
//   - err is the step's error. It may be nil.
//
// It returns true and nil when it failed the item, true and err for any
// other error, which the pass returns for a retry, and false and nil when
// err is nil and the restore goes on.
func settled(item *backupv1alpha1.RestoreItem, err error) (bool, error) {
	if failRestoreItem(item, err) {
		return true, nil
	}
	return err != nil, err
}

// startJob moves a Pending in-place volume item a step further, and starts
// its restore Job once nothing holds it back.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - index is the item's position in status.items, which names its Job
//     (see jobName).
//   - item is the volume item, which startJob updates in place.
//
// It returns a Ready reason and message while the item waits, and empty
// strings otherwise: ClaimInUse naming the pod while a pod mounts the claim,
// and SourceBusy naming the other run while another run holds the Lease of
// the claim or its repository (see acquireLeases) or a backup of either is
// in progress (see otherMover). The item stays Pending in each of these
// waits. It returns an error, and leaves the item as it was, when an API
// call or the listing fails for a reason a retry may fix.
//
// A Job the run created under the item's name is taken over before
// anything else (see takeOverJob). Any other Pending item waits until no pod
// mounts the claim, then passes the start checks (see startRefusal), takes
// the Leases, and lists the repository again (see recheckJobSnapshot). A
// refusal from any of them fails the item with its reason and says that
// nothing was written to the claim. Then the run creates the Job (see
// createJob).
func (r *RestoreRunReconciler) startJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (reason, message string, err error) {
	if taken, err := r.takeOverJob(ctx, run, index, item); taken || err != nil {
		_, err = settled(item, err)
		return "", "", err
	}
	holder, err := claimHolder(ctx, r.Reader, run.Namespace, item.Name)
	if err != nil {
		return "", "", fmt.Errorf("look for a pod holding claim %s: %w", item.Name, err)
	}
	if holder != "" {
		return backupv1alpha1.ReasonClaimInUse,
			fmt.Sprintf("claim %s is mounted by pod %s; stop the workload and this restore starts on its own", item.Name, holder), nil
	}
	settings, err := r.startRefusal(ctx, run, item.Name)
	if done, err := settled(item, err); done {
		return "", "", err
	}
	// The Leases make the restore and a backup of the claim or its
	// repository exclusive: of two runs that get here in the same instant,
	// only one creates each Lease.
	busy, err := acquireLeases(ctx, r.Client, r.Reader, leaseHolder{kind: backupv1alpha1.KindRestoreRun, run: run, item: item.Name},
		run.Namespace, item.Name, settings.Secret)
	if done, err := settled(item, nothingWrittenTo(item.Name, err)); done {
		return "", "", err
	}
	if busy != "" {
		return backupv1alpha1.ReasonSourceBusy, busy, nil
	}
	// A backup that already has its trigger on a ReplicationSource goes
	// first, as one started before the controller took Leases does.
	backing, err := otherMover(ctx, r.Reader, run.Namespace, item.Name, settings.Secret, backupMover)
	if err != nil {
		return "", "", err
	}
	if backing != "" {
		return backupv1alpha1.ReasonSourceBusy, backing, nil
	}
	err = nothingWrittenTo(item.Name, r.recheckJobSnapshot(ctx, run, *item, settings.Secret))
	if done, err := settled(item, err); done {
		return "", "", err
	}
	_, err = settled(item, r.createJob(ctx, run, index, item, settings))
	return "", "", err
}

// takeOverJob gives an item the restore Job an earlier pass created
// for it, when there is one.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - index is the item's position in status.items.
//   - item is the volume item that has no Job yet: a Pending in-place item,
//     or the Running item of an into restore. takeOverJob updates it in
//     place.
//
// It returns true when a Job holds the item's name or the item already
// records one, so the pass starts nothing for it, and false when no Job
// holds the name. With true it may return a *refusalError, which the caller
// fails the item with (see settled). A failed read comes back as a plain
// error.
//
// An item that records a Job UID already had its Job, so it moves to
// Running and is never given a second one: Stop gates only on the pods of
// the recorded UID. A Job the run created under the item's name (see
// jobOfRun) means an earlier pass ran every start check, created the Job,
// and lost the status write that recorded it. The item takes it over,
// records its name and UID in the same status write, and moves to Running,
// but only when the Job's snapshot-id annotation is the full ID the item
// recorded. A Job with another ID is the controller's own bug: the refusal
// has reason RestoreJobFailed, the item keeps the Job's name and UID, and
// the run stops that Job like any other. A Job the run did not create under
// that name gives a refusal with reason RestoreJobRefused, and the run
// leaves that Job alone.
func (r *RestoreRunReconciler) takeOverJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (bool, error) {
	if item.JobUID != "" {
		item.Phase = backupv1alpha1.ItemRunning
		return true, nil
	}
	job, err := r.lostJob(ctx, run, index)
	if job == nil || err != nil {
		return false, err
	}
	if !jobOfRun(job, run) {
		return true, refuse(backupv1alpha1.ItemReasonRestoreJobRefused,
			"a Job named %s that the run did not create holds the name of the item's restore Job; the run started no restore and leaves that Job alone", job.Name)
	}
	item.Phase, item.Job, item.JobUID = backupv1alpha1.ItemRunning, job.Name, job.UID
	if found := job.Annotations[restorejob.AnnotationSnapshotID]; found != item.SnapshotID {
		return true, refuse(backupv1alpha1.ItemReasonRestoreJobFailed,
			"restore Job %s restores snapshot %s, and the run selected %s; the run stops it", job.Name, found, item.SnapshotID)
	}
	return true, nil
}

// followJob reads a Running volume item's restore Job, in place or into a
// new claim, and records how it ended.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - item is the Running volume item, which names its Job and the Job's
//     UID. followJob updates it in place.
//
// It returns why the Job's newest pod has not started, when the Job still
// runs and its pod waits (see restorejob.Read), and nil otherwise. The error
// is a *refusalError or a *restorejob.FailureError when the item fails,
// which the caller records (see settled), and a plain error from a failed
// read, which leaves the item as it was.
//
// A Job that is gone, or whose name now holds a Job with another UID, was
// deleted before it finished: the run records an item's end before it
// deletes the Job. The refusal has reason RestoreJobDeleted, and the run
// stops the Job by its recorded UID like any other, so its pods, which keep
// that UID, hold the app until they have ended. It never creates a second
// Job for the item. A Job the run no longer controls is refused with reason
// RestoreJobFailed and stopped the same way. Otherwise the Job's terminal
// conditions decide (see recordJobEnd).
func (r *RestoreRunReconciler) followJob(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (*restorejob.Waiting, error) {
	job, err := r.jobs().GetJob(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Job})
	switch {
	case apierrors.IsNotFound(err):
		return nil, refuse(backupv1alpha1.ItemReasonRestoreJobDeleted,
			"the restore Job %s was deleted before it finished", item.Job)
	case err != nil:
		return nil, fmt.Errorf("get restore Job %s: %w", item.Job, err)
	case job.UID != item.JobUID:
		return nil, refuse(backupv1alpha1.ItemReasonRestoreJobDeleted,
			"the restore Job %s was deleted before it finished, and a Job with another UID holds its name now", item.Job)
	case !metav1.IsControlledBy(job, run):
		return nil, refuse(backupv1alpha1.ItemReasonRestoreJobFailed,
			"the restore Job %s is no longer controlled by the run", item.Job)
	}
	pods, err := r.jobs().ListJobPods(ctx, run.Namespace, job.UID)
	if err != nil {
		return nil, fmt.Errorf("list the pods of restore Job %s: %w", job.Name, err)
	}
	return r.recordJobEnd(ctx, run, item, restorejob.Read(job, pods))
}

// recordJobEnd records on a volume item how its restore Job stands.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - item is the Running volume item, updated in place.
//   - outcome is what restorejob.Read found in the Job and its pods.
//
// It returns the outcome's waiting reason while the Job runs, and nil
// otherwise. The error is a *refusalError with reason ClaimLost, or the
// outcome's *restorejob.FailureError, when the item fails (see settled), and
// a plain error from a failed read of the claim or the Leases, which leaves
// the item Running.
//
// Only the Job's terminal conditions decide. Complete=True means restic
// exited 0 for exactly the item's snapshot, and the Job controller adds it
// only once the pod has ended; the item succeeds when the claim is still the
// one the run checked or created (see claimLostError), and fails with reason
// ClaimLost otherwise. Failed=True fails it with the
// *restorejob.FailureError, reason RestoreJobFailed, whose message carries
// restic's exit code, its meaning and restic's last lines. A Job that has
// neither keeps the item Running, and its message shows why the newest pod
// waits, if it does. The message is for a person and decides nothing.
func (r *RestoreRunReconciler) recordJobEnd(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, outcome restorejob.Outcome) (*restorejob.Waiting, error) {
	switch outcome.State {
	case restorejob.Succeeded:
		if err := r.claimLostError(ctx, run, *item); err != nil {
			return nil, err
		}
		item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
	case restorejob.Failed:
		return nil, outcome.Failure
	case restorejob.Running:
		item.Message = ""
		if outcome.Waiting != nil {
			item.Message = outcome.Waiting.String()
		}
		return outcome.Waiting, nil
	}
	return nil, nil
}

// claimLostError checks that the claim a volume item's restore Job wrote
// into is still the one the run restored into.
//
// Parameters:
//   - run is the RestoreRun the item belongs to. spec.into says which rule
//     applies.
//   - item is the volume item; its name is the claim's.
//
// It returns nil while the claim is the run's, a *refusalError with reason
// ClaimLost when it is not, and a plain error from a failed read of the
// claim or the Leases, which leaves the item as it was.
//
// An in-place item's claim is the one the run checked and took its claim
// Lease on (see inPlaceClaimLost). An into item's claim is the one the run
// created and controls (see claimLost). The run took its claim Lease before
// it created that claim, on the source claim when there is one, so the
// in-place rule would take the run's own new claim for a replaced one.
func (r *RestoreRunReconciler) claimLostError(ctx context.Context, run *backupv1alpha1.RestoreRun, item backupv1alpha1.RestoreItem) error {
	if run.Spec.Into != "" {
		return r.claimLost(ctx, run)
	}
	return r.inPlaceClaimLost(ctx, run, item.Name)
}
