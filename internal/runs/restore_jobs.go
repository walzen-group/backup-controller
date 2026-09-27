package runs

import (
	"context"
	"fmt"
	"slices"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The code in this file runs a volume restore, in place or into a new
// claim, through the controller's own restore Job (internal/restorejob): it
// creates the Job for the snapshot the run's checks selected, reads how the
// Job ended, and stops it.

// jobs returns the API the run's restore Jobs are created, read and stopped
// through. Its reads go through the reconciler's Reader, which reads the API
// server directly: the stop gate gives the app back once every pod it reads
// has ended, and a cache that lags behind would show a pod as ended, or miss
// a new one, while it still writes.
func (r *RestoreRunReconciler) jobs() restorejob.API {
	return restorejob.NewAPI(r.Reader, r.Client)
}

// jobName returns the name of the restore Job of one item of a run.
//
// Parameters:
//   - uid is the run's UID, which binds the name to the run.
//   - index is the item's position in status.items, which tells the run's
//     Jobs apart.
//
// The name is restore-, the run's full UID, a dash and the position. It is
// at most 48 characters, so it needs no shortening, and a run finds its Job
// again by it after a restart.
func jobName(uid types.UID, index int) string {
	return fmt.Sprintf("restore-%s-%d", uid, index)
}

// jobRef returns the Ref an item recorded for its restore Job, which Stop
// decides on.
//
// Parameters:
//   - run is the RestoreRun, whose namespace holds the Job.
//   - item is the volume item, which records the Job's name and UID.
func jobRef(run *backupv1alpha1.RestoreRun, item backupv1alpha1.RestoreItem) restorejob.Ref {
	return restorejob.Ref{Namespace: run.Namespace, Name: item.Job, UID: item.JobUID}
}

// lostJob reads the Job that holds the name of an item's restore Job.
//
// Parameters:
//   - run is the RestoreRun; its UID and the index name the Job.
//   - index is the item's position in status.items.
//
// It returns the Job, or nil when there is none, and an error from any
// other failed read. The Job may be one the run did not create; the
// caller checks (see jobOfRun).
func (r *RestoreRunReconciler) lostJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int) (*batchv1.Job, error) {
	name := jobName(run.UID, index)
	job, err := r.jobs().GetJob(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name})
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("get restore Job %s: %w", name, err)
	}
	return job, nil
}

// jobOfRun reports whether the Job that holds the name of one of a run's
// restore Jobs is one the run created.
//
// Parameters:
//   - job is the Job under the name, which jobName binds to the run.
//   - run is the RestoreRun.
//
// It returns true when the Job's restore-run label
// (restorejob.LabelRestoreRun), which Build sets, holds the run's UID. The
// name and the label decide, and the controller reference does not: a
// delete of the run with --cascade=orphan makes the garbage collector remove
// that reference while the run's finalizer still holds the run, and the run
// must still stop the Job then.
func jobOfRun(job *batchv1.Job, run *backupv1alpha1.RestoreRun) bool {
	return job.Labels[restorejob.LabelRestoreRun] == string(run.UID)
}

// recheckJobSnapshot lists the repository again right before the run
// creates a volume item's restore Job, and refuses the item when the
// snapshot its checks recorded is no longer there.
//
// Parameters:
//   - run is the RestoreRun; the Secret is read in its namespace.
//   - item is the volume item. item.SnapshotID is the full ID the checks
//     recorded, which the Job restores.
//   - secretName names the repository Secret.
//
// It returns nil when the repository still holds a snapshot with exactly
// that ID. It returns a *refusalError with reason SnapshotChanged when it
// does not: the message says a quiesced backup rewrote the snapshot, when a
// listed snapshot records it as its original, and that a backup's retention
// forgot it otherwise. It refuses with reason RepositorySecretMissing a
// Secret that is gone, and with reason RestoreJobRefused an item that
// records no full ID. A Secret that can't be read for another reason, and a
// failed listing, come back as a plain error, and the caller retries.
//
// The Job restores the snapshot by its full ID, which restic resolves to
// that one snapshot file and nothing else, so no other snapshot of the same
// second can take its place.
func (r *RestoreRunReconciler) recheckJobSnapshot(ctx context.Context, run *backupv1alpha1.RestoreRun, item backupv1alpha1.RestoreItem, secretName string) error {
	if item.SnapshotID == "" {
		return refuse(backupv1alpha1.ItemReasonRestoreJobRefused,
			"the item records no full snapshot ID (snapshot %q), so the run has no snapshot to restore by ID", item.Snapshot)
	}
	snapshots, err := r.repositorySnapshots(ctx, run, secretName)
	switch {
	case err != nil:
		return err
	case slices.ContainsFunc(snapshots, func(s restic.Snapshot) bool { return s.ID == item.SnapshotID }):
		return nil
	}
	for _, s := range snapshots {
		if s.Original == item.SnapshotID {
			return refuse(backupv1alpha1.ItemReasonSnapshotChanged,
				"snapshot %s, which the checks selected, was rewritten as %s at %s by a quiesced backup after the checks",
				item.Snapshot, s.ShortID(), s.Time.UTC().Format(time.RFC3339))
		}
	}
	taken := "time not recorded"
	if item.SnapshotTime != nil {
		taken = item.SnapshotTime.UTC().Format(time.RFC3339)
	}
	return refuse(backupv1alpha1.ItemReasonSnapshotChanged,
		"snapshot %s (%s), which the checks selected, is no longer in the repository; a backup's retention (restic forget) removed it after the checks",
		item.Snapshot, taken)
}

// createJob creates a volume item's restore Job, in place or into the claim
// an into restore created, and moves the item to Running.
//
// Parameters:
//   - run is the RestoreRun, which controls the Job.
//   - index is the item's position in status.items, which names the Job.
//   - item is the volume item. The Job restores item.SnapshotID into the
//     claim item.Name, and createJob records the Job's name and UID on it.
//   - settings are the repository and mover settings, from startRefusal
//     or intoChecks.
//
// It returns nil once the Job exists and the item names it. It returns the
// *restorejob.SpecError of a spec Build refuses, and a *refusalError with
// reason RestoreJobRefused when the API server refuses the create as
// Forbidden or Invalid (the admission policy on the controller's Jobs, or a
// spec the server rejects). Both say that nothing was written to the claim
// (see nothingWrittenTo). Any other failed read or create comes back as a
// plain error for a retry; a create that went through although it answered
// with an error is taken over by the next pass (see takeOverJob).
//
// The Job restores with --delete, so the claim ends up holding exactly the
// snapshot. It runs as root with the capabilities to restore file ownership
// when the run's namespace lets movers run privileged, the way VolSync
// decides it for its own mover (see restorejob.PrivilegedMovers).
func (r *RestoreRunReconciler) createJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem, settings restoreSettings) error {
	namespace := &corev1.Namespace{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Name: run.Namespace}, namespace); err != nil {
		return fmt.Errorf("get Namespace %s: %w", run.Namespace, err)
	}
	job, err := restorejob.Build(restorejob.Spec{
		Name:                  jobName(run.UID, index),
		Namespace:             run.Namespace,
		Origin:                restorejob.Origin{Kind: restorejob.OriginRestoreRun, UID: run.UID},
		Owner:                 *metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind(backupv1alpha1.KindRestoreRun)),
		SnapshotID:            item.SnapshotID,
		Claim:                 item.Name,
		Repository:            settings.Secret,
		Image:                 r.RestoreImage,
		Delete:                true,
		Privileged:            restorejob.PrivilegedMovers(namespace),
		SecurityContext:       settings.MoverSecurityContext,
		PodLabels:             settings.MoverPodLabels,
		CacheStorageClassName: settings.CacheStorageClassName,
		CacheCapacity:         settings.CacheCapacity,
	})
	if err != nil {
		return nothingWrittenTo(item.Name, err)
	}
	err = r.jobs().CreateJob(ctx, job)
	switch {
	case apierrors.IsForbidden(err) || apierrors.IsInvalid(err):
		return nothingWrittenTo(item.Name, refuse(backupv1alpha1.ItemReasonRestoreJobRefused,
			"the API server refused to create restore Job %s: %v", job.Name, err))
	case err != nil:
		return fmt.Errorf("create restore Job %s: %w", job.Name, err)
	}
	item.Phase, item.Job, item.JobUID, item.Message = backupv1alpha1.ItemRunning, job.Name, job.UID, ""
	return nil
}

// settleJobs reads the restore Job of each unfinished volume item of a run
// that ends early, so each item records how its Job really ended.
//
// Parameters:
//   - run is the RestoreRun that aborts or times out. Its items are updated
//     in place, and the caller writes the status.
//
// It returns, by the item's position in status.items, the waiting reason of
// each Job that still runs and whose newest pod waits, which the caller
// adds to that item's message once it has failed it. A failed read comes
// back as an error, and the run retries its end.
//
// A deadline that passes just as a Job completes then records the item
// Succeeded, and one whose Job failed records restic's exit code; only an
// item whose Job has not ended gets the run's message (P11). An item that
// records no Job first takes over the one a pass created and lost the
// status write of (see settleJob), so a lost create is recorded the same
// way.
func (r *RestoreRunReconciler) settleJobs(ctx context.Context, run *backupv1alpha1.RestoreRun) (map[int]*restorejob.Waiting, error) {
	waits := map[int]*restorejob.Waiting{}
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindClaim || finished(*item) {
			continue
		}
		waiting, err := r.settleJob(ctx, run, i, item)
		if err != nil {
			return nil, err
		}
		if waiting != nil {
			waits[i] = waiting
		}
	}
	return waits, nil
}

// settleJob reads how the restore Job of one unfinished volume item stands,
// for settleJobs.
//
// Parameters:
//   - run is the RestoreRun that ends early.
//   - index is the item's position in status.items, which names its Job.
//   - item is the volume item, updated in place.
//
// It returns the waiting reason of a Job that still runs and whose newest
// pod waits, and nil otherwise. A failed read comes back as an error.
//
// An item that records no Job takes over the one the run created under its
// name, as a pass would (see takeOverJob), and an item with no such Job
// has nothing to read. A refused takeover fails the item with its reason.
// The Job an item then records is read and its end recorded (see
// followJob).
func (r *RestoreRunReconciler) settleJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (*restorejob.Waiting, error) {
	if item.JobUID == "" {
		taken, err := r.takeOverJob(ctx, run, index, item)
		if done, err := settled(item, err); done || !taken {
			return nil, err
		}
	}
	waiting, err := r.followJob(ctx, run, item)
	if _, err := settled(item, err); err != nil {
		return nil, err
	}
	return waiting, nil
}

// addWaits adds to the message of each item a run failed as it ended early
// why its restore Job's newest pod was still waiting.
//
// Parameters:
//   - run is the RestoreRun, after failRemainingItems.
//   - waits are the waiting reasons settleJobs returned, by item position.
func addWaits(run *backupv1alpha1.RestoreRun, waits map[int]*restorejob.Waiting) {
	for i, waiting := range waits {
		run.Status.Items[i].Message += "; the restore Job's pod was " + waiting.String()
	}
}

// jobLeft is a restore Job the run has stopped that may still write.
type jobLeft struct {
	// item is the name of the item the Job restored.
	item string
	// state is how far Stop got, for the wait message.
	state restorejob.StopState
}

// jobList holds the restore Jobs a run has stopped that may still write, in
// the order of status.items.
type jobList []jobLeft

// message returns the Ready message for a run that waits for the first Job
// in the list. The caller asks only for a list that is not empty.
func (l jobList) message() string {
	j := l[0]
	return fmt.Sprintf("waiting for the restore Job of claim %s, which the run stopped, to end: %s. "+
		"The run gives the app back and lets other runs at the claim only after that", j.item, j.state)
}

// stopJobs stops the restore Jobs of the volume items a run is done with,
// and reports those that may still write (rule X2).
//
// Parameters:
//   - run is the RestoreRun. The item of a Job that is stopped has its Job
//     UID cleared in place, and the caller writes the status.
//   - which chooses the items to stop: finished from work, so a Job that
//     still restores keeps running, and anyItem from finish and finalize.
//
// It returns the chosen Jobs that are not stopped yet, in the order of
// status.items, and an error that is a *releaseError naming the Job when a
// read, the suspend, the pod list or the delete fails. The caller keeps
// waiting on an error, and the next pass tries again.
//
// A chosen volume item that never named a Job gets the one the run created
// under the item's name, if there is one, whatever the item's phase (see
// nameLostJob). Each Job is stopped through restorejob.Stop by the
// recorded Ref: suspended, then deleted once no pod of its UID can still
// write. An item keeps its Job's name and UID until Stop reports it
// stopped, so every pass can stop it again, and holderLive keeps the
// item's Leases while it names a UID. The stop then clears only the UID:
// the name stays as the record that the item had a Job and the run stopped
// it, so a Job that holds the name later is never taken for a lost one.
func (r *RestoreRunReconciler) stopJobs(ctx context.Context, run *backupv1alpha1.RestoreRun, which func(backupv1alpha1.RestoreItem) bool) (jobList, error) {
	var left jobList
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindClaim || !which(*item) {
			continue
		}
		if err := r.nameLostJob(ctx, run, i, item); err != nil {
			return nil, jobReleaseError(jobName(run.UID, i), err)
		}
		if item.JobUID == "" {
			continue
		}
		state, err := restorejob.Stop(ctx, r.jobs(), jobRef(run, *item))
		if err != nil {
			return nil, jobReleaseError(item.Job, err)
		}
		if !state.Stopped {
			left = append(left, jobLeft{item: item.Name, state: state})
			continue
		}
		item.JobUID = ""
	}
	return left, nil
}

// nameLostJob records on a volume item that never named a restore Job the
// Job the run created under the item's name, so the run stops it.
//
// Parameters:
//   - run is the RestoreRun.
//   - index is the item's position in status.items, which names the Job
//     (see jobName).
//   - item is the volume item, updated in place.
//
// It returns an error from a failed read, and changes nothing then.
//
// A create can store its Job although the status write that would record it
// was lost, or although the API server answered it with an error: on a
// request that outlives its deadline the server answers 504 Timeout and
// may still store the object. The Job can then appear after a later pass
// failed the item, so the item's phase does not tell whether it has a lost
// Job, and an item of any phase is looked at. An item that names a Job is
// skipped. It either has its Job, or it had one that the run stopped, since
// createJob and takeOverJob record the name and the UID together and a stop
// clears only the UID (see stopJobs). A Job under the name of a stopped Job
// is one the run never recorded, and the run leaves it alone. A Job the run
// did not create is left alone too (see jobOfRun).
func (r *RestoreRunReconciler) nameLostJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) error {
	if item.JobUID != "" || item.Job != "" {
		return nil
	}
	job, err := r.lostJob(ctx, run, index)
	if job == nil || err != nil || !jobOfRun(job, run) {
		return err
	}
	item.Job, item.JobUID = job.Name, job.UID
	return nil
}

// jobReleaseError returns the error for a restore Job the run could not
// stop.
//
// Parameters:
//   - name is the Job's name, which the action and the advice name.
//   - err is what failed.
//
// It returns a *releaseError, which releaseFailure puts on the run's Ready
// condition.
func jobReleaseError(name string, err error) error {
	return &releaseError{
		action: "stop its restore Job " + name,
		advice: fmt.Sprintf("Fix the cause, or delete Job %s with its pods yourself (kubectl delete job %s --cascade=foreground) "+
			"and make sure none of its pods still runs; the run then finishes by itself.", name, name),
		err: err,
	}
}
