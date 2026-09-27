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

// The code in this file runs an in-place volume restore through the
// controller's own restore Job (internal/restorejob): it creates the Job for
// the snapshot the run's checks selected, reads how the Job ended, and stops
// it. An into restore still writes through a ReplicationDestination.

// jobs returns the API the run's restore Jobs are created, read and stopped
// through. Its reads go through the reconciler's Reader, which reads the API
// server directly: the stop gate gives the app back once every pod it reads
// has ended, and a cache that lags behind would show a pod as ended, or miss
// a new one, while it still writes.
func (r *RestoreRunReconciler) jobs() restorejob.API {
	return restorejob.NewAPI(r.Reader, r.Client)
}

// jobName returns the name of the restore Job of one item of a run:
// restore-, the run's full UID, a dash and the item's position in
// status.items. The name is at most 48 characters, so it needs no
// shortening, and a run finds its Job again by it after a restart.
func jobName(uid types.UID, index int) string {
	return fmt.Sprintf("restore-%s-%d", uid, index)
}

// jobRef returns the Ref an item recorded for its restore Job, which Stop
// decides on.
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
// other failed read. The Job may be one the run does not control; the
// caller checks.
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

// recheckJobSnapshot lists the repository again right before the run
// creates an in-place item's restore Job, and refuses the item when the
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
	snapshots, missing, err := r.listRepository(ctx, run, secretName)
	switch {
	case err != nil:
		return err
	case missing != "":
		return refuse(backupv1alpha1.ItemReasonRepositorySecretMissing, "%s", missing)
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

// createJob creates an in-place item's restore Job and moves the item to
// Running.
//
// Parameters:
//   - run is the RestoreRun, which controls the Job.
//   - index is the item's position in status.items, which names the Job.
//   - item is the volume item. The Job restores item.SnapshotID into the
//     claim item.Name, and createJob records the Job's name and UID on it.
//   - settings are the repository and mover settings from startRefusal.
//
// It returns nil once the Job exists and the item names it. It returns the
// *restorejob.SpecError of a spec Build refuses, and a *refusalError with
// reason RestoreJobRefused, which says nothing was written to the claim,
// when the API server refuses the create as Forbidden or Invalid (the
// admission policy on the controller's Jobs, or a spec the server rejects).
// Any other failed read or create comes back as a plain error for a retry;
// a create that went through although it answered with an error is taken
// over by the next pass (see takeOverJob).
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
		return err
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

// settleJobs reads the restore Job of each Running in-place item of a run
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
// item whose Job has not ended gets the run's message (P11).
func (r *RestoreRunReconciler) settleJobs(ctx context.Context, run *backupv1alpha1.RestoreRun) (map[int]*restorejob.Waiting, error) {
	waits := map[int]*restorejob.Waiting{}
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindClaim || item.Phase != backupv1alpha1.ItemRunning || item.JobUID == "" {
			continue
		}
		waiting, err := r.followJob(ctx, run, item)
		if done, err := settled(item, err); done && err != nil {
			return nil, err
		}
		if waiting != nil {
			waits[i] = waiting
		}
	}
	return waits, nil
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

// stopJobs stops the restore Jobs of the in-place items a run is done with,
// and reports those that may still write (rule X2).
//
// Parameters:
//   - run is the RestoreRun. The item of a Job that is stopped has its Job
//     name and UID cleared in place, and the caller writes the status.
//   - which chooses the items to stop: finished from work, so a Job that
//     still restores keeps running, and anyItem from finish and finalize.
//
// It returns the chosen Jobs that are not stopped yet, in the order of
// status.items, and an error that is a *releaseError naming the Job when a
// read, the suspend, the pod list or the delete fails. The caller keeps
// waiting on an error, and the next pass tries again.
//
// A chosen volume item that names no Job gets the one the run controls
// under the item's name, if there is one (see lostJob): a pass that created
// it and lost the status write left it unnamed. Each Job is stopped through
// restorejob.Stop by the recorded Ref: suspended, then deleted once no pod of
// its UID can still write. An item keeps its Job's name and UID until Stop
// reports it stopped, so every pass can stop it again, and holderLive keeps
// the item's Leases while it names one.
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
		item.Job, item.JobUID = "", ""
	}
	return left, nil
}

// nameLostJob records on an item that names no restore Job the Job the run
// controls under the item's name, so the run stops it.
//
// Parameters:
//   - run is the RestoreRun.
//   - index is the item's position in status.items.
//   - item is the volume item, updated in place.
//
// It returns an error from a failed read, and changes nothing then.
func (r *RestoreRunReconciler) nameLostJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) error {
	if item.JobUID != "" {
		return nil
	}
	job, err := r.lostJob(ctx, run, index)
	if job == nil || err != nil || !metav1.IsControlledBy(job, run) {
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

// moverStops is what a run's stop of its movers left: the movers of its
// ReplicationDestinations, which into restores still use, and its restore
// Jobs.
type moverStops struct {
	// destinations are the destinations' movers that are not gone yet.
	destinations moverList
	// jobs are the restore Jobs that may still write.
	jobs jobList
}

// waiting reports whether a stopped mover may still write, so the run must
// wait before it gives anything back.
func (s moverStops) waiting() bool {
	return len(s.destinations) > 0 || len(s.jobs) > 0
}

// message returns the Ready message of the wait. The caller asks only when
// waiting reports true.
func (s moverStops) message() string {
	if len(s.jobs) > 0 {
		return s.jobs.message()
	}
	return s.destinations.message()
}

// stopMovers stops the movers of the items a run is done with: it deletes
// their ReplicationDestinations (see removeDestinations) and stops their
// restore Jobs (see stopJobs).
//
// Parameters:
//   - run is the RestoreRun. Items are updated in place, and the caller
//     writes the status.
//   - which chooses the items, as removeDestinations and stopJobs take it.
//
// It returns what is left of both, and the first error either returns.
func (r *RestoreRunReconciler) stopMovers(ctx context.Context, run *backupv1alpha1.RestoreRun, which func(backupv1alpha1.RestoreItem) bool) (moverStops, error) {
	destinations, err := r.removeDestinations(ctx, run, which)
	if err != nil {
		return moverStops{}, err
	}
	jobs, err := r.stopJobs(ctx, run, which)
	return moverStops{destinations: destinations, jobs: jobs}, err
}
