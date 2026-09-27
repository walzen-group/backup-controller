package runs

import (
	"context"
	"fmt"
	"slices"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
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
// It returns the hold of the item while it waits, and the zero hold
// otherwise: a hold of kind holdClaimInUse that names the pod while a pod
// mounts the claim, and a hold of kind holdSourceBusy that names the other
// run while another run holds the Lease of the claim or its repository (see
// acquireLeases) or a backup of either is in progress (see otherMover). The item stays Pending in each of these
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
func (r *RestoreRunReconciler) startJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (hold, error) {
	if taken, err := r.takeOverJob(ctx, run, index, item); taken || err != nil {
		_, err = settled(item, err)
		return hold{}, err
	}
	holder, err := claimHolder(ctx, r.Reader, run.Namespace, item.Name)
	if err != nil {
		return hold{}, fmt.Errorf("look for a pod holding claim %s: %w", item.Name, err)
	}
	if holder != "" {
		return claimInUse("claim %s is mounted by pod %s; stop the workload and this restore starts on its own", item.Name, holder), nil
	}
	settings, err := r.startRefusal(ctx, run, item.Name)
	if done, err := settled(item, err); done {
		return hold{}, err
	}
	busy, err := r.takeSource(ctx, run, item.Name, item.Name, settings.Secret)
	if done, err := settled(item, nothingWrittenTo(item.Name, err)); done {
		return hold{}, err
	}
	if busy.held() {
		return busy, nil
	}
	err = nothingWrittenTo(item.Name, r.recheckJobSnapshot(ctx, run, *item, settings.Secret))
	if done, err := settled(item, err); done {
		return hold{}, err
	}
	_, err = settled(item, r.createJob(ctx, run, index, item, settings))
	return hold{}, err
}

// takeSource takes the Leases of the claim and the repository a volume
// item's restore Job writes or reads, right before the run creates it.
//
// Parameters:
//   - run is the RestoreRun that takes the Leases.
//   - item is the name of the volume item, which the Leases list.
//   - claim is the claim the Lease is taken on: the item's own claim in
//     place, and the source claim, or else the new claim, for an into
//     restore.
//   - secret names the repository Secret.
//
// It returns the hold of kind holdSourceBusy that names the other run while
// another run holds one of the Leases (see acquireLeases) or a backup of the
// claim or the repository has its mover object already (see otherMover),
// and the zero hold once the run holds both Leases. A repository Secret
// that does not exist comes back as the refusal of acquireLeases, and a
// failed read or write as a plain error.
//
// The Leases make the restore and a backup of the claim or its repository
// exclusive: of two runs that get here in the same instant, only one creates
// each Lease. A backup whose trigger is already on a ReplicationSource goes
// first, as one started before the controller took Leases does.
func (r *RestoreRunReconciler) takeSource(ctx context.Context, run *backupv1alpha1.RestoreRun, item, claim, secret string) (hold, error) {
	busy, err := acquireLeases(ctx, r.Client, r.Reader, leaseRequest{
		holder:    leaseHolder{kind: backupv1alpha1.KindRestoreRun, run: run, item: item},
		namespace: run.Namespace, claim: claim, secret: secret,
	})
	if err != nil || busy.held() {
		return busy, err
	}
	return otherMover(ctx, r.Reader, run.Namespace, claim, secret, backupMover)
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
// where a later pass resumes the suspended Job (see resumeJob), but only when the Job's snapshot-id annotation is the full ID the item
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

// followed is what followJob found in a Running volume item's restore Job.
type followed struct {
	// waiting is why the Job's newest pod has not started, when the Job
	// still runs and its pod waits (see restorejob.Read), and nil otherwise.
	waiting *restorejob.Waiting
	// unresumed is the Job as followJob read it when it is still suspended
	// since its create (see restorejob.AwaitsResume), and nil otherwise.
	unresumed *batchv1.Job
}

// followJob reads a Running volume item's restore Job, in place or into a
// new claim, and records how it ended.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - item is the Running volume item, which names its Job and the Job's
//     UID. followJob updates it in place.
//
// It returns what it found in a Job that still runs: why its newest pod
// waits, and the Job when it still awaits the run's resume, which the caller
// resumes only in a pass that goes on with the restore (see resumeJob). The
// error is a *refusalError or a *restorejob.FailureError when the item
// fails, which the caller records (see settled), and a plain error from a
// failed read, which leaves the item as it was.
//
// A Job that is gone, or whose name now holds a Job with another UID, was
// deleted before it finished: the run records an item's end before it
// deletes the Job. The refusal has reason RestoreJobDeleted, and the run
// stops the Job by its recorded UID like any other, so its pods, which keep
// that UID, hold the app until they have ended. It never creates a second
// Job for the item. A Job the run no longer controls is refused with reason
// RestoreJobFailed and stopped the same way. Otherwise the Job's terminal
// conditions decide (see recordJobEnd).
func (r *RestoreRunReconciler) followJob(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (followed, error) {
	job, err := r.jobs().GetJob(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Job})
	switch {
	case apierrors.IsNotFound(err):
		return followed{}, jobDeleted(item.Job, false)
	case err != nil:
		return followed{}, fmt.Errorf("get restore Job %s: %w", item.Job, err)
	case job.UID != item.JobUID:
		return followed{}, jobDeleted(item.Job, true)
	case !metav1.IsControlledBy(job, run):
		return followed{}, refuse(backupv1alpha1.ItemReasonRestoreJobFailed,
			"the restore Job %s is no longer controlled by the run", item.Job)
	}
	pods, err := r.jobs().ListJobPods(ctx, run.Namespace, job.UID)
	if err != nil {
		return followed{}, fmt.Errorf("list the pods of restore Job %s: %w", job.Name, err)
	}
	waiting, err := r.recordJobEnd(ctx, run, item, restorejob.Read(job, pods))
	if err != nil || finished(*item) || !restorejob.AwaitsResume(job) {
		return followed{waiting: waiting}, err
	}
	return followed{waiting: waiting, unresumed: job}, nil
}

// resumeJob resumes a Running volume item's restore Job, which Build
// created suspended, so its pod can run.
//
// Parameters:
//   - run is the RestoreRun as this pass read it, from the informer cache.
//   - index is the item's position in status.items, where the stored run
//     is checked for the same item.
//   - item is the Running volume item, which names its Job and the Job's
//     UID. The caller passes it only in a pass that goes on with the
//     restore.
//   - job is the item's Job as followJob just read it, still suspended.
//
// It returns nil once the resume went through. It returns a plain error,
// for a retry, when the stored run no longer goes on with the item's Job
// (see storedRunGoesOn) or can't be read, and when the patch fails for a
// reason a retry may fix, such as a 500 from an admission webhook that
// can't be reached, a 503 or a 504. When the API server refuses the resume
// as Forbidden or Invalid, it returns a *refusalError with reason
// RestoreJobRefused that says nothing was written to the claim. The resume
// is a merge patch that carries the Job's UID, and the API server answers
// such a patch on a Job created again under the same name with 422 Invalid,
// so a Job replaced since the read is refused the same way and the run
// stops the recorded one.
//
// The pass's copy of the run can lag the stored run, and a resume is a
// write that no later status conflict undoes. So the stored run is read
// through the uncached Reader right before the patch, and the Job is
// resumed only while that run still has the item Running on this Job. The
// patch carries the Job's UID, so a Job created under the name since the
// read is left alone. A Job no stored status records, such as one whose
// create answered with an error and was stored later, stays suspended with
// no pod, and the run stops it (see stopJobs). A refused resume leaves the
// Job suspended too, and the run stops it once the item has failed. A pass
// that ends the run never resumes: a stop right after a resume could miss
// a pod the Job controller creates for it (see restorejob.Stop).
func (r *RestoreRunReconciler) resumeJob(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item backupv1alpha1.RestoreItem, job *batchv1.Job) error {
	goesOn, err := r.storedRunGoesOn(ctx, run, index, item)
	switch {
	case err != nil:
		return err
	case !goesOn:
		return fmt.Errorf("the stored RestoreRun %s/%s no longer has item %s Running on restore Job %s; "+
			"this pass read an older copy of the run, resumes nothing and is retried", run.Namespace, run.Name, item.Name, job.Name)
	}
	err = r.jobs().ResumeJob(ctx, job)
	switch {
	case apierrors.IsForbidden(err) || apierrors.IsInvalid(err):
		return refusedResume(item, job, err)
	case err != nil:
		return fmt.Errorf("resume restore Job %s: %w", job.Name, err)
	}
	return nil
}

// storedRunGoesOn reports whether the stored RestoreRun still restores a
// volume item on the restore Job the pass is about to resume.
//
// Parameters:
//   - run is the RestoreRun as this pass read it.
//   - index is the item's position in status.items.
//   - item is the item as this pass read it, Running and naming its Job.
//
// It returns true when the run read through the uncached Reader is the
// same run, is not being deleted, has recorded no ending and has not
// finished, and its item at that position is the same item, Running, with
// the same Job name and UID. It returns false for a run that is gone, and
// a failed read comes back as an error.
func (r *RestoreRunReconciler) storedRunGoesOn(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item backupv1alpha1.RestoreItem) (bool, error) {
	stored := &backupv1alpha1.RestoreRun{}
	err := r.Reader.Get(ctx, client.ObjectKeyFromObject(run), stored)
	switch {
	case apierrors.IsNotFound(err):
		return false, nil
	case err != nil:
		return false, fmt.Errorf("read RestoreRun %s/%s before resuming restore Job %s: %w", run.Namespace, run.Name, item.Job, err)
	case stored.UID != run.UID || stored.DeletionTimestamp != nil || stored.Status.Ending != nil ||
		stored.Status.Phase.Finished() || index >= len(stored.Status.Items):
		return false, nil
	}
	s := stored.Status.Items[index]
	return s.Name == item.Name && s.Phase == backupv1alpha1.ItemRunning && s.Job == item.Job && s.JobUID == item.JobUID, nil
}

// refusedResume returns the refusal for a resume the API server refused.
//
// Parameters:
//   - item is the volume item, whose claim the message names.
//   - job is the item's Job, which never ran.
//   - err is the API server's refusal, which the message quotes.
//
// It returns a *refusalError with reason RestoreJobRefused that says
// nothing was written to the claim.
func refusedResume(item backupv1alpha1.RestoreItem, job *batchv1.Job, err error) error {
	return nothingWrittenTo(item.Name, refuse(backupv1alpha1.ItemReasonRestoreJobRefused,
		"the API server refused to resume restore Job %s, which never ran: %v", job.Name, err))
}

// jobDeleted returns the refusal for a volume item whose restore Job was
// deleted before it finished.
//
// Parameters:
//   - name is the name of the Job the item records.
//   - replaced says that a Job with another UID holds the name now, which
//     the message adds.
//
// It returns a *refusalError with reason RestoreJobDeleted.
func jobDeleted(name string, replaced bool) error {
	if replaced {
		return refuse(backupv1alpha1.ItemReasonRestoreJobDeleted,
			"the restore Job %s was deleted before it finished, and a Job with another UID holds its name now", name)
	}
	return refuse(backupv1alpha1.ItemReasonRestoreJobDeleted, "the restore Job %s was deleted before it finished", name)
}

// recordJobEnd records on a volume item how its restore Job stands.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - item is the Running volume item, updated in place.
//   - outcome is what restorejob.Read found in the Job and its pods.
//
// It returns the outcome's waiting reason while the Job runs, and nil
// otherwise. The error is a *claimLostError (reason ClaimLost), or the
// outcome's *restorejob.FailureError, when the item fails (see settled), and
// a plain error from a failed read of the claim or the Leases, which leaves
// the item Running.
//
// Only the Job's terminal conditions decide. Complete=True means restic
// exited 0 for exactly the item's snapshot, and the Job controller adds it
// only once the pod has ended; the item succeeds when the claim is still the
// one the run checked or created (see checkClaimKept), and fails with reason
// ClaimLost otherwise. Failed=True fails it with the
// *restorejob.FailureError, reason RestoreJobFailed, whose message carries
// restic's exit code, its meaning and restic's last lines. A Job that has
// neither keeps the item Running, and its message shows why the newest pod
// waits, if it does, or that the Job has not been resumed yet. The message
// is for a person and decides nothing.
func (r *RestoreRunReconciler) recordJobEnd(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, outcome restorejob.Outcome) (*restorejob.Waiting, error) {
	switch outcome.State {
	case restorejob.Succeeded:
		if err := r.checkClaimKept(ctx, run, *item); err != nil {
			return nil, err
		}
		item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
	case restorejob.Failed:
		return nil, outcome.Failure
	case restorejob.Running:
		item.Message = ""
		switch {
		case outcome.Waiting != nil:
			item.Message = outcome.Waiting.String()
		case outcome.Starting:
			item.Message = "starting: the restore Job was created suspended, and the run resumes it once the status records it"
		}
		return outcome.Waiting, nil
	}
	return nil, nil
}

// checkClaimKept checks that the claim a volume item's restore Job wrote
// into is still the one the run restored into.
//
// Parameters:
//   - run is the RestoreRun the item belongs to. spec.into says which rule
//     applies.
//   - item is the volume item; its name is the claim's.
//
// It returns nil while the claim is the run's, a *claimLostError (reason
// ClaimLost) when it is not, and a plain error from a failed read of the
// claim or the Leases, which leaves the item as it was.
//
// An in-place item's claim is the one the run checked and took its claim
// Lease on (see inPlaceClaimLost). An into item's claim is the one the run
// created and controls (see claimLost). The run took its claim Lease before
// it created that claim, on the source claim when there is one, so the
// in-place rule would take the run's own new claim for a replaced one.
func (r *RestoreRunReconciler) checkClaimKept(ctx context.Context, run *backupv1alpha1.RestoreRun, item backupv1alpha1.RestoreItem) error {
	if run.Spec.Into != "" {
		return r.claimLost(ctx, run, false)
	}
	return r.inPlaceClaimLost(ctx, run, item.Name)
}

// restoreVolume moves one in-place volume restore a step further, through
// the item's restore Job.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - index is the item's position in status.items. It goes into the name of
//     the item's restore Job (see jobName).
//   - item is the volume item, which restoreVolume updates in place.
//
// It returns the hold of a Pending item that waits (see startJob), and the
// zero hold otherwise. It returns an error, which leaves the item as it
// was, when an API call fails for a reason a retry may fix.
//
// A Pending item starts its restore Job once nothing holds it back (see
// startJob). A Running item follows its Job to its end (see followVolume).
// The run stops the Job once the item's end is in the status (see
// stopJobs).
func (r *RestoreRunReconciler) restoreVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) (hold, error) {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		return r.startJob(ctx, run, index, item)
	case backupv1alpha1.ItemRunning:
		return hold{}, r.followVolume(ctx, run, index, item)
	default:
		// An item in any other phase has finished, so there is nothing
		// left to restore.
		return hold{}, nil
	}
}

// followVolume moves a Running in-place volume item a step further on its
// restore Job.
//
// Parameters:
//   - run is the RestoreRun the item belongs to.
//   - index is the item's position in status.items.
//   - item is the Running volume item, which followVolume updates in place.
//
// It returns nil when the step is done, which includes an item it failed
// (see settled). It returns an error, which leaves the item as it was, when
// an API call fails for a reason a retry may fix.
//
// The item follows its Job to its end (see followJob). Its Job, created
// suspended, is resumed in a pass that does not end the run, once the
// stored run, read again right before the resume, has the item Running on
// that Job (see resumeJob).
func (r *RestoreRunReconciler) followVolume(ctx context.Context, run *backupv1alpha1.RestoreRun, index int, item *backupv1alpha1.RestoreItem) error {
	seen, err := r.followJob(ctx, run, item)
	if done, err := settled(item, err); done || seen.unresumed == nil {
		return err
	}
	_, err = settled(item, r.resumeJob(ctx, run, index, *item, seen.unresumed))
	return err
}

// startRefusal checks whether the item's claim or repository settings refuse
// a Pending in-place restore, and reads those settings when they don't.
// restoreVolume calls it before it takes the Leases, and quiesce calls it in
// its pre-check, so an item that cannot start fails before the app is
// stopped for it.
//
// Parameters:
//   - run is the asking run; its namespace is read, and spec.repository and
//     spec.moverSecurityContext are used the way repositoryFor uses them.
//   - claimName names the item's claim, which is also the item's name.
//
// It returns the settings from repositoryFor when the restore can go on. A
// claim that is being deleted (reason ClaimDeleting), and a refusal from
// repositoryFor (a claim that is gone, or a VolumeRestore that is missing
// when the run names no repository), come back as a *refusalError that
// says nothing was written to the claim (see nothingWrittenTo); the caller
// fails the item with it through failRestoreItem. A claim being deleted is
// refused because the mover would write into a claim that is about to go,
// and the scheduler does not place a pod whose claim is being deleted
// (podHasPVCs, in the PreFilter of the volumebinding plugin). A failed read
// comes back as a plain error, and the caller leaves the item Pending.
func (r *RestoreRunReconciler) startRefusal(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string) (restoreSettings, error) {
	claim := &corev1.PersistentVolumeClaim{}
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim)
	switch {
	case apierrors.IsNotFound(err):
		// repositoryFor refuses the claim that is gone with its message.
	case err != nil:
		return restoreSettings{}, fmt.Errorf("get PersistentVolumeClaim %s/%s: %w", run.Namespace, claimName, err)
	case claim.DeletionTimestamp != nil:
		return restoreSettings{}, nothingWrittenTo(claimName,
			refuse(backupv1alpha1.ItemReasonClaimDeleting, "claim %s is being deleted", claimName))
	}
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	return settings, nothingWrittenTo(claimName, err)
}

// inPlaceClaimLost checks that the claim an in-place item restored is
// still the claim the run checked and took its Lease on.
//
// Parameters:
//   - run is the RestoreRun. Its claim Leases are the ones labelled with its
//     UID.
//   - claimName names the claim, which is also the item's name.
//
// It returns nil while that claim is there, and a *claimLostError, which
// fails the item with reason ClaimLost, when the claim is gone, is being
// deleted, or is another claim.
// A run that holds no claim Lease for the item can't tell which claim the
// restore Job wrote into, and gets that error too, since the item must not
// succeed without that evidence. A failed read of the claim or the Leases
// comes back as a plain error, and the caller leaves the item as it was.
//
// The claim the run checked is the one it took its claim Lease on right
// before it created the item's restore Job: the Lease is named after that
// claim's UID (see claimLeaseName) and lists the item. A claim whose UID is
// not among the run's claim Leases for the item was created after that
// create, so it replaced the claim the run checked. The Job mounts the claim
// by name, so a pod of it that started after the replacement may have
// written into the new claim, and the message asks for that claim's data to
// be checked.
//
// A claim deleted while a pod of its restore Job mounts it stays,
// Terminating, until the pod is gone (pvc-protection), so the Job can
// complete into a claim that is about to go.
func (r *RestoreRunReconciler) inPlaceClaimLost(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string) error {
	leases := &coordinationv1.LeaseList{}
	if err := r.Reader.List(ctx, leases, client.InNamespace(run.Namespace), client.MatchingLabels{labelLeaseHolderUID: string(run.UID)}); err != nil {
		return fmt.Errorf("list the Leases of RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	prefix := claimLeaseName("")
	var leased []string
	for _, lease := range leases.Items {
		if strings.HasPrefix(lease.Name, prefix) && slices.Contains(leaseItems(&lease), claimName) {
			leased = append(leased, strings.TrimPrefix(lease.Name, prefix))
		}
	}
	if len(leased) == 0 {
		return &claimLostError{claim: claimName, loss: lossUnleased}
	}

	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return &claimLostError{claim: claimName, loss: lossGone}
		}
		return fmt.Errorf("get claim %s/%s: %w", run.Namespace, claimName, err)
	}
	if claim.DeletionTimestamp != nil {
		return &claimLostError{claim: claimName, loss: lossDeleting}
	}
	if !slices.Contains(leased, string(claim.UID)) {
		return &claimLostError{claim: claimName, loss: lossReplaced, found: string(claim.UID), leased: leased}
	}
	return nil
}

// claimHolder returns the name of a pod that mounts a claim.
//
// Parameters:
//   - c lists the pods. The run passes its uncached Reader: without
//     spec.quiesce this check is the only thing that keeps a second writer
//     off the volume, and an informer cache that has not yet seen a new pod
//     would report the claim free.
//   - namespace is the run's namespace, which holds the claim and its pods.
//   - claim is the name of the claim an in-place restore is about to write
//     into.
//
// It returns the name of the first pod in the namespace, in the order the
// list gives, whose volumes name the claim, and "" when none does. A pod in
// phase Succeeded or Failed has no container left and doesn't count. A
// failed list comes back as an error.
func claimHolder(ctx context.Context, c client.Reader, namespace, claim string) (string, error) {
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
				return pod.Name, nil
			}
		}
	}
	return "", nil
}
