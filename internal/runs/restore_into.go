package runs

import (
	"context"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The code in this file runs an into restore (spec.into) that
// planIntoNewClaim has checked: it creates the new claim and the restore Job
// that fills it, and follows the Job to its end, through the same restore
// Job code as an in-place item (restore_jobs.go and restore_volume.go).

// The kinds of the objects named spec.into, as the refusals of an into
// restore name them (see notCreatedByRun).
const (
	// kindClaim is the kind word of a PersistentVolumeClaim.
	kindClaim = "claim"
	// kindVolumeRestore is the kind word of a VolumeRestore.
	kindVolumeRestore = "VolumeRestore"
)

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
		return r.takeOverInto(ctx, run, item)
	}
	if deadline, over := r.overdue(run); over {
		return r.timeOut(ctx, run, intoTimedOut(run.Spec.Into, deadline, run.Status.Conditions))
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

// takeOverInto records the restore Job that an into restore took over from
// a pass that lost its status write.
//
// Parameters:
//   - run is the RestoreRun.
//   - item is its single item, which now records the Job's name and UID.
//     takeOverInto updates it in place.
//
// It returns what the pass returns (see restoreIntoEmptyClaim).
//
// The Job is still suspended, because only a pass that recorded its UID
// resumes it. A claim of the spec.into name that the run did not create
// can be there in place of the claim of the lost pass. Then the item fails
// with reason IntoClaimTaken (see intoTaken), and the run stops the Job
// before it writes. Otherwise the status write records the Job.
func (r *RestoreRunReconciler) takeOverInto(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (ctrl.Result, error) {
	if done, err := settled(item, r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, kindClaim)); done {
		return r.endInto(ctx, run, err)
	}
	return after(pollInterval, r.recordJob(ctx, run))
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
	if err := r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, kindClaim); err != nil {
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
// The status write that follows records the Job's name and UID together,
// and a later pass resumes the Job, which Build created suspended (see
// followIntoJob).
func (r *RestoreRunReconciler) createInto(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, settings restoreSettings) (ctrl.Result, error) {
	err := nothingWrittenTo(run.Spec.Into, r.recheckJobSnapshot(ctx, run, *item, settings.Secret))
	if done, err := settled(item, err); done {
		return r.endInto(ctx, run, err)
	}
	if done, err := settled(item, r.createOwned(ctx, run, scratchClaim(run, settings), &corev1.PersistentVolumeClaim{}, kindClaim)); done {
		return r.endInto(ctx, run, err)
	}
	if done, err := settled(item, r.createJob(ctx, run, 0, item, settings)); done {
		return r.endInto(ctx, run, err)
	}
	return after(pollInterval, r.recordJob(ctx, run))
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
// also checks that the claim is still the run's (see claimLost), so a
// claim deleted or replaced mid-restore fails the item with reason
// ClaimLost and the run stops the Job. A run past its deadline ends with
// reason TimedOut. Otherwise a Job still suspended since its create is
// resumed, once the stored run, read again right before the resume, still
// has the item Running on that Job (see resumeJob). A refused resume fails the
// item with reason RestoreJobRefused and ends the run Failed. The item's
// message shows why the Job's pod waits, if it does, and the status is
// written when it changed.
func (r *RestoreRunReconciler) followIntoJob(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (ctrl.Result, error) {
	seen, err := r.followJob(ctx, run, item)
	if err == nil && !finished(*item) {
		err = r.claimLost(ctx, run, seen.unresumed != nil)
	}
	if done, err := settled(item, err); done || finished(*item) {
		return r.endInto(ctx, run, err)
	}
	if deadline, over := r.overdue(run); over {
		return r.timeOut(ctx, run, intoTimedOut(run.Spec.Into, deadline, run.Status.Conditions))
	}
	if seen.unresumed != nil {
		if done, err := settled(item, r.resumeJob(ctx, run, 0, *item, seen.unresumed)); done {
			return r.endInto(ctx, run, err)
		}
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

// recordJob writes the status of an into restore once its item names its
// restore Job, and moves a run that waited back to Running.
//
// Parameters:
//   - run is the RestoreRun, whose item records the Job's name and UID.
//
// It returns the error of the status write. The write records the Job's
// name and UID together, and the next pass resumes the Job; a lost write
// leaves the Job suspended for the next pass's takeover (see takeOverJob).
func (r *RestoreRunReconciler) recordJob(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if run.Status.Phase == backupv1alpha1.RunPhaseWaiting {
		run.Status.Phase = backupv1alpha1.RunPhaseRunning
		backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
			fmt.Sprintf("restoring into claim %s", run.Spec.Into))
	}
	return r.writeStatus(ctx, run)
}

// planIntoNewClaim checks an into restore before it creates anything, and
// starts the run when the check passes.
//
// Parameters:
//   - run is the RestoreRun with an empty phase and spec.into set. It
//     restores from the backups of spec.claim, or from spec.repository.
//
// It returns the result of the pass: a run whose check passed is moved to
// Running, with status.startedAt set and its single item Running on the
// selected snapshot, and requeued after a second; a run that ends here
// returns what finish returns; a run that waits for a backup returns what
// waitAtChecks returns. A failed read, and a failed listing of the
// repository, come back as an error for a retry (see planFailed).
//
// The check selects the snapshot the restore would use and records its full
// ID, short ID and time on the item (see recordSnapshot); the restore Job
// that restoreIntoEmptyClaim creates restores exactly that full ID. The
// item names no Job yet. Before it selects the snapshot, planIntoNewClaim
// waits, as plan does, while a backup of the source claim or the repository
// is in progress (see otherMover and waitAtChecks).
//
// A spec that can't work ends the run as Failed with reason Invalid: a claim
// named spec.into that exists and that the run did not create (see
// notCreatedByRun); for a restore from spec.claim, a VolumeRestore of that
// name, which describes the backups of a claim of that name; a source claim
// or VolumeRestore that is missing; a restoreAsOf that doesn't parse; or a
// restore from spec.repository alone without spec.intoSize. The run writes
// only into a claim it creates itself, so it never overwrites or takes over
// one it finds. A run with no snapshot in reach, or whose snapshot
// selectSnapshot refuses, ends with reason NoBackupInReach.
func (r *RestoreRunReconciler) planIntoNewClaim(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	run.Status.Target = run.Spec.Into
	err := r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, kindClaim)
	if err == nil && run.Spec.Claim != "" {
		err = r.intoTaken(ctx, run, &backupv1alpha1.VolumeRestore{}, kindVolumeRestore)
	}
	if asRunRefusal(err) {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if asRunRefusal(err) {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if settings.Capacity == nil && run.Spec.IntoSize == nil {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid,
			"spec.intoSize is required when spec.repository names the source, because there is no source claim to copy a size from")
	}
	at, err := target(run)
	if err != nil {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	// The snapshot is selected once no backup of the repository is in
	// progress, so after that backup's forget and retime.
	busy, err := otherMover(ctx, r.Reader, run.Namespace, run.Spec.Claim, settings.Secret, backupMover)
	if err != nil {
		return ctrl.Result{}, err
	}
	if busy.held() {
		return r.waitAtChecks(ctx, run, busy)
	}
	snapshot, err := r.selectSnapshot(ctx, run, settings.Secret, at, false)
	if _, refused := asItemFailure(err); refused {
		return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	item := backupv1alpha1.RestoreItem{
		Kind: backupv1alpha1.ItemKindClaim, Name: run.Spec.Into, Phase: backupv1alpha1.ItemRunning,
	}
	recordSnapshot(&item, snapshot)
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = &now
	run.Status.Items = []backupv1alpha1.RestoreItem{item}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("restoring into claim %s", run.Spec.Into))
	return after(time.Second, r.writeStatus(ctx, run))
}

// intoTimedOut returns the message of an into restore that ran past its
// deadline.
//
// Parameters:
//   - into is the claim the run restores into, spec.into, which the message
//     names.
//   - deadline is the run's deadline as overdue returns it, which the
//     message gives.
//   - conditions are the run's status.conditions before it ends, which
//     show the wait the run was in, if any.
//
// The message ends with the SourceBusy or VolSyncUnsupported wait the run
// was in, as timedOutMessage's does (see waitedFor). timeOut puts the
// message on the run and on its item, and records it in status.ending,
// which a later pass ends the run with.
func intoTimedOut(into string, deadline time.Time, conditions []metav1.Condition) string {
	return fmt.Sprintf("claim %s had not been restored by %s", into, deadline.Format(time.RFC3339)) + waitedFor(conditions)
}

// intoTaken reads the object named spec.into into object, and refuses it
// when it exists and the run did not create it.
//
// Parameters:
//   - object is an empty claim or VolumeRestore, of the kind to read.
//   - kind is kindClaim or kindVolumeRestore, for the message.
//
// It returns nil when the object does not exist or the run controls it, and
// the *refusalError from notCreatedByRun, with reason IntoClaimTaken,
// otherwise. The read goes through the uncached Reader, so an object
// created a moment ago is seen. A failed read comes back as a plain error,
// and the caller retries.
func (r *RestoreRunReconciler) intoTaken(ctx context.Context, run *backupv1alpha1.RestoreRun, object client.Object, kind string) error {
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	if err := r.Reader.Get(ctx, key, object); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("get %s %s: %w", kind, key, err)
	}
	return notCreatedByRun(run, kind, object)
}

// createOwned creates object, which carries the run's controller reference,
// and makes sure the object of that name is one the run controls.
//
// Parameters:
//   - object is the claim to create.
//   - existing is an empty object of the same kind, which the stored object
//     is read into when the create finds one.
//   - kind is kindClaim, for the message.
//
// It returns nil once an object of that name exists that the run controls,
// and the *refusalError from notCreatedByRun, with reason IntoClaimTaken,
// for any other; nothing is written to that one. A failed create or read
// comes back as a plain error, and the caller retries.
//
// A create that finds the name taken reads the stored object. The run's own,
// left by a pass whose create went through but whose answer was lost, is
// fine.
func (r *RestoreRunReconciler) createOwned(ctx context.Context, run *backupv1alpha1.RestoreRun, object, existing client.Object, kind string) error {
	err := r.Create(ctx, object)
	if err == nil {
		return nil
	}
	if !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create %s %s/%s: %w", kind, object.GetNamespace(), object.GetName(), err)
	}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(object), existing); err != nil {
		return fmt.Errorf("get %s %s/%s: %w", kind, object.GetNamespace(), object.GetName(), err)
	}
	return notCreatedByRun(run, kind, existing)
}

// claimLost checks that the claim an into restore writes into is still the
// run's own.
//
// Parameters:
//   - run is the RestoreRun. spec.into names the claim, and the claim is the
//     run's own when the run is its controller (see notCreatedByRun).
//   - unstarted is true when the restore Job is still suspended since its
//     create, so it has not run. The error then says that the Job wrote
//     nothing.
//
// It returns nil while the run's own claim is there, and a *claimLostError
// for an into item when the claim is gone (lossGone), is being deleted
// (lossDeleting), or is controlled by something other than the run
// (lossNotOwned). asItemFailure fails the item with it, with reason
// ClaimLost. A failed read that is not NotFound comes back as a plain
// error, and the caller leaves the item as it was.
//
// An into restore checks it on every pass once its restore Job exists (see
// followIntoJob and checkClaimKept), so a Job that finished never counts
// as a restore into a claim that is no longer the run's. inPlaceClaimLost
// does the same for an in-place item.
func (r *RestoreRunReconciler) claimLost(ctx context.Context, run *backupv1alpha1.RestoreRun, unstarted bool) error {
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	err := r.Reader.Get(ctx, key, claim)
	switch {
	case apierrors.IsNotFound(err):
		return &claimLostError{claim: run.Spec.Into, loss: lossGone, unstarted: unstarted}
	case err != nil:
		return fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	case claim.DeletionTimestamp != nil:
		return &claimLostError{claim: run.Spec.Into, loss: lossDeleting, unstarted: unstarted}
	case !metav1.IsControlledBy(claim, run):
		return &claimLostError{claim: run.Spec.Into, loss: lossNotOwned, unstarted: unstarted}
	}
	return nil
}

// waitForBackup keeps an into restore from creating anything while a backup
// of its claim or its repository is in progress.
//
// Parameters:
//   - claim is the name of the claim the run takes a Lease on: the source
//     claim, or the new claim for a restore from spec.repository alone.
//   - secret is the name of the repository Secret.
//
// The run calls it right before it creates its first object. It first takes
// the Leases of the claim and the repository for the run's item (see
// acquireLeases), then looks for a backup's mover object (see otherMover),
// which catches a backup started before the controller took Leases.
//
// It returns true when the run has to wait. It has then moved the run to
// Waiting with reason SourceBusy and a message naming the run that holds a
// Lease or the BackupRun. A repository Secret that does not exist comes back
// as the refusal acquireLeases gives, and the caller ends the run. A failed
// read, write or status write comes back as an error, which the caller
// retries.
func (r *RestoreRunReconciler) waitForBackup(ctx context.Context, run *backupv1alpha1.RestoreRun, claim, secret string) (bool, error) {
	busy, err := acquireLeases(ctx, r.Client, r.Reader, leaseRequest{
		holder:    leaseHolder{kind: backupv1alpha1.KindRestoreRun, run: run, item: run.Status.Items[0].Name},
		namespace: run.Namespace, claim: claim, secret: secret,
	})
	if err != nil {
		return false, err
	}
	if busy.held() {
		return true, r.waitFor(ctx, run, busy.readyReason(), busy.text)
	}
	backing, err := otherMover(ctx, r.Reader, run.Namespace, claim, secret, backupMover)
	if err != nil || !backing.held() {
		return false, err
	}
	return true, r.waitFor(ctx, run, backing.readyReason(), backing.text)
}

// notCreatedByRun refuses an object named spec.into that the run does not
// control.
//
// Parameters:
//   - run is the RestoreRun. It controls an object whose controller
//     reference carries its UID, as scratchClaim sets it; a run created
//     again under the same name does not.
//   - kind is kindClaim or kindVolumeRestore, for the message.
//   - object is the object as read from the API server.
//
// It returns nil when the run controls the object, and otherwise a
// *refusalError with reason IntoClaimTaken whose message names the object
// and says how to choose another name.
//
// When another RestoreRun controls the object, the message names that run:
// the garbage collector deletes the object when that run is deleted, so a
// run that took the object over would lose it under its own success. A
// VolumeRestore describes the backups of the claim of its name, so its name
// is taken by a claim too, even while that claim does not exist.
func notCreatedByRun(run *backupv1alpha1.RestoreRun, kind string, object metav1.Object) error {
	if metav1.IsControlledBy(object, run) {
		return nil
	}
	owner := ""
	if ref := metav1.GetControllerOf(object); ref != nil && ref.Kind == backupv1alpha1.KindRestoreRun &&
		ref.APIVersion == backupv1alpha1.GroupVersion.String() {
		owner = fmt.Sprintf("; it belongs to RestoreRun %s, which deletes it when it is deleted", ref.Name)
	}
	if kind == kindClaim {
		return refuse(backupv1alpha1.ItemReasonIntoClaimTaken, "claim %s already exists and this run did not create it%s. "+
			"spec.into names a new claim for the run to create, and a restore never writes into a claim it did not create. "+
			"Choose a name no claim in this namespace has. To overwrite an existing claim, restore it in place with spec.claim.",
			object.GetName(), owner)
	}
	return refuse(backupv1alpha1.ItemReasonIntoClaimTaken, "%[1]s %[2]s already exists and this run did not create it%[3]s. "+
		"A %[1]s describes the backups of the claim of its name, so that name belongs to another claim, and "+
		"spec.into names a new claim for the run to create. Choose a name no claim or %[1]s in this namespace has.",
		kind, object.GetName(), owner)
}

// scratchClaim builds the plain claim that an into restore creates and its
// mover fills.
//
// Parameters:
//   - run is the RestoreRun. The claim takes its name from spec.into, and
//     spec.intoSize, when set, replaces the source claim's size.
//   - settings are the source claim's settings from repositoryFor. A
//     restore from spec.repository alone has none.
//
// The claim has no data source: the run's restore Job writes the selected
// snapshot into it (see createInto). It takes the source claim's size and
// class, so the copy is provisioned the way the original was. It is an
// ordinary dynamic claim. Deleting it deletes its dataset too,
// which makes a scratch copy cheap to throw away. The run is its controller
// owner.
//
// It also takes the source claim's selected node,
// volume.kubernetes.io/selected-node. On a WaitForFirstConsumer class, which
// is every class on the walzen cluster, the provisioner creates the volume
// on that node at once, and the scheduler places the mover pod, which has no
// other pod to follow, on the node that holds the volume. The copy is made to
// be compared against the original, and both datasets belong on the pool
// that already holds one of them. A restore from spec.repository alone has
// no source node, and the scheduler places the claim with the mover pod,
// which is its first consumer.
func scratchClaim(run *backupv1alpha1.RestoreRun, settings restoreSettings) *corev1.PersistentVolumeClaim {
	size := settings.Capacity
	if run.Spec.IntoSize != nil {
		size = run.Spec.IntoSize
	}

	annotations := map[string]string{}
	if settings.SelectedNode != "" {
		annotations[selectedNodeAnnotation] = settings.SelectedNode
	}

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        run.Spec.Into,
			Namespace:   run.Namespace,
			Annotations: annotations,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind(backupv1alpha1.KindRestoreRun)),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: settings.StorageClassName,
		},
	}
	if size != nil {
		claim.Spec.Resources = corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: *size},
		}
	}
	return claim
}
