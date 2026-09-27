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

// The code in this file checks an into restore (spec.into) and gives it the
// checks right before its restore Job. The run then goes through the same
// restore Job code as an in-place run (see work, startJob and followVolume):
// its single item names the new claim, which startJob creates right before
// the Job.

// The kinds of the objects named spec.into, as the refusals of an into
// restore name them (see notCreatedByRun).
const (
	// kindClaim is the kind word of a PersistentVolumeClaim.
	kindClaim = "claim"
	// kindVolumeRestore is the kind word of a VolumeRestore.
	kindVolumeRestore = "VolumeRestore"
)

// intoChecks runs the checks an into restore makes right before it takes
// the Leases and creates anything.
//
// Parameters:
//   - run is the RestoreRun, with no restore Job yet.
//
// It returns the repository and mover settings. It returns a
// *refusalError, which the caller fails the item with (see settled), for a
// source claim, VolumeRestore or repository Secret that is gone, each saying
// nothing was written to the claim spec.into names, and for a claim of that
// name the run did not create, with reason IntoClaimTaken (see intoTaken). A
// failed read comes back as a plain error for a retry.
//
// repositoryFor never refuses the spec here: the CRD's CEL rule "into needs
// claim or repository" keeps a spec that names neither out of the API
// server, on create and on every update.
func (r *RestoreRunReconciler) intoChecks(ctx context.Context, run *backupv1alpha1.RestoreRun) (restoreSettings, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		return settings, nothingWrittenTo(run.Spec.Into, err)
	}
	// A claim that appeared since the plan's checks ends the run before it
	// waits for anything: it is not the run's to write into.
	return settings, r.intoTaken(ctx, run, &corev1.PersistentVolumeClaim{}, kindClaim)
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
// that startJob creates restores exactly that full ID. The
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
	var settings restoreSettings
	if err == nil {
		settings, err = repositoryFor(ctx, r.Reader, run.Namespace, run.Spec.Claim, run.Spec.Repository, run.Spec.MoverSecurityContext)
	}
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
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, runningMessage(run))
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
// It returns nil while the run's own claim is there, and a *refusalError
// for an into item when the claim is gone, is being deleted, or is
// controlled by something other than the run (see claimLostRefusal).
// failRestoreItem fails the item with it, with reason
// ClaimLost. A failed read that is not NotFound comes back as a plain
// error, and the caller leaves the item as it was.
//
// An into restore checks it on every pass once its restore Job exists (see
// followVolume and checkClaimKept), so a Job that finished never counts
// as a restore into a claim that is no longer the run's. inPlaceClaimLost
// does the same for an in-place item.
func (r *RestoreRunReconciler) claimLost(ctx context.Context, run *backupv1alpha1.RestoreRun, unstarted bool) error {
	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	err := r.Reader.Get(ctx, key, claim)
	switch {
	case apierrors.IsNotFound(err):
		return claimLostRefusal(unstarted, "claim %s was deleted before its restore Job ran, so the Job wrote nothing", lostGone, run.Spec.Into)
	case err != nil:
		return fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	case claim.DeletionTimestamp != nil:
		return claimLostRefusal(unstarted, "claim %s is being deleted, and its restore Job has not run, so the Job wrote nothing", lostDeleting, run.Spec.Into)
	case !metav1.IsControlledBy(claim, run):
		return claimLostRefusal(unstarted, "claim %s is no longer controlled by the run, and its restore Job has not run, so the Job wrote nothing",
			"claim %[1]s is no longer controlled by the run, so it may not be the claim the run created. The restore Job mounts "+
				"claim %[1]s by name, so it may have written into it; check its data, and create a new RestoreRun to restore it", run.Spec.Into)
	}
	return nil
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
// snapshot into it (see startJob). It takes the source claim's size and
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

// runningMessage returns the Ready message of a Running run with nothing to
// wait for: "restoring", or "restoring into claim <into>" for an into
// restore.
func runningMessage(run *backupv1alpha1.RestoreRun) string {
	if run.Spec.Into != "" {
		return fmt.Sprintf("restoring into claim %s", run.Spec.Into)
	}
	return "restoring"
}

// succeededMessage returns the Ready message of a run whose items all
// succeeded: "every item holds the restored data", or "claim <into> holds
// the restored data" for an into restore.
func succeededMessage(run *backupv1alpha1.RestoreRun) string {
	if run.Spec.Into != "" {
		return fmt.Sprintf("claim %s holds the restored data", run.Spec.Into)
	}
	return "every item holds the restored data"
}
