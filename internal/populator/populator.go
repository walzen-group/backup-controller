// Package populator holds the callbacks that the volume populator library
// calls to fill a claim whose data source is a VolumeRestore.
package populator

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	internalvolsync "github.com/walzen-group/backup-controller/internal/volsync"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Operations are the Kubernetes API calls the callbacks make. The binary
// implements them with a client (clientOperations in cmd/backup-controller),
// and the tests use an in-memory fake.
type Operations interface {
	GetReplicationDestination(ctx context.Context, namespace, name string) (*volsyncv1alpha1.ReplicationDestination, error)
	CreateReplicationDestination(ctx context.Context, rd *volsyncv1alpha1.ReplicationDestination) error
	DeleteReplicationDestination(ctx context.Context, namespace, name string) error
	GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error)
	CreateSecret(ctx context.Context, secret *corev1.Secret) error
	DeleteSecret(ctx context.Context, namespace, name string) error
	SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error
	// UpdateVolumeRestore writes the VolumeRestore's metadata, which is how
	// the callbacks add and remove Finalizer. The write carries the
	// resourceVersion of the object passed, so it fails with a conflict when
	// the object has changed since it was read.
	UpdateVolumeRestore(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error
}

// Finalizer is the finalizer the populator keeps on a VolumeRestore while a
// claim has an entry in its status.claims. The library looks the VolumeRestore
// up before it handles a deleted claim, and when the VolumeRestore is gone it
// returns without calling Cleanup and leaves its own finalizer on the claim.
// This finalizer keeps the VolumeRestore in place until Cleanup has deleted
// the claim's ReplicationDestination and Secret copy.
const Finalizer = "backup.wlz.li/volume-populator"

// Callbacks holds the three functions that the volume populator library calls
// for a claim whose data source is a VolumeRestore: Populate, Complete and
// Cleanup. One Callbacks serves one controller namespace, where the prime
// claims and the ReplicationDestinations live.
type Callbacks struct {
	operations Operations
	namespace  string
	snapshots  restic.Lister
}

// New returns the callbacks for one controller namespace.
//
// Parameters:
//   - operations makes the Kubernetes API calls.
//   - namespace is the controller namespace, where the ReplicationDestinations
//     and the repository Secret copies are created.
//   - snapshots lists a repository's snapshots. Populate uses it to check a
//     restore pinned to a moment, by the claim's backup.wlz.li/restore-as-of
//     annotation or by the VolumeRestore's spec.restoreAsOf. When it's nil,
//     Populate skips that check.
func New(operations Operations, namespace string, snapshots restic.Lister) *Callbacks {
	return &Callbacks{operations: operations, namespace: namespace, snapshots: snapshots}
}

// pinnedOutOfReach checks a restore pinned to a moment. When no snapshot is at
// or before that moment, VolSync prints "No eligible snapshots found",
// restores nothing and reports success, and the claim would bind an empty
// volume. This check catches that before the restore starts.
//
// Parameters:
//   - vr and claim give the moment, which RestoreAsOf resolves the same way
//     it does for the destination: the claim's backup.wlz.li/restore-as-of
//     annotation first, then the VolumeRestore's spec.restoreAsOf.
//   - repo is the app's repository Secret, which the lister opens.
//
// It returns a reason when the moment isn't an RFC 3339 time, or when no
// snapshot in the repository reaches it. The reason names where the moment
// came from. It returns "" when neither the claim nor the VolumeRestore pins a
// moment, when the moment is in reach, or when the Callbacks have no lister.
// It returns an error when the snapshots can't be listed.
func (c *Callbacks) pinnedOutOfReach(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim, repo *corev1.Secret) (string, error) {
	pin := internalvolsync.RestoreAsOf(vr, claim)
	if pin == nil || c.snapshots == nil {
		return "", nil
	}
	value := *pin
	source := "spec.restoreAsOf"
	if _, ok := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; ok {
		source = backupv1alpha1.AnnotationRestoreAsOf
	}
	at, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return fmt.Sprintf("%s is %q, which is not an RFC 3339 time", source, value), nil
	}
	snapshots, err := c.snapshots.Snapshots(ctx, repo)
	if err != nil {
		return "", fmt.Errorf("list the snapshots in %s: %w", repo.Name, err)
	}
	if _, ok := restic.AtOrBefore(snapshots, at); ok {
		return "", nil
	}
	if len(snapshots) == 0 {
		return fmt.Sprintf("%s asks for %s and the repository holds no snapshot", source, value), nil
	}
	return fmt.Sprintf("%s asks for %s; the oldest snapshot, %s, is from %s",
		source, value, snapshots[0].ShortID(), snapshots[0].Time.UTC().Format(time.RFC3339)), nil
}

// Populate starts filling one claim from its VolumeRestore. The params the
// library passes carry the claim, the prime claim and the VolumeRestore.
//
// It first puts Finalizer on the VolumeRestore with holdVolumeRestore, and
// returns an error when it can't. Then it copies the repository Secret that
// the VolumeRestore names from the claim's namespace into the controller
// namespace, and creates the ReplicationDestination that restores into the
// prime claim. Both steps leave an object that already exists alone, so a
// repeated call creates nothing twice.
//
// Before it creates the destination, it checks a pinned restore with
// pinnedOutOfReach. When no snapshot reaches the pinned moment, it marks the
// claim Failed and sets Ready to False with reason NoBackupInReach in the
// VolumeRestore's status. Then it returns an error and creates no
// destination.
//
// When the destination already exists and its latest mover run failed, it
// leaves the status alone and returns nil. Complete, which the library calls
// next, reports that failure, and the status stays Failed with the mover's
// logs until a later sync succeeds.
//
// Otherwise it marks the claim Restoring and sets Ready to False with reason
// Restoring and a message that names the destination and its namespace. It
// writes the status only when that changed something, and returns nil.
func (c *Callbacks) Populate(ctx context.Context, params populatormachinery.PopulatorParams) error {
	if err := validateParams(params); err != nil {
		return err
	}
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	claim := params.Pvc
	prime := params.PvcPrime
	if err := c.holdVolumeRestore(ctx, vr); err != nil {
		return err
	}

	repo, err := c.operations.GetSecret(ctx, claim.Namespace, vr.Spec.Repository)
	if err != nil {
		return fmt.Errorf("get repository Secret %s/%s: %w", claim.Namespace, vr.Spec.Repository, err)
	}
	secretName := internalvolsync.SecretCopyName(claim.UID)
	if _, err := c.operations.GetSecret(ctx, c.namespace, secretName); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("check copied repository Secret %s/%s: %w", c.namespace, secretName, err)
		}
		if err := c.operations.CreateSecret(ctx, internalvolsync.SecretCopy(repo, claim.UID, c.namespace)); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create copied repository Secret %s/%s: %w", c.namespace, secretName, err)
		}
	}

	destinationName := "restore-" + internalvolsync.Trigger(claim)
	existing, err := c.operations.GetReplicationDestination(ctx, c.namespace, destinationName)
	if err == nil {
		// The library calls Complete right after this, with the same cached
		// VolumeRestore. While the mover keeps failing, Complete reports
		// RestoreFailed with the logs. Writing Restoring here would change the
		// status and bump its resource version, Complete's write would then
		// conflict, and the reason would flip on every pass.
		if _, failed := internalvolsync.Failure(existing); failed {
			return nil
		}
	} else {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get ReplicationDestination %s/%s: %w", c.namespace, destinationName, err)
		}
		reason, err := c.pinnedOutOfReach(ctx, vr, claim, repo)
		if err != nil {
			return err
		}
		if reason != "" {
			// The error leaves the claim Pending, and the library calls
			// Populate again. Once someone fixes the annotation or the
			// VolumeRestore's spec.restoreAsOf, or recreates the claim without
			// the pin, a later call creates the destination and the claim
			// fills.
			setClaimStatus(vr, claim, backupv1alpha1.RestorePhaseFailed)
			backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonNoBackupInReach, reason)
			if err := c.operations.SetStatus(ctx, vr); err != nil {
				return fmt.Errorf("set VolumeRestore status: %w", err)
			}
			return fmt.Errorf("claim %s/%s: %s", claim.Namespace, claim.Name, reason)
		}
		destination := internalvolsync.New(vr, claim, prime.Name, c.namespace)
		if err := c.operations.CreateReplicationDestination(ctx, destination); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create ReplicationDestination %s/%s: %w", c.namespace, destinationName, err)
		}
	}

	before := vr.Status.DeepCopy()
	setClaimStatus(vr, claim, backupv1alpha1.RestorePhaseRestoring)
	// The message names the namespace too. The destination is in the
	// controller's namespace, and a reader who looks in the app's namespace
	// won't find it.
	waiting := fmt.Sprintf("waiting for ReplicationDestination %s in %s", destinationName, c.namespace)
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoring, waiting)
	if equality.Semantic.DeepEqual(before, &vr.Status) {
		return nil
	}
	if err := c.operations.SetStatus(ctx, vr); err != nil {
		return fmt.Errorf("set VolumeRestore status: %w", err)
	}
	return nil
}

// Complete reports whether the restore of one claim has finished, which is
// when VolSync has recorded the claim's manual trigger in the destination's
// status.lastManualSync. It returns an error when it can't read the
// destination.
//
// When the destination's latest mover run failed, Complete marks the claim
// Failed and sets Ready to False with reason RestoreFailed and the mover's
// logs as the message. It writes that status and returns false.
func (c *Callbacks) Complete(ctx context.Context, params populatormachinery.PopulatorParams) (bool, error) {
	if err := validateParams(params); err != nil {
		return false, err
	}
	rdName := "restore-" + internalvolsync.Trigger(params.Pvc)
	rd, err := c.operations.GetReplicationDestination(ctx, c.namespace, rdName)
	if err != nil {
		return false, fmt.Errorf("get ReplicationDestination %s/%s: %w", c.namespace, rdName, err)
	}
	if reason, failed := internalvolsync.Failure(rd); failed {
		vr, err := decodeVolumeRestore(params)
		if err != nil {
			return false, err
		}
		setClaimStatus(vr, params.Pvc, backupv1alpha1.RestorePhaseFailed)
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoreFailed, reason)
		if err := c.operations.SetStatus(ctx, vr); err != nil {
			return false, fmt.Errorf("set failed VolumeRestore status: %w", err)
		}
		return false, nil
	}
	return internalvolsync.Complete(rd, internalvolsync.Trigger(params.Pvc)), nil
}

// Cleanup runs after a claim's restore has finished. It deletes the
// ReplicationDestination and the repository Secret copy that Populate created,
// and skips any that are already gone.
//
// It then removes the claim's entry from the VolumeRestore's status.claims.
// It sets Ready to True with reason Restored when no claim is left, and to
// False with reason Restoring while other claims are still being filled. It
// writes the status only when that changed something. When the VolumeRestore
// is gone by then, it returns nil, because nothing is left to report on.
//
// Once no claim is left in status.claims, it removes Finalizer from the
// VolumeRestore. A VolumeRestore deleted while a claim was being filled goes
// then.
func (c *Callbacks) Cleanup(ctx context.Context, params populatormachinery.PopulatorParams) error {
	if err := validateCleanupParams(params); err != nil {
		return err
	}
	claim := params.Pvc
	if err := c.operations.DeleteReplicationDestination(ctx, c.namespace, "restore-"+internalvolsync.Trigger(claim)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ReplicationDestination: %w", err)
	}
	if err := c.operations.DeleteSecret(ctx, c.namespace, internalvolsync.SecretCopyName(claim.UID)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete copied repository Secret: %w", err)
	}

	// The restore has ended, so the claim's entry goes. A VolumeRestore stays
	// in place after its restores end. Its status holds an entry only for a
	// claim being filled right now, and it reads Ready once no claim is.
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	before := vr.Status.DeepCopy()

	retireClaimStatus(vr, claim)
	if len(vr.Status.Claims) == 0 {
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionTrue, backupv1alpha1.ReasonRestored, "no claim is being restored")
	} else {
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoring, fmt.Sprintf("%d claim(s) still restoring", len(vr.Status.Claims)))
	}

	// The library has no early return for a claim it has already populated. It
	// walks the whole path on every resync, reaches the completion branch and
	// calls Cleanup again, for the life of the claim. Writing every time would
	// send one status update per volume every resync interval, for as long as
	// the claim exists, and none of them would change anything.
	if !equality.Semantic.DeepEqual(before, &vr.Status) {
		if err := c.operations.SetStatus(ctx, vr); err != nil {
			if apierrors.IsNotFound(err) {
				// The VolumeRestore is gone, and so is the status that
				// reported this claim. The Secret copy and the destination
				// are already deleted, which is all this claim needed.
				return nil
			}
			return fmt.Errorf("set VolumeRestore status: %w", err)
		}
	}

	// The status write above bumped the resource version in vr, so this
	// update doesn't conflict with it. It does conflict when another claim's
	// Populate has added an entry since the object was read, and then the
	// finalizer stays for that claim.
	if len(vr.Status.Claims) == 0 && controllerutil.ContainsFinalizer(vr, Finalizer) {
		controllerutil.RemoveFinalizer(vr, Finalizer)
		if err := c.operations.UpdateVolumeRestore(ctx, vr); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("remove finalizer %s from VolumeRestore: %w", Finalizer, err)
		}
	}
	return nil
}

// holdVolumeRestore adds Finalizer to the VolumeRestore that vr holds, and
// writes it, unless it's there already. Populate calls it before it creates
// anything, so the VolumeRestore stays until Cleanup has deleted what
// Populate created.
//
// It returns an error, and adds nothing, when the VolumeRestore is being
// deleted without the finalizer: the API server refuses a new finalizer on
// such an object, so a restore started from it could leave the Secret copy
// behind. It also returns an error when the write fails.
func (c *Callbacks) holdVolumeRestore(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if controllerutil.ContainsFinalizer(vr, Finalizer) {
		return nil
	}
	if vr.DeletionTimestamp != nil {
		return fmt.Errorf("VolumeRestore %s/%s is being deleted, so no restore starts from it", vr.Namespace, vr.Name)
	}
	controllerutil.AddFinalizer(vr, Finalizer)
	if err := c.operations.UpdateVolumeRestore(ctx, vr); err != nil {
		return fmt.Errorf("add finalizer %s to VolumeRestore %s/%s: %w", Finalizer, vr.Namespace, vr.Name, err)
	}
	return nil
}

// validateParams checks that the params carry what Populate and Complete need:
// the claim, the VolumeRestore and the prime claim. Cleanup calls
// validateCleanupParams, which doesn't ask for the prime claim.
func validateParams(params populatormachinery.PopulatorParams) error {
	if err := validateCleanupParams(params); err != nil {
		return err
	}
	if params.PvcPrime == nil {
		return fmt.Errorf("populator parameters have no prime PVC")
	}
	return nil
}

// validateCleanupParams checks that the params carry what Cleanup needs: the
// claim and the VolumeRestore. It doesn't ask for the prime claim.
//
// The library deletes the prime claim after it calls PopulateCleanupFn, so the
// prime claim is present on the pass that finishes a restore and absent on
// every pass after it. The library also has no early return for a claim it has
// already populated, so it walks the completion path on every resync for the
// life of the claim. When this check required the prime claim, every one of
// those passes failed, and the library requeues on error. One restore left the
// claim erroring and emitting PopulatorFinished again for as long as the claim
// existed.
//
// Cleanup exists to remove things. The prime claim being gone is the state it
// works towards, so its absence is nothing to reject.
func validateCleanupParams(params populatormachinery.PopulatorParams) error {
	if params.Pvc == nil {
		return fmt.Errorf("populator parameters have no application PVC")
	}
	if params.Unstructured == nil {
		return fmt.Errorf("populator parameters have no VolumeRestore")
	}
	return nil
}

// decodeVolumeRestore converts the VolumeRestore that the library passes as an
// unstructured object into the typed form.
func decodeVolumeRestore(params populatormachinery.PopulatorParams) (*backupv1alpha1.VolumeRestore, error) {
	vr := new(backupv1alpha1.VolumeRestore)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(params.Unstructured.Object, vr); err != nil {
		return nil, fmt.Errorf("decode VolumeRestore: %w", err)
	}
	return vr, nil
}

// setClaimStatus sets the phase of the claim's entry in the VolumeRestore's
// status.claims, and adds the entry when there isn't one. It also updates the
// entry's name, and sets startedAt to now when the entry has no start time
// yet. It changes the status in memory only, and the caller writes it.
func setClaimStatus(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim, phase backupv1alpha1.RestorePhase) {
	for i := range vr.Status.Claims {
		status := &vr.Status.Claims[i]
		if status.UID != claim.UID {
			continue
		}
		status.Name = claim.Name
		status.Phase = phase
		if status.StartedAt == nil {
			now := metav1.Now()
			status.StartedAt = &now
		}
		return
	}
	now := metav1.Now()
	vr.Status.Claims = append(vr.Status.Claims, backupv1alpha1.ClaimRestoreStatus{
		Name:      claim.Name,
		UID:       claim.UID,
		Phase:     phase,
		StartedAt: &now,
	})
}

// retireClaimStatus removes the claim's entry from the VolumeRestore's
// status.claims, which ends the VolumeRestore's report of that restore. When
// no entry remains, it sets claims to nil. It changes the status in memory
// only, and the caller writes it.
func retireClaimStatus(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) {
	remaining := vr.Status.Claims[:0]
	for _, status := range vr.Status.Claims {
		if status.UID != claim.UID {
			remaining = append(remaining, status)
		}
	}
	if len(remaining) == 0 {
		vr.Status.Claims = nil
		return
	}
	vr.Status.Claims = remaining
}
