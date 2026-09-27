// Package populator holds the callbacks that the volume populator library
// calls to fill a claim whose data source is a VolumeRestore, and the
// reconciler that finishes the cleanup for a claim whose VolumeRestore is
// gone.
//
// A claim is filled by the controller's own restic restore Job
// (internal/restorejob), which restores one snapshot, named by its full ID,
// into the prime claim the library created. Populate creates, records,
// resumes and replaces the Job, Complete reads its Complete condition, and
// Cleanup stops it before the library hands the volume to the app claim.
package populator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// Operations are the Kubernetes API calls the callbacks make. The binary
// implements them with a client (clientOperations in cmd/backup-controller),
// and the tests with a strict fake client. Every read goes to the API server
// directly.
type Operations interface {
	Jobs
	// GetNamespace reads a namespace, for its privileged-movers annotation.
	GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error)
	// GetClaim reads a claim.
	GetClaim(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error)
	// GetVolume reads a PersistentVolume, the prime claim's, whose claimRef
	// tells whether the library has handed it to the app claim.
	GetVolume(ctx context.Context, name string) (*corev1.PersistentVolume, error)
	// PatchClaim sends a patch to a claim; Populate records the restore
	// Job's UID on the prime claim with it.
	PatchClaim(ctx context.Context, claim *corev1.PersistentVolumeClaim, patch client.Patch) error
	// GetSecret reads a Secret.
	GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error)
	// CreateSecret creates a Secret.
	CreateSecret(ctx context.Context, secret *corev1.Secret) error
	// DeleteSecret deletes a Secret.
	DeleteSecret(ctx context.Context, namespace, name string) error
	// SetStatus writes the VolumeRestore's status subresource.
	SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error
	// UpdateVolumeRestore writes the VolumeRestore's metadata, which is how
	// the callbacks add and remove Finalizer. The write carries the
	// resourceVersion of the object passed, so it fails with a conflict when
	// the object has changed since it was read.
	UpdateVolumeRestore(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error
	// ListVolumeRestores lists the VolumeRestores of a namespace. Their
	// repositories give the moment of a synced restore (see selectSnapshot).
	ListVolumeRestores(ctx context.Context, namespace string) ([]backupv1alpha1.VolumeRestore, error)
}

// Finalizer is the finalizer the populator keeps on a VolumeRestore while a
// claim has an entry in its status.claims. The library looks the VolumeRestore
// up before it handles a deleted claim, and when the VolumeRestore is gone it
// returns without calling Cleanup and leaves its own finalizer on the claim.
// This finalizer keeps the VolumeRestore in place until Cleanup has stopped
// the claim's restore Job and deleted its Secret copy. Populate adds it only
// once the prime claim is bound, so a VolumeRestore deleted before that goes
// at once; OrphanReconciler then finishes the cleanup for its claims.
const Finalizer = "backup.wlz.li/volume-populator"

// Callbacks holds the three functions that the volume populator library calls
// for a claim whose data source is a VolumeRestore: Populate, Complete and
// Cleanup. One Callbacks serves one controller namespace, where the prime
// claims, the Secret copies and the restore Jobs live.
type Callbacks struct {
	operations Operations
	namespace  string
	image      string
	snapshots  restic.SnapshotLister
	// paused is true when the controller runs with --pause (see Pause).
	paused bool
	// pauseNoted holds the UID of each claim whose wait for the pause was
	// logged, so each claim logs it once.
	pauseNoted sync.Map
}

// New returns the callbacks for one controller namespace.
//
// Parameters:
//   - operations makes the Kubernetes API calls.
//   - namespace is the controller namespace, where the restore Jobs and the
//     repository Secret copies are created.
//   - image is the restic image the restore Jobs run, the controller's
//     --restore-image.
//   - snapshots lists a repository's snapshots, to pick the one a claim is
//     filled from and to tell a first deploy's empty repository apart.
func New(operations Operations, namespace, image string, snapshots restic.SnapshotLister) *Callbacks {
	return &Callbacks{operations: operations, namespace: namespace, image: image, snapshots: snapshots}
}

// Populate moves one claim's restore on.
//
// Parameters:
//   - ctx bounds the API calls.
//   - params are what the library passes for the claim: the claim, its
//     bound prime claim from the library's cache, and the VolumeRestore the
//     claim names, as an unstructured object from that cache.
//
// It returns nil when the library may go on to Complete in this sync, and an
// error otherwise, which makes the library requeue the claim and leaves it
// Pending.
//
// It first puts Finalizer on the VolumeRestore and copies the repository
// Secret into the controller namespace (see copySecret). It then reads the
// claim's restore Job, JobName(claim UID), fresh from the API server:
//   - No Job: once no pod of an earlier Job of the claim may still write, it
//     picks the snapshot (see selectSnapshot), creates the Job suspended
//     for it, records the Job's UID on the prime claim and marks the claim
//     Restoring. A repository with no snapshot at all and no pin creates no
//     Job, and Complete binds the claim empty. No snapshot that can fill the
//     claim marks it Failed with reason NoBackupInReach and returns an
//     error.
//   - A Job being deleted: it returns an error and creates nothing, until
//     the Job is gone.
//   - A Job built for an earlier prime claim, by its owner reference: it
//     stops the Job and returns an error, so a new Job for the prime of now
//     is created once the old one is gone (see stopEarlierJob).
//   - A failed Job: it marks the claim Failed with reason RestoreFailed and
//     the Job's failure, then stops the Job, and returns an error (see
//     failJob).
//   - Any other Job: it records the Job on the prime claim, or resumes it
//     once recorded while the prime's volume is still the prime's (see
//     resume), and marks the claim Restoring.
//
// A Job create or resume that the API server refuses with 403 Forbidden or
// 422 Invalid, such as the admission policy refusing the VolumeRestore's
// moverSecurityContext, marks the claim Failed with reason
// RestoreJobRefused and the API server's answer, and returns an error, so
// the next sync tries again (see jobCallError).
//
// While the controller runs with --pause, a claim whose restore has not
// started waits: Populate returns nil and does nothing (see waitsForPause).
//
// Every status write happens only when it changes something.
func (c *Callbacks) Populate(ctx context.Context, params populatormachinery.PopulatorParams) error {
	if err := validateParams(params); err != nil {
		return err
	}
	if waits, err := c.waitsForPause(ctx, params); waits || err != nil {
		return err
	}
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	if err := c.holdVolumeRestore(ctx, vr); err != nil {
		return err
	}
	if err := c.copySecret(ctx, vr, params.Pvc); err != nil {
		return err
	}
	return c.populate(ctx, restore{vr: vr, claim: params.Pvc, prime: params.PvcPrime})
}

// Complete reports whether one claim's restore has finished, so the library
// may hand the prime claim's volume to the claim.
//
// Parameters:
//   - ctx bounds the API calls.
//   - params are what the library passes for the claim, as for Populate.
//     The library calls Complete only in a sync whose Populate returned nil.
//
// It returns true when the claim's restore Job was built for the prime claim
// the library passes now, by its owner reference, and has Complete=True, the
// one sign that restic restored the Job's snapshot into that prime's volume.
// A Job of an earlier prime claim, one the library has since created again
// under the same name, filled a volume that went with that prime, so it
// counts for nothing and Populate replaces it. With no Job, it returns true
// only when the repository holds no snapshot at all and nothing pins the
// claim, which is how a first deploy binds its empty volume (see choose). It
// returns false for any other Job and in any other case, and an error when
// the Job, the repository Secret or the snapshots can't be read. It writes
// nothing: a Job that failed after Populate read it is Populate's in the
// next sync.
// While the controller runs with --pause, it returns false for a claim
// whose restore has not started (see waitsForPause).
func (c *Callbacks) Complete(ctx context.Context, params populatormachinery.PopulatorParams) (bool, error) {
	if err := validateParams(params); err != nil {
		return false, err
	}
	if waits, err := c.waitsForPause(ctx, params); waits || err != nil {
		return false, err
	}
	key := c.jobKey(params.Pvc.UID)
	job, err := c.operations.GetJob(ctx, key)
	switch {
	case apierrors.IsNotFound(err):
		return c.bindsEmpty(ctx, params)
	case err != nil:
		return false, fmt.Errorf("read restore Job %s: %w", key, err)
	default:
		return ownedByPrime(job, params.PvcPrime) && restorejob.Read(job, nil).State == restorejob.Succeeded, nil
	}
}

// bindsEmpty reports whether a claim with no restore Job may bind its empty
// volume.
//
// Parameters:
//   - params are the library's params for the claim.
//
// It returns true only when choose selects nothing to restore, false when
// no snapshot can fill the claim, and an error when the VolumeRestore can't
// be decoded or the Secret or the snapshots can't be read.
func (c *Callbacks) bindsEmpty(ctx context.Context, params populatormachinery.PopulatorParams) (bool, error) {
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return false, err
	}
	chosen, err := c.selectSnapshot(ctx, vr, params.Pvc)
	var none *noBackupError
	switch {
	case errors.As(err, &none):
		return false, nil
	case err != nil:
		return false, err
	default:
		return chosen.empty, nil
	}
}

// Cleanup runs after a claim's restore has finished, and when the claim is
// deleted before that. The library deletes the prime claim only once Cleanup
// returns nil.
//
// Parameters:
//   - ctx bounds the API calls.
//   - params are what the library passes for the claim. The prime claim is
//     absent on every sync after the one that deleted it, and Cleanup needs
//     none.
//
// It first stops every restore Job of the claim (see stopRestore), and
// returns an error while one of them may still write, so the prime claim and
// its volume stay out of the app's hands until then. It then deletes the
// repository Secret copy, skipping one that is already gone.
//
// It then removes the claim's entry from the VolumeRestore's status.claims.
// It sets Ready from the entries that remain with summarize: Restored when no
// claim is left, Restoring while other claims are being filled, and
// unchanged while another claim's entry is Failed. It writes the status only
// when that changed something. When the VolumeRestore is gone by then, it
// returns nil, because nothing is left to report on.
//
// Once no claim is left in status.claims, it removes Finalizer from the
// VolumeRestore. A VolumeRestore deleted while a claim was being filled goes
// then.
func (c *Callbacks) Cleanup(ctx context.Context, params populatormachinery.PopulatorParams) error {
	if err := validateCleanupParams(params); err != nil {
		return err
	}
	claim := params.Pvc
	state, err := stopRestore(ctx, c.operations, c.namespace, claim.UID)
	if err != nil {
		return claimError(claim, err)
	}
	if !state.Stopped {
		return claimError(claim, &stoppingError{state: state})
	}
	if err := c.operations.DeleteSecret(ctx, c.namespace, SecretCopyName(claim.UID)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete copied repository Secret: %w", err)
	}
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	if err := c.retire(ctx, vr, claim); err != nil {
		return err
	}

	// The status write in retire bumped the resource version in vr, so this
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

// retire removes a claim's entry from the VolumeRestore's status, sets Ready
// with summarize and writes the status when that changed something.
//
// Parameters:
//   - vr is the VolumeRestore, changed in place and written.
//   - claim is the claim whose restore has ended.
//
// It returns an error from summarize or from the write, and nil when the
// write finds the VolumeRestore gone, since nothing is left to report on.
//
// The library has no early return for a claim it has already populated. It
// walks the whole path on every resync, reaches the completion branch and
// calls Cleanup again, for the life of the claim. Writing every time would
// send one status update per volume every resync interval, and none of them
// would change anything.
func (c *Callbacks) retire(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) error {
	before := vr.Status.DeepCopy()
	retireClaimStatus(vr, claim)
	if err := c.summarize(ctx, vr, ""); err != nil {
		return err
	}
	if err := c.writeChanged(ctx, vr, before); err != nil && !apierrors.IsNotFound(err) {
		return err
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
