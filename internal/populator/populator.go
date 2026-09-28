// Package populator holds the callbacks that the volume populator library
// calls to fill a claim whose data source is a VolumeRestore.
//
// The controller selects the snapshot itself and restores it with its own
// restic Job (internal/restorejob), which writes the snapshot into the prime
// claim that the library created. The Job's success or failure is the only
// result the callbacks read.
package populator

import (
	"context"
	"errors"
	"fmt"
	"time"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	"github.com/walzen-group/backup-controller/internal/synced"
	internalvolsync "github.com/walzen-group/backup-controller/internal/volsync"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// errPaused is the error Populate returns while the controller runs with
// --pause, so the library calls Populate again later.
var errPaused = errors.New("the controller runs with --pause")

// Operations are the Kubernetes API calls the callbacks make. The binary
// implements them with a client (clientOperations in cmd/backup-controller).
type Operations interface {
	GetJob(ctx context.Context, namespace, name string) (*batchv1.Job, error)
	CreateJob(ctx context.Context, job *batchv1.Job) error
	DeleteJob(ctx context.Context, namespace, name string) error
	GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error)
	CreateSecret(ctx context.Context, secret *corev1.Secret) error
	DeleteSecret(ctx context.Context, namespace, name string) error
	SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error
}

// Config is what the callbacks work with.
type Config struct {
	// Operations makes the Kubernetes API calls.
	Operations Operations
	// Reader reads the VolumeRestores and repository Secrets of a namespace,
	// to find the namespace's paused moment (see internal/synced).
	Reader client.Reader
	// Namespace is the controller namespace, where the prime claims, the
	// restore Jobs and the repository Secret copies are.
	Namespace string
	// Image is the image of the restore Jobs, from --restore-image.
	Image string
	// Snapshots lists the snapshots of a repository.
	Snapshots restic.Lister
	// Paused is the --pause flag. While it is set, Populate starts no
	// restore.
	Paused bool
}

// Callbacks holds the three functions that the volume populator library calls
// for a claim whose data source is a VolumeRestore: Populate, Complete and
// Cleanup.
type Callbacks struct {
	config Config
}

// New returns the callbacks.
//
// Parameters:
//   - config holds the API calls, the controller namespace, the restore
//     image, the snapshot lister and the pause setting that the callbacks
//     use.
func New(config Config) *Callbacks {
	return &Callbacks{config: config}
}

// Populate starts filling one claim from its VolumeRestore.
//
// Parameters:
//   - params are what the library passes: the claim, the prime claim and
//     the VolumeRestore.
//
// It returns nil when the restore Job runs, or when the claim binds empty
// because its repository holds no snapshot. It returns an error while the
// controller is paused, when no snapshot reaches the claim's pin, and when
// an API call or a snapshot listing fails. After an error, the library
// calls Populate again.
//
// When the claim's restore Job exists, Populate returns at once. Else it
// uses the snapshot that the claim's Restoring entry records, or it selects
// one and records it (see choose). Then it copies the repository Secret into
// the controller namespace and creates the Job. A claim whose entry records
// no snapshot gets no Job.
func (c *Callbacks) Populate(ctx context.Context, params populatormachinery.PopulatorParams) error {
	if err := validateParams(params); err != nil {
		return err
	}
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	claim := params.Pvc
	name := jobName(claim.UID)
	if _, err := c.config.Operations.GetJob(ctx, c.config.Namespace, name); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get restore Job %s/%s: %w", c.config.Namespace, name, err)
	}
	if c.config.Paused {
		return c.waitPaused(ctx, vr, claim)
	}
	status, ok := decided(vr, claim.UID)
	if !ok {
		if status, err = c.decide(ctx, vr, claim); err != nil {
			return err
		}
	}
	if status.SnapshotID == "" {
		return nil
	}
	return c.start(ctx, vr, claim, params.PvcPrime.Name, status.SnapshotID)
}

// waitPaused reports a claim whose restore waits for the controller to run
// without --pause.
//
// Parameters:
//   - vr is the claim's VolumeRestore. Its Ready condition gets reason
//     ControllerPaused.
//   - claim is the claim that waits.
//
// It returns an error that wraps errPaused, so the library calls Populate
// again, or an error when the status write fails.
func (c *Callbacks) waitPaused(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) error {
	before := vr.Status.DeepCopy()
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonControllerPaused,
		fmt.Sprintf("the restore of claim %s/%s starts once the controller runs without --pause", claim.Namespace, claim.Name))
	if err := c.write(ctx, vr, before); err != nil {
		return err
	}
	return fmt.Errorf("claim %s/%s waits: %w", claim.Namespace, claim.Name, errPaused)
}

// decide selects the snapshot that fills a claim and records it.
//
// Parameters:
//   - vr is the claim's VolumeRestore. The claim's entry gets phase
//     Restoring and the snapshot, and Ready gets reason Restoring.
//   - claim is the claim to fill.
//
// It returns the claim's entry as written. When no snapshot reaches the
// claim's pin, it marks the entry Failed, sets Ready to False with reason
// NoBackupInReach, and returns an error. It also returns an error when an
// API call or a snapshot listing fails.
//
// The status is written before the Job exists, so a later Populate uses the
// recorded snapshot and does not select again.
func (c *Callbacks) decide(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) (backupv1alpha1.ClaimRestoreStatus, error) {
	before := vr.Status.DeepCopy()
	snapshot, found, err := c.pick(ctx, vr, claim)
	var reach *outOfReachError
	if errors.As(err, &reach) {
		return backupv1alpha1.ClaimRestoreStatus{}, c.unreachable(ctx, vr, claim, reach)
	}
	if err != nil {
		return backupv1alpha1.ClaimRestoreStatus{}, err
	}
	status := entry(vr, claim)
	status.Phase = backupv1alpha1.RestorePhaseRestoring
	setSnapshot(status, snapshot, found)
	recorded := *status
	message := fmt.Sprintf("repository %s holds no snapshot, so claim %s/%s binds empty", vr.Spec.Repository, claim.Namespace, claim.Name)
	if found {
		message = fmt.Sprintf("restoring snapshot %s from %s into claim %s/%s with Job %s/%s", snapshot.ShortID(),
			snapshot.Time.UTC().Format(time.RFC3339), claim.Namespace, claim.Name, c.config.Namespace, jobName(claim.UID))
	}
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoring, message)
	if err := c.write(ctx, vr, before); err != nil {
		return backupv1alpha1.ClaimRestoreStatus{}, err
	}
	return recorded, nil
}

// pick selects the snapshot that fills a claim from the claim's repository.
//
// Parameters:
//   - vr is the claim's VolumeRestore. It gives the repository and can set
//     the pin.
//   - claim is the claim to fill. Its annotation can set the pin, and its
//     namespace gives the paused moment.
//
// It returns what choose returns. It returns an error when the repository
// Secret cannot be read or a listing fails.
//
// With a pin, it lists only the claim's repository. Without a pin, it lists
// every repository of the namespace, to find the paused moment.
func (c *Callbacks) pick(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) (restic.Snapshot, bool, error) {
	pinned, isPinned, err := pinOf(vr, claim)
	if err != nil {
		return restic.Snapshot{}, false, err
	}
	if isPinned {
		snapshots, err := c.snapshots(ctx, claim.Namespace, vr.Spec.Repository)
		if err != nil {
			return restic.Snapshot{}, false, err
		}
		return choose(snapshots, &pinned, nil)
	}
	repositories, err := synced.Repositories(ctx, c.config.Reader, c.config.Snapshots, claim.Namespace)
	if err != nil {
		return restic.Snapshot{}, false, fmt.Errorf("find the paused moment of %s: %w", claim.Namespace, err)
	}
	snapshots, listed := repositories[vr.Spec.Repository]
	if !listed {
		if snapshots, err = c.snapshots(ctx, claim.Namespace, vr.Spec.Repository); err != nil {
			return restic.Snapshot{}, false, err
		}
	}
	if moment, ok := synced.Moment(repositories); ok {
		return choose(snapshots, nil, &moment)
	}
	return choose(snapshots, nil, nil)
}

// snapshots lists the snapshots of one repository.
//
// Parameters:
//   - namespace is the app's namespace, which holds the repository Secret.
//   - repository is the name of the repository Secret.
//
// It returns the snapshots. A repository that is not initialised holds no
// snapshot. It returns an error when the Secret cannot be read or the
// listing fails.
func (c *Callbacks) snapshots(ctx context.Context, namespace, repository string) ([]restic.Snapshot, error) {
	secret, err := c.config.Operations.GetSecret(ctx, namespace, repository)
	if err != nil {
		return nil, fmt.Errorf("get repository Secret %s/%s: %w", namespace, repository, err)
	}
	snapshots, err := c.config.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return nil, fmt.Errorf("list the snapshots in %s/%s: %w", namespace, repository, err)
	}
	return snapshots, nil
}

// unreachable reports a claim whose pin no snapshot reaches.
//
// Parameters:
//   - vr is the claim's VolumeRestore. The claim's entry gets phase Failed
//     and no snapshot, and Ready gets reason NoBackupInReach.
//   - claim is the claim.
//   - reach says why no snapshot reaches the pin.
//
// It returns an error that wraps the cause, so the claim stays Pending and
// the library calls Populate again. When someone corrects the pin, a later
// call starts the restore.
func (c *Callbacks) unreachable(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim, reach *outOfReachError) error {
	before := vr.Status.DeepCopy()
	status := entry(vr, claim)
	status.Phase = backupv1alpha1.RestorePhaseFailed
	setSnapshot(status, restic.Snapshot{}, false)
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonNoBackupInReach, reach.Error())
	if err := c.write(ctx, vr, before); err != nil {
		return err
	}
	return fmt.Errorf("claim %s/%s: %w", claim.Namespace, claim.Name, reach)
}

// start copies the repository Secret into the controller namespace and
// creates the restore Job.
//
// Parameters:
//   - vr is the claim's VolumeRestore. It gives the repository Secret and
//     gives the Job's security context.
//   - claim is the claim to fill. The Job and the Secret copy are named
//     after its UID.
//   - prime is the name of the prime claim, which the Job writes into.
//   - snapshotID is the full ID of the snapshot to restore.
//
// It returns an error when a read or a create fails. An object that exists
// already is left as it is.
func (c *Callbacks) start(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim, prime, snapshotID string) error {
	if err := c.copySecret(ctx, vr, claim); err != nil {
		return err
	}
	job := restorejob.New(restorejob.Spec{
		Namespace:        c.config.Namespace,
		Name:             jobName(claim.UID),
		Image:            c.config.Image,
		Claim:            prime,
		RepositorySecret: internalvolsync.SecretCopyName(claim.UID),
		SnapshotID:       snapshotID,
		SecurityContext:  vr.Spec.MoverSecurityContext,
		// The VolumeRestore's moverPodLabels carry the Kueue queue label,
		// so Kueue admits the populator's restores through the queue, as
		// no run admits them.
		Labels: vr.Spec.MoverLabels(),
		// A label value cannot hold a slash, so the label holds the claim's
		// namespace. The Job's name holds the claim's UID.
		Owner: claim.Namespace,
	})
	if err := c.config.Operations.CreateJob(ctx, job); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create restore Job %s/%s: %w", job.Namespace, job.Name, err)
	}
	return nil
}

// copySecret copies the claim's repository Secret into the controller
// namespace, where the restore Job reads it.
//
// Parameters:
//   - vr is the claim's VolumeRestore, which gives the repository Secret.
//   - claim is the claim. The copy is named after its UID.
//
// It returns an error when a read or the create fails. An existing copy is
// left as it is.
func (c *Callbacks) copySecret(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) error {
	name := internalvolsync.SecretCopyName(claim.UID)
	_, err := c.config.Operations.GetSecret(ctx, c.config.Namespace, name)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("check copied repository Secret %s/%s: %w", c.config.Namespace, name, err)
	}
	repo, err := c.config.Operations.GetSecret(ctx, claim.Namespace, vr.Spec.Repository)
	if err != nil {
		return fmt.Errorf("get repository Secret %s/%s: %w", claim.Namespace, vr.Spec.Repository, err)
	}
	if err := c.config.Operations.CreateSecret(ctx, internalvolsync.SecretCopy(repo, claim.UID, c.config.Namespace)); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create copied repository Secret %s/%s: %w", c.config.Namespace, name, err)
	}
	return nil
}

// Complete reports whether the restore of one claim has finished.
//
// Parameters:
//   - params are what the library passes: the claim, the prime claim and
//     the VolumeRestore.
//
// It returns true when the restore Job succeeded, or when there is no Job
// and the claim's entry records that the claim binds empty. It returns false
// while the Job runs, and before Populate has created it. It returns an
// error when the Job cannot be read or a status write fails.
//
// When the Job failed, it marks the claim's entry Failed, sets Ready to
// False with reason RestoreFailed, and returns false. It reads no log.
func (c *Callbacks) Complete(ctx context.Context, params populatormachinery.PopulatorParams) (bool, error) {
	if err := validateParams(params); err != nil {
		return false, err
	}
	claim := params.Pvc
	name := jobName(claim.UID)
	job, err := c.config.Operations.GetJob(ctx, c.config.Namespace, name)
	if apierrors.IsNotFound(err) {
		vr, err := decodeVolumeRestore(params)
		if err != nil {
			return false, err
		}
		status, ok := decided(vr, claim.UID)
		return ok && status.SnapshotID == "", nil
	}
	if err != nil {
		return false, fmt.Errorf("get restore Job %s/%s: %w", c.config.Namespace, name, err)
	}
	switch restorejob.Result(job) {
	case restorejob.Succeeded:
		return true, nil
	case restorejob.Failed:
		return false, c.failed(ctx, params)
	default:
		return false, nil
	}
}

// failed reports a claim whose restore Job failed.
//
// Parameters:
//   - params are what the library passes: the claim and the VolumeRestore.
//
// It returns an error when the VolumeRestore cannot be decoded or the
// status write fails.
//
// The claim's entry gets phase Failed and keeps its snapshot. Ready gets
// reason RestoreFailed and a message that tells where the Job's logs are.
func (c *Callbacks) failed(ctx context.Context, params populatormachinery.PopulatorParams) error {
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	claim := params.Pvc
	before := vr.Status.DeepCopy()
	status := entry(vr, claim)
	status.Phase = backupv1alpha1.RestorePhaseFailed
	message := fmt.Sprintf("restic failed to restore snapshot %s into claim %s/%s; see kubectl -n %s logs job/%s",
		status.Snapshot, claim.Namespace, claim.Name, c.config.Namespace, jobName(claim.UID))
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoreFailed, message)
	return c.write(ctx, vr, before)
}

// Cleanup runs after a claim's restore has finished.
//
// Parameters:
//   - params are what the library passes: the claim and the VolumeRestore.
//     The prime claim can be gone.
//
// It returns an error when a delete or the status write fails.
//
// It deletes the restore Job and the repository Secret copy, and skips any
// that are already gone. The claim's entry gets phase Restored and keeps its
// snapshot, which the bootstrap webhook reads. Then settle sets Ready.
//
// The library calls Cleanup on every resync for the life of the claim, so it
// writes the status only when something changed.
func (c *Callbacks) Cleanup(ctx context.Context, params populatormachinery.PopulatorParams) error {
	if err := validateCleanupParams(params); err != nil {
		return err
	}
	claim := params.Pvc
	if err := c.config.Operations.DeleteJob(ctx, c.config.Namespace, jobName(claim.UID)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete restore Job: %w", err)
	}
	if err := c.config.Operations.DeleteSecret(ctx, c.config.Namespace, internalvolsync.SecretCopyName(claim.UID)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete copied repository Secret: %w", err)
	}
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	before := vr.Status.DeepCopy()
	if status := find(vr, claim.UID); status != nil {
		status.Phase = backupv1alpha1.RestorePhaseRestored
	}
	settle(vr)
	return c.write(ctx, vr, before)
}

// write writes the VolumeRestore's status when it differs from an earlier
// copy.
//
// Parameters:
//   - vr is the VolumeRestore with the new status.
//   - before is a copy of the status as it was read.
//
// It returns an error when the write fails. The library calls the callbacks
// again and again for one claim, and most calls change no field, so a write
// only for a change keeps the API server quiet.
func (c *Callbacks) write(ctx context.Context, vr *backupv1alpha1.VolumeRestore, before *backupv1alpha1.VolumeRestoreStatus) error {
	if equality.Semantic.DeepEqual(before, &vr.Status) {
		return nil
	}
	if err := c.config.Operations.SetStatus(ctx, vr); err != nil {
		return fmt.Errorf("set VolumeRestore status: %w", err)
	}
	return nil
}

// jobName returns the name of a claim's restore Job.
//
// Parameters:
//   - uid is the claim's UID, which makes the name unique.
func jobName(uid types.UID) string {
	return "restore-" + string(uid)
}

// validateParams checks that the params carry what Populate and Complete
// need.
//
// Parameters:
//   - params are what the library passes.
//
// It returns an error when the claim, the VolumeRestore or the prime claim
// is missing. Cleanup calls validateCleanupParams, which does not ask for the
// prime claim.
func validateParams(params populatormachinery.PopulatorParams) error {
	if err := validateCleanupParams(params); err != nil {
		return err
	}
	if params.PvcPrime == nil {
		return errors.New("populator parameters have no prime PVC")
	}
	return nil
}

// validateCleanupParams checks that the params carry what Cleanup needs.
//
// Parameters:
//   - params are what the library passes.
//
// It returns an error when the claim or the VolumeRestore is missing.
//
// It does not ask for the prime claim. The library deletes the prime claim
// after it calls Cleanup, and it calls Cleanup again on every resync for the
// life of the claim. A check for the prime claim would fail each of those
// calls, and the library requeues the claim on each error.
func validateCleanupParams(params populatormachinery.PopulatorParams) error {
	if params.Pvc == nil {
		return errors.New("populator parameters have no application PVC")
	}
	if params.Unstructured == nil {
		return errors.New("populator parameters have no VolumeRestore")
	}
	return nil
}

// decodeVolumeRestore converts the VolumeRestore that the library passes as
// an unstructured object into the typed form.
//
// Parameters:
//   - params are what the library passes.
//
// It returns the VolumeRestore, or an error when the conversion fails.
func decodeVolumeRestore(params populatormachinery.PopulatorParams) (*backupv1alpha1.VolumeRestore, error) {
	vr := new(backupv1alpha1.VolumeRestore)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(params.Unstructured.Object, vr); err != nil {
		return nil, fmt.Errorf("decode VolumeRestore: %w", err)
	}
	return vr, nil
}
