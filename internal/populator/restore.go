package populator

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restore is one claim's restore as a Populate pass sees it.
type restore struct {
	// vr is the VolumeRestore the claim names, decoded from the library's
	// cache. The status writes go through it.
	vr *backupv1alpha1.VolumeRestore
	// claim is the app claim being filled.
	claim *corev1.PersistentVolumeClaim
	// prime is the prime claim the library created and bound, which the
	// restore Job writes. It comes from the library's cache and is never
	// changed in place.
	prime *corev1.PersistentVolumeClaim
	// repo is the app's repository Secret.
	repo *corev1.Secret
}

// errJobBeingDeleted is a claim whose restore Job is being deleted. A new Job
// takes the same name, so none is created until the old one is gone.
var errJobBeingDeleted = errors.New("its restore Job is being deleted; a new one is created once it is gone")

// claimError wraps an error of one claim's restore with the claim's name.
func claimError(claim *corev1.PersistentVolumeClaim, err error) error {
	return fmt.Errorf("claim %s/%s: %w", claim.Namespace, claim.Name, err)
}

// jobKey returns the namespace and name of a claim's restore Job.
func (c *Callbacks) jobKey(claimUID types.UID) types.NamespacedName {
	return types.NamespacedName{Namespace: c.namespace, Name: JobName(claimUID)}
}

// followJob acts on a claim's restore Job as it stands.
//
// Parameters:
//   - r is the claim's restore.
//   - job is the claim's Job as Populate just read it, not being deleted,
//     and built for the prime claim the library passes now.
//
// It returns what failJob returns for a failed Job, which is always an
// error. For a running Job it returns an error from recording or resuming
// the Job (see resume). Otherwise it marks the claim Restoring and returns
// the error of that status write.
func (c *Callbacks) followJob(ctx context.Context, r restore, job *batchv1.Job) error {
	pods, err := c.operations.ListJobPods(ctx, job.Namespace, job.UID)
	if err != nil {
		return fmt.Errorf("list the pods of restore Job %s/%s: %w", job.Namespace, job.Name, err)
	}
	outcome := restorejob.Read(job, pods)
	switch outcome.State {
	case restorejob.Failed:
		return c.failJob(ctx, r, job, outcome.Failure)
	case restorejob.Running:
		if err := c.resume(ctx, r, job); err != nil {
			return err
		}
	default:
	}
	return c.markRestoring(ctx, r.vr, r.claim)
}

// failJob records a failed restore Job on the claim's status, then stops the
// Job so that a new one can be created.
//
// Parameters:
//   - r is the claim's restore.
//   - job is the failed Job.
//   - failure is what restorejob.Read found in the Job and its pods.
//
// It always returns an error, so the library requeues the claim and does not
// call Complete in this sync: the error of the status write, or the failure
// itself, joined with the error of the stop if there is one.
//
// The status entry reads Failed with reason RestoreFailed and the failure's
// message before anything is stopped, so a status write that fails stops
// nothing and the next sync tries again. The Job is finished, so the stop
// deletes it once its pods have ended. The entry keeps RestoreFailed while
// the Job is being deleted, and a new Job replaces it once it is gone.
func (c *Callbacks) failJob(ctx context.Context, r restore, job *batchv1.Job, failure *restorejob.FailureError) error {
	if err := c.markFailed(ctx, r.vr, r.claim, backupv1alpha1.ReasonRestoreFailed, failure); err != nil {
		return err
	}
	if _, err := restorejob.Stop(ctx, c.operations, restorejob.RefOf(job)); err != nil {
		return claimError(r.claim, fmt.Errorf("%w; stopping the Job: %w", failure, err))
	}
	return claimError(r.claim, failure)
}

// resume lets a running restore Job's pod start, once the Job is recorded on
// the prime claim.
//
// Parameters:
//   - r is the claim's restore.
//   - job is the claim's running Job, as Populate just read it.
//
// It returns an error from a read, from the record or from the resume.
//
// restorejob.Build creates every Job suspended. A Job whose UID the prime
// claim does not record yet, one this pass created or one a create that
// answered with an error still stored, is recorded here and resumed in a
// later pass. Only a pass that reads the record back fresh from the API
// server, with this Job's UID in it, resumes the Job, and only while the app
// claim, read fresh too, is not being deleted: a deleted claim's Job is
// Cleanup's or the orphan reconciler's to stop, and a Job their stop
// suspended looks the same as one never resumed (restorejob.AwaitsResume).
// The resume carries the Job's UID, so a Job created under the name since
// the read is left alone.
func (c *Callbacks) resume(ctx context.Context, r restore, job *batchv1.Job) error {
	prime, err := c.operations.GetClaim(ctx, r.prime.Namespace, r.prime.Name)
	if err != nil {
		return fmt.Errorf("read prime claim %s/%s: %w", r.prime.Namespace, r.prime.Name, err)
	}
	if types.UID(prime.Annotations[AnnotationJobUID]) != job.UID {
		return c.recordJob(ctx, prime, job.UID)
	}
	if !restorejob.AwaitsResume(job) {
		return nil
	}
	claim, err := c.operations.GetClaim(ctx, r.claim.Namespace, r.claim.Name)
	if err != nil {
		return fmt.Errorf("read claim %s/%s: %w", r.claim.Namespace, r.claim.Name, err)
	}
	if claim.UID != r.claim.UID || claim.DeletionTimestamp != nil {
		return nil
	}
	if err := c.operations.ResumeJob(ctx, job); err != nil {
		return fmt.Errorf("resume restore Job %s/%s: %w", job.Namespace, job.Name, err)
	}
	return nil
}

// recordJob records a restore Job's UID on the prime claim, as
// AnnotationJobUID.
//
// Parameters:
//   - prime is the prime claim as the caller read it, with its UID.
//   - uid is the Job's UID.
//
// It returns the error of the patch. The merge patch carries the prime's
// UID, so the API server refuses it for a prime created again under the same
// name since the read.
func (c *Callbacks) recordJob(ctx context.Context, prime *corev1.PersistentVolumeClaim, uid types.UID) error {
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"uid":         prime.UID,
		"annotations": map[string]string{AnnotationJobUID: string(uid)},
	}})
	if err != nil {
		return fmt.Errorf("build the record patch: %w", err)
	}
	if err := c.operations.PatchClaim(ctx, prime.DeepCopy(), client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("record restore Job %s on prime claim %s/%s: %w", uid, prime.Namespace, prime.Name, err)
	}
	return nil
}

// startJob creates the restore Job of a claim that has none.
//
// Parameters:
//   - r is the claim's restore.
//
// It returns nil once the Job is created and recorded, or when the
// repository is empty and nothing pins the claim, which Complete then binds
// empty; the claim is marked Restoring either way. It returns an error, and
// creates nothing, while a pod of an earlier Job of the claim may still
// write (a *stoppingError), and when no snapshot can fill the claim, after it
// marked the claim Failed with reason NoBackupInReach (a *noBackupError).
// Any failed call returns its error.
func (c *Callbacks) startJob(ctx context.Context, r restore) error {
	state, err := stopRestore(ctx, c.operations, c.namespace, r.claim.UID)
	if err != nil {
		return claimError(r.claim, err)
	}
	if !state.Stopped {
		return claimError(r.claim, &stoppingError{state: state})
	}
	chosen, err := c.selectSnapshot(ctx, r.vr, r.claim, r.repo)
	var none *noBackupError
	switch {
	case errors.As(err, &none):
		if err := c.markFailed(ctx, r.vr, r.claim, backupv1alpha1.ReasonNoBackupInReach, none); err != nil {
			return err
		}
		return claimError(r.claim, none)
	case err != nil:
		return claimError(r.claim, err)
	case chosen.empty:
		return c.markRestoring(ctx, r.vr, r.claim)
	}
	job, err := c.createJob(ctx, r, chosen.snapshot)
	if err != nil {
		return err
	}
	if err := c.recordJob(ctx, r.prime, job.UID); err != nil {
		return err
	}
	return c.markRestoring(ctx, r.vr, r.claim)
}

// createJob creates the suspended restore Job that fills a prime claim with
// one snapshot.
//
// Parameters:
//   - r is the claim's restore. The VolumeRestore gives the cache, the
//     security context and the pod labels.
//   - snapshot is the snapshot to restore, by its full ID.
//
// It returns the created Job, with its UID, and an error from the namespace
// read, from restorejob.Build or from the create. A create that finds the
// name taken returns its AlreadyExists error; the next pass follows that Job.
//
// Every Job restores with --delete. A Job that replaces a failed or deleted
// one selects the snapshot again, perhaps a newer one, and restores it over
// what the earlier Job left in the prime claim, so without the flag files of
// the earlier snapshot would stay behind. On the first Job's new, empty
// volume the flag removes nothing, and the prime claim is the populator's
// own, so nothing in it needs keeping.
func (c *Callbacks) createJob(ctx context.Context, r restore, snapshot restic.Snapshot) (*batchv1.Job, error) {
	ns, err := c.operations.GetNamespace(ctx, c.namespace)
	if err != nil {
		return nil, fmt.Errorf("read namespace %s: %w", c.namespace, err)
	}
	job, err := restorejob.Build(restorejob.Spec{
		Name:      JobName(r.claim.UID),
		Namespace: c.namespace,
		Origin:    restorejob.Origin{Kind: restorejob.OriginClaim, UID: r.claim.UID},
		Owner: metav1.OwnerReference{
			APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: r.prime.Name, UID: r.prime.UID,
		},
		SnapshotID:            snapshot.ID,
		Delete:                true,
		Claim:                 r.prime.Name,
		Repository:            SecretCopyName(r.claim.UID),
		Image:                 c.image,
		Privileged:            restorejob.PrivilegedMovers(ns),
		SecurityContext:       r.vr.Spec.MoverSecurityContext,
		PodLabels:             r.vr.Spec.MoverLabels(),
		CacheStorageClassName: r.vr.Spec.CacheStorageClassName,
		CacheCapacity:         r.vr.Spec.CacheCapacity,
	})
	if err != nil {
		return nil, claimError(r.claim, err)
	}
	if err := c.operations.CreateJob(ctx, job); err != nil {
		return nil, fmt.Errorf("create restore Job %s/%s: %w", job.Namespace, job.Name, err)
	}
	return job, nil
}

// populate reads a claim's restore Job and moves the restore on from there.
//
// Parameters:
//   - r is the claim's restore.
//
// It returns what startJob returns when the claim has no Job, an error
// wrapping errJobBeingDeleted while the Job is being deleted, what
// stopEarlierJob returns for a Job built for an earlier prime claim, and
// what followJob returns otherwise.
func (c *Callbacks) populate(ctx context.Context, r restore) error {
	key := c.jobKey(r.claim.UID)
	job, err := c.operations.GetJob(ctx, key)
	switch {
	case apierrors.IsNotFound(err):
		return c.startJob(ctx, r)
	case err != nil:
		return fmt.Errorf("read restore Job %s: %w", key, err)
	case job.DeletionTimestamp != nil:
		return claimError(r.claim, errJobBeingDeleted)
	case !ownedByPrime(job, r.prime):
		return c.stopEarlierJob(ctx, r, job)
	default:
		return c.followJob(ctx, r, job)
	}
}
