package populator

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	internalvolsync "github.com/walzen-group/backup-controller/internal/volsync"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// Operations contains the Kubernetes operations used by the callbacks.
type Operations interface {
	GetReplicationDestination(ctx context.Context, namespace, name string) (*volsyncv1alpha1.ReplicationDestination, error)
	CreateReplicationDestination(ctx context.Context, rd *volsyncv1alpha1.ReplicationDestination) error
	DeleteReplicationDestination(ctx context.Context, namespace, name string) error
	GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error)
	CreateSecret(ctx context.Context, secret *corev1.Secret) error
	DeleteSecret(ctx context.Context, namespace, name string) error
	SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error
}

// Callbacks is the provider callback set for one controller namespace.
type Callbacks struct {
	operations Operations
	namespace  string
}

// New returns provider callbacks backed by the supplied operations.
func New(operations Operations, namespace string) *Callbacks {
	return &Callbacks{operations: operations, namespace: namespace}
}

// Populate creates the repository Secret copy and ReplicationDestination.
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
	if _, err := c.operations.GetReplicationDestination(ctx, c.namespace, destinationName); err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("get ReplicationDestination %s/%s: %w", c.namespace, destinationName, err)
		}
		destination := internalvolsync.New(vr, claim, prime.Name, c.namespace)
		if err := c.operations.CreateReplicationDestination(ctx, destination); err != nil && !apierrors.IsAlreadyExists(err) {
			return fmt.Errorf("create ReplicationDestination %s/%s: %w", c.namespace, destinationName, err)
		}
	}

	setClaimStatus(vr, claim, backupv1alpha1.RestorePhaseRestoring)
	// The name alone leaves a reader hunting for the namespace it is in, which
	// is the controller's rather than the app's.
	waiting := fmt.Sprintf("waiting for ReplicationDestination %s in %s", destinationName, c.namespace)
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoring, waiting)
	if err := c.operations.SetStatus(ctx, vr); err != nil {
		return fmt.Errorf("set VolumeRestore status: %w", err)
	}
	return nil
}

// Complete reports whether the triggered restore finished and records mover failure.
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

// Cleanup removes the destination and copied repository Secret.
func (c *Callbacks) Cleanup(ctx context.Context, params populatormachinery.PopulatorParams) error {
	if err := validateParams(params); err != nil {
		return err
	}
	claim := params.Pvc
	if err := c.operations.DeleteReplicationDestination(ctx, c.namespace, "restore-"+internalvolsync.Trigger(claim)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ReplicationDestination: %w", err)
	}
	if err := c.operations.DeleteSecret(ctx, c.namespace, internalvolsync.SecretCopyName(claim.UID)); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete copied repository Secret: %w", err)
	}

	// The restore has ended, so the claim stops being reported. A VolumeRestore
	// is a standing declaration rather than a job: it holds an entry only for a
	// claim being filled right now, and it reads Ready once no claim is.
	vr, err := decodeVolumeRestore(params)
	if err != nil {
		return err
	}
	retireClaimStatus(vr, claim)
	if len(vr.Status.Claims) == 0 {
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionTrue, backupv1alpha1.ReasonRestored, "no claim is being restored")
	} else {
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoring, fmt.Sprintf("%d claim(s) still restoring", len(vr.Status.Claims)))
	}
	if err := c.operations.SetStatus(ctx, vr); err != nil {
		return fmt.Errorf("set VolumeRestore status: %w", err)
	}
	return nil
}

func validateParams(params populatormachinery.PopulatorParams) error {
	if params.Pvc == nil {
		return fmt.Errorf("populator parameters have no application PVC")
	}
	if params.PvcPrime == nil {
		return fmt.Errorf("populator parameters have no prime PVC")
	}
	if params.Unstructured == nil {
		return fmt.Errorf("populator parameters have no VolumeRestore")
	}
	return nil
}

func decodeVolumeRestore(params populatormachinery.PopulatorParams) (*backupv1alpha1.VolumeRestore, error) {
	vr := new(backupv1alpha1.VolumeRestore)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(params.Unstructured.Object, vr); err != nil {
		return nil, fmt.Errorf("decode VolumeRestore: %w", err)
	}
	return vr, nil
}

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

// retireClaimStatus removes the claim's entry from the VolumeRestore's status,
// which is what ends the object's report of that restore.
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
