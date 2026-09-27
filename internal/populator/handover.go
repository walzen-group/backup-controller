package populator

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// mayResume reports whether a recorded restore Job may start its pod, from
// the app claim and the prime claim's volume as the API server holds them
// now.
//
// Parameters:
//   - r is the claim's restore.
//   - prime is the prime claim, read fresh by the caller.
//
// It returns false when the app claim is gone, was created again, is being
// deleted, or names a volume, and when the prime claim's volume no longer
// names the prime claim in its claimRef. It returns an error when a read
// fails.
//
// A deleted claim's Job is Cleanup's or the orphan reconciler's to stop. Once
// Complete has returned true, the library points the prime's volume at the
// app claim, and the PV controller then binds the app claim to it. A Job
// resumed in that window would write the app's volume.
func (c *Callbacks) mayResume(ctx context.Context, r restore, prime *corev1.PersistentVolumeClaim) (bool, error) {
	claim, err := c.operations.GetClaim(ctx, r.claim.Namespace, r.claim.Name)
	if err != nil {
		return false, fmt.Errorf("read claim %s/%s: %w", r.claim.Namespace, r.claim.Name, err)
	}
	if claim.UID != r.claim.UID || claim.DeletionTimestamp != nil || claim.Spec.VolumeName != "" {
		return false, nil
	}
	if prime.Spec.VolumeName == "" {
		return false, nil
	}
	volume, err := c.operations.GetVolume(ctx, prime.Spec.VolumeName)
	if err != nil {
		return false, fmt.Errorf("read volume %s of prime claim %s/%s: %w", prime.Spec.VolumeName, prime.Namespace, prime.Name, err)
	}
	ref := volume.Spec.ClaimRef
	return ref != nil && ref.Namespace == prime.Namespace && ref.Name == prime.Name && ref.UID == prime.UID, nil
}
