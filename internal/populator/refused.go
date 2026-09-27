package populator

import (
	"context"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// jobRefusedError is a restore Job create or resume that the API server
// refused with 403 Forbidden or 422 Invalid, such as an admission policy
// that refuses the Job's pod security context. The same request fails the
// same way until the VolumeRestore or the cluster changes.
type jobRefusedError struct {
	// err is the failed call's error, with the API server's answer.
	err error
}

// Error returns the failed call's error.
func (e *jobRefusedError) Error() string {
	return e.err.Error()
}

// Unwrap returns the failed call's error.
func (e *jobRefusedError) Unwrap() error {
	return e.err
}

// jobCallError returns the error of a failed restore Job create or resume,
// and records a refusal on the VolumeRestore's status first.
//
// Parameters:
//   - r is the claim's restore.
//   - err is the failed call's error, wrapped with what the call was.
//
// It returns err unchanged unless the API server refused the call with 403
// Forbidden or 422 Invalid, which apierrors reads from the typed status of
// the error. For a refusal it marks the claim Failed with reason
// RestoreJobRefused and the API server's answer, and returns a
// *jobRefusedError, or the error of that status write. Either way the
// library requeues the claim and the next sync tries the call again.
func (c *Callbacks) jobCallError(ctx context.Context, r restore, err error) error {
	if !apierrors.IsForbidden(err) && !apierrors.IsInvalid(err) {
		return err
	}
	refusal := &jobRefusedError{err: err}
	if err := c.markFailed(ctx, r.vr, r.claim, backupv1alpha1.ReasonRestoreJobRefused, refusal); err != nil {
		return err
	}
	return claimError(r.claim, refusal)
}
