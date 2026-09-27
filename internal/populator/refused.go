package populator

import (
	"context"
	"errors"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation/field"
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

// uidField is the path of an object's UID in the API server's field errors.
var uidField = field.NewPath("metadata", "uid").String()

// jobReplaced reports whether a failed resume met a restore Job created
// again under the same name since the resumed one was read.
//
// Parameters:
//   - err is the failed resume's error, wrapped with what the call was.
//
// It returns true only for a 422 Invalid whose typed status carries a
// FieldValueInvalid cause on field metadata.uid. The resume is a merge
// patch that carries the UID of the Job as read, and the API server answers
// a patch that would change a stored object's UID with that cause
// (ValidateObjectMetaAccessorUpdate, k8s.io/apimachinery@v0.36.0
// pkg/api/validation/objectmeta.go:332). That answer says the Job was
// replaced, so the caller retries without the RestoreJobRefused reason, and
// the next sync reads the new Job.
func jobReplaced(err error) bool {
	var status apierrors.APIStatus
	if !apierrors.IsInvalid(err) || !errors.As(err, &status) || status.Status().Details == nil {
		return false
	}
	for _, cause := range status.Status().Details.Causes {
		if cause.Type == metav1.CauseTypeFieldValueInvalid && cause.Field == uidField {
			return true
		}
	}
	return false
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
