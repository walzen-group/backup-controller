package populator

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
)

// jobsResource is the group and resource of a Job, for the API errors.
var jobsResource = schema.GroupResource{Group: "batch", Resource: "jobs"}

// lastReady returns the Ready condition of the last status write.
func lastReady(t *testing.T, ops *fakeOperations) (backupv1alpha1.RestorePhase, string, string) {
	t.Helper()
	if len(ops.statuses) == 0 {
		t.Fatal("no status write")
	}
	last := ops.statuses[len(ops.statuses)-1]
	ready := findCondition(last.Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || len(last.Status.Claims) == 0 {
		t.Fatalf("status = %+v, want Ready and the claim's entry", last.Status)
	}
	return last.Status.Claims[0].Phase, ready.Reason, ready.Message
}

// TestARefusedJobCreateShowsOnReady checks that a restore Job create the
// API server refuses with 422 Invalid, as an admission policy does, puts
// reason RestoreJobRefused and the API server's answer on Ready, creates
// nothing and returns an error, so the claim stays Pending. Once the cause
// is gone, the next sync creates the Job and Ready reads Restoring.
func TestARefusedJobCreateShowsOnReady(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	ops.refuseCreate = apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "restore-claim-123", field.ErrorList{
		field.Invalid(field.NewPath("spec", "template", "spec", "securityContext", "sysctls"), nil, "sysctls are not allowed"),
	})
	callbacks := newCallbacks(ops, monday)

	if done, err := librarySync(ctx, callbacks, params()); err == nil || done {
		t.Fatalf("sync = %t, %v; want an error", done, err)
	}
	phase, reason, message := lastReady(t, ops)
	if phase != backupv1alpha1.RestorePhaseFailed || reason != backupv1alpha1.ReasonRestoreJobRefused || !strings.Contains(message, "sysctls are not allowed") {
		t.Fatalf("entry %s, Ready %s %q; want Failed and RestoreJobRefused with the API server's answer", phase, reason, message)
	}
	if job := ops.job(t, "claim-123"); job != nil {
		t.Fatalf("Job %s exists after a refused create", job.Name)
	}

	ops.refuseCreate = nil
	if _, err := librarySync(ctx, callbacks, params()); err != nil {
		t.Fatalf("sync once the create is admitted: %v", err)
	}
	if phase, reason, _ := lastReady(t, ops); phase != backupv1alpha1.RestorePhaseRestoring || reason != backupv1alpha1.ReasonRestoring {
		t.Fatalf("entry %s, Ready %s; want Restoring", phase, reason)
	}
}

// TestARefusedResumeShowsOnReady checks that a resume the API server
// refuses with 403 Forbidden puts reason RestoreJobRefused on Ready, leaves
// the Job suspended and returns an error.
func TestARefusedResumeShowsOnReady(t *testing.T) {
	ops := recordedSuspendedJob(t)
	ops.refuseResume = apierrors.NewForbidden(jobsResource, "restore-claim-123", nil)

	if done, err := librarySync(context.Background(), newCallbacks(ops, monday), params()); err == nil || done {
		t.Fatalf("sync = %t, %v; want an error", done, err)
	}
	if phase, reason, _ := lastReady(t, ops); phase != backupv1alpha1.RestorePhaseFailed || reason != backupv1alpha1.ReasonRestoreJobRefused {
		t.Fatalf("entry %s, Ready %s; want Failed and RestoreJobRefused", phase, reason)
	}
	if !ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, false) {
		t.Error("the Job runs after a refused resume")
	}
}

// TestAnotherJobErrorIsNoRefusal checks that a create that fails for
// another reason, such as a timeout, is retried without the
// RestoreJobRefused reason.
func TestAnotherJobErrorIsNoRefusal(t *testing.T) {
	ops := populatedOperations(t)
	ops.refuseCreate = apierrors.NewTimeoutError("the API server is slow", 1)

	if _, err := librarySync(context.Background(), newCallbacks(ops, monday), params()); err == nil {
		t.Fatal("sync returned no error")
	}
	for _, status := range ops.statuses {
		if ready := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady); ready != nil && ready.Reason == backupv1alpha1.ReasonRestoreJobRefused {
			t.Fatalf("Ready = %+v, want no RestoreJobRefused for a timeout", ready)
		}
	}
}
