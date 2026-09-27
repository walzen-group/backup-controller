package populator

import (
	"context"
	"slices"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// hasDelete reports whether a restore Job's restic container passes
// --delete.
func hasDelete(job *batchv1.Job) bool {
	return slices.ContainsFunc(job.Spec.Template.Spec.Containers, func(c corev1.Container) bool {
		return slices.Contains(c.Args, "--delete")
	})
}

// TestEveryPopulatorJobRestoresWithDelete checks that the first restore Job
// and the Job that replaces a failed one both pass --delete. The replacement
// picks the snapshot again, here a newer one, and restores it over what the
// failed Job left, so files of the earlier snapshot would stay behind.
func TestEveryPopulatorJobRestoresWithDelete(t *testing.T) {
	ctx := context.Background()
	ops := newStrictOperations(t, backupv1alpha1.VolumeRestoreStatus{})
	if _, err := librarySync(ctx, newCallbacks(ops, monday), ops.paramsFor(t, "notes", "claim-123")); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	first := ops.job(t, "claim-123")
	if !hasDelete(first) {
		t.Errorf("first Job args = %v, want --delete", first.Spec.Template.Spec.Containers[0].Args)
	}
	ops.runJob(t, "claim-123")
	jobFailedWithExit1(t, ops.fakeOperations, first)

	callbacks := newCallbacks(ops, monday, tuesday)
	for range 3 {
		_, _ = librarySync(ctx, callbacks, ops.paramsFor(t, "notes", "claim-123"))
	}
	replaced := ops.job(t, "claim-123")
	if replaced == nil || replaced.UID == first.UID || replaced.Annotations[restorejob.AnnotationSnapshotID] != tuesday.ID {
		t.Fatalf("Job = %+v, want a replacement for tuesday", replaced)
	}
	if !hasDelete(replaced) {
		t.Errorf("replacement Job args = %v, want --delete", replaced.Spec.Template.Spec.Containers[0].Args)
	}
}
