package populator

import (
	"context"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// recordOnPrime records a Job's UID on the prime claim of notes, as Populate
// does before it resumes the Job.
func (f *fakeOperations) recordOnPrime(t *testing.T, job *batchv1.Job) {
	t.Helper()
	prime := f.prime(t, "claim-123")
	prime.Annotations = map[string]string{AnnotationJobUID: string(job.UID)}
	if err := f.cluster.Update(context.Background(), prime); err != nil {
		t.Fatal(err)
	}
}

// recordedSuspendedJob returns a cluster whose notes Job is recorded on the
// prime claim and waits to be resumed.
func recordedSuspendedJob(t *testing.T) *fakeOperations {
	t.Helper()
	ops := populatedOperations(t)
	ops.recordOnPrime(t, ops.createJob(t))
	return ops
}

// TestNoJobIsResumedOnceTheVolumeIsHandedOver checks that Populate resumes
// no Job once the library has handed the prime claim's volume to the app
// claim: the app claim names a volume, or the volume's claimRef no longer
// names the prime claim. The Job's pod would write the app's volume.
func TestNoJobIsResumedOnceTheVolumeIsHandedOver(t *testing.T) {
	for name, handOver := range map[string]func(*testing.T, *fakeOperations){
		"the app claim names a volume": func(t *testing.T, ops *fakeOperations) {
			claim, err := ops.GetClaim(context.Background(), appNS, "notes")
			if err != nil {
				t.Fatal(err)
			}
			claim.Spec.VolumeName = "pv-123"
			if err := ops.cluster.Update(context.Background(), claim); err != nil {
				t.Fatal(err)
			}
		},
		"the volume's claimRef names the app claim": func(t *testing.T, ops *fakeOperations) {
			volume, err := ops.GetVolume(context.Background(), "pv-123")
			if err != nil {
				t.Fatal(err)
			}
			volume.Spec.ClaimRef = &corev1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1", Namespace: appNS, Name: "notes", UID: "claim-123"}
			if err := ops.cluster.Update(context.Background(), volume); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ops := recordedSuspendedJob(t)
			handOver(t, ops)
			if err := newCallbacks(ops, monday).Populate(context.Background(), params()); err != nil {
				t.Fatalf("Populate() error = %v", err)
			}
			if !ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, false) {
				t.Error("Populate resumed the Job after the volume was handed over")
			}
		})
	}
}
