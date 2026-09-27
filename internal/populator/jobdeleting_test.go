package populator

import (
	"context"
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestAJobBeingDeletedIsLeftAlone checks that Populate neither resumes nor
// records a Job that is being deleted, even one recorded on the prime claim
// and waiting to be resumed, and returns errJobBeingDeleted until it is
// gone.
func TestAJobBeingDeletedIsLeftAlone(t *testing.T) {
	ctx := context.Background()
	ops := recordedSuspendedJob(t)
	job := ops.job(t, "claim-123")
	job.Finalizers = append(job.Finalizers, "test.wlz.li/hold")
	if err := ops.cluster.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := ops.cluster.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}

	err := newCallbacks(ops, monday).Populate(ctx, params())
	if !errors.Is(err, errJobBeingDeleted) {
		t.Fatalf("Populate() = %v, want errJobBeingDeleted", err)
	}
	if got := ops.job(t, "claim-123"); got == nil || !ptr.Deref(got.Spec.Suspend, false) {
		t.Fatalf("Job = %+v, want it left suspended", got)
	}
}
