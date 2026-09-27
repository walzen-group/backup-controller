package runs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The tests in this file check that a RestoreRun reports a failure to put
// back what it changed the way a BackupRun does (RR7b): RestartFailed while
// the app is still down, ReleaseFailed when only a release step failed, each
// with the reason's Warning event and advice that fits.

// A run deleted while it still holds its Leases, and that cannot release
// them, says which step failed with reason ReleaseFailed and a Warning event.
// The app is back by then, so RestartFailed would say it is down, and the
// advice names the Leases rather than the workloads.
func TestADeletedRestoreThatCannotReleaseItsLeasesSaysWhatFailed(t *testing.T) {
	run, job := quiescedMidRestore(t)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, lease)
	refused := errors.New("a policy refuses the delete")
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*coordinationv1.Lease); ok {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "leases"}, obj.GetName(), refused)
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r) // suspends the restore Job, and waits for the Job controller
	markSuspended(t, c, job)
	recorded(recorder)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the deletion pass succeeded, want the refused delete returned")
	}

	deleting := readRestoreRun(t, c)
	if len(deleting.Finalizers) == 0 {
		t.Error("the run dropped its finalizer, want the deletion held while its Leases are there")
	}
	if readyReason(deleting.Status.Conditions) != backupv1alpha1.ReasonReleaseFailed {
		t.Fatalf("reason = %q, message = %q; want %s", readyReason(deleting.Status.Conditions),
			readyMessage(deleting.Status.Conditions), backupv1alpha1.ReasonReleaseFailed)
	}
	if got := recorded(recorder); len(got) != 1 || !strings.HasPrefix(got[0], "Warning "+backupv1alpha1.ReasonReleaseFailed+" ") {
		t.Errorf("events = %q, want one Warning %s", got, backupv1alpha1.ReasonReleaseFailed)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want the stopped one deleted", jobs)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d, want the app back before the Leases are released", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization stayed suspended")
	}
	message := readyMessage(deleting.Status.Conditions)
	for _, want := range []string{"Lease", labelLeaseHolderUID, refused.Error()} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q does not name %q", message, want)
		}
	}
	for _, wrong := range []string{"scale Deployment", "resume Kustomization"} {
		if strings.Contains(message, wrong) {
			t.Errorf("message %q says %q, which is advice for a restart that failed", message, wrong)
		}
	}
}

// A run that cannot give the app back says so with reason RestartFailed, a
// Warning event, and the workload with the replicas a person can set by hand.
// Nothing is released: the app is still down.
func TestARestoreThatCannotGiveTheAppBackSaysWhatFailed(t *testing.T) {
	run, job := quiescedMidRestore(t)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, lease)
	r.Client = refuseDeploymentPatches(c)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	restoreStep(t, r) // suspends the restore Job, and waits for the Job controller
	markSuspended(t, c, job)
	recorded(recorder)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the timeout pass succeeded, want the refused patch returned")
	}

	after := readRestoreRun(t, c)
	if after.Status.Phase.Finished() || readyReason(after.Status.Conditions) != backupv1alpha1.ReasonRestartFailed {
		t.Fatalf("phase = %q, reason = %q; want the run unfinished with %s",
			after.Status.Phase, readyReason(after.Status.Conditions), backupv1alpha1.ReasonRestartFailed)
	}
	if got := recorded(recorder); len(got) != 1 || !strings.HasPrefix(got[0], "Warning "+backupv1alpha1.ReasonRestartFailed+" ") {
		t.Errorf("events = %q, want one Warning %s", got, backupv1alpha1.ReasonRestartFailed)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d, want the app still down", got)
	}
	message := readyMessage(after.Status.Conditions)
	for _, want := range []string{"Deployment " + appN, "2", "patch refused"} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q does not name %q", message, want)
		}
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it still held while the app is down", err)
	}
}

// A run that cannot delete its stopped restore Job says which Job it could
// not delete and what a person can delete by hand. The app is still down at
// that point, since the Job must be stopped before the app comes back, so
// the reason is RestartFailed and the message names the workload with the
// replicas a person can set and the Kustomization to resume.
func TestARestoreThatCannotDeleteItsJobSaysWhatFailed(t *testing.T) {
	run, job := quiescedMidRestore(t)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job)
	refused := errors.New("a policy refuses the delete")
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return apierrors.NewForbidden(schema.GroupResource{Group: "batch", Resource: "jobs"}, obj.GetName(), refused)
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	restoreStep(t, r) // suspends the restore Job, and waits for the Job controller
	markSuspended(t, c, job)
	recorded(recorder)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the timeout pass succeeded, want the refused delete returned")
	}

	after := readRestoreRun(t, c)
	if after.Status.Phase.Finished() || readyReason(after.Status.Conditions) != backupv1alpha1.ReasonRestartFailed {
		t.Fatalf("phase = %q, reason = %q; want the run unfinished with %s",
			after.Status.Phase, readyReason(after.Status.Conditions), backupv1alpha1.ReasonRestartFailed)
	}
	if got := recorded(recorder); len(got) != 1 || !strings.HasPrefix(got[0], "Warning "+backupv1alpha1.ReasonRestartFailed+" ") {
		t.Errorf("events = %q, want one Warning %s", got, backupv1alpha1.ReasonRestartFailed)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d, want the app still down while the restore Job is there", got)
	}
	message := readyMessage(after.Status.Conditions)
	for _, want := range []string{"restore Job " + job.Name, refused.Error(), "scale Deployment " + appN + " to 2", "resume Kustomization flux-system/" + appN} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q does not name %q", message, want)
		}
	}
}
