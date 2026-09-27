package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
)

// The tests in this file check that a RestoreRun reports a failure to put
// back what it changed the way a BackupRun does (RR7b): RestartFailed while
// the app is still down, ReleaseFailed when only a release step failed, each
// with the reason's Warning event and advice that fits.

// A run that cannot give the app back says so with reason RestartFailed, a
// Warning event, and the workload with the replicas a person can set by hand.
// Nothing is released: the app is still down.
func TestARestoreThatCannotGiveTheAppBackSaysWhatFailed(t *testing.T) {
	t.Parallel()
	run, job := quiescedMidRestore(t)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, lease)
	r.Client = refuseDeploymentScales(c)
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
	for _, want := range []string{"Deployment " + appN, "2", "scale refused"} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q does not name %q", message, want)
		}
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it still held while the app is down", err)
	}
}
