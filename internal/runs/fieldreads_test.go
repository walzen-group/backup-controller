package runs

import (
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
)

// The tests in this file stand in for a release of another project that
// moves or renames a field the controller reads, and check that the run
// fails loudly, naming the field, where reading the field as unset would
// make it act on a wrong answer.

// A run that Kueue never admits, as when a Kueue release renames the
// Admitted condition or the queueName field, does not wait forever holding
// the namespace's schedule: once its timeout has passed since its creation
// it fails, naming Kueue and its Workload, and gives the quota back. Its
// items record the reason TimedOut.
func TestAQueuedRunThatKueueNeverAdmitsFails(t *testing.T) {
	t.Parallel()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(), localQueueObject())
	step(t, r) // plan
	step(t, r) // admit: the Workload waits
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Fatalf("phase = %q, want Queued", run.Status.Phase)
	}
	step(t, r) // still waiting within the timeout
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Fatalf("phase = %q within the timeout, want Queued", run.Status.Phase)
	}

	r.Now = func() time.Time { return frozen.Add(2 * time.Hour) } // the run's timeout is an hour
	step(t, r)

	run := readBackupRun(t, c)
	message := readyMessage(run.Status.Conditions)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(message, "Kueue") || !strings.Contains(message, kueue.WorkloadName(runUID)) {
		t.Fatalf("phase = %q, Ready = %q; want Failed naming Kueue and the Workload %s", run.Status.Phase, message, kueue.WorkloadName(runUID))
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut {
		t.Errorf("item = %+v, want Failed with reason TimedOut, as every other timeout gives", item)
	}
	if _, ok := getUnstructured(t, c, kueue.WorkloadGVK, ns, kueue.WorkloadName(runUID)); ok {
		t.Error("the Workload outlived the failed run")
	}
}
