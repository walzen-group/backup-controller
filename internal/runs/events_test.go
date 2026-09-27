package runs

import (
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/client-go/tools/events"
)

// recorded drains the events a fake recorder has received and returns one
// "type reason note" line per event.
func recorded(recorder *events.FakeRecorder) []string {
	var lines []string
	for {
		select {
		case line := <-recorder.Events:
			lines = append(lines, line)
		default:
			return lines
		}
	}
}

// TestARunRecordsOneEventPerReason checks that each change of the Ready
// condition's reason records one event, and that a reconcile which leaves the
// reason as it was records none.
func TestARunRecordsOneEventPerReason(t *testing.T) {
	t.Parallel()
	r, _ := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder

	step(t, r) // plan
	step(t, r) // admit
	step(t, r) // start
	step(t, r) // still waiting on VolSync

	want := []string{
		"Normal Queued waiting for the backup queue to admit the run",
		"Normal Running backing up",
	}
	got := recorded(recorder)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("events = %q, want %q", got, want)
	}
}
