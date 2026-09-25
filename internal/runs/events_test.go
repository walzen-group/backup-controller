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

// TestARefusedRunRecordsAWarningWithTheReason checks that a namespace run that
// finds nothing to back up records a Warning event on the run that says why.
// kubectl describe and a UI's event list show the run's events.
func TestARefusedRunRecordsAWarningWithTheReason(t *testing.T) {
	r, _ := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }))
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder

	step(t, r)

	got := recorded(recorder)
	want := `Warning Invalid nothing in this namespace is marked backup.wlz.li/enabled: "true"`
	if len(got) != 1 || got[0] != want {
		t.Fatalf("events = %q, want [%q]", got, want)
	}
}

// TestARunRecordsOneEventPerReason checks that each change of the Ready
// condition's reason records one event, and that a reconcile which leaves the
// reason as it was records none.
func TestARunRecordsOneEventPerReason(t *testing.T) {
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

// TestAFailedRestoreRecordsAWarning checks that a RestoreRun that fails
// records a Warning event with the Ready condition's reason.
func TestAFailedRestoreRecordsAWarning(t *testing.T) {
	r, _ := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-01T00:00:00Z")),
		claim(), volumeRestore(), repository())
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder

	restoreStep(t, r)

	got := recorded(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Warning NoBackupInReach ") {
		t.Fatalf("events = %q, want one Warning NoBackupInReach", got)
	}
}

// TestALongNoteIsCutToTheAPILimit checks that fitNote cuts a note longer than
// events.k8s.io/v1 accepts to at most maxNote bytes, on a character boundary,
// so the API server accepts the event.
func TestALongNoteIsCutToTheAPILimit(t *testing.T) {
	note := strings.Repeat("ä", maxNote)
	cut := fitNote(note)
	if len(cut) > maxNote {
		t.Fatalf("len = %d, want at most %d", len(cut), maxNote)
	}
	if !strings.HasPrefix(note, cut) || !strings.HasSuffix(cut, "ä") {
		t.Fatalf("the note was not cut on a character boundary")
	}
}
