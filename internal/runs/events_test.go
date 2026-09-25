package runs

import (
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/client-go/tools/events"
)

// recorded drains what a fake recorder has received, one "type reason note"
// line per event.
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

// A namespace run that finds nothing to back up says why on the run's events,
// where kubectl describe and a UI's event list look.
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

// Each move of the Ready condition to another reason is one event, and a
// reconcile that leaves the reason where it was records none.
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

// A restore that fails records a Warning carrying the condition's message.
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

// A note past what events.k8s.io/v1 accepts is cut to fit, so the API server
// keeps the event instead of rejecting it.
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
