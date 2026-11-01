package restorejob

import "testing"

// TestNewLogsProgress checks that the restore Job tells restic to print a
// progress line every 10 seconds, so the Job log shows how far a restore is.
//
// Parameters:
//   - t is the test handle that the go test runner gives.
//
// It returns nothing. It reports an error when the restic container has no
// RESTIC_PROGRESS_FPS variable or when the value is different from "0.1".
//
// restic prints progress lines only to a terminal, or at the rate that
// RESTIC_PROGRESS_FPS gives. The Job has no terminal. VolSync's mover sets
// the same value in mover-restic/entry.sh, so a restore log shows progress
// at the same rate as a backup log.
func TestNewLogsProgress(t *testing.T) {
	job := New(Spec{Namespace: "app", Name: "restore-data", Image: "restic", Claim: "data", RepositorySecret: "data-restic", SnapshotID: "abc"})
	for _, env := range job.Spec.Template.Spec.Containers[0].Env {
		if env.Name == "RESTIC_PROGRESS_FPS" {
			if env.Value != "0.1" {
				t.Errorf("RESTIC_PROGRESS_FPS = %q; want %q", env.Value, "0.1")
			}
			return
		}
	}
	t.Errorf("the restic container has no RESTIC_PROGRESS_FPS variable; want %q", "0.1")
}
