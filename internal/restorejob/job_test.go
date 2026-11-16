package restorejob

import (
	"slices"
	"testing"
)

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

// TestNewLimitsTheDownloadRate checks that the restore Job gives restic
// --limit-download only for a limit above 0. Without the flag, a restore
// reads from S3 as fast as the link allows, even when the admin set a limit.
//
// Parameters:
//   - t is the test handle that the go test runner gives.
//
// It has no return value. It reports an error when a limit of 20480 does not end
// the restic arguments with --limit-download 20480, or when a limit of 0
// adds the flag.
//
// restic's --limit-download is in KiB/s (restic restore --help in
// quay.io/backube/volsync:0.17.0), so the Job passes the value unchanged.
func TestNewLimitsTheDownloadRate(t *testing.T) {
	spec := Spec{Namespace: "app", Name: "restore-data", Image: "restic", Claim: "data", RepositorySecret: "data-restic", SnapshotID: "abc", LimitDownloadKiBps: 20480}
	args := New(spec).Spec.Template.Spec.Containers[0].Args
	if tail := args[len(args)-2:]; !slices.Equal(tail, []string{"--limit-download", "20480"}) {
		t.Errorf("last arguments = %q; want --limit-download 20480", tail)
	}

	spec.LimitDownloadKiBps = 0
	args = New(spec).Spec.Template.Spec.Containers[0].Args
	if slices.Contains(args, "--limit-download") {
		t.Errorf("arguments = %q for a limit of 0; want no --limit-download", args)
	}
}
