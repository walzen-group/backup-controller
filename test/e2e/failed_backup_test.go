//go:build e2e

package e2e

import (
	"fmt"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// TestAFailedBackupResumesTheApp backs up a namespace whose restic Secret
// has the wrong password for an existing repository, so the mover fails.
// VolSync retries the mover up to its Job's backoff limit of 8 and then
// records the result Failed, about 20 minutes later. The run notices that
// result and ends Failed with a reason on the item, well before its
// 45-minute timeout, and the app runs again.
//
// The retries of VolSync are the longest wait of the suite. So the test
// starts the failing run before it calls t.Parallel, and go test runs that
// part with the scenarios that do not run in parallel. This file sorts
// before pause_test.go and queue_test.go, so the retries start at the
// beginning of the suite and the other scenarios run while they go on.
func TestAFailedBackupResumesTheApp(t *testing.T) {
	a := newApp(t, "failed")
	a.publish(volumeManifests())
	a.write("state")
	first := a.backup("first", "source: data")
	mustSucceed(t, "BackupRun", "first", first.Status.Phase, first.Status.Conditions, a.describe)

	kubectl(t, "", "-n", a.ns, "patch", "secret", "restic", "--type=merge", "-p", `{"stringData":{"RESTIC_PASSWORD":"wrong"}}`)
	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: BackupRun\nmetadata:\n  name: wrong-password\n  namespace: %s\nspec:\n  all: true\n  timeout: 45m\n", a.ns))
	waitFor(t, "BackupRun wrong-password to be admitted", 5*time.Minute, func() (bool, string) {
		var run backupv1alpha1.BackupRun
		if err := getJSON(a.ns, "backuprun", "wrong-password", &run); err != nil {
			return false, err.Error()
		}
		return run.Status.StartedAt != nil, fmt.Sprintf("%s %s", run.Status.Phase, ready(run.Status.Conditions))
	}, a.describe)
	t.Parallel()

	failed := a.waitBackup("wrong-password", 50*time.Minute)
	if failed.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("BackupRun wrong-password ended %s, want Failed\n%s", failed.Status.Phase, a.describe())
	}
	if reason := readyReason(failed.Status.Conditions); reason == backupv1alpha1.ReasonTimedOut {
		t.Errorf("BackupRun wrong-password failed only at its timeout; the run has to notice the failed mover")
	}
	for _, item := range failed.Status.Items {
		if item.Kind == "ReplicationSource" && item.Phase != backupv1alpha1.ItemFailed {
			t.Errorf("item %s is %s, want Failed", item.Name, item.Phase)
		}
	}
	a.waitNote("the app to run again after the failed backup", "state")
}
