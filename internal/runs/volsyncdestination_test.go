package runs

import (
	"path/filepath"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// crdsWithDestinationAt returns the CRD files of newClient with VolSync's
// ReplicationDestination served at the version given alone and
// ReplicationSource still at v1alpha1, as after a VolSync release that
// moves only ReplicationDestination off v1alpha1.
func crdsWithDestinationAt(t *testing.T, version string) []string {
	t.Helper()
	files := crdsWithVolSyncAt(t, version)
	for i, f := range files {
		if filepath.Base(f) == filepath.Base(volsyncFiles[0]) {
			files[i] = volsyncFiles[0]
		}
	}
	return files
}

// The controller no longer uses ReplicationDestination, so a VolSync that
// serves it at v1beta1 alone, with ReplicationSource still at v1alpha1, is
// no incompatibility: the check reports nothing, and a BackupRun goes on
// and never ends with reason VolSyncUnsupported.
func TestAVolSyncThatMovesOnlyReplicationDestinationEndsNoBackup(t *testing.T) {
	c := newClientWithCRDs(t, crdsWithDestinationAt(t, "v1beta1"), backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment())
	if err := volsyncSourceUnserved(c.RESTMapper()); err != nil {
		t.Fatalf("volsyncSourceUnserved() = %v, want nothing to report", err)
	}
	served := servingOnly(c)
	r := &BackupRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	for range 4 {
		_ = tryStep(r)
	}

	run := readBackupRun(t, c)
	if reason := readyReason(run.Status.Conditions); reason == backupv1alpha1.ReasonVolSyncUnsupported || run.Status.Phase == backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase %q, Ready reason %q (%s); want the run going on", run.Status.Phase, reason, readyMessage(run.Status.Conditions))
	}
}
