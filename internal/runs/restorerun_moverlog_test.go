package runs

import (
	"context"
	"errors"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The tests in this file check how the controller reads a VolSync restore
// mover's log. No restore reads one any more: in place and into a new claim,
// a RestoreRun reads its own restore Job's result instead, and the reader
// goes with the destination code.

// recordedRestoreLog and recordedNoEligibleLog are the status.latestMoverStatus.logs
// VolSync 0.16.0 wrote on two ReplicationDestinations built like
// directDestination builds them, recorded by the e2e test
// TestRestoreMoverLogNamesTheSnapshot on the docker-desktop cluster on
// 2026-09-26. The first was pinned to the second of snapshot 6526a2ff, the
// second an hour before every snapshot. Both movers ended Successful and
// completed their trigger.
const (
	recordedRestoreLog = "RESTORE_OPTIONS: --delete\n" +
		"restoring snapshot 6526a2ff of [/data] at 2026-09-26 09:03:53.753460659 +0000 UTC by root@volsync to .\n" +
		"Restic completed in 3s"
	recordedRestoreID     = "6526a2ff"
	recordedNoEligibleLog = "No eligible snapshots found\n" +
		"=== No data will be restored ===\n" +
		"Restic completed in 2s"
)

// restoreLogFor returns the recorded restore log with the restored
// snapshot's short ID replaced by short.

// startedRestore returns a reconciler whose run of the given shape selected
// monday and created its restore Job, and the client.
func startedRestore(t *testing.T, mutate func(*backupv1alpha1.RestoreRun)) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	r, c := restoreReconciler(t, nil, restoreRun(mutate), claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create
	item := readRestoreRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning || item.Snapshot != monday.ShortID() {
		t.Fatalf("item = %+v; want Running on monday's snapshot", item)
	}
	return r, c
}

// The recorded logs read as what the movers did: the first restored
// 6526a2ff, the second nothing. restic before 0.17 printed the snapshot in
// angle brackets, and that form reads the same.
func TestTheRecordedMoverLogsSayWhatTheMoverRestored(t *testing.T) {
	ids, none := restoredSnapshots(recordedRestoreLog)
	if len(ids) != 1 || ids[0] != recordedRestoreID || none {
		t.Errorf("restore log: ids = %v, no eligible = %v; want [%s], false", ids, none, recordedRestoreID)
	}
	ids, none = restoredSnapshots(recordedNoEligibleLog)
	if len(ids) != 0 || !none {
		t.Errorf("no-eligible log: ids = %v, no eligible = %v; want none, true", ids, none)
	}
	ids, _ = restoredSnapshots("restoring <Snapshot 1a2b3c4d of [/data] at 2022-01-01 00:00:00 +0000 UTC by root@volsync> to .")
	if len(ids) != 1 || ids[0] != "1a2b3c4d" {
		t.Errorf("restic 0.16 log: ids = %v, want [1a2b3c4d]", ids)
	}
}

// loseFailedRunWrite returns a client over c that fails, with a conflict,
// the first status write that ends a RestoreRun Failed: finish's write,
// which comes after it deleted the run's destinations.
func loseFailedRunWrite(c client.Client) client.Client {
	lost := false
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if run, ok := obj.(*backupv1alpha1.RestoreRun); ok && !lost && run.Status.Phase == backupv1alpha1.RunPhaseFailed {
				lost = true
				return apierrors.NewConflict(backupv1alpha1.GroupVersion.WithResource("restoreruns").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}
