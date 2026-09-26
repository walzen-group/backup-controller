package runs

import (
	"context"
	"errors"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The tests in this file check that a RestoreRun reports a volume restored
// only when the mover's log names the snapshot the checks recorded
// (designs/restorerun.md C5). VolSync marks the run's trigger complete
// whatever the mover restored, including nothing at all.

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
func restoreLogFor(short string) string {
	return strings.ReplaceAll(recordedRestoreLog, recordedRestoreID, short)
}

// truncatedLog returns the last max bytes of logs, the way VolSync's
// TruncateString (internal/controller/utils/podlogs.go:219-227 at v0.16.0)
// keeps the filtered log within MOVER_LOG_MAX_BYTES.
func truncatedLog(logs string, max int) string {
	if max <= 0 {
		return ""
	}
	if len(logs) > max {
		return logs[len(logs)-max:]
	}
	return logs
}

// restoredStatus returns the status VolSync writes on a ReplicationDestination
// whose mover completed the run's trigger and restored the snapshot whose
// short ID is short: lastManualSync, and a Successful latestMoverStatus with
// the recorded log naming that snapshot.
func restoredStatus(short string) *volsyncv1alpha1.ReplicationDestinationStatus {
	return &volsyncv1alpha1.ReplicationDestinationStatus{
		LastManualSync:    string(restoreUID),
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultSuccessful, Logs: restoreLogFor(short)},
	}
}

// finishMover marks the ReplicationDestination of the run's first item as
// VolSync marks one whose mover succeeded: the run's trigger completed, and
// latestMoverStatus Successful with the given logs, in one status write.
func finishMover(t *testing.T, c client.Client, logs string) {
	t.Helper()
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, readRestoreRun(t, c).Status.Items[0].Destination, rd)
	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LastManualSync:    string(restoreUID),
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultSuccessful, Logs: logs},
	}
	if err := c.Status().Update(context.Background(), rd); err != nil {
		t.Fatal(err)
	}
}

// startedRestore returns a reconciler whose run of the given shape selected
// monday and created its destination, and the client.
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

// targetClaim is the claim a restore of the given shape writes into.
func targetClaim(into string) string {
	if into != "" {
		return into
	}
	return claimN
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

// A restore whose mover's log names the snapshot the checks recorded
// succeeds.
func TestARestoreConfirmsTheSnapshotFromTheMoversLog(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			r, c := startedRestore(t, shape.mutate)
			finishMover(t, c, restoreLogFor(monday.ShortID()))
			restoreStep(t, r)
			restoreStep(t, r)

			run := readRestoreRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded {
				t.Fatalf("phase = %q, item = %+v (%s); want Succeeded", run.Status.Phase, run.Status.Items[0], readyMessage(run.Status.Conditions))
			}
		})
	}
}

// A mover that found no snapshot at or before its pin prints "No eligible
// snapshots found", restores nothing and exits 0, and VolSync completes the
// trigger. The item fails and says what the claim holds.
func TestARestoreThatRestoredNothingFails(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			r, c := startedRestore(t, shape.mutate)
			finishMover(t, c, recordedNoEligibleLog)
			restoreStep(t, r)
			restoreStep(t, r)

			holds := "claim " + claimN + " holds what it held before"
			if shape.into != "" {
				holds = "claim " + shape.into + " is empty"
			}
			expectItemFailed(t, c, "the mover found no snapshot at or before 2026-09-21T05:00:02Z and wrote nothing; "+holds)
			if names := destinations(t, c); len(names) != 0 {
				t.Errorf("destinations = %v, want the failed item's deleted", names)
			}
		})
	}
}

// A mover that restored another snapshot than the one the checks recorded
// fails the item, naming both.
func TestARestoreThatRestoredAnotherSnapshotFails(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			r, c := startedRestore(t, shape.mutate)
			finishMover(t, c, restoreLogFor(sunday.ShortID()))
			restoreStep(t, r)
			restoreStep(t, r)

			expectItemFailed(t, c, "the mover restored snapshot 2edf5bab where the checks selected 6e473100; claim "+
				targetClaim(shape.into)+" now holds 2edf5bab")
		})
	}
}

// A mover whose log names no snapshot fails the item: the log is empty when
// VolSync can't read the pod's logs or MOVER_LOG_MAX_BYTES is 0, and cut
// when the filtered log is longer than that setting. The run can't tell what
// the claim holds then.
func TestARestoreWithoutLogsFails(t *testing.T) {
	logs := map[string]string{
		"empty":     "",
		"truncated": truncatedLog(restoreLogFor(monday.ShortID()), 64),
	}
	for _, shape := range restoreShapes {
		for name, log := range logs {
			t.Run(shape.name+"/"+name, func(t *testing.T) {
				r, c := startedRestore(t, shape.mutate)
				finishMover(t, c, log)
				restoreStep(t, r)
				restoreStep(t, r)

				expectItemFailed(t, c, "the mover finished, but its logs name no snapshot, so the run cannot confirm what claim "+
					targetClaim(shape.into)+" holds", "MOVER_LOG_MAX_BYTES bytes (1024 by default)", "Logs: "+log)
			})
		}
	}
}

// A pass that failed an item on its mover's log and lost the status write
// leaves the item Running and the destination in place. The next pass reads
// the same log, fails the item again, and only then deletes the destination.
func TestAMoverLogFailureSurvivesALostStatusWrite(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			r, c := startedRestore(t, shape.mutate)
			finishMover(t, c, recordedNoEligibleLog)

			r.Client = loseNextStatusWrite(c)
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
				t.Fatal("the pass whose status write was lost succeeded, want the error returned")
			}
			if names := destinations(t, c); len(names) != 1 {
				t.Fatalf("destinations = %v, want the item's kept while its end is not recorded", names)
			}
			r.Client = c
			restoreStep(t, r)
			restoreStep(t, r)

			expectItemFailed(t, c, "the mover found no snapshot")
			if names := destinations(t, c); len(names) != 0 {
				t.Errorf("destinations = %v, want it deleted once the end is recorded", names)
			}
		})
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

// An into restore failed on its mover's log whose final write is lost after
// the destination was deleted ends Failed on the next pass. It does not take
// the missing destination for one never created, and creates no new mover.
func TestAnIntoRestoreFailedOnItsLogIsNotStartedAgain(t *testing.T) {
	r, c := startedRestore(t, fromRepository)
	finishMover(t, c, recordedNoEligibleLog)

	r.Client = loseFailedRunWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose final write was lost succeeded, want the error returned")
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Fatalf("destinations = %v, want the item's deleted before the lost write", names)
	}
	r.Client = c
	restoreStep(t, r)

	expectItemFailed(t, c, "the mover found no snapshot")
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want no new mover", names)
	}
}

// runningRun returns the RestoreRun back-to-monday with a Running volume item
// recording the snapshot given in snapshot and no snapshotTime, and the
// item's destination. The checks never record an item like it; it stands for
// a status the run can't confirm anything from.
func runningRun(snapshot string) (*backupv1alpha1.RestoreRun, *volsyncv1alpha1.ReplicationDestination) {
	started := metav1.NewTime(frozen)
	name := destinationName(restoreUID, 0)
	run := restoreRun(asOf("2026-09-21T06:00:00Z"), func(r *backupv1alpha1.RestoreRun) {
		one := int32(1)
		r.Spec.Claim, r.Spec.Previous = claimN, &one
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = &started
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN,
			Phase: backupv1alpha1.ItemRunning, Snapshot: snapshot, Destination: name}}
	})
	destination := directDestination(run, run.Status.Items[0], restoreSettings{Secret: repoN}, name)
	return run, destination
}

// A Running item that records no snapshot can't be confirmed from any log,
// so it fails whatever the mover restored.
func TestARunningItemWithoutASnapshotFails(t *testing.T) {
	run, destination := runningRun("")
	r, c := restoreReconciler(t, nil, run, destination, claim(), volumeRestore(), repository())
	finishMover(t, c, restoreLogFor(monday.ShortID()))
	restoreStep(t, r)
	restoreStep(t, r)

	expectItemFailed(t, c, "records no snapshot", "cannot confirm what claim "+claimN+" holds")
}
