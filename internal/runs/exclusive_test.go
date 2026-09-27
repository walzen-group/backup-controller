package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// backingUp returns the claim's ReplicationSource with the open trigger of
// the BackupRun manual-notes (see otherRun), which VolSync has not completed.
func backingUp() *volsyncv1alpha1.ReplicationSource {
	source := idleSource()
	source.Spec.Trigger.Manual = TriggerFor(otherRunUID)
	source.Spec.Restic = &volsyncv1alpha1.ReplicationSourceResticSpec{Repository: repoN}
	return source
}

// ownSourceTag returns the manual tag on the claim's ReplicationSource, or
// "" when there is no source.
func ownSourceTag(t *testing.T, c client.Client) string {
	t.Helper()
	sources := &volsyncv1alpha1.ReplicationSourceList{}
	if err := c.List(context.Background(), sources, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	for _, s := range sources.Items {
		if s.Name == claimN {
			return manualTag(&s)
		}
	}
	return ""
}

// startOtherBackup stands in for the BackupRun manual-notes (see otherRun)
// starting after a restore's checks: it creates the run and the claim's
// ReplicationSource with the run's open trigger (see backingUp), each with
// its status. The API server gives the created run a UID of its own, and the
// trigger is made from it.
func startOtherBackup(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()
	run, source := otherRun(), backingUp()
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	run.Status = otherRun().Status
	run.Status.Items[0].Trigger = TriggerFor(run.UID)
	if err := c.Status().Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	source.Spec.Trigger.Manual = TriggerFor(run.UID)
	if err := c.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	source.Status = backingUp().Status
	if err := c.Status().Update(ctx, source); err != nil {
		t.Fatal(err)
	}
}

// A restore past its checks whose claim's repository a backup started on
// since waits with reason SourceBusy, names the backup, and creates no
// restore Job. (A backup already running at the checks holds the
// checks themselves; see
// TestARestoreSelectsItsSnapshotOnlyOnceABackupOfItsRepositoryHasFinished.)
func TestARestoreWaitsWhileABackupRuns(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	startOtherBackup(t, c)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if msg := readyMessage(run.Status.Conditions); !strings.Contains(msg, "BackupRun manual-notes") {
		t.Errorf("message = %q, want it to name the BackupRun manual-notes", msg)
	}
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("restore Jobs = %v, want none while the backup runs", names)
	}
}

// A backup and a restore of the same claim never wait on each other:
// whichever writes its mover object first goes on, and the other waits for
// it and names it. That holds when one starts well before the other, and when
// both are reconciled in turn from the start, as in one pass of the manager.
func TestABackupAndARestoreStartedTogetherNeverDeadlock(t *testing.T) {
	t.Parallel()
	orders := map[string][]string{
		"backup long before":     {"b", "b", "b", "r", "r", "b", "r"},
		"restore long before":    {"r", "r", "b", "b", "b", "r", "b"},
		"in turn, backup first":  {"b", "r", "b", "r", "b", "r", "b", "r"},
		"in turn, restore first": {"r", "b", "r", "b", "r", "b", "r", "b"},
	}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
				claim(), volume(), volumeRestore(), repository())
			br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
			rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}
			for _, who := range order {
				if who == "b" {
					step(t, br)
				} else {
					restoreStep(t, rr)
				}
			}

			backup, restore := readBackupRun(t, c), readRestoreRun(t, c)
			backupWaits := readyReason(backup.Status.Conditions) == backupv1alpha1.ReasonSourceBusy
			restoreWaits := readyReason(restore.Status.Conditions) == backupv1alpha1.ReasonSourceBusy
			if backupWaits == restoreWaits {
				t.Fatalf("backup waits = %v, restore waits = %v; want exactly one to wait", backupWaits, restoreWaits)
			}
			triggered, restoring := ownSourceTag(t, c) == TriggerFor(runUID), len(movers(t, c)) == 1
			if triggered == restoring {
				t.Fatalf("trigger written = %v, mover created = %v; want exactly one mover object", triggered, restoring)
			}
			if restoring {
				if !backupWaits || !strings.Contains(readyMessage(backup.Status.Conditions), "RestoreRun back-to-monday") {
					t.Errorf("backup reason = %q, message = %q; want it to wait for the RestoreRun",
						readyReason(backup.Status.Conditions), readyMessage(backup.Status.Conditions))
				}
			} else if !restoreWaits || !strings.Contains(readyMessage(restore.Status.Conditions), "BackupRun before-upgrade") {
				t.Errorf("restore reason = %q, message = %q; want it to wait for the BackupRun",
					readyReason(restore.Status.Conditions), readyMessage(restore.Status.Conditions))
			}
			if strings.HasSuffix(name, "long before") && strings.HasPrefix(name, "backup") != triggered {
				t.Errorf("triggered = %v; the run that started long before must go first", triggered)
			}
		})
	}
}

// frozenNow returns the frozen clock's time.
func frozenNow() time.Time { return frozen }

// A BackupRun that has finished can leave its trigger open on the claim's
// ReplicationSource while VolSync retries the sync it started
// (status.lastSyncStartTime set): a mover pod may be writing the repository.
// A restore of the claim waits with reason SourceBusy, creates no
// restore Job, and its message says how the retrying sync ends
// and when the source may be deleted.
func TestARestoreWaitsWhileVolSyncRetriesTheSyncOfAFinishedBackup(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan

	ctx := context.Background()
	startOtherBackup(t, c)
	failed := &backupv1alpha1.BackupRun{}
	get(t, c, ns, "manual-notes", failed)
	failed.Status.Phase = backupv1alpha1.RunPhaseFailed
	failed.Status.Items[0].Phase = backupv1alpha1.ItemFailed
	if err := c.Status().Update(ctx, failed); err != nil {
		t.Fatal(err)
	}
	retrying := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, retrying)
	started := metav1.NewTime(frozen.Add(-time.Hour))
	retrying.Status.LastSyncStartTime = &started
	if err := c.Status().Update(ctx, retrying); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("restore Jobs = %v, want none while VolSync retries the sync", names)
	}
	msg := readyMessage(run.Status.Conditions)
	for _, want := range []string{"ReplicationSource " + claimN + " is still syncing", "delete the ReplicationSource " + claimN,
		"no pod of Job volsync-src-" + claimN + " is running"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message = %q, want it to contain %q", msg, want)
		}
	}
}
