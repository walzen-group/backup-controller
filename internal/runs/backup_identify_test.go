package runs

import (
	"context"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file check how a BackupRun finds the snapshot its sync
// wrote (designs/restic-jobs.md J3): it lists the repository and takes the
// snapshots a mover wrote inside the window VolSync's status gives, and it
// never reads the mover's logs.

// completeSync stands in for VolSync completing the run's sync, with the
// status VolSync 0.16.0 writes at that moment (statemachine/machine.go
// 177-219): lastManualSync set to the trigger, lastSyncTime in whole
// seconds, lastSyncDuration, lastSyncStartTime cleared, and a Successful
// mover with no logs, as a manager with MOVER_LOG_MAX_BYTES=0 leaves them.
//
// Parameters:
//   - c is the test's client, which holds the claim's ReplicationSource.
//   - ended is when the sync completed; the status keeps it in whole
//     seconds, as the API server stores it.
//   - took is the sync's duration.
func completeSync(t *testing.T, c client.Client, ended time.Time, took time.Duration) {
	t.Helper()
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	at := metav1.NewTime(ended.Truncate(time.Second))
	source.Status = &volsyncv1alpha1.ReplicationSourceStatus{
		LastManualSync:    manualTag(source),
		LastSyncTime:      &at,
		LastSyncDuration:  &metav1.Duration{Duration: took},
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultSuccessful},
	}
	if err := c.Status().Update(context.Background(), source); err != nil {
		t.Fatalf("complete the source: %v", err)
	}
}

// volumeRunOver returns a BackupRun of the test claim, driven until its
// source carries the run's trigger, whose reconciler lists the given
// snapshots. objects are added to the client beside the run's own.
func volumeRunOver(t *testing.T, list SnapshotLister, objects ...client.Object) (*BackupRunReconciler, client.Client) {
	t.Helper()
	objects = append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository()}, objects...)
	r, c := backupReconciler(t, objects...)
	r.Snapshots = list
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // start
	return r, c
}

// A sync whose mover logs VolSync did not keep still yields its snapshot:
// the run finds it in the repository by the window of the sync.
func TestABackupFindsItsSnapshotWithoutLogs(t *testing.T) {
	t.Parallel()
	r, c := volumeRunOver(t, snapshots{sunday, monday})
	completeSync(t, c, monday.Time.Add(2*time.Second), 3*time.Second)
	step(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || item.SnapshotID != monday.ID || item.Snapshot != monday.ShortID() ||
		item.SnapshotTime == nil || !item.SnapshotTime.Time.Equal(monday.Time) || item.Empty {
		t.Fatalf("item = %+v, want Succeeded with snapshot %s at %s", item, monday.ID, monday.Time)
	}
}

// Two listings that both show no snapshot of the sync, pollInterval apart,
// make the claim empty. A pass sooner than that lists again but decides
// nothing.
func TestASecondEmptyListingMakesTheClaimEmpty(t *testing.T) {
	t.Parallel()
	r, c := volumeRunOver(t, snapshots{sunday, monday})
	completeSync(t, c, frozen.Add(time.Minute), 20*time.Second)
	step(t, r)

	first := readBackupRun(t, c).Status.Items[0]
	if first.Phase != backupv1alpha1.ItemRunning || first.Empty || first.NoSnapshotListedAt == nil {
		t.Fatalf("item = %+v after the first listing, want it Running with the listing's time recorded", first)
	}

	r.Now = func() time.Time { return frozen.Add(pollInterval / 2) }
	step(t, r)
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Empty ||
		!item.NoSnapshotListedAt.Equal(first.NoSnapshotListedAt) {
		t.Fatalf("item = %+v a pass before pollInterval passed, want it Running with the first listing's time", item)
	}

	r.Now = func() time.Time { return frozen.Add(pollInterval + time.Second) }
	step(t, r)
	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || !item.Empty || item.SnapshotID != "" {
		t.Fatalf("item = %+v, want Succeeded and Empty after the second empty listing", item)
	}
}
