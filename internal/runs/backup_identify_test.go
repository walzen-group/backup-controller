package runs

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file check how a BackupRun finds the snapshot its sync
// wrote (designs/restic-jobs.md J3): it lists the repository and takes the
// snapshots a mover wrote inside the window VolSync's status gives, and it
// never reads the mover's logs.

// The snapshots of the recorded same-time repository, which
// hack/fixtures/restic-same-time.sh wrote with the restic of VolSync's mover
// image. Two mover snapshots share the time 10:00:00; a01fbba8 sorts before
// b242fce0 by ID. retimedCopy is a copy of b242fce0 moved to 09:00:00, and
// the other three were written at 10:01, 10:02 and 10:03 by another host,
// with two paths, and of another path.
const (
	sameTimeMoverLow  = "a01fbba84d455f44892521805e721950612ea32e567588ca798bce074f52fef1"
	sameTimeMoverHigh = "b242fce0149229379eb25dd5f4e5fcbb895903627a02a13bc347cfb0dfe1bf91"
)

// sameTimeRepository returns the snapshots of the recorded same-time
// repository, read through the restic package as the controller reads a
// real repository. It fails the test when the repository can't be read.
func sameTimeRepository(t *testing.T) snapshots {
	t.Helper()
	dir := filepath.Join("..", "restic", "testdata", "same-time", "restic-"+versions.Of(t, "restic-mover"), "repo")
	repo, err := restic.Open(context.Background(), restic.DirStore(dir), "backup")
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	list, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	return list
}

// sameTimeAt returns the given clock time on the day the same-time
// repository was recorded.
func sameTimeAt(hour, minute, second int) time.Time {
	return time.Date(2026, 9, 26, hour, minute, second, 0, time.UTC)
}

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

// listTwice reconciles the run once, and once more relistAfter later, so a
// sync with no snapshot in its window gets the second listing that makes its
// claim empty.
func listTwice(t *testing.T, r *BackupRunReconciler) {
	t.Helper()
	step(t, r)
	r.Now = func() time.Time { return frozen.Add(relistAfter) }
	step(t, r)
}

// A sync whose mover logs VolSync did not keep still yields its snapshot:
// the run finds it in the repository by the window of the sync.
func TestABackupFindsItsSnapshotWithoutLogs(t *testing.T) {
	r, c := volumeRunOver(t, snapshots{sunday, monday})
	completeSync(t, c, monday.Time.Add(2*time.Second), 3*time.Second)
	step(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || item.SnapshotID != monday.ID || item.Snapshot != monday.ShortID() ||
		item.SnapshotTime == nil || !item.SnapshotTime.Time.Equal(monday.Time) || item.Empty {
		t.Fatalf("item = %+v, want Succeeded with snapshot %s at %s", item, monday.ID, monday.Time)
	}
}

// A sync whose first mover pod saved a snapshot and then failed, and whose
// retry saved another, leaves two snapshots in its window. The newest by time
// and then by ID is the one the sync completed with, and the item names the
// other. The recorded repository's two mover snapshots share their second,
// so the tie goes to the higher ID. The three snapshots in the window that
// another host wrote, or that hold other paths, are no candidates at all.
func TestTheNewestSnapshotInTheWindowWins(t *testing.T) {
	r, c := volumeRunOver(t, sameTimeRepository(t))
	completeSync(t, c, sameTimeAt(10, 3, 1), 3*time.Minute+5*time.Second)
	step(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || item.SnapshotID != sameTimeMoverHigh || item.Snapshot != sameTimeMoverHigh[:8] {
		t.Fatalf("item = %+v, want Succeeded with snapshot %s", item, sameTimeMoverHigh)
	}
	if !strings.Contains(item.Message, sameTimeMoverLow[:8]) {
		t.Errorf("item message %q does not name the other snapshot %s", item.Message, sameTimeMoverLow[:8])
	}
	for _, other := range []string{"35845334", "c83597cb", "6f062c42"} {
		if strings.Contains(item.Message, other) {
			t.Errorf("item message %q names %s, which no mover wrote", item.Message, other)
		}
	}
}

// A sync that completed with no new snapshot in the repository backed up an
// empty claim: VolSync's mover skips a volume that holds only lost+found and
// still reports Successful. Once a second listing, relistAfter later, also
// shows no snapshot, the item succeeds with Empty set.
func TestNoSnapshotInTheWindowIsAnEmptyClaim(t *testing.T) {
	r, c := volumeRunOver(t, snapshots{sunday, monday})
	completeSync(t, c, frozen.Add(time.Minute), 20*time.Second)
	listTwice(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || !item.Empty || item.Snapshot != "" || item.SnapshotID != "" {
		t.Fatalf("item = %+v, want Succeeded and Empty with no snapshot", item)
	}
	if item.Message != "the volume held no files, so VolSync took no snapshot" {
		t.Errorf("item message = %q", item.Message)
	}
}

// laggingListing is a SnapshotLister whose listing lags the repository by a
// given number of calls, as an S3 listing can after a write: the first lag
// calls return the older list, and every later call returns the newer one.
type laggingListing struct {
	older, newer snapshots
	lag, calls   int
}

// Snapshots returns the older list for the first lag calls and the newer one
// after that.
func (l *laggingListing) Snapshots(ctx context.Context, secret *corev1.Secret) ([]restic.Snapshot, error) {
	l.calls++
	if l.calls <= l.lag {
		return l.older.Snapshots(ctx, secret)
	}
	return l.newer.Snapshots(ctx, secret)
}

// A listing that does not show the mover's snapshot yet is no proof of an
// empty claim. The first pass that finds no snapshot keeps the item Running
// and records when it listed, and a pass at least pollInterval later lists
// again. That pass finds the snapshot, and the item records it.
func TestALaggingListingIsNoEmptyClaim(t *testing.T) {
	list := &laggingListing{older: snapshots{sunday}, newer: snapshots{sunday, monday}, lag: 1}
	r, c := volumeRunOver(t, list)
	completeSync(t, c, monday.Time.Add(2*time.Second), 3*time.Second)
	step(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning || item.Empty || item.NoSnapshotListedAt == nil {
		t.Fatalf("item = %+v after a listing without the snapshot, want it Running with the listing's time recorded", item)
	}

	r.Now = func() time.Time { return frozen.Add(pollInterval + time.Second) }
	step(t, r)

	item = readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || item.Empty || item.SnapshotID != monday.ID || item.NoSnapshotListedAt != nil {
		t.Fatalf("item = %+v, want Succeeded with snapshot %s", item, monday.ID)
	}
}

// Two listings that both show no snapshot of the sync, pollInterval apart,
// make the claim empty. A pass sooner than that lists again but decides
// nothing.
func TestASecondEmptyListingMakesTheClaimEmpty(t *testing.T) {
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

// The status keeps the first listing's time in whole seconds, so a first
// listing half a second past a whole second is stored half a second early.
// A pass pollInterval after that listing is then less than pollInterval
// after it in truth, and it decides nothing. Only a pass relistAfter after
// the stored time, which is at least pollInterval after the real listing,
// makes the claim empty.
func TestAListingTimeInWholeSecondsStillWaitsAPollInterval(t *testing.T) {
	r, c := volumeRunOver(t, snapshots{sunday, monday})
	completeSync(t, c, frozen.Add(time.Minute), 20*time.Second)
	firstListing := frozen.Add(500 * time.Millisecond)
	r.Now = func() time.Time { return firstListing }
	step(t, r)

	first := readBackupRun(t, c).Status.Items[0]
	if first.Phase != backupv1alpha1.ItemRunning || first.NoSnapshotListedAt == nil || !first.NoSnapshotListedAt.Time.Equal(frozen) {
		t.Fatalf("item = %+v after the first listing, want it Running with the listing's time stored as %s", first, frozen)
	}

	r.Now = func() time.Time { return firstListing.Add(pollInterval) }
	step(t, r)
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Empty {
		t.Fatalf("item = %+v a pass pollInterval after a listing at %s, want it Running: the stored time is half a second early",
			item, firstListing)
	}

	r.Now = func() time.Time { return frozen.Add(relistAfter) }
	step(t, r)
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemSucceeded || !item.Empty {
		t.Fatalf("item = %+v, want Succeeded and Empty relistAfter after the stored listing time", item)
	}
}

// Snapshots in the window that another host wrote, or that hold other
// paths than /data, are no mover's: a sync with only those in its window
// took no snapshot.
func TestASnapshotOfAnotherHostOrPathIsIgnored(t *testing.T) {
	r, c := volumeRunOver(t, sameTimeRepository(t))
	completeSync(t, c, sameTimeAt(10, 3, 1), 2*time.Minute+10*time.Second)
	listTwice(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || !item.Empty || item.SnapshotID != "" {
		t.Fatalf("item = %+v, want Succeeded and Empty: the window holds only other hosts' and paths' snapshots", item)
	}
}

// A retimed copy keeps the mover's host and path, but its original says a
// rewrite wrote it: it is never the snapshot a sync wrote.
func TestARetimedCopyIsNotTheMoversSnapshot(t *testing.T) {
	r, c := volumeRunOver(t, sameTimeRepository(t))
	completeSync(t, c, sameTimeAt(9, 0, 2), 3*time.Second)
	listTwice(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || !item.Empty || item.SnapshotID != "" {
		t.Fatalf("item = %+v, want Succeeded and Empty: the window holds only a retimed copy", item)
	}
}

// The window reaches back lastSyncDuration from lastSyncTime, so a sync
// that ran for a long time still finds the snapshot restic stamped when the
// backup began.
func TestIdentifyUsesLastSyncDuration(t *testing.T) {
	r, c := volumeRunOver(t, snapshots{sunday, monday})
	completeSync(t, c, monday.Time.Add(40*time.Minute), 40*time.Minute+time.Second)
	step(t, r)

	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemSucceeded || item.SnapshotID != monday.ID {
		t.Fatalf("item = %+v, want Succeeded with snapshot %s", item, monday.ID)
	}
}

// The window is [lastSyncTime - lastSyncDuration - 5s, lastSyncTime + 1s +
// 5s]: five seconds for clocks that differ between the mover's node and
// VolSync's, and one more at the end because lastSyncTime drops the
// fraction of its second. A snapshot on either edge is the sync's, and one
// just past an edge is not.
func TestIdentifyWindowAllowsSkew(t *testing.T) {
	ended := time.Date(2026, 9, 24, 12, 0, 10, 0, time.UTC)
	for name, tc := range map[string]struct {
		at    time.Time
		found bool
	}{
		"on the start edge":  {at: ended.Add(-35 * time.Second), found: true},
		"before the start":   {at: ended.Add(-35*time.Second - time.Millisecond), found: false},
		"on the end edge":    {at: ended.Add(6 * time.Second), found: true},
		"after the end":      {at: ended.Add(6*time.Second + time.Millisecond), found: false},
		"inside the seconds": {at: ended.Add(900 * time.Millisecond), found: true},
	} {
		t.Run(name, func(t *testing.T) {
			snapshot := moverSnapshot("5a5a5a5a", tc.at)
			r, c := volumeRunOver(t, snapshots{sunday, snapshot})
			completeSync(t, c, ended, 30*time.Second)
			listTwice(t, r)

			item := readBackupRun(t, c).Status.Items[0]
			if item.Phase != backupv1alpha1.ItemSucceeded {
				t.Fatalf("item = %+v, want Succeeded", item)
			}
			if got := item.SnapshotID == snapshot.ID; got != tc.found || item.Empty == tc.found {
				t.Errorf("item = %+v; snapshot at %s found = %v, want %v", item, tc.at.Format(time.RFC3339Nano), got, tc.found)
			}
		})
	}
}

// A snapshot another BackupRun of the claim already recorded is that run's,
// so it is no candidate even inside the window.
func TestASnapshotAnotherRunRecordedIsNotACandidate(t *testing.T) {
	other := otherRun()
	other.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	other.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	other.Status.Items[0].SnapshotID = sameTimeMoverHigh
	r, c := volumeRunOver(t, sameTimeRepository(t), other)
	completeSync(t, c, sameTimeAt(10, 0, 2), 3*time.Second)
	step(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || item.SnapshotID != sameTimeMoverLow {
		t.Fatalf("item = %+v, want Succeeded with snapshot %s", item, sameTimeMoverLow)
	}
}

// A completed sync whose status lacks lastSyncTime or lastSyncDuration
// gives no window, and a run without one can't tell a snapshot from an
// empty claim. The item fails with reason NoMoverSnapshot.
func TestASyncWithoutItsTimesFailsTheItem(t *testing.T) {
	for name, drop := range map[string]func(*volsyncv1alpha1.ReplicationSourceStatus){
		"lastSyncTime":     func(s *volsyncv1alpha1.ReplicationSourceStatus) { s.LastSyncTime = nil },
		"lastSyncDuration": func(s *volsyncv1alpha1.ReplicationSourceStatus) { s.LastSyncDuration = nil },
	} {
		t.Run(name, func(t *testing.T) {
			r, c := volumeRunOver(t, snapshots{sunday, monday})
			completeSync(t, c, monday.Time.Add(2*time.Second), 3*time.Second)
			source := &volsyncv1alpha1.ReplicationSource{}
			get(t, c, ns, claimN, source)
			drop(source.Status)
			if err := c.Status().Update(context.Background(), source); err != nil {
				t.Fatal(err)
			}
			step(t, r)

			item := readBackupRun(t, c).Status.Items[0]
			if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonNoMoverSnapshot {
				t.Fatalf("item = %+v, want Failed with reason NoMoverSnapshot", item)
			}
			if !strings.Contains(item.Message, name) {
				t.Errorf("item message %q does not name %s", item.Message, name)
			}
		})
	}
}

// A quiesced run records the snapshot it found before it retimes it, and
// then retimes it by the full ID it recorded, so a pass that crashed after
// the retime finds the item's snapshot by its original ID.
func TestAQuiescedRunRetimesByTheRecordedFullID(t *testing.T) {
	r, c := quiescedRunToUpload(t)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning || item.SnapshotID != monday.ID {
		t.Fatalf("item = %+v after the identify pass, want it Running with snapshot %s recorded", item, monday.ID)
	}
	if calls := r.Retimer.(*retimer).calls; len(calls) != 0 {
		t.Fatalf("retime calls = %+v in the pass that found the snapshot, want none before the status is written", calls)
	}

	// The repository no longer lists the mover's snapshot: a retime that
	// went through before a lost status write leaves only its copy.
	r.Snapshots = snapshots{sunday}
	step(t, r)

	run := readBackupRun(t, c)
	calls := r.Retimer.(*retimer).calls
	if len(calls) != 1 || calls[0].id != monday.ID {
		t.Fatalf("retime calls = %+v, want one by the full ID %s", calls, monday.ID)
	}
	item = run.Status.Items[0]
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || item.Snapshot != "c0ffee00" || item.SnapshotID != fullID("c0ffee00") || item.Empty {
		t.Errorf("phase = %q, item = %+v; want Succeeded with the rewritten snapshot", run.Status.Phase, item)
	}
}
