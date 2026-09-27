package restic

import (
	"context"
	"errors"
	"testing"
	"time"
)

// quiescedAtTimed is a moment between the oldest snapshot of the recorded
// timed fixture, which a VolSync mover wrote at 21:20:55, and the newer ones.
var quiescedAtTimed = time.Date(2026, 9, 25, 21, 20, 56, 0, time.UTC)

// quiescedRepository copies the recorded timed repository of the mover's
// restic and, when at is not zero, rewrites its oldest snapshot to at with
// the tag quiesced, as a quiesced BackupRun does. The newer snapshots stay
// live. It returns every snapshot of the copy.
func quiescedRepository(t *testing.T, at time.Time) []Snapshot {
	t.Helper()
	f := fixtures(t, "timed")[0]
	_, repo := f.writable(t)
	if !at.IsZero() {
		if _, err := repo.Retime(context.Background(), f.snapshots(t)[0].ID, at, QuiescedTag); err != nil {
			t.Fatalf("retime: %v", err)
		}
	}
	all, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("snapshots: %v", err)
	}
	return all
}

// TestSyncedMomentIsTheTimeOfTheQuiescedSnapshots checks that the moment of
// an automatic restore is the time of the newest quiesced snapshots, and
// that a repository with no quiesced snapshot does not count. The live
// snapshots are newer than the moment, so a pick of the newest snapshot
// would give another time.
func TestSyncedMomentIsTheTimeOfTheQuiescedSnapshots(t *testing.T) {
	repositories := map[string][]Snapshot{
		"data":  quiescedRepository(t, quiescedAtTimed),
		"media": quiescedRepository(t, quiescedAtTimed),
		"live":  quiescedRepository(t, time.Time{}),
	}
	moment, ok, err := SyncedMoment(repositories, nil)
	if err != nil || !ok || !moment.Equal(quiescedAtTimed) {
		t.Fatalf("SyncedMoment = %s, %t, %v; want %s", moment, ok, err, quiescedAtTimed)
	}
}

// TestSyncedMomentWithoutQuiescedSnapshotsIsNone checks that a namespace
// with live backups only, or with a pin before every quiesced snapshot,
// gets no moment, so its restore works as before.
func TestSyncedMomentWithoutQuiescedSnapshotsIsNone(t *testing.T) {
	live := map[string][]Snapshot{"data": quiescedRepository(t, time.Time{})}
	if _, ok, err := SyncedMoment(live, nil); ok || err != nil {
		t.Errorf("live backups only: ok %t, err %v; want no moment", ok, err)
	}
	before := quiescedAtTimed.Add(-time.Hour)
	pinned := map[string][]Snapshot{"data": quiescedRepository(t, quiescedAtTimed)}
	if _, ok, err := SyncedMoment(pinned, &before); ok || err != nil {
		t.Errorf("pin before the quiesced snapshot: ok %t, err %v; want no moment", ok, err)
	}
}

// TestSyncedMomentRefusesTwoTimes checks that two repositories whose newest
// quiesced snapshots have different times give a *NotSyncedError, and that
// a pin both reach gives the older moment.
func TestSyncedMomentRefusesTwoTimes(t *testing.T) {
	later := quiescedAtTimed.Add(time.Minute)
	repositories := map[string][]Snapshot{
		"data":  quiescedRepository(t, quiescedAtTimed),
		"media": quiescedRepository(t, later),
	}
	var notSynced *NotSyncedError
	if _, _, err := SyncedMoment(repositories, nil); !errors.As(err, &notSynced) {
		t.Fatalf("err = %v, want a *NotSyncedError", err)
	}
	if notSynced.FirstRepository != "data" || notSynced.OtherRepository != "media" {
		t.Errorf("error names %s and %s, want data and media", notSynced.FirstRepository, notSynced.OtherRepository)
	}
	pin := quiescedAtTimed.Add(30 * time.Second)
	moment, ok, err := SyncedMoment(repositories, &pin)
	if err != nil || !ok || !moment.Equal(quiescedAtTimed) {
		t.Errorf("pinned SyncedMoment = %s, %t, %v; want %s", moment, ok, err, quiescedAtTimed)
	}
}
