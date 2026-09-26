package restic

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
)

// The tests in this file read testdata/same-time, a repository that
// testdata/same-time/record.sh wrote with the restic of the VolSync mover
// image: two mover snapshots with identical times, and one snapshot for each
// way a snapshot can differ from what a mover writes.

// sameTime returns the same-time fixture recorded with the restic version
// versions.json pins for the mover, and fails the test when there is none.
func sameTime(t *testing.T) recordedFixture {
	t.Helper()
	name := "restic-" + versions.Of(t, "restic-mover")
	dir := filepath.Join("testdata", "same-time", name)
	return recordedFixture{name: "same-time/" + name, dir: dir}
}

// reversedStore is a Store whose List returns the names in the reverse of the
// order the wrapped store gives. A DirStore and S3 both list names sorted, so
// this is the one way a test can hand Snapshots two snapshots with the same
// time in the other order.
type reversedStore struct{ Store }

// List returns the wrapped store's names in reverse order.
func (s reversedStore) List(ctx context.Context, dir string) ([]string, error) {
	names, err := s.Store.List(ctx, dir)
	slices.Reverse(names)
	return names, err
}

// TestSnapshotsOrdersSameTimeSnapshotsByID checks that Snapshots returns the
// snapshots restic lists, oldest first and, among snapshots with the same
// time, by ID, whatever order the store lists the files in. restic stamps two
// backups given the same --time with identical times, and restic snapshots
// lists such snapshots in no set order, so the tie needs an order of its own
// for "the newest snapshot" to name one snapshot every time.
func TestSnapshotsOrdersSameTimeSnapshotsByID(t *testing.T) {
	f := sameTime(t)
	want := f.snapshots(t)
	slices.SortFunc(want, func(a, b recordedSnapshot) int {
		if c := a.Time.Compare(b.Time); c != 0 {
			return c
		}
		return strings.Compare(a.ID, b.ID)
	})
	var kinds map[string][]string
	f.readJSON(t, "kinds.json", &kinds)
	if m := kinds["mover"]; len(m) != 2 || !snapshotTime(want, m[0]).Equal(snapshotTime(want, m[1])) {
		t.Fatalf("the fixture's two mover snapshots %v should have identical times", m)
	}

	master := f.open(t).master
	for _, store := range []Store{DirStore(filepath.Join(f.dir, "repo")), reversedStore{DirStore(filepath.Join(f.dir, "repo"))}} {
		got, err := (&Repository{store: store, master: master}).Snapshots(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		ids := make([]string, 0, len(got))
		for _, s := range got {
			ids = append(ids, s.ID)
		}
		wantIDs := make([]string, 0, len(want))
		for _, s := range want {
			wantIDs = append(wantIDs, s.ID)
		}
		if !slices.Equal(ids, wantIDs) {
			t.Errorf("%T: Snapshots lists %v, want %v (by time, then ID)", store, ids, wantIDs)
		}
	}
}

// snapshotTime returns the time of the snapshot with the given ID in the
// list restic printed, and the zero time when the list has no such snapshot.
func snapshotTime(snapshots []recordedSnapshot, id string) time.Time {
	for _, s := range snapshots {
		if s.ID == id {
			return s.Time
		}
	}
	return time.Time{}
}

// TestMoverWrittenAcceptsOnlyWhatAMoverWrites checks MoverWritten against
// snapshots real restic wrote. Every snapshot VolSync's mover script wrote in
// the recorded timed, same-second and killed-mover repositories must pass, and
// in the same-time repository only the two written as the mover writes them
// may pass: the snapshot of another host, the one with a second path, the one
// of another directory and the retimed copy are passed over.
func TestMoverWrittenAcceptsOnlyWhatAMoverWrites(t *testing.T) {
	for _, kind := range []string{"timed", "same-second", "killed-mover"} {
		for _, f := range fixtures(t, kind) {
			for _, s := range f.snapshots(t) {
				if !MoverWritten(recordedAsSnapshot(s)) {
					t.Errorf("%s: MoverWritten rejects %s, which the mover wrote", f.name, s.ShortID)
				}
			}
		}
	}

	f := sameTime(t)
	var kinds map[string][]string
	f.readJSON(t, "kinds.json", &kinds)
	written := map[string]string{}
	for kind, ids := range kinds {
		for _, id := range ids {
			written[id] = kind
		}
	}
	snapshots := f.snapshots(t)
	if len(snapshots) != len(written) || len(kinds) != 5 {
		t.Fatalf("kinds.json names %d snapshots of %d kinds, restic lists %d", len(written), len(kinds), len(snapshots))
	}
	for _, s := range snapshots {
		kind := written[s.ID]
		if got, want := MoverWritten(recordedAsSnapshot(s)), kind == "mover"; got != want {
			t.Errorf("MoverWritten(%s, written as %q) = %v, want %v", s.ShortID, kind, got, want)
		}
	}
}

// recordedAsSnapshot converts an entry of restic snapshots --json into the
// Snapshot this package reads.
func recordedAsSnapshot(s recordedSnapshot) Snapshot {
	return Snapshot{ID: s.ID, Time: s.Time, Hostname: s.Hostname, Paths: s.Paths, Tags: s.Tags, Original: s.Original}
}
