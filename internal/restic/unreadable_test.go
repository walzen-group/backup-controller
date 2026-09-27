package restic

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// TestSnapshotsSkipsAnUnreadableSnapshotFile checks that Snapshots leaves out
// a snapshot file that does not decrypt and lists the others, as restic
// snapshots does: FindFilteredSnapshots warns "Ignoring" and goes on when a
// snapshot fails to load (restic v0.18.1 cmd/restic/find.go:51-55,
// cmd/restic/cmd_snapshots.go:77). The fixture is the recorded timed
// repository, with its oldest snapshot file truncated.
func TestSnapshotsSkipsAnUnreadableSnapshotFile(t *testing.T) {
	f := fixtures(t, "timed")[0]
	store, repo := f.writable(t)
	want := f.snapshots(t)
	if len(want) < 2 {
		t.Fatalf("the fixture lists %d snapshots, the test needs 2", len(want))
	}
	corrupt := want[0].ID
	if err := os.WriteFile(filepath.Join(string(store), "snapshots", corrupt), []byte("truncated"), 0o600); err != nil {
		t.Fatal(err)
	}

	got, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("one unreadable snapshot file failed the listing: %v", err)
	}
	if len(got) != len(want)-1 {
		t.Fatalf("got %d snapshots, want the %d readable ones", len(got), len(want)-1)
	}
	for _, s := range got {
		if s.ID == corrupt {
			t.Fatalf("the unreadable snapshot %s is listed", corrupt)
		}
	}
}

// missingSnapshotStore is a Store whose listing of snapshots/ names one more
// file than the store holds, as a listing does when a restic forget deletes
// the file between the list and the read.
type missingSnapshotStore struct{ Store }

// List adds a snapshot name that the store does not hold.
func (s missingSnapshotStore) List(ctx context.Context, dir string) ([]string, error) {
	names, err := s.Store.List(ctx, dir)
	if dir == "snapshots" {
		names = append(names, "0000000000000000000000000000000000000000000000000000000000000000")
	}
	return names, err
}

// TestSnapshotsFailsOnAFileGoneAfterTheListing checks that a snapshot file
// that the listing names and the read does not find fails the listing. A
// concurrent forget causes this, and the caller retries on the error.
func TestSnapshotsFailsOnAFileGoneAfterTheListing(t *testing.T) {
	f := fixtures(t, "timed")[0]
	repo := &Repository{store: missingSnapshotStore{DirStore(filepath.Join(f.dir, "repo"))}, master: f.open(t).master}
	if _, err := repo.Snapshots(context.Background()); err == nil {
		t.Fatal("a snapshot file gone after the listing gave no error")
	}
}
