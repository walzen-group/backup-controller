package restic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

const (
	sundayID = "49319ee865f4a00a86a35af5edf18730836cb27e770aba03ceca2de36a37be0f"
	mondayID = "88dc3648f06c94f727f601cb69653191e7e79602d36965a8e7892388d7875b81"
)

// quiescedAt is a moment inside a quiesce window, a few seconds before the
// time restic stamped on the monday snapshot.
var quiescedAt = time.Date(2026, 9, 21, 4, 59, 57, 0, time.UTC)

// writableFixture copies the fixture repository into a temporary directory the
// test may write to, and opens it.
func writableFixture(t *testing.T) (DirStore, *Repository) {
	t.Helper()
	dir := t.TempDir()
	err := filepath.WalkDir(string(fixture), func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(string(fixture), p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dir, rel), 0o700)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dir, rel), raw, 0o600)
	})
	if err != nil {
		t.Fatalf("copy the fixture: %v", err)
	}
	// A fresh clone has no locks/, because git keeps no empty directory.
	if err := os.RemoveAll(filepath.Join(dir, "locks")); err != nil {
		t.Fatal(err)
	}

	store := DirStore(dir)
	repo, err := Open(context.Background(), store, "backup")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	withoutLockWait(t)
	return store, repo
}

// withoutLockWait drops the pause restic makes between writing its lock and
// checking for others, so the tests do not sleep.
func withoutLockWait(t *testing.T) {
	t.Helper()
	saved := lockCheckDelay
	lockCheckDelay = 0
	t.Cleanup(func() { lockCheckDelay = saved })
}

// document decrypts one file of the repository into its JSON fields.
func document(t *testing.T, store DirStore, repo *Repository, name string) map[string]json.RawMessage {
	t.Helper()
	sealed, err := store.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	plain, err := repo.master.decrypt(sealed)
	if err != nil {
		t.Fatalf("decrypt %s: %v", name, err)
	}
	unpacked, err := unpack(plain)
	if err != nil {
		t.Fatalf("unpack %s: %v", name, err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(unpacked, &doc); err != nil {
		t.Fatalf("decode %s: %v", name, err)
	}
	return doc
}

// moverHost is the hostname a VolSync mover pod writes into its locks.
const moverHost = "volsync-src-notes-data-abcde"

// placeLock writes a lock file the way restic does, for the host and process
// given.
func placeLock(t *testing.T, store DirStore, repo *Repository, at time.Time, exclusive bool, host string, pid int) {
	t.Helper()
	plain, err := json.Marshal(map[string]any{"time": at, "exclusive": exclusive, "hostname": host, "pid": pid})
	if err != nil {
		t.Fatal(err)
	}
	sealed, err := repo.master.seal(plain)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(sealed)
	if err := store.Put(context.Background(), path.Join("locks", hex.EncodeToString(sum[:])), sealed); err != nil {
		t.Fatal(err)
	}
}

func lockFiles(t *testing.T, store DirStore) []string {
	t.Helper()
	names, err := store.List(context.Background(), "locks")
	if err != nil {
		t.Fatalf("list locks: %v", err)
	}
	return names
}

// A quiesced backup's snapshot is written again with the quiesce moment as its
// time and the quiesced tag, the way restic rewrite --forget --new-time does:
// a new snapshot file replaces the old one, which the new one names as its
// original.
func TestRetimeReplacesTheSnapshotWithOneAtTheNewTime(t *testing.T) {
	store, repo := writableFixture(t)

	got, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("retime: %v", err)
	}
	if got.ID == mondayID {
		t.Fatal("the snapshot kept its ID, so nothing was written")
	}
	if !got.Time.Equal(quiescedAt) || !slices.Contains(got.Tags, QuiescedTag) || got.Original != mondayID {
		t.Errorf("retimed = %+v, want time %s, tag %q and original %s", got, quiescedAt, QuiescedTag, mondayID)
	}

	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("snapshots: %v", err)
	}
	var ids []string
	for _, s := range snapshots {
		ids = append(ids, s.ID)
	}
	if !slices.Equal(ids, []string{sundayID, got.ID}) {
		t.Errorf("snapshots = %v, want sunday's untouched and monday's replaced by %s", ids, got.ID)
	}
	if left := lockFiles(t, store); len(left) != 0 {
		t.Errorf("locks left behind: %v", left)
	}
}

// The rewrite changes the time, the tags and the original and carries every
// other field over, so restic still finds the tree and groups the snapshot with
// the others for forget.
func TestRetimeKeepsTheSnapshotsOtherFields(t *testing.T) {
	store, repo := writableFixture(t)
	before := document(t, store, repo, path.Join("snapshots", mondayID))

	got, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("retime: %v", err)
	}
	after := document(t, store, repo, path.Join("snapshots", got.ID))

	for field, value := range before {
		switch field {
		case "time", "tags", "original":
			continue
		}
		if string(after[field]) != string(value) {
			t.Errorf("%s = %s after the rewrite, was %s", field, after[field], value)
		}
	}
}

// A controller that restarts after writing the new snapshot and before
// recording it asks again with the old ID, and gets the snapshot it wrote.
func TestRetimeAgainReturnsTheFirstRewrite(t *testing.T) {
	_, repo := writableFixture(t)

	first, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("first retime: %v", err)
	}
	second, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("second retime: %v", err)
	}
	if second.ID != first.ID {
		t.Errorf("second retime = %s, want the first rewrite %s", second.ID, first.ID)
	}
	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 {
		t.Errorf("got %d snapshots after two retimes, want 2", len(snapshots))
	}
}

// A lock another process holds, even a shared one a backup takes, keeps the
// exclusive lock the rewrite needs. The rewrite writes nothing and takes its
// own lock back out.
func TestRetimeBacksOffFromAnotherLock(t *testing.T) {
	store, repo := writableFixture(t)
	placeLock(t, store, repo, time.Now().Add(-time.Minute), false, moverHost, 12)

	_, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	var locked *LockedError
	if !errors.As(err, &locked) {
		t.Fatalf("err = %v, want a LockedError", err)
	}
	if locked.Hostname != moverHost {
		t.Errorf("locked by %q, want the other lock's host", locked.Hostname)
	}
	if left := lockFiles(t, store); len(left) != 1 {
		t.Errorf("locks = %v, want only the other process's", left)
	}
	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if snapshots[1].ID != mondayID {
		t.Error("the snapshot was rewritten while another process held a lock")
	}
}

// restic refreshes a live lock every five minutes and calls one older than
// thirty minutes stale, so a lock that old belongs to a process that is gone.
func TestRetimeIgnoresAStaleLock(t *testing.T) {
	store, repo := writableFixture(t)
	placeLock(t, store, repo, time.Now().Add(-31*time.Minute), true, moverHost, 12)

	if _, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag); err != nil {
		t.Fatalf("retime past a stale lock: %v", err)
	}
}

// restic's prune checks for other locks without skipping stale ones, so a lock
// the controller failed to remove would stop every prune until someone runs
// restic unlock. A lock carrying this process's host and PID is one it wrote
// itself, and the next retime removes it.
func TestRetimeRemovesALockThisProcessLeftBehind(t *testing.T) {
	store, repo := writableFixture(t)
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	placeLock(t, store, repo, time.Now().Add(-time.Minute), true, host, os.Getpid())

	if _, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag); err != nil {
		t.Fatalf("retime past this process's own lock: %v", err)
	}
	if left := lockFiles(t, store); len(left) != 0 {
		t.Errorf("locks left behind: %v", left)
	}
}

func TestRetimeOfAnUnknownSnapshotSaysSo(t *testing.T) {
	_, repo := writableFixture(t)
	if _, err := repo.Retime(context.Background(), "ffffffff", quiescedAt, QuiescedTag); err == nil {
		t.Fatal("retime of a snapshot the repository does not hold succeeded")
	}
}
