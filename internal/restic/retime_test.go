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

// sundayID and mondayID are the full IDs of the fixture's two snapshots, taken
// on Sunday 20 and Monday 21 September 2026.
const (
	sundayID = "49319ee865f4a00a86a35af5edf18730836cb27e770aba03ceca2de36a37be0f"
	mondayID = "88dc3648f06c94f727f601cb69653191e7e79602d36965a8e7892388d7875b81"
)

// quiescedAt is a moment inside a quiesce window, three seconds before the time
// restic stamped on the Monday snapshot.
var quiescedAt = time.Date(2026, 9, 21, 4, 59, 57, 0, time.UTC)

// writableFixture copies the fixture repository into a temporary directory that
// the test may write to, opens it, and turns off the lock protocol's pause for
// the test.
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
	// Removing it here makes every run start the way a fresh clone does.
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

// withoutLockWait sets lockCheckDelay and unlockRetryDelay to zero for the
// length of the test, so the tests don't sleep through restic's pause between
// writing a lock and checking for others, or through the pause between two
// tries to remove a lock.
func withoutLockWait(t *testing.T) {
	t.Helper()
	savedCheck, savedRetry := lockCheckDelay, unlockRetryDelay
	lockCheckDelay, unlockRetryDelay = 0, 0
	t.Cleanup(func() { lockCheckDelay, unlockRetryDelay = savedCheck, savedRetry })
}

// document decrypts one file of the repository and returns its JSON fields.
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

// placeLock writes a lock file the way restic does, with the time, kind, host
// and PID given.
func placeLock(t *testing.T, store DirStore, repo *Repository, at time.Time, exclusive bool, host string, pid int) {
	t.Helper()
	placeUserLock(t, store, repo, at, exclusive, host, pid, "")
}

// placeUserLock writes a lock file like placeLock does, with the username
// given as well. An empty username leaves the field out, as a VolSync mover
// running as no named user does.
func placeUserLock(t *testing.T, store DirStore, repo *Repository, at time.Time, exclusive bool, host string, pid int, user string) {
	t.Helper()
	fields := map[string]any{"time": at, "exclusive": exclusive, "hostname": host, "pid": pid}
	if user != "" {
		fields["username"] = user
	}
	plain, err := json.Marshal(fields)
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

// lockFiles returns the names of the lock files in the repository.
func lockFiles(t *testing.T, store DirStore) []string {
	t.Helper()
	names, err := store.List(context.Background(), "locks")
	if err != nil {
		t.Fatalf("list locks: %v", err)
	}
	return names
}

// TestRetimeReplacesTheSnapshotWithOneAtTheNewTime checks that Retime replaces
// a quiesced backup's snapshot with a new snapshot file. The new file carries
// the quiesce moment as its time, the quiesced tag, and the old ID in its
// original field, as restic rewrite --forget --new-time would write it. The
// other snapshot stays as it was, and no lock is left behind.
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

// TestRetimeKeepsTheSnapshotsOtherFields checks that the rewrite changes only
// the time, the tags and the original field, and copies every other field
// unchanged. Those fields let restic find the snapshot's tree and group the
// snapshot with the others for forget.
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

// TestRetimeAgainReturnsTheFirstRewrite checks that a second Retime with the
// old ID returns the snapshot the first one wrote, and writes nothing more. A
// controller that restarts after the rewrite and before it records the new ID
// makes exactly that second call.
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

// TestRetimeBacksOffFromAnotherLock checks that Retime returns a *LockedError
// and writes nothing while another process holds a lock, even a shared one
// such as a backup takes. The rewrite needs the exclusive lock, which no other
// lock may share. Retime also removes its own lock before it returns.
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

// TestRetimeIgnoresAStaleLock checks that Retime goes ahead past a lock older
// than thirty minutes. restic refreshes a live lock every five minutes and
// calls a lock older than thirty minutes stale, so a lock that old belongs to
// a process that is gone.
func TestRetimeIgnoresAStaleLock(t *testing.T) {
	store, repo := writableFixture(t)
	placeLock(t, store, repo, time.Now().Add(-31*time.Minute), true, moverHost, 12)

	if _, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag); err != nil {
		t.Fatalf("retime past a stale lock: %v", err)
	}
}

// TestRetimeRemovesALockThisProcessLeftBehind checks that Retime removes a lock
// that carries this process's host and PID, which this process wrote and
// failed to remove. restic's prune checks for other locks without skipping
// stale ones, so a lock like that would stop every prune until someone ran
// restic unlock.
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

// TestRetimeRemovesAStaleLockAnEarlierControllerLeft checks that Retime
// removes a stale lock that carries the controller's username, whatever host
// and PID it names. A controller pod that was replaced between writing its
// lock and removing it leaves such a lock, and no later pod shares its host.
// restic's prune never skips a stale lock, and VolSync runs restic unlock only
// when spec.restic.unlock changes, so the lock would stop every prune. A stale
// lock a mover left stays, because it isn't the controller's to remove.
func TestRetimeRemovesAStaleLockAnEarlierControllerLeft(t *testing.T) {
	store, repo := writableFixture(t)
	placeUserLock(t, store, repo, time.Now().Add(-31*time.Minute), true, "backup-controller-7c9f-old", 1, lockUser)
	placeLock(t, store, repo, time.Now().Add(-31*time.Minute), false, moverHost, 12)

	if _, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag); err != nil {
		t.Fatalf("retime: %v", err)
	}
	left := lockFiles(t, store)
	if len(left) != 1 {
		t.Fatalf("locks = %v, want only the mover's stale lock", left)
	}
	if host := string(document(t, store, repo, path.Join("locks", left[0]))["hostname"]); host != `"`+moverHost+`"` {
		t.Errorf("the lock left is %s's, want the mover's", host)
	}
}

// TestRetimeKeepsALiveLockFromAnotherControllerPod checks that a lock with the
// controller's username but another host blocks Retime while it is younger
// than restic's stale age. The pod that wrote it may still be running.
func TestRetimeKeepsALiveLockFromAnotherControllerPod(t *testing.T) {
	store, repo := writableFixture(t)
	placeUserLock(t, store, repo, time.Now().Add(-time.Minute), true, "backup-controller-7c9f-old", 1, lockUser)

	_, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	var locked *LockedError
	if !errors.As(err, &locked) {
		t.Fatalf("err = %v, want a LockedError", err)
	}
	if left := lockFiles(t, store); len(left) != 1 {
		t.Errorf("locks = %v, want the other pod's lock only", left)
	}
}

// failLockRemove is a Store that refuses the first Remove under locks/, the way
// an S3 DELETE can fail once.
type failLockRemove struct {
	DirStore
	failed bool
}

func (s *failLockRemove) Remove(ctx context.Context, name string) error {
	if path.Dir(name) == "locks" && !s.failed {
		s.failed = true
		return errors.New("delete refused")
	}
	return s.DirStore.Remove(ctx, name)
}

// TestRetimeRetriesTheRemovalOfItsLock checks that Retime tries again when the
// removal of its own lock fails once, so one failed DELETE leaves no lock
// behind to block the movers.
func TestRetimeRetriesTheRemovalOfItsLock(t *testing.T) {
	dir, _ := writableFixture(t)
	store := &failLockRemove{DirStore: dir}
	repo, err := Open(context.Background(), store, "backup")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag); err != nil {
		t.Fatalf("retime: %v", err)
	}
	if left := lockFiles(t, dir); len(left) != 0 {
		t.Errorf("locks left behind: %v", left)
	}
}

// TestRetimeOfAnUnknownSnapshotSaysSo checks that Retime returns an error for
// an ID the repository doesn't hold.
func TestRetimeOfAnUnknownSnapshotSaysSo(t *testing.T) {
	_, repo := writableFixture(t)
	if _, err := repo.Retime(context.Background(), "ffffffff", quiescedAt, QuiescedTag); err == nil {
		t.Fatal("retime of a snapshot the repository does not hold succeeded")
	}
}

// failFirstRemove is a Store that refuses the first Remove of one file, the
// way an S3 DELETE can fail after the PUT before it went through.
type failFirstRemove struct {
	DirStore
	name   string
	failed bool
}

func (s *failFirstRemove) Remove(ctx context.Context, name string) error {
	if name == s.name && !s.failed {
		s.failed = true
		return errors.New("delete refused")
	}
	return s.DirStore.Remove(ctx, name)
}

// TestRetimeAfterAFailedRemoveWritesNoSecondCopy checks that when Retime wrote
// the copy but couldn't delete the old snapshot file, the next Retime deletes
// the old file and returns the copy it already wrote. Encryption uses a random
// IV, so writing the copy again would add a second snapshot with another ID.
func TestRetimeAfterAFailedRemoveWritesNoSecondCopy(t *testing.T) {
	dir, _ := writableFixture(t)
	store := &failFirstRemove{DirStore: dir, name: path.Join("snapshots", mondayID)}
	repo, err := Open(context.Background(), store, "backup")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag); err == nil {
		t.Fatal("first retime succeeded, want the refused delete")
	}
	got, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("second retime: %v", err)
	}

	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, s := range snapshots {
		ids = append(ids, s.ID)
	}
	if len(snapshots) != 2 {
		t.Fatalf("snapshots after the retry = %v, want Sunday's and one copy of Monday's", ids)
	}
	if slices.Contains(ids, mondayID) {
		t.Errorf("the old Monday snapshot %s is still there", mondayID)
	}
	if !slices.Contains(ids, got.ID) {
		t.Errorf("the retry returned %s, which the repository doesn't hold: %v", got.ID, ids)
	}
}

// TestRetimeStampsWholeSeconds checks that Retime drops the fraction of a
// second from the time it writes, and that a retry after a failed delete
// finds the copy when the caller passes the time without its fraction. A
// BackupRun reports whole seconds, and VolSync compares whole seconds, so a
// copy stamped T.123456789 would miss the check that reuses it, and a second
// copy would land in the repository.
func TestRetimeStampsWholeSeconds(t *testing.T) {
	dir, _ := writableFixture(t)
	store := &failFirstRemove{DirStore: dir, name: path.Join("snapshots", mondayID)}
	repo, err := Open(context.Background(), store, "backup")
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if _, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt.Add(123456789), QuiescedTag); err == nil {
		t.Fatal("first retime succeeded, want the refused delete")
	}
	got, err := repo.Retime(context.Background(), mondayID[:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("second retime: %v", err)
	}
	if !got.Time.Equal(quiescedAt) {
		t.Errorf("retimed to %s, want %s", got.Time, quiescedAt)
	}
	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshots) != 2 {
		t.Errorf("got %d snapshots after the retry, want Sunday's and one copy of Monday's", len(snapshots))
	}
}
