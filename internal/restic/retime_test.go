package restic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path"
	"slices"
	"testing"
	"time"
)

// quiescedAt is a moment inside a quiesce window. Retime takes any moment,
// so the tests use one moment for every fixture.
var quiescedAt = time.Date(2026, 9, 21, 4, 59, 57, 0, time.UTC)

// writableFixture copies the recorded timed repository of the first restic
// version into a temporary directory that the test may write to, and opens it
// with the master key the conformance tests derive for it.
//
// It returns the copy's store, the opened repository, and the full IDs of the
// repository's snapshots as restic listed them. The tests retime the middle
// one, as the recorder did.
func writableFixture(t *testing.T) (DirStore, *Repository, []string) {
	t.Helper()
	f := fixtures(t, "timed")[0]
	store, repo := f.writable(t)
	var ids []string
	for _, s := range f.snapshots(t) {
		ids = append(ids, s.ID)
	}
	if len(ids) != 3 {
		t.Fatalf("%s holds %d snapshots, the tests need 3", f.name, len(ids))
	}
	return store, repo, ids
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
// given as well. These locks are sealed by this package's own writer; the
// conformance tests read the locks restic itself wrote. An empty username is
// written as "username": "", the way restic writes the lock of a VolSync mover
// whose uid has no passwd entry (testdata/recorded/*/killed-mover/locks.json).
func placeUserLock(t *testing.T, store DirStore, repo *Repository, at time.Time, exclusive bool, host string, pid int, user string) {
	t.Helper()
	fields := map[string]any{"time": at, "exclusive": exclusive, "hostname": host, "pid": pid, "username": user}
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
// other snapshots stay as they were, and no lock is left behind.
func TestRetimeReplacesTheSnapshotWithOneAtTheNewTime(t *testing.T) {
	store, repo, ids := writableFixture(t)

	got, err := repo.Retime(context.Background(), ids[1][:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("retime: %v", err)
	}
	if got.ID == ids[1] {
		t.Fatal("the snapshot kept its ID, so nothing was written")
	}
	if !got.Time.Equal(quiescedAt) || !slices.Contains(got.Tags, QuiescedTag) || got.Original != ids[1] {
		t.Errorf("retimed = %+v, want time %s, tag %q and original %s", got, quiescedAt, QuiescedTag, ids[1])
	}

	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("snapshots: %v", err)
	}
	var after []string
	for _, s := range snapshots {
		after = append(after, s.ID)
	}
	if want := []string{got.ID, ids[0], ids[2]}; !slices.Equal(after, want) {
		t.Errorf("snapshots = %v, want the others untouched and the middle one replaced by %s", after, got.ID)
	}
	if left := lockFiles(t, store); len(left) != 0 {
		t.Errorf("locks left behind: %v", left)
	}
}

// TestRetimeAgainReturnsTheFirstRewrite checks that a second Retime with the
// old full ID returns the snapshot the first one wrote, and writes nothing
// more. A BackupRun retimes by the full ID it recorded, and one whose
// controller restarts after the rewrite and before it records the new ID
// makes exactly that second call.
func TestRetimeAgainReturnsTheFirstRewrite(t *testing.T) {
	_, repo, ids := writableFixture(t)

	first, err := repo.Retime(context.Background(), ids[1], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("first retime: %v", err)
	}
	second, err := repo.Retime(context.Background(), ids[1], quiescedAt, QuiescedTag)
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
	if len(snapshots) != 3 {
		t.Errorf("got %d snapshots after two retimes, want 3", len(snapshots))
	}
}

// TestRetimeRemovesALockThisProcessLeftBehind checks that Retime removes a lock
// that carries this process's host and PID, which this process wrote and
// failed to remove. restic's prune checks for other locks without skipping
// stale ones, so a lock like that would stop every prune until someone ran
// restic unlock.
func TestRetimeRemovesALockThisProcessLeftBehind(t *testing.T) {
	store, repo, ids := writableFixture(t)
	host, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	placeLock(t, store, repo, time.Now().Add(-time.Minute), true, host, os.Getpid())

	if _, err := repo.Retime(context.Background(), ids[1][:8], quiescedAt, QuiescedTag); err != nil {
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
	store, repo, ids := writableFixture(t)
	placeUserLock(t, store, repo, time.Now().Add(-31*time.Minute), true, "backup-controller-7c9f-old", 1, lockUser)
	placeLock(t, store, repo, time.Now().Add(-31*time.Minute), false, moverHost, 12)

	if _, err := repo.Retime(context.Background(), ids[1][:8], quiescedAt, QuiescedTag); err != nil {
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
	dir, opened, ids := writableFixture(t)
	store := &failFirstRemove{DirStore: dir, name: path.Join("snapshots", ids[1])}
	repo := &Repository{store: store, master: opened.master}

	if _, err := repo.Retime(context.Background(), ids[1][:8], quiescedAt, QuiescedTag); err == nil {
		t.Fatal("first retime succeeded, want the refused delete")
	}
	got, err := repo.Retime(context.Background(), ids[1][:8], quiescedAt, QuiescedTag)
	if err != nil {
		t.Fatalf("second retime: %v", err)
	}

	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var after []string
	for _, s := range snapshots {
		after = append(after, s.ID)
	}
	if len(snapshots) != 3 {
		t.Fatalf("snapshots after the retry = %v, want the two others and one copy of the middle one", after)
	}
	if slices.Contains(after, ids[1]) {
		t.Errorf("the old snapshot %s is still there", ids[1])
	}
	if !slices.Contains(after, got.ID) {
		t.Errorf("the retry returned %s, which the repository doesn't hold: %v", got.ID, after)
	}
}

// TestRetimeStampsWholeSeconds checks that Retime drops the fraction of a
// second from the time it writes, and that a retry after a failed delete
// finds the copy when the caller passes the time without its fraction. A
// BackupRun reports whole seconds, and VolSync compares whole seconds, so a
// copy stamped T.123456789 would miss the check that reuses it, and a second
// copy would land in the repository.
func TestRetimeStampsWholeSeconds(t *testing.T) {
	dir, opened, ids := writableFixture(t)
	store := &failFirstRemove{DirStore: dir, name: path.Join("snapshots", ids[1])}
	repo := &Repository{store: store, master: opened.master}

	if _, err := repo.Retime(context.Background(), ids[1][:8], quiescedAt.Add(123456789), QuiescedTag); err == nil {
		t.Fatal("first retime succeeded, want the refused delete")
	}
	got, err := repo.Retime(context.Background(), ids[1][:8], quiescedAt, QuiescedTag)
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
	if len(snapshots) != 3 {
		t.Errorf("got %d snapshots after the retry, want the two others and one copy of the middle one", len(snapshots))
	}
}
