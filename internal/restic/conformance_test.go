package restic

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
)

// The tests in this file compare this package with restic itself, through the
// repositories and verdicts that hack/fixtures/restic.sh recorded
// under testdata/recorded with real restic 0.18.1 (the VolSync v0.16.0 mover's
// version) and 0.19.1, running VolSync's own mover script in a sandbox shaped
// like the mover container. make fixtures-restic regenerates them.

// recorded is the directory the recorder writes.
const recorded = "testdata/recorded"

// recordedSnapshot is one entry of restic snapshots --json, with the fields
// this package reads.
type recordedSnapshot struct {
	ID       string    `json:"id"`
	ShortID  string    `json:"short_id"`
	Time     time.Time `json:"time"`
	Hostname string    `json:"hostname"`
	Paths    []string  `json:"paths"`
	Tags     []string  `json:"tags"`
	Original string    `json:"original"`
}

// selectionRow is one run of entry.sh restore: the RESTORE_AS_OF and
// SELECT_PREVIOUS it had, and the short ID of the snapshot it selected, empty
// when it logged "No eligible snapshots found".
type selectionRow struct {
	RestoreAsOf    string `json:"restoreAsOf"`
	SelectPrevious int    `json:"selectPrevious"`
	Selected       string `json:"selected"`
}

// recordedFixture is one recorded repository and what restic said about it.
type recordedFixture struct {
	// name is the version and fixture, such as restic-0.18.1/timed.
	name string
	// dir holds repo/ and the JSON files next to it.
	dir string
}

// readJSON decodes one JSON file of a fixture into v and fails the test when
// it can't.
func (f recordedFixture) readJSON(t *testing.T, file string, v any) {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.dir, file))
	if err != nil {
		t.Fatalf("%s: %v", f.name, err)
	}
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("%s/%s: %v", f.name, file, err)
	}
}

// snapshots returns what restic snapshots --json listed for the fixture.
func (f recordedFixture) snapshots(t *testing.T) []recordedSnapshot {
	t.Helper()
	var s []recordedSnapshot
	f.readJSON(t, "snapshots.json", &s)
	return s
}

// open opens the fixture's repository where it lies, read only. The first
// call per fixture goes through Open; later ones reuse its master key.
func (f recordedFixture) open(t *testing.T) *Repository {
	t.Helper()
	return openCached(t, f.dir, DirStore(filepath.Join(f.dir, "repo")))
}

// writable copies the fixture's repository into a temporary directory the
// test may change, opens the copy with the fixture's master key, and turns
// off the lock protocol's pause.
func (f recordedFixture) writable(t *testing.T) (DirStore, *Repository) {
	t.Helper()
	dir := t.TempDir()
	if err := copyTree(filepath.Join(f.dir, "repo"), dir); err != nil {
		t.Fatalf("copy %s: %v", f.name, err)
	}
	store := DirStore(dir)
	withoutLockWait(t)
	return store, &Repository{store: store, master: f.open(t).master}
}

// copyTree copies the files under src into dst, creating directories as it
// goes.
func copyTree(src, dst string) error {
	return filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(dst, rel), 0o700)
		}
		raw, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(dst, rel), raw, 0o600)
	})
}

// fixtures returns the recorded fixtures of the given kind (timed,
// same-second or killed-mover) for every restic version recorded, and fails
// the test when there are none.
func fixtures(t *testing.T, kind string) []recordedFixture {
	t.Helper()
	dirs, err := filepath.Glob(filepath.Join(recorded, "restic-*", kind))
	if err != nil || len(dirs) == 0 {
		t.Fatalf("no recorded %s fixtures under %s: %v", kind, recorded, err)
	}
	var out []recordedFixture
	for _, dir := range dirs {
		out = append(out, recordedFixture{name: filepath.ToSlash(filepath.Join(filepath.Base(filepath.Dir(dir)), kind)), dir: dir})
	}
	return out
}

// TestTheRecordingsCameFromThePinnedTools checks that the recorded fixtures
// were written by the tool versions versions.json pins now, one directory per
// restic version. A pin bumped without make fixtures-restic fails here.
func TestTheRecordingsCameFromThePinnedTools(t *testing.T) {
	var provenance struct {
		Tools map[string]string `json:"tools"`
	}
	raw, err := os.ReadFile(filepath.Join(recorded, "provenance.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(raw, &provenance); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"restic-mover", "restic", "volsync", "bubblewrap"} {
		if got, want := provenance.Tools[name], versions.Of(t, name); got != want {
			t.Errorf("the fixtures were recorded with %s %s, versions.json pins %s; run make fixtures-restic", name, got, want)
		}
	}
	for _, name := range []string{"restic-mover", "restic"} {
		dir := filepath.Join(recorded, "restic-"+versions.Of(t, name))
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("no fixtures for %s at %s: %v", name, dir, err)
		}
	}
}

// TestSnapshotsReadsWhatResticLists checks that Snapshots reads every
// recorded repository the way restic snapshots --json lists it: the same
// snapshots, oldest first, with the same full IDs, the same times to the
// nanosecond, hosts, paths, tags and original. The subtests run in parallel, so
// the first Open of each fixture, which derives its key with scrypt, runs on
// its own core.
func TestSnapshotsReadsWhatResticLists(t *testing.T) {
	for _, kind := range []string{"timed", "same-second", "killed-mover"} {
		for _, f := range fixtures(t, kind) {
			t.Run(f.name, func(t *testing.T) {
				t.Parallel()
				want := f.snapshots(t)
				got, err := f.open(t).Snapshots(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				if len(got) != len(want) {
					t.Fatalf("got %d snapshots, restic lists %d", len(got), len(want))
				}
				for i, w := range want {
					g := got[i]
					if g.ID != w.ID || g.ShortID() != w.ShortID || !g.Time.Equal(w.Time) || g.Time.Nanosecond() != w.Time.Nanosecond() ||
						g.Hostname != w.Hostname || !slices.Equal(g.Paths, w.Paths) || !slices.Equal(g.Tags, w.Tags) || g.Original != w.Original {
						t.Errorf("snapshot %d = %+v, restic lists %+v", i, g, w)
					}
				}
			})
		}
	}
}

// TestAtOrBeforePicksWhatTheMoverPicks checks that AtOrBefore predicts the
// snapshot VolSync's mover restores. For every RESTORE_AS_OF the recorder ran
// entry.sh restore with (one second before, at, and after each snapshot's
// whole second) and SELECT_PREVIOUS 0, AtOrBefore must pick the snapshot the
// mover selected, or none when the mover found no eligible snapshot. With no
// RESTORE_AS_OF the mover restores the newest snapshot, which is the one a
// run with no moment selects.
func TestAtOrBeforePicksWhatTheMoverPicks(t *testing.T) {
	for _, kind := range []string{"timed", "same-second"} {
		for _, f := range fixtures(t, kind) {
			t.Run(f.name, func(t *testing.T) {
				snapshots, err := f.open(t).Snapshots(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var rows []selectionRow
				f.readJSON(t, "selection.json", &rows)
				for _, row := range rows {
					if row.SelectPrevious != 0 {
						continue
					}
					var got string
					if row.RestoreAsOf == "" {
						got = snapshots[len(snapshots)-1].ShortID()
					} else {
						at, err := time.Parse(time.RFC3339, row.RestoreAsOf)
						if err != nil {
							t.Fatal(err)
						}
						if s, ok := AtOrBefore(snapshots, at); ok {
							got = s.ShortID()
						}
					}
					if got != row.Selected {
						t.Errorf("RESTORE_AS_OF %q: AtOrBefore picks %q, the mover selected %q", row.RestoreAsOf, got, row.Selected)
					}
				}
			})
		}
	}
}

// TestRetimeWritesWhatResticRewriteWrites checks that Retime makes the same
// change to a snapshot that restic makes with rewrite --forget --new-time
// followed by tag --add: the recorder ran both on the middle snapshot of the
// timed repository, and the document Retime writes must hold the same fields
// with the same values as restic's.
func TestRetimeWritesWhatResticRewriteWrites(t *testing.T) {
	for _, f := range fixtures(t, "timed") {
		t.Run(f.name, func(t *testing.T) {
			var want struct {
				Source   string                     `json:"source"`
				NewTime  time.Time                  `json:"newTime"`
				Tag      string                     `json:"tag"`
				Document map[string]json.RawMessage `json:"document"`
			}
			f.readJSON(t, "rewrite.json", &want)

			store, repo := f.writable(t)
			got, err := repo.Retime(context.Background(), want.Source[:8], want.NewTime, want.Tag)
			if err != nil {
				t.Fatal(err)
			}
			doc := document(t, store, repo, path.Join("snapshots", got.ID))

			for field, value := range want.Document {
				if !sameJSON(t, doc[field], value) {
					t.Errorf("%s = %s, restic writes %s", field, doc[field], value)
				}
			}
			for field := range doc {
				if _, ok := want.Document[field]; !ok {
					t.Errorf("Retime writes %s, which restic's rewrite does not", field)
				}
			}
		})
	}
}

// sameJSON reports whether two JSON values decode to the same value.
func sameJSON(t *testing.T, a, b json.RawMessage) bool {
	t.Helper()
	var x, y any
	if len(a) == 0 || len(b) == 0 {
		return len(a) == len(b)
	}
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatal(err)
	}
	ja, _ := json.Marshal(x)
	jb, _ := json.Marshal(y)
	return string(ja) == string(jb)
}

// recordedLock is one entry of locks.json: a lock file's storage ID and the
// document restic cat lock printed for it.
type recordedLock struct {
	ID       string                     `json:"id"`
	Document map[string]json.RawMessage `json:"document"`
}

// killedMoverLock returns the one lock the killed mover left in a fixture,
// and fails the test when the recording holds another number of locks.
func killedMoverLock(t *testing.T, f recordedFixture) recordedLock {
	t.Helper()
	var locks []recordedLock
	f.readJSON(t, "locks.json", &locks)
	if len(locks) != 1 {
		t.Fatalf("%s: the killed mover left %d locks, want 1", f.name, len(locks))
	}
	return locks[0]
}

// TestAKilledMoverLeavesALockThatStopsEveryForget checks what restic itself
// did with the lock a mover killed during its backup left behind, while that
// lock was younger than 30 minutes: forget refused with exit code 11, the
// plain unlock VolSync runs removed nothing, and only unlock --remove-all
// removed it. A BackupRun that deletes a source VolSync is retrying leaves
// exactly this lock (finding B1). The recording is restic's behaviour, so
// this test fails when a regenerated fixture shows another restic behaving
// differently.
func TestAKilledMoverLeavesALockThatStopsEveryForget(t *testing.T) {
	for _, f := range fixtures(t, "killed-mover") {
		t.Run(f.name, func(t *testing.T) {
			lock := killedMoverLock(t, f)
			var host string
			if err := json.Unmarshal(lock.Document["hostname"], &host); err != nil || host != "volsync-src-notes-data-7xk2p" {
				t.Errorf("the lock names host %s, want the mover pod's name", lock.Document["hostname"])
			}
			var verdicts struct {
				Forget struct {
					ExitCode int `json:"exitCode"`
				} `json:"forget"`
				Unlock struct {
					LocksLeft int `json:"locksLeft"`
				} `json:"unlock"`
				UnlockRemoveAll struct {
					LocksLeft int `json:"locksLeft"`
				} `json:"unlockRemoveAll"`
			}
			f.readJSON(t, "verdicts.json", &verdicts)
			if verdicts.Forget.ExitCode != 11 {
				t.Errorf("forget exited %d, want 11 (repository locked)", verdicts.Forget.ExitCode)
			}
			if verdicts.Unlock.LocksLeft != 1 {
				t.Errorf("restic unlock left %d locks, want the mover's one", verdicts.Unlock.LocksLeft)
			}
			if verdicts.UnlockRemoveAll.LocksLeft != 0 {
				t.Errorf("restic unlock --remove-all left %d locks", verdicts.UnlockRemoveAll.LocksLeft)
			}
		})
	}
}

// TestTheLockReaderReadsAKilledMoversLockAsResticDoes checks that the lock
// file a killed mover left decodes, through this package's reader, to what
// restic cat lock printed for it: the host, the PID inside the mover's PID
// namespace, the kind and the time. The mover's uid has no passwd entry, so
// restic writes an empty username.
func TestTheLockReaderReadsAKilledMoversLockAsResticDoes(t *testing.T) {
	for _, f := range fixtures(t, "killed-mover") {
		t.Run(f.name, func(t *testing.T) {
			want := killedMoverLock(t, f)
			repo := f.open(t)
			raw, err := repo.load(context.Background(), path.Join("locks", want.ID))
			if err != nil {
				t.Fatal(err)
			}
			var got lockJSON
			if err := json.Unmarshal(raw, &got); err != nil {
				t.Fatal(err)
			}
			var w lockJSON
			wantRaw, _ := json.Marshal(want.Document)
			if err := json.Unmarshal(wantRaw, &w); err != nil {
				t.Fatal(err)
			}
			if got.Hostname != w.Hostname || got.PID != w.PID || got.Exclusive != w.Exclusive || !got.Time.Equal(w.Time) || got.Username != w.Username {
				t.Errorf("read %+v, restic printed %+v", got, w)
			}
			if _, ok := want.Document["username"]; !ok || got.Username != "" {
				t.Errorf("restic's lock carries username %s, want an empty one", want.Document["username"])
			}
		})
	}
}

// TestRetimeBacksOffFromAKilledMoversLockWhileItIsLive checks that Retime
// refuses to rewrite while the lock a killed mover left is younger than
// restic's 30 minutes, and names the mover's pod. The recorded lock is older
// than that by the time a test reads it, so the test writes restic's own
// lock document back with only its time moved to a minute ago.
func TestRetimeBacksOffFromAKilledMoversLockWhileItIsLive(t *testing.T) {
	for _, f := range fixtures(t, "killed-mover") {
		t.Run(f.name, func(t *testing.T) {
			lock := killedMoverLock(t, f)
			store, repo := f.writable(t)
			fields := map[string]json.RawMessage{}
			for k, v := range lock.Document {
				fields[k] = v
			}
			fields["time"], _ = json.Marshal(time.Now().Add(-time.Minute))
			plain, _ := json.Marshal(fields)
			if err := store.Remove(context.Background(), path.Join("locks", lock.ID)); err != nil {
				t.Fatal(err)
			}
			if _, err := repo.save(context.Background(), "locks", plain); err != nil {
				t.Fatal(err)
			}

			snapshots := f.snapshots(t)
			_, err := repo.Retime(context.Background(), snapshots[1].ShortID, quiescedAt, QuiescedTag)
			var locked *LockedError
			if !errors.As(err, &locked) {
				t.Fatalf("err = %v, want a LockedError", err)
			}
			if locked.Hostname != "volsync-src-notes-data-7xk2p" || locked.Exclusive {
				t.Errorf("locked by %+v, want the mover pod's shared lock", locked)
			}
		})
	}
}

// TestRetimeLeavesAKilledMoversStaleLockInPlace checks that Retime goes ahead
// past the lock a killed mover left once that lock is older than restic's 30
// minutes, as restic does, and leaves it where it is: the lock isn't the
// controller's, and VolSync's unlock removes it.
func TestRetimeLeavesAKilledMoversStaleLockInPlace(t *testing.T) {
	for _, f := range fixtures(t, "killed-mover") {
		t.Run(f.name, func(t *testing.T) {
			lock := killedMoverLock(t, f)
			var at time.Time
			if err := json.Unmarshal(lock.Document["time"], &at); err != nil {
				t.Fatal(err)
			}
			if time.Since(at) <= staleLockAge {
				t.Skip("the fixture was recorded less than 30 minutes ago, so its lock is still live")
			}
			store, repo := f.writable(t)
			snapshots := f.snapshots(t)
			if _, err := repo.Retime(context.Background(), snapshots[1].ShortID, quiescedAt, QuiescedTag); err != nil {
				t.Fatalf("retime past a stale mover lock: %v", err)
			}
			if left := lockFiles(t, store); !slices.Equal(left, []string{lock.ID}) {
				t.Errorf("locks = %v, want the mover's %s only", left, lock.ID)
			}
		})
	}
}
