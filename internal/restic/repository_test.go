package restic

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"
)

// fixture is testdata/repo, a version 2 repository that restic 0.19.1 wrote
// with the password "backup". It holds two snapshots of one file, made with
// these commands:
//
//	restic init --repository-version 2
//	restic backup --host volsync --time "2026-09-20 05:00:00" data
//	restic backup --host volsync --time "2026-09-21 05:00:00" data
//
// restic snapshots listed them as 49319ee8 and 88dc3648.
const fixture = DirStore("testdata/repo")

// masterKeys caches the master key of each test repository, keyed by the
// directory the repository's key files come from. Open derives the key from
// the password with restic's scrypt parameters, which takes seconds under the
// race detector, and every test of one repository needs the same key.
var masterKeys sync.Map

// openCached opens the repository in a store with the password "backup".
//
// Parameters:
//   - source names the repository the store's key files come from, such as
//     the fixture directory that a temporary copy was made from. Stores with
//     the same source share one master key.
//   - store is the store the returned Repository reads and writes. It may be
//     a copy of source, or a wrapper around such a copy.
//
// It returns the opened repository, and fails the test when Open fails.
//
// The first call for a source goes through Open, so Open's key path runs once
// per test repository. Later calls pair the cached master key with the store.
func openCached(t *testing.T, source string, store Store) *Repository {
	t.Helper()
	if master, ok := masterKeys.Load(source); ok {
		return &Repository{store: store, master: master.(key)}
	}
	repo, err := Open(context.Background(), store, "backup")
	if err != nil {
		t.Fatalf("open %s: %v", source, err)
	}
	masterKeys.Store(source, repo.master)
	return repo
}

// TestAWrongPasswordOpensNothing checks that Open fails when no key file opens
// with the password.
func TestAWrongPasswordOpensNothing(t *testing.T) {
	_, err := Open(context.Background(), fixture, "not-the-password")
	if err == nil {
		t.Fatal("a wrong password opened the repository")
	}
}

// TestALocationWithNoKeysIsNoRepository checks that Open returns
// ErrNoRepository for a location whose keys/ directory is empty. S3 lists a
// prefix that doesn't exist as empty, and that's how the location of a claim
// that has never been backed up looks. The empty keys/ stands in for it here.
func TestALocationWithNoKeysIsNoRepository(t *testing.T) {
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "keys"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(context.Background(), DirStore(dir), "backup"); !errors.Is(err, ErrNoRepository) {
		t.Fatalf("err = %v, want ErrNoRepository", err)
	}
}

// TestAtOrBeforeRoundsBackToTheSnapshotBefore checks that AtOrBefore picks the
// newest snapshot at or before the time, and nothing when every snapshot is
// later. A VolSync restore with restoreAsOf picks the same snapshot, or
// restores nothing, and AtOrBefore answers that question before the restore
// starts.
func TestAtOrBeforeRoundsBackToTheSnapshotBefore(t *testing.T) {
	snapshots := []Snapshot{
		{ID: "a", Time: time.Date(2026, 9, 20, 5, 0, 0, 0, time.UTC)},
		{ID: "b", Time: time.Date(2026, 9, 21, 5, 0, 0, 0, time.UTC)},
	}
	cases := []struct {
		at   time.Time
		want string
		ok   bool
	}{
		{time.Date(2026, 9, 20, 4, 59, 59, 0, time.UTC), "", false},
		{time.Date(2026, 9, 20, 5, 0, 0, 0, time.UTC), "a", true},
		{time.Date(2026, 9, 20, 13, 40, 0, 0, time.UTC), "a", true},
		{time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC), "b", true},
	}
	for _, c := range cases {
		got, ok := AtOrBefore(snapshots, c.at)
		if ok != c.ok || got.ID != c.want {
			t.Errorf("AtOrBefore(%s) = %q, %v; want %q, %v", c.at, got.ID, ok, c.want, c.ok)
		}
	}
}

// TestAtOrBeforeComparesWholeSecondsAsVolSyncDoes checks that AtOrBefore
// drops the fraction of a second from each snapshot's time before it compares
// it with the time asked for. VolSync's restic mover does the same, so a
// snapshot taken at 06:00:00.7 is in reach of a restoreAsOf of 06:00:00, the
// whole-second time a BackupRun reports for it.
func TestAtOrBeforeComparesWholeSecondsAsVolSyncDoes(t *testing.T) {
	snapshots := []Snapshot{
		{ID: "a", Time: time.Date(2026, 9, 20, 5, 0, 0, 0, time.UTC)},
		{ID: "b", Time: time.Date(2026, 9, 20, 6, 0, 0, 700_000_000, time.UTC)},
	}
	got, ok := AtOrBefore(snapshots, time.Date(2026, 9, 20, 6, 0, 0, 0, time.UTC))
	if !ok || got.ID != "b" {
		t.Errorf("AtOrBefore(06:00:00) = %q, %v; want the 06:00:00.7 snapshot", got.ID, ok)
	}
	if _, ok := AtOrBefore(snapshots[1:], time.Date(2026, 9, 20, 5, 59, 59, 999_000_000, time.UTC)); ok {
		t.Error("AtOrBefore(05:59:59.999) found the 06:00:00.7 snapshot, which VolSync would not restore")
	}
}

// TestMoverLayoutNeedsHostVolsyncAndOnlyDataButAllowsACopy checks each
// condition MoverLayout puts on a snapshot on its own: host exactly volsync
// and paths exactly /data and nothing else. A retimed copy keeps the host
// and the paths of the snapshot it copies, so it has the layout too, and a
// RestoreRun restores it by its own ID. MoverWritten is MoverLayout for a
// snapshot with no original.
func TestMoverLayoutNeedsHostVolsyncAndOnlyDataButAllowsACopy(t *testing.T) {
	mover := Snapshot{ID: "a", Hostname: "volsync", Paths: []string{"/data"}}
	cases := []struct {
		name   string
		change func(s *Snapshot)
		want   bool
	}{
		{"as a mover writes it", func(*Snapshot) {}, true},
		{"with tags", func(s *Snapshot) { s.Tags = []string{"quiesced"} }, true},
		{"a retimed copy", func(s *Snapshot) { s.Original = "b" }, true},
		{"another host", func(s *Snapshot) { s.Hostname = "volsync-src-notes-data-7xk2p" }, false},
		{"no host", func(s *Snapshot) { s.Hostname = "" }, false},
		{"no paths", func(s *Snapshot) { s.Paths = nil }, false},
		{"a second path", func(s *Snapshot) { s.Paths = []string{"/data", "/extra"} }, false},
		{"/data twice", func(s *Snapshot) { s.Paths = []string{"/data", "/data"} }, false},
		{"a path below /data", func(s *Snapshot) { s.Paths = []string{"/data/sub"} }, false},
		{"another path", func(s *Snapshot) { s.Paths = []string{"/srv"} }, false},
	}
	for _, c := range cases {
		s := mover
		s.Paths = slices.Clone(mover.Paths)
		c.change(&s)
		if got := MoverLayout(s); got != c.want {
			t.Errorf("%s: MoverLayout(%+v) = %v, want %v", c.name, s, got, c.want)
		}
		if got, want := MoverWritten(s), c.want && s.Original == ""; got != want {
			t.Errorf("%s: MoverWritten(%+v) = %v, want MoverLayout and no original, %v", c.name, s, got, want)
		}
	}
}

// TestParseRepositoryReadsEveryFormResticReads checks ParseRepository
// against the cases of restic v0.18.1 internal/backend/s3/config_test.go:
// s3://host/bucket/prefix, s3:host/bucket/prefix and s3:http(s)://host/...
// all name a repository, and a trailing "/" leaves the prefix unchanged.
// restic reads them with ParseConfig (internal/backend/s3/config.go:60-106).
func TestParseRepositoryReadsEveryFormResticReads(t *testing.T) {
	for in, want := range map[string]Location{
		"s3://eu-central-1/bucketname":                            {Endpoint: "eu-central-1", Secure: true, Bucket: "bucketname"},
		"s3://eu-central-1/bucketname/":                           {Endpoint: "eu-central-1", Secure: true, Bucket: "bucketname"},
		"s3://eu-central-1/bucketname/prefix/directory":           {Endpoint: "eu-central-1", Secure: true, Bucket: "bucketname", Prefix: "prefix/directory"},
		"s3://eu-central-1/bucketname/prefix/directory/":          {Endpoint: "eu-central-1", Secure: true, Bucket: "bucketname", Prefix: "prefix/directory"},
		"s3:eu-central-1/foobar/prefix/directory/":                {Endpoint: "eu-central-1", Secure: true, Bucket: "foobar", Prefix: "prefix/directory"},
		"s3:hostname.foo/foobar":                                  {Endpoint: "hostname.foo", Secure: true, Bucket: "foobar"},
		"s3:https://hostname:9999/foobar/":                        {Endpoint: "hostname:9999", Secure: true, Bucket: "foobar"},
		"s3:http://hostname:9999/bucket/prefix/directory/":        {Endpoint: "hostname:9999", Secure: false, Bucket: "bucket", Prefix: "prefix/directory"},
		"s3://rustfs.backup-system.svc:9000/prod-backup/app/data": {Endpoint: "rustfs.backup-system.svc:9000", Secure: true, Bucket: "prod-backup", Prefix: "app/data"},
		// restic cuts at the first two "/" and cleans the rest with path.Clean
		// (config.go:84-88 and createConfig), which keeps the leading "/".
		"s3:host/bucket//a//b": {Endpoint: "host", Secure: true, Bucket: "bucket", Prefix: "/a/b"},
	} {
		got, err := ParseRepository(in)
		if err != nil {
			t.Errorf("ParseRepository(%q): %v", in, err)
			continue
		}
		if got != want {
			t.Errorf("ParseRepository(%q) = %+v, want %+v", in, got, want)
		}
	}
	// restic refuses these with "s3: invalid format" (config_test.go:123-132).
	for _, bad := range []string{"s3://", "s3:///", "s3:////", "s3:///bucket/prefix"} {
		if _, err := ParseRepository(bad); err == nil {
			t.Errorf("ParseRepository(%q) accepted it", bad)
		}
	}
}
