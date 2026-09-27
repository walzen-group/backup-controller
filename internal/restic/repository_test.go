package restic

import (
	"context"
	"errors"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
)

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
	repo := DirStore(filepath.Join(fixtures(t, "timed")[0].dir, "repo"))
	_, err := Open(context.Background(), repo, "not-the-password")
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

// emptyFirstPage is a ListObjectsV2 answer that holds no key and says more
// follow. S3 may cut a page short at any count, none included.
const emptyFirstPage = `<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Name>repo</Name><Prefix>locks/</Prefix><KeyCount>0</KeyCount><MaxKeys>1000</MaxKeys><Delimiter>/</Delimiter><IsTruncated>true</IsTruncated><NextContinuationToken>page-2</NextContinuationToken></ListBucketResult>`

// firstPageThenCancel answers the first page of a ListObjectsV2 with
// emptyFirstPage and calls cancel when the client closes that answer, which
// minio-go does once it has read the page and before it asks for the next.
// Every other request goes to next.
type firstPageThenCancel struct {
	next   http.RoundTripper
	cancel context.CancelFunc
}

// RoundTrip serves one request as the type's comment says.
func (f firstPageThenCancel) RoundTrip(r *http.Request) (*http.Response, error) {
	query := r.URL.Query()
	if query.Get("list-type") != "2" || query.Has("continuation-token") {
		return f.next.RoundTrip(r)
	}
	return &http.Response{
		StatusCode:    http.StatusOK,
		Header:        http.Header{"Content-Type": {"application/xml"}},
		Body:          cancelOnClose{Reader: strings.NewReader(emptyFirstPage), cancel: f.cancel},
		ContentLength: int64(len(emptyFirstPage)),
		Request:       r,
	}, nil
}

// cancelOnClose is a response body that calls cancel when it is closed.
type cancelOnClose struct {
	io.Reader
	cancel context.CancelFunc
}

// Close calls cancel.
func (c cancelOnClose) Close() error {
	c.cancel()
	return nil
}

// TestAListingCutShortByTheContextIsAnError checks that S3Store.List fails
// when its context ends between two pages of the listing. minio-go then
// stops without an error, and a List that took the names so far as all of
// them would report a repository whose lock is live as unlocked. The s3fake
// server holds a lock; the first page comes back empty and truncated, and
// the context ends once minio-go has read it. Finding W5.
func TestAListingCutShortByTheContextIsAnError(t *testing.T) {
	server := s3fake.New(t, "repo")
	server.Put("repo", "locks/0123abcd", s3fake.Object{Body: []byte("lock")})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client, err := minio.New(server.Endpoint(), &minio.Options{
		Creds:     credentials.NewStaticV4(server.AccessKey, server.SecretKey, ""),
		Transport: firstPageThenCancel{next: http.DefaultTransport, cancel: cancel},
	})
	if err != nil {
		t.Fatal(err)
	}
	store := &S3Store{client: client, at: Location{Endpoint: server.Endpoint(), Bucket: "repo"}}

	names, err := store.List(ctx, "locks")

	if err == nil {
		t.Fatalf("List gave no error and %v, although the context ended before the second page", names)
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("err = %v, want it to wrap context.Canceled", err)
	}
}

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

// The tests in this file read testdata/same-time, a repository that
// hack/fixtures/restic-same-time.sh wrote with the restic of the VolSync mover
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
