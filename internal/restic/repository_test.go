package restic

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

// TestTheFixtureListsBothSnapshotsWithResticsTimes checks that Snapshots reads
// both fixture snapshots with the IDs and times restic gave them, oldest first.
func TestTheFixtureListsBothSnapshotsWithResticsTimes(t *testing.T) {
	repo, err := Open(context.Background(), fixture, "backup")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	snapshots, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("snapshots: %v", err)
	}

	want := []struct {
		id   string
		time time.Time
	}{
		{"49319ee865f4a00a86a35af5edf18730836cb27e770aba03ceca2de36a37be0f", time.Date(2026, 9, 20, 5, 0, 0, 0, time.UTC)},
		{"88dc3648f06c94f727f601cb69653191e7e79602d36965a8e7892388d7875b81", time.Date(2026, 9, 21, 5, 0, 0, 0, time.UTC)},
	}
	if len(snapshots) != len(want) {
		t.Fatalf("got %d snapshots, want %d: %+v", len(snapshots), len(want), snapshots)
	}
	for i, w := range want {
		if snapshots[i].ID != w.id {
			t.Errorf("snapshot %d id = %s, want %s", i, snapshots[i].ID, w.id)
		}
		if !snapshots[i].Time.Equal(w.time) {
			t.Errorf("snapshot %d time = %s, want %s", i, snapshots[i].Time, w.time)
		}
	}
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

// TestByShortIDFindsTheSnapshotTheMoverLogged checks that ByShortID finds a
// snapshot by the eight-character ID a mover logs, and that an empty ID
// matches nothing.
func TestByShortIDFindsTheSnapshotTheMoverLogged(t *testing.T) {
	snapshots := []Snapshot{{ID: "6e473100aaaa"}, {ID: "2edf5babbbbb"}}
	got, ok := ByShortID(snapshots, "6e473100")
	if !ok || got.ID != "6e473100aaaa" {
		t.Fatalf("ByShortID = %+v, %v", got, ok)
	}
	if _, ok := ByShortID(snapshots, ""); ok {
		t.Fatal("an empty short id matched a snapshot")
	}
}

// TestParseRepositoryReadsResticsS3Form checks that ParseRepository reads
// restic's s3 form with and without a scheme, and rejects another backend,
// another scheme, and a string that names no bucket.
func TestParseRepositoryReadsResticsS3Form(t *testing.T) {
	cases := []struct {
		in   string
		want Location
	}{
		{"s3:http://quasar.example:10172/prod-cluster-backup/canary-data",
			Location{Endpoint: "quasar.example:10172", Secure: false, Bucket: "prod-cluster-backup", Prefix: "canary-data"}},
		{"s3:https://store.example/bucket/a/b",
			Location{Endpoint: "store.example", Secure: true, Bucket: "bucket", Prefix: "a/b"}},
		{"s3:store.example/bucket",
			Location{Endpoint: "store.example", Secure: true, Bucket: "bucket", Prefix: ""}},
	}
	for _, c := range cases {
		got, err := ParseRepository(c.in)
		if err != nil {
			t.Errorf("ParseRepository(%q): %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("ParseRepository(%q) = %+v, want %+v", c.in, got, c.want)
		}
	}
	for _, bad := range []string{"b2:bucket/x", "s3:ftp://host/bucket", "s3:http://host"} {
		if _, err := ParseRepository(bad); err == nil {
			t.Errorf("ParseRepository(%q) accepted it", bad)
		}
	}
}

// TestFromSecretNamesTheMissingKey checks that FromSecret's error names the key
// a Secret is missing, and that a complete Secret gives the password, the
// prefix and the access key.
func TestFromSecretNamesTheMissingKey(t *testing.T) {
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-restic-data", Namespace: "app"},
		Data: map[string][]byte{
			KeyRepository: []byte("s3:http://host:1/bucket/app-data"),
			KeyPassword:   []byte("backup"),
			KeyAccessKey:  []byte("key"),
		},
	}
	_, err := FromSecret(secret)
	if err == nil || !strings.Contains(err.Error(), KeySecretKey) {
		t.Fatalf("err = %v, want it to name %s", err, KeySecretKey)
	}

	secret.Data[KeySecretKey] = []byte("secret")
	at, err := FromSecret(secret)
	if err != nil {
		t.Fatal(err)
	}
	if at.Password != "backup" || at.Prefix != "app-data" || at.AccessKey != "key" {
		t.Fatalf("FromSecret = %+v", at)
	}
}

// TestUnpackRejectsAnUnknownEncoding checks that unpack rejects an unknown
// encoding byte and returns plain JSON unchanged.
func TestUnpackRejectsAnUnknownEncoding(t *testing.T) {
	if _, err := unpack([]byte{7, 1, 2}); err == nil {
		t.Fatal("an unknown encoding byte was accepted")
	}
	if got, err := unpack([]byte(`{"a":1}`)); err != nil || string(got) != `{"a":1}` {
		t.Fatalf("plain JSON: %q, %v", got, err)
	}
}
