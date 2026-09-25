package barmanstore_test

import (
	"context"
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 digests of the body.
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
)

// TestAnUploadedStoreListsWithTheRecordedETags uploads every recorded store
// into s3fake and lists it with minio-go: each key has to come back with the
// ETag RustFS gave it, the multipart "-2" ETag of a data.tar included.
func TestAnUploadedStoreListsWithTheRecordedETags(t *testing.T) {
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatalf("list the recorded stores: %v", err)
	}
	multipart := 0
	for _, name := range names {
		s := barmanstore.MustLoad(t, name)
		srv := s3fake.New(t, testBucket)
		if err := s.Upload(srv, testBucket, testPrefix); err != nil {
			t.Fatalf("%s: upload: %v", name, err)
		}
		got := map[string]string{}
		for o := range client(t, srv).ListObjects(context.Background(), testBucket, minio.ListObjectsOptions{Prefix: testPrefix + "/", Recursive: true}) {
			if o.Err != nil {
				t.Fatalf("%s: list: %v", name, o.Err)
			}
			got[strings.TrimPrefix(o.Key, testPrefix+"/")] = o.ETag
		}
		if len(got) != len(s.Objects) {
			t.Errorf("%s: listed %d objects, the store has %d", name, len(got), len(s.Objects))
		}
		for _, o := range s.Objects {
			// minio-go strips the quotes from a listed ETag.
			if want := strings.Trim(o.ETag, `"`); got[o.Key] != want {
				t.Errorf("%s: %s lists with ETag %q, RustFS gave %q", name, o.Key, got[o.Key], want)
			}
			if strings.HasSuffix(o.ETag, `-2"`) {
				multipart++
			}
		}
	}
	if multipart == 0 {
		t.Error("no recorded object has a multipart ETag; the test no longer covers one")
	}
}

// TestARecordedBackupInfoHasTheMD5ETag checks the assumption Shift and the
// With helpers make when they rewrite a backup.info: RustFS gives an object
// barman uploaded in one part the MD5 of its body as its ETag.
func TestARecordedBackupInfoHasTheMD5ETag(t *testing.T) {
	for _, name := range []string{"failed-base", "done-base"} {
		for _, o := range barmanstore.MustLoad(t, name).Objects {
			if o.Body == nil || !strings.HasSuffix(o.Key, "/backup.info") {
				continue
			}
			sum := md5.Sum([]byte(*o.Body)) //nolint:gosec // S3 ETags are MD5 digests.
			if want := `"` + hex.EncodeToString(sum[:]) + `"`; o.ETag != want {
				t.Errorf("%s: %s has ETag %s, the MD5 of its body is %s", name, o.Key, o.ETag, want)
			}
		}
	}
}

// TestTheProberSeesAddedBackups adds a FAILED and then a DONE backup to
// done-base, a day after its recorded one, and checks that the prober lists
// the recorded DONE backup and the added one, with the added one's end time
// moved as far as its start, and never the FAILED one.
func TestTheProberSeesAddedBackups(t *testing.T) {
	const server = "app-pg"
	// done-base's backup started at 21:52:25 and barman wrote its end_time as
	// 21:52:26.001513.
	recordedStart := time.Date(2026, 9, 25, 21, 52, 25, 0, time.UTC)
	recordedEnd := time.Date(2026, 9, 25, 21, 52, 26, 1513000, time.UTC)
	failedAt := recordedStart.Add(24 * time.Hour)
	doneAt := failedAt.Add(time.Hour + 7*time.Minute + 11*time.Second)

	s := barmanstore.MustLoad(t, "done-base")
	s, err := s.WithFailedBackup(server, failedAt)
	if err != nil {
		t.Fatalf("WithFailedBackup: %v", err)
	}
	s, err = s.WithDoneBackup(server, doneAt)
	if err != nil {
		t.Fatalf("WithDoneBackup: %v", err)
	}
	if s.Verdicts != nil {
		t.Error("the changed store kept barman's verdicts")
	}
	failedID := failedAt.Format("20060102T150405")
	doneID := doneAt.Format("20060102T150405")
	if got, want := strings.Join(s.Backups()[server], ","), "20260925T215225,"+failedID+","+doneID; got != want {
		t.Errorf("backups are %s, want %s", got, want)
	}
	for _, o := range s.Objects {
		if strings.HasSuffix(o.Key, "/backup.info") && strings.Contains(o.Key, failedID) &&
			!strings.Contains(*o.Body, "\nstatus=FAILED\n") {
			t.Errorf("%s: the added FAILED backup.info doesn't say status=FAILED", o.Key)
		}
	}
	if _, err := s.WithDoneBackup(server, doneAt); err == nil {
		t.Error("adding a second backup with the same ID gave no error")
	}

	srv := s3fake.New(t, testBucket)
	if err := s.Upload(srv, testBucket, testPrefix); err != nil {
		t.Fatalf("upload: %v", err)
	}
	got, err := bootstrap.S3Prober{}.BaseBackups(context.Background(), location(srv, server))
	if err != nil {
		t.Fatalf("BaseBackups: %v", err)
	}
	want := []bootstrap.BaseBackup{
		{ID: "20260925T215225", End: recordedEnd},
		{ID: doneID, End: recordedEnd.Add(doneAt.Sub(recordedStart))},
	}
	if describe(got, 0) != describe(want, 0) {
		t.Errorf("BaseBackups = %s, want %s", describe(got, 0), describe(want, 0))
	}
}

// TestTheProberTellsAnAddedFailedBackupFromADoneOne builds on the empty
// store: with only an added FAILED backup HasBaseBackup is false, and with an
// added DONE backup it is true.
func TestTheProberTellsAnAddedFailedBackupFromADoneOne(t *testing.T) {
	const server = "app-pg"
	at := time.Date(2026, 10, 1, 3, 0, 0, 0, time.UTC)
	failed, err := barmanstore.MustLoad(t, "empty").WithFailedBackup(server, at)
	if err != nil {
		t.Fatalf("WithFailedBackup: %v", err)
	}
	done, err := barmanstore.MustLoad(t, "empty").WithDoneBackup(server, at)
	if err != nil {
		t.Fatalf("WithDoneBackup: %v", err)
	}
	for _, c := range []struct {
		name  string
		store barmanstore.Store
		want  bool
	}{{"failed", failed, false}, {"done", done, true}} {
		srv := s3fake.New(t, testBucket)
		if err := c.store.Upload(srv, testBucket, testPrefix); err != nil {
			t.Fatalf("%s: upload: %v", c.name, err)
		}
		has, err := bootstrap.S3Prober{}.HasBaseBackup(context.Background(), location(srv, server))
		if err != nil {
			t.Fatalf("%s: HasBaseBackup: %v", c.name, err)
		}
		if has != c.want {
			t.Errorf("%s: HasBaseBackup = %v, want %v", c.name, has, c.want)
		}
	}
}

// client is a minio-go client for srv.
func client(t *testing.T, srv *s3fake.Server) *minio.Client {
	t.Helper()
	c, err := minio.New(srv.Endpoint(), &minio.Options{Creds: credentials.NewStaticV4(srv.AccessKey, srv.SecretKey, "")})
	if err != nil {
		t.Fatalf("minio client: %v", err)
	}
	return c
}
