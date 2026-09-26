package barmanstore_test

import (
	"context"
	"path"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
)

// testBucket and testPrefix are where the tests upload a store, so that the
// prober reads it under a prefix other than the one it was recorded under.
const (
	testBucket = "archive"
	testPrefix = "clusters/team-a"
)

// TestTheRecordingsCameFromThePinnedTools checks that provenance.json names
// the barman, RustFS and PostgreSQL versions versions.json pins, so a bump
// that forgets to regenerate the stores fails here.
func TestTheRecordingsCameFromThePinnedTools(t *testing.T) {
	p, err := barmanstore.Provenance()
	if err != nil {
		t.Fatalf("read provenance.json: %v", err)
	}
	tools, ok := p["tools"].(map[string]any)
	if !ok {
		t.Fatalf("provenance.json has no tools map: %v", p)
	}
	for _, name := range []string{"barman", "rustfs", "postgresql"} {
		if got, want := tools[name], versions.Of(t, name); got != want {
			t.Errorf("the stores were recorded with %s %v, versions.json pins %s", name, got, want)
		}
	}
}

// TestEveryRecordedStoreLoads checks that the eight stores are there and
// decode, and that each has barman's verdict for at least one server.
func TestEveryRecordedStoreLoads(t *testing.T) {
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatalf("list the recorded stores: %v", err)
	}
	want := []string{"after-recovery", "done-base", "empty", "failed-base", "many-failed", "started-base", "two-servers", "wal-only"}
	if strings.Join(names, ",") != strings.Join(want, ",") {
		t.Fatalf("recorded stores are %v, want %v", names, want)
	}
	for _, name := range names {
		s := barmanstore.MustLoad(t, name)
		if s.Store != name {
			t.Errorf("store %s: manifest names itself %q", name, s.Store)
		}
		if len(s.Verdicts) == 0 {
			t.Errorf("store %s: no verdicts", name)
		}
	}
	if _, err := barmanstore.Load("no-such-store"); err == nil {
		t.Error("Load of a store that isn't recorded gave no error")
	}
}

// TestTheProberAgreesWithBarman uploads every recorded store into s3fake and
// runs the controller's S3Prober against each server in it. BaseBackups has
// to list exactly the DONE backups barman-cloud-backup-list listed, with the
// same IDs and end times, and Survey has to find a DONE backup exactly when
// there is one.
func TestTheProberAgreesWithBarman(t *testing.T) {
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatalf("list the recorded stores: %v", err)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			s := barmanstore.MustLoad(t, name)
			srv := s3fake.New(t, testBucket)
			if err := s.Upload(srv, testBucket, testPrefix); err != nil {
				t.Fatalf("upload: %v", err)
			}
			for server, verdict := range s.Verdicts {
				t.Run(server, func(t *testing.T) {
					at := location(srv, server)
					want := doneBackups(t, verdict.Backups)
					got, err := bootstrap.S3Prober{}.BaseBackups(context.Background(), at)
					if err != nil {
						t.Fatalf("BaseBackups: %v", err)
					}
					if describe(got, time.Second) != describe(want, 0) {
						t.Errorf("BaseBackups = %s, barman lists DONE %s", describe(got, time.Second), describe(want, 0))
					}
					archive, err := bootstrap.S3Prober{}.Survey(context.Background(), at, nil)
					if err != nil {
						t.Fatalf("Survey: %v", err)
					}
					if has := archive.Found != nil; has != (len(want) > 0) {
						t.Errorf("Survey found a DONE backup: %v, barman lists %d DONE backups", has, len(want))
					}
				})
			}
		})
	}
}

// TestAStoreLeftToInitdbPassesBarmansArchiveCheck checks the webhook's
// decision against barman's: the webhook leaves a Cluster to initdb only
// where Survey finds no completed base backup and nothing at all under the
// prefix, and CloudNativePG then runs
// barman-cloud-check-wal-archive, which must pass on every such store or the
// Cluster never archives. Finding W1, designs/webhook.md A.
func TestAStoreLeftToInitdbPassesBarmansArchiveCheck(t *testing.T) {
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatalf("list the recorded stores: %v", err)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			s := barmanstore.MustLoad(t, name)
			srv := s3fake.New(t, testBucket)
			if err := s.Upload(srv, testBucket, testPrefix); err != nil {
				t.Fatalf("upload: %v", err)
			}
			for server, verdict := range s.Verdicts {
				t.Run(server, func(t *testing.T) {
					at := location(srv, server)
					archive, err := bootstrap.S3Prober{}.Survey(context.Background(), at, nil)
					if err != nil {
						t.Fatalf("Survey: %v", err)
					}
					initdb := archive.Found == nil && archive.Empty
					if initdb && verdict.CheckWalArchive.ExitCode != 0 {
						t.Errorf("the webhook leaves the Cluster to initdb, but barman-cloud-check-wal-archive fails on the store (exit %d: %s), so the Cluster could never archive",
							verdict.CheckWalArchive.ExitCode, strings.TrimSpace(verdict.CheckWalArchive.Output))
					}
				})
			}
		})
	}
}

// TestShiftMovesEveryTime shifts every recorded store by a day and a bit and
// checks that each object's time, each backup ID and each timestamp in a
// backup.info moved by the same amount, that nothing else in a body changed,
// and that the prober reads the shifted end times.
func TestShiftMovesEveryTime(t *testing.T) {
	const d = 26*time.Hour + 7*time.Minute + 3*time.Second
	names, err := barmanstore.Names()
	if err != nil {
		t.Fatalf("list the recorded stores: %v", err)
	}
	for _, name := range names {
		t.Run(name, func(t *testing.T) {
			s := barmanstore.MustLoad(t, name)
			shifted := s.Shift(d)
			if shifted.Verdicts != nil {
				t.Error("the shifted store kept barman's verdicts")
			}
			if len(shifted.Objects) != len(s.Objects) {
				t.Fatalf("shifted store has %d objects, the recording %d", len(shifted.Objects), len(s.Objects))
			}

			byKey := map[string]barmanstore.Object{}
			for _, o := range shifted.Objects {
				byKey[o.Key] = o
			}
			for _, o := range s.Objects {
				key := shiftKey(t, o.Key, d)
				moved, ok := byKey[key]
				if !ok {
					t.Errorf("%s: no object at %s after the shift", o.Key, key)
					continue
				}
				if !moved.LastModified.Equal(o.LastModified.Add(d)) {
					t.Errorf("%s: LastModified %s, want %s", o.Key, moved.LastModified, o.LastModified.Add(d))
				}
				if moved.Size != o.Size {
					t.Errorf("%s: size %d, want %d", o.Key, moved.Size, o.Size)
				}
				if (o.Body == nil) != (moved.Body == nil) {
					t.Errorf("%s: body presence changed", o.Key)
					continue
				}
				if o.Body != nil {
					checkShiftedBody(t, o.Key, *o.Body, *moved.Body, d)
				}
			}

			for server, ids := range s.Backups() {
				got := shifted.Backups()[server]
				if len(got) != len(ids) {
					t.Errorf("server %s: %d backups after the shift, %d before", server, len(got), len(ids))
					continue
				}
				for i, id := range ids {
					if want := shiftID(t, id, d); got[i] != want {
						t.Errorf("server %s: backup %s became %s, want %s", server, id, got[i], want)
					}
				}
			}

			srv := s3fake.New(t, testBucket)
			if err := shifted.Upload(srv, testBucket, testPrefix); err != nil {
				t.Fatalf("upload: %v", err)
			}
			for server, verdict := range s.Verdicts {
				want := doneBackups(t, verdict.Backups)
				for i := range want {
					want[i].ID = shiftID(t, want[i].ID, d)
					want[i].End = want[i].End.Add(d)
				}
				got, err := bootstrap.S3Prober{}.BaseBackups(context.Background(), location(srv, server))
				if err != nil {
					t.Fatalf("server %s: BaseBackups: %v", server, err)
				}
				if describe(got, time.Second) != describe(want, 0) {
					t.Errorf("server %s: BaseBackups of the shifted store = %s, want %s", server, describe(got, time.Second), describe(want, 0))
				}
			}
		})
	}
}

// location is the Location of one server of a store uploaded under
// testPrefix into srv.
func location(srv *s3fake.Server, server string) bootstrap.Location {
	return bootstrap.Location{
		Endpoint:  srv.URL,
		Bucket:    testBucket,
		Prefix:    testPrefix + "/" + server,
		AccessKey: srv.AccessKey,
		SecretKey: srv.SecretKey,
	}
}

// doneBackups turns barman-cloud-backup-list's DONE entries into the
// BaseBackups the prober should return, in the list's order, which is oldest
// first. barman prints end_time in ctime form, to the second, in the
// recorder's zone, UTC.
func doneBackups(t *testing.T, entries []barmanstore.BackupListEntry) []bootstrap.BaseBackup {
	t.Helper()
	var out []bootstrap.BaseBackup
	for _, e := range entries {
		if e.Status != "DONE" {
			continue
		}
		end, err := time.ParseInLocation("Mon Jan _2 15:04:05 2006", e.EndTime, time.UTC)
		if err != nil {
			t.Fatalf("backup %s: barman's end_time %q: %v", e.BackupID, e.EndTime, err)
		}
		out = append(out, bootstrap.BaseBackup{ID: e.BackupID, End: end})
	}
	return out
}

// describe prints a list of backups for comparison, with each end time
// truncated to the given precision (0 leaves it) and in UTC.
func describe(backups []bootstrap.BaseBackup, precision time.Duration) string {
	parts := make([]string, 0, len(backups))
	for _, b := range backups {
		end := b.End.UTC()
		if precision > 0 {
			end = end.Truncate(precision)
		}
		parts = append(parts, b.ID+"@"+end.Format(time.RFC3339Nano))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// backupIDPattern matches a barman backup ID anywhere in a key or body.
var backupIDPattern = regexp.MustCompile(`\d{8}T\d{6}`)

// timePattern matches a timestamp as barman and PostgreSQL write them in a
// backup.info: 2026-09-25 21:52:25.406367+00:00, or 2026-09-25 21:52:25 UTC
// inside the backup label.
var timePattern = regexp.MustCompile(`\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?(\+00:00| UTC)`)

// shiftID moves a backup ID by d.
func shiftID(t *testing.T, id string, d time.Duration) string {
	t.Helper()
	at, err := time.Parse("20060102T150405", id)
	if err != nil {
		t.Fatalf("backup ID %q: %v", id, err)
	}
	return at.Add(d).Format("20060102T150405")
}

// shiftKey is the key an object should have after a shift by d: a base
// backup's directory moves with its ID, anything else keeps its key.
func shiftKey(t *testing.T, key string, d time.Duration) string {
	t.Helper()
	parts := strings.SplitN(key, "/", 4)
	if len(parts) >= 3 && parts[1] == "base" {
		parts[2] = shiftID(t, parts[2], d)
	}
	return strings.Join(parts, "/")
}

// checkShiftedBody checks that the shifted body is the recorded one with
// every timestamp and backup ID moved by d and nothing else changed. It
// rebuilds the expected body from the recorded one independently of Shift.
func checkShiftedBody(t *testing.T, key, recorded, shifted string, d time.Duration) {
	t.Helper()
	want := timePattern.ReplaceAllStringFunc(recorded, func(s string) string {
		m := timePattern.FindStringSubmatch(s)
		layout := "2006-01-02 15:04:05"
		if m[1] != "" {
			layout += "." + strings.Repeat("0", len(m[1])-1)
		}
		at, err := time.Parse(layout, strings.TrimSuffix(s, m[2]))
		if err != nil {
			t.Fatalf("%s: timestamp %q: %v", key, s, err)
		}
		return at.Add(d).Format(layout) + m[2]
	})
	want = backupIDPattern.ReplaceAllStringFunc(want, func(id string) string { return shiftID(t, id, d) })
	if shifted != want {
		t.Errorf("%s: shifted body differs from the recorded one moved by %s\n got: %q\nwant: %q", key, d, shifted, want)
	}
	if path.Base(key) == "backup.info" && !timePattern.MatchString(recorded) {
		t.Errorf("%s: a backup.info with no timestamp; the pattern is out of date", key)
	}
}
