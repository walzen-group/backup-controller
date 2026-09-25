// Package barmanstore holds object stores that real barman-cloud 3.20.0 wrote
// (the version plugin-barman-cloud v0.15.0's sidecar runs), recorded by
// hack/fixtures/barman-stores.sh against RustFS 1.0.0 with a real PostgreSQL,
// and a builder that puts them into any S3 server under any prefix.
//
// A recorded store stands in for the archive a CloudNativePG Cluster leaves
// behind. It keeps every object's key, size and time, and the full body of
// the files the controller and CloudNativePG read (backup.info and timeline
// .history files); the data and WAL files are kept as placeholders of their
// recorded size, because nothing in the controller reads them. Next to each
// store are barman's own verdicts on it (barman-cloud-backup-list and
// barman-cloud-check-wal-archive, per server), and a transcript of the
// requests the controller's S3 reader made against RustFS with RustFS's
// answers, which s3fake's tests replay.
//
// What a store deliberately doesn't carry: the content of data and WAL
// files, so nothing can restore from it; and S3 metadata beyond size, time
// and ETag.
//
// It is test support: only tests and the fixture recorders import it.
package barmanstore

import (
	"crypto/md5" //nolint:gosec // S3 ETags are MD5 digests of the body.
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/testinfra/s3fake"
)

// The recorded stores, one directory each, and provenance.json.
//
//go:embed recorded
var recorded embed.FS

// Object is one recorded object.
type Object struct {
	// Key is the object's key relative to the store's prefix, starting with
	// the server name: app-pg/base/20260925T212400/backup.info.
	Key string `json:"key"`
	// Size is the object's size as the recording listed it.
	Size int64 `json:"size"`
	// LastModified is the object's time as the recording listed it.
	LastModified time.Time `json:"lastModified"`
	// ETag is the entity tag RustFS gave the object.
	ETag string `json:"etag"`
	// Body is the full content, for backup.info and .history files. It is nil
	// for a placeholder.
	Body *string `json:"body,omitempty"`
}

// Manifest is one recorded store as manifest.json keeps it.
type Manifest struct {
	// Store is the store's name, the directory it is recorded in.
	Store string `json:"store"`
	// Objects are the store's objects in key order.
	Objects []Object `json:"objects"`
}

// Verdict is what barman said about one server of a recorded store.
type Verdict struct {
	// Backups is barman-cloud-backup-list --format json's list for the
	// server.
	Backups []BackupListEntry `json:"backups"`
	// CheckWalArchive is barman-cloud-check-wal-archive's result for the
	// server, the check CloudNativePG runs before a new Cluster archives.
	CheckWalArchive Result `json:"checkWalArchive"`
}

// BackupListEntry is one backup as barman-cloud-backup-list --format json
// prints it, with the fields the tests compare.
type BackupListEntry struct {
	BackupID   string `json:"backup_id"`
	BackupName string `json:"backup_name"`
	Status     string `json:"status"`
	EndTime    string `json:"end_time"`
}

// Result is the exit code and combined output of a barman command.
type Result struct {
	ExitCode int    `json:"exitCode"`
	Output   string `json:"output"`
}

// Store is a recorded store, loaded and optionally changed by the builder
// methods, ready to be written into a server with Upload.
type Store struct {
	Manifest
	// Verdicts are barman's verdicts per server name, for the store as
	// recorded. A builder method that changes the store drops them, since
	// barman never saw the changed store.
	Verdicts map[string]Verdict
}

// Names returns the names of the recorded stores.
func Names() ([]string, error) {
	entries, err := recorded.ReadDir("recorded")
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	return names, nil
}

// Provenance returns recorded/provenance.json: the generator and the tool
// versions the stores were recorded with.
func Provenance() (map[string]any, error) {
	raw, err := recorded.ReadFile("recorded/provenance.json")
	if err != nil {
		return nil, err
	}
	var p map[string]any
	return p, json.Unmarshal(raw, &p)
}

// Load reads one recorded store by name, such as done-base. It returns an
// error when there is no such store or its files don't decode.
func Load(name string) (Store, error) {
	var s Store
	if err := readJSON(path.Join("recorded", name, "manifest.json"), &s.Manifest); err != nil {
		return Store{}, err
	}
	if err := readJSON(path.Join("recorded", name, "verdicts.json"), &s.Verdicts); err != nil {
		return Store{}, err
	}
	return s, nil
}

// MustLoad is Load for a test: it fails the test when the store can't be
// loaded.
func MustLoad(t testing.TB, name string) Store {
	t.Helper()
	s, err := Load(name)
	if err != nil {
		t.Fatalf("load recorded store %s: %v", name, err)
	}
	return s
}

// Transcript returns the exchanges recorded while bootstrap.S3Prober read one
// server of a recorded store from RustFS, for the tests that replay them.
func Transcript(name, server string) ([]Exchange, error) {
	var exchanges []Exchange
	return exchanges, readJSON(path.Join("recorded", name, "transcript-"+server+".json"), &exchanges)
}

// readJSON decodes one embedded file.
func readJSON(name string, v any) error {
	raw, err := recorded.ReadFile(name)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return fmt.Errorf("decode %s: %w", name, err)
	}
	return nil
}

// Exchange is one recorded S3 request and the answer RustFS gave it.
type Exchange struct {
	// Step names what the recorder was doing, such as prober-list.
	Step string `json:"step"`
	// Method, Path and Query are the request line; Query is the raw query
	// with its parameters in the order the client sent them.
	Method string `json:"method"`
	Path   string `json:"path"`
	Query  string `json:"query"`
	// WrongKey is true for a request signed with an access key the server
	// doesn't know.
	WrongKey bool `json:"wrongKey,omitempty"`
	// Range is the request's Range header, when it had one.
	Range string `json:"range,omitempty"`
	// Status, Headers and Body are RustFS's answer. Headers keeps the ones a
	// client reads: Content-Type, Content-Length, Content-Range, ETag,
	// Last-Modified and Accept-Ranges.
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
	Body    string            `json:"body"`
}

// Behaviour is recorded/rustfs-behaviour.json: exchanges that show how
// RustFS pages, rolls up prefixes and reports errors, over a set of small
// objects the recorder wrote for the purpose, which Objects lists.
type Behaviour struct {
	Bucket    string     `json:"bucket"`
	Objects   []Object   `json:"objects"`
	Exchanges []Exchange `json:"exchanges"`
}

// RustFSBehaviour returns the recorded rustfs-behaviour.json.
func RustFSBehaviour() (Behaviour, error) {
	var b Behaviour
	return b, readJSON("recorded/rustfs-behaviour.json", &b)
}

// Writer is an S3 server a store can be written into, such as a thin wrapper
// over a minio client for RustFS. A real server sets each object's ETag
// itself, so PutObject has no ETag parameter; s3fake.Server implements Writer
// too, and Upload gives it the recorded ETags through s3fake's Put instead.
type Writer interface {
	PutObject(bucket, key string, body []byte, size int64, modified time.Time) error
}

// Upload writes every object of the store into bucket under prefix, so that
// an object recorded as app-pg/base/x/backup.info lands at
// <prefix>/app-pg/base/x/backup.info. A placeholder is written with its
// recorded size and no body.
//
// When w is an *s3fake.Server, each object keeps its recorded ETag, so a
// listing shows what RustFS showed, such as the "-2" multipart ETag of a
// data.tar. Any other Writer gets PutObject, and the server picks the ETag.
// It returns the first error a put gives.
func (s Store) Upload(w Writer, bucket, prefix string) error {
	fake, isFake := w.(*s3fake.Server)
	for _, o := range s.Objects {
		var body []byte
		if o.Body != nil {
			body = []byte(*o.Body)
		}
		key := strings.TrimPrefix(strings.TrimSuffix(prefix, "/")+"/"+o.Key, "/")
		if isFake {
			fake.Put(bucket, key, s3fake.Object{Body: body, Size: o.Size, LastModified: o.LastModified, ETag: o.ETag})
			continue
		}
		if err := w.PutObject(bucket, key, body, o.Size, o.LastModified); err != nil {
			return fmt.Errorf("put %s: %w", key, err)
		}
	}
	return nil
}

// bodyETag returns the quoted MD5 of a body, the ETag RustFS gives an object
// uploaded in one part, as barman uploads a backup.info.
func bodyETag(body string) string {
	sum := md5.Sum([]byte(body)) //nolint:gosec // S3 ETags are MD5 digests.
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// backupDir matches the base backup directory in a key: the server name, then
// base/ and barman's backup ID, which is the backup's start time in the form
// 20060102T150405.
var backupDir = regexp.MustCompile(`^([^/]+)/base/(\d{8}T\d{6})/`)

// barmanTime matches a timestamp in a backup.info line, in the form barman
// writes: 2026-09-25 21:24:00.123456+00:00, with or without the fraction.
var barmanTime = regexp.MustCompile(`^(\w+)=(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?\+00:00)$`)

// labelTime matches the start time PostgreSQL writes into the backup label,
// which backup.info keeps on its backup_label line: START TIME: 2026-09-25
// 21:52:25 UTC.
var labelTime = regexp.MustCompile(`START TIME: (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}) UTC`)

// Shift moves the whole store in time by d, as if barman had written it that
// much later: every backup ID, the times in every backup.info, and every
// object's LastModified. WAL segment names don't carry a time and stay. A
// rewritten backup.info gets the MD5 ETag of its new body. The shifted store
// has no verdicts.
func (s Store) Shift(d time.Duration) Store {
	out := Store{Manifest: Manifest{Store: s.Store}}
	for _, o := range s.Objects {
		o.LastModified = o.LastModified.Add(d)
		if m := backupDir.FindStringSubmatch(o.Key); m != nil {
			id := shiftID(m[2], d)
			o.Key = m[1] + "/base/" + id + "/" + strings.TrimPrefix(o.Key, m[0])
			if o.Body != nil && path.Base(o.Key) == "backup.info" {
				body := shiftInfo(*o.Body, m[2], id, d)
				o.Body = &body
				o.ETag = bodyETag(body)
			}
		}
		out.Objects = append(out.Objects, o)
	}
	sort.Slice(out.Objects, func(i, j int) bool { return out.Objects[i].Key < out.Objects[j].Key })
	return out
}

// shiftID moves a backup ID of the form 20060102T150405 by d.
func shiftID(id string, d time.Duration) string {
	t, err := time.Parse("20060102T150405", id)
	if err != nil {
		return id
	}
	return t.Add(d).Format("20060102T150405")
}

// shiftInfo moves every timestamp in a backup.info body by d, and replaces
// the old backup ID wherever the body names it. The times are barman's own
// key=value timestamps and the START TIME in the backup label.
func shiftInfo(body, oldID, newID string, d time.Duration) string {
	lines := strings.Split(body, "\n")
	for i, line := range lines {
		m := barmanTime.FindStringSubmatch(line)
		if m == nil {
			continue
		}
		layout := "2006-01-02 15:04:05-07:00"
		if m[3] != "" {
			// Keep the fraction as wide as barman wrote it, trailing zeros too.
			layout = "2006-01-02 15:04:05." + strings.Repeat("0", len(m[3])-1) + "-07:00"
		}
		t, err := time.Parse(layout, m[2])
		if err != nil {
			continue
		}
		lines[i] = m[1] + "=" + t.Add(d).Format(layout)
	}
	out := labelTime.ReplaceAllStringFunc(strings.Join(lines, "\n"), func(s string) string {
		const layout = "2006-01-02 15:04:05"
		t, err := time.Parse(layout, labelTime.FindStringSubmatch(s)[1])
		if err != nil {
			return s
		}
		return "START TIME: " + t.Add(d).Format(layout) + " UTC"
	})
	return strings.ReplaceAll(out, oldID, newID)
}

// WithFailedBackup returns the store with one more FAILED base backup for
// server, a copy of the one barman left in failed-base (a backup.info with
// status=FAILED and no data), started at start.
//
// Parameters:
//   - server is the server name, the first part of the keys, such as app-pg.
//   - start is the backup's start time. barman's backup ID is that time in
//     UTC to the second, so start is truncated to the second.
//
// It returns an error when the server already has a backup with that ID. See
// withBackup for how the copy is made.
func (s Store) WithFailedBackup(server string, start time.Time) (Store, error) {
	return s.withBackup("failed-base", server, start)
}

// WithDoneBackup returns the store with one more DONE base backup for server,
// a copy of the one barman left in done-base (its backup.info with
// status=DONE and the data.tar placeholder), started at start. The
// parameters and errors are WithFailedBackup's; see withBackup for how the
// copy is made.
func (s Store) WithDoneBackup(server string, start time.Time) (Store, error) {
	return s.withBackup("done-base", server, start)
}

// withBackup adds a copy of the single base backup of the recorded store
// template to s, for server, started at start. It moves the copy the way
// Shift moves a store: by d, the time from the recorded backup's ID to the
// new one, it moves the directory name, every time in the backup.info and
// each object's LastModified. The copied backup.info gets the MD5 ETag of its
// new body; a placeholder keeps its recorded ETag. The WAL of the template
// isn't copied. The result is in key order and has no verdicts.
func (s Store) withBackup(template, server string, start time.Time) (Store, error) {
	tmpl, err := Load(template)
	if err != nil {
		return Store{}, err
	}
	newID := start.UTC().Truncate(time.Second).Format("20060102T150405")
	for _, id := range s.Backups()[server] {
		if id == newID {
			return Store{}, fmt.Errorf("server %s already has a base backup %s", server, newID)
		}
	}
	out := Store{Manifest: Manifest{Store: s.Store, Objects: append([]Object(nil), s.Objects...)}}
	for _, o := range tmpl.Objects {
		m := backupDir.FindStringSubmatch(o.Key)
		if m == nil {
			continue
		}
		oldStart, err := time.Parse("20060102T150405", m[2])
		if err != nil {
			return Store{}, fmt.Errorf("recorded backup ID %s: %w", m[2], err)
		}
		d := start.UTC().Truncate(time.Second).Sub(oldStart)
		o.Key = server + "/base/" + newID + "/" + strings.TrimPrefix(o.Key, m[0])
		o.LastModified = o.LastModified.Add(d)
		if o.Body != nil && path.Base(o.Key) == "backup.info" {
			body := shiftInfo(*o.Body, m[2], newID, d)
			o.Body = &body
			o.ETag = bodyETag(body)
		}
		out.Objects = append(out.Objects, o)
	}
	sort.Slice(out.Objects, func(i, j int) bool { return out.Objects[i].Key < out.Objects[j].Key })
	return out, nil
}

// Backups returns the IDs of the base backups in the store, per server, in
// key order.
func (s Store) Backups() map[string][]string {
	out := map[string][]string{}
	seen := map[string]bool{}
	for _, o := range s.Objects {
		if m := backupDir.FindStringSubmatch(o.Key); m != nil && !seen[m[0]] {
			seen[m[0]] = true
			out[m[1]] = append(out[m[1]], m[2])
		}
	}
	return out
}
