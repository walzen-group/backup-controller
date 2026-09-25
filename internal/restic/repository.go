// Package restic reads the snapshot list of a restic repository, and writes a
// snapshot again at another time.
//
// The controller needs three things from a repository: which snapshots it
// holds, when each was taken, and, after a quiesced backup, the snapshot moved
// to the moment the workloads were stopped. A RestoreRun refuses a point in
// time no snapshot reaches before it overwrites anything, and a BackupRun
// reports the time on the snapshot it made. All of it is done here, on the
// repository's own files, so the controller runs no restic binary and pins no
// image beside VolSync's.
package restic

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Store is where a repository's files live: an S3 bucket in the cluster, a
// directory in the tests.
type Store interface {
	// List returns the names of the files directly under dir, none when dir
	// does not exist.
	List(ctx context.Context, dir string) ([]string, error)
	// Get returns one file's content.
	Get(ctx context.Context, name string) ([]byte, error)
	// Put writes one file.
	Put(ctx context.Context, name string, data []byte) error
	// Remove deletes one file.
	Remove(ctx context.Context, name string) error
}

// Snapshot is one entry of the repository's snapshot list.
type Snapshot struct {
	// ID is the storage ID, the SHA-256 of the encrypted snapshot file, as
	// lower-case hex. restic prints its first eight characters as the short ID.
	ID string
	// Time is the time restic stamped on the snapshot when the backup started.
	Time time.Time
	// Paths are the directories the snapshot holds.
	Paths []string
	// Tags are the snapshot's restic tags.
	Tags []string
	// Original is the ID of the snapshot this one was rewritten from, empty
	// for a snapshot a backup wrote.
	Original string
}

// ShortID is the eight-character form restic prints, for example in the
// "snapshot 6e473100 saved" line a VolSync mover logs.
func (s Snapshot) ShortID() string {
	if len(s.ID) < 8 {
		return s.ID
	}
	return s.ID[:8]
}

// Repository is an opened repository: its store and its master key.
type Repository struct {
	store  Store
	master key
}

// ErrNoRepository is a location holding no key files, which is what a claim
// that has never been backed up has: VolSync initialises the repository on the
// first backup.
var ErrNoRepository = errors.New("no restic repository at this location yet")

// Open finds the key file the password unlocks and keeps the master key.
func Open(ctx context.Context, store Store, password string) (*Repository, error) {
	names, err := store.List(ctx, "keys")
	if err != nil {
		return nil, fmt.Errorf("list the key files: %w", err)
	}
	if len(names) == 0 {
		return nil, ErrNoRepository
	}
	for _, name := range names {
		raw, err := store.Get(ctx, path.Join("keys", name))
		if err != nil {
			return nil, fmt.Errorf("read key file %s: %w", name, err)
		}
		master, err := masterKey(password, raw)
		if errors.Is(err, errWrongKey) {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("key file %s: %w", name, err)
		}
		return &Repository{store: store, master: master}, nil
	}
	return nil, fmt.Errorf("no key file opens with this password")
}

// snapshotJSON holds the fields of a snapshot document this package reads.
type snapshotJSON struct {
	Time     time.Time `json:"time"`
	Paths    []string  `json:"paths"`
	Tags     []string  `json:"tags"`
	Original string    `json:"original"`
}

// snapshotFile is one snapshot document: what this package reads from it, and
// every field as written, which a rewrite carries over.
type snapshotFile struct {
	snapshot Snapshot
	fields   map[string]json.RawMessage
}

// Snapshots returns every snapshot in the repository, oldest first.
func (r *Repository) Snapshots(ctx context.Context) ([]Snapshot, error) {
	files, err := r.snapshotFiles(ctx)
	if err != nil {
		return nil, err
	}
	snapshots := make([]Snapshot, 0, len(files))
	for _, f := range files {
		snapshots = append(snapshots, f.snapshot)
	}
	return snapshots, nil
}

// snapshotFiles reads every snapshot document, oldest first.
func (r *Repository) snapshotFiles(ctx context.Context) ([]snapshotFile, error) {
	names, err := r.store.List(ctx, "snapshots")
	if err != nil {
		return nil, fmt.Errorf("list the snapshots: %w", err)
	}

	files := make([]snapshotFile, 0, len(names))
	for _, name := range names {
		if !isID(name) {
			continue
		}
		document, err := r.load(ctx, path.Join("snapshots", name))
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", name, err)
		}
		f, err := parseSnapshot(name, document)
		if err != nil {
			return nil, fmt.Errorf("decode snapshot %s: %w", name, err)
		}
		files = append(files, f)
	}

	sort.Slice(files, func(i, j int) bool { return files[i].snapshot.Time.Before(files[j].snapshot.Time) })
	return files, nil
}

func parseSnapshot(id string, document []byte) (snapshotFile, error) {
	var doc snapshotJSON
	if err := json.Unmarshal(document, &doc); err != nil {
		return snapshotFile{}, err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(document, &fields); err != nil {
		return snapshotFile{}, err
	}
	return snapshotFile{
		snapshot: Snapshot{ID: id, Time: doc.Time, Paths: doc.Paths, Tags: doc.Tags, Original: doc.Original},
		fields:   fields,
	}, nil
}

// load reads, decrypts and unpacks one file.
func (r *Repository) load(ctx context.Context, name string) ([]byte, error) {
	sealed, err := r.store.Get(ctx, name)
	if err != nil {
		return nil, err
	}
	plain, err := r.master.decrypt(sealed)
	if err != nil {
		return nil, fmt.Errorf("decrypt: %w", err)
	}
	return unpack(plain)
}

// save encrypts a JSON document into dir under its storage ID, the SHA-256 of
// the encrypted file, and returns that ID. The document goes in uncompressed,
// which restic reads in repository versions 1 and 2 alike.
func (r *Repository) save(ctx context.Context, dir string, document []byte) (string, error) {
	sealed, err := r.master.seal(document)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(sealed)
	id := hex.EncodeToString(sum[:])
	if err := r.store.Put(ctx, path.Join(dir, id), sealed); err != nil {
		return "", fmt.Errorf("write %s/%s: %w", dir, id, err)
	}
	return id, nil
}

// isID reports whether a file name is a storage ID, 64 hex digits.
func isID(name string) bool {
	_, err := hex.DecodeString(name)
	return err == nil && len(name) == 64
}

// AtOrBefore returns the newest snapshot taken at or before t, which is the
// one a VolSync restore with restoreAsOf t selects, and false when none is.
func AtOrBefore(snapshots []Snapshot, t time.Time) (Snapshot, bool) {
	var found Snapshot
	ok := false
	for _, s := range snapshots {
		if s.Time.After(t) {
			continue
		}
		if !ok || s.Time.After(found.Time) {
			found, ok = s, true
		}
	}
	return found, ok
}

// ByShortID returns the snapshot whose ID starts with short.
func ByShortID(snapshots []Snapshot, short string) (Snapshot, bool) {
	for _, s := range snapshots {
		if short != "" && strings.HasPrefix(s.ID, short) {
			return s, true
		}
	}
	return Snapshot{}, false
}

// DirStore keeps a repository in a local directory. The tests use it on a
// fixture restic wrote.
type DirStore string

// List returns the file names under dir. restic's local backend spreads data
// files over subdirectories; the directories this package lists hold files.
func (d DirStore) List(_ context.Context, dir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(string(d), dir))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	return names, nil
}

// Get reads one file.
func (d DirStore) Get(_ context.Context, name string) ([]byte, error) {
	return os.ReadFile(filepath.Join(string(d), name))
}

// Put writes one file, creating its directory when it is missing.
func (d DirStore) Put(_ context.Context, name string, data []byte) error {
	full := filepath.Join(string(d), name)
	if err := os.MkdirAll(filepath.Dir(full), 0o700); err != nil {
		return err
	}
	return os.WriteFile(full, data, 0o600)
}

// Remove deletes one file.
func (d DirStore) Remove(_ context.Context, name string) error {
	return os.Remove(filepath.Join(string(d), name))
}
