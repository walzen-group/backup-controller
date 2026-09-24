// Package restic reads the snapshot list of a restic repository.
//
// The controller needs two answers from a repository and nothing more: which
// snapshots it holds, and when each was taken. A RestoreRun refuses a point in
// time no snapshot reaches before it overwrites anything, and a BackupRun
// reports the time restic stamped on the snapshot it made. Both are read here,
// from the repository's own files, so the controller runs no restic binary and
// pins no image beside VolSync's.
package restic

import (
	"context"
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

// Store is where a repository's files are read from: an S3 bucket in the
// cluster, a directory in the tests.
type Store interface {
	// List returns the names of the files directly under dir.
	List(ctx context.Context, dir string) ([]string, error)
	// Get returns one file's content.
	Get(ctx context.Context, name string) ([]byte, error)
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
	Time  time.Time `json:"time"`
	Paths []string  `json:"paths"`
}

// Snapshots returns every snapshot in the repository, oldest first.
func (r *Repository) Snapshots(ctx context.Context) ([]Snapshot, error) {
	names, err := r.store.List(ctx, "snapshots")
	if err != nil {
		return nil, fmt.Errorf("list the snapshots: %w", err)
	}

	snapshots := make([]Snapshot, 0, len(names))
	for _, name := range names {
		if _, err := hex.DecodeString(name); err != nil || len(name) != 64 {
			continue
		}
		sealed, err := r.store.Get(ctx, path.Join("snapshots", name))
		if err != nil {
			return nil, fmt.Errorf("read snapshot %s: %w", name, err)
		}
		plain, err := r.master.decrypt(sealed)
		if err != nil {
			return nil, fmt.Errorf("decrypt snapshot %s: %w", name, err)
		}
		document, err := unpack(plain)
		if err != nil {
			return nil, fmt.Errorf("snapshot %s: %w", name, err)
		}
		var doc snapshotJSON
		if err := json.Unmarshal(document, &doc); err != nil {
			return nil, fmt.Errorf("decode snapshot %s: %w", name, err)
		}
		snapshots = append(snapshots, Snapshot{ID: name, Time: doc.Time, Paths: doc.Paths})
	}

	sort.Slice(snapshots, func(i, j int) bool { return snapshots[i].Time.Before(snapshots[j].Time) })
	return snapshots, nil
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

// DirStore reads a repository from a local directory. The tests use it on a
// fixture restic wrote.
type DirStore string

// List returns the file names under dir. restic's local backend spreads data
// files over subdirectories; the directories this package lists hold files.
func (d DirStore) List(_ context.Context, dir string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(string(d), dir))
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
