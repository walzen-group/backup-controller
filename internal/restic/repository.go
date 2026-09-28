// Package restic reads the snapshot list of a restic repository, and rewrites
// a snapshot with a new time.
//
// The controller needs three things from a repository: which snapshots it
// holds, when each was taken, and, after a paused backup, a snapshot moved to
// the time the run started the workloads again. Before a RestoreRun overwrites
// anything, it checks that some snapshot reaches the point in time it asks
// for, and it refuses the restore when none does. A BackupRun reports the time
// on the snapshot it made. This package does all of that by reading and
// writing the repository's files itself, so the controller runs no restic
// binary and pins no image besides VolSync's.
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
	"slices"
	"sort"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

// Store reads and writes the files of one repository. In the cluster it's an
// S3 bucket (S3Store), and in the tests it's a local directory (DirStore).
// File names are paths from the repository's root, such as "keys" or
// "snapshots/<id>".
type Store interface {
	// List returns the names of the files directly under the directory dir.
	// When dir doesn't exist, it returns no names and no error.
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
	// ID is the snapshot's storage ID: the SHA-256 of the encrypted snapshot
	// file, in lower-case hex. restic prints its first eight characters as the
	// short ID.
	ID string
	// Time is the time restic stamped on the snapshot when the backup started,
	// or the time Retime gave it.
	Time time.Time
	// Paths are the directories the snapshot holds.
	Paths []string
	// Tags are the snapshot's restic tags.
	Tags []string
	// Original is the ID of the snapshot this one was rewritten from. It's
	// empty for a snapshot that a backup wrote.
	Original string
}

// ShortID returns the first eight characters of the snapshot's ID, the form
// restic prints. A VolSync mover logs it in a line such as
// "snapshot 6e473100 saved".
func (s Snapshot) ShortID() string {
	if len(s.ID) < 8 {
		return s.ID
	}
	return s.ID[:8]
}

// Repository is an opened repository. It keeps the store that holds the files
// and the master key that decrypts them.
type Repository struct {
	store  Store
	master key
}

// ErrNoRepository means the location holds no key files. That's how the
// location of a claim that has never been backed up looks, because VolSync
// initialises the repository on the first backup.
var ErrNoRepository = errors.New("no restic repository at this location yet")

// Open opens the repository in a store with the given password. It tries each
// key file under keys/ and keeps the master key from the first one that the
// password opens.
//
// It returns ErrNoRepository when keys/ holds no files. It returns an error
// when no key file opens with the password, or when a key file can't be read
// or decoded.
func Open(ctx context.Context, store Store, password string) (*Repository, error) {
	names, err := store.List(ctx, "keys")
	if err != nil {
		return nil, fmt.Errorf("list the key files: %w", err)
	}
	if len(names) == 0 {
		return nil, ErrNoRepository
	}
	master, err := openKeys(ctx, store, password, names)
	if err != nil {
		return nil, err
	}
	return &Repository{store: store, master: master}, nil
}

// snapshotJSON holds the fields of a snapshot document that this package reads.
type snapshotJSON struct {
	Time     time.Time `json:"time"`
	Paths    []string  `json:"paths"`
	Tags     []string  `json:"tags"`
	Original string    `json:"original"`
}

// snapshotFile is one snapshot document. It keeps the fields this package reads
// as a Snapshot, and every field as written, so that a rewrite can carry over
// the fields it doesn't change.
type snapshotFile struct {
	snapshot Snapshot
	fields   map[string]json.RawMessage
}

// Snapshots returns every snapshot in the repository, sorted by time with the
// oldest first.
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

// snapshotReads is how many snapshot documents snapshotFiles reads from the
// store at once. Each read is one small GET, so a repository with many
// snapshots is listed in a fraction of the time a sequential read needs,
// which keeps the webhook well inside its timeout.
const snapshotReads = 8

// snapshotFiles reads, decrypts and decodes every snapshot document under
// snapshots/, and returns them sorted by time with the oldest first. It skips
// files whose names aren't storage IDs. It reads up to snapshotReads
// documents at once, and returns the first error.
func (r *Repository) snapshotFiles(ctx context.Context) ([]snapshotFile, error) {
	names, err := r.store.List(ctx, "snapshots")
	if err != nil {
		return nil, fmt.Errorf("list the snapshots: %w", err)
	}
	names = slices.DeleteFunc(names, func(name string) bool { return !isID(name) })

	files := make([]snapshotFile, len(names))
	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(snapshotReads)
	for i, name := range names {
		group.Go(func() error {
			document, err := r.load(groupCtx, path.Join("snapshots", name))
			if err != nil {
				return fmt.Errorf("snapshot %s: %w", name, err)
			}
			f, err := parseSnapshot(name, document)
			if err != nil {
				return fmt.Errorf("decode snapshot %s: %w", name, err)
			}
			files[i] = f
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, err
	}

	sort.Slice(files, func(i, j int) bool { return files[i].snapshot.Time.Before(files[j].snapshot.Time) })
	return files, nil
}

// parseSnapshot decodes a decrypted snapshot document stored under the given
// ID. It reads the fields a Snapshot needs, and keeps every field as raw JSON
// for a rewrite.
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

// load reads one file of the repository, decrypts it with the master key, and
// removes its encoding header.
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

// save encrypts a JSON document and writes it into the directory dir under its
// storage ID, which is the SHA-256 of the encrypted file. It returns that ID.
// The document is stored uncompressed, which restic reads in both repository
// version 1 and version 2.
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

// isID reports whether a file name is a storage ID, which is 64 hex digits.
func isID(name string) bool {
	_, err := hex.DecodeString(name)
	return err == nil && len(name) == 64
}

// AtOrBefore returns the newest snapshot taken at or before the time t, and
// false when every snapshot is later. A VolSync restore whose restoreAsOf is t
// picks the same snapshot, so the controller uses AtOrBefore to check a
// restore before it starts one.
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

// ByShortID returns the first snapshot whose ID starts with the prefix in
// short, and false when none does or the prefix is empty. A BackupRun passes
// the short ID that a mover logged.
func ByShortID(snapshots []Snapshot, short string) (Snapshot, bool) {
	for _, s := range snapshots {
		if short != "" && strings.HasPrefix(s.ID, short) {
			return s, true
		}
	}
	return Snapshot{}, false
}

// DirStore keeps a repository in the local directory whose path is its value.
// The tests use it on a fixture that restic wrote.
type DirStore string

// List returns the names of the files directly under dir, and leaves out
// subdirectories. restic's local backend spreads the data files over
// subdirectories, but the directories this package lists (keys, snapshots and
// locks) hold only files. When dir doesn't exist, List returns no names and no
// error.
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

// Put writes one file, and creates its directory when it's missing.
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

// SyncedSnapshot finds the snapshot that one VolSync sync wrote.
//
// Parameters:
//   - snapshots are the repository's snapshots, in any order.
//   - end is the sync's end, from the ReplicationSource's status.lastSyncTime.
//   - took is the sync's length, from status.lastSyncDuration.
//
// VolSync records both only when a sync completes, and the mover starts
// restic inside that sync, so restic stamps the snapshot with a time between
// the sync's start and its end. SyncedSnapshot returns the newest snapshot in
// that window and true. A mover that VolSync ran again within one sync can
// leave two snapshots in the window; both hold the same clone, and the newest
// is the one the successful mover saved. It returns false when the window
// holds no snapshot: the volume held no files, and VolSync's mover skipped
// the backup.
func SyncedSnapshot(snapshots []Snapshot, end time.Time, took time.Duration) (Snapshot, bool) {
	// restic writes whole seconds of precision and more; the status holds
	// whole seconds. One second on each side keeps the window from cutting
	// off a snapshot at its edge.
	start := end.Add(-took).Add(-time.Second)
	end = end.Add(time.Second)
	var found Snapshot
	ok := false
	for _, s := range snapshots {
		if s.Time.Before(start) || s.Time.After(end) {
			continue
		}
		if !ok || s.Time.After(found.Time) {
			found, ok = s, true
		}
	}
	return found, ok
}

// BaseBackupTag is the tag a BackupRun adds to each snapshot it moves to its
// resume moment, for one base backup that completed while the app was
// paused. It is "base-backup/<cluster>=<backup ID>", such as
// "base-backup/db=20260928T093012".
func BaseBackupTag(cluster, backupID string) string {
	return baseBackupTagPrefix + cluster + "=" + backupID
}

// baseBackupTagPrefix starts every BaseBackupTag.
const baseBackupTagPrefix = "base-backup/"

// HibernatedTag is the tag a BackupRun adds to each snapshot it moves to its
// resume moment, for one Cluster that was hibernated during the run and so
// got no base backup. It is "hibernated/<cluster>", such as
// "hibernated/db". A hibernated database writes nothing, so the end of its
// archive holds its state at that moment, until it runs again.
func HibernatedTag(cluster string) string {
	return "hibernated/" + cluster
}

// PausedBaseBackup returns the base backup ID that a snapshot's
// BaseBackupTag gives for a Cluster, and true.
//
// Parameters:
//   - snapshot is a snapshot tagged paused.
//   - cluster is the name of the Cluster.
//
// It returns false when the snapshot carries no such tag, for example when
// the Cluster's Backup completed after the run resumed the app.
func PausedBaseBackup(snapshot Snapshot, cluster string) (string, bool) {
	prefix := baseBackupTagPrefix + cluster + "="
	for _, tag := range snapshot.Tags {
		if id, ok := strings.CutPrefix(tag, prefix); ok && id != "" {
			return id, true
		}
	}
	return "", false
}
