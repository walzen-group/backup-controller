package restic

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"
	"time"
)

// QuiescedTag marks a snapshot a quiesced BackupRun moved to the moment its
// workloads were stopped. A restore that recovers the databases to a volume's
// snapshot takes only a snapshot carrying it.
const QuiescedTag = "quiesced"

// The two numbers restic's lock protocol runs on, from internal/restic/lock.go
// in restic 0.18.1: the pause between writing a lock and checking for others,
// and the age past which restic calls a lock stale. A live restic refreshes its
// lock every five minutes, so a lock older than that belongs to a process that
// is gone.
var (
	lockCheckDelay = 200 * time.Millisecond
	staleLockAge   = 30 * time.Minute
)

// lockUser is the username the controller's locks carry, which restic prints
// when it reports the repository locked.
const lockUser = "backup-controller"

// lockJSON is a lock document, as restic writes and reads it.
type lockJSON struct {
	Time      time.Time `json:"time"`
	Exclusive bool      `json:"exclusive"`
	Hostname  string    `json:"hostname"`
	Username  string    `json:"username,omitempty"`
	PID       int       `json:"pid"`
}

// LockedError is another process's lock, which keeps the exclusive lock a
// rewrite needs. The rewrite is tried again once that process lets go.
type LockedError struct {
	Hostname  string
	Time      time.Time
	Exclusive bool
}

func (e *LockedError) Error() string {
	kind := "a shared"
	if e.Exclusive {
		kind = "an exclusive"
	}
	return fmt.Sprintf("%s has held %s lock on the repository since %s", e.Hostname, kind, e.Time.UTC().Format(time.RFC3339))
}

// Retime writes the snapshot whose ID starts with short again, at the time at
// and carrying tag, and removes the original: what restic rewrite --forget
// --new-time does, with the tag restic tag --add would add. The new snapshot
// names the old one as its original, the way restic's rewrite does.
//
// It holds restic's exclusive lock while it writes, so it never runs beside a
// mover. Asked again for a snapshot it already rewrote, it returns the rewrite,
// so a controller that restarts before recording the new ID loses nothing.
func (r *Repository) Retime(ctx context.Context, short string, at time.Time, tag string) (Snapshot, error) {
	if short == "" {
		return Snapshot{}, errors.New("no snapshot named")
	}
	unlock, err := r.lockExclusive(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	written, err := r.retime(ctx, short, at, tag)
	if unlockErr := unlock(); unlockErr != nil {
		return Snapshot{}, errors.Join(err, fmt.Errorf("remove the controller's lock: %w", unlockErr))
	}
	return written, err
}

func (r *Repository) retime(ctx context.Context, short string, at time.Time, tag string) (Snapshot, error) {
	files, err := r.snapshotFiles(ctx)
	if err != nil {
		return Snapshot{}, err
	}

	var old *snapshotFile
	for i := range files {
		if strings.HasPrefix(files[i].snapshot.ID, short) {
			old = &files[i]
			break
		}
	}
	if old == nil {
		for _, f := range files {
			if strings.HasPrefix(f.snapshot.Original, short) && slices.Contains(f.snapshot.Tags, tag) {
				return f.snapshot, nil
			}
		}
		return Snapshot{}, fmt.Errorf("the repository holds no snapshot %s", short)
	}

	fields := make(map[string]json.RawMessage, len(old.fields)+2)
	for k, v := range old.fields {
		fields[k] = v
	}
	if fields["time"], err = json.Marshal(at); err != nil {
		return Snapshot{}, err
	}
	tags := old.snapshot.Tags
	if !slices.Contains(tags, tag) {
		tags = append(slices.Clone(tags), tag)
	}
	if fields["tags"], err = json.Marshal(tags); err != nil {
		return Snapshot{}, err
	}
	if old.snapshot.Original == "" {
		if fields["original"], err = json.Marshal(old.snapshot.ID); err != nil {
			return Snapshot{}, err
		}
	}

	document, err := json.Marshal(fields)
	if err != nil {
		return Snapshot{}, err
	}
	id, err := r.save(ctx, "snapshots", document)
	if err != nil {
		return Snapshot{}, err
	}
	if err := r.store.Remove(ctx, path.Join("snapshots", old.snapshot.ID)); err != nil {
		return Snapshot{}, fmt.Errorf("remove snapshot %s after writing %s: %w", old.snapshot.ID, id, err)
	}
	written, err := parseSnapshot(id, document)
	if err != nil {
		return Snapshot{}, err
	}
	return written.snapshot, nil
}

// lockExclusive takes restic's exclusive lock the way restic does: check for
// other locks, write its own, wait, and check again, giving its own lock back
// when the second check finds another. It returns the function that removes
// the lock.
func (r *Repository) lockExclusive(ctx context.Context) (func() error, error) {
	host, _ := os.Hostname()
	pid := os.Getpid()

	if err := r.checkLocks(ctx, "", host, pid); err != nil {
		return nil, err
	}
	document, err := json.Marshal(lockJSON{Time: time.Now(), Exclusive: true, Hostname: host, Username: lockUser, PID: pid})
	if err != nil {
		return nil, err
	}
	id, err := r.save(ctx, "locks", document)
	if err != nil {
		return nil, err
	}
	unlock := func() error {
		return r.store.Remove(context.WithoutCancel(ctx), path.Join("locks", id))
	}

	select {
	case <-ctx.Done():
		return nil, errors.Join(ctx.Err(), unlock())
	case <-time.After(lockCheckDelay):
	}
	if err := r.checkLocks(ctx, id, host, pid); err != nil {
		return nil, errors.Join(err, unlock())
	}
	return unlock, nil
}

// checkLocks returns a LockedError for the first lock another process holds.
// Every other lock conflicts with an exclusive one, shared or not. A lock
// older than restic's stale age is passed over. A lock carrying this process's
// host and PID, other than own, is one it failed to remove before, and is
// removed now: restic's prune does not pass over stale locks, so one left
// behind would stop every prune until someone ran restic unlock.
func (r *Repository) checkLocks(ctx context.Context, own, host string, pid int) error {
	names, err := r.store.List(ctx, "locks")
	if err != nil {
		return fmt.Errorf("list the locks: %w", err)
	}
	for _, name := range names {
		if !isID(name) || name == own {
			continue
		}
		document, err := r.load(ctx, path.Join("locks", name))
		if err != nil {
			return fmt.Errorf("lock %s: %w", name, err)
		}
		var lock lockJSON
		if err := json.Unmarshal(document, &lock); err != nil {
			return fmt.Errorf("decode lock %s: %w", name, err)
		}
		switch {
		case lock.Hostname == host && lock.PID == pid:
			if err := r.store.Remove(ctx, path.Join("locks", name)); err != nil {
				return fmt.Errorf("remove the controller's earlier lock %s: %w", name, err)
			}
		case time.Since(lock.Time) > staleLockAge:
		default:
			return &LockedError{Hostname: lock.Hostname, Time: lock.Time, Exclusive: lock.Exclusive}
		}
	}
	return nil
}
