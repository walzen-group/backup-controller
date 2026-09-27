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

// QuiescedTag is the restic tag that a quiesced BackupRun adds to each snapshot
// it moves to its restartedAt time, the moment it gave the stopped workloads
// their replicas back. A RestoreRun with syncDatabaseToVolume restores a volume
// only from a snapshot that carries this tag.
const QuiescedTag = "quiesced"

// lockCheckDelay and staleLockAge are the two timings restic's lock protocol
// uses, taken from internal/restic/lock.go in restic 0.18.1. lockCheckDelay is
// how long restic waits after it writes its lock before it checks again for
// other locks. staleLockAge is the age at which restic calls a lock stale. A
// running restic refreshes its lock every five minutes, so a lock older than
// staleLockAge belongs to a process that is gone. lockCheckDelay is a variable
// so the tests can set it to zero.
//
// unlockAttempts and unlockRetryDelay are the controller's own: how often it
// tries to remove its lock, and how long it waits between tries. A lock it
// leaves blocks every VolSync backup and prune until a later Retime removes it.
var (
	lockCheckDelay   = 200 * time.Millisecond
	staleLockAge     = 30 * time.Minute
	unlockAttempts   = 3
	unlockRetryDelay = time.Second
)

// lockUser is the username the controller writes into its lock files. restic
// prints it when it reports that the repository is locked.
const lockUser = "backup-controller"

// lockJSON is the content of one lock file under locks/, in the JSON form
// restic writes and reads.
type lockJSON struct {
	Time      time.Time `json:"time"`
	Exclusive bool      `json:"exclusive"`
	Hostname  string    `json:"hostname"`
	Username  string    `json:"username,omitempty"`
	PID       int       `json:"pid"`
}

// LockedError reports a lock that another process holds on the repository.
// Retime needs restic's exclusive lock, and it can't take that lock while any
// other lock is in place. So it returns this error and changes nothing, and a
// BackupRun tries the rewrite again on a later reconcile.
type LockedError struct {
	// Hostname is the host named in the other lock. For a VolSync mover it's
	// the mover pod's name.
	Hostname string
	// Time is the time written in the other lock.
	Time time.Time
	// Exclusive is true for an exclusive lock and false for a shared one.
	Exclusive bool
}

// Error names the host that holds the lock, the kind of lock, and the time in
// it.
func (e *LockedError) Error() string {
	kind := "a shared"
	if e.Exclusive {
		kind = "an exclusive"
	}
	return fmt.Sprintf("%s has held %s lock on the repository since %s", e.Hostname, kind, e.Time.UTC().Format(time.RFC3339))
}

// Retime changes the time of one snapshot and adds a tag to it, so that a
// volume snapshot carries the same time as the database state it belongs with.
//
// Parameters:
//   - ctx cancels the wait for the lock and the calls to the storage backend.
//   - id is the snapshot's ID. A BackupRun passes the full ID it recorded
//     when it found the snapshot its sync wrote in the repository. The start
//     of an ID works too: Retime then takes the first snapshot whose ID
//     starts with it.
//   - at is the time the snapshot should carry. A BackupRun passes its
//     restartedAt: the volume didn't change between the last pod stopping and
//     that moment, so a database recovered to that time matches the files.
//     Retime drops any fraction of a second from it: VolSync compares
//     snapshot times in whole seconds, a BackupRun reports whole seconds,
//     and a retry that passes the time without its fraction must find the
//     copy an earlier call wrote.
//   - tag is added to the snapshot's tags. A BackupRun passes QuiescedTag, which
//     is how a RestoreRun with syncDatabaseToVolume finds the snapshots it can
//     recover a database alongside.
//
// It returns the snapshot as written, with its new ID. When another process
// holds a lock on the repository, it returns a *LockedError and changes
// nothing, and the caller tries again later.
//
// restic names each snapshot file after the SHA-256 of its encrypted contents,
// so a snapshot can't keep its ID once its time changes. Retime therefore
// writes a copy with the new time and tag, then deletes the old snapshot file.
// The copy points at the same tree, so no backed-up data is removed, and it
// records the old ID in its original field. This is the same change restic
// rewrite --forget --new-time makes.
//
// Retime holds restic's exclusive lock while it writes, so it never runs at the
// same time as a mover. If the snapshot was already retimed, Retime finds the
// copy through its original field and returns it. A controller that restarts
// before it records the new ID gets the same snapshot back.
func (r *Repository) Retime(ctx context.Context, id string, at time.Time, tag string) (Snapshot, error) {
	if id == "" {
		return Snapshot{}, errors.New("no snapshot named")
	}
	unlock, err := r.lockExclusive(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	written, err := r.retime(ctx, id, at.Truncate(time.Second), tag)
	if unlockErr := unlock(); unlockErr != nil {
		return Snapshot{}, errors.Join(err, fmt.Errorf("remove the controller's lock: %w", unlockErr))
	}
	return written, err
}

// retime does the work of Retime once the exclusive lock is held.
//
// Parameters:
//   - id is the snapshot's ID, or its start, as Retime got it.
//   - at is the time the copy carries, already without a fraction of a
//     second.
//   - tag is the tag the copy carries.
//
// It returns the copy, and an error when the repository holds neither the
// snapshot nor a copy of it, or when a read, write or removal fails.
//
// It looks for the snapshot whose ID starts with the given ID. If none does,
// the snapshot may have been rewritten already, so retime looks for a
// snapshot whose original field starts with that ID and which carries the
// tag, and returns it without writing anything.
//
// To rewrite a snapshot, it copies every field of the old snapshot document,
// sets the time field to the new time, and adds the tag to the tags when it
// isn't there yet. It records the old ID in the original field, unless the
// old snapshot was itself a rewrite and already has one. It saves the copy
// under its new ID and then removes the old snapshot file. If that removal
// fails, the copy stays in the repository and the error names both IDs.
//
// When the old snapshot is still there next to a copy an earlier call wrote,
// that earlier call failed to remove it. retime then removes the old file and
// returns that copy. It matches the copy's time to the second, so a copy that
// an older controller stamped with a fraction of a second still counts.
// Writing the copy again would add a second snapshot under another ID,
// because every encryption uses a fresh random IV.
func (r *Repository) retime(ctx context.Context, id string, at time.Time, tag string) (Snapshot, error) {
	files, err := r.snapshotFiles(ctx)
	if err != nil {
		return Snapshot{}, err
	}
	old := snapshotWithID(files, id)
	if old == nil {
		if written, ok := retimedCopy(files, id, tag); ok {
			return written, nil
		}
		return Snapshot{}, fmt.Errorf("the repository holds no snapshot %s", id)
	}
	if written, ok := earlierCopy(files, old.snapshot, at, tag); ok {
		if err := r.store.Remove(ctx, path.Join("snapshots", old.snapshot.ID)); err != nil {
			return Snapshot{}, fmt.Errorf("remove snapshot %s, already written as %s: %w", old.snapshot.ID, written.ID, err)
		}
		return written, nil
	}
	document, err := retimedDocument(old, at, tag)
	if err != nil {
		return Snapshot{}, err
	}
	newID, err := r.save(ctx, "snapshots", document)
	if err != nil {
		return Snapshot{}, err
	}
	if err := r.store.Remove(ctx, path.Join("snapshots", old.snapshot.ID)); err != nil {
		return Snapshot{}, fmt.Errorf("remove snapshot %s after writing %s: %w", old.snapshot.ID, newID, err)
	}
	written, err := parseSnapshot(newID, document)
	if err != nil {
		return Snapshot{}, err
	}
	return written.snapshot, nil
}

// snapshotWithID returns the first snapshot file whose ID starts with id, or
// nil when no snapshot ID starts with it.
func snapshotWithID(files []snapshotFile, id string) *snapshotFile {
	for i := range files {
		if strings.HasPrefix(files[i].snapshot.ID, id) {
			return &files[i]
		}
	}
	return nil
}

// retimedCopy finds a copy that an earlier retime wrote and whose old
// snapshot is gone.
//
// Parameters:
//   - id is the old snapshot's ID, or its start, as Retime got it.
//   - tag is the tag that the copy carries.
//
// It returns the first snapshot whose original field starts with id and that
// carries tag, and true. It returns false when no snapshot is such a copy.
func retimedCopy(files []snapshotFile, id, tag string) (Snapshot, bool) {
	for _, f := range files {
		if strings.HasPrefix(f.snapshot.Original, id) && slices.Contains(f.snapshot.Tags, tag) {
			return f.snapshot, true
		}
	}
	return Snapshot{}, false
}

// earlierCopy finds a copy of old that an earlier retime wrote but did not
// remove old after.
//
// Parameters:
//   - old is the snapshot that retime rewrites.
//   - at is the time that the copy carries.
//   - tag is the tag that the copy carries.
//
// It returns the copy and true, or false when the repository holds no such
// copy. A copy has another ID, the same origin as old, the time at to the
// second, and the tag.
func earlierCopy(files []snapshotFile, old Snapshot, at time.Time, tag string) (Snapshot, bool) {
	origin := old.Original
	if origin == "" {
		origin = old.ID
	}
	for _, f := range files {
		s := f.snapshot
		if s.ID != old.ID && s.Original == origin && s.Time.Unix() == at.Unix() && slices.Contains(s.Tags, tag) {
			return s, true
		}
	}
	return Snapshot{}, false
}

// retimedDocument makes the snapshot document of the copy that retime writes.
//
// Parameters:
//   - old is the snapshot file that retime rewrites.
//   - at is the time that the copy carries.
//   - tag is the tag that the copy carries.
//
// It returns the JSON document, or an error when a field does not encode.
//
// It copies every field of old, sets the time field to at, and adds tag to
// the tags when it is not there yet. It records the ID of old in the original
// field, unless old already has an original field.
func retimedDocument(old *snapshotFile, at time.Time, tag string) ([]byte, error) {
	fields := make(map[string]json.RawMessage, len(old.fields)+2)
	for k, v := range old.fields {
		fields[k] = v
	}
	var err error
	if fields["time"], err = json.Marshal(at); err != nil {
		return nil, err
	}
	tags := old.snapshot.Tags
	if !slices.Contains(tags, tag) {
		tags = append(slices.Clone(tags), tag)
	}
	if fields["tags"], err = json.Marshal(tags); err != nil {
		return nil, err
	}
	if old.snapshot.Original == "" {
		if fields["original"], err = json.Marshal(old.snapshot.ID); err != nil {
			return nil, err
		}
	}
	return json.Marshal(fields)
}

// lockExclusive takes restic's exclusive lock on the repository, with the same
// steps restic uses. It checks locks/ for locks held by other processes, writes
// its own lock file, waits for lockCheckDelay, and checks again. The second
// check finds a process that wrote its lock at about the same time. The lock
// file carries this host's name, this process's PID and lockUser.
//
// If either check finds another lock, it returns the *LockedError from
// checkLocks. After the second check it removes its own lock first. ctx
// cancels the wait, and the lock is removed then too.
//
// On success it returns a function that removes the lock. That function
// ignores the cancellation of the context, so the lock is removed even after
// the caller's context has ended. It tries unlockAttempts times, with
// unlockRetryDelay between tries, and returns the last error when every try
// fails.
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
		var err error
		for attempt := range unlockAttempts {
			if attempt > 0 {
				time.Sleep(unlockRetryDelay)
			}
			if err = r.store.Remove(context.WithoutCancel(ctx), path.Join("locks", id)); err == nil {
				return nil
			}
		}
		return err
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

// checkLocks reads the lock files under locks/ and returns a *LockedError for
// the first one that blocks an exclusive lock.
//
// Parameters:
//   - own is the ID of the lock file this process has just written, which the
//     check skips. lockExclusive passes "" on its first check, before it has
//     written one.
//   - host and pid are this process's hostname and process ID. A lock that
//     carries both was written by this process earlier and never removed.
//
// Any lock from another process blocks an exclusive lock, whether that lock is
// shared or exclusive. A lock older than staleLockAge is skipped, and so is a
// file whose name isn't a storage ID.
//
// Two kinds of leftover lock are removed. One is a lock that this process left
// behind. The other is a stale lock that carries lockUser, whatever its host
// and PID. Only this controller writes that username, and a controller pod
// that was replaced before it removed its lock leaves one with a host no later
// pod has. restic's prune doesn't skip stale locks, and VolSync runs restic
// unlock only when spec.restic.unlock changes, so a leftover lock would stop
// every prune, and a leftover exclusive lock every backup, until someone
// removed it by hand.
//
// It returns an error when it can't list, read or decode a lock file, or can't
// remove a leftover one.
func (r *Repository) checkLocks(ctx context.Context, own, host string, pid int) error {
	names, err := r.store.List(ctx, "locks")
	if err != nil {
		return fmt.Errorf("list the locks: %w", err)
	}
	for _, name := range names {
		if !isID(name) || name == own {
			continue
		}
		lock, err := r.readLock(ctx, name)
		if err != nil {
			return err
		}
		if err := r.checkLock(ctx, name, lock, host, pid); err != nil {
			return err
		}
	}
	return nil
}

// readLock reads and decodes the lock file with the given name under locks/.
// It returns an error that names the lock when the read or the decode fails.
func (r *Repository) readLock(ctx context.Context, name string) (lockJSON, error) {
	document, err := r.load(ctx, path.Join("locks", name))
	if err != nil {
		return lockJSON{}, fmt.Errorf("lock %s: %w", name, err)
	}
	var lock lockJSON
	if err := json.Unmarshal(document, &lock); err != nil {
		return lockJSON{}, fmt.Errorf("decode lock %s: %w", name, err)
	}
	return lock, nil
}

// checkLock is checkLocks for one lock file.
//
// Parameters:
//   - name is the name of the lock file under locks/.
//   - lock is the content of that file.
//   - host and pid are this process's hostname and process ID, as checkLocks
//     got them.
//
// It removes a lock that this process left behind and a stale lock that
// carries lockUser. It skips any other stale lock. It returns a *LockedError
// for a lock that blocks an exclusive lock, and an error when a removal fails.
func (r *Repository) checkLock(ctx context.Context, name string, lock lockJSON, host string, pid int) error {
	switch {
	case lock.Hostname == host && lock.PID == pid:
		if err := r.store.Remove(ctx, path.Join("locks", name)); err != nil {
			return fmt.Errorf("remove the controller's earlier lock %s: %w", name, err)
		}
	case lock.Username == lockUser && time.Since(lock.Time) > staleLockAge:
		if err := r.store.Remove(ctx, path.Join("locks", name)); err != nil {
			return fmt.Errorf("remove the stale controller lock %s from %s: %w", name, lock.Hostname, err)
		}
	case time.Since(lock.Time) > staleLockAge:
	default:
		return &LockedError{Hostname: lock.Hostname, Time: lock.Time, Exclusive: lock.Exclusive}
	}
	return nil
}
