package runs

import (
	"cmp"
	"fmt"
	"slices"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
)

// syncSkew is how far the clock of the node a mover runs on may differ from
// the clock of VolSync's manager. restic stamps a snapshot with the mover's
// clock, and VolSync records the sync's times with its own, so the window a
// backup looks for its snapshot in is this much wider on each side.
const syncSkew = 5 * time.Second

// syncWindow is the span of time in which restic stamped every snapshot one
// VolSync sync wrote, both ends included.
type syncWindow struct {
	// start is the earliest time such a snapshot can carry.
	start time.Time
	// end is the latest time such a snapshot can carry.
	end time.Time
}

// holds reports whether a snapshot stamped at the given time falls in the
// window, counting both ends.
func (w syncWindow) holds(at time.Time) bool {
	return !at.Before(w.start) && !at.After(w.end)
}

// identifyError says a completed sync left no record of when it ran, so the
// run can't tell which snapshot in the repository the sync wrote. The item
// fails with it and records reason NoMoverSnapshot (see asItemFailure); no
// retry brings the missing time back.
type identifyError struct {
	// source names the ReplicationSource whose status lacks the time.
	source string
	// field names the status field VolSync did not record usably, such as
	// lastSyncTime.
	field string
	// negative is the duration VolSync recorded when it is below zero, and
	// zero when the field is missing.
	negative time.Duration
}

// Error says which field of the source's status is missing or unusable and
// why that keeps the run from finding its snapshot.
func (e *identifyError) Error() string {
	problem := "has no status." + e.field
	if e.negative != 0 {
		problem = fmt.Sprintf("records status.%s as %s, which no sync can take", e.field, e.negative)
	}
	return fmt.Sprintf("ReplicationSource %s completed the run's sync but %s, so VolSync did not record when the sync ran "+
		"and the run can't tell which snapshot in the repository the sync wrote", e.source, problem)
}

// windowOf returns the window in which restic stamped the snapshots of the
// sync a source completed last.
//
// Parameters:
//   - source is the ReplicationSource whose status records the completed
//     sync. The caller has checked that its lastManualSync is the run's
//     trigger, so the sync is the run's.
//
// It returns an *identifyError when the status lacks lastSyncTime or
// lastSyncDuration, or records a negative duration.
//
// VolSync 0.16.0 sets lastSyncTime when the sync completes, and
// lastSyncDuration from the same clock as the time since lastSyncStartTime,
// which it then clears (statemachine/machine.go 196-219). restic stamps each
// snapshot when its backup starts, inside that span. The status keeps
// lastSyncTime in whole seconds, so the sync ended before one more second
// had passed. The window is therefore [lastSyncTime - lastSyncDuration -
// syncSkew, lastSyncTime + 1s + syncSkew].
func windowOf(source *volsyncv1alpha1.ReplicationSource) (syncWindow, error) {
	status := source.Status
	missing := func(field string) error {
		return &identifyError{source: source.Name, field: field}
	}
	switch {
	case status == nil || status.LastSyncTime == nil:
		return syncWindow{}, missing("lastSyncTime")
	case status.LastSyncDuration == nil:
		return syncWindow{}, missing("lastSyncDuration")
	case status.LastSyncDuration.Duration < 0:
		return syncWindow{}, &identifyError{source: source.Name, field: "lastSyncDuration", negative: status.LastSyncDuration.Duration}
	}
	end := status.LastSyncTime.Time
	start := end.Add(-status.LastSyncDuration.Duration)
	return syncWindow{start: start.Add(-syncSkew), end: end.Add(time.Second + syncSkew)}, nil
}

// identified is what identify found of the snapshots one sync wrote.
type identified struct {
	// snapshot is the snapshot the sync completed with. It is the zero
	// Snapshot when found is false.
	snapshot restic.Snapshot
	// found is false when the sync wrote no snapshot.
	found bool
	// others are the sync's other snapshots, newest first: those of mover
	// pods that saved a snapshot and then failed, before the retry that
	// succeeded.
	others []restic.Snapshot
}

// identify picks the snapshot a sync wrote from a repository's snapshots.
//
// Parameters:
//   - window is the sync's window (see windowOf).
//   - snapshots are the repository's snapshots, in any order.
//   - recorded holds the full IDs that other BackupRuns' items of the same
//     claim recorded. Such a snapshot is theirs, so it is never this sync's.
//
// It returns the newest candidate, by time and then by ID, as the sync's
// snapshot, and the other candidates beside it. It returns found false when
// there is no candidate. A candidate is a snapshot a mover wrote (see
// restic.MoverWritten: host volsync, the one path /data, no original) whose
// time is in the window and whose ID is not in recorded.
//
// VolSync retries a mover pod that fails inside one sync, and a pod that
// saved its snapshot and then failed leaves it, so one sync can write
// several snapshots. The last one is the attempt that succeeded, and restic
// stamps each attempt when it starts, so the newest is that one. The ID
// breaks a tie the same way restic.Repository.Snapshots orders one.
func identify(window syncWindow, snapshots []restic.Snapshot, recorded map[string]bool) identified {
	var candidates []restic.Snapshot
	for _, s := range snapshots {
		if restic.MoverWritten(s) && window.holds(s.Time) && !recorded[s.ID] {
			candidates = append(candidates, s)
		}
	}
	if len(candidates) == 0 {
		return identified{}
	}
	slices.SortFunc(candidates, func(a, b restic.Snapshot) int {
		if c := b.Time.Compare(a.Time); c != 0 {
			return c
		}
		return cmp.Compare(b.ID, a.ID)
	})
	return identified{snapshot: candidates[0], found: true, others: candidates[1:]}
}

// note returns the sentence an item's message carries when the sync left
// other snapshots beside the one it completed with, and "" when it left
// none. It is for a person; no decision reads it.
func (found identified) note() string {
	if len(found.others) == 0 {
		return ""
	}
	ids := make([]string, 0, len(found.others))
	for _, s := range found.others {
		ids = append(ids, s.ShortID())
	}
	noun := "snapshot"
	if len(ids) > 1 {
		noun = "snapshots"
	}
	return fmt.Sprintf("the sync also left %s %s from failed attempts; they stay in the repository until retention forgets them",
		noun, strings.Join(ids, ", "))
}
