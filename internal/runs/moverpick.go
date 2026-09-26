package runs

import (
	"fmt"
	"regexp"
	"slices"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
)

// unpinnable says why VolSync's mover would not restore a snapshot a
// RestoreRun's checks selected. The run pins the mover to the snapshot's time
// in whole seconds (selectedMoment), and the mover picks again from that
// second, the way restic.MoverPick emulates.
//
// Parameters:
//   - all is every snapshot in the repository, unfiltered, because the mover
//     sees every snapshot whatever its tags.
//   - s is the snapshot the checks selected.
//   - quiescedOnly says the run picked among quiesced snapshots only, as a
//     run with spec.syncDatabaseToVolume does. The advice in the reason then
//     keeps to quiesced snapshots.
//
// It returns "" when the mover, pinned to s's second, restores s. Otherwise it
// returns the reason for the item: which case it is, which snapshot the
// mover would restore where that is known, and what to choose instead. The
// cases are a snapshot the mover does not list, a later snapshot in the same
// second, a snapshot with the same full time, and a repository with a
// snapshot the mover misreads.
func unpinnable(all []restic.Snapshot, s restic.Snapshot, quiescedOnly bool) string {
	id, second := s.ShortID(), s.Time.UTC().Format(time.RFC3339)
	if !restic.MoverLists(s) {
		listed := "none"
		if len(s.Paths) > 0 {
			listed = strings.Join(s.Paths, ", ")
		}
		return fmt.Sprintf("snapshot %s (%s) has no path containing /data (its paths: %s), and VolSync's mover passes over such snapshots, "+
			"so it cannot restore %s. Choose a snapshot a VolSync mover took", id, second, listed, id)
	}
	picked, ok := restic.MoverPick(all, s.Time)
	if ok && picked.ID == s.ID {
		return ""
	}
	if ok {
		if quiescedOnly && !slices.Contains(picked.Tags, restic.QuiescedTag) {
			return fmt.Sprintf("snapshot %s (%s) shares its second with snapshot %s, which is not tagged %s, and VolSync's mover picks by the whole second, "+
				"so it would restore %s. Set restoreAsOf before %s, or previous, to choose a %s snapshot in another second",
				id, second, picked.ShortID(), restic.QuiescedTag, picked.ShortID(), second, restic.QuiescedTag)
		}
		return fmt.Sprintf("snapshot %s (%s) shares its second with snapshot %s, and VolSync's mover picks by the whole second, "+
			"so it would restore %s. Choose %s or a snapshot in another second", id, second, picked.ShortID(), picked.ShortID(), picked.ShortID())
	}
	for _, other := range all {
		if restic.MoverMisreads(other) {
			return fmt.Sprintf("snapshot %s lists /data after its first path (its paths: %s), which VolSync's mover misreads as a snapshot of its own "+
				"dated the day the mover runs, so the run cannot tell which snapshot the mover would restore from this repository. "+
				"VolSync writes snapshots with /data as their only path; restore from a repository that holds only those",
				other.ShortID(), strings.Join(other.Paths, ", "))
		}
	}
	// What is left is a tie: two or more snapshots the mover lists share the
	// greatest full time in s's second.
	var last []restic.Snapshot
	for _, other := range all {
		if !restic.MoverLists(other) || other.Time.Unix() != s.Time.Unix() {
			continue
		}
		switch {
		case len(last) == 0 || other.Time.After(last[0].Time):
			last = []restic.Snapshot{other}
		case other.Time.Equal(last[0].Time):
			last = append(last, other)
		}
	}
	var names []string
	mine := false
	for _, other := range last {
		if other.ID == s.ID {
			mine = true
			continue
		}
		names = append(names, other.ShortID())
	}
	which := "either"
	if len(last) > 2 {
		which = "any of them"
	}
	switch {
	case len(names) == 0:
		return fmt.Sprintf("VolSync's mover, pinned to %s, would not restore snapshot %s, and the run cannot tell what it would restore. "+
			"Choose a snapshot in another second", second, id)
	case mine:
		return fmt.Sprintf("snapshot %s (%s) has the same time as snapshot %s, to the nanosecond, and restic lists such snapshots in no set order, "+
			"so VolSync's mover may restore %s. Choose a snapshot in another second", id, second, strings.Join(names, " and snapshot "), which)
	default:
		return fmt.Sprintf("snapshot %s (%s) shares its second with snapshot %s, taken later in that second at the same time to the nanosecond, "+
			"and VolSync's mover picks by the whole second, so it may restore %s. Choose a snapshot in another second",
			id, second, strings.Join(names, " and snapshot "), which)
	}
}

// changedSince checks, against the repository as it is listed now, the
// snapshot a RestoreRun's checks recorded on a volume item, and says why
// VolSync's mover would no longer restore it.
//
// Parameters:
//   - all is every snapshot in the repository now, unfiltered.
//   - item is the volume item. item.Snapshot is the recorded short ID, and
//     item.SnapshotTime the time the run pins the mover to.
//   - quiescedOnly is passed on to unpinnable for its advice.
//
// It returns the listed snapshot with the recorded short ID, and "" when the
// mover, pinned to the item's time, restores it. Otherwise it returns the
// reason, which says what happened after the checks:
//
//   - the recorded ID is gone and a snapshot whose original field starts
//     with it is listed: a retime (or restic rewrite) wrote it again under
//     another ID and time. The run does not follow the copy: the run
//     recorded no tree to compare, and a rewrite can change the tree.
//   - the recorded ID is gone otherwise: a backup's restic forget removed it.
//   - the snapshot is there, and the mover would pick another one in its
//     second, or can't be predicted (see unpinnable).
//
// An item with no recorded snapshot, or no recorded snapshot time, gets a
// reason too, since the run can't pin the mover to anything. Only an older
// release planned such an item. It does not change all.
func changedSince(all []restic.Snapshot, item backupv1alpha1.RestoreItem, quiescedOnly bool) (restic.Snapshot, string) {
	id := item.Snapshot
	if id == "" {
		return restic.Snapshot{}, "the item records no snapshot, so the run can't pin the mover to one"
	}
	if item.SnapshotTime == nil {
		return restic.Snapshot{}, fmt.Sprintf("the item records no snapshot time for snapshot %s, so the run can't pin the mover to it", id)
	}
	recorded, ok := restic.ByShortID(all, id)
	if !ok {
		for _, s := range all {
			if s.Original != "" && strings.HasPrefix(s.Original, id) {
				return restic.Snapshot{}, fmt.Sprintf("snapshot %s, which the checks selected, was rewritten as %s at %s by a quiesced backup after the checks",
					id, s.ShortID(), s.Time.UTC().Format(time.RFC3339))
			}
		}
		return restic.Snapshot{}, fmt.Sprintf("snapshot %s (%s), which the checks selected, is no longer in the repository; "+
			"a backup's retention (restic forget) removed it after the checks", id, item.SnapshotTime.UTC().Format(time.RFC3339))
	}
	pin := item.SnapshotTime.Time
	if picked, ok := restic.MoverPick(all, pin); ok && picked.ID == recorded.ID {
		return recorded, ""
	}
	if why := unpinnable(all, recorded, quiescedOnly); why != "" {
		return recorded, "the repository changed after the checks: " + why
	}
	// The snapshot is still the mover's pick for its own second, so the
	// item's time points at another second.
	return recorded, fmt.Sprintf("snapshot %s is from %s, and the item pins the mover to %s, where it would not restore it",
		id, recorded.Time.UTC().Format(time.RFC3339), pin.UTC().Format(time.RFC3339))
}

// restoringSnapshot matches the line restic restore prints as it starts,
// such as "restoring snapshot 6526a2ff of [/data] at 2026-09-26
// 09:03:53.753460659 +0000 UTC by root@volsync to .", and captures the
// snapshot's ID. restic 0.18.1 prints it at cmd/restic/cmd_restore.go:243
// through the snapshot's String (internal/restic/snapshot.go:118-120), which
// gives the short ID. The "restoring <Snapshot 1a2b3c4d of ...>" form of
// restic before 0.17 matches too. VolSync 0.16.0 keeps the line in a
// successful mover's log (mover/restic/logfilter.go:34), and the e2e test
// TestRestoreMoverLogNamesTheSnapshot recorded it from a real mover.
var restoringSnapshot = regexp.MustCompile(`(?m)^\s*restoring <?[Ss]napshot ([0-9a-f]{8,64}) of`)

// noEligibleSnapshots matches the line VolSync's restore mover prints when
// no snapshot is at or before its restoreAsOf (mover-restic/entry.sh:339-341
// at v0.16.0). The mover then restores nothing and exits 0, and VolSync
// completes the trigger. The log filter keeps the line (logfilter.go:29).
var noEligibleSnapshots = regexp.MustCompile(`(?m)^\s*No eligible snapshots found`)

// restoredSnapshots reads the filtered log of a restore mover, as VolSync
// writes it to status.latestMoverStatus.logs.
//
// It returns the ID of every snapshot the log says restic restored, in the
// order of the lines, and reports whether the log says the mover found no
// snapshot to restore.
func restoredSnapshots(logs string) (ids []string, noneEligible bool) {
	for _, match := range restoringSnapshot.FindAllStringSubmatch(logs, -1) {
		ids = append(ids, match[1])
	}
	return ids, noEligibleSnapshots.MatchString(logs)
}

// unconfirmedRestore checks the log of a restore mover that completed the
// run's trigger against the snapshot the run's checks recorded.
//
// Parameters:
//   - destination is the item's ReplicationDestination. VolSync writes the
//     mover's filtered log to status.latestMoverStatus in the same status
//     update that sets lastManualSync (mover/restic/mover.go:648-650), so the
//     log belongs to the sync that completed the trigger.
//   - recorded is the item's snapshot, the short ID the checks recorded.
//   - claim is the claim the mover wrote into, for the message.
//   - created says the run created that claim empty for this restore, which
//     changes what the message says the claim holds after a mover that
//     restored nothing.
//
// It returns "" when the log names the recorded snapshot, and otherwise the
// item's failure message.
//
// VolSync completes the trigger whatever the mover restored, so the log is
// the only evidence of what the claim holds. The item counts as restored
// only when the log names the recorded snapshot, names no other, and does not
// say the mover found none. It fails when the log says the mover found no
// snapshot (the mover exits 0 then), when it names another snapshot, when it
// says both, and when it names none: VolSync leaves the log empty when it can't read the
// pod's logs, and keeps only its last MOVER_LOG_MAX_BYTES bytes (1024 by
// default, utils/podlogs.go:40, :186, :213-227), which can cut the line. A
// restic that prints the line in another form fails the same way. An item
// whose recorded short ID is empty or shorter than restic's eight characters
// fails too, since no log can confirm it.
func unconfirmedRestore(destination *volsyncv1alpha1.ReplicationDestination, recorded, claim string, created bool) string {
	logs := ""
	if destination.Status != nil && destination.Status.LatestMoverStatus != nil {
		logs = destination.Status.LatestMoverStatus.Logs
	}
	if len(recorded) < 8 {
		return fmt.Sprintf("the item records no snapshot to compare the mover's log with, so the run cannot confirm what claim %s holds. Logs: %s",
			claim, logs)
	}
	ids, noneEligible := restoredSnapshots(logs)
	var others []string
	for _, id := range ids {
		same := strings.HasPrefix(id, recorded) || strings.HasPrefix(recorded, id)
		if !same && !slices.Contains(others, id) {
			others = append(others, id)
		}
	}
	switch {
	case len(others) > 0:
		other := strings.Join(others, " and ")
		return fmt.Sprintf("the mover restored snapshot %s where the checks selected %s; claim %s now holds %s",
			other, recorded, claim, other)
	case len(ids) > 0 && !noneEligible:
		return ""
	case len(ids) > 0:
		return fmt.Sprintf("the mover's logs name snapshot %s and also say it found no snapshot, so the run cannot confirm what claim %s holds. Logs: %s",
			ids[0], claim, logs)
	case noneEligible:
		pin := "in the repository"
		if destination.Spec.Restic != nil && destination.Spec.Restic.RestoreAsOf != nil {
			pin = "at or before " + *destination.Spec.Restic.RestoreAsOf
		}
		holds := "holds what it held before"
		if created {
			holds = "is empty"
		}
		return fmt.Sprintf("the mover found no snapshot %s and wrote nothing; claim %s %s", pin, claim, holds)
	}
	return fmt.Sprintf("the mover finished, but its logs name no snapshot, so the run cannot confirm what claim %s holds. "+
		"VolSync keeps only the last MOVER_LOG_MAX_BYTES bytes (1024 by default) of the filtered log. Logs: %s", claim, logs)
}
