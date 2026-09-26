package runs

import (
	"fmt"
	"slices"
	"strings"
	"time"

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
