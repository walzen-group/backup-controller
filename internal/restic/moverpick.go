package restic

import (
	"strings"
	"time"
)

// This file emulates how VolSync's restic mover picks the snapshot a restore
// writes. The source is select_restic_snapshot_to_restore in VolSync v0.16.0
// mover-restic/entry.sh, lines 265 to 320, which reads the table that restic
// 0.18.1 snapshots prints (cmd/restic/cmd_snapshots.go, PrintSnapshots,
// lines 168 to 278):
//
//   - The table has one row per snapshot, oldest first by full time
//     (cmd_snapshots.go:185). That sort is stable, but the list reaches it
//     from sort.Sort (cmd_snapshots.go:100, whose Less in
//     internal/restic/snapshot.go:260 compares only the time), which is not,
//     so two snapshots with the same time come out in no set order.
//   - A row's first line holds the short ID, the time in whole seconds, the
//     host, the tags joined by commas and the first path. Every further path
//     goes on a line of its own below it.
//   - entry.sh keeps each line that contains /data anywhere (entry.sh:284),
//     reads its first field as the ID and its second and third as the time,
//     and maps each whole-second epoch to the ID of the last line with that
//     epoch (entry.sh:285-291).
//   - It takes the newest epoch at or before RESTORE_AS_OF, then steps back
//     SELECT_PREVIOUS lines (entry.sh:295-319). A RestoreRun hands the mover a
//     snapshot's own second and no previous, so this emulation covers
//     SELECT_PREVIOUS 0.

// moverPattern is the text entry.sh greps the snapshot table for.
const moverPattern = "/data"

// MoverLists reports whether VolSync's mover sees the snapshot s at all:
// whether the first line of its row in restic's snapshot table contains
// /data. That line holds the host, the tags joined by commas and the first
// path. A snapshot whose first path lacks /data is passed over, unless its
// host or a tag holds /data.
func MoverLists(s Snapshot) bool {
	if strings.Contains(s.Hostname, moverPattern) || strings.Contains(strings.Join(s.Tags, ","), moverPattern) {
		return true
	}
	return len(s.Paths) > 0 && strings.Contains(s.Paths[0], moverPattern)
}

// MoverMisreads reports whether restic's snapshot table puts /data on a line
// of its own for the snapshot s: a path after the first one contains it. The
// mover keeps that line and reads the path as a snapshot ID with an empty
// time, which GNU date turns into midnight of the day the mover runs
// (entry.sh:215-219). What the mover then picks depends on that day, so it
// can't be predicted.
func MoverMisreads(s Snapshot) bool {
	for _, p := range s.Paths[min(1, len(s.Paths)):] {
		if strings.Contains(p, moverPattern) {
			return true
		}
	}
	return false
}

// MoverPick returns the snapshot VolSync's restic mover restores when it is
// given pin as restoreAsOf and no previous.
//
// Parameters:
//   - snapshots is every snapshot in the repository, in any order. The mover
//     lists them all, whatever their host or tags.
//   - pin is the restoreAsOf. Only its whole seconds count.
//
// It returns false when the mover restores nothing, because no snapshot it
// lists is at or before pin, and also when the pick can't be predicted:
// the newest second in reach holds two snapshots whose full times are equal
// and greatest in that second, which restic lists in no set order, or a
// snapshot in the repository is one MoverMisreads.
//
// It works as entry.sh does: it keeps the snapshots MoverLists accepts,
// groups them by their time in whole seconds, and returns the one with the
// greatest full time in the newest group at or before pin. That is the last
// one restic lists in that second, which is the one entry.sh's map of epochs
// to IDs keeps. It does not change snapshots.
func MoverPick(snapshots []Snapshot, pin time.Time) (Snapshot, bool) {
	var picked Snapshot
	found, tied := false, false
	for _, s := range snapshots {
		if MoverMisreads(s) {
			return Snapshot{}, false
		}
		if !MoverLists(s) || s.Time.Unix() > pin.Unix() {
			continue
		}
		switch {
		case !found, s.Time.Unix() > picked.Time.Unix(), s.Time.Unix() == picked.Time.Unix() && s.Time.After(picked.Time):
			picked, found, tied = s, true, false
		case s.Time.Equal(picked.Time):
			tied = true
		}
	}
	if !found || tied {
		return Snapshot{}, false
	}
	return picked, true
}
