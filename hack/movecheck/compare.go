package main

import (
	"fmt"
	"io"
	"slices"
	"strings"
)

// difference is the outcome of matching the declarations of two
// directories by key.
type difference struct {
	// changed holds the keys found on both sides with different hashes.
	changed []string
	// same counts the keys found on both sides with equal hashes.
	same int
	// oldOnly counts the keys found on the old side only.
	oldOnly int
	// newOnly holds the keys found on the new side only, sorted.
	newOnly []string
}

// compare fingerprints two directories and prints what differs between
// them.
//
// Parameters:
//   - opts holds the old and the new directory, the rename map applied to
//     the old side, and the names the new side may add.
//   - stdout receives a "changed: <key>" line per differing declaration, an
//     "only in new: <key>" line per unlisted declaration found on the new
//     side only, and an "unused -new entry: <name>" line per -new name that
//     matched none of them.
//   - stderr receives errors and the counts of each kind of match.
//
// It returns 0 when nothing is printed to stdout, 1 when something is, and
// 2 when a directory cannot be read, parsed or resolved.
//
// The new directory is read first, because the rename map spells an
// identifier moved into another package with the import path the new
// directory uses for it.
func compare(opts options, stdout, stderr io.Writer) int {
	oldDir, newDir := opts.dirs[0], opts.dirs[1]
	into, err := readTarget(newDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	oldDecls, err := scan(oldDir, opts.renames, into)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	newDecls, err := scan(newDir, nil, nil)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	diff := match(oldDecls, newDecls)
	_, _ = fmt.Fprintf(stderr, "%d identical, %d changed, %d only in %s, %d only in %s\n",
		diff.same, len(diff.changed), diff.oldOnly, oldDir, len(diff.newOnly), newDir)
	return report(diff, opts.added, stdout)
}

// match pairs the declarations of the old and the new side by key.
//
// Parameters:
//   - oldDecls are the old side's declarations, with the rename map
//     already applied.
//   - newDecls are the new side's declarations.
//
// It returns what matched, what differs and what is found on one side
// only.
func match(oldDecls, newDecls []decl) difference {
	newHash := make(map[string]string, len(newDecls))
	for _, d := range newDecls {
		newHash[d.key] = d.hash
	}
	var diff difference
	for _, d := range oldDecls {
		h, ok := newHash[d.key]
		delete(newHash, d.key)
		switch {
		case !ok:
			diff.oldOnly++
		case h == d.hash:
			diff.same++
		default:
			diff.changed = append(diff.changed, d.key)
		}
	}
	for key := range newHash {
		diff.newOnly = append(diff.newOnly, key)
	}
	slices.Sort(diff.newOnly)
	return diff
}

// report prints the lines of a comparison that fail it.
//
// Parameters:
//   - diff is the comparison's outcome.
//   - added holds the names the -new flag lists, which a declaration found
//     only on the new side may have.
//   - stdout receives the lines described at compare.
//
// It returns 1 when it printed a line and 0 otherwise.
//
// The name of a key is everything after its kind, so "type Result" is
// covered by the entry Result.
func report(diff difference, added map[string]bool, stdout io.Writer) int {
	var lines []string
	for _, key := range diff.changed {
		lines = append(lines, "changed: "+key)
	}
	used := map[string]bool{}
	for _, key := range diff.newOnly {
		_, name, _ := strings.Cut(key, " ")
		if added[name] {
			used[name] = true
			continue
		}
		lines = append(lines, "only in new: "+key)
	}
	for name := range added {
		if !used[name] {
			lines = append(lines, "unused -new entry: "+name)
		}
	}
	slices.Sort(lines)
	for _, line := range lines {
		_, _ = fmt.Fprintln(stdout, line)
	}
	if len(lines) > 0 {
		return 1
	}
	return 0
}
