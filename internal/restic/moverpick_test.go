package restic

import (
	"context"
	"slices"
	"testing"
	"time"
)

// TestMoverPickMatchesTheMover checks MoverPick against VolSync's mover
// itself. For every RESTORE_AS_OF that hack/fixtures/restic.sh ran entry.sh
// restore with and SELECT_PREVIOUS 0, MoverPick must return the snapshot the
// mover selected, or none when the mover logged "No eligible snapshots
// found". A run with no RESTORE_AS_OF selects the newest second, which
// MoverPick reaches with a pin after every snapshot.
func TestMoverPickMatchesTheMover(t *testing.T) {
	for _, kind := range []string{"timed", "same-second"} {
		for _, f := range fixtures(t, kind) {
			t.Run(f.name, func(t *testing.T) {
				snapshots, err := f.open(t).Snapshots(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				var rows []selectionRow
				f.readJSON(t, "selection.json", &rows)
				checked := 0
				for _, row := range rows {
					if row.SelectPrevious != 0 {
						continue
					}
					pin := snapshots[len(snapshots)-1].Time.Add(24 * time.Hour)
					if row.RestoreAsOf != "" {
						if pin, err = time.Parse(time.RFC3339, row.RestoreAsOf); err != nil {
							t.Fatal(err)
						}
					}
					var got string
					if s, ok := MoverPick(snapshots, pin); ok {
						got = s.ShortID()
					}
					if got != row.Selected {
						t.Errorf("RESTORE_AS_OF %q: MoverPick picks %q, the mover selected %q", row.RestoreAsOf, got, row.Selected)
					}
					checked++
				}
				if checked == 0 {
					t.Fatal("the recording holds no run with SELECT_PREVIOUS 0")
				}
			})
		}
	}
}

// TestMoverPickFollowsEntrySh checks MoverPick against cases taken from the
// selection in VolSync v0.16.0 mover-restic/entry.sh, lines 265 to 320, that
// the recorded repositories do not hold: identical times, snapshots the
// mover's grep for /data keeps or drops, and a snapshot whose second path
// the mover misreads as a snapshot of its own.
func TestMoverPickFollowsEntrySh(t *testing.T) {
	second := time.Date(2026, 9, 25, 20, 10, 21, 0, time.UTC)
	at := func(nanos int) time.Time { return second.Add(time.Duration(nanos)) }
	snap := func(id string, when time.Time, mutate ...func(*Snapshot)) Snapshot {
		s := Snapshot{ID: id + "00000000", Time: when, Hostname: "volsync", Paths: []string{"/data"}}
		for _, m := range mutate {
			m(&s)
		}
		return s
	}
	paths := func(p ...string) func(*Snapshot) { return func(s *Snapshot) { s.Paths = p } }
	tags := func(t ...string) func(*Snapshot) { return func(s *Snapshot) { s.Tags = t } }
	host := func(h string) func(*Snapshot) { return func(s *Snapshot) { s.Hostname = h } }

	early := snap("11111111", at(-2e9+100))
	first := snap("23cbc21a", at(326e6))
	last := snap("f1863005", at(900e6))
	twin := snap("aaaaaaaa", at(900e6))
	next := snap("99999999", at(1e9+5))

	cases := []struct {
		name      string
		snapshots []Snapshot
		pin       time.Time
		want      string // short ID, or "" for none
	}{
		{"no snapshots", nil, second, ""},
		{"every snapshot is later", []Snapshot{first, next}, second.Add(-time.Second), ""},
		{"the newest second at or before the pin", []Snapshot{early, first, next}, second.Add(500 * time.Millisecond), "23cbc21a"},
		{"a pin with a fraction compares whole seconds", []Snapshot{early, last}, second.Add(100 * time.Millisecond), "f1863005"},
		{"a pin past every snapshot takes the newest second", []Snapshot{early, first, next}, second.Add(time.Hour), "99999999"},
		{"the last snapshot in the second wins", []Snapshot{early, first, last}, second, "f1863005"},
		{"in whatever order the list holds them", []Snapshot{last, first, early}, second, "f1863005"},
		{"a later second past the pin is out of reach", []Snapshot{first, last, next}, second, "f1863005"},
		{"two with the greatest time in the second", []Snapshot{first, last, twin}, second, ""},
		{"two with the same time before the second's last", []Snapshot{first, snap("bbbbbbbb", at(326e6)), last}, second, "f1863005"},
		{"two with the same time in an older second", []Snapshot{snap("cccccccc", at(-2e9)), snap("dddddddd", at(-2e9)), first}, second, "23cbc21a"},
		{"two with the same time in the second the pin falls back to", []Snapshot{snap("cccccccc", at(-2e9)), snap("dddddddd", at(-2e9)), next}, second, ""},
		{"a snapshot without /data is not listed", []Snapshot{first, snap("eeeeeeee", at(900e6), paths("/srv"))}, second, "23cbc21a"},
		{"a path that merely contains /data lists it, as grep does", []Snapshot{first, snap("eeeeeeee", at(900e6), paths("/srv/database"))}, second, "eeeeeeee"},
		{"a tag holding /data lists it", []Snapshot{first, snap("eeeeeeee", at(900e6), paths("/srv"), tags("from:/data"))}, second, "eeeeeeee"},
		{"a host holding /data lists it", []Snapshot{first, snap("eeeeeeee", at(900e6), paths("/srv"), host("x/data"))}, second, "eeeeeeee"},
		{"/data as the first of several paths lists it", []Snapshot{first, snap("eeeeeeee", at(900e6), paths("/data", "/etc"))}, second, "eeeeeeee"},
		{"/data after the first path is misread", []Snapshot{first, snap("eeeeeeee", at(-9e9), paths("/srv", "/data"))}, second, ""},
		{"a snapshot with no paths is not listed", []Snapshot{first, snap("eeeeeeee", at(900e6), paths())}, second, "23cbc21a"},
		{"a rewritten snapshot counts like any other", []Snapshot{first, snap("c0ffee00", at(900e6), func(s *Snapshot) { s.Original = first.ID })}, second, "c0ffee00"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var got string
			if s, ok := MoverPick(c.snapshots, c.pin); ok {
				got = s.ShortID()
			}
			if got != c.want {
				t.Errorf("MoverPick = %q, want %q", got, c.want)
			}
		})
	}
}

// TestMoverPickLeavesItsInputAlone checks that MoverPick neither reorders
// nor changes the list it is given, which a RestoreRun goes on to use.
func TestMoverPickLeavesItsInputAlone(t *testing.T) {
	a := Snapshot{ID: "b0000000", Time: time.Date(2026, 9, 25, 1, 0, 0, 5, time.UTC), Paths: []string{"/data"}}
	b := Snapshot{ID: "a0000000", Time: time.Date(2026, 9, 25, 1, 0, 0, 9, time.UTC), Paths: []string{"/data"}}
	list := []Snapshot{b, a}
	MoverPick(list, b.Time)
	if list[0].ID != b.ID || list[1].ID != a.ID || !slices.Equal(list[0].Paths, []string{"/data"}) {
		t.Errorf("list = %+v after MoverPick", list)
	}
}
