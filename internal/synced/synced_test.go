package synced

import (
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/restic"
)

// TestMoment checks that the paused moment is the newest paused time in the
// namespace when every repository holds it, and that a repository without
// it gives no moment, also when an older time is common to all.
func TestMoment(t *testing.T) {
	at := func(h int) time.Time { return time.Date(2026, 9, 28, h, 0, 0, 0, time.UTC) }
	paused := func(h int) restic.Snapshot { return restic.Snapshot{Time: at(h), Tags: []string{restic.PausedTag}} }
	live := func(h int) restic.Snapshot { return restic.Snapshot{Time: at(h)} }
	cases := []struct {
		name  string
		repos map[string][]restic.Snapshot
		want  time.Time
		found bool
	}{
		{"one repository", map[string][]restic.Snapshot{"a": {paused(1), paused(3), live(5)}}, at(3), true},
		{"the newest time both hold", map[string][]restic.Snapshot{"a": {paused(1), paused(3)}, "b": {paused(1), paused(3), live(4)}}, at(3), true},
		{"only an older time both hold", map[string][]restic.Snapshot{"a": {paused(1), paused(3)}, "b": {paused(1)}}, time.Time{}, false},
		{"no time both hold", map[string][]restic.Snapshot{"a": {paused(3)}, "b": {paused(1)}}, time.Time{}, false},
		{"one repository without a paused snapshot", map[string][]restic.Snapshot{"a": {paused(3)}, "b": {live(3)}}, time.Time{}, false},
		{"no repository", map[string][]restic.Snapshot{}, time.Time{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, found := Moment(c.repos)
			if found != c.found || !got.Equal(c.want) {
				t.Errorf("Moment = %s, %t; want %s, %t", got, found, c.want, c.found)
			}
		})
	}
}
