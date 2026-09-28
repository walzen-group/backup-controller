package runs

import (
	"testing"
	"time"

	"github.com/robfig/cron/v3"
)

// TestDueTick checks which tick a schedule is due for: none before the first
// tick after the baseline, the tick itself once it has come, and only the
// newest when several were missed.
func TestDueTick(t *testing.T) {
	schedule, err := cron.ParseStandard("0 3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	day := func(d, h, m int) time.Time { return time.Date(2026, 9, d, h, m, 0, 0, time.UTC) }
	cases := []struct {
		name          string
		baseline, now time.Time
		want          time.Time
		due           bool
	}{
		{"before the first tick", day(27, 3, 0), day(28, 2, 59), time.Time{}, false},
		{"at the tick", day(27, 3, 0), day(28, 3, 0), day(28, 3, 0), true},
		{"after the tick", day(27, 3, 0), day(28, 9, 30), day(28, 3, 0), true},
		{"three ticks missed", day(25, 3, 0), day(28, 9, 30), day(28, 3, 0), true},
		{"a new namespace", day(28, 1, 15), day(28, 3, 1), day(28, 3, 0), true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, due := dueTick(schedule, c.baseline, c.now)
			if due != c.due || !got.Equal(c.want) {
				t.Errorf("dueTick = %s, %t; want %s, %t", got, due, c.want, c.due)
			}
		})
	}
}

// TestDueTickFollowsTheScheduleZone checks that a CRON_TZ= schedule ticks on
// that zone's clock: 03:00 in Europe/Berlin is 01:00 UTC in summer.
func TestDueTickFollowsTheScheduleZone(t *testing.T) {
	schedule, err := cron.ParseStandard("CRON_TZ=Europe/Berlin 0 3 * * *")
	if err != nil {
		t.Fatal(err)
	}
	baseline := time.Date(2026, 9, 27, 1, 0, 0, 0, time.UTC)
	got, due := dueTick(schedule, baseline, time.Date(2026, 9, 28, 1, 30, 0, 0, time.UTC))
	if want := time.Date(2026, 9, 28, 1, 0, 0, 0, time.UTC); !due || !got.Equal(want) {
		t.Errorf("dueTick = %s, %t; want %s", got.UTC(), due, want)
	}
}
