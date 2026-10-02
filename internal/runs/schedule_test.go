package runs

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestExportScheduleNextRun checks that the next-run series holds the first
// tick after now: a Sunday 03:00 schedule read on Friday 2 October 2026 next
// ticks on Sunday 4 October, and the interval is one week.
func TestExportScheduleNextRun(t *testing.T) {
	schedule, err := cron.ParseStandard("0 3 * * 0")
	if err != nil {
		t.Fatal(err)
	}
	namespace := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "next-run"}}
	defer scheduleLabels(namespace.Name).deleteAll()

	(&Scheduler{}).exportSchedule(namespace, schedule, nil, time.Date(2026, 10, 2, 14, 0, 0, 0, time.UTC))

	want := time.Date(2026, 10, 4, 3, 0, 0, 0, time.UTC)
	if got := testutil.ToFloat64(nextRun.WithLabelValues(namespace.Name)); got != float64(want.Unix()) {
		t.Errorf("next run = %s; want %s", time.Unix(int64(got), 0).UTC(), want)
	}
	if got := testutil.ToFloat64(scheduleInterval.WithLabelValues(namespace.Name)); got != (7 * 24 * time.Hour).Seconds() {
		t.Errorf("interval = %v s; want one week", got)
	}
}

// TestExportScheduleInfo checks that the schedule-info series carries the
// annotation as written, and that editing the schedule leaves one series
// with the new string.
func TestExportScheduleInfo(t *testing.T) {
	defer scheduleLabels("schedule-info").deleteAll()

	exportScheduleInfo("schedule-info", "0 3 * * 0")
	exportScheduleInfo("schedule-info", "CRON_TZ=Europe/Berlin 30 2 * * *")

	if got := testutil.CollectAndCount(scheduleInfo); got != 1 {
		t.Fatalf("schedule-info series = %d; want 1", got)
	}
	if got := testutil.ToFloat64(scheduleInfo.WithLabelValues("schedule-info", "CRON_TZ=Europe/Berlin 30 2 * * *")); got != 1 {
		t.Errorf("schedule-info for the edited schedule = %v; want 1", got)
	}
}

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
