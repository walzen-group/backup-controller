package runs

import (
	"context"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func scheduledNamespace(schedule string, created time.Time) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{
		Name:              ns,
		CreationTimestamp: metav1.NewTime(created),
		Annotations:       map[string]string{backupv1alpha1.AnnotationSchedule: schedule},
	}}
}

func schedule(t *testing.T, now time.Time, objects ...client.Object) (ctrl.Result, client.Client) {
	t.Helper()
	c := newClient(t, objects...)
	s := &Scheduler{Client: c, Reader: c, Now: func() time.Time { return now }}
	result, err := s.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: ns}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result, c
}

func scheduledRuns(t *testing.T, c client.Client) []backupv1alpha1.BackupRun {
	t.Helper()
	runs := &backupv1alpha1.BackupRunList{}
	if err := c.List(context.Background(), runs, client.InNamespace(ns), client.HasLabels{backupv1alpha1.LabelScheduledFor}); err != nil {
		t.Fatal(err)
	}
	return runs.Items
}

func TestADueTickCreatesARunOfTheWholeNamespace(t *testing.T) {
	created := time.Date(2026, 9, 24, 4, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 24, 5, 0, 30, 0, time.UTC)

	result, c := schedule(t, now, scheduledNamespace("0 5 * * *", created))

	runs := scheduledRuns(t, c)
	if len(runs) != 1 {
		t.Fatalf("runs = %d, want 1", len(runs))
	}
	run := runs[0]
	if !run.Spec.All || run.Name != "scheduled-20260924-0500" || run.Labels[backupv1alpha1.LabelScheduledFor] != "1790226000" {
		t.Errorf("run = %s %v all=%v", run.Name, run.Labels, run.Spec.All)
	}
	if want := 5 * time.Minute; result.RequeueAfter != want {
		t.Errorf("requeue after = %v, want the refresh ceiling %v", result.RequeueAfter, want)
	}
}

// A CRON_TZ prefix puts the schedule on that zone's clock: 04:00 in Berlin is
// 02:00 UTC in September.
func TestAScheduleWithAZoneTicksOnThatZonesClock(t *testing.T) {
	created := time.Date(2026, 9, 24, 1, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 24, 2, 0, 30, 0, time.UTC)

	_, c := schedule(t, now, scheduledNamespace("CRON_TZ=Europe/Berlin 0 4 * * *", created))

	runs := scheduledRuns(t, c)
	if len(runs) != 1 || runs[0].Labels[backupv1alpha1.LabelScheduledFor] != "1790215200" {
		t.Fatalf("runs = %v, want one for 2026-09-24T02:00:00Z (1790215200)", runs)
	}
}

func TestATickNotYetDueCreatesNothing(t *testing.T) {
	created := time.Date(2026, 9, 24, 4, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 24, 4, 58, 0, 0, time.UTC)

	result, c := schedule(t, now, scheduledNamespace("0 5 * * *", created))

	if runs := scheduledRuns(t, c); len(runs) != 0 {
		t.Fatalf("runs = %d before the tick, want 0", len(runs))
	}
	if result.RequeueAfter != 2*time.Minute {
		t.Errorf("requeue after = %v, want the two minutes to the tick", result.RequeueAfter)
	}
}

// Ticks missed while the controller was down run once, for the newest.
func TestMissedTicksRunOnceForTheNewest(t *testing.T) {
	created := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 24, 5, 30, 0, 0, time.UTC)

	_, c := schedule(t, now, scheduledNamespace("0 5 * * *", created))

	runs := scheduledRuns(t, c)
	if len(runs) != 1 || runs[0].Name != "scheduled-20260924-0500" {
		t.Fatalf("runs = %v, want one for the newest tick", runs)
	}
}

// One namespace backup at a time: a second would find every source busy.
func TestAnUnfinishedNamespaceRunHoldsTheTick(t *testing.T) {
	created := time.Date(2026, 9, 24, 4, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 24, 5, 0, 30, 0, time.UTC)
	running := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "by-hand", Namespace: ns},
		Spec:       backupv1alpha1.BackupRunSpec{All: true},
		Status:     backupv1alpha1.BackupRunStatus{Phase: backupv1alpha1.RunPhaseRunning},
	}

	_, c := schedule(t, now, scheduledNamespace("0 5 * * *", created), running)

	if runs := scheduledRuns(t, c); len(runs) != 0 {
		t.Fatalf("runs = %d while another namespace run works, want 0", len(runs))
	}
}

func TestANamespaceWithoutAScheduleIsLeftAlone(t *testing.T) {
	plain := &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}
	_, c := schedule(t, frozen, plain)
	if runs := scheduledRuns(t, c); len(runs) != 0 {
		t.Fatalf("runs = %d, want 0", len(runs))
	}
}
