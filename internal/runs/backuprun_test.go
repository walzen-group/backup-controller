package runs

import (
	"context"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// frozen is the clock every test runs on, so a deadline is a subtraction rather
// than a sleep.
var frozen = time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)

const (
	namespace = "canary-backup"
	sourceNm  = "canary-backup"
	runUID    = types.UID("3f2a1c7e")
	schedule  = "*/15 * * * *"
)

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, volsyncv1alpha1.AddToScheme, backupv1alpha1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("register types: %v", err)
		}
	}
	return s
}

// source is a ReplicationSource on a schedule, which is every source this
// controller meets: the repository module and the Flux component both write one.
func source(trigger *volsyncv1alpha1.ReplicationSourceTriggerSpec, lastManual string) *volsyncv1alpha1.ReplicationSource {
	sched := schedule
	if trigger == nil {
		trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{Schedule: &sched}
	}
	return &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: sourceNm, Namespace: namespace},
		Spec: volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: "canary-backup",
			Trigger:   trigger,
		},
		Status: &volsyncv1alpha1.ReplicationSourceStatus{LastManualSync: lastManual},
	}
}

func backupRun(mutate ...func(*backupv1alpha1.BackupRun)) *backupv1alpha1.BackupRun {
	run := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "before-the-rebuild", Namespace: namespace, UID: runUID, Generation: 1,
			Finalizers: []string{Finalizer},
		},
		Spec: backupv1alpha1.BackupRunSpec{
			Source:  sourceNm,
			Timeout: &metav1.Duration{Duration: time.Hour},
		},
	}
	for _, m := range mutate {
		m(run)
	}
	return run
}

func reconcile(t *testing.T, objects ...client.Object) (*BackupRunReconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(scheme(t)).
		WithObjects(objects...).
		WithStatusSubresource(&backupv1alpha1.BackupRun{}, &volsyncv1alpha1.ReplicationSource{}).
		Build()
	return &BackupRunReconciler{Client: c, Now: func() time.Time { return frozen }}, c
}

func run(t *testing.T, r *BackupRunReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: "before-the-rebuild"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

func readRun(t *testing.T, c client.Client) *backupv1alpha1.BackupRun {
	t.Helper()
	got := &backupv1alpha1.BackupRun{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: "before-the-rebuild"}, got); err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	return got
}

func readSource(t *testing.T, c client.Client) *volsyncv1alpha1.ReplicationSource {
	t.Helper()
	got := &volsyncv1alpha1.ReplicationSource{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: sourceNm}, got); err != nil {
		t.Fatalf("read the source back: %v", err)
	}
	return got
}

func TestStartWritesTheTagAndLeavesTheScheduleAlone(t *testing.T) {
	r, c := reconcile(t, backupRun(), source(nil, ""))

	if got := run(t, r).RequeueAfter; got != pollInterval {
		t.Errorf("requeue after = %v, want %v", got, pollInterval)
	}

	got := readSource(t, c)
	if manualTag(got) != TriggerFor(runUID) {
		t.Errorf("manual tag = %q, want %q", manualTag(got), TriggerFor(runUID))
	}
	// The design rests on this: the controller never declares the schedule, so
	// whatever owns it keeps it. Losing the schedule here would mean a backup
	// on demand silently stopped the volume's real backups.
	if got.Spec.Trigger.Schedule == nil || *got.Spec.Trigger.Schedule != schedule {
		t.Errorf("schedule = %#v, want it untouched at %q", got.Spec.Trigger.Schedule, schedule)
	}

	run := readRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseRunning {
		t.Errorf("phase = %q, want Running", run.Status.Phase)
	}
	if run.Status.StartedAt == nil {
		t.Error("startedAt was not recorded")
	}
}

func TestWaitsWhileTheMoverRuns(t *testing.T) {
	started := metav1.NewTime(frozen)
	r, c := reconcile(t,
		backupRun(func(b *backupv1alpha1.BackupRun) {
			b.Status.Phase = backupv1alpha1.RunPhaseRunning
			b.Status.Trigger = TriggerFor(runUID)
			b.Status.StartedAt = &started
		}),
		source(&volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: TriggerFor(runUID)}, ""),
	)

	if got := run(t, r).RequeueAfter; got != pollInterval {
		t.Errorf("requeue after = %v, want %v", got, pollInterval)
	}
	if phase := readRun(t, c).Status.Phase; phase != backupv1alpha1.RunPhaseRunning {
		t.Errorf("phase = %q, want it still Running", phase)
	}
	if tag := manualTag(readSource(t, c)); tag != TriggerFor(runUID) {
		t.Errorf("manual tag = %q, want it still set while the mover runs", tag)
	}
}

func TestCompletionClearsTheTagAndRecordsTheSnapshot(t *testing.T) {
	started := metav1.NewTime(frozen.Add(-time.Minute))
	synced := metav1.NewTime(frozen)
	src := source(&volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: TriggerFor(runUID)}, TriggerFor(runUID))
	src.Status.LastSyncTime = &synced

	r, c := reconcile(t,
		backupRun(func(b *backupv1alpha1.BackupRun) {
			b.Status.Phase = backupv1alpha1.RunPhaseRunning
			b.Status.Trigger = TriggerFor(runUID)
			b.Status.StartedAt = &started
		}),
		src,
	)

	run(t, r)

	if tag := manualTag(readSource(t, c)); tag != "" {
		t.Errorf("manual tag = %q, want it cleared so the schedule resumes", tag)
	}
	got := readRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", got.Status.Phase)
	}
	if got.Status.SnapshotTime == nil || !got.Status.SnapshotTime.Equal(&synced) {
		t.Errorf("snapshotTime = %#v, want the source's lastSyncTime %v", got.Status.SnapshotTime, synced)
	}
	if len(got.Finalizers) != 0 {
		t.Errorf("finalizers = %v, want none once the run is finished", got.Finalizers)
	}
}

func TestATriggerHeldByAnythingElseIsRefused(t *testing.T) {
	r, c := reconcile(t,
		backupRun(),
		source(&volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: "someone-elses-tag"}, ""),
	)

	run(t, r)

	got := readRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if reason := readyReason(got.Status.Conditions); reason != backupv1alpha1.ReasonTriggerHeld {
		t.Errorf("reason = %q, want TriggerHeld", reason)
	}
	// Writing over it would take a backup its holder never sees finish, and
	// leave that holder unable to clear a field it no longer owns.
	if tag := manualTag(readSource(t, c)); tag != "someone-elses-tag" {
		t.Errorf("manual tag = %q, want the other holder's tag untouched", tag)
	}
}

func TestATimedOutMoverClearsTheTagAndFails(t *testing.T) {
	started := metav1.NewTime(frozen.Add(-2 * time.Hour))
	r, c := reconcile(t,
		backupRun(func(b *backupv1alpha1.BackupRun) {
			b.Status.Phase = backupv1alpha1.RunPhaseRunning
			b.Status.Trigger = TriggerFor(runUID)
			b.Status.StartedAt = &started
		}),
		source(&volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: TriggerFor(runUID)}, ""),
	)

	run(t, r)

	// A stuck mover must not leave the source holding a spent tag, because that
	// stops its backups with nothing reporting the fact.
	if tag := manualTag(readSource(t, c)); tag != "" {
		t.Errorf("manual tag = %q, want it cleared on timeout", tag)
	}
	got := readRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", got.Status.Phase)
	}
	if reason := readyReason(got.Status.Conditions); reason != backupv1alpha1.ReasonTimedOut {
		t.Errorf("reason = %q, want TimedOut", reason)
	}
}

func TestDeletingARunningRunTakesItsTriggerWithIt(t *testing.T) {
	deleted := metav1.NewTime(frozen)
	started := metav1.NewTime(frozen)
	r, c := reconcile(t,
		backupRun(func(b *backupv1alpha1.BackupRun) {
			b.DeletionTimestamp = &deleted
			b.Status.Phase = backupv1alpha1.RunPhaseRunning
			b.Status.Trigger = TriggerFor(runUID)
			b.Status.StartedAt = &started
		}),
		source(&volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: TriggerFor(runUID)}, ""),
	)

	run(t, r)

	// Without the finalizer this is the silent failure the whole kind exists to
	// prevent: the run is gone and the source never takes another backup.
	if tag := manualTag(readSource(t, c)); tag != "" {
		t.Errorf("manual tag = %q, want deletion to clear it", tag)
	}
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: namespace, Name: "before-the-rebuild"},
		&backupv1alpha1.BackupRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("reading the run back gave %v, want NotFound once the finalizer is dropped", err)
	}
}

func TestAMissingSourceFailsTheRun(t *testing.T) {
	r, c := reconcile(t, backupRun())

	run(t, r)

	got := readRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if reason := readyReason(got.Status.Conditions); reason != backupv1alpha1.ReasonInvalid {
		t.Errorf("reason = %q, want Invalid", reason)
	}
}

func readyReason(conditions []metav1.Condition) string {
	for _, condition := range conditions {
		if condition.Type == backupv1alpha1.ConditionReady {
			return condition.Reason
		}
	}
	return ""
}
