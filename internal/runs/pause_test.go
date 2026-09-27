package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// leaseCount returns the number of Leases in the client.
func leaseCount(t *testing.T, c client.Client) int {
	t.Helper()
	leases := &coordinationv1.LeaseList{}
	if err := c.List(context.Background(), leases); err != nil {
		t.Fatal(err)
	}
	return len(leases.Items)
}

// A new BackupRun waits while the controller runs with --pause. It keeps an
// empty phase with reason Paused, and it takes no Lease, creates no Workload
// or ReplicationSource, and stops no workload. A reconciler without the flag
// then plans it.
func TestANewBackupRunWaitsWhilePaused(t *testing.T) {
	t.Parallel()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false), localQueueObject())
	r.Paused = true
	for range 3 {
		step(t, r)
	}

	run := readBackupRun(t, c)
	if run.Status.Phase != "" || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonPaused {
		t.Fatalf("phase = %q, reason = %q while paused, want no phase and reason Paused", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if n := leaseCount(t, c); n != 0 {
		t.Errorf("Leases = %d while paused, want none", n)
	}
	if _, ok := getUnstructured(t, c, kueue.WorkloadGVK, ns, kueue.WorkloadName(runUID)); ok {
		t.Error("the paused run created a Workload")
	}
	sources := &volsyncv1alpha1.ReplicationSourceList{}
	if err := c.List(context.Background(), sources); err != nil || len(sources.Items) != 0 {
		t.Errorf("ReplicationSources = %d (%v) while paused, want none", len(sources.Items), err)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Errorf("replicas = %d while paused, want the app left at 2", replicas)
	}

	r.Paused = false
	step(t, r)
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Errorf("phase = %q after the pause, want Queued", run.Status.Phase)
	}
}

// A BackupRun that has stopped its app runs to its end while the controller
// runs with --pause, and gives the app back.
func TestABackupRunInProgressFinishesWhilePaused(t *testing.T) {
	t.Parallel()
	r, c := quiescedVolumeRun(t, annotatedNamespace(nil))
	r.Paused = true

	cutClone(t, c)
	step(t, r)
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Fatalf("replicas = %d after the clone was cut while paused, want 2 back", replicas)
	}
	complete(t, c)
	for range 4 {
		if readBackupRun(t, c).Status.Phase.Finished() {
			break
		}
		step(t, r)
	}
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s) while paused, want Succeeded", run.Status.Phase, readyReason(run.Status.Conditions))
	}
}

// A new RestoreRun waits while the controller runs with --pause: no restore
// Job, no stopped app, no Lease. A reconciler without the flag then plans it.
func TestANewRestoreRunWaitsWhilePaused(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), deployment(), kustomization(false))
	r.Paused = true
	for range 3 {
		restoreStep(t, r)
	}

	run := readRestoreRun(t, c)
	if run.Status.Phase != "" || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonPaused {
		t.Fatalf("phase = %q, reason = %q while paused, want no phase and reason Paused", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if readyMessage(run.Status.Conditions) != pausedMessage {
		t.Errorf("message = %q, want %q", readyMessage(run.Status.Conditions), pausedMessage)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v while paused, want none", jobs)
	}
	if replicas := replicasOf(t, c); replicas != 2 || suspended(t, c) {
		t.Errorf("replicas = %d, suspended = %v while paused, want the app left alone", replicas, suspended(t, c))
	}
	if n := leaseCount(t, c); n != 0 {
		t.Errorf("Leases = %d while paused, want none", n)
	}

	r.Paused = false
	restoreStep(t, r)
	if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseRunning {
		t.Errorf("phase = %q after the pause, want Running", run.Status.Phase)
	}
}

// The Scheduler creates no BackupRun for a due tick while the controller
// runs with --pause. Without the flag it creates one run, for the newest
// tick it missed.
func TestTheSchedulerCreatesNoRunWhilePaused(t *testing.T) {
	t.Parallel()
	created := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	now := time.Date(2026, 9, 24, 5, 30, 0, 0, time.UTC)
	s, c, _ := scheduler(t, now, scheduledNamespace("0 5 * * *", created), claim())
	s.Paused = true
	tick(t, s)
	if runs := scheduledRuns(t, c); len(runs) != 0 {
		t.Fatalf("runs = %v while paused, want none", runs)
	}

	s.Paused = false
	s.Now = func() time.Time { return now.Add(48 * time.Hour) }
	tick(t, s)
	if runs := scheduledRuns(t, c); len(runs) != 1 || runs[0].Name != "scheduled-20260926-0500" {
		t.Fatalf("runs = %v after the pause, want one for the newest tick", runs)
	}
}

// A new BackupRun that waited Paused for longer than its timeout does not
// fail on its first pass after the pause. The admission wait counts from the
// moment the controller first worked on the run after the pause, so the run
// waits Queued, and it fails only when its timeout has passed since then.
func TestABackupRunPausedPastItsTimeoutWaitsForAdmissionAfterThePause(t *testing.T) {
	t.Parallel()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) {
		b.Spec.Source = claimN
		b.CreationTimestamp = metav1.NewTime(frozen)
	}), claim(), volume(), volumeRestore(), repository(), localQueueObject())
	r.Paused = true
	step(t, r)

	resumed := frozen.Add(2 * time.Hour) // the run's timeout is an hour
	r.Now = func() time.Time { return resumed }
	r.Paused = false
	step(t, r) // plan
	step(t, r) // admit: the Workload waits
	step(t, r)
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Fatalf("phase = %q (%s) after the pause, want Queued", run.Status.Phase, readyMessage(run.Status.Conditions))
	}

	r.Now = func() time.Time { return resumed.Add(2 * time.Hour) }
	step(t, r)
	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(readyMessage(run.Status.Conditions), "from the end of the pause") {
		t.Fatalf("phase = %q, Ready = %q past the timeout after the pause; want Failed, counted from the end of the pause", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}
