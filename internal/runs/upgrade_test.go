package runs

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/populator"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
)

// The tests in this file check that a run records the status format it was
// planned under, and that an unfinished run an older release planned ends
// with reason Upgraded through the normal finish, which gives back what it
// stopped (designs/no-inflight-upgrade.md U1 and U2).

// olderBackupRun returns the BackupRun before-upgrade with spec.all set as
// v0.8.1 left it in the middle of its quiesce: admitted, with the app's
// Deployment recorded at 2 replicas and stopped, its Kustomization
// suspended, the items not started yet, and no status.plannedBy.
func olderBackupRun() *backupv1alpha1.BackupRun {
	started := metav1.NewTime(frozen.Add(-2 * time.Minute))
	quiescedAt := metav1.NewTime(frozen.Add(-time.Minute))
	return backupRun(func(b *backupv1alpha1.BackupRun) {
		b.Finalizers = []string{Finalizer}
		b.Spec.All = true
		b.Status.PlannedBy = ""
		b.Status.Phase = backupv1alpha1.RunPhaseRunning
		b.Status.StartedAt, b.Status.QuiescedAt = &started, &quiescedAt
		b.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
		b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		b.Status.Items = []backupv1alpha1.BackupItem{
			{Kind: "ReplicationSource", Name: claimN, Phase: backupv1alpha1.ItemPending},
			{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemPending},
		}
	})
}

// A BackupRun v0.8.1 left in the middle of its quiesce ends Failed with
// reason Upgraded on the first pass: its app gets its 2 replicas back, its
// Kustomization is resumed, its items fail with the same message, nothing is
// started, and the run records a Warning event. Before, the run went on
// with the plan the older release recorded.
func TestABackupRunAnOlderReleasePlannedEndsUpgraded(t *testing.T) {
	r, c := backupReconciler(t, olderBackupRun(),
		claim(), volume(), volumeRestore(), repository(), cluster(), stoppedDeployment(), kustomization(true))
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder

	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonUpgraded {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed, %s", run.Status.Phase, readyReason(run.Status.Conditions),
			readyMessage(run.Status.Conditions), backupv1alpha1.ReasonUpgraded)
	}
	message := readyMessage(run.Status.Conditions)
	for _, want := range []string{"started by an older version of backup-controller", "gave back the workloads it had stopped", "Create a new BackupRun"} {
		if !strings.Contains(message, want) {
			t.Errorf("message = %q, want it to hold %q", message, want)
		}
	}
	for _, item := range run.Status.Items {
		if item.Phase != backupv1alpha1.ItemFailed || item.Message != message {
			t.Errorf("item %s %s = %s %q, want Failed with the run's message", item.Kind, item.Name, item.Phase, item.Message)
		}
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d, want the 2 the run recorded given back", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization the run suspended was not resumed")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: claimN}, &volsyncv1alpha1.ReplicationSource{}); !apierrors.IsNotFound(err) {
		t.Errorf("ReplicationSource %s: %v, want none written", claimN, err)
	}
	if _, ok := getUnstructured(t, c, BackupGVK, ns, backupName(pgN, runUID)); ok {
		t.Error("the run created a Backup, want nothing started")
	}
	if slices.Contains(run.Finalizers, Finalizer) {
		t.Errorf("finalizers = %v, want %s dropped", run.Finalizers, Finalizer)
	}
	if got := recorded(recorder); len(got) != 1 || !strings.HasPrefix(got[0], "Warning Upgraded ") {
		t.Errorf("events = %q, want one Warning with reason Upgraded", got)
	}
}

// A BackupRun that nothing has planned yet plans as usual, whatever its
// status.plannedBy says, and records this release's format with its plan.
// The next pass admits it rather than ending it.
func TestANewBackupRunRecordsTheFormatItWasPlannedUnder(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) {
		b.Spec.Source = claimN
		b.Status.PlannedBy = ""
	}), claim(), volume(), volumeRestore(), repository())

	step(t, r) // plan
	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseQueued || run.Status.PlannedBy != runFormat {
		t.Fatalf("phase = %q, plannedBy = %q; want Queued, %q", run.Status.Phase, run.Status.PlannedBy, runFormat)
	}
	step(t, r) // admit
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseRunning {
		t.Fatalf("phase = %q (%s), want Running", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}

// A RestoreRun records this release's format with its plan, in place and
// into a new claim alike.
func TestANewRestoreRunRecordsTheFormatItWasPlannedUnder(t *testing.T) {
	for name, mutate := range map[string]func(*backupv1alpha1.RestoreRun){
		"in place": func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN },
		"into":     intoMonday,
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(mutate, func(r *backupv1alpha1.RestoreRun) { r.Status.PlannedBy = "" }),
				sourceOnNode(), volumeRestore(), repository())
			restoreStep(t, r)
			run := readRestoreRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseRunning || run.Status.PlannedBy != runFormat {
				t.Fatalf("phase = %q, plannedBy = %q (%s); want Running, %q", run.Status.Phase, run.Status.PlannedBy,
					readyMessage(run.Status.Conditions), runFormat)
			}
		})
	}
}

// olderRestoreRun returns the RestoreRun back-to-monday as v0.8.1 left it
// while its mover wrote notes-data: the app's Deployment recorded at 2
// replicas and stopped, its Kustomization suspended, and the volume item
// Running with the run's ReplicationDestination, but no status.plannedBy.
// It also returns that destination.
func olderRestoreRun() (*backupv1alpha1.RestoreRun, *volsyncv1alpha1.ReplicationDestination) {
	started := metav1.NewTime(frozen.Add(-2 * time.Minute))
	quiescedAt := metav1.NewTime(frozen.Add(-time.Minute))
	name := destinationName(restoreUID, 0)
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Spec.Claim = claimN
		r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
		r.Status.PlannedBy = ""
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt, r.Status.QuiescedAt = &started, &quiescedAt
		r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
		r.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemRunning,
			Snapshot: monday.ShortID(), SnapshotTime: &metav1.Time{Time: monday.Time}, Destination: name}}
	})
	destination := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: string(restoreUID)},
		},
	}
	return run, destination
}

// A RestoreRun v0.8.1 left while its mover wrote the claim ends Failed with
// reason Upgraded on the first pass: its ReplicationDestination is deleted,
// the app gets its 2 replicas back and its Kustomization is resumed, and the
// run records a Warning event.
func TestARestoreRunAnOlderReleasePlannedEndsUpgraded(t *testing.T) {
	run, destination := olderRestoreRun()
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), destination, stoppedDeployment(), kustomization(true))
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder

	restoreStep(t, r)

	got := readRestoreRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(got.Status.Conditions) != backupv1alpha1.ReasonUpgraded {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed, %s", got.Status.Phase, readyReason(got.Status.Conditions),
			readyMessage(got.Status.Conditions), backupv1alpha1.ReasonUpgraded)
	}
	message := readyMessage(got.Status.Conditions)
	for _, want := range []string{"started by an older version of backup-controller", "gave back the workloads it had stopped", "Create a new RestoreRun"} {
		if !strings.Contains(message, want) {
			t.Errorf("message = %q, want it to hold %q", message, want)
		}
	}
	if item := got.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Message != message || item.Destination != "" {
		t.Errorf("item = %+v, want Failed with the run's message and no destination", item)
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the run's deleted", names)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Errorf("replicas = %d, want the 2 the run recorded given back", replicas)
	}
	if suspended(t, c) {
		t.Error("the Kustomization the run suspended was not resumed")
	}
	if events := recorded(recorder); len(events) != 1 || !strings.HasPrefix(events[0], "Warning Upgraded ") {
		t.Errorf("events = %q, want one Warning with reason Upgraded", events)
	}
}

// A RestoreRun v0.8.1 left waiting for a Cluster it deleted ends Upgraded,
// and its message names the Cluster that has not been recovered, so a person
// knows to restore it with a new run.
func TestAnUpgradedRestoreNamesTheClusterItDeleted(t *testing.T) {
	started := metav1.NewTime(frozen.Add(-2 * time.Minute))
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Spec.Database = pgN
		r.Status.PlannedBy = ""
		r.Status.Phase = backupv1alpha1.RunPhaseWaiting
		r.Status.StartedAt = &started
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemDeleted,
			ClusterUID: "old-cluster-uid", BaseBackup: saturday.ID}}
	})
	r, c := restoreReconciler(t, prober{saturday}, run, objectStore(), storeSecret())

	restoreStep(t, r)

	got := readRestoreRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(got.Status.Conditions) != backupv1alpha1.ReasonUpgraded {
		t.Fatalf("phase = %q, reason = %q; want Failed, %s", got.Status.Phase, readyReason(got.Status.Conditions), backupv1alpha1.ReasonUpgraded)
	}
	want := "Cluster " + pgN + " was deleted by this run and has not been recovered; create a new RestoreRun for it"
	if message := readyMessage(got.Status.Conditions); !strings.Contains(message, want) {
		t.Errorf("message = %q, want it to hold %q", message, want)
	}
	if message := got.Status.Items[0].Message; got.Status.Items[0].Phase != backupv1alpha1.ItemFailed || !strings.Contains(message, want) {
		t.Errorf("item = %+v, want Failed holding %q", got.Status.Items[0], want)
	}
	if strings.Contains(readyMessage(got.Status.Conditions), "gave back the workloads") {
		t.Errorf("message = %q, want nothing said of workloads the run never stopped", readyMessage(got.Status.Conditions))
	}
}

// A RestoreRun v0.8.1 started through the VolumeRestore populator ends
// Upgraded, and leaves the claim and the VolumeRestore that release created
// as they are. The run keeps its finalizer while the VolumeRestore carries
// the populator's finalizer, so deleting the run later releases it (see
// releaseVolumeRestore). Before, the run followed the populator.
func TestAnUpgradedPopulatorRestoreDeletesNothing(t *testing.T) {
	run := populatorRun()
	run.Status.PlannedBy = ""
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run, populator.Finalizer), populatorClaim(run, corev1.ClaimPending, populator.ClaimFinalizer))
	left := snapshotVersions(t, c, &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday"}})

	restoreStep(t, r)

	got := readRestoreRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(got.Status.Conditions) != backupv1alpha1.ReasonUpgraded {
		t.Fatalf("phase = %q, reason = %q; want Failed, %s", got.Status.Phase, readyReason(got.Status.Conditions), backupv1alpha1.ReasonUpgraded)
	}
	expectUnchanged(t, c, left)
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want none", names)
	}
	if !slices.Contains(got.Finalizers, Finalizer) {
		t.Errorf("finalizers = %v, want %s kept while the VolumeRestore is held", got.Finalizers, Finalizer)
	}
}

// A run being deleted is put back by its finalizer as before, whatever
// release planned it: the deletion comes before the format check.
func TestADeletedOlderRunIsPutBackByItsFinalizer(t *testing.T) {
	r, c := backupReconciler(t, olderBackupRun(),
		claim(), volume(), volumeRestore(), repository(), cluster(), stoppedDeployment(), kustomization(true))
	if err := c.Delete(context.Background(), readBackupRun(t, c)); err != nil {
		t.Fatal(err)
	}

	step(t, r)

	if replicas := replicasOf(t, c); replicas != 2 {
		t.Errorf("replicas = %d, want the 2 the run recorded given back", replicas)
	}
	k, _ := getUnstructured(t, c, KustomizationGVK, "flux-system", appN)
	if on, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend"); on {
		t.Error("the Kustomization the run suspended was not resumed")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "before-upgrade"}, &backupv1alpha1.BackupRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("BackupRun: %v, want it gone once its finalizer is dropped", err)
	}
}

// A BackupRun v0.8.1 left under the v0.7.2 BackupRun CRD with its restart
// recorded ends Upgraded. That CRD drops status.restartPending, so the
// recorded restart may be one that failed, and the message says to check the
// workloads rather than claim they are back.
func TestAnUpgradedRunWhoseStatusShowsTheRestartSaysToCheckTheApp(t *testing.T) {
	older := olderBackupRun()
	older.Status.RestartedAt = atFrozen(-30 * time.Second)
	c := newClientWithCRDs(t, withOldCRD("backup.wlz.li_backupruns.yaml"), older,
		claim(), volume(), volumeRestore(), repository(), cluster(), stoppedDeployment(), kustomization(true))
	r := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonUpgraded {
		t.Fatalf("phase = %q, reason = %q; want Failed, Upgraded", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	message := readyMessage(run.Status.Conditions)
	if !strings.Contains(message, "check that each workload in status.quiesced runs with the replicas recorded there") ||
		strings.Contains(message, "The run gave back") {
		t.Errorf("message = %q, want it to ask for a check of the workloads and claim nothing", message)
	}
}
