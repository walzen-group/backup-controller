package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

const restoreUID = types.UID("9b7d4e21-0000-4000-8000-000000000002")

func restoreRun(mutate ...func(*backupv1alpha1.RestoreRun)) *backupv1alpha1.RestoreRun {
	run := &backupv1alpha1.RestoreRun{
		ObjectMeta: metav1.ObjectMeta{Name: "back-to-monday", Namespace: ns, UID: restoreUID, Generation: 1},
		Spec:       backupv1alpha1.RestoreRunSpec{Timeout: &metav1.Duration{Duration: 4 * time.Hour}},
	}
	for _, m := range mutate {
		m(run)
	}
	return run
}

func asOf(value string) func(*backupv1alpha1.RestoreRun) {
	return func(r *backupv1alpha1.RestoreRun) { r.Spec.RestoreAsOf = &value }
}

func restoreReconciler(t *testing.T, backups prober, objects ...client.Object) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	c := newClient(t, objects...)
	return &RestoreRunReconciler{
		Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Prober: backups,
		Now: func() time.Time { return frozen },
	}, c
}

func restoreStep(t *testing.T, r *RestoreRunReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

func readRestoreRun(t *testing.T, c client.Client) *backupv1alpha1.RestoreRun {
	t.Helper()
	run := &backupv1alpha1.RestoreRun{}
	get(t, c, ns, "back-to-monday", run)
	return run
}

// VolSync restores nothing and reports success when no snapshot reaches the
// moment. The run fails before it creates anything.
func TestARestoreBeforeEverySnapshotFailsBeforeTouchingAnything(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-01T00:00:00Z")),
		claim(), volumeRestore(), repository())

	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("phase = %q, reason = %q; want Failed, NoBackupInReach", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if !strings.Contains(run.Status.Items[0].Message, "2edf5bab") {
		t.Errorf("message = %q, want it to name the oldest snapshot", run.Status.Items[0].Message)
	}
	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil || len(destinations.Items) != 0 {
		t.Fatalf("destinations = %v, want none", destinations.Items)
	}
}

// A time between two snapshots rounds back to the one before it.
func TestAClaimRestoreSelectsTheSnapshotBeforeItsMoment(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())

	restoreStep(t, r) // plan
	restoreStep(t, r) // restore

	run := readRestoreRun(t, c)
	item := run.Status.Items[0]
	if item.Snapshot != "2edf5bab" {
		t.Errorf("snapshot = %q, want 2edf5bab, the one before 04:00", item.Snapshot)
	}
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, item.Destination, rd)
	if *rd.Spec.Restic.DestinationPVC != claimN || *rd.Spec.Restic.RestoreAsOf != "2026-09-21T04:00:00Z" {
		t.Errorf("destination = %+v", rd.Spec.Restic)
	}

	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{LastManualSync: string(restoreUID)}
	if err := c.Status().Update(context.Background(), rd); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	run = readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", run.Status.Phase)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: item.Destination}, rd); err == nil {
		t.Error("the destination outlived the run")
	}
}

// quiet is a snapshot a quiesced BackupRun moved to its restart moment, taken
// between sunday's and monday's.
var quiet = restic.Snapshot{ID: "c0ffee00" + "00000000", Time: time.Date(2026, 9, 21, 3, 0, 5, 0, time.UTC), Tags: []string{restic.QuiescedTag}}

// A synced restore takes the newest quiesced snapshot, passes over monday's
// untagged one, and restores the volume and the database to that snapshot's
// moment.
func TestASyncedRestoreRestoresEverythingToTheQuiescedMoment(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All, r.Spec.SyncDatabaseToVolume = true, true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())
	r.Snapshots = snapshots{sunday, quiet, monday}

	restoreStep(t, r) // plan
	run := readRestoreRun(t, c)
	if run.Status.SyncedTo == nil || !run.Status.SyncedTo.Equal(&metav1.Time{Time: quiet.Time}) {
		t.Fatalf("syncedTo = %v (%s), want the quiesced snapshot's %s", run.Status.SyncedTo, readyReason(run.Status.Conditions), quiet.Time)
	}
	if run.Status.Items[0].Snapshot != "c0ffee00" {
		t.Errorf("volume snapshot = %q, want c0ffee00, the quiesced one", run.Status.Items[0].Snapshot)
	}

	restoreStep(t, r) // restore the volume
	run = readRestoreRun(t, c)
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, run.Status.Items[0].Destination, rd)
	if rd.Spec.Restic.RestoreAsOf == nil || *rd.Spec.Restic.RestoreAsOf != "2026-09-21T03:00:05Z" || rd.Spec.Restic.Previous != nil {
		t.Errorf("destination restoreAsOf = %v, previous = %v; want 2026-09-21T03:00:05Z and none, so the mover selects the quiesced snapshot",
			rd.Spec.Restic.RestoreAsOf, rd.Spec.Restic.Previous)
	}
}

// A snapshot without the tag was taken while the app ran, so no moment makes
// the database match it. The run refuses before touching anything.
func TestASyncedRestoreRefusesUntaggedSnapshots(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All, r.Spec.SyncDatabaseToVolume = true, true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())

	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("phase = %q, reason = %q; want Failed, NoBackupInReach", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if !strings.Contains(run.Status.Items[0].Message, restic.QuiescedTag) {
		t.Errorf("message = %q, want it to say no snapshot is tagged %s", run.Status.Items[0].Message, restic.QuiescedTag)
	}
	if _, ok := getUnstructured(t, c, ClusterGVK, ns, pgN); !ok {
		t.Error("the Cluster was deleted by a run that could not restore it")
	}
}

// Two writers on one filesystem corrupt the volume, so the run waits.
func TestAMountedClaimMakesTheRestoreWait(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-5d9f", Namespace: ns},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimN},
		}}}},
	}
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository(), pod)
	restoreStep(t, r)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonClaimInUse {
		t.Fatalf("phase = %q, reason = %q; want Waiting, ClaimInUse", run.Status.Phase, readyReason(run.Status.Conditions))
	}
}

// The database restore: the run deletes the Cluster, the webhook recovers the
// one created next, and the run follows it until it is healthy.
func TestADatabaseRestoreDeletesAndFollowsTheCluster(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }, asOf("2026-09-22T00:00:00Z")),
		cluster(), objectStore(), storeSecret())

	restoreStep(t, r) // plan: a base backup is in reach
	restoreStep(t, r) // delete

	run := readRestoreRun(t, c)
	if run.Status.Items[0].Phase != backupv1alpha1.ItemDeleted || run.Status.Items[0].BaseBackup != saturday.ID {
		t.Fatalf("item = %+v, want Deleted from base backup %s", run.Status.Items[0], saturday.ID)
	}
	if _, ok := getUnstructured(t, c, ClusterGVK, ns, pgN); ok {
		t.Fatal("the Cluster was not deleted")
	}
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonRecreate {
		t.Errorf("reason = %q, want the run waiting for the Cluster to be created again", readyReason(run.Status.Conditions))
	}

	// Flux or tofu creates the Cluster again and the webhook recovers it.
	recovered := cluster(func(u *unstructured.Unstructured) {
		annotations := u.GetAnnotations()
		annotations[backupv1alpha1.AnnotationRestoreRun] = "back-to-monday"
		u.SetAnnotations(annotations)
	})
	if err := c.Create(context.Background(), recovered); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	if phase := readRestoreRun(t, c).Status.Items[0].Phase; phase != backupv1alpha1.ItemRecovering {
		t.Fatalf("item phase = %q, want Recovering", phase)
	}

	_ = unstructured.SetNestedField(recovered.Object, healthyPhase, "status", "phase")
	if err := c.Update(context.Background(), recovered); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", run.Status.Phase)
	}
}

// Deleting a Cluster with no base backup in reach would bring it back empty.
func TestADatabaseWithoutABaseBackupIsNotDeleted(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }, asOf("2026-09-01T00:00:00Z")),
		cluster(), objectStore(), storeSecret())

	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("phase = %q, reason = %q", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if _, ok := getUnstructured(t, c, ClusterGVK, ns, pgN); !ok {
		t.Fatal("the Cluster was deleted although no base backup reaches the moment")
	}
}

// The namespace restore: volumes first, then the databases. A failed volume
// leaves the databases running.
func TestANamespaceRestoreLeavesTheDatabasesWhenAVolumeFails(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())

	restoreStep(t, r) // plan
	restoreStep(t, r) // volume destination
	run := readRestoreRun(t, c)
	if run.Status.Items[1].Phase != backupv1alpha1.ItemPending {
		t.Fatalf("the Cluster was handled before its volumes finished: %+v", run.Status.Items[1])
	}

	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, run.Status.Items[0].Destination, rd)
	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{LatestMoverStatus: &volsyncv1alpha1.MoverStatus{
		Result: volsyncv1alpha1.MoverResultFailed, Logs: "Fatal: unable to open repository",
	}}
	if err := c.Status().Update(context.Background(), rd); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	run = readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || run.Status.Items[1].Phase != backupv1alpha1.ItemSkipped {
		t.Fatalf("run = %+v, want Failed with the Cluster skipped", run.Status)
	}
	if _, ok := getUnstructured(t, c, ClusterGVK, ns, pgN); !ok {
		t.Fatal("the Cluster was deleted after a volume restore failed")
	}
}

func TestAnIntoRestoreChecksBeforeCreatingAnything(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN; r.Spec.Into = "notes-data-monday" },
			asOf("2026-09-01T00:00:00Z")),
		claim(), volumeRestore(), repository())

	restoreStep(t, r)

	if run := readRestoreRun(t, c); readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("reason = %q, want NoBackupInReach", readyReason(run.Status.Conditions))
	}
	scratch := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}, scratch); err == nil {
		t.Fatal("a scratch claim was created although no snapshot is in reach")
	}
}
