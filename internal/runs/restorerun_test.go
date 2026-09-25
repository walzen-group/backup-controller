package runs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// restoreUID is the UID of the RestoreRun back-to-monday. The run's
// ReplicationDestinations use it as their manual trigger, so a test marks a
// restore done by writing it to the destination's lastManualSync.
const restoreUID = types.UID("9b7d4e21-0000-4000-8000-000000000002")

// restoreRun returns the RestoreRun back-to-monday with a four-hour timeout,
// after applying each of the mutate functions to it.
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

// asOf returns a mutate function for restoreRun that sets spec.restoreAsOf to
// the given value.
func asOf(value string) func(*backupv1alpha1.RestoreRun) {
	return func(r *backupv1alpha1.RestoreRun) { r.Spec.RestoreAsOf = &value }
}

// restoreReconciler returns a RestoreRunReconciler over a fake client that
// holds the given objects, and the client itself. The reconciler runs on the
// frozen clock, its lister holds sunday's and monday's snapshots, and its
// Prober returns the base backups passed in backups.
func restoreReconciler(t *testing.T, backups prober, objects ...client.Object) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	c := newClient(t, objects...)
	return &RestoreRunReconciler{
		Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Prober: backups,
		Now: func() time.Time { return frozen },
	}, c
}

// restoreStep reconciles the RestoreRun back-to-monday once and returns the
// result. An error from the reconcile fails the test.
func restoreStep(t *testing.T, r *RestoreRunReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

// readRestoreRun reads the RestoreRun back-to-monday back from the client.
func readRestoreRun(t *testing.T, c client.Client) *backupv1alpha1.RestoreRun {
	t.Helper()
	run := &backupv1alpha1.RestoreRun{}
	get(t, c, ns, "back-to-monday", run)
	return run
}

// A restore to a moment before the oldest snapshot fails with reason
// NoBackupInReach, names the oldest snapshot, and creates no
// ReplicationDestination. VolSync itself would restore nothing and report
// success.
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

// A claim restore to a time between two snapshots selects the earlier one,
// hands the time to its ReplicationDestination, and deletes the destination
// once the restore succeeds.
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

// quiet is a snapshot tagged quiesced, as a quiesced BackupRun leaves it after
// moving it to its restart moment. Its time falls between sunday's and
// monday's.
var quiet = restic.Snapshot{ID: "c0ffee00" + "00000000", Time: time.Date(2026, 9, 21, 3, 0, 5, 0, time.UTC), Tags: []string{restic.QuiescedTag}}

// A synced restore selects the newest quiesced snapshot and passes over
// monday's newer untagged one. It records that snapshot's time in syncedTo and
// hands the same time to the volume's ReplicationDestination, with no
// previous, so the mover selects the quiesced snapshot too.
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

// A synced restore of a repository with no quiesced snapshot fails with
// reason NoBackupInReach and leaves the Cluster in place. An untagged
// snapshot was taken while the app ran, so no moment makes the database match
// it.
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

// A restore in place waits with reason ClaimInUse while a pod mounts the
// claim, because two writers on one filesystem corrupt the volume.
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

// A database restore deletes the Cluster and waits for it to be created
// again. It follows the Cluster the webhook marked as its recovery until the
// Cluster is healthy, then succeeds.
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

// readyMessage returns the message on a run's Ready condition, or an empty
// string when there is none.
func readyMessage(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == "Ready" {
			return c.Message
		}
	}
	return ""
}

// writerPod returns the app's running pod, which mounts the claim.
func writerPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-5d9f", Namespace: ns, Labels: map[string]string{"app": appN}},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimN},
		}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// quiescedRestore returns a namespace restore whose spec.quiesce lists the
// app's Deployment.
func quiescedRestore() *backupv1alpha1.RestoreRun {
	return restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.All = true
		r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
	})
}

// replicasOf returns the replica count of the app's Deployment.
func replicasOf(t *testing.T, c client.Client) int32 {
	t.Helper()
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	return *d.Spec.Replicas
}

// suspended reports whether the app's Flux Kustomization is suspended.
func suspended(t *testing.T, c client.Client) bool {
	t.Helper()
	k, _ := getUnstructured(t, c, KustomizationGVK, "flux-system", appN)
	s, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend")
	return s
}

// A quiesced restore stops the app the way a BackupRun does, restores nothing
// while its pod is still there, and gives the app back once the volume is
// restored and the database deleted, so Flux can create the Cluster again.
func TestAQuiescedRestoreStopsTheAppUntilTheDatabaseIsDeleted(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())

	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce
	run := readRestoreRun(t, c)
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the run started, want 0", got)
	}
	if !suspended(t, c) {
		t.Error("the Kustomization was not suspended, and Flux would put the replicas back")
	}
	if len(run.Status.Quiesced) != 1 || run.Status.Quiesced[0].Replicas != 2 || run.Status.QuiescedAt == nil {
		t.Errorf("quiesced = %+v at %v, want the Deployment recorded with its 2 replicas", run.Status.Quiesced, run.Status.QuiescedAt)
	}

	restoreStep(t, r) // the pod is still there
	if run := readRestoreRun(t, c); run.Status.Items[0].Phase != backupv1alpha1.ItemPending {
		t.Fatalf("volume item = %+v while the app's pod was still running, want Pending", run.Status.Items[0])
	}

	if err := c.Delete(context.Background(), writerPod()); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r) // restore the volume
	run = readRestoreRun(t, c)
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, run.Status.Items[0].Destination, rd)
	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{LastManualSync: string(restoreUID)}
	if err := c.Status().Update(context.Background(), rd); err != nil {
		t.Fatal(err)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while the volume restored, want 0", got)
	}

	restoreStep(t, r) // volume done, database deleted
	run = readRestoreRun(t, c)
	if run.Status.Items[1].Phase != backupv1alpha1.ItemDeleted {
		t.Fatalf("database item = %+v, want Deleted", run.Status.Items[1])
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the database was deleted, want the 2 it had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization is still suspended, so Flux never creates the Cluster again")
	}
	if run.Status.RestartedAt == nil {
		t.Error("restartedAt is unset")
	}
}

// oldInstance is the deleted Cluster's instance pod and its PVC, which
// Kubernetes removes only after Postgres has shut down.
func oldInstance() (*corev1.Pod, *corev1.PersistentVolumeClaim) {
	meta := func() metav1.ObjectMeta {
		return metav1.ObjectMeta{
			Name: pgN + "-1", Namespace: ns,
			Labels: map[string]string{clusterLabel: pgN},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: ClusterGVK.GroupVersion().String(), Kind: ClusterGVK.Kind, Name: pgN, UID: "old-cluster-uid",
			}},
		}
	}
	return &corev1.Pod{ObjectMeta: meta()}, &corev1.PersistentVolumeClaim{ObjectMeta: meta()}
}

// A Cluster's instance pod keeps running through its shutdown after the
// Cluster is gone. The run gives the app back and resumes Flux only once that
// pod and its PVC are gone, so the app never reaches the old Postgres and the
// new Cluster never waits behind the old one's names.
func TestAQuiescedRestoreWaitsForTheOldInstanceToShutDown(t *testing.T) {
	pod, pvc := oldInstance()
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), pod, pvc)

	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce
	restoreStep(t, r) // restore the volume
	run := readRestoreRun(t, c)
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, run.Status.Items[0].Destination, rd)
	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{LastManualSync: string(restoreUID)}
	if err := c.Status().Update(context.Background(), rd); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r) // volume done, database deleted
	restoreStep(t, r) // the old instance is still shutting down
	run = readRestoreRun(t, c)
	if run.Status.Items[1].Phase != backupv1alpha1.ItemDeleted {
		t.Fatalf("database item = %+v, want Deleted", run.Status.Items[1])
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d while pod %s-1 was still shutting down, want 0", got, pgN)
	}
	if !suspended(t, c) {
		t.Errorf("the Kustomization was resumed while pod %s-1 was still shutting down", pgN)
	}
	if !strings.Contains(readyMessage(run.Status.Conditions), pgN+"-1") {
		t.Errorf("ready message = %q, want it to name the instance the run waits for", readyMessage(run.Status.Conditions))
	}

	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the old instance was gone, want the 2 it had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization stayed suspended after the old instance was gone")
	}
	if msg := readyMessage(readRestoreRun(t, c).Status.Conditions); !strings.Contains(msg, "recreate "+pgN) {
		t.Errorf("ready message = %q, want the run asking for the Cluster to be created again", msg)
	}
}

// A quiesced restore that times out starts the app again and resumes the
// Kustomization it suspended.
func TestATimedOutQuiescedRestoreGivesTheAppBack(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())
	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce

	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed after the timeout", run.Status.Phase)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d after the run gave up, want the 2 it had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization the run suspended stayed suspended")
	}
}

// A spec.quiesce entry naming a workload the namespace does not hold fails
// the run at the checks, before it stops anything or deletes a database.
func TestAQuiescedRestoreOfAMissingWorkloadFailsBeforeStoppingAnything(t *testing.T) {
	run := quiescedRestore()
	run.Spec.Quiesce = append(run.Spec.Quiesce, backupv1alpha1.WorkloadRef{Kind: "StatefulSet", Name: "notes-worker"})
	r, c := restoreReconciler(t, prober{saturday}, run,
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())
	restoreStep(t, r)

	got := readRestoreRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(readyMessage(got.Status.Conditions), "notes-worker") {
		t.Fatalf("phase = %q, message = %q; want Failed naming notes-worker", got.Status.Phase, readyMessage(got.Status.Conditions))
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Errorf("replicas = %d, want the app untouched", replicas)
	}
}

// A restore without spec.quiesce leaves the app running and waits with reason
// ClaimInUse for its pod to stop. The backup.wlz.li/quiesce annotation only
// tells a BackupRun what to stop, so it makes no difference here.
func TestARestoreWithoutQuiesceLeavesTheAppRunning(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())
	restoreStep(t, r)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d, want the app left at 2", got)
	}
	if reason := readyReason(readRestoreRun(t, c).Status.Conditions); reason != backupv1alpha1.ReasonClaimInUse {
		t.Errorf("reason = %q, want ClaimInUse", reason)
	}
}

// A database restore to a moment before every base backup fails with reason
// NoBackupInReach and leaves the Cluster in place. Deleting it would bring it
// back empty.
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

// A namespace restore restores the volumes before the databases. When a volume
// restore fails, the run fails and skips the Cluster, which keeps running.
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

// A restore into a new claim fails with reason NoBackupInReach, and creates
// no claim, when no snapshot reaches its moment.
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

// refuseDeploymentPatches returns a client over c that refuses every patch to
// a Deployment, the way the API server does when the controller's
// ServiceAccount lacks patch on deployments.
func refuseDeploymentPatches(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				return apierrors.NewForbidden(appsv1.Resource("deployments"), obj.GetName(), errors.New("patch refused"))
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
}

// A quiesced restore that fails to stop a workload ends at once as Failed,
// with reason Failed, and resumes the Kustomization it already suspended. A
// BackupRun does the same. Waiting would leave the app running beside the
// restore until the timeout.
func TestAQuiescedRestoreThatCannotStopTheAppFailsAtOnce(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())
	r.Client = refuseDeploymentPatches(c)

	restoreStep(t, r) // plan
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err != nil {
		t.Fatalf("quiesce returned %v, want the run ended with the error in its status", err)
	}

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Fatalf("phase = %q, reason = %q; want Failed with reason Failed", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if !strings.Contains(readyMessage(run.Status.Conditions), "patch refused") {
		t.Errorf("ready message = %q, want the scale error", readyMessage(run.Status.Conditions))
	}
	if suspended(t, c) {
		t.Error("the Kustomization the run suspended stayed suspended")
	}
}

// A spec.quiesce workload deleted while the run holds it stopped ends the run
// as Failed, with reason Failed. The run has not timed out, so TimedOut would
// point at the wrong cause.
func TestAQuiescedRestoreWhoseWorkloadVanishesFails(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())
	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce

	if err := c.Delete(context.Background(), deployment()); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Fatalf("phase = %q, reason = %q; want Failed with reason Failed", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if !strings.Contains(readyMessage(run.Status.Conditions), appN) {
		t.Errorf("ready message = %q, want it to name %s", readyMessage(run.Status.Conditions), appN)
	}
}

// A failed read of a spec.quiesce workload while the run holds it stopped is
// returned for a retry, and the run keeps going. Only a workload that is gone
// ends the run.
func TestAQuiescedRestoreRetriesAFailedWorkloadRead(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())
	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce

	r.Reader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				return apierrors.NewServiceUnavailable("etcd leader changed")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Error("reconcile succeeded, want the read error returned for a retry")
	}
	if run := readRestoreRun(t, c); run.Status.Phase.Finished() {
		t.Errorf("phase = %q after a failed read, want the run still going", run.Status.Phase)
	}
}
