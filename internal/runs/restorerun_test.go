package runs

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// restoreUID is the UID of the RestoreRun back-to-monday. The run's
// restore Jobs take their names from it and carry it as their controller
// reference, so a test builds the Job a run would create.
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
// holds the given objects and the run's namespace, and the client itself.
// The reconciler runs on the frozen clock, its lister holds sunday's and
// monday's snapshots, its Prober returns the base backups passed in backups,
// and its restore Jobs run testImage.
func restoreReconciler(t *testing.T, backups prober, objects ...client.Object) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	c := newClient(t, objects...)
	return &RestoreRunReconciler{
		Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Prober: backups,
		RestoreImage: testImage, Now: func() time.Time { return frozen },
	}, c
}

// restoreStep reconciles the RestoreRun back-to-monday once and returns the
// result. An error from the reconcile fails the test.
//
// It then moves the reconciler's clock forward by the result's RequeueAfter,
// the time the controller waits before the next pass when no event comes
// sooner. restoreStepAtOnce leaves the clock where it is, for a pass that an
// event, such as the run's own status write, starts at once.
func restoreStep(t *testing.T, r *RestoreRunReconciler) ctrl.Result {
	t.Helper()
	result := restoreStepAtOnce(t, r)
	if result.RequeueAfter > 0 {
		advance(r, result.RequeueAfter)
	}
	return result
}

// restoreStepAtOnce reconciles the RestoreRun back-to-monday once, with the
// clock where it is, and returns the result. An error from the reconcile
// fails the test.
func restoreStepAtOnce(t *testing.T, r *RestoreRunReconciler) ctrl.Result {
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
// NoBackupInReach, names the oldest snapshot, and creates no restore Job.
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
	if names := movers(t, c); len(names) != 0 {
		t.Fatalf("movers = %v, want none", names)
	}
}

// A claim restore to a time between two snapshots selects the earlier one,
// records its full ID, and creates a restore Job that restores exactly that
// ID into the claim. Once the Job is Complete the run succeeds, and the Job
// does not outlive it.
func TestAClaimRestoreSelectsTheSnapshotBeforeItsMoment(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())

	restoreStep(t, r) // plan
	restoreStep(t, r) // restore

	item := readRestoreRun(t, c).Status.Items[0]
	if item.Snapshot != "2edf5bab" || item.SnapshotID != sunday.ID {
		t.Errorf("snapshot = %q (%s), want 2edf5bab, the one before 04:00, with its full ID", item.Snapshot, item.SnapshotID)
	}
	job := itemJob(t, c)
	if args := job.Spec.Template.Spec.Containers[0].Args; !slices.Contains(args, sunday.ID) {
		t.Errorf("restore args = %v, want them to name %s", args, sunday.ID)
	}

	completeJob(t, c)
	restoreStep(t, r)
	restoreStep(t, r) // the pass after the stop finds the restore Job stopped

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", run.Status.Phase)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want none once the run has ended", jobs)
	}
}

// quiet is a snapshot tagged quiesced, as a quiesced BackupRun leaves it after
// moving it to its restart moment. Its time falls between sunday's and
// monday's.
var quiet = restic.Snapshot{ID: fullID("c0ffee00"), Time: time.Date(2026, 9, 21, 3, 0, 5, 0, time.UTC), Hostname: "volsync", Paths: []string{"/data"}, Tags: []string{restic.QuiescedTag}}

// A synced restore selects the newest quiesced snapshot and passes over
// monday's newer untagged one. It records that snapshot's time in syncedTo,
// and the volume's restore Job restores exactly that snapshot by its full
// ID.
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
	job := itemJob(t, c)
	if got := job.Annotations[restorejob.AnnotationSnapshotID]; got != quiet.ID {
		t.Errorf("restore Job snapshot = %s, want the quiesced %s", got, quiet.ID)
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
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
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
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); ok {
		t.Fatal("the Cluster was not deleted")
	}
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonRecreate {
		t.Errorf("reason = %q, want the run waiting for the Cluster to be created again", readyReason(run.Status.Conditions))
	}

	// Flux or tofu creates the Cluster again and the webhook recovers it.
	recovered := cluster(func(u *unstructured.Unstructured) {
		u.SetUID("new-cluster-uid")
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

	_ = unstructured.SetNestedField(recovered.Object, cnpg.HealthyPhase, "status", "phase")
	if err := c.Status().Update(context.Background(), recovered); err != nil {
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
	return restoreRun(quiescedInPlace)
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
	k, _ := getUnstructured(t, c, quiesce.KustomizationGVK, "flux-system", appN)
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
	completeJob(t, c)
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while the volume restored, want 0", got)
	}

	restoreStep(t, r) // volume done, database deleted
	restoreStep(t, r) // the volume's stopped mover is gone
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
			Labels: map[string]string{cnpg.ClusterLabel: pgN},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: cnpg.ClusterGVK.GroupVersion().String(), Kind: cnpg.ClusterGVK.Kind, Name: pgN, UID: "old-cluster-uid",
			}},
		}
	}
	pod := &corev1.Pod{ObjectMeta: meta()}
	pod.Labels[cnpg.PodRoleLabel] = cnpg.PodRoleInstance
	return pod, &corev1.PersistentVolumeClaim{ObjectMeta: meta()}
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
	completeJob(t, c)

	restoreStep(t, r) // volume done, database deleted
	restoreStep(t, r) // the old instance is still shutting down
	run := readRestoreRun(t, c)
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

// A restore in place stops the workloads marked backup.wlz.li/quiesce, the
// set a BackupRun of the namespace stops, also when spec.quiesce is empty.
// The run scales them to zero before it creates a restore Job, and gives
// them back once the restore is done.
func TestARestoreStopsTheMarkedAppBeforeItsJob(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())

	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the run started, want 0", got)
	}
	if !suspended(t, c) {
		t.Error("the Kustomization was not suspended, and Flux would put the replicas back")
	}
	restoreStep(t, r) // the pod is still there
	jobs := &batchv1.JobList{}
	if err := c.List(context.Background(), jobs); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("restore Job %s exists while the app's pod still ran", jobs.Items[0].Name)
	}

	if err := c.Delete(context.Background(), writerPod()); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r) // restore the volume
	completeJob(t, c)
	restoreStep(t, r) // volume done, database deleted
	restoreStep(t, r) // the volume's stopped mover is gone
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the restore was done, want the 2 it had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization is still suspended after the restore")
	}
}

// A restore stops the workloads spec.quiesce lists together with the ones
// marked backup.wlz.li/quiesce, and gives each back its own count.
func TestARestoreStopsTheListedAndTheMarkedWorkloads(t *testing.T) {
	worker := deployment()
	worker.Name, worker.Annotations, worker.Labels = "notes-worker", nil, nil
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.All = true
		r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: worker.Name}}
	})
	r, c := restoreReconciler(t, prober{saturday}, run,
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), worker, kustomization(false), writerPod())
	replicas := func(name string) int32 {
		d := &appsv1.Deployment{}
		get(t, c, ns, name, d)
		return *d.Spec.Replicas
	}

	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce
	if replicas(appN) != 0 || replicas(worker.Name) != 0 {
		t.Fatalf("replicas = %d and %d after the run started, want both 0", replicas(appN), replicas(worker.Name))
	}
	if got := readRestoreRun(t, c).Status.Quiesced; len(got) != 2 {
		t.Errorf("quiesced = %+v, want the listed and the marked Deployment", got)
	}

	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r)
	if replicas(appN) != 2 || replicas(worker.Name) != 2 {
		t.Errorf("replicas = %d and %d after the run ended, want both back at 2", replicas(appN), replicas(worker.Name))
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
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
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
	restoreStep(t, r) // volume restore Job
	run := readRestoreRun(t, c)
	if run.Status.Items[1].Phase != backupv1alpha1.ItemPending {
		t.Fatalf("the Cluster was handled before its volumes finished: %+v", run.Status.Items[1])
	}

	endJob(t, c, itemJob(t, c).Name, batchv1.JobFailed, "PodFailurePolicy", "Container restore for pod notes/restore-x exited with code 10 matching FailJob rule at index 0")
	restoreStep(t, r)
	restoreStep(t, r) // the pass after the stop finds the restore Job stopped

	run = readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || run.Status.Items[1].Phase != backupv1alpha1.ItemSkipped {
		t.Fatalf("run = %+v, want Failed with the Cluster skipped", run.Status)
	}
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
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

// refuseDeploymentScales returns a client over c that refuses every write to
// a Deployment's scale subresource, the way the API server does when the
// controller's ServiceAccount lacks update on deployments/scale.
func refuseDeploymentScales(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := deploymentScale(sub, obj, opts); ok {
				return apierrors.NewForbidden(appsv1.Resource("deployments/scale"), obj.GetName(), errors.New("scale refused"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
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
	r.Client = refuseDeploymentScales(c)

	restoreStep(t, r) // plan
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err != nil {
		t.Fatalf("quiesce returned %v, want the run ended with the error in its status", err)
	}

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Fatalf("phase = %q, reason = %q; want Failed with reason Failed", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if !strings.Contains(readyMessage(run.Status.Conditions), "scale refused") {
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

// A quiesce pass whose status write is lost after the app was stopped is
// run again. The retry keeps the replica count and the Kustomization the
// first pass recorded, so a run that then times out gives the app its 2
// replicas back and resumes the Kustomization.
func TestAQuiescedRestoreRetriedAfterALostStatusWriteGivesTheAppBack(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), writerPod())
	restoreStep(t, r) // plan

	r.Client = loseStatusWriteAt(c, 0)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the quiesce pass succeeded, want its lost status write returned")
	}
	restoreStep(t, r) // quiesce again

	run := readRestoreRun(t, c)
	if len(run.Status.Quiesced) != 1 || run.Status.Quiesced[0].Replicas != 2 {
		t.Errorf("quiesced = %+v, want the Deployment with the 2 replicas it had", run.Status.Quiesced)
	}

	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d after the run gave up, want the 2 it had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization the run suspended stayed suspended")
	}
}

// staleCache returns a client over c whose pod and claim lists come back
// empty, the way the informer cache answers before it has seen a pod the API
// server already holds.
func staleCache(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *corev1.PodList, *corev1.PersistentVolumeClaimList:
				return nil
			}
			return cl.List(ctx, list, opts...)
		},
	})
}

// A restore in place looks for a pod mounting the claim through the uncached
// reader. A pod the informer cache has not seen yet still makes the run wait
// with reason ClaimInUse, because without spec.quiesce that check is the only
// thing that keeps a second writer off the volume.
func TestARestoreSeesAPodTheCacheHasNotSeenYet(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository(), writerPod())
	r.Client = staleCache(c)
	restoreStep(t, r) // plan
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if reason := readyReason(run.Status.Conditions); reason != backupv1alpha1.ReasonClaimInUse || run.Status.Items[0].Phase != backupv1alpha1.ItemPending {
		t.Fatalf("reason = %q, item = %+v; want ClaimInUse with the item Pending", reason, run.Status.Items[0])
	}
}

// A database restore looks for the deleted Cluster's instance pods through the
// uncached reader. An instance pod the informer cache has not seen yet keeps
// the run waiting with reason WaitingForShutdown and the app stopped.
func TestADatabaseRestoreSeesAnInstanceTheCacheHasNotSeenYet(t *testing.T) {
	pod, pvc := oldInstance()
	r, c := restoreReconciler(t, prober{saturday}, quiescedRestore(),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
		deployment(), kustomization(false), pod, pvc)
	r.Client = staleCache(c)

	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce
	restoreStep(t, r) // restore the volume
	completeJob(t, c)
	restoreStep(t, r) // volume done, database deleted

	if reason := readyReason(readRestoreRun(t, c).Status.Conditions); reason != backupv1alpha1.ReasonShutdown {
		t.Errorf("reason = %q, want WaitingForShutdown while pod %s-1 is still there", reason, pgN)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d while the old instance was still there, want 0", got)
	}
}

// unavailable returns a client over c whose reads answer 503 Service
// Unavailable, the way the API server does while etcd elects a leader. A Get
// fails when match accepts the object read into, and a List fails when match
// accepts the list.
func unavailable(c client.Client, match func(any) bool) client.Client {
	refused := apierrors.NewServiceUnavailable("etcd leader changed")
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if match(obj) {
				return refused
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if match(list) {
				return refused
			}
			return cl.List(ctx, list, opts...)
		},
	})
}

// is reports whether the value given is of type T. A test passes it to
// unavailable, instantiated with the type whose reads should fail.
func is[T any](v any) bool {
	_, ok := v.(T)
	return ok
}

// objectStores matches an unstructured ObjectStore read, for unavailable.
func objectStores(v any) bool {
	u, ok := v.(*unstructured.Unstructured)
	return ok && u.GetKind() == "ObjectStore"
}

// reconcileExpectingRetry reconciles back-to-monday once and fails the test
// unless the reconcile returned an error, the way a failed read is handed back
// for a retry, and the run is still unfinished afterwards.
func reconcileExpectingRetry(t *testing.T, r *RestoreRunReconciler, c client.Client) *backupv1alpha1.RestoreRun {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Error("reconcile succeeded, want the read error returned for a retry")
	}
	run := readRestoreRun(t, c)
	if run.Status.Phase.Finished() {
		t.Errorf("phase = %q, reason = %q after a failed read, want the run still going", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	return run
}

// A read that fails with a server error while a run checks its items is
// returned for a retry. The run neither ends as Invalid nor reports the item
// as having no backup in reach, because the next attempt may well succeed.
func TestARestorePlanRetriesAFailedRead(t *testing.T) {
	cases := []struct {
		name  string
		spec  func(*backupv1alpha1.RestoreRun)
		match func(any) bool
	}{
		{"into, claim read", func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "notes-data-monday" }, is[*corev1.PersistentVolumeClaim]},
		{"all, claim list", func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }, is[*corev1.PersistentVolumeClaimList]},
		{"claim, VolumeRestore read", func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, is[*backupv1alpha1.VolumeRestore]},
		{"claim, repository Secret read", func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, is[*corev1.Secret]},
		{"database, ObjectStore read", func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }, objectStores},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c := restoreReconciler(t, prober{saturday}, restoreRun(tc.spec),
				claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())
			r.Reader = unavailable(c, tc.match)
			run := reconcileExpectingRetry(t, r, c)
			for _, item := range run.Status.Items {
				if item.Phase == backupv1alpha1.ItemFailed {
					t.Errorf("item %s %s failed with %q after a failed read", item.Kind, item.Name, item.Message)
				}
			}
		})
	}
}

// A failed read of a claim's VolumeRestore when its restore is about to start
// is returned for a retry. The volume item stays Pending, and the database of
// a namespace restore is not skipped as if the volume had failed.
func TestARestoreRetriesAFailedReadBeforeAVolumeStarts(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday}, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())
	restoreStep(t, r) // plan

	r.Reader = unavailable(c, is[*backupv1alpha1.VolumeRestore])
	run := reconcileExpectingRetry(t, r, c)
	if run.Status.Items[0].Phase != backupv1alpha1.ItemPending || run.Status.Items[1].Phase != backupv1alpha1.ItemPending {
		t.Errorf("items = %+v, want both still Pending", run.Status.Items)
	}
}

// brokenLister is a SnapshotLister whose every listing fails with err, the
// way restic fails against a repository whose password is wrong.
type brokenLister struct{ err error }

// Snapshots returns the lister's error.
func (b brokenLister) Snapshots(context.Context, *corev1.Secret) ([]restic.Snapshot, error) {
	return nil, b.err
}

// A run whose repository can't be listed says why on its Ready condition while
// it retries, so `kubectl get` shows more than an empty phase. Once
// spec.timeout has passed since the run was created, it gives up as Failed
// with reason TimedOut, although it never started.
func TestARestoreWhoseChecksKeepFailingSaysWhyAndTimesOut(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) {
			r.Spec.Claim = claimN
			r.CreationTimestamp = metav1.NewTime(frozen)
		}),
		claim(), volumeRestore(), repository())
	r.Snapshots = brokenLister{errors.New("Fatal: wrong password or no key found")}

	run := reconcileExpectingRetry(t, r, c)
	if cond := readyMessage(run.Status.Conditions); !strings.Contains(cond, "wrong password") {
		t.Errorf("ready message = %q, want the listing error", cond)
	}
	if run.Status.Phase != "" {
		t.Errorf("phase = %q, want the run still unplanned", run.Status.Phase)
	}

	r.Now = func() time.Time { return frozen.Add(4 * time.Hour) }
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err != nil {
		t.Fatalf("reconcile at the timeout returned %v, want the run ended", err)
	}
	run = readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Fatalf("phase = %q, reason = %q at the timeout; want Failed, TimedOut", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if cond := readyMessage(run.Status.Conditions); !strings.Contains(cond, "wrong password") {
		t.Errorf("ready message = %q, want the error the run gave up on", cond)
	}
}

// A database restore whose delete of the Cluster failed deletes it again on
// the next pass. The old Cluster still carries backup.wlz.li/restore-run with
// this run's name, left there by an earlier run named back-to-monday, and
// that must not count as the recovery. Only a Cluster created again, with
// another UID, does.
func TestADatabaseRestoreWhoseDeleteFailedDeletesTheOldClusterAgain(t *testing.T) {
	old := cluster(func(u *unstructured.Unstructured) {
		annotations := u.GetAnnotations()
		annotations[backupv1alpha1.AnnotationRestoreRun] = "back-to-monday"
		u.SetAnnotations(annotations)
		_ = unstructured.SetNestedField(u.Object, cnpg.HealthyPhase, "status", "phase")
	})
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }), old, objectStore(), storeSecret())
	restoreStep(t, r) // plan

	refused := false
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == cnpg.ClusterGVK.Kind && !refused {
				refused = true
				return apierrors.NewServiceUnavailable("etcd leader changed")
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose delete failed succeeded, want the error returned")
	}
	restoreStep(t, r)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase.Finished() || run.Status.Items[0].Phase != backupv1alpha1.ItemDeleted {
		t.Errorf("phase = %q, item = %+v; want the run waiting with the item Deleted", run.Status.Phase, run.Status.Items[0])
	}
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); ok {
		t.Error("the old Cluster is still there; the run took it for its recovery")
	}
}

// optedOut is a mutate function for cluster that adds the annotation asking
// the bootstrap webhook to leave the Cluster empty.
func optedOut(u *unstructured.Unstructured) {
	annotations := u.GetAnnotations()
	annotations[bootstrap.OptOutAnnotation] = bootstrap.OptOutValue
	u.SetAnnotations(annotations)
}

// A namespace restore skips a Cluster that opts out of the bootstrap webhook
// with backup.wlz.li/bootstrap: initdb, names the annotation in the item's
// message, and never deletes it. The webhook would let the Cluster come back
// empty, and the run would delete it again and again. The volumes restore as
// usual, with and without syncDatabaseToVolume.
func TestANamespaceRestoreLeavesAnOptedOutClusterAlone(t *testing.T) {
	for _, sync := range []bool{false, true} {
		t.Run(map[bool]string{false: "plain", true: "synced"}[sync], func(t *testing.T) {
			r, c := restoreReconciler(t, prober{saturday},
				restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All, r.Spec.SyncDatabaseToVolume = true, sync }),
				claim(), volumeRestore(), repository(), cluster(optedOut), objectStore(), storeSecret())
			r.Snapshots = snapshots{sunday, quiet, monday}

			restoreStep(t, r) // plan
			run := readRestoreRun(t, c)
			if item := run.Status.Items[1]; item.Phase != backupv1alpha1.ItemSkipped || !strings.Contains(item.Message, bootstrap.OptOutAnnotation) {
				t.Fatalf("database item = %+v (%s), want Skipped naming %s", item, readyMessage(run.Status.Conditions), bootstrap.OptOutAnnotation)
			}
			restoreStep(t, r) // restore the volume
			completeJob(t, c)
			restoreStep(t, r)
			restoreStep(t, r) // the pass after the stop finds the restore Job stopped

			run = readRestoreRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded {
				t.Errorf("phase = %q, items = %+v; want Succeeded with the volume restored", run.Status.Phase, run.Status.Items)
			}
			if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
				t.Error("the opted-out Cluster was deleted")
			}
		})
	}
}

// A namespace restore whose every item is Skipped restores nothing, and ends
// Failed with reason NoBackupInReach and a message that says so and gives
// each item's reason. Before, it ended Succeeded saying every item holds the
// restored data (finding H).
func TestARestoreThatSkippedEveryItemFails(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }), cluster(optedOut), objectStore(), storeSecret())
	restoreStep(t, r) // plan
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	want := "nothing was restored: Cluster " + pgN + ": " + run.Status.Items[0].Message
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach ||
		readyMessage(run.Status.Conditions) != want {
		t.Errorf("phase = %q, reason = %q, message = %q; want Failed, NoBackupInReach, %q",
			run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions), want)
	}
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
		t.Error("the opted-out Cluster was deleted")
	}
}

// A restore of one database that opts out of the bootstrap webhook ends as
// Invalid at its checks, naming the annotation, and leaves the Cluster alone.
func TestARestoreOfAnOptedOutDatabaseIsInvalid(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }), cluster(optedOut), objectStore(), storeSecret())
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
		!strings.Contains(readyMessage(run.Status.Conditions), bootstrap.OptOutAnnotation) {
		t.Fatalf("phase = %q, reason = %q, message = %q; want Failed, Invalid, naming %s",
			run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions), bootstrap.OptOutAnnotation)
	}
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
		t.Error("the opted-out Cluster was deleted")
	}
}

// A restore refused at its checks whose release fails keeps its refusal:
// the pass that refused it records the ending Invalid and fails to list its
// Leases, the Cluster opts back in between, and the next pass ends the run
// Invalid with the refusal's message. It never plans the run again.
func TestARefusedRestoreWhoseReleaseFailedEndsInvalid(t *testing.T) {
	c := newClient(t, restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Database = pgN
		r.Finalizers = []string{Finalizer}
	}), cluster(optedOut), objectStore(), storeSecret())
	failList := true
	reader := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*coordinationv1.LeaseList); ok && failList {
				return apierrors.NewInternalError(errors.New("the API server cannot list the Leases"))
			}
			return cl.List(ctx, list, opts...)
		},
	})
	r := &RestoreRunReconciler{Client: c, Reader: reader, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday},
		Now: func() time.Time { return frozen }}

	if err := tryRestoreStep(r); err == nil {
		t.Fatal("the pass whose release failed returned no error")
	}
	refusal := ""
	if ending := readRestoreRun(t, c).Status.Ending; ending != nil {
		refusal = ending.Message
	}

	back, _ := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN)
	annotations := back.GetAnnotations()
	delete(annotations, bootstrap.OptOutAnnotation)
	back.SetAnnotations(annotations)
	if err := c.Update(context.Background(), back); err != nil {
		t.Fatal(err)
	}
	failList = false
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
		!strings.Contains(readyMessage(run.Status.Conditions), bootstrap.OptOutAnnotation) {
		t.Fatalf("phase = %q, reason = %q, message = %q; want Failed, Invalid, the refusal naming %s", run.Status.Phase,
			readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions), bootstrap.OptOutAnnotation)
	}
	if got := readyMessage(run.Status.Conditions); got != refusal {
		t.Errorf("message = %q, want the ending the failed pass recorded, %q", got, refusal)
	}
}

// loseNextStatusWrite returns a client over c that fails the first status
// write of a RestoreRun with a conflict, the way a lost write or a controller
// crash loses it.
func loseNextStatusWrite(c client.Client) client.Client {
	lost := false
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := obj.(*backupv1alpha1.RestoreRun); ok && !lost {
				lost = true
				return apierrors.NewConflict(backupv1alpha1.GroupVersion.WithResource("restoreruns").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

// A volume restore whose Job completed, but whose pass lost the status write
// that recorded it, finishes on the next pass. The run records the item's
// success before it stops the restore Job, so the next pass still reads the
// Job's Complete condition, and the item does not fail as a Job deleted
// before it finished.
func TestAVolumeRestoreWhoseSuccessWasNotRecordedFinishes(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // restore
	completeJob(t, c)

	r.Client = loseNextStatusWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose status write was lost succeeded, want the error returned")
	}
	restoreStep(t, r)
	restoreStep(t, r) // the pass after the stop finds the restore Job stopped

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded {
		t.Fatalf("phase = %q, item = %+v; want Succeeded", run.Status.Phase, run.Status.Items[0])
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want none once the run has ended", jobs)
	}
}

// previousOne is a mutate function for restoreRun that sets spec.previous to
// 1.
func previousOne(r *backupv1alpha1.RestoreRun) {
	one := int32(1)
	r.Spec.Previous = &one
}

// An into restore with spec.previous fills its claim from the snapshot it
// recorded: its restore Job restores that snapshot's full ID, so nothing
// steps back again from a newer snapshot.
func TestAnIntoRestoreWithPreviousRestoresTheSnapshotItRecorded(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "notes-data-monday" }, previousOne),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create

	run := readRestoreRun(t, c)
	if run.Status.Items[0].Snapshot != sunday.ShortID() {
		t.Fatalf("snapshot = %q, want %s, one before the newest", run.Status.Items[0].Snapshot, sunday.ShortID())
	}
	job := itemJob(t, c)
	if got := job.Annotations[restorejob.AnnotationSnapshotID]; got != sunday.ID || !slices.Contains(job.Spec.Template.Spec.Containers[0].Args, sunday.ID) {
		t.Errorf("restore Job snapshot = %s, args = %v; want sunday's full ID %s", got, job.Spec.Template.Spec.Containers[0].Args, sunday.ID)
	}
}

// An in-place restore hands its restore Job the full ID of the snapshot its
// checks selected. A backup taken between the checks and the start would
// shift what "newest" or previous means, and the Job does not pick again.
func TestAClaimRestoreHandsTheMoverTheSnapshotItSelected(t *testing.T) {
	early := sunday
	early.Time = sunday.Time.Add(700 * time.Millisecond)
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, previousOne),
		claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{early, monday}
	restoreStep(t, r) // plan

	tuesday := restic.Snapshot{ID: fullID("7a11ce00"), Time: monday.Time.Add(24 * time.Hour), Hostname: "volsync", Paths: []string{"/data"}}
	r.Snapshots = snapshots{early, monday, tuesday}
	restoreStep(t, r) // restore

	job := itemJob(t, c)
	if got := job.Annotations[restorejob.AnnotationSnapshotID]; got != early.ID {
		t.Errorf("restore Job snapshot = %s, want %s, the one the checks selected", got, early.ID)
	}
	if args := job.Spec.Template.Spec.Containers[0].Args; !slices.Contains(args, early.ID) {
		t.Errorf("restore args = %v, want them to name %s", args, early.ID)
	}
}

// fromRepository is a mutate function for restoreRun that restores the
// repository Secret into a new 2Gi claim named scratch, with no source claim.
func fromRepository(r *backupv1alpha1.RestoreRun) {
	size := resource.MustParse("2Gi")
	r.Spec.Repository, r.Spec.Into, r.Spec.IntoSize = repoN, "scratch", &size
}

// An into restore from a repository no claim backs up to has no source claim
// to take a node from, and a claim on a WaitForFirstConsumer class gets one
// only from the pod that first uses it. So the run creates a plain claim of
// spec.intoSize and a restore Job that writes the selected snapshot into
// it. The Job's pod is the claim's first consumer, and the scheduler places
// the claim with it. The run succeeds once the Job is Complete, and deletes
// the Job.
func TestAnIntoRestoreFromARepositoryFillsAClaimTheMoverPlaces(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create

	scratch := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "scratch", scratch)
	if request := scratch.Spec.Resources.Requests[corev1.ResourceStorage]; request.Cmp(resource.MustParse("2Gi")) != 0 ||
		scratch.Spec.DataSourceRef != nil || scratch.Annotations[selectedNodeAnnotation] != "" {
		t.Fatalf("claim request = %s, dataSourceRef = %v, annotations = %v; want 2Gi, none and no node", request.String(), scratch.Spec.DataSourceRef, scratch.Annotations)
	}
	job := itemJob(t, c)
	pod := job.Spec.Template.Spec
	if jobClaim(job) != "scratch" || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID || pod.NodeName != "" || len(pod.NodeSelector) != 0 {
		t.Fatalf("Job claim = %q, annotations = %v, node %q, selector %v; want monday's snapshot written into scratch with no placement of its own",
			jobClaim(job), job.Annotations, pod.NodeName, pod.NodeSelector)
	}

	completeJob(t, c)
	run := stepUntilFinished(t, r, c, 3)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded {
		t.Fatalf("phase = %q, item = %+v; want Succeeded", run.Status.Phase, run.Status.Items[0])
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want the Job gone with the run's end", jobs)
	}
}

// An into restore from a repository without spec.intoSize ends as Invalid at
// its checks. With no source claim there is no size to copy, and the API
// server refuses a claim without a storage request.
func TestAnIntoRestoreFromARepositoryNeedsASize(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository, func(r *backupv1alpha1.RestoreRun) { r.Spec.IntoSize = nil }), repository())
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid || !strings.Contains(readyMessage(run.Status.Conditions), "intoSize") {
		t.Fatalf("reason = %q, message = %q; want Invalid naming intoSize", readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
}

// An into restore whose claim the API server keeps refusing still times out.
// The run checks its deadline before it creates anything, so an error from
// the create can't keep it waiting past spec.timeout.
func TestAnIntoRestoreTimesOutWhileItsClaimIsRefused(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "notes-data-monday" }),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan

	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok {
				return apierrors.NewServiceUnavailable("admission webhook timed out")
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err != nil {
		t.Fatalf("reconcile past the timeout returned %v, want the run ended", err)
	}
	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Fatalf("phase = %q, reason = %q; want Failed, TimedOut", run.Status.Phase, readyReason(run.Status.Conditions))
	}
}

// declaring returns a mutate function for cluster that gives the Cluster its
// own bootstrap method in spec.bootstrap, with the given content.
func declaring(method string, content map[string]any) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		if err := unstructured.SetNestedMap(u.Object, map[string]any{method: content}, "spec", "bootstrap"); err != nil {
			panic(err)
		}
	}
}

// A namespace restore skips a Cluster that declares its own bootstrap method,
// names the method in the item's message, and never deletes it. Flux would
// create it again with that method, the webhook would refuse it while the run
// waits, and the database would stay down until the timeout. The volumes
// restore as usual.
func TestANamespaceRestoreLeavesAClusterWithADeclaredBootstrapAlone(t *testing.T) {
	for name, method := range map[string]func(*unstructured.Unstructured){
		"pg_basebackup": declaring("pg_basebackup", map[string]any{"source": "legacy-db"}),
		"recovery":      declaring("recovery", map[string]any{"source": "owners-archive"}),
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, prober{saturday},
				restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }),
				claim(), volumeRestore(), repository(), cluster(method), objectStore(), storeSecret())

			restoreStep(t, r) // plan
			run := readRestoreRun(t, c)
			if item := run.Status.Items[1]; item.Phase != backupv1alpha1.ItemSkipped || !strings.Contains(item.Message, "spec.bootstrap."+name) {
				t.Fatalf("database item = %+v (%s), want Skipped naming spec.bootstrap.%s", item, readyMessage(run.Status.Conditions), name)
			}
			restoreStep(t, r) // restore the volume
			completeJob(t, c)
			restoreStep(t, r)
			restoreStep(t, r) // the pass after the stop finds the restore Job stopped

			run = readRestoreRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded {
				t.Errorf("phase = %q, items = %+v; want Succeeded with the volume restored", run.Status.Phase, run.Status.Items)
			}
			if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
				t.Errorf("the Cluster declaring %s was deleted", name)
			}
		})
	}
}

// A restore of one database that declares its own bootstrap method ends as
// Invalid at its checks, naming the method, and leaves the Cluster alone.
func TestARestoreOfADatabaseWithADeclaredBootstrapIsInvalid(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }),
		cluster(declaring("pg_basebackup", map[string]any{"source": "legacy-db"})), objectStore(), storeSecret())
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
		!strings.Contains(readyMessage(run.Status.Conditions), "spec.bootstrap.pg_basebackup") {
		t.Fatalf("phase = %q, reason = %q, message = %q; want Failed, Invalid, naming spec.bootstrap.pg_basebackup",
			run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); !ok {
		t.Error("the Cluster declaring pg_basebackup was deleted")
	}
}

// A Cluster the webhook recovered in an earlier restore carries the recovery
// the webhook wrote, with source backup-controller. That recovery is the
// controller's own, so a later restore restores the Cluster as usual.
func TestARestoreOfAClusterTheWebhookRecoveredBeforeDeletesIt(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }),
		cluster(declaring("recovery", map[string]any{"source": bootstrap.RecoverySource})), objectStore(), storeSecret())
	restoreStep(t, r) // plan
	restoreStep(t, r) // delete

	run := readRestoreRun(t, c)
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemDeleted {
		t.Fatalf("phase = %q, item = %+v (%s); want the Cluster deleted for recovery", run.Status.Phase, item, readyMessage(run.Status.Conditions))
	}
}
