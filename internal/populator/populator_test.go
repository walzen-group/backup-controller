package populator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestPopulateCreatesTheJobForTheNewestSnapshot checks that Populate names
// the restore Job restore-<claim UID> and the Secret copy after the claim UID,
// and creates the Job suspended, for the full ID of the newest snapshot, into
// the prime claim, owned by the prime claim, from the Secret copy.
func TestPopulateCreatesTheJobForTheNewestSnapshot(t *testing.T) {
	ops := populatedOperations(t)
	callbacks := newCallbacks(ops, monday, tuesday)
	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	job := ops.job(t, "claim-123")
	if job == nil {
		t.Fatal("no Job restore-claim-123")
	}
	if _, ok := ops.secrets[namespacedName(controllerNS, "claim-123")]; !ok {
		t.Fatalf("secret names = %v, want claim-123", mapKeys(ops.secrets))
	}
	if !ptr.Deref(job.Spec.Suspend, false) {
		t.Error("the Job was created running, want suspended")
	}
	if got := job.Annotations[restorejob.AnnotationSnapshotID]; got != tuesday.ID {
		t.Errorf("snapshot = %s, want the newest, %s", got, tuesday.ID)
	}
	if got := job.Spec.Template.Spec.Volumes[0].PersistentVolumeClaim.ClaimName; got != PrimeClaimName("claim-123") {
		t.Errorf("claim = %s, want the prime claim", got)
	}
	if got := job.Spec.Template.Spec.Containers[0].EnvFrom[0].SecretRef.Name; got != "claim-123" {
		t.Errorf("Secret = %s, want the copy", got)
	}
	if len(job.OwnerReferences) != 1 || job.OwnerReferences[0].UID != "prime-uid-123" {
		t.Errorf("owner references = %+v, want the prime claim", job.OwnerReferences)
	}
	if got := job.Labels[restorejob.LabelRestoreClaim]; got != "claim-123" {
		t.Errorf("claim label = %q, want claim-123", got)
	}
}

// TestAPopulatorJobRunsOnlyOnceItsUIDIsRecorded checks the order of a
// restore: the Job is created suspended and its UID recorded on the prime
// claim in the first sync; only a later sync, which reads that record back,
// resumes it; and once the Job controller runs it to Complete, Complete
// reports the claim filled. restorejob.Build creates every Job suspended, so
// a populator that never resumed would leave every claim Pending for good.
func TestAPopulatorJobRunsOnlyOnceItsUIDIsRecorded(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	callbacks := newCallbacks(ops, monday)

	if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
		t.Fatalf("first sync = %t, %v; want not done", done, err)
	}
	job := ops.job(t, "claim-123")
	if job == nil || !ptr.Deref(job.Spec.Suspend, false) {
		t.Fatalf("after the first sync the Job is %+v, want it created and still suspended", job)
	}
	if got := ops.prime(t, "claim-123").Annotations[AnnotationJobUID]; got != string(job.UID) {
		t.Fatalf("prime claim records %q, want the Job's UID %s", got, job.UID)
	}
	ops.setJob(t, "claim-123", func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })

	if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
		t.Fatalf("second sync = %t, %v; want not done", done, err)
	}
	if ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, true) {
		t.Fatal("the recorded Job is still suspended after the second sync, so it never runs")
	}

	pod := ops.createPod(t, job, "restore-claim-123-x7k2p", "node-a", corev1.PodRunning)
	if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
		t.Fatalf("sync while the pod runs = %t, %v; want not done", done, err)
	}
	ops.setPod(t, pod, func(p *corev1.Pod) { p.Status.Phase = corev1.PodSucceeded })
	ops.setJob(t, "claim-123", complete)
	if done, err := librarySync(ctx, callbacks, params()); err != nil || !done {
		t.Fatalf("sync after the Job completed = %t, %v; want done", done, err)
	}
	if n := len(ops.cluster.Cascades()); n != 0 {
		t.Errorf("deletes = %d, want none before Cleanup", n)
	}
}

// TestCompleteTrustsOnlyTheJob checks that Complete reports the restore
// finished only for a Job with Complete=True.
func TestCompleteTrustsOnlyTheJob(t *testing.T) {
	for name, test := range map[string]struct {
		change func(*batchv1.Job)
		want   bool
	}{
		"complete":  {change: complete, want: true},
		"running":   {change: func(*batchv1.Job) {}},
		"failed":    {change: failed},
		"suspended": {change: func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) }},
	} {
		t.Run(name, func(t *testing.T) {
			ops := restoringOperations(t)
			ops.setJob(t, "claim-123", func(j *batchv1.Job) { j.Status.Conditions = nil; test.change(j) })
			got, err := newCallbacks(ops, monday).Complete(context.Background(), params())
			if err != nil || got != test.want {
				t.Fatalf("Complete() = %t, %v, want %t, nil", got, err, test.want)
			}
		})
	}
}

// TestAnEmptyRepositoryBindsAnEmptyClaim checks that a claim whose
// repository holds no snapshot at all, and that nothing pins, gets no Job and
// binds its empty volume, as a first deploy does.
func TestAnEmptyRepositoryBindsAnEmptyClaim(t *testing.T) {
	ops := populatedOperations(t)
	done, err := librarySync(context.Background(), newCallbacks(ops), params())
	if err != nil || !done {
		t.Fatalf("sync = %t, %v; want done", done, err)
	}
	if job := ops.job(t, "claim-123"); job != nil {
		t.Errorf("Job %s created for an empty repository", job.Name)
	}
	ready := findCondition(ops.statuses[len(ops.statuses)-1].Status.Conditions, backupv1alpha1.ConditionReady)
	if want := "waiting for claim notes to bind its empty volume, since its repository holds no snapshot to restore"; ready == nil || ready.Message != want {
		t.Errorf("Ready = %#v, want the message %q", ready, want)
	}
}

// failingLister is a SnapshotLister whose listing fails.
type failingLister struct{}

func (failingLister) Snapshots(context.Context, *corev1.Secret) ([]restic.Snapshot, error) {
	return nil, errors.New("S3 is away")
}

// TestAListingErrorNeverBindsEmpty checks that a repository that can't be
// listed never counts as empty: Populate and Complete return the error and
// the claim stays Pending.
func TestAListingErrorNeverBindsEmpty(t *testing.T) {
	ops := populatedOperations(t)
	callbacks := New(ops, controllerNS, testImage, failingLister{})
	if done, err := librarySync(context.Background(), callbacks, params()); err == nil || done {
		t.Fatalf("sync = %t, %v; want an error", done, err)
	}
	if done, err := callbacks.Complete(context.Background(), params()); err == nil || done {
		t.Fatalf("Complete() = %t, %v; want an error", done, err)
	}
}

// TestSnapshotsOfAnotherLayoutNeverBindEmpty checks that a repository whose
// snapshots all have another layout than a VolSync mover's gives no Job and
// never binds the claim empty: the claim stays Pending with NoBackupInReach,
// and the message names the snapshots passed over.
func TestSnapshotsOfAnotherLayoutNeverBindEmpty(t *testing.T) {
	ops := populatedOperations(t)
	other := monday
	other.Hostname = "laptop"
	callbacks := newCallbacks(ops, other)

	done, err := librarySync(context.Background(), callbacks, params())
	if err == nil || done {
		t.Fatalf("sync = %t, %v; want an error and not done", done, err)
	}
	if job := ops.job(t, "claim-123"); job != nil {
		t.Errorf("Job %s created from a snapshot of another layout", job.Name)
	}
	last := ops.statuses[len(ops.statuses)-1]
	ready := findCondition(last.Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || ready.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(ready.Message, "the repository holds 1 snapshot, which no VolSync mover wrote (host volsync, paths [/data]): 6e473100 (host laptop") {
		t.Fatalf("Ready = %#v, want NoBackupInReach naming the one snapshot passed over", ready)
	}
	if got, err := callbacks.Complete(context.Background(), params()); err != nil || got {
		t.Errorf("Complete() = %t, %v; want false", got, err)
	}
}

// jobFailedWithExit1 marks the claim's Job Failed after its pod ended with
// restic's exit code 1, as the Job controller and the kubelet leave it.
func jobFailedWithExit1(t *testing.T, ops *fakeOperations, job *batchv1.Job) *corev1.Pod {
	t.Helper()
	pod := ops.createPod(t, job, "restore-claim-123-x7k2p", "node-a", corev1.PodRunning)
	ops.setPod(t, pod, func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodFailed
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "restore", State: corev1.ContainerState{
			Terminated: &corev1.ContainerStateTerminated{ExitCode: 1, Message: "Fatal: failed to find snapshot"},
		}}}
	})
	ops.setJob(t, "claim-123", failed)
	return pod
}

// TestAFailedJobKeepsRestoreFailedUntilReplaced checks F3: a failed Job is
// recorded on the status as RestoreFailed with restic's exit code before it
// is stopped, the entry keeps that reason while the Job is being deleted,
// no Job is created until the old one is gone, and the entry reads
// Restoring once the new Job exists. The syncs run in the library's order,
// so Complete never sees a Job Populate is taking down. Both Jobs restore
// with --delete: the new one selects the snapshot again, here a newer one, and
// restores it over what the failed Job left, so files of the earlier snapshot
// would stay behind without the flag.
func TestAFailedJobKeepsRestoreFailedUntilReplaced(t *testing.T) {
	ctx := context.Background()
	ops := newStrictOperations(t, backupv1alpha1.VolumeRestoreStatus{})
	callbacks := newCallbacks(ops, monday)
	if _, err := librarySync(ctx, callbacks, ops.paramsFor(t, "notes", "claim-123")); err != nil {
		t.Fatalf("first sync: %v", err)
	}
	job := ops.job(t, "claim-123")
	ops.setJob(t, "claim-123", func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })
	if _, err := librarySync(ctx, callbacks, ops.paramsFor(t, "notes", "claim-123")); err != nil {
		t.Fatalf("resume sync: %v", err)
	}
	pod := jobFailedWithExit1(t, ops.fakeOperations, job)
	// The pod keeps the Job controller's tracking finalizer until the Job
	// controller has counted it, so the foreground delete waits for it.
	ops.addTracking(t, pod)

	done, err := librarySync(ctx, callbacks, ops.paramsFor(t, "notes", "claim-123"))
	if err == nil || done {
		t.Fatalf("sync of the failed Job = %t, %v; want an error", done, err)
	}
	assertEntry(t, ops, backupv1alpha1.RestorePhaseFailed, backupv1alpha1.ReasonRestoreFailed, "restic exited 1")
	stopped := ops.job(t, "claim-123")
	if stopped == nil || stopped.DeletionTimestamp == nil || stopped.UID != job.UID {
		t.Fatalf("the failed Job is %+v, want it being deleted", stopped)
	}

	creates := ops.jobCreates
	if _, err := librarySync(ctx, callbacks, ops.paramsFor(t, "notes", "claim-123")); err == nil {
		t.Fatal("sync while the failed Job is being deleted returned no error")
	}
	if ops.jobCreates != creates {
		t.Errorf("Job creates while the old Job is being deleted = %d, want none", ops.jobCreates-creates)
	}
	assertEntry(t, ops, backupv1alpha1.RestorePhaseFailed, backupv1alpha1.ReasonRestoreFailed, "restic exited 1")
	if got := ops.job(t, "claim-123"); got == nil || got.UID != job.UID {
		t.Fatalf("Job = %+v while the old one is being deleted, want the old one and no create", got)
	}

	ops.removeTracking(t, pod, "claim-123")
	if ops.job(t, "claim-123") != nil {
		t.Fatal("the old Job is still there once its pod is gone")
	}
	if _, err := librarySync(ctx, newCallbacks(ops, monday, tuesday), ops.paramsFor(t, "notes", "claim-123")); err != nil {
		t.Fatalf("sync once the old Job is gone: %v", err)
	}
	replaced := ops.job(t, "claim-123")
	if replaced == nil || replaced.UID == job.UID || replaced.Annotations[restorejob.AnnotationSnapshotID] != tuesday.ID {
		t.Fatalf("Job = %+v, want a new one for the newer snapshot", replaced)
	}
	if !hasDelete(job) || !hasDelete(replaced) {
		t.Error("a restore Job restores without --delete")
	}
	assertEntry(t, ops, backupv1alpha1.RestorePhaseRestoring, backupv1alpha1.ReasonRestoring, "restore-claim-123")
}

// TestAJobDeletedByAPersonWaitsForItsPods checks that a restore Job a person
// deleted while its pod runs, with the pods orphaned, is not replaced while
// that pod may still write the prime claim, and is replaced once the pod has
// ended.
func TestAJobDeletedByAPersonWaitsForItsPods(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	ops.createJob(t)
	job := ops.runJob(t, "claim-123")
	pod := ops.createPod(t, job, "restore-claim-123-x7k2p", "node-a", corev1.PodRunning)
	if err := ops.cluster.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil {
		t.Fatal(err)
	}
	callbacks := newCallbacks(ops, monday)

	err := callbacks.Populate(ctx, params())
	var stopping *stoppingError
	if !errors.As(err, &stopping) || !strings.Contains(err.Error(), pod.Name) {
		t.Fatalf("Populate() = %v, want it waiting for pod %s", err, pod.Name)
	}
	if got := ops.job(t, "claim-123"); got != nil {
		t.Fatalf("Job %s created while a pod of the deleted one runs", got.Name)
	}

	ops.setPod(t, pod, func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed })
	if err := callbacks.Populate(ctx, params()); err != nil {
		t.Fatalf("Populate() once the pod ended: %v", err)
	}
	if got := ops.job(t, "claim-123"); got == nil || got.UID == job.UID {
		t.Fatalf("Job = %+v, want a new one", got)
	}
}

// TestCleanupWaitsForTheGate checks that Cleanup of a claim whose restore
// Job runs suspends the Job and returns an error while its pod may still
// write, so the library keeps the prime claim, and deletes the Job and the
// Secret copy once the pod has ended. Before, Cleanup deleted the
// destination and returned nil while its mover pod still ran.
func TestCleanupWaitsForTheGate(t *testing.T) {
	ctx := context.Background()
	ops := restoringOperations(t)
	job := ops.runJob(t, "claim-123")
	pod := ops.createPod(t, job, "restore-claim-123-x7k2p", "node-a", corev1.PodRunning)
	callbacks := newCallbacks(ops, monday)

	if err := callbacks.Cleanup(ctx, params()); err == nil {
		t.Fatal("Cleanup() returned nil while the Job runs")
	}
	if !ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, false) {
		t.Fatal("Cleanup did not suspend the running Job")
	}
	ops.setJob(t, "claim-123", func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })
	if err := callbacks.Cleanup(ctx, params()); err == nil || !strings.Contains(err.Error(), pod.Name) {
		t.Fatalf("Cleanup() = %v while the pod runs, want an error naming it", err)
	}
	if len(ops.deletedSec) != 0 {
		t.Fatalf("Secret copy deleted while the pod runs: %v", ops.deletedSec)
	}

	ops.setPod(t, pod, func(p *corev1.Pod) { p.Status.Phase = corev1.PodFailed })
	if err := callbacks.Cleanup(ctx, params()); err != nil {
		t.Fatalf("Cleanup() once the pod ended: %v", err)
	}
	if ops.job(t, "claim-123") != nil {
		t.Error("the Job is still there after Cleanup")
	}
	if len(ops.deletedSec) != 1 {
		t.Errorf("deleted Secrets = %v, want the copy", ops.deletedSec)
	}
}

// pinned returns the default params with the claim pinned by the
// backup.wlz.li/restore-as-of annotation to the time given in at.
func pinned(at string) populatormachinery.PopulatorParams {
	p := params()
	p.Pvc.Annotations = map[string]string{backupv1alpha1.AnnotationRestoreAsOf: at}
	return p
}

// TestAPinnedClaimRestoresItsMoment checks that a claim pinned to a moment
// gets a Job for the newest snapshot at or before it. The VolumeRestore alone
// would restore the newest snapshot.
func TestAPinnedClaimRestoresItsMoment(t *testing.T) {
	ops := populatedOperations(t)
	if err := newCallbacks(ops, monday, tuesday).Populate(context.Background(), pinned("2026-09-21T12:00:00Z")); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	job := ops.job(t, "claim-123")
	if job == nil || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID {
		t.Fatalf("Job = %+v, want one for %s", job, monday.ID)
	}
}

// TestAVolumeRestorePinnedBeforeEverySnapshotStaysPending checks that
// Populate checks the VolumeRestore's own spec.restoreAsOf when the claim has
// no annotation: it returns an error, creates no Job, and reports
// NoBackupInReach naming spec.restoreAsOf and the oldest snapshot.
func TestAVolumeRestorePinnedBeforeEverySnapshotStaysPending(t *testing.T) {
	ops := populatedOperations(t)
	p := paramsWith(func(vr *backupv1alpha1.VolumeRestore) { vr.Spec.RestoreAsOf = new("2026-09-01T00:00:00Z") })

	if err := newCallbacks(ops, monday).Populate(context.Background(), p); err == nil {
		t.Fatal("Populate() filled a claim whose VolumeRestore is pinned before every snapshot")
	}
	if job := ops.job(t, "claim-123"); job != nil {
		t.Fatalf("a Job was created: %s", job.Name)
	}
	last := ops.statuses[len(ops.statuses)-1]
	if cond := last.Status.Conditions[0]; cond.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(cond.Message, "spec.restoreAsOf") || !strings.Contains(cond.Message, "6e473100") {
		t.Fatalf("condition = %+v, want NoBackupInReach naming spec.restoreAsOf and the oldest snapshot", cond)
	}
}

// params returns the library's parameters for the claim notes (UID claim-123)
// in apps, its bound prime claim in backup-system, and a VolumeRestore that
// names the Secret repo-secret.
func params() populatormachinery.PopulatorParams {
	return populatormachinery.PopulatorParams{
		Pvc: &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "notes", Namespace: appNS, UID: types.UID("claim-123"), Generation: 3}},
		PvcPrime: &corev1.PersistentVolumeClaim{
			ObjectMeta: metav1.ObjectMeta{Name: PrimeClaimName("claim-123"), Namespace: controllerNS, UID: "prime-uid-123"},
			Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: "pv-123"},
		},
		Unstructured: volumeRestoreUnstructured(),
	}
}

// volumeRestoreUnstructured returns the VolumeRestore notes-data as an
// unstructured object, the form in which the library passes it.
func volumeRestoreUnstructured() *unstructured.Unstructured {
	vr := &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: appNS, Generation: 3}, Spec: backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"}}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vr)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: object}
}

// addRepository puts the repository Secret repo-secret into apps.
func addRepository(ops *fakeOperations) {
	ops.secrets[namespacedName(appNS, "repo-secret")] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "repo-secret", Namespace: appNS}, Data: map[string][]byte{"repository": []byte("s3://bucket")}}
}

// populatedOperations returns a fake cluster that holds the repository
// Secret repo-secret in apps and no restore Job.
func populatedOperations(t *testing.T) *fakeOperations {
	t.Helper()
	ops := newFakeOperations(t)
	addRepository(ops)
	return ops
}

// restoringOperations returns a fake cluster with the repository Secret
// copied into the controller namespace and a completed restore Job for
// claim-123. Complete and Cleanup read those objects, so their tests start
// from this state.
func restoringOperations(t *testing.T) *fakeOperations {
	t.Helper()
	ops := populatedOperations(t)
	ops.secrets[namespacedName(controllerNS, "claim-123")] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-123", Namespace: controllerNS},
		Data:       map[string][]byte{"repository": []byte("s3://bucket")},
	}
	ops.createJob(t)
	ops.setJob(t, "claim-123", complete)
	return ops
}

// namespacedName returns the namespace/name key the fake stores Secrets
// under.
func namespacedName(namespace, name string) string { return namespace + "/" + name }

// findCondition returns the condition of the given type, or nil when there is
// none.
func findCondition(conditions []metav1.Condition, typ string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == typ {
			return &conditions[i]
		}
	}
	return nil
}

// mapKeys returns a map's keys, for failure messages.
func mapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

// TestCleanupRetiresTheClaimAndReportsRestored checks that Cleanup removes the
// entry of the last claim being filled and sets Ready to True with reason
// Restored. It runs without a prime claim: the library deletes the prime
// claim after it calls PopulateCleanupFn, so every pass after the one that
// finished the restore arrives without it, and Cleanup, which exists to remove
// things, must succeed then too. Rejecting those passes once made one restored
// claim fail its sync several times a second for as long as it existed.
func TestCleanupRetiresTheClaimAndReportsRestored(t *testing.T) {
	ops := restoringOperations(t)
	params := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{
		Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring,
	})
	params.PvcPrime = nil

	if err := newCallbacks(ops, monday).Cleanup(context.Background(), params); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Fatalf("status writes = %d, want 1", len(ops.statuses))
	}
	status := ops.statuses[0]
	if len(status.Status.Claims) != 0 {
		t.Errorf("claims = %#v, want none once the restore has ended", status.Status.Claims)
	}
	condition := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != backupv1alpha1.ReasonRestored {
		t.Fatalf("Ready condition = %#v, want True/Restored", condition)
	}
}

// paramsWithClaims returns the default params with a VolumeRestore whose
// status.claims holds the given entries.
func paramsWithClaims(claims ...backupv1alpha1.ClaimRestoreStatus) populatormachinery.PopulatorParams {
	return paramsWith(func(vr *backupv1alpha1.VolumeRestore) { vr.Status.Claims = claims })
}

// paramsWith returns the default params with the VolumeRestore notes-data
// changed by the function given.
func paramsWith(change func(*backupv1alpha1.VolumeRestore)) populatormachinery.PopulatorParams {
	p := params()
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: appNS, Generation: 3},
		Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"},
	}
	change(vr)
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vr)
	if err != nil {
		panic(err)
	}
	p.Unstructured = &unstructured.Unstructured{Object: object}
	return p
}

// TestPopulateHoldsTheVolumeRestoreWhileItRestores checks that Populate puts
// the populator's finalizer on the VolumeRestore before it copies the
// repository Secret. The library looks the VolumeRestore up before it handles
// a deleted claim, and does nothing when it's gone.
func TestPopulateHoldsTheVolumeRestoreWhileItRestores(t *testing.T) {
	ops := populatedOperations(t)
	callbacks := newCallbacks(ops, monday)

	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if len(ops.updates) != 1 || !slices.Contains(ops.updates[0].Finalizers, Finalizer) {
		t.Fatalf("VolumeRestore updates = %v, want one that adds %s", ops.updates, Finalizer)
	}

	// A second pass finds the finalizer in place and writes nothing more.
	held := paramsWith(func(vr *backupv1alpha1.VolumeRestore) { vr.Finalizers = []string{Finalizer} })
	if err := callbacks.Populate(context.Background(), held); err != nil {
		t.Fatalf("second Populate() error = %v", err)
	}
	if len(ops.updates) != 1 {
		t.Errorf("VolumeRestore updates = %d, want the finalizer added once", len(ops.updates))
	}
}

// TestPopulateStartsNothingForADeletedVolumeRestore checks that Populate
// copies no Secret and creates no Job for a VolumeRestore that is being
// deleted without the populator's finalizer. The API server refuses a new
// finalizer on it, so nothing would keep it in place until Cleanup ran.
func TestPopulateStartsNothingForADeletedVolumeRestore(t *testing.T) {
	ops := populatedOperations(t)
	deleted := paramsWith(func(vr *backupv1alpha1.VolumeRestore) {
		vr.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		vr.Finalizers = []string{"someone.else/keeps-it"}
	})

	if err := newCallbacks(ops, monday).Populate(context.Background(), deleted); err == nil {
		t.Fatal("Populate() started a restore from a VolumeRestore being deleted")
	}
	if len(ops.createdSec) != 0 || ops.job(t, "claim-123") != nil {
		t.Fatalf("created Secrets %v and Job %v, want none", ops.createdSec, ops.job(t, "claim-123"))
	}
}

// TestCleanupReleasesTheVolumeRestoreOnceNoClaimIsLeft checks that Cleanup of
// the last claim deletes the Secret copy, retires the claim, and then removes
// the populator's finalizer, so a VolumeRestore deleted mid-restore goes once
// its claim is cleaned up.
func TestCleanupReleasesTheVolumeRestoreOnceNoClaimIsLeft(t *testing.T) {
	ops := restoringOperations(t)
	deleted := paramsWith(func(vr *backupv1alpha1.VolumeRestore) {
		vr.DeletionTimestamp = &metav1.Time{Time: time.Now()}
		vr.Finalizers = []string{Finalizer}
		vr.Status.Claims = []backupv1alpha1.ClaimRestoreStatus{{Name: "notes", UID: "claim-123", Phase: backupv1alpha1.RestorePhaseRestoring}}
	})

	if err := newCallbacks(ops, monday).Cleanup(context.Background(), deleted); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.deletedSec) != 1 {
		t.Errorf("deleted Secrets = %v, want the copy", ops.deletedSec)
	}
	if len(ops.updates) != 1 || slices.Contains(ops.updates[0].Finalizers, Finalizer) {
		t.Fatalf("VolumeRestore updates = %v, want one that removes %s", ops.updates, Finalizer)
	}
}

// TestCleanupKeepsTheVolumeRestoreWhileAnotherClaimRestores checks that
// Cleanup leaves the finalizer in place while another claim still has an
// entry in status.claims. That claim's Cleanup needs the VolumeRestore too.
func TestCleanupKeepsTheVolumeRestoreWhileAnotherClaimRestores(t *testing.T) {
	ops := restoringOperations(t)
	two := paramsWith(func(vr *backupv1alpha1.VolumeRestore) {
		vr.Finalizers = []string{Finalizer}
		vr.Status.Claims = []backupv1alpha1.ClaimRestoreStatus{
			{Name: "notes", UID: "claim-123", Phase: backupv1alpha1.RestorePhaseRestoring},
			{Name: "photos", UID: "claim-456", Phase: backupv1alpha1.RestorePhaseRestoring},
		}
	})

	if err := newCallbacks(ops, monday).Cleanup(context.Background(), two); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.updates) != 0 {
		t.Errorf("VolumeRestore updates = %v, want the finalizer kept", ops.updates)
	}
}

// TestCleanupOfAGoneVolumeRestoreSucceeds checks that Cleanup succeeds when the
// VolumeRestore is gone by the time it writes the status. The Job and the
// Secret copy are deleted, and there is no status left to write.
func TestCleanupOfAGoneVolumeRestoreSucceeds(t *testing.T) {
	ops := restoringOperations(t)
	ops.volumeRestoreGone = true
	p := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{Name: "notes", UID: "claim-123", Phase: backupv1alpha1.RestorePhaseRestoring})

	if err := newCallbacks(ops, monday).Cleanup(context.Background(), p); err != nil {
		t.Fatalf("Cleanup() error = %v, want NotFound from the status write ignored", err)
	}
	if len(ops.deletedSec) != 1 || ops.job(t, "claim-123") != nil {
		t.Errorf("deleted Secrets %v, Job %v; want the copy deleted and the Job gone", ops.deletedSec, ops.job(t, "claim-123"))
	}
}

// VolSync 0.16.0 reads the privileged-movers annotation from the namespace
// of its own object, the namespace of the app
// (internal/controller/replicationdestination_controller.go:108). The
// populator reads it from the namespace of the VolumeRestore. Thus a
// restore into an annotated app namespace runs as root, although the Job
// runs in the controller namespace, which has no annotation.
func TestPopulateReadsPrivilegedMoversFromTheVolumeRestoreNamespace(t *testing.T) {
	ops := populatedOperations(t)
	ctx := context.Background()
	ns := &corev1.Namespace{}
	if err := ops.cluster.Get(ctx, client.ObjectKey{Name: appNS}, ns); err != nil {
		t.Fatal(err)
	}
	ns.Annotations = map[string]string{restorejob.AnnotationPrivilegedMovers: "true"}
	if err := ops.cluster.Update(ctx, ns); err != nil {
		t.Fatal(err)
	}
	if err := newCallbacks(ops, monday).Populate(ctx, params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	job := ops.job(t, "claim-123")
	if job == nil {
		t.Fatal("no Job restore-claim-123")
	}
	sc := job.Spec.Template.Spec.Containers[0].SecurityContext
	if sc == nil || ptr.Deref(sc.RunAsUser, -1) != 0 {
		t.Errorf("security context = %+v, want the privileged mover (runAsUser 0)", sc)
	}
}

// hasDelete reports whether a restore Job's restic container passes
// --delete.
func hasDelete(job *batchv1.Job) bool {
	return slices.ContainsFunc(job.Spec.Template.Spec.Containers, func(c corev1.Container) bool {
		return slices.Contains(c.Args, "--delete")
	})
}

// earlierPrimeUID is the UID of a prime claim of notes that is gone: the
// library created the prime claim again under the same name since.
const earlierPrimeUID types.UID = "prime-uid-OLD"

// createEarlierJob creates the restore Job of notes as Populate built it for
// the earlier prime claim, owned by that prime, in the state the Job
// controller leaves it in right after the create.
func (f *fakeOperations) createEarlierJob(t *testing.T) *batchv1.Job {
	t.Helper()
	job, err := restorejob.Build(restorejob.Spec{
		Name: JobName("claim-123"), Namespace: controllerNS,
		Origin:     restorejob.Origin{Kind: restorejob.OriginClaim, UID: "claim-123"},
		Owner:      metav1.OwnerReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: PrimeClaimName("claim-123"), UID: earlierPrimeUID},
		SnapshotID: monday.ID, Claim: PrimeClaimName("claim-123"), Repository: "claim-123", Image: testImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.cluster.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return f.setJob(t, "claim-123", func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })
}

// TestAFinishedJobOfAnEarlierPrimeIsNeverTrusted checks that a Job with
// Complete=True that filled an earlier prime claim of the claim never
// completes the claim: its volume went with that prime, and the library
// would bind the new, empty prime. Populate stops the Job and creates a new
// one for the prime it has now only once the old one is gone. The Job is
// never recorded on the prime claim of now, so it is never resumed either:
// a suspended one would write that prime until the garbage collector killed
// it mid-restore.
func TestAFinishedJobOfAnEarlierPrimeIsNeverTrusted(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	old := ops.createEarlierJob(t)
	ops.setJob(t, "claim-123", complete)
	callbacks := newCallbacks(ops, monday)

	if done, err := callbacks.Complete(ctx, params()); err != nil || done {
		t.Fatalf("Complete() = %t, %v; want false for the Job of an earlier prime", done, err)
	}
	if done, err := librarySync(ctx, callbacks, params()); err == nil || done {
		t.Fatalf("sync = %t, %v; want an error and not done", done, err)
	}
	if got := ops.job(t, "claim-123"); got != nil && got.DeletionTimestamp == nil {
		t.Fatalf("the Job of the earlier prime is %+v, want it stopped", got)
	}
	if got := ops.prime(t, "claim-123").Annotations[AnnotationJobUID]; got == string(old.UID) {
		t.Fatal("the Job of an earlier prime was recorded on the prime claim of now")
	}
	if _, err := librarySync(ctx, callbacks, params()); err != nil {
		t.Fatalf("sync once the old Job is gone: %v", err)
	}
	replaced := ops.job(t, "claim-123")
	if replaced == nil || replaced.UID == old.UID || len(replaced.OwnerReferences) != 1 || replaced.OwnerReferences[0].UID != "prime-uid-123" {
		t.Fatalf("Job = %+v, want a new one owned by prime-uid-123", replaced)
	}
}

// TestAStalePrimeNeverRecordsOrResumesAJob checks that Populate records and
// resumes no Job while the library's cache still passes an earlier prime
// claim and the API server already holds a new one under the same name. The
// Job was built for the earlier prime, so ownedByPrime accepts it against
// the cached prime; the fresh read shows the prime of now, and Populate
// returns an error until the cache catches up.
func TestAStalePrimeNeverRecordsOrResumesAJob(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	old := ops.createEarlierJob(t)
	stale := params()
	stale.PvcPrime.UID = earlierPrimeUID
	callbacks := newCallbacks(ops, monday)

	for sync := range 2 {
		if err := callbacks.Populate(ctx, stale); !errors.Is(err, errStalePrime) {
			t.Fatalf("sync %d: Populate() = %v, want errStalePrime", sync, err)
		}
		if got := ops.prime(t, "claim-123").Annotations[AnnotationJobUID]; got == string(old.UID) {
			t.Fatalf("sync %d recorded the Job of the cached prime on the prime of now", sync)
		}
		if job := ops.job(t, "claim-123"); !ptr.Deref(job.Spec.Suspend, false) {
			t.Fatalf("sync %d resumed the Job of the cached prime", sync)
		}
	}
}

// recordOnPrime records a Job's UID on the prime claim of notes, as Populate
// does before it resumes the Job.
func (f *fakeOperations) recordOnPrime(t *testing.T, job *batchv1.Job) {
	t.Helper()
	prime := f.prime(t, "claim-123")
	prime.Annotations = map[string]string{AnnotationJobUID: string(job.UID)}
	if err := f.cluster.Update(context.Background(), prime); err != nil {
		t.Fatal(err)
	}
}

// recordedSuspendedJob returns a cluster whose notes Job is recorded on the
// prime claim and waits to be resumed.
func recordedSuspendedJob(t *testing.T) *fakeOperations {
	t.Helper()
	ops := populatedOperations(t)
	ops.recordOnPrime(t, ops.createJob(t))
	return ops
}

// TestNoJobIsResumedOnceTheVolumeIsHandedOver checks that Populate resumes
// no Job once the library has handed the prime claim's volume to the app
// claim: the app claim names a volume, or the volume's claimRef no longer
// names the prime claim by its namespace, name and UID. The Job's pod would
// write the app's volume. A prime claim that names no volume resumes no Job
// either, and Populate returns no error for it. Nor does a Job of an app
// claim being deleted resume: Cleanup or the orphan reconciler stops that
// Job, and a Job their stop suspended looks the same as one never resumed.
func TestNoJobIsResumedOnceTheVolumeIsHandedOver(t *testing.T) {
	for name, handOver := range map[string]func(*testing.T, *fakeOperations){
		"the app claim names a volume": func(t *testing.T, ops *fakeOperations) {
			claim := &corev1.PersistentVolumeClaim{}
			if err := ops.cluster.Get(context.Background(), client.ObjectKey{Namespace: appNS, Name: "notes"}, claim); err != nil {
				t.Fatal(err)
			}
			claim.Spec.VolumeName = "pv-123"
			if err := ops.cluster.Update(context.Background(), claim); err != nil {
				t.Fatal(err)
			}
		},
		"the volume's claimRef names the app claim": func(t *testing.T, ops *fakeOperations) {
			volume := &corev1.PersistentVolume{}
			if err := ops.cluster.Get(context.Background(), client.ObjectKey{Name: "pv-123"}, volume); err != nil {
				t.Fatal(err)
			}
			volume.Spec.ClaimRef = &corev1.ObjectReference{Kind: "PersistentVolumeClaim", APIVersion: "v1", Namespace: appNS, Name: "notes", UID: "claim-123"}
			if err := ops.cluster.Update(context.Background(), volume); err != nil {
				t.Fatal(err)
			}
		},
		"the volume's claimRef names the prime claim with another UID": func(t *testing.T, ops *fakeOperations) {
			volume := &corev1.PersistentVolume{}
			if err := ops.cluster.Get(context.Background(), client.ObjectKey{Name: "pv-123"}, volume); err != nil {
				t.Fatal(err)
			}
			volume.Spec.ClaimRef.UID = "prime-uid-OTHER"
			if err := ops.cluster.Update(context.Background(), volume); err != nil {
				t.Fatal(err)
			}
		},
		"the app claim is being deleted": func(t *testing.T, ops *fakeOperations) {
			claim := &corev1.PersistentVolumeClaim{}
			if err := ops.cluster.Get(context.Background(), client.ObjectKey{Namespace: appNS, Name: "notes"}, claim); err != nil {
				t.Fatal(err)
			}
			claim.Finalizers = []string{ClaimFinalizer}
			if err := ops.cluster.Update(context.Background(), claim); err != nil {
				t.Fatal(err)
			}
			if err := ops.cluster.Delete(context.Background(), claim); err != nil {
				t.Fatal(err)
			}
		},
		"the prime claim names no volume": func(t *testing.T, ops *fakeOperations) {
			prime := ops.prime(t, "claim-123")
			prime.Spec.VolumeName = ""
			if err := ops.cluster.Update(context.Background(), prime); err != nil {
				t.Fatal(err)
			}
		},
	} {
		t.Run(name, func(t *testing.T) {
			ops := recordedSuspendedJob(t)
			handOver(t, ops)
			if err := newCallbacks(ops, monday).Populate(context.Background(), params()); err != nil {
				t.Fatalf("Populate() error = %v", err)
			}
			if !ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, false) {
				t.Error("Populate resumed the Job after the volume was handed over")
			}
		})
	}
}

// TestAJobBeingDeletedIsLeftAlone checks that Populate neither resumes nor
// records a Job that is being deleted, even one recorded on the prime claim
// and waiting to be resumed, and returns errJobBeingDeleted until it is
// gone.
func TestAJobBeingDeletedIsLeftAlone(t *testing.T) {
	ctx := context.Background()
	ops := recordedSuspendedJob(t)
	job := ops.job(t, "claim-123")
	job.Finalizers = append(job.Finalizers, "test.wlz.li/hold")
	if err := ops.cluster.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := ops.cluster.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationForeground)); err != nil {
		t.Fatal(err)
	}

	err := newCallbacks(ops, monday).Populate(ctx, params())
	if !errors.Is(err, errJobBeingDeleted) {
		t.Fatalf("Populate() = %v, want errJobBeingDeleted", err)
	}
	if got := ops.job(t, "claim-123"); got == nil || !ptr.Deref(got.Spec.Suspend, false) {
		t.Fatalf("Job = %+v, want it left suspended", got)
	}
}

// A claim with no restore Job waits while the controller runs with --pause.
// The sync returns the library's "not yet" result with no error, and the
// callbacks create no Job and no Secret copy and write no status. An empty
// repository does not bind the claim empty either.
func TestAPausedPopulatorStartsNoRestore(t *testing.T) {
	ctx := context.Background()
	for name, snapshots := range map[string][]restic.Snapshot{"a snapshot": {monday}, "an empty repository": nil} {
		t.Run(name, func(t *testing.T) {
			ops := populatedOperations(t)
			callbacks := newCallbacks(ops, snapshots...)
			callbacks.Pause()
			if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
				t.Fatalf("sync while paused = %t, %v; want not done and no error", done, err)
			}
			if job := ops.job(t, "claim-123"); job != nil {
				t.Errorf("Job %s created while paused", job.Name)
			}
			if _, ok := ops.secrets[namespacedName(controllerNS, "claim-123")]; ok {
				t.Error("the Secret copy was created while paused")
			}
			if len(ops.statuses) != 0 {
				t.Errorf("status writes = %d while paused, want none", len(ops.statuses))
			}
		})
	}
}

// A claim whose restore Job exists goes on while the controller runs with
// --pause: the next sync resumes the recorded Job.
func TestAPausedPopulatorContinuesAClaimWithAJob(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	callbacks := newCallbacks(ops, monday)
	if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
		t.Fatalf("first sync = %t, %v; want not done", done, err)
	}
	ops.setJob(t, "claim-123", func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })

	callbacks.Pause()
	if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
		t.Fatalf("sync while paused = %t, %v; want not done", done, err)
	}
	if ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, true) {
		t.Error("the recorded Job is still suspended while paused, want it resumed")
	}
}

// snapshotsBySecret is a SnapshotLister that returns the snapshots of each
// repository by the name of its Secret.
type snapshotsBySecret map[string][]restic.Snapshot

func (s snapshotsBySecret) Snapshots(_ context.Context, secret *corev1.Secret) ([]restic.Snapshot, error) {
	return s[secret.Name], nil
}

// quiesced returns the snapshot with the tag a quiesced BackupRun adds.
func quiesced(s restic.Snapshot) restic.Snapshot {
	s.Tags = []string{restic.QuiescedTag}
	return s
}

// addMediaVolume adds a second VolumeRestore in apps, media-data, whose
// repository Secret is media-secret.
func addMediaVolume(t *testing.T, ops *fakeOperations) {
	t.Helper()
	vr := &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "media-data", Namespace: appNS}, Spec: backupv1alpha1.VolumeRestoreSpec{Repository: "media-secret"}}
	if err := ops.cluster.Create(context.Background(), vr); err != nil {
		t.Fatal(err)
	}
	ops.secrets[namespacedName(appNS, "media-secret")] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "media-secret", Namespace: appNS}}
}

// TestAQuiescedNamespaceRestoresTheQuiescedSnapshot checks that a claim of a
// namespace with quiesced backups is filled from the newest quiesced
// snapshot, not from a newer live one, so the volume matches the moment the
// bootstrap webhook recovers the database to.
func TestAQuiescedNamespaceRestoresTheQuiescedSnapshot(t *testing.T) {
	ops := populatedOperations(t)
	if err := newCallbacks(ops, quiesced(monday), tuesday).Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	job := ops.job(t, "claim-123")
	if job == nil || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID {
		t.Fatalf("Job = %+v, want one for the quiesced snapshot %s", job, monday.ID)
	}
}

// TestAClaimOfAnotherMomentWaitsForAPin checks that a claim stays Pending
// with reason NoBackupInReach when another repository of the namespace has
// its newest quiesced snapshot at another time, and that a pin both reach
// fills it from the quiesced snapshot of that moment.
func TestAClaimOfAnotherMomentWaitsForAPin(t *testing.T) {
	ops := populatedOperations(t)
	addMediaVolume(t, ops)
	lister := snapshotsBySecret{
		"repo-secret":  {quiesced(monday), quiesced(tuesday)},
		"media-secret": {quiesced(monday)},
	}
	if err := New(ops, controllerNS, testImage, lister).Populate(context.Background(), params()); err == nil {
		t.Fatal("Populate() filled a claim whose namespace has two quiesced moments")
	}
	last := ops.statuses[len(ops.statuses)-1]
	if cond := last.Status.Conditions[0]; cond.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(cond.Message, "media-secret") {
		t.Fatalf("condition = %+v, want NoBackupInReach naming media-secret", cond)
	}

	ops = populatedOperations(t)
	addMediaVolume(t, ops)
	pin := monday.Time.Add(time.Hour).Format(time.RFC3339)
	if err := New(ops, controllerNS, testImage, lister).Populate(context.Background(), pinned(pin)); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if job := ops.job(t, "claim-123"); job == nil || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID {
		t.Fatalf("Job = %+v, want one for %s", job, monday.ID)
	}
}

// lastReady returns the Ready condition of the last status write.
func lastReady(t *testing.T, ops *fakeOperations) (backupv1alpha1.RestorePhase, string, string) {
	t.Helper()
	if len(ops.statuses) == 0 {
		t.Fatal("no status write")
	}
	last := ops.statuses[len(ops.statuses)-1]
	ready := findCondition(last.Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || len(last.Status.Claims) == 0 {
		t.Fatalf("status = %+v, want Ready and the claim's entry", last.Status)
	}
	return last.Status.Claims[0].Phase, ready.Reason, ready.Message
}

// TestARefusedJobCreateShowsOnReady checks that a restore Job create the
// API server refuses with 422 Invalid, as an admission policy does, puts
// reason RestoreJobRefused and the API server's answer on Ready, creates
// nothing and returns an error, so the claim stays Pending. Once the cause
// is gone, the next sync creates the Job and Ready reads Restoring.
func TestARefusedJobCreateShowsOnReady(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	ops.refuseCreate = apierrors.NewInvalid(schema.GroupKind{Group: "batch", Kind: "Job"}, "restore-claim-123", field.ErrorList{
		field.Invalid(field.NewPath("spec", "template", "spec", "securityContext", "sysctls"), nil, "sysctls are not allowed"),
	})
	callbacks := newCallbacks(ops, monday)

	if done, err := librarySync(ctx, callbacks, params()); err == nil || done {
		t.Fatalf("sync = %t, %v; want an error", done, err)
	}
	phase, reason, message := lastReady(t, ops)
	if phase != backupv1alpha1.RestorePhaseFailed || reason != backupv1alpha1.ReasonRestoreJobRefused || !strings.Contains(message, "sysctls are not allowed") {
		t.Fatalf("entry %s, Ready %s %q; want Failed and RestoreJobRefused with the API server's answer", phase, reason, message)
	}
	if job := ops.job(t, "claim-123"); job != nil {
		t.Fatalf("Job %s exists after a refused create", job.Name)
	}

	ops.refuseCreate = nil
	if _, err := librarySync(ctx, callbacks, params()); err != nil {
		t.Fatalf("sync once the create is admitted: %v", err)
	}
	if phase, reason, _ := lastReady(t, ops); phase != backupv1alpha1.RestorePhaseRestoring || reason != backupv1alpha1.ReasonRestoring {
		t.Fatalf("entry %s, Ready %s; want Restoring", phase, reason)
	}
}

// TestAResumeThatMeetsAReplacedJobIsNoRefusal checks that a resume sent to
// a Job created again under the same name since Populate read it, which the
// API server answers with 422 Invalid on field metadata.uid, is retried
// without the RestoreJobRefused reason. The replacement stays suspended,
// and the next sync records it on the prime claim without resuming it: a Job
// the prime claim does not record, such as one whose create answered with an
// error and was stored anyway, is resumed only in a later sync.
func TestAResumeThatMeetsAReplacedJobIsNoRefusal(t *testing.T) {
	ctx := context.Background()
	ops := recordedSuspendedJob(t)
	var replacement *batchv1.Job
	ops.beforeResume = func(ctx context.Context) error {
		ops.beforeResume = nil
		if err := ops.cluster.Delete(ctx, ops.job(t, "claim-123"), client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
			return err
		}
		replacement = ops.createJob(t)
		return nil
	}
	callbacks := newCallbacks(ops, monday)

	if done, err := librarySync(ctx, callbacks, params()); err == nil || done || !apierrors.IsInvalid(err) {
		t.Fatalf("sync = %t, %v; want the API server's 422 Invalid for the replaced Job", done, err)
	}
	for _, status := range ops.statuses {
		if ready := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady); ready != nil && ready.Reason == backupv1alpha1.ReasonRestoreJobRefused {
			t.Fatalf("Ready = %+v, want no RestoreJobRefused for a replaced Job", ready)
		}
	}
	job := ops.job(t, "claim-123")
	if job.UID != replacement.UID || !ptr.Deref(job.Spec.Suspend, false) {
		t.Fatalf("Job %s suspended %t; want the replacement %s, suspended", job.UID, ptr.Deref(job.Spec.Suspend, false), replacement.UID)
	}

	if _, err := librarySync(ctx, callbacks, params()); err != nil {
		t.Fatalf("sync after the replacement: %v", err)
	}
	if got := ops.prime(t, "claim-123").Annotations[AnnotationJobUID]; got != string(replacement.UID) {
		t.Fatalf("prime records Job %q, want the replacement %s", got, replacement.UID)
	}
}
