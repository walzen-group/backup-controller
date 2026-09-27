package populator

import (
	"context"
	"errors"
	"reflect"
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// TestPopulateCopiesRepositorySecret checks that Populate copies the repository
// Secret from the app's namespace into the controller namespace with the same
// data.
func TestPopulateCopiesRepositorySecret(t *testing.T) {
	ops := populatedOperations(t)
	ops.secrets[namespacedName(appNS, "repo-secret")].Data["password"] = []byte("secret")
	callbacks := newCallbacks(ops, monday)

	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}

	copied, ok := ops.secrets[namespacedName(controllerNS, "claim-123")]
	if !ok || !reflect.DeepEqual(copied.Data, ops.secrets[namespacedName(appNS, "repo-secret")].Data) {
		t.Fatalf("copied repository secret = %#v, want same data in backup-system", copied)
	}
}

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

// TestAJobFoundByNameIsRecordedBeforeItRuns checks that a Job of the claim
// the prime claim does not record, such as one whose create answered with an
// error and was stored anyway, is recorded in one sync and resumed only in
// the next.
func TestAJobFoundByNameIsRecordedBeforeItRuns(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	job := ops.createJob(t)
	callbacks := newCallbacks(ops, monday)

	if err := callbacks.Populate(ctx, params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if !ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, false) {
		t.Fatal("a Job the prime claim did not record was resumed in the sync that recorded it")
	}
	if got := ops.prime(t, "claim-123").Annotations[AnnotationJobUID]; got != string(job.UID) {
		t.Fatalf("prime claim records %q, want %s", got, job.UID)
	}
	if err := callbacks.Populate(ctx, params()); err != nil {
		t.Fatalf("second Populate() error = %v", err)
	}
	if ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, true) {
		t.Error("the recorded Job was not resumed")
	}
}

// TestAJobOfADeletedClaimIsNeverResumed checks that Populate resumes no Job
// while the app claim, read fresh, is being deleted. Cleanup or the orphan
// reconciler stops that claim's Job, and a Job their stop suspended looks
// the same as one never resumed.
func TestAJobOfADeletedClaimIsNeverResumed(t *testing.T) {
	ctx := context.Background()
	c := newCluster(t)
	ops := fakeOperationsOn(c)
	addRepository(ops)
	job := ops.createJob(t)
	prime := ops.prime(t, "claim-123")
	prime.Annotations = map[string]string{AnnotationJobUID: string(job.UID)}
	if err := c.Update(ctx, prime); err != nil {
		t.Fatal(err)
	}
	claim := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: appNS, Name: "notes"}, claim); err != nil {
		t.Fatal(err)
	}
	claim.Finalizers = []string{ClaimFinalizer}
	if err := c.Update(ctx, claim); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, claim); err != nil {
		t.Fatal(err)
	}

	if err := newCallbacks(ops, monday).Populate(ctx, params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if !ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, false) {
		t.Error("Populate resumed the Job of a claim being deleted")
	}
}

// TestPopulateReusesAnExistingJob checks that Populate leaves an existing
// restore Job's spec as it is and creates no second one.
func TestPopulateReusesAnExistingJob(t *testing.T) {
	ops := populatedOperations(t)
	existing := ops.createJob(t)
	callbacks := newCallbacks(ops, monday, tuesday)

	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	got := ops.job(t, "claim-123")
	if got.UID != existing.UID || !reflect.DeepEqual(got.Spec.Template, existing.Spec.Template) || got.Annotations[restorejob.AnnotationSnapshotID] != monday.ID {
		t.Fatalf("the existing Job changed: got %+v, want %+v", got, existing)
	}
}

// TestPopulateReportsRestoring checks that Populate writes the status once,
// with Ready False and reason Restoring naming the restore Job and its
// namespace, and a Restoring entry for the claim that has a start time.
func TestPopulateReportsRestoring(t *testing.T) {
	ops := populatedOperations(t)
	callbacks := newCallbacks(ops, monday)
	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Fatalf("status writes = %d, want 1", len(ops.statuses))
	}
	status := ops.statuses[0]
	condition := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != backupv1alpha1.ReasonRestoring {
		t.Fatalf("Ready condition = %#v, want False/Restoring", condition)
	}
	// The Job lives in the controller's namespace, so the message says which
	// namespace to look in as well as what to look for.
	if condition.Message != "waiting for restore Job restore-claim-123 (snapshot 6e473100) in backup-system" {
		t.Errorf("Ready message = %q, want the Job and its namespace", condition.Message)
	}
	if len(status.Status.Claims) != 1 || status.Status.Claims[0].Name != "notes" || status.Status.Claims[0].UID != types.UID("claim-123") || status.Status.Claims[0].Phase != backupv1alpha1.RestorePhaseRestoring || status.Status.Claims[0].StartedAt == nil {
		t.Fatalf("claim status = %#v, want restoring claim with start time", status.Status.Claims)
	}
}

// TestTheReadyMessageShowsWhyThePodWaits checks that while the restore
// Job's pod waits, such as for its image, the Ready message says so.
func TestTheReadyMessageShowsWhyThePodWaits(t *testing.T) {
	ops := populatedOperations(t)
	job := ops.createJob(t)
	pod := ops.createPod(t, job, "restore-claim-123-x7k2p", "node-a", corev1.PodPending)
	ops.setPod(t, pod, func(p *corev1.Pod) {
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "restore", State: corev1.ContainerState{
			Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image"},
		}}}
	})
	if err := newCallbacks(ops, monday).Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	ready := findCondition(ops.statuses[len(ops.statuses)-1].Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || !strings.Contains(ready.Message, "restore-claim-123 (snapshot 6e473100; waiting: restore: ImagePullBackOff") {
		t.Fatalf("Ready = %#v, want the pod's waiting reason", ready)
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
	if ready == nil || ready.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(ready.Message, "6e473100 (host laptop") {
		t.Fatalf("Ready = %#v, want NoBackupInReach naming the snapshot passed over", ready)
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
// so Complete never sees a Job Populate is taking down.
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
	if _, err := librarySync(ctx, callbacks, ops.paramsFor(t, "notes", "claim-123")); err != nil {
		t.Fatalf("sync once the old Job is gone: %v", err)
	}
	replaced := ops.job(t, "claim-123")
	if replaced == nil || replaced.UID == job.UID {
		t.Fatalf("Job = %+v, want a new one", replaced)
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

// TestCleanupDeletesTheJobAndTheSecret checks that Cleanup of a completed
// restore deletes the Job and the Secret copy.
func TestCleanupDeletesTheJobAndTheSecret(t *testing.T) {
	ops := restoringOperations(t)
	if err := newCallbacks(ops, monday).Cleanup(context.Background(), params()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if ops.job(t, "claim-123") != nil || len(ops.deletedSec) != 1 {
		t.Fatalf("Job %v, deleted Secrets %v; want the Job gone and the copy deleted", ops.job(t, "claim-123"), ops.deletedSec)
	}
}

// TestCleanupToleratesMissingObjects checks that Cleanup succeeds when the
// Job and the Secret copy are already gone.
func TestCleanupToleratesMissingObjects(t *testing.T) {
	if err := newCallbacks(newFakeOperations(t), monday).Cleanup(context.Background(), params()); err != nil {
		t.Fatalf("Cleanup() error = %v, want nil for missing objects", err)
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

// TestAPinnedClaimNoSnapshotReachesStaysPending checks that Populate returns
// an error and creates no Job for a claim pinned before every snapshot, and
// that it reports NoBackupInReach with the oldest snapshot's ID.
func TestAPinnedClaimNoSnapshotReachesStaysPending(t *testing.T) {
	for name, snapshots := range map[string][]restic.Snapshot{"older snapshots": {monday}, "no snapshot": nil} {
		t.Run(name, func(t *testing.T) {
			ops := populatedOperations(t)
			err := newCallbacks(ops, snapshots...).Populate(context.Background(), pinned("2026-09-01T00:00:00Z"))
			if err == nil {
				t.Fatal("Populate() filled a claim pinned before every snapshot")
			}
			if job := ops.job(t, "claim-123"); job != nil {
				t.Fatalf("a Job was created: %s", job.Name)
			}
			last := ops.statuses[len(ops.statuses)-1]
			if cond := last.Status.Conditions[0]; cond.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(cond.Message, backupv1alpha1.AnnotationRestoreAsOf) {
				t.Fatalf("condition = %+v, want NoBackupInReach naming the annotation", cond)
			}
		})
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

// TestCleanupToleratesTheAlreadyDeletedPrimeClaim checks that Cleanup succeeds
// without a prime claim and still writes the status that ends the restore.
//
// api.md's status table says the Ready condition is True when no claim is
// being filled from the VolumeRestore, and that claims[] holds only the claims
// being populated now. Cleanup is the only callback that runs when a restore
// ends, so it's the one place that can remove the entry and report the
// VolumeRestore Ready. The library deletes the prime claim after it calls
// PopulateCleanupFn, so every pass after the one that finished the restore
// arrives without it. Rejecting those passes failed the sync, and the library
// requeues on error, so one restored claim errored several times a second for
// as long as it existed:
//
//	error syncing 'pvc/canary-backup/canary-backup': populator parameters
//	have no prime PVC, requeuing
//
// Cleanup exists to remove things, and the prime claim being gone is the state
// it works towards.
func TestCleanupToleratesTheAlreadyDeletedPrimeClaim(t *testing.T) {
	ops := restoringOperations(t)
	params := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{
		Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring,
	})
	params.PvcPrime = nil

	if err := newCallbacks(ops, monday).Cleanup(context.Background(), params); err != nil {
		t.Fatalf("Cleanup() without a prime claim = %v, want it tolerated", err)
	}
	if len(ops.statuses) != 1 {
		t.Errorf("status writes = %d, want the restore still reported as ended", len(ops.statuses))
	}
}

// TestCleanupWritesNothingWhenThereIsNothingToChange checks that a second
// Cleanup with nothing to change writes no status.
//
// The library has no early return for a claim it has already populated. Every
// resync walks the whole path, reaches the completion branch and calls Cleanup
// again, for the life of the claim. Writing status each time would send one
// update per volume every resync interval, forever, and none of them would
// change anything.
func TestCleanupWritesNothingWhenThereIsNothingToChange(t *testing.T) {
	ops := restoringOperations(t)
	callbacks := newCallbacks(ops, monday)
	params := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{
		Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring,
	})

	if err := callbacks.Cleanup(context.Background(), params); err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Fatalf("status writes after the first call = %d, want 1", len(ops.statuses))
	}

	// Feed back what the first call wrote, which is what the next resync reads
	// from the cluster. The claim is already retired and the condition already
	// says so, so the second call has nothing to write.
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ops.statuses[0])
	if err != nil {
		t.Fatalf("convert the written status back: %v", err)
	}
	retired := params
	retired.Unstructured = &unstructured.Unstructured{Object: object}
	if err := callbacks.Cleanup(context.Background(), retired); err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Errorf("status writes after the second call = %d, want the second to write nothing", len(ops.statuses))
	}
}

// TestCleanupRetiresTheClaimAndReportsRestored checks that Cleanup removes the
// entry of the last claim being filled and sets Ready to True with reason
// Restored.
func TestCleanupRetiresTheClaimAndReportsRestored(t *testing.T) {
	ops := restoringOperations(t)
	params := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{
		Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring,
	})

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

// TestCleanupLeavesOtherClaimsRestoring checks that Cleanup removes only the
// finished claim's entry. A VolumeRestore can populate more than one claim at
// a time, so the other claims stay reported, and Ready stays False with reason
// Restoring.
func TestCleanupLeavesOtherClaimsRestoring(t *testing.T) {
	ops := restoringOperations(t)
	params := paramsWithClaims(
		backupv1alpha1.ClaimRestoreStatus{Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring},
		backupv1alpha1.ClaimRestoreStatus{Name: "photos", UID: types.UID("claim-456"), Phase: backupv1alpha1.RestorePhaseRestoring},
	)

	if err := newCallbacks(ops, monday).Cleanup(context.Background(), params); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	status := ops.statuses[0]
	if len(status.Status.Claims) != 1 || status.Status.Claims[0].UID != types.UID("claim-456") {
		t.Fatalf("claims = %#v, want only photos still restoring", status.Status.Claims)
	}
	condition := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != backupv1alpha1.ReasonRestoring {
		t.Fatalf("Ready condition = %#v, want False/Restoring while photos is filling", condition)
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
