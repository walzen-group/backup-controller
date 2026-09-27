package runs

import (
	"context"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The helpers in this file stand in for the Job controller around an
// in-place restore's restore Job. The fake client runs no Job controller, so
// a test sets the Job's conditions and creates its pods the way the Job
// controller does: pods labelled from the template plus
// batch.kubernetes.io/controller-uid with the Job's UID, with a controller
// reference to the Job.

// testImage is the restic image the tests' restore Jobs run.
const testImage = "quay.io/backube/volsync:0.16.0-test"

// jobUID is the UID a test gives a restore Job it creates itself.
const jobUID = types.UID("restore-job-uid")

// withNamespace returns objects with the run's Namespace added when they
// hold none of that name. The run reads its Namespace before it creates a
// restore Job, and on a cluster it always exists.
func withNamespace(objects []client.Object) []client.Object {
	for _, o := range objects {
		if n, ok := o.(*corev1.Namespace); ok && n.Name == ns {
			return objects
		}
	}
	return append(objects, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
}

// restoreJobFor returns the restore Job the run back-to-monday creates for
// its first item, restoring the snapshot with the given full ID into the
// claim named claimName, as createJob builds it, with the UID jobUID.
func restoreJobFor(t *testing.T, run *backupv1alpha1.RestoreRun, claimName, snapshotID string) *batchv1.Job {
	t.Helper()
	job, err := restorejob.Build(restorejob.Spec{
		Name: jobName(run.UID, 0), Namespace: run.Namespace,
		Origin:     restorejob.Origin{Kind: restorejob.OriginRestoreRun, UID: run.UID},
		Owner:      *metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind(backupv1alpha1.KindRestoreRun)),
		SnapshotID: snapshotID, Claim: claimName, Repository: repoN, Image: testImage, Delete: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	job.UID = jobUID
	return job
}

// restoreJobs returns the restore Jobs in the test namespace.
func restoreJobs(t *testing.T, c client.Client) []batchv1.Job {
	t.Helper()
	list := &batchv1.JobList{}
	if err := c.List(context.Background(), list, client.InNamespace(ns), client.MatchingLabels(restorejob.ManagedLabels())); err != nil {
		t.Fatal(err)
	}
	return list.Items
}

// itemJob reads the restore Job the run back-to-monday's first item names.
func itemJob(t *testing.T, c client.Client) *batchv1.Job {
	t.Helper()
	item := readRestoreRun(t, c).Status.Items[0]
	if item.Job == "" {
		t.Fatalf("item = %+v, want it to name its restore Job", item)
	}
	job := &batchv1.Job{}
	get(t, c, ns, item.Job, job)
	return job
}

// endJob sets the condition of the given type True on the stored Job named
// name, with the reason and the message given, as the Job controller does
// once every pod of the Job has ended.
func endJob(t *testing.T, c client.Client, name string, typ batchv1.JobConditionType, reason, message string) {
	t.Helper()
	job := &batchv1.Job{}
	get(t, c, ns, name, job)
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
		Type: typ, Status: corev1.ConditionTrue, Reason: reason, Message: message,
	})
	if err := c.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
}

// completeJob marks the restore Job of the run back-to-monday's first item
// Complete, as the Job controller does once restic exited 0.
func completeJob(t *testing.T, c client.Client) {
	t.Helper()
	endJob(t, c, itemJob(t, c).Name, batchv1.JobComplete, "CompletionsReached", "Reached expected number of succeeded pods")
}

// jobPodOf returns a pod of the Job, named name, in the phase given, as the
// Job controller creates it: the template's labels plus the controller-uid
// label, a controller reference to the Job, and the claim the Job mounts.
func jobPodOf(job *batchv1.Job, name string, phase corev1.PodPhase) *corev1.Pod {
	labels := map[string]string{batchv1.ControllerUidLabel: string(job.UID), batchv1.JobNameLabel: job.Name}
	for key, value := range job.Spec.Template.Labels {
		labels[key] = value
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: job.Namespace, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID,
				Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}},
		},
		Spec: corev1.PodSpec{
			NodeName:   "worker-1",
			Containers: []corev1.Container{{Name: "restore", Image: testImage}},
			Volumes:    job.Spec.Template.Spec.Volumes,
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// createPod creates the pod and then writes its status, as the kubelet
// would, since a create drops the status.
func createPod(t *testing.T, c client.Client, pod *corev1.Pod) {
	t.Helper()
	status := pod.Status
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	pod.Status = status
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
}

// setPodPhase writes the phase of the stored pod.
func setPodPhase(t *testing.T, c client.Client, pod *corev1.Pod, phase corev1.PodPhase) {
	t.Helper()
	get(t, c, pod.Namespace, pod.Name, pod)
	pod.Status.Phase = phase
	if err := c.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
}

// movers returns the names of the restore movers in the test namespace: the
// restore Jobs a run creates, and any ReplicationDestination, which no run
// creates any more, so a test that wants no mover sees one of either kind.
func movers(t *testing.T, c client.Client) []string {
	t.Helper()
	names := destinations(t, c)
	for _, job := range restoreJobs(t, c) {
		names = append(names, job.Name)
	}
	return names
}

// swappedJob is a way the Job under a Running item's name stops being the
// Job the run recorded, for the tests of how followJob refuses it.
type swappedJob struct {
	// name names the subtest.
	name string
	// swap changes the recorded Job in place before the test stores it.
	swap func(job *batchv1.Job)
	// reason is the reason the item fails with.
	reason backupv1alpha1.ItemReason
	// stopped is true when the Job under the name still has the recorded
	// UID, so the run suspends and deletes it.
	stopped bool
}

// swappedJobs are the Jobs followJob refuses although one holds the item's
// name: a Job someone created under the name after the recorded one was
// deleted, which is Complete, and the recorded Job after someone removed
// its controller reference to the run.
var swappedJobs = []swappedJob{
	{name: "replaced", reason: backupv1alpha1.ItemReasonRestoreJobDeleted, swap: func(job *batchv1.Job) {
		job.UID = "replacement-uid"
		job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	}},
	{name: "no longer controlled", reason: backupv1alpha1.ItemReasonRestoreJobFailed, stopped: true, swap: func(job *batchv1.Job) {
		job.OwnerReferences = nil
	}},
}

// countJobCreates returns a client over c that counts the Jobs created
// through it, and the count.
func countJobCreates(c client.Client) (client.Client, *int) {
	creates := 0
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				creates++
			}
			return cl.Create(ctx, obj, opts...)
		},
	}), &creates
}

// expectSwappedJobLeft checks the Jobs a run left once it stopped the Job it
// recorded for a swapped Job: none when the Job under the name had the
// recorded UID, and otherwise that Job, neither suspended nor deleted.
func expectSwappedJobLeft(t *testing.T, c client.Client, tc swappedJob, job *batchv1.Job) {
	t.Helper()
	jobs := restoreJobs(t, c)
	if tc.stopped {
		if len(jobs) != 0 {
			t.Errorf("restore Jobs = %v, want the recorded Job deleted", jobs)
		}
		return
	}
	if len(jobs) != 1 || jobs[0].UID != job.UID || suspendedJob(t, c, job.Name) {
		t.Errorf("restore Jobs = %v, want the Job with UID %s left alone", jobs, job.UID)
	}
}
