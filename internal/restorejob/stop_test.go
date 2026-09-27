package restorejob_test

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/walzen-group/backup-controller/internal/restorejob"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
)

// jobTracking is the finalizer the Job controller puts on every pod it
// creates and removes once it has counted the pod's end. A pod that carries it
// stays after a delete, as a pod does on a real cluster while its kubelet
// stops it.
const jobTracking = "batch.kubernetes.io/job-tracking"

// noReads is a client whose reads fail, so a test sees any read that goes
// through the writer instead of the uncached reader.
type noReads struct{ client.Client }

func (noReads) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("read through the writing client")
}

func (noReads) List(context.Context, client.ObjectList, ...client.ListOption) error {
	return errors.New("list through the writing client")
}

// cluster is a strict client with the garbage collector on, and the API the
// package builds over it.
type cluster struct {
	t   *testing.T
	c   *strictclient.Client
	api restorejob.API
}

func newCluster(t *testing.T) *cluster {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c := strictclient.New(fake.NewClientBuilder().WithScheme(scheme).Build(), strictclient.Options{
		Clock:          func() time.Time { return podTime },
		GarbageCollect: true,
	})
	return &cluster{t: t, c: c, api: restorejob.NewAPI(c, noReads{c})}
}

// created creates the Job Build makes from runSpec, which starts
// suspended, and sets Suspended=True as the Job controller does right after
// the create. Nothing has resumed it.
func (k *cluster) created() *batchv1.Job {
	k.t.Helper()
	ctx := context.Background()
	job := build(k.t, runSpec())
	if err := k.api.CreateJob(ctx, job); err != nil {
		k.t.Fatal(err)
	}
	job = k.read(job)
	withCondition(job, batchv1.JobSuspended, "JobSuspended", "Job suspended")
	if err := k.c.Status().Update(ctx, job); err != nil {
		k.t.Fatal(err)
	}
	return k.read(job)
}

// createJob creates the Job Build makes from runSpec and resumes it, as its
// creator does once it has recorded the Job, with the conditions given set
// True in its status.
func (k *cluster) createJob(conditions ...batchv1.JobConditionType) *batchv1.Job {
	k.t.Helper()
	ctx := context.Background()
	job := k.created()
	if err := k.api.ResumeJob(ctx, job); err != nil {
		k.t.Fatal(err)
	}
	// The Job controller sets Suspended to False with reason JobResumed.
	job = k.read(job)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuspended, Status: corev1.ConditionFalse, Reason: "JobResumed"}}
	for _, typ := range conditions {
		withCondition(job, typ, "", "")
	}
	if err := k.c.Status().Update(ctx, job); err != nil {
		k.t.Fatal(err)
	}
	return k.read(job)
}

// suspended creates a Job whose spec.suspend is set and that has the
// Suspended condition, as the Job controller leaves it after a suspend.
func (k *cluster) suspended() *batchv1.Job {
	k.t.Helper()
	job := k.createJob()
	if err := k.api.SuspendJob(context.Background(), job); err != nil {
		k.t.Fatal(err)
	}
	job = k.read(job)
	withCondition(job, batchv1.JobSuspended, "JobSuspended", "Job suspended")
	if err := k.c.Status().Update(context.Background(), job); err != nil {
		k.t.Fatal(err)
	}
	return k.read(job)
}

func (k *cluster) read(job *batchv1.Job) *batchv1.Job {
	k.t.Helper()
	got, err := k.api.GetJob(context.Background(), client.ObjectKeyFromObject(job))
	if err != nil {
		k.t.Fatal(err)
	}
	return got
}

// createPod creates a pod of the Job whose UID is given, as the Job
// controller does, on the node given (none for an unscheduled pod), in the
// phase given.
func (k *cluster) createPod(name string, job *batchv1.Job, uid types.UID, node string, phase corev1.PodPhase) *corev1.Pod {
	k.t.Helper()
	ctx := context.Background()
	pod := jobPod(name, uid, 0)
	pod.Labels = map[string]string{batchv1.ControllerUidLabel: string(uid), batchv1.JobNameLabel: job.Name}
	for key, v := range job.Spec.Template.Labels {
		pod.Labels[key] = v
	}
	pod.Finalizers = []string{jobTracking}
	pod.OwnerReferences = []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: uid,
		Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}}
	pod.Spec = corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "restore", Image: "restic"}}}
	if err := k.c.Create(ctx, &pod); err != nil {
		k.t.Fatal(err)
	}
	pod.Status.Phase = phase
	if err := k.c.Status().Update(ctx, &pod); err != nil {
		k.t.Fatal(err)
	}
	return &pod
}

func (k *cluster) setPhase(pod *corev1.Pod, phase corev1.PodPhase) {
	k.t.Helper()
	if err := k.c.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil {
		k.t.Fatal(err)
	}
	pod.Status.Phase = phase
	if err := k.c.Status().Update(context.Background(), pod); err != nil {
		k.t.Fatal(err)
	}
}

func (k *cluster) stop(job *batchv1.Job) restorejob.StopState {
	k.t.Helper()
	state, err := restorejob.Stop(context.Background(), k.api, restorejob.RefOf(job))
	if err != nil {
		k.t.Fatal(err)
	}
	return state
}

// foregroundDeleted reports whether the Job was deleted with Foreground
// propagation.
func (k *cluster) foregroundDeleted(job *batchv1.Job) bool {
	for _, cs := range k.c.Cascades() {
		if cs.Owner.UID == job.UID && cs.Propagation == metav1.DeletePropagationForeground {
			return true
		}
	}
	return false
}

func TestStopGateWaitsForARunningPodOfASuspendedJob(t *testing.T) {
	k := newCluster(t)
	job := k.suspended()
	pod := k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
	// The Job controller deletes the pod of a suspended Job; the kubelet
	// stops it within its grace period.
	if err := k.c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	state := k.stop(job)
	if state.Stopped || !slices.Equal(state.Pods, []string{"p"}) {
		t.Fatalf("state = %+v, want not stopped with pod p left", state)
	}
	if k.foregroundDeleted(job) {
		t.Fatal("Stop deleted the Job while its pod may still write")
	}
	k.setPhase(pod, corev1.PodFailed)
	state = k.stop(k.read(job))
	if !state.Stopped || len(state.Pods) != 0 {
		t.Fatalf("state = %+v, want stopped", state)
	}
	if !k.foregroundDeleted(job) {
		t.Error("the Job was not deleted with Foreground propagation")
	}
}

func TestStopOfAFinishedJobWaitsForAPodThatMayStillWrite(t *testing.T) {
	for _, typ := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run(string(typ), func(t *testing.T) {
			k := newCluster(t)
			job := k.createJob(typ)
			// A pod left running on a node, as a Job controller older than
			// Kubernetes 1.31 could leave one past the terminal condition.
			pod := k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
			state := k.stop(job)
			if state.Stopped || !slices.Equal(state.Pods, []string{"p"}) {
				t.Fatalf("state = %+v, want not stopped with pod p left", state)
			}
			if k.foregroundDeleted(job) {
				t.Fatal("Stop deleted a finished Job while its pod may still write")
			}
			k.setPhase(pod, corev1.PodSucceeded)
			if state := k.stop(k.read(job)); !state.Stopped {
				t.Fatalf("state = %+v, want stopped once the pod ended", state)
			}
		})
	}
}

func TestStopDoesNotPassTheGateInTheCallThatSuspends(t *testing.T) {
	k := newCluster(t)
	job := k.suspended()
	// Someone resumes the Job. The read still shows Suspended=True, since
	// the Job controller writes JobResumed only after it created the pods.
	job.Spec.Suspend = ptr.To(false)
	if err := k.c.Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	job = k.read(job)
	state := k.stop(job)
	if state.Stopped || !state.Suspending {
		t.Fatalf("state = %+v, want Suspending after the suspend patch", state)
	}
	if k.foregroundDeleted(job) {
		t.Fatal("Stop deleted the Job in the call that suspended it")
	}
	job = k.read(job)
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		t.Fatalf("spec.suspend = %v, want true", job.Spec.Suspend)
	}
	if state := k.stop(job); !state.Stopped {
		t.Fatalf("state = %+v, want stopped once a read shows the Job suspended", state)
	}
}

// MayStillWrite holds a Job the Job controller may still start a pod for,
// and a Job with a pod that may still run restic, and passes a Job that is
// finished, suspended in one read or being deleted once each of its own
// pods has ended or was never scheduled and is being deleted.
func TestMayStillWrite(t *testing.T) {
	deleting := metav1.NewTime(podTime)
	pod := func(phase corev1.PodPhase, node string, deleted bool, uid types.UID) corev1.Pod {
		p := jobPod("restore-3a7c-0-x7k2p", uid, 0)
		p.Spec.NodeName, p.Status.Phase = node, phase
		if deleted {
			p.DeletionTimestamp = &deleting
		}
		return p
	}
	for _, tc := range []struct {
		name  string
		job   func(job *batchv1.Job)
		pods  []corev1.Pod
		holds bool
	}{
		{name: "running, no pod yet", holds: true},
		{name: "created suspended, never resumed", job: suspendedJob},
		{name: "suspend asked, condition not yet set", job: func(j *batchv1.Job) { j.Spec.Suspend = ptr.To(true) }, holds: true},
		{name: "suspended", job: suspendedJob},
		{name: "suspended, pod running", job: suspendedJob, pods: []corev1.Pod{pod(corev1.PodRunning, "worker-1", true, "job-uid")}, holds: true},
		{name: "suspended, unscheduled pod being deleted", job: suspendedJob, pods: []corev1.Pod{pod(corev1.PodPending, "", true, "job-uid")}},
		{name: "suspended, unscheduled pod not deleted", job: suspendedJob, pods: []corev1.Pod{pod(corev1.PodPending, "", false, "job-uid")}, holds: true},
		{name: "complete, pod succeeded", job: completeJob, pods: []corev1.Pod{pod(corev1.PodSucceeded, "worker-1", false, "job-uid")}},
		{name: "failed, pod failed", job: func(j *batchv1.Job) { withCondition(j, batchv1.JobFailed, "BackoffLimitExceeded", "") },
			pods: []corev1.Pod{pod(corev1.PodFailed, "worker-1", false, "job-uid")}},
		{name: "complete, another Job's pod running", job: completeJob, pods: []corev1.Pod{pod(corev1.PodRunning, "worker-1", false, "other-uid")}},
		{name: "being deleted, pods gone", job: func(j *batchv1.Job) { j.DeletionTimestamp = &deleting }},
		{name: "being deleted, pod running", job: func(j *batchv1.Job) { j.DeletionTimestamp = &deleting },
			pods: []corev1.Pod{pod(corev1.PodRunning, "worker-1", false, "job-uid")}, holds: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			job := readJob(t)
			if tc.job != nil {
				tc.job(job)
			}
			if got := restorejob.MayStillWrite(job, tc.pods); got != tc.holds {
				t.Errorf("MayStillWrite = %t, want %t", got, tc.holds)
			}
		})
	}
}

// suspendedJob marks a Job suspended in one read: spec.suspend true and
// Suspended=True.
func suspendedJob(job *batchv1.Job) {
	job.Spec.Suspend = ptr.To(true)
	withCondition(job, batchv1.JobSuspended, "JobSuspended", "Job suspended")
}

// completeJob marks a Job Complete.
func completeJob(job *batchv1.Job) {
	withCondition(job, batchv1.JobComplete, "CompletionsReached", "")
}

// ResumeJob clears spec.suspend on the Job it is given, and on no Job that
// replaced it under the same name.
func TestResumeJobResumesOnlyTheJobGiven(t *testing.T) {
	k := newCluster(t)
	job := k.created()
	stale := job.DeepCopy()
	stale.UID = "an-earlier-job"
	if err := k.api.ResumeJob(context.Background(), stale); err == nil {
		t.Fatal("ResumeJob resumed a Job other than the one it was given")
	}
	if got := k.read(job); !ptr.Deref(got.Spec.Suspend, false) {
		t.Fatal("the Job that replaced the one given was resumed")
	}
	if err := k.api.ResumeJob(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	if got := k.read(job); ptr.Deref(got.Spec.Suspend, true) {
		t.Errorf("spec.suspend = %v after ResumeJob, want false", got.Spec.Suspend)
	}
}

// A Job that was created suspended and never resumed has no pod, so Stop
// deletes it at once, with no suspend patch.
func TestStopOfAJobNeverResumedDeletesIt(t *testing.T) {
	k := newCluster(t)
	job := k.created()
	if state := k.stop(job); !state.Stopped {
		t.Fatalf("state = %+v, want a Job that never ran stopped at once", state)
	}
	if !k.foregroundDeleted(job) {
		t.Error("the Job was not deleted with Foreground propagation")
	}
}
