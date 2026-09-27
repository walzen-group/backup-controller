package restorejob_test

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

// createJob creates the Job Build makes from runSpec, with the conditions
// given set True in its status.
func (k *cluster) createJob(conditions ...batchv1.JobConditionType) *batchv1.Job {
	k.t.Helper()
	ctx := context.Background()
	job := build(k.t, runSpec())
	if err := k.api.CreateJob(ctx, job); err != nil {
		k.t.Fatal(err)
	}
	for _, typ := range conditions {
		withCondition(job, typ, "", "")
	}
	if len(conditions) > 0 {
		if err := k.c.Status().Update(ctx, job); err != nil {
			k.t.Fatal(err)
		}
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

func TestStopSuspendsARunningJobAndWaits(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
	state := k.stop(job)
	if state.Stopped {
		t.Fatal("Stop reports a running Job stopped")
	}
	job = k.read(job)
	if job.Spec.Suspend == nil || !*job.Spec.Suspend {
		t.Fatalf("spec.suspend = %v, want true", job.Spec.Suspend)
	}
	if job.DeletionTimestamp != nil || k.foregroundDeleted(job) {
		t.Error("Stop deleted a Job that is not yet suspended")
	}
	if state.Job != job.Name || state.String() == "" {
		t.Errorf("state = %+v (%q), want the Job named", state, state.String())
	}
	// Until the Job controller sets Suspended, the gate holds even with no pod.
	k.setPhase(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "app"}}, corev1.PodFailed)
	if k.stop(job).Stopped {
		t.Error("Stop passed the gate before the Job is Suspended")
	}
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

func TestStopGatePassesAnUnscheduledPodOnlyWhileItIsDeleted(t *testing.T) {
	k := newCluster(t)
	job := k.suspended()
	pod := k.createPod("gated", job, job.UID, "", corev1.PodPending)
	if state := k.stop(job); state.Stopped || !slices.Equal(state.Pods, []string{"gated"}) {
		t.Fatalf("state = %+v, want an unscheduled pod that is not being deleted to hold the gate", state)
	}
	if err := k.c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if state := k.stop(k.read(job)); !state.Stopped {
		t.Fatalf("state = %+v, want an unscheduled pod being deleted to pass", state)
	}
}

func TestStopOfAFinishedJobDeletesItWithoutSuspending(t *testing.T) {
	for _, typ := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
		t.Run(string(typ), func(t *testing.T) {
			k := newCluster(t)
			job := k.createJob(typ)
			k.createPod("p", job, job.UID, "node-a", corev1.PodSucceeded)
			state := k.stop(job)
			if !state.Stopped {
				t.Fatalf("state = %+v, want stopped", state)
			}
			if !k.foregroundDeleted(job) {
				t.Error("the Job was not deleted with Foreground propagation")
			}
			// The pod keeps its finalizer, so the Job waits for it.
			got := &batchv1.Job{}
			if err := k.c.Get(context.Background(), client.ObjectKeyFromObject(job), got); err != nil {
				t.Fatal(err)
			}
			if got.Spec.Suspend != nil && *got.Spec.Suspend {
				t.Error("Stop suspended a finished Job")
			}
			// A second pass finds the Job being deleted and stops it again
			// without an error.
			if again := k.stop(got); !again.Stopped {
				t.Errorf("second Stop = %+v, want stopped", again)
			}
		})
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

func TestStopStateNamesNoEmptyPodList(t *testing.T) {
	got := restorejob.StopState{Job: "j"}.String()
	if strings.Contains(got, "pods") || strings.Contains(got, "  ") {
		t.Errorf("String() = %q, want no pod list when no pod is named", got)
	}
	got = restorejob.StopState{Job: "j", Pods: []string{"a", "b"}}.String()
	if !strings.Contains(got, "a, b") {
		t.Errorf("String() = %q, want the pods named", got)
	}
}

func TestStopOfAJobAlreadyGone(t *testing.T) {
	k := newCluster(t)
	job := k.createJob(batchv1.JobComplete)
	if err := k.c.Delete(context.Background(), job, client.PropagationPolicy(metav1.DeletePropagationBackground)); err != nil {
		t.Fatal(err)
	}
	if state := k.stop(job); !state.Stopped {
		t.Errorf("state = %+v, want a Job that is gone stopped", state)
	}
}

func TestSuspendRefusesAReplacedJob(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	stale := job.DeepCopy()
	stale.UID = "an-earlier-job"
	if err := k.api.SuspendJob(context.Background(), stale); err == nil {
		t.Fatal("SuspendJob suspended a Job other than the one it was given")
	}
	if got := k.read(job); got.Spec.Suspend != nil && *got.Spec.Suspend {
		t.Error("the Job that replaced the one given was suspended")
	}
}

func TestDeleteRefusesAReplacedJob(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	stale := job.DeepCopy()
	stale.UID = "an-earlier-job"
	if err := k.api.DeleteJob(context.Background(), stale); !apierrors.IsConflict(err) {
		t.Fatalf("DeleteJob = %v, want a Conflict for a Job other than the one it was given", err)
	}
	if got := k.read(job); got.DeletionTimestamp != nil || k.foregroundDeleted(job) {
		t.Error("the Job that replaced the one given was deleted")
	}
}

func TestStopAndReadIgnoreAnOlderJobsPods(t *testing.T) {
	k := newCluster(t)
	job := k.suspended()
	// A pod of an earlier Job of the same name, with the same labels but
	// the earlier Job's UID, still running and with a failed container.
	old := k.createPod("old", job, "an-earlier-job", "node-a", corev1.PodRunning)
	old.Status.ContainerStatuses = []corev1.ContainerStatus{terminated("restore", 1, "Fatal: older")}
	if err := k.c.Status().Update(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	pods, err := k.api.ListJobPods(context.Background(), job.Namespace, job.UID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pods) != 0 {
		t.Errorf("ListJobPods = %d pods, want none of an earlier Job", len(pods))
	}
	if state := k.stop(job); !state.Stopped {
		t.Errorf("state = %+v, want an earlier Job's pod to leave the gate open", state)
	}
	failed := withCondition(job.DeepCopy(), batchv1.JobFailed, "BackoffLimitExceeded", "")
	if f := restorejob.Read(failed, []corev1.Pod{*old}).Failure; f == nil || f.ExitCode != nil {
		t.Errorf("failure = %+v, want no exit code from an earlier Job's pod", f)
	}
}

func TestReadsGoThroughTheReader(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
	// k.api writes through noReads, so each read below fails if it goes
	// through the writing client.
	if _, err := k.api.GetJob(context.Background(), client.ObjectKeyFromObject(job)); err != nil {
		t.Error(err)
	}
	pods, err := k.api.ListJobPods(context.Background(), job.Namespace, job.UID)
	if err != nil || len(pods) != 1 {
		t.Errorf("ListJobPods = %d pods, %v; want p", len(pods), err)
	}
	if _, err := restorejob.Stop(context.Background(), k.api, restorejob.RefOf(job)); err != nil {
		t.Error(err)
	}
}
