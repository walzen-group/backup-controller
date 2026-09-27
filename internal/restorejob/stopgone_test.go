package restorejob_test

import (
	"context"
	"slices"
	"strings"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/walzen-group/backup-controller/internal/restorejob"
)

// deleteJob deletes the Job as a person would, with the propagation given.
func (k *cluster) deleteJob(job *batchv1.Job, propagation metav1.DeletionPropagation) {
	k.t.Helper()
	if err := k.c.Delete(context.Background(), job, client.PropagationPolicy(propagation)); err != nil {
		k.t.Fatal(err)
	}
}

// podGone reports whether the pod no longer exists.
func (k *cluster) podGone(pod *corev1.Pod) bool {
	k.t.Helper()
	err := k.c.Get(context.Background(), client.ObjectKeyFromObject(pod), &corev1.Pod{})
	if client.IgnoreNotFound(err) != nil {
		k.t.Fatal(err)
	}
	return err != nil
}

func TestStopHoldsForAPodOfAJobDeletedWithOrphan(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	pod := k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
	// kubectl delete job --cascade=orphan: the Job goes, its pod keeps
	// running restic.
	k.deleteJob(job, metav1.DeletePropagationOrphan)
	if k.podGone(pod) {
		t.Fatal("the orphan delete took the pod with it")
	}
	state := k.stop(job)
	if state.Stopped || !slices.Equal(state.Pods, []string{"p"}) {
		t.Fatalf("state = %+v, want not stopped with pod p left", state)
	}
	k.setPhase(pod, corev1.PodSucceeded)
	if state := k.stop(job); !state.Stopped {
		t.Fatalf("state = %+v, want stopped once the pod ended", state)
	}
}

func TestStopHoldsForATerminatingPodOfAJobDeletedInBackground(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	pod := k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
	// kubectl delete job: the Job goes at once, and the garbage collector
	// deletes the pod, which runs on until its kubelet has stopped it.
	k.deleteJob(job, metav1.DeletePropagationBackground)
	if err := k.c.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	if pod.DeletionTimestamp == nil {
		t.Fatal("the garbage collector did not delete the pod")
	}
	state := k.stop(job)
	if state.Stopped || !slices.Equal(state.Pods, []string{"p"}) {
		t.Fatalf("state = %+v, want not stopped with the terminating pod p left", state)
	}
	pod.Finalizers = nil
	if err := k.c.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if !k.podGone(pod) {
		t.Fatal("the pod stayed after its last finalizer went")
	}
	if state := k.stop(job); !state.Stopped {
		t.Fatalf("state = %+v, want stopped once the pod is gone", state)
	}
}

func TestStopOfARunningJobDeletedWithNoPodsLeft(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	k.deleteJob(job, metav1.DeletePropagationBackground)
	if state := k.stop(job); !state.Stopped {
		t.Fatalf("state = %+v, want stopped with no pod left", state)
	}
}

// deletingSuspend is an API whose SuspendJob first deletes the Job with
// Orphan propagation, as a person could between Stop's read and its patch.
type deletingSuspend struct {
	restorejob.API
	k *cluster
}

func (d deletingSuspend) SuspendJob(ctx context.Context, job *batchv1.Job) error {
	d.k.deleteJob(job, metav1.DeletePropagationOrphan)
	return d.API.SuspendJob(ctx, job)
}

func TestStopOfAJobDeletedBetweenItsReadAndTheSuspend(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	pod := k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
	api := deletingSuspend{API: k.api, k: k}
	state, err := restorejob.Stop(context.Background(), api, restorejob.RefOf(job))
	if err != nil || state.Stopped || !slices.Equal(state.Pods, []string{"p"}) {
		t.Fatalf("Stop = %+v, %v; want no error and not stopped with pod p left", state, err)
	}
	k.setPhase(pod, corev1.PodFailed)
	if state := k.stop(job); !state.Stopped {
		t.Fatalf("state = %+v, want stopped once the pod ended", state)
	}
}

func TestStopLeavesAJobThatReplacedItsRefAlone(t *testing.T) {
	k := newCluster(t)
	job := k.createJob(batchv1.JobFailed)
	// The ref names an earlier Job of the same name whose pod still runs.
	earlier := restorejob.RefOf(job)
	earlier.UID = "an-earlier-job"
	pod := k.createPod("old", job, earlier.UID, "node-a", corev1.PodRunning)
	state, err := restorejob.Stop(context.Background(), k.api, earlier)
	if err != nil || state.Stopped || !slices.Equal(state.Pods, []string{"old"}) {
		t.Fatalf("Stop = %+v, %v; want not stopped with the earlier Job's pod left", state, err)
	}
	k.setPhase(pod, corev1.PodFailed)
	state, err = restorejob.Stop(context.Background(), k.api, earlier)
	if err != nil || !state.Stopped {
		t.Fatalf("Stop = %+v, %v; want stopped once the earlier Job's pod ended", state, err)
	}
	if got := k.read(job); got.DeletionTimestamp != nil || k.foregroundDeleted(job) {
		t.Error("Stop deleted the Job that replaced the one its ref names")
	}
}

func TestStopRefusesARefWithoutAUID(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	ref := restorejob.RefOf(job)
	ref.UID = ""
	state, err := restorejob.Stop(context.Background(), k.api, ref)
	if err == nil || state.Stopped {
		t.Fatalf("Stop = %+v, %v; want an error and not stopped", state, err)
	}
	if got := k.read(job); got.Spec.Suspend != nil && *got.Spec.Suspend {
		t.Error("Stop suspended a Job of a ref without a UID")
	}
}

func TestStopOfAJobBeingDeletedInForegroundGoesToTheGate(t *testing.T) {
	k := newCluster(t)
	job := k.createJob()
	pod := k.createPod("p", job, job.UID, "node-a", corev1.PodRunning)
	// kubectl delete job --cascade=foreground: the Job stays with a
	// deletionTimestamp until the garbage collector has deleted its pod, and
	// the Job controller never suspends a Job that is being deleted.
	k.deleteJob(job, metav1.DeletePropagationForeground)
	if got := k.read(job); got.DeletionTimestamp == nil {
		t.Fatal("the foreground delete did not leave the Job with a deletionTimestamp")
	}
	state := k.stop(job)
	if state.Stopped || state.Suspending || !slices.Equal(state.Pods, []string{"p"}) {
		t.Fatalf("state = %+v, want not stopped, not suspending, with pod p left", state)
	}
	if msg := state.String(); !strings.Contains(msg, "being deleted") || strings.Contains(msg, "suspend") {
		t.Errorf("String() = %q, want it to say the Job is being deleted", msg)
	}
	if got := k.read(job); got.Spec.Suspend != nil && *got.Spec.Suspend {
		t.Error("Stop patched the suspend on a Job that is being deleted")
	}
	if err := k.c.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = nil
	if err := k.c.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	if state := k.stop(job); !state.Stopped {
		t.Fatalf("state = %+v, want stopped once the pod is gone", state)
	}
}
