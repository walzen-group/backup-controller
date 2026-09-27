package restorejob_test

import (
	"context"
	"slices"
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
