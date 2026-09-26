package restorejob

import (
	"context"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/utils/ptr"
)

// StopState is how far Stop got with a restore Job.
type StopState struct {
	// Stopped is true once no pod of the Job can still write and the Job was
	// deleted. Every caller decides on this field alone.
	Stopped bool
	// Job is the Job's name, for the wait message.
	Job string
	// Suspending is true while the Job is neither finished nor Suspended, so
	// the Job controller may still start a pod. It is for the wait message.
	Suspending bool
	// Pods are the names of the Job's pods that may still write, for the
	// wait message.
	Pods []string
}

// String renders what the stop still waits for.
func (s StopState) String() string {
	switch {
	case s.Stopped:
		return fmt.Sprintf("restore Job %s stopped", s.Job)
	case s.Suspending:
		return fmt.Sprintf("restore Job %s: waiting for the Job controller to suspend it", s.Job)
	default:
		return fmt.Sprintf("restore Job %s: waiting for pods %s to end", s.Job, strings.Join(s.Pods, ", "))
	}
}

// Stop ends a restore Job so that nothing writes the claim any more: it
// suspends the Job, waits at the gate, then deletes the Job.
//
// Parameters:
//   - ctx bounds the API calls.
//   - api makes the calls; its reads come fresh from the API server.
//   - job is the Job as the caller just read it through api. Stop acts on
//     this Job's UID only, so a Job created under the same name since then
//     is left alone.
//
// It returns a StopState whose Stopped field says whether the Job is
// stopped, and an error from any API call but a delete that finds the Job
// already gone. A caller that gets Stopped false waits and calls Stop again
// with the Job read again.
//
// A Job that is neither Complete nor Failed and not yet suspended is
// suspended first; the Job controller then deletes its active pods, restic
// gets SIGTERM, and the Job gets the condition Suspended=True. The gate
// passes when the Job is finished or Suspended=True, and every pod the Job's
// UID selects has ended (phase Succeeded or Failed) or was never scheduled
// and is being deleted. Such a pod gets grace period 0 and can no longer be
// bound to a node (kubernetes v1.36.3 pkg/registry/core/pod/strategy.go:179-181,
// pkg/registry/core/pod/storage/storage.go:236). Past the gate, the Job is
// deleted with Foreground propagation and a UID precondition; Stop does not
// wait for it to go, since nothing of it can write any more.
func Stop(ctx context.Context, api API, job *batchv1.Job) (StopState, error) {
	state := StopState{Job: job.Name}
	finished := conditionTrue(job, batchv1.JobComplete) || conditionTrue(job, batchv1.JobFailed)
	if !finished && !ptr.Deref(job.Spec.Suspend, false) {
		if err := api.SuspendJob(ctx, job); err != nil {
			return state, fmt.Errorf("suspend restore Job %s: %w", job.Name, err)
		}
	}
	if !finished && !conditionTrue(job, batchv1.JobSuspended) {
		state.Suspending = true
		return state, nil
	}
	pods, err := api.ListJobPods(ctx, job)
	if err != nil {
		return state, fmt.Errorf("list the pods of restore Job %s: %w", job.Name, err)
	}
	for _, p := range pods {
		if mayWrite(&p) {
			state.Pods = append(state.Pods, p.Name)
		}
	}
	if len(state.Pods) > 0 {
		return state, nil
	}
	if err := api.DeleteJob(ctx, job); err != nil && !apierrors.IsNotFound(err) {
		return state, fmt.Errorf("delete restore Job %s: %w", job.Name, err)
	}
	state.Stopped = true
	return state, nil
}

// mayWrite reports whether a pod may still run restic: it has not ended, and
// it is scheduled or could still be.
func mayWrite(p *corev1.Pod) bool {
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return false
	}
	return p.Spec.NodeName != "" || p.DeletionTimestamp == nil
}
