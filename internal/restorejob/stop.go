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
	// Suspending is true while the Job is not finished and the last read did
	// not show both spec.suspend true and Suspended=True, so the Job
	// controller may still start a pod. It is for the wait message.
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
	case len(s.Pods) > 0:
		return fmt.Sprintf("restore Job %s: waiting for pods %s to end", s.Job, strings.Join(s.Pods, ", "))
	default:
		return fmt.Sprintf("restore Job %s: stop not finished", s.Job)
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
// gets SIGTERM, and the Job gets the condition Suspended=True. The call
// that patches the suspend stops there. The gate passes when the Job is
// finished, or one read shows both spec.suspend true and Suspended=True, and
// every pod the Job's UID selects has ended (phase Succeeded or Failed) or
// was never scheduled and is being deleted. Such a pod gets grace period 0 and can no longer be
// bound to a node (kubernetes v1.36.3 pkg/registry/core/pod/strategy.go:179-181,
// pkg/registry/core/pod/storage/storage.go:236). Past the gate, the Job is
// deleted with Foreground propagation and a UID precondition; Stop does not
// wait for it to go, since nothing of it can write any more.
func Stop(ctx context.Context, api API, job *batchv1.Job) (StopState, error) {
	state := StopState{Job: job.Name}
	finished := conditionTrue(job, batchv1.JobComplete) || conditionTrue(job, batchv1.JobFailed)
	if !finished {
		suspended, err := suspend(ctx, api, job)
		if err != nil {
			return state, err
		}
		if !suspended {
			state.Suspending = true
			return state, nil
		}
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

// suspend makes sure an unfinished restore Job is suspended.
//
// Parameters:
//   - ctx bounds the API call.
//   - api makes the call.
//   - job is the Job as the caller just read it.
//
// It returns true when this read of the Job shows both spec.suspend true and
// the condition Suspended=True, and an error from the suspend patch.
//
// When the read shows spec.suspend false, suspend patches it and returns
// false even if the read also shows Suspended=True. Such a Job may have just
// been resumed: the Job controller writes JobResumed only after it created
// the pods (kubernetes v1.36.3 pkg/controller/job/job_controller.go:1137-1190),
// so a pod list in the same pass could miss a new pod. Only a later read that
// shows the suspend and the condition together lets the gate go on.
func suspend(ctx context.Context, api API, job *batchv1.Job) (bool, error) {
	if !ptr.Deref(job.Spec.Suspend, false) {
		if err := api.SuspendJob(ctx, job); err != nil {
			return false, fmt.Errorf("suspend restore Job %s: %w", job.Name, err)
		}
		return false, nil
	}
	return conditionTrue(job, batchv1.JobSuspended), nil
}

// mayWrite reports whether a pod may still run restic: it has not ended, and
// it is scheduled or could still be.
func mayWrite(p *corev1.Pod) bool {
	if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
		return false
	}
	return p.Spec.NodeName != "" || p.DeletionTimestamp == nil
}
