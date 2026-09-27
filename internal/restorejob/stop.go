package restorejob

import (
	"context"
	"errors"
	"fmt"
	"strings"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// errNoUID is a Ref without a UID. Stop cannot find the pods of such a Job
// once the Job is gone, so it refuses rather than guess.
var errNoUID = errors.New("no UID recorded for the restore Job")

// Ref is a restore Job as its caller recorded it when it created the Job or
// took it over. The UID is what Stop decides on: a Job of the same name with
// another UID is not this one.
type Ref struct {
	// Namespace is the Job's namespace.
	Namespace string
	// Name is the Job's name.
	Name string
	// UID is the Job's UID. The Job controller puts it on every pod of the
	// Job as the batch.kubernetes.io/controller-uid label, and the pods keep
	// it after the Job is gone.
	UID types.UID
}

// RefOf returns the Ref of a Job the caller created or just read.
func RefOf(job *batchv1.Job) Ref {
	return Ref{Namespace: job.Namespace, Name: job.Name, UID: job.UID}
}

// StopState is how far Stop got with a restore Job.
type StopState struct {
	// Stopped is true once no pod of the Job can still write and the Job was
	// deleted or is gone. Every caller decides on this field alone.
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
//   - ref is the Job as the caller recorded it when it created the Job or
//     took it over, with its UID. Stop reads the Job itself on every call,
//     so the caller passes the same ref each time.
//
// It returns a StopState whose Stopped field says whether the Job is
// stopped, and an error for a ref without a UID or from any API call but a
// suspend or delete that finds the Job already gone. A caller that gets
// Stopped false waits and calls Stop again.
//
// A Job that is neither Complete nor Failed and not yet suspended is
// suspended first; the Job controller then deletes its active pods, restic
// gets SIGTERM, and the Job gets the condition Suspended=True. The call
// that patches the suspend stops there. The gate passes when the Job is
// finished, or one read shows both spec.suspend true and Suspended=True, and
// every pod the ref's UID selects has ended (phase Succeeded or Failed) or
// was never scheduled and is being deleted. Such a pod gets grace period 0
// and can no longer be bound to a node (kubernetes v1.36.3
// pkg/registry/core/pod/strategy.go:179-181,
// pkg/registry/core/pod/storage/storage.go:236). Past the gate, the Job is
// deleted with Foreground propagation and a UID precondition; Stop does not
// wait for it to go, since nothing of it can write any more.
//
// A Job that is gone (NotFound, or the name now holds a Job with another
// UID, which Stop leaves alone) goes straight to the gate on the ref's UID.
// Someone may have deleted it while it ran: with the Orphan propagation its
// pods keep running, and with Background they are stopped within their
// grace period. The pods keep their controller-uid label either way, so the
// gate holds until each of them has ended.
func Stop(ctx context.Context, api API, ref Ref) (StopState, error) {
	state := StopState{Job: ref.Name}
	if ref.UID == "" {
		return state, fmt.Errorf("stop restore Job %s: %w", ref.Name, errNoUID)
	}
	job, err := readOwn(ctx, api, ref)
	if err != nil {
		return state, err
	}
	if job != nil && !finished(job) {
		suspended, err := suspend(ctx, api, job)
		switch {
		case apierrors.IsNotFound(err):
			job = nil
		case err != nil:
			return state, err
		case !suspended:
			state.Suspending = true
			return state, nil
		}
	}
	if state.Pods, err = podsThatMayWrite(ctx, api, ref); err != nil || len(state.Pods) > 0 {
		return state, err
	}
	if job != nil {
		if err := api.DeleteJob(ctx, job); err != nil && !apierrors.IsNotFound(err) {
			return state, fmt.Errorf("delete restore Job %s: %w", job.Name, err)
		}
	}
	state.Stopped = true
	return state, nil
}

// readOwn reads the Job a ref names.
//
// Parameters:
//   - ctx bounds the API call.
//   - api makes the call.
//   - ref names the Job and carries its UID.
//
// It returns the Job, or nil when no Job of that name exists or the one that
// does has another UID, and an error from any other failed read.
func readOwn(ctx context.Context, api API, ref Ref) (*batchv1.Job, error) {
	job, err := api.GetJob(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name})
	switch {
	case apierrors.IsNotFound(err):
		return nil, nil
	case err != nil:
		return nil, fmt.Errorf("read restore Job %s: %w", ref.Name, err)
	case job.UID != ref.UID:
		return nil, nil
	default:
		return job, nil
	}
}

// finished reports whether the Job has Complete=True or Failed=True.
func finished(job *batchv1.Job) bool {
	return conditionTrue(job, batchv1.JobComplete) || conditionTrue(job, batchv1.JobFailed)
}

// podsThatMayWrite lists the pods a ref's UID selects and returns the names
// of those that may still write, and an error from the list.
func podsThatMayWrite(ctx context.Context, api API, ref Ref) ([]string, error) {
	pods, err := api.ListJobPods(ctx, ref.Namespace, ref.UID)
	if err != nil {
		return nil, fmt.Errorf("list the pods of restore Job %s: %w", ref.Name, err)
	}
	var names []string
	for _, p := range pods {
		if mayWrite(&p) {
			names = append(names, p.Name)
		}
	}
	return names, nil
}

// suspend makes sure an unfinished restore Job is suspended.
//
// Parameters:
//   - ctx bounds the API call.
//   - api makes the call.
//   - job is the Job as Stop just read it.
//
// It returns true when this read of the Job shows both spec.suspend true and
// the condition Suspended=True, and an error from the suspend patch, which
// wraps the API server's NotFound when the Job is gone.
//
// When the read shows spec.suspend false, suspend patches it and returns
// false even if the read also shows Suspended=True. Such a Job may have just
// been resumed: the Job controller writes JobResumed only after it created
// the pods (kubernetes v1.36.3 pkg/controller/job/job_controller.go:1137-1190),
// so a pod list in the same pass could miss a new pod. Only a later read that
// shows the suspend and the condition together lets the gate go on.
//
// That narrows the gap after a resume by someone other than the controller
// without closing it. When Stop's patch lands between the Job controller's
// pod create and its JobResumed write, that write conflicts, and the Job
// keeps the old Suspended=True beside spec.suspend true. A later read then
// passes, and the pod list sees every pod created before the patch. A pod
// the Job controller creates from a cache that still shows the Job resumed
// can come after that list, within the Job controller's cache lag. The
// declared setup has no one who resumes the controller's Jobs.
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
