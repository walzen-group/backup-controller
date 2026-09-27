package restorejob

import (
	"cmp"
	"fmt"
	"slices"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"
)

// restic's exit codes (cmd/restic/main.go:203-224 and doc/075_scripting.rst
// in restic 0.18.1). An exit code the table does not name is a failure.
const (
	exitSuccess       = 0
	exitFailure       = 1
	exitGoRuntime     = 2
	exitSourceData    = 3
	exitNoRepository  = 10
	exitLocked        = 11
	exitWrongPassword = 12
	exitInterrupted   = 130
	// exitKilled is the container runtime's code for a process ended by
	// SIGKILL (128 + 9), which restic cannot catch.
	exitKilled = 137
)

// State is where a restore Job stands, from its terminal conditions alone.
type State int

// The three states of a restore Job.
const (
	// Running is a Job with neither Complete nor Failed True.
	Running State = iota + 1
	// Succeeded is a Job with Complete True: restic exited 0 for the
	// Job's snapshot.
	Succeeded
	// Failed is a Job with Failed True.
	Failed
)

// Outcome is what Read finds in a restore Job and its pods.
type Outcome struct {
	// State decides: the caller acts on it and on nothing else here.
	State State
	// Failure describes a Failed Job for the item's message.
	Failure *FailureError
	// Waiting is why a Running Job's newest pod has not started, for the
	// item's message. It is nil when the pod is not waiting.
	Waiting *Waiting
	// Starting is true for a Running Job that still awaits its creator's
	// resume (see AwaitsResume), for the item's message. It decides nothing.
	Starting bool
}

// FailureError is a failed restore Job, rendered for a person. Nothing
// decides on its fields; the Job's Failed condition already decided.
type FailureError struct {
	// Condition is the Job's Failed condition, whose reason says which limit
	// or rule ended the Job.
	Condition batchv1.JobCondition
	// ExitCode is the exit code of the newest pod's container that ended
	// with a non-zero code. It is nil when no pod of the Job shows one, as
	// when the pods are already gone.
	ExitCode *int32
	// Container is the name of that container: unlock or restore.
	Container string
	// Message is that container's termination message, the tail of its log.
	Message string
}

// Error renders the failure with restic's exit code and its meaning, or,
// with no exit code, the Job condition's own message.
func (e *FailureError) Error() string {
	if e.ExitCode == nil {
		return fmt.Sprintf("the restore Job failed (%s), and no pod of it shows how restic ended: %q",
			e.Condition.Reason, e.Condition.Message)
	}
	return fmt.Sprintf("the restore Job failed (%s): restic exited %d (%s) in container %s: %q",
		e.Condition.Reason, *e.ExitCode, ExitMeaning(*e.ExitCode), e.Container, e.Message)
}

// Waiting is why a restore Job's pod has not started: a container's waiting
// state, or the pod's PodScheduled=False condition. It is shown to a person
// and decides nothing.
type Waiting struct {
	// Container is the waiting container's name, and empty for the
	// PodScheduled condition.
	Container string
	// Reason is the waiting state's or the condition's reason, such as
	// ImagePullBackOff or SchedulingGated.
	Reason string
	// Message is the waiting state's or the condition's message.
	Message string
}

// String renders the wait, as in "waiting: restore: ImagePullBackOff: ...".
func (w Waiting) String() string {
	if w.Container == "" {
		return fmt.Sprintf("waiting: unscheduled: %s: %q", w.Reason, w.Message)
	}
	return fmt.Sprintf("waiting: %s: %s: %q", w.Container, w.Reason, w.Message)
}

// Read reports where a restore Job stands.
//
// Parameters:
//   - job is the Job as the caller just read it.
//   - pods are pods the caller listed for it, such as by ListJobPods. Read
//     considers only those whose batch.kubernetes.io/controller-uid label is
//     the Job's UID, so a pod of an earlier Job of the same name never counts.
//
// It returns Succeeded for Complete=True and Failed for Failed=True, else
// Running. The Job controller adds either condition only once every pod has
// ended. A Failed outcome carries a FailureError built from the newest pod
// whose container or init container ended non-zero. A Running outcome carries
// the newest pod's waiting reason, if it has one. It is Starting while the Job
// still awaits its creator's resume. None of these decides anything.
func Read(job *batchv1.Job, pods []corev1.Pod) Outcome {
	own := ownPods(job, pods)
	switch {
	case conditionTrue(job, batchv1.JobComplete):
		return Outcome{State: Succeeded}
	case conditionTrue(job, batchv1.JobFailed):
		return Outcome{State: Failed, Failure: failure(job, own)}
	default:
		return Outcome{State: Running, Waiting: newestWaiting(own), Starting: AwaitsResume(job)}
	}
}

// conditionTrue reports whether the Job has the condition of the given type
// with status True.
func conditionTrue(job *batchv1.Job, typ batchv1.JobConditionType) bool {
	return slices.ContainsFunc(job.Status.Conditions, func(c batchv1.JobCondition) bool {
		return c.Type == typ && c.Status == corev1.ConditionTrue
	})
}

// ownPods returns the pods the Job's controller-uid label selects, newest
// first by creation time, then by name.
func ownPods(job *batchv1.Job, pods []corev1.Pod) []corev1.Pod {
	var own []corev1.Pod
	for _, p := range pods {
		if p.Labels[batchv1.ControllerUidLabel] == string(job.UID) {
			own = append(own, p)
		}
	}
	slices.SortStableFunc(own, func(a, b corev1.Pod) int {
		if c := b.CreationTimestamp.Compare(a.CreationTimestamp.Time); c != 0 {
			return c
		}
		return cmp.Compare(b.Name, a.Name)
	})
	return own
}

// failure builds the FailureError of a failed Job from its pods, newest
// first.
func failure(job *batchv1.Job, pods []corev1.Pod) *FailureError {
	f := &FailureError{}
	for _, c := range job.Status.Conditions {
		if c.Type == batchv1.JobFailed && c.Status == corev1.ConditionTrue {
			f.Condition = c
		}
	}
	for _, p := range pods {
		statuses := slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses)
		for _, s := range statuses {
			if t := s.State.Terminated; t != nil && t.ExitCode != exitSuccess {
				code := t.ExitCode
				f.ExitCode, f.Container, f.Message = &code, s.Name, t.Message
				return f
			}
		}
	}
	return f
}

// newestWaiting returns the waiting reason of the newest pod: its first
// waiting init container, else its first waiting container, else its
// PodScheduled=False condition. It returns nil when that pod waits for
// nothing, or when there is no pod.
func newestWaiting(pods []corev1.Pod) *Waiting {
	if len(pods) == 0 {
		return nil
	}
	p := pods[0]
	for _, s := range slices.Concat(p.Status.InitContainerStatuses, p.Status.ContainerStatuses) {
		if w := s.State.Waiting; w != nil {
			return &Waiting{Container: s.Name, Reason: w.Reason, Message: w.Message}
		}
	}
	for _, c := range p.Status.Conditions {
		if c.Type == corev1.PodScheduled && c.Status == corev1.ConditionFalse {
			return &Waiting{Reason: c.Reason, Message: c.Message}
		}
	}
	return nil
}

// ExitMeaning says what one of restic's exit codes means, for a person.
//
// Parameters:
//   - code is the exit code of a restic container.
//
// It returns restic's own description of the code, and for a code restic
// does not document, that it is unknown and counts as a failure.
func ExitMeaning(code int32) string {
	switch code {
	case exitSuccess:
		return "success"
	case exitFailure:
		return "failure"
	case exitGoRuntime:
		return "Go runtime error"
	case exitSourceData:
		return "some source data could not be read"
	case exitNoRepository:
		return "no repository"
	case exitLocked:
		return "the repository is locked"
	case exitWrongPassword:
		return "wrong password"
	case exitInterrupted:
		return "interrupted"
	case exitKilled:
		return "killed with SIGKILL, such as out of memory or at the end of its grace period"
	default:
		return "unknown exit code, counted as a failure"
	}
}

// AwaitsResume reports whether a restore Job is still suspended since Build
// created it, so its creator has to resume it before any pod runs.
//
// Parameters:
//   - job is the Job as the caller just read it.
//
// It returns true for a Job with spec.suspend true that is neither Complete
// nor Failed and not being deleted. A Job that Stop suspended also has
// spec.suspend true, and the answer alone does not tell the two apart. So
// the caller resumes a Job only while the stored record of its creator,
// read fresh from the API server, still restores with that Job and has not
// decided to end.
func AwaitsResume(job *batchv1.Job) bool {
	return ptr.Deref(job.Spec.Suspend, false) && !finished(job) && job.DeletionTimestamp == nil
}
