package restorejob_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"github.com/walzen-group/backup-controller/internal/restorejob"
)

var podTime = time.Date(2026, 9, 26, 10, 0, 0, 0, time.UTC)

// withCondition returns job with a condition of the type given set True.
func withCondition(job *batchv1.Job, typ batchv1.JobConditionType, reason, message string) *batchv1.Job {
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{
		Type: typ, Status: corev1.ConditionTrue, Reason: reason, Message: message,
	})
	return job
}

// jobPod returns a pod of the Job whose UID is given, labelled and owned the
// way the Job controller does it, created the given number of seconds after
// podTime.
func jobPod(name string, jobUID types.UID, second int) corev1.Pod {
	return corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "app",
		CreationTimestamp: metav1.NewTime(podTime.Add(time.Duration(second) * time.Second)),
		Labels:            map[string]string{batchv1.ControllerUidLabel: string(jobUID), batchv1.JobNameLabel: "restore-3a7c-0"},
	}}
}

func terminated(name string, code int32, message string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: code, Message: message},
	}}
}

func waiting(name, reason, message string) corev1.ContainerStatus {
	return corev1.ContainerStatus{Name: name, State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: reason, Message: message},
	}}
}

func readJob(t *testing.T) *batchv1.Job {
	t.Helper()
	job := build(t, runSpec())
	job.UID = "job-uid"
	return job
}

func TestReadSucceedsOnlyOnComplete(t *testing.T) {
	job := readJob(t)
	if got := restorejob.Read(job, nil); got.State != restorejob.Running {
		t.Errorf("a Job without a terminal condition reads %v, want Running", got.State)
	}
	withCondition(job, batchv1.JobSuccessCriteriaMet, "CompletionsReached", "")
	if got := restorejob.Read(job, nil); got.State != restorejob.Running {
		t.Errorf("SuccessCriteriaMet alone reads %v, want Running until Complete", got.State)
	}
	withCondition(job, batchv1.JobComplete, "CompletionsReached", "")
	got := restorejob.Read(job, nil)
	if got.State != restorejob.Succeeded || got.Failure != nil || got.Waiting != nil {
		t.Errorf("Read = %+v, want Succeeded alone", got)
	}
	job = readJob(t)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionFalse}}
	if got := restorejob.Read(job, nil); got.State != restorejob.Running {
		t.Errorf("Complete=False reads %v, want Running", got.State)
	}
}

func TestReadTakesExitCodeFromContainerStatuses(t *testing.T) {
	t.Run("main container", func(t *testing.T) {
		job := withCondition(readJob(t), batchv1.JobFailed, "PodFailurePolicy", "Container restore for pod app/p2 failed with exit code 12 matching FailJob rule at index 0")
		older := jobPod("p1", job.UID, 0)
		older.Status.ContainerStatuses = []corev1.ContainerStatus{terminated("restore", 1, "Fatal: older")}
		newer := jobPod("p2", job.UID, 60)
		newer.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated("unlock", 0, "")}
		newer.Status.ContainerStatuses = []corev1.ContainerStatus{terminated("restore", 12, "Fatal: wrong password or no key found")}
		got := restorejob.Read(job, []corev1.Pod{newer, older})
		if got.State != restorejob.Failed || got.Failure == nil {
			t.Fatalf("Read = %+v, want Failed with a failure", got)
		}
		f := got.Failure
		if f.ExitCode == nil || *f.ExitCode != 12 || f.Container != "restore" || f.Message != "Fatal: wrong password or no key found" ||
			f.Condition.Reason != "PodFailurePolicy" {
			t.Errorf("failure = %+v, want exit 12 in restore from the newest pod", f)
		}
		msg := f.Error()
		for _, part := range []string{"PodFailurePolicy", "restic exited 12 (wrong password)", "restore", "Fatal: wrong password"} {
			if !strings.Contains(msg, part) {
				t.Errorf("Error() = %q, want it to name %q", msg, part)
			}
		}
		var fe *restorejob.FailureError
		if !errors.As(error(f), &fe) {
			t.Error("FailureError is not an error")
		}
	})
	t.Run("init container", func(t *testing.T) {
		job := withCondition(readJob(t), batchv1.JobFailed, "PodFailurePolicy", "")
		pod := jobPod("p1", job.UID, 0)
		pod.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated("unlock", 10, "Fatal: repository does not exist")}
		pod.Status.ContainerStatuses = []corev1.ContainerStatus{waiting("restore", "PodInitializing", "")}
		f := restorejob.Read(job, []corev1.Pod{pod}).Failure
		if f == nil || f.ExitCode == nil || *f.ExitCode != 10 || f.Container != "unlock" || f.Message != "Fatal: repository does not exist" {
			t.Fatalf("failure = %+v, want exit 10 in unlock", f)
		}
		if !strings.Contains(f.Error(), "restic exited 10 (no repository)") {
			t.Errorf("Error() = %q", f.Error())
		}
	})
	t.Run("no pod left", func(t *testing.T) {
		job := withCondition(readJob(t), batchv1.JobFailed, "BackoffLimitExceeded", "Job has reached the specified backoff limit")
		f := restorejob.Read(job, nil).Failure
		if f == nil || f.ExitCode != nil {
			t.Fatalf("failure = %+v, want one without an exit code", f)
		}
		msg := f.Error()
		if !strings.Contains(msg, "BackoffLimitExceeded") || !strings.Contains(msg, "Job has reached the specified backoff limit") ||
			!strings.Contains(msg, "no pod") {
			t.Errorf("Error() = %q, want the condition's reason and message and that no pod shows the exit", msg)
		}
	})
}

func TestReadShowsTheWaitingReasonWhileTheJobRuns(t *testing.T) {
	job := readJob(t)
	for _, tc := range []struct {
		name string
		pod  func() corev1.Pod
		want restorejob.Waiting
	}{
		{"init container", func() corev1.Pod {
			p := jobPod("p", job.UID, 0)
			p.Status.InitContainerStatuses = []corev1.ContainerStatus{waiting("unlock", "ImagePullBackOff", "Back-off pulling image")}
			p.Status.ContainerStatuses = []corev1.ContainerStatus{waiting("restore", "PodInitializing", "")}
			return p
		}, restorejob.Waiting{Container: "unlock", Reason: "ImagePullBackOff", Message: "Back-off pulling image"}},
		{"main container", func() corev1.Pod {
			p := jobPod("p", job.UID, 0)
			p.Status.InitContainerStatuses = []corev1.ContainerStatus{terminated("unlock", 0, "")}
			p.Status.ContainerStatuses = []corev1.ContainerStatus{waiting("restore", "CreateContainerConfigError", `couldn't find key RESTIC_PASSWORD in Secret app/restic-data`)}
			return p
		}, restorejob.Waiting{Container: "restore", Reason: "CreateContainerConfigError", Message: `couldn't find key RESTIC_PASSWORD in Secret app/restic-data`}},
		{"unscheduled", func() corev1.Pod {
			p := jobPod("p", job.UID, 0)
			p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionFalse,
				Reason: "SchedulingGated", Message: "Scheduling is blocked due to non-empty scheduling gates"}}
			return p
		}, restorejob.Waiting{Reason: "SchedulingGated", Message: "Scheduling is blocked due to non-empty scheduling gates"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			running := jobPod("old", job.UID, -60)
			got := restorejob.Read(job, []corev1.Pod{running, tc.pod()})
			if got.State != restorejob.Running || got.Waiting == nil || *got.Waiting != tc.want {
				t.Fatalf("Read = %+v (waiting %+v), want Running and %+v", got, got.Waiting, tc.want)
			}
			if got.Waiting.String() == "" {
				t.Error("Waiting renders to nothing")
			}
		})
	}
	t.Run("running pod", func(t *testing.T) {
		p := jobPod("p", job.UID, 0)
		p.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "restore", State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}}}}
		if got := restorejob.Read(job, []corev1.Pod{p}); got.Waiting != nil {
			t.Errorf("waiting = %+v for a running pod, want none", got.Waiting)
		}
	})
	t.Run("terminal Job", func(t *testing.T) {
		p := jobPod("p", job.UID, 0)
		p.Status.ContainerStatuses = []corev1.ContainerStatus{waiting("restore", "ImagePullBackOff", "")}
		for _, typ := range []batchv1.JobConditionType{batchv1.JobComplete, batchv1.JobFailed} {
			got := restorejob.Read(withCondition(readJob(t), typ, "", ""), []corev1.Pod{p})
			if got.Waiting != nil {
				t.Errorf("%s Job: waiting = %+v, want none", typ, got.Waiting)
			}
		}
	})
}

func TestExitMeaning(t *testing.T) {
	for code, want := range map[int32]string{
		0: "success", 1: "failure", 3: "some source data could not be read", 10: "no repository",
		11: "the repository is locked", 12: "wrong password", 130: "interrupted",
	} {
		if got := restorejob.ExitMeaning(code); !strings.Contains(got, want) {
			t.Errorf("ExitMeaning(%d) = %q, want it to say %q", code, got, want)
		}
	}
	if got := restorejob.ExitMeaning(42); !strings.Contains(got, "unknown") || !strings.Contains(got, "failure") {
		t.Errorf("ExitMeaning(42) = %q, want an unknown code treated as failure", got)
	}
}
