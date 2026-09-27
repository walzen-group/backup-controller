package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file check that a restore stops a mover the way rule X2
// (rework-plan.md) requires: the restore Job, in place or into a new claim,
// is suspended first; its pods are waited for; and only then does the run
// give the app back, release the Leases or continue.

// quiescedInPlace is a mutate function for restoreRun that makes it a
// namespace restore which stops the app's Deployment (see quiescedRestore).
func quiescedInPlace(r *backupv1alpha1.RestoreRun) {
	r.Spec.All = true
	r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
}

// stoppedDeployment returns the app's Deployment scaled to zero, as a run
// that quiesced it leaves it.
func stoppedDeployment() *appsv1.Deployment {
	d := deployment()
	zero := int32(0)
	d.Spec.Replicas = &zero
	return d
}

// runningOnJob returns a Running volume item for the claim named claimName
// at the given position in status.items, naming the restore Job the run
// created for it with UID jobUID and restoring monday's snapshot.
func runningOnJob(claimName string, index int) backupv1alpha1.RestoreItem {
	return backupv1alpha1.RestoreItem{Kind: backupv1alpha1.ItemKindClaim, Name: claimName, Phase: backupv1alpha1.ItemRunning,
		Snapshot: monday.ShortID(), SnapshotID: monday.ID, SnapshotTime: &metav1.Time{Time: monday.Time},
		Job: jobName(restoreUID, index), JobUID: jobUID}
}

// quiescedMidRestore returns the RestoreRun back-to-monday in the middle of a
// quiesced in-place restore of the claim: Running, holding its finalizer,
// with the app stopped (status.quiesced records the Deployment's 2 replicas
// and the Kustomization the run suspended), and one volume item Running that
// names the restore Job the run created for it, and that Job.
// stoppedDeployment and kustomization(true) are the app as the stopping pass
// left it.
func quiescedMidRestore(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job) {
	t.Helper()
	started := metav1.NewTime(frozen)
	run := restoreRun(quiescedInPlace, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = &started
		r.Status.QuiescedAt = &started
		r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
		r.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		r.Status.Items = []backupv1alpha1.RestoreItem{runningOnJob(claimN, 0)}
	})
	return run, restoreJobFor(t, run, claimN, monday.ID)
}

// markSuspended gives the stored Job the condition Suspended=True, as the
// Job controller does once it has seen spec.suspend and deleted the Job's
// active pods. The fake client runs no Job controller.
func markSuspended(t *testing.T, c client.Client, job *batchv1.Job) {
	t.Helper()
	endJob(t, c, job.Name, batchv1.JobSuspended, "JobSuspended", "Job suspended")
}

// suspendedJob reports whether the stored Job named name has spec.suspend
// set, and false when it is gone.
func suspendedJob(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	job := &batchv1.Job{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, job); err != nil {
		if apierrors.IsNotFound(err) {
			return false
		}
		t.Fatal(err)
	}
	return job.Spec.Suspend != nil && *job.Spec.Suspend
}

// A run past its timeout stops its restore Job before it gives the app back:
// it suspends the Job, and while the Job controller has not reported it
// suspended, or a pod of the Job has not ended, the run waits instead of
// restarting the app, however far past the timeout the clock has moved. Once
// the pod has ended the Job is deleted, the app comes back and the run ends
// TimedOut.
func TestATimedOutRestoreWaitsForItsStoppedMoversPod(t *testing.T) {
	t.Parallel()
	run, job := quiescedMidRestore(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod)

	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r) // suspends the Job
	if !suspendedJob(t, c, job.Name) {
		t.Fatal("the Job is not suspended after the timeout")
	}
	markSuspended(t, c, job)
	restoreStep(t, r) // the gate finds the pod

	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while pod %s was still there, want the app still down", got, pod.Name)
	}
	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() {
		t.Errorf("phase = %q with the pod still there, want the run unfinished", waiting.Status.Phase)
	}
	if readyReason(waiting.Status.Conditions) != backupv1alpha1.ReasonShutdown ||
		!strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Errorf("reason = %q, message = %q; want %s naming pod %s",
			readyReason(waiting.Status.Conditions), readyMessage(waiting.Status.Conditions), backupv1alpha1.ReasonShutdown, pod.Name)
	}
	if !suspended(t, c) {
		t.Error("the Kustomization was resumed while the pod was still there")
	}

	// The timeout ends the restore once; the pod bounds the wait.
	restoreStep(t, r)
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d a pass later with the pod still there, want the app still down", got)
	}

	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the pod had ended, want the 2 the app had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization stayed suspended after the pod had ended")
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want the stopped one deleted", jobs)
	}
	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
	}
}

// advance moves the reconciler's clock forward by d from wherever it is now.
func advance(r *RestoreRunReconciler, d time.Duration) {
	now := r.Now
	r.Now = func() time.Time { return now().Add(d) }
}
