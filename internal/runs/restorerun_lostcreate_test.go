package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file cover an in-place restore whose pass created the
// item's restore Job and lost the status write that recorded it (UFR1). The
// item stays Pending and names no Job, while the Job's pod mounts the claim.

// A quiesced in-place restore whose Job create went through and whose status
// write was lost stops that Job when it is deleted or times out. It suspends
// the Job, keeps the app down, its finalizer and the claim Lease while the
// Job's pod may still write, and gives the app back only once the pod has
// ended (rule X2). That holds when a pass ran between the lost write and the
// end, which took the Job over, and when the end came on the very next pass,
// which finds the Job under the item's name.
func TestALostJobCreateIsStoppedAtTheEnd(t *testing.T) {
	for name, tc := range map[string]struct {
		passBetween bool
		timeout     bool
	}{
		"deleted after a pass":   {passBetween: true},
		"timed out after a pass": {passBetween: true, timeout: true},
		"deleted on the next":    {},
		"timed out on the next":  {timeout: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, quiescedRestore(), claim(), volumeRestore(), repository(),
				deployment(), kustomization(false))
			restoreStep(t, r) // plan
			restoreStep(t, r) // quiesce

			r.Client = loseNextStatusWrite(c)
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
				t.Fatal("the pass whose status write was lost succeeded, want the error returned")
			}
			r.Client = c
			jobs := restoreJobs(t, c)
			if len(jobs) != 1 || jobs[0].Name != jobName(restoreUID, 0) {
				t.Fatalf("restore Jobs = %v after the lost write, want %s", jobs, jobName(restoreUID, 0))
			}
			job := &jobs[0]
			if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemPending || item.JobUID != "" {
				t.Fatalf("item = %+v after the lost write, want Pending with no Job", item)
			}
			pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
			createPod(t, c, pod)

			if tc.passBetween {
				restoreStep(t, r)
				between := readRestoreRun(t, c)
				if item := between.Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.JobUID != job.UID {
					t.Errorf("item = %+v, reason = %q; want Running naming %s, the Job the lost pass created",
						item, readyReason(between.Status.Conditions), job.Name)
				}
			}

			if tc.timeout {
				r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
			} else if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r) // suspends the Job
			if !suspendedJob(t, c, job.Name) {
				t.Fatalf("restore Job %s is not suspended, want the lost pass's stopped", job.Name)
			}
			markSuspended(t, c, job)
			restoreStep(t, r) // the gate finds the pod

			if got := replicasOf(t, c); got != 0 {
				t.Fatalf("replicas = %d while pod %s was still there, want the app still down", got, pod.Name)
			}
			waiting := readRestoreRun(t, c)
			if waiting.Status.Phase.Finished() || len(waiting.Finalizers) == 0 {
				t.Errorf("phase = %q, finalizers = %v with the pod still there; want the run unfinished and holding its finalizer",
					waiting.Status.Phase, waiting.Finalizers)
			}
			if !strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
				t.Errorf("ready message = %q, want it to name pod %s", readyMessage(waiting.Status.Conditions), pod.Name)
			}
			lease := types.NamespacedName{Namespace: ns, Name: claimLeaseName("claim-uid")}
			if err := c.Get(context.Background(), lease, &coordinationv1.Lease{}); err != nil {
				t.Errorf("get the claim Lease = %v, want it held while the pod was still there", err)
			}

			setPodPhase(t, c, pod, corev1.PodFailed)
			restoreStep(t, r)

			if got := replicasOf(t, c); got != 2 {
				t.Errorf("replicas = %d once the pod had ended, want the 2 the app had", got)
			}
			if err := c.Get(context.Background(), lease, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
				t.Errorf("get the claim Lease = %v, want it released once the pod had ended", err)
			}
			if tc.timeout {
				done := readRestoreRun(t, c)
				if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
					t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
				}
				return
			}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
				t.Errorf("RestoreRun = %v, want it gone once its finalizer was dropped", err)
			}
		})
	}
}

// A run that times out before any pass took over the restore Job of a lost
// create records how that Job ended: the timeout takes the Job over under
// the item's name before it fails what is left (P11). A Job that completed
// records the item Succeeded, and one that failed records restic's exit
// code; the run still ends TimedOut.
func TestATimeoutRecordsTheJobOfALostCreate(t *testing.T) {
	for name, tc := range map[string]struct {
		condition batchv1.JobConditionType
		phase     backupv1alpha1.ItemPhase
		reason    backupv1alpha1.ItemReason
	}{
		"complete": {batchv1.JobComplete, backupv1alpha1.ItemSucceeded, ""},
		"failed":   {batchv1.JobFailed, backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonRestoreJobFailed},
	} {
		t.Run(name, func(t *testing.T) {
			run := checkedRestore(inPlace)
			lost := restoreJobFor(t, run, claimN, monday.ID)
			lost.Status.Conditions = []batchv1.JobCondition{{Type: tc.condition, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
			r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), lost, heldClaimLease(run, claimN))
			r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
			restoreStep(t, r)
			restoreStep(t, r)

			done := readRestoreRun(t, c)
			if item := done.Status.Items[0]; item.Phase != tc.phase || item.Reason != tc.reason {
				t.Errorf("item = %+v, want %s with reason %q from the lost create's Job", item, tc.phase, tc.reason)
			}
			if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
				t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
			}
		})
	}
}

// orphanedLostJob returns the restore Job a pass created for the run's
// first item and lost the status write of, after someone deleted the run
// with --cascade=orphan: the garbage collector has removed the Job's
// controller reference while the run's finalizer holds the run. The Job
// keeps its name and its restore-run label.
func orphanedLostJob(t *testing.T, run *backupv1alpha1.RestoreRun, claimName string) *batchv1.Job {
	t.Helper()
	job := restoreJobFor(t, run, claimName, monday.ID)
	job.OwnerReferences = nil
	return job
}

// A run deleted with --cascade=orphan after a pass created a restore Job and
// lost the status write that recorded it still stops that Job: the Job's
// name and its restore-run label bind it to the run, although the garbage
// collector removed its controller reference. The run suspends the Job and
// keeps its finalizer, and a quiesced run keeps the app down, until the
// Job's pod has ended. That holds for an in-place and an into restore.
func TestARunDeletedWithOrphanStopsTheJobOfALostCreate(t *testing.T) {
	quiesced := checkedRestore(quiescedInPlace, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.QuiescedAt = atFrozen(0)
		r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
		r.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
	})
	into := plannedInto()
	for name, tc := range map[string]struct {
		run     *backupv1alpha1.RestoreRun
		claim   string
		objects []client.Object
	}{
		"in place": {quiesced, claimN, []client.Object{claim(), stoppedDeployment(), kustomization(true)}},
		"into":     {into, into.Spec.Into, []client.Object{intoClaim(into), sourceOnNode()}},
	} {
		t.Run(name, func(t *testing.T) {
			job := orphanedLostJob(t, tc.run, tc.claim)
			pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
			r, c := restoreReconciler(t, nil, append(tc.objects, tc.run, volumeRestore(), repository(), job, pod)...)
			if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r)
			markSuspended(t, c, job)
			restoreStep(t, r)

			waiting := readRestoreRun(t, c)
			if !suspendedJob(t, c, job.Name) || len(waiting.Finalizers) == 0 || !strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
				t.Fatalf("Job suspended = %t, finalizers = %v, message = %q; want the Job stopped and the run waiting for pod %s",
					suspendedJob(t, c, job.Name), waiting.Finalizers, readyMessage(waiting.Status.Conditions), pod.Name)
			}
			if tc.run.Spec.Into == "" && replicasOf(t, c) != 0 {
				t.Errorf("replicas = %d while pod %s runs, want the app still down", replicasOf(t, c), pod.Name)
			}

			setPodPhase(t, c, pod, corev1.PodFailed)
			restoreStep(t, r)
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
				t.Errorf("RestoreRun = %v, want it gone once the pod had ended", err)
			}
			if jobs := restoreJobs(t, c); len(jobs) != 0 {
				t.Errorf("restore Jobs = %v, want the stopped Job deleted", jobs)
			}
		})
	}
}
