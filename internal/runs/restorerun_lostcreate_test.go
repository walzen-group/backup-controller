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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	t.Parallel()
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

// createLandsLater returns a client over c whose restore Job creates answer
// 504 Timeout without storing the Job, and a function that stores the last
// such Job later. The API server behaves that way: when a request outlives
// its deadline it answers with a timeout and leaves the handler running,
// which may still store the object (k8s.io/apiserver
// pkg/endpoints/handlers/finisher/finisher.go).
func createLandsLater(t *testing.T, c client.Client) (client.Client, func() *batchv1.Job) {
	var pending *batchv1.Job
	timingOut := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if job, ok := obj.(*batchv1.Job); ok {
				pending = job.DeepCopy()
				return apierrors.NewTimeoutError("request did not complete within requested timeout", 0)
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	land := func() *batchv1.Job {
		t.Helper()
		if pending == nil {
			t.Fatal("no restore Job create was sent, want one that timed out")
		}
		pending.ResourceVersion = ""
		if err := c.Create(context.Background(), pending); err != nil {
			t.Fatal(err)
		}
		return &restoreJobs(t, c)[0]
	}
	return timingOut, land
}

// A restore Job whose create answered 504 Timeout and was stored only after
// the pass that timed the run out is still stopped before the app comes
// back. The item names no Job, because the create reported an error, and
// the timeout failed it with reason TimedOut. The restart fails once, so the
// run is still finishing when the Job lands. The next pass records the Job
// on the failed item, suspends it and keeps the app down while its pod runs
// (rule X2), and gives the app back once the pod has ended.
func TestAJobCreateThatLandsAfterATimeoutIsStopped(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, nil, quiescedRestore(), claim(), volumeRestore(), repository(),
		deployment(), kustomization(false))
	restoreStep(t, r) // plan
	restoreStep(t, r) // quiesce
	req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}

	var land func() *batchv1.Job
	r.Client, land = createLandsLater(t, c)
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("the pass whose Job create timed out succeeded, want the timeout returned")
	}
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	r.Client = refuseDeploymentScales(c)
	if _, err := r.Reconcile(context.Background(), req); err == nil {
		t.Fatal("the pass whose restart was refused succeeded, want the refusal returned")
	}
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut || item.Job != "" {
		t.Fatalf("item = %+v, want Failed with reason TimedOut and no Job", item)
	}
	r.Client = c

	job := land()
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	createPod(t, c, pod)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 0 || !suspendedJob(t, c, job.Name) {
		t.Fatalf("replicas = %d, Job suspended = %t while pod %s of the run's own Job runs; want the app down and the Job stopped",
			got, suspendedJob(t, c, job.Name), pod.Name)
	}
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut || item.JobUID != job.UID {
		t.Errorf("item = %+v, want Failed with reason TimedOut, naming the late Job's UID %s", item, job.UID)
	}

	markSuspended(t, c, job)
	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the pod had ended, want the 2 the app had", got)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want the late Job deleted", jobs)
	}
}
