package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The tests in this file check that a restore stops a mover the way rule X2
// (rework-plan.md) requires: the restore Job, in place or into a new claim,
// is suspended first; its pods are waited for; and only then does the run
// give the app back, release the Leases or continue.

// intoClaim returns the plain claim an into restore creates for spec.into,
// controlled by the run.
func intoClaim(run *backupv1alpha1.RestoreRun) *corev1.PersistentVolumeClaim {
	class := "zfs"
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: run.Spec.Into, Namespace: ns, UID: "into-claim-uid",
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind("RestoreRun"))},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimPending},
	}
}

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

// heldClaimLease returns the claim Lease the run holds for the item named
// item, as acquireLeases writes it.
func heldClaimLease(run *backupv1alpha1.RestoreRun, item string) *coordinationv1.Lease {
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: claimLeaseName("claim-uid"), Namespace: ns}}
	stamp(lease, leaseHolder{kind: "RestoreRun", run: run, item: item}, []string{item})
	return lease
}

// restoringOnJob returns the RestoreRun back-to-monday in the middle of an
// in-place restore of the claim, not quiesced, with its item Running and
// naming its restore Job, and that Job, which restores monday's snapshot.
func restoringOnJob(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job) {
	t.Helper()
	started := metav1.NewTime(frozen)
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = &started
		r.Status.Items = []backupv1alpha1.RestoreItem{runningOnJob(claimN, 0)}
	})
	return run, restoreJobFor(t, run, claimN, monday.ID)
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

// A run deleted in the middle of a restore stops its restore Job, waits for
// that Job's pod, and only then releases its Leases and gives the app back.
// It keeps its finalizer until this is done.
func TestADeletedRestoreWaitsForItsStoppedMoversPod(t *testing.T) {
	run, job := quiescedMidRestore(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod, lease)
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r) // suspends the Job
	markSuspended(t, c, job)
	restoreStep(t, r) // the gate finds the pod

	if !suspendedJob(t, c, job.Name) {
		t.Error("the restore Job is not suspended, want it stopped")
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while pod %s was still there, want the app still down", got, pod.Name)
	}
	deleting := readRestoreRun(t, c)
	if len(deleting.Finalizers) == 0 {
		t.Error("the run dropped its finalizer while the pod was still there")
	}
	if !strings.Contains(readyMessage(deleting.Status.Conditions), pod.Name) {
		t.Errorf("ready message = %q, want it to name pod %s", readyMessage(deleting.Status.Conditions), pod.Name)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while the pod was still there", err)
	}

	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the pod had ended, want the 2 the app had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization stayed suspended after the pod had ended")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Errorf("get the claim Lease = %v, want it released once the pod had ended", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("RestoreRun = %v, want it gone once its finalizer was dropped", err)
	}
}

// quiescedRestoreDone returns the run from quiescedMidRestore with its volume
// item Succeeded in an earlier pass, whose status write recorded the end
// before the run stopped the restore Job, and that Job, which is Complete.
func quiescedRestoreDone(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job) {
	t.Helper()
	run, job := quiescedMidRestore(t)
	run.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	return run, job
}

// A run whose finished item's stopped restore Job still has a pod that may
// write does not give the app back, however far the rest of the run has
// come: that pod may still write into the claim the app would mount (rule
// X2).
func TestARestoreDoesNotGiveTheAppBackWhileAFinishedItemsMoverIsThere(t *testing.T) {
	run, job := quiescedRestoreDone(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod)

	restoreStep(t, r)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d while pod %s is there, want the app still down", got, pod.Name)
	}
	if !suspended(t, c) {
		t.Error("the Kustomization was resumed while the pod was there")
	}
	waiting := readRestoreRun(t, c)
	if readyReason(waiting.Status.Conditions) != backupv1alpha1.ReasonShutdown ||
		!strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Errorf("reason = %q, message = %q; want %s naming pod %s", readyReason(waiting.Status.Conditions),
			readyMessage(waiting.Status.Conditions), backupv1alpha1.ReasonShutdown, pod.Name)
	}
}

// advance moves the reconciler's clock forward by d from wherever it is now.
func advance(r *RestoreRunReconciler, d time.Duration) {
	now := r.Now
	r.Now = func() time.Time { return now().Add(d) }
}

// A restore whose items have all finished only waits for its stopped
// restore Jobs before it gives the app back, and a deadline that passes
// during that wait does not end it TimedOut: it ends as its items say. Here
// the item Succeeded before the deadline, and the run still waits for a pod
// of the Job when the deadline passes. Before, the pass after the deadline
// timed the run out, and it ended Failed although it restored everything.
func TestAFinishedRestoreEndsAsItsItemsSayWhenTheDeadlinePassesInItsMoverWait(t *testing.T) {
	run, job := quiescedRestoreDone(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod)
	deadline := frozen.Add(4 * time.Hour)
	r.Now = func() time.Time { return deadline.Add(-30 * time.Second) }

	restoreStep(t, r)
	restoreStep(t, r)
	if reason := readyReason(readRestoreRun(t, c).Status.Conditions); reason != backupv1alpha1.ReasonShutdown {
		t.Fatalf("reason = %q before the deadline, want %s", reason, backupv1alpha1.ReasonShutdown)
	}
	r.Now = func() time.Time { return deadline.Add(time.Minute) }
	restoreStep(t, r)
	setPodPhase(t, c, pod, corev1.PodSucceeded)
	restoreStep(t, r)
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseSucceeded || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonSucceeded {
		t.Errorf("phase = %q, reason = %q, message = %q; want Succeeded, %s", done.Status.Phase, readyReason(done.Status.Conditions),
			readyMessage(done.Status.Conditions), backupv1alpha1.ReasonSucceeded)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the pod had ended, want the 2 the app had", got)
	}
}

// endsAfterFirstRead returns a client over c on which the restore Job named
// name ends Failed right after the first read of it returns it running, as
// a Job whose last pod failed between followJob's read and settleJobs'.
func endsAfterFirstRead(t *testing.T, c client.Client, name string) client.WithWatch {
	read := false
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			err := cl.Get(ctx, key, obj, opts...)
			if _, ok := obj.(*batchv1.Job); ok && err == nil && !read && key.Name == name {
				read = true
				endJob(t, c, name, batchv1.JobFailed, "BackoffLimitExceeded", "Job has reached the specified backoff limit")
			}
			return err
		},
	})
}

// A timeout whose stop of the restore Job completes in the same pass keeps
// what it decided when that pass's final status write is lost: finish
// stores the ending and the items' ends before it stops the Job. The next
// pass ends the run TimedOut from the stored ending, and the item keeps the
// end the timeout pass gave it: TimedOut in place, where the timeout reads
// the Job first while it still runs, and the Job's own Failed into a new
// claim, where the item's first read saw it running and the timeout's saw
// it Failed. Before, the lost write left the item Running on a Job that was
// gone, and the next pass failed it as a Job deleted before it finished.
func TestATimeoutWhoseStopEndsInOnePassKeepsItsEnding(t *testing.T) {
	for _, tc := range []struct {
		name   string
		shape  func(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job, []client.Object)
		reason backupv1alpha1.ItemReason
	}{
		{"in place", func(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job, []client.Object) {
			run, job := restoringOnJob(t)
			return run, job, []client.Object{claim(), volumeRestore(), repository()}
		}, backupv1alpha1.ItemReasonTimedOut},
		{"into", func(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job, []client.Object) {
			run, job := intoOnJob(t)
			return run, job, []client.Object{intoClaim(run), sourceOnNode(), volumeRestore(), repository()}
		}, backupv1alpha1.ItemReasonRestoreJobFailed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run, job, objects := tc.shape(t)
			r, c := restoreReconciler(t, nil, append([]client.Object{run, job}, objects...)...)
			r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
			ends := endsAfterFirstRead(t, c, job.Name)
			r.Client, r.Reader = loseFailedRunWrite(ends), ends
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
				t.Fatal("the pass whose final write was lost succeeded, want the error returned")
			}
			if jobs := restoreJobs(t, c); len(jobs) != 0 {
				t.Fatalf("restore Jobs = %d after the lost write, want the stop completed in that pass", len(jobs))
			}

			r.Client, r.Reader = c, c
			done := stepUntilFinished(t, r, c, 3)
			if item := done.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != tc.reason {
				t.Errorf("item = %+v, want Failed with reason %s", item, tc.reason)
			}
			if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
				t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
			}
		})
	}
}
