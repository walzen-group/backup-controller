package runs

import (
	"context"
	"errors"
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
	"k8s.io/utils/ptr"
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

// An into restore whose claim is gone records the item's end, with reason
// ClaimLost, before finish stops its restore Job. A pass that loses that
// write leaves the Job as it was, and the pass after it stops the Job, so a
// restore into a claim that is gone never counts as done.
func TestAnIntoRestoreWhoseClaimIsLostRecordsTheEndBeforeTheMoverGoes(t *testing.T) {
	run, job := intoOnJob(t)
	claim := intoClaim(run)
	r, c := restoreReconciler(t, nil, run, claim, sourceOnNode(), volumeRestore(), repository(), job)
	if err := c.Delete(context.Background(), claim); err != nil {
		t.Fatal(err)
	}

	r.Client = loseNextStatusWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose status write was lost succeeded, want the error returned")
	}
	if suspendedJob(t, c, job.Name) {
		t.Fatalf("Job %s suspended after the lost write, want it left as it was until the end is recorded", job.Name)
	}

	r.Client = c
	restoreStep(t, r)
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonClaimLost {
		t.Fatalf("item = %+v, want Failed with reason ClaimLost", item)
	}
	markSuspended(t, c, job)
	after := stepUntilFinished(t, r, c, 2)

	if after.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", after.Status.Phase)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want the run's deleted", jobs)
	}
}

// The same holds for an item whose restore Job failed: the end goes into the
// status before the Job is deleted, so a lost write leaves a record that
// stops the next pass from starting a restore again.
func TestAnIntoRestoreWhoseMoverFailedRecordsTheEndBeforeTheMoverGoes(t *testing.T) {
	run, job := intoOnJob(t)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), job)

	r.Client = loseNextStatusWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose status write was lost succeeded, want the error returned")
	}
	if jobs := restoreJobs(t, c); len(jobs) != 1 {
		t.Fatalf("restore Jobs = %v after the lost write, want %s still there", jobs, job.Name)
	}

	r.Client = c
	after := stepUntilFinished(t, r, c, 3)
	if item := after.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobFailed ||
		!strings.Contains(item.Message, "BackoffLimitExceeded") {
		t.Errorf("item = %+v, want it Failed with reason RestoreJobFailed and the Job's condition", item)
	}
	if after.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", after.Status.Phase)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want the run's deleted", jobs)
	}
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

// A restore Job that is still restoring has a pod that belongs there. The
// run does not stop it or wait for it: it would never finish the restore it
// started, and it goes on with the pass.
func TestARunningMoversPodDoesNotHoldTheRestore(t *testing.T) {
	run, job := restoringOnJob(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), job, pod)

	restoreStep(t, r)

	after := readRestoreRun(t, c)
	if readyReason(after.Status.Conditions) != backupv1alpha1.ReasonRunning || after.Status.Items[0].Phase != backupv1alpha1.ItemRunning {
		t.Errorf("reason = %q, message = %q, item = %+v; want the run still restoring", readyReason(after.Status.Conditions),
			readyMessage(after.Status.Conditions), after.Status.Items[0])
	}
	if suspendedJob(t, c, job.Name) {
		t.Error("the run suspended the restore Job that was still restoring")
	}
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

// loseShutdownWrite returns a client over c that fails, with a conflict,
// the first status write of a RestoreRun whose Ready reason is
// WaitingForShutdown: the write of the wait for a stopped mover, which
// comes after finish stored the ending.
func loseShutdownWrite(c client.Client) client.Client {
	lost := false
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if run, ok := obj.(*backupv1alpha1.RestoreRun); ok && !lost && readyReason(run.Status.Conditions) == backupv1alpha1.ReasonShutdown {
				lost = true
				return apierrors.NewConflict(backupv1alpha1.GroupVersion.WithResource("restoreruns").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

// A wait for a stopped Job's pod whose status write is lost is reported
// again on the next pass. The run holds what it must meanwhile: the pod, its
// finalizer and the Leases.
func TestALostStatusWriteInTheMoverWaitConverges(t *testing.T) {
	run, job := quiescedMidRestore(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod, lease)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	r.Client = loseShutdownWrite(c)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the wait pass succeeded, want its lost status write returned")
	}
	if !suspendedJob(t, c, job.Name) {
		t.Error("the restore Job is not suspended, want it stopped")
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d, want the app still down while the pod is there", got)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while the pod is there", err)
	}

	r.Client = c
	markSuspended(t, c, job)
	restoreStep(t, r)

	waiting := readRestoreRun(t, c)
	if len(waiting.Finalizers) == 0 {
		t.Error("the run dropped its finalizer while the pod was still there")
	}
	if readyReason(waiting.Status.Conditions) != backupv1alpha1.ReasonShutdown ||
		!strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Errorf("reason = %q, message = %q; want the wait reported again, naming pod %s",
			readyReason(waiting.Status.Conditions), readyMessage(waiting.Status.Conditions), pod.Name)
	}
}

// A stopped restore Job without a pod holds the run until the Job controller
// reports it suspended. Until then it may be about to start a pod: the
// Job controller writes the condition only after a sync that saw the
// suspend. The app comes back only once a read shows the Job suspended.
func TestAStoppedMoversJobWithoutAPodHoldsTheRestore(t *testing.T) {
	run, job := quiescedMidRestore(t)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStep(t, r)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while Job %s was not reported suspended, want the app still down", got, job.Name)
	}
	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() || !strings.Contains(readyMessage(waiting.Status.Conditions), job.Name) {
		t.Errorf("phase = %q, message = %q; want the run waiting and naming Job %s",
			waiting.Status.Phase, readyMessage(waiting.Status.Conditions), job.Name)
	}

	markSuspended(t, c, job)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the Job was reported suspended, want the 2 the app had", got)
	}
	if done := readRestoreRun(t, c); readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Errorf("reason = %q, want %s", readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
	}
}

// Which pods of a suspended restore Job hold the run: a pod that has
// Succeeded or Failed has no container left that writes, and a pod in any
// other phase on a node still does. A pod that was never scheduled holds the
// run too while it is not being deleted, since it could still be bound.
func TestWhichPodsOfAStoppedMoverHoldTheRestore(t *testing.T) {
	for _, tc := range []struct {
		phase       corev1.PodPhase
		unscheduled bool
		holds       bool
	}{
		{corev1.PodSucceeded, false, false},
		{corev1.PodFailed, false, false},
		{corev1.PodPending, false, true},
		{corev1.PodRunning, false, true},
		{corev1.PodUnknown, false, true},
		{corev1.PodPending, true, true},
	} {
		name := string(tc.phase) + " pod"
		if tc.unscheduled {
			name = "unscheduled " + name
		}
		t.Run(name, func(t *testing.T) {
			run, job := quiescedMidRestore(t)
			job.Spec.Suspend = ptr.To(true)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuspended, Status: corev1.ConditionTrue}}
			pod := jobPodOf(job, "restore-pod", tc.phase)
			if tc.unscheduled {
				pod.Spec.NodeName = ""
			}
			r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true),
				job, pod)
			r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

			restoreStep(t, r)
			restoreStep(t, r)

			if back := replicasOf(t, c) == 2; back == tc.holds {
				t.Errorf("app back = %t, want %t", back, !tc.holds)
			}
		})
	}
}

// The pass that suspends a restore Job does not go on to give the app back,
// even when it finds no pod. The Job controller may be in the middle of a
// sync that creates a pod, and only a later read that shows the Job
// suspended lets the run go on.
func TestThePassThatStopsAMoverDoesNotGiveTheAppBack(t *testing.T) {
	run, job := quiescedMidRestore(t)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	result := restoreStep(t, r)

	if !suspendedJob(t, c, job.Name) {
		t.Fatal("the restore Job is not suspended at the timeout")
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d in the pass that suspended the Job, want the app still down", got)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("result = %+v, want a requeue to look at the Job again", result)
	}

	markSuspended(t, c, job)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d on the pass after, want the 2 the app had", got)
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

// A finished item whose stopped restore Job still has a pod that may write
// keeps its Lease. work releases the Leases of finished items at the start
// of each pass, so a backup of the claim need not wait for the rest of the
// run, but a Job that may still write keeps the claim and the repository to
// its run (rule X2).
func TestAFinishedItemKeepsItsLeaseWhileItsStoppedMoverIsThere(t *testing.T) {
	run, job := quiescedRestoreDone(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod, lease)

	restoreStep(t, r)
	restoreStep(t, r)

	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while pod %s is there", err, pod.Name)
	}
	if item := readRestoreRun(t, c).Status.Items[0]; item.JobUID != jobUID {
		t.Errorf("item = %+v, want it to keep naming the Job while its pod is there", item)
	}
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

// A run that times out while it waits for another run, and then waits for
// its own stopped restore Job, ends with the wait it timed out on in its
// Ready message. The pass that times out records that message in
// status.ending, in the write that fails the item with reason TimedOut. The
// Job wait replaces the SourceBusy condition and the item's message is
// edited before the last pass, and the run still ends with the ending as
// recorded.
func TestATimedOutRunKeepsTheWaitItTimedOutOn(t *testing.T) {
	run, job := quiescedMidRestore(t)
	wait := "BackupRun manual-notes holds Lease backup-controller-repo-secret-uid for notes-data; this run starts once that run has finished with it"
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonSourceBusy, wait)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStep(t, r)
	want := "the run had not finished by " + frozen.Add(4*time.Hour).Format(time.RFC3339) + "; it was waiting: " + wait
	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() {
		t.Fatal("the run finished with its restore Job's pod still there, want it waiting")
	}
	if got := waiting.Status.Ending; got == nil || *got != (backupv1alpha1.RunEnding{Reason: backupv1alpha1.ReasonTimedOut, Message: want}) {
		t.Fatalf("ending = %+v during the Job wait, want reason TimedOut and %q", got, want)
	}
	if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut || item.Message != want {
		t.Errorf("item = %+v, want it Failed with reason TimedOut and %q", item, want)
	}
	waiting.Status.Items[0].Message = "edited between the passes"
	if err := c.Status().Update(context.Background(), waiting); err != nil {
		t.Fatal(err)
	}
	markSuspended(t, c, job)
	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut ||
		readyMessage(done.Status.Conditions) != want {
		t.Errorf("phase = %q, reason = %q, message = %q; want Failed, %s, %q", done.Status.Phase, readyReason(done.Status.Conditions),
			readyMessage(done.Status.Conditions), backupv1alpha1.ReasonTimedOut, want)
	}
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

// The pass that decides to end a run records the ending, even when it then
// waits for the same stopped restore Job with the same message as the pass
// before. Here one item failed before the deadline and its Job's pod is
// still there, and the other waits for a pod that mounts its claim. The pass
// after the deadline fails the waiting item with reason TimedOut, and that
// item and the ending reach the stored status. Before, the wait wrote only a
// changed Ready condition, so both were lost.
func TestTheEndingIsStoredWhenTheMoverWaitIsUnchanged(t *testing.T) {
	const other = "notes-cache"
	run, job := quiescedMidRestore(t)
	run.Status.Items[0].Phase = backupv1alpha1.ItemFailed
	run.Status.Items = append(run.Status.Items, backupv1alpha1.RestoreItem{Kind: "PersistentVolumeClaim", Name: other,
		Phase: backupv1alpha1.ItemPending})
	mover := jobPodOf(job, "restore-pod", corev1.PodRunning)
	mounting := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-5d9f", Namespace: ns},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: other},
		}}}},
	}
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, mover, mounting)
	r.Now = func() time.Time { return frozen.Add(time.Hour) }

	restoreStep(t, r) // suspends the failed item's Job
	markSuspended(t, c, job)
	restoreStep(t, r) // finds the Job's pod still there
	before := readRestoreRun(t, c)
	if readyReason(before.Status.Conditions) != backupv1alpha1.ReasonShutdown || !strings.Contains(readyMessage(before.Status.Conditions), mover.Name) {
		t.Fatalf("reason = %q, message = %q before the deadline; want %s naming pod %s", readyReason(before.Status.Conditions),
			readyMessage(before.Status.Conditions), backupv1alpha1.ReasonShutdown, mover.Name)
	}
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r)

	stored := readRestoreRun(t, c)
	if stored.Status.Ending == nil || stored.Status.Ending.Reason != backupv1alpha1.ReasonTimedOut {
		t.Errorf("ending = %+v after the pass that timed out, want reason %s stored", stored.Status.Ending, backupv1alpha1.ReasonTimedOut)
	}
	if item := stored.Status.Items[1]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut {
		t.Errorf("item %s = %+v, want it stored Failed with reason TimedOut", other, item)
	}

	// A pass that finds the same wait and changes nothing writes nothing,
	// since every write starts another reconcile.
	restoreStep(t, r)
	if again := readRestoreRun(t, c); again.ResourceVersion != stored.ResourceVersion {
		t.Errorf("resourceVersion = %s after a pass that changed nothing, want %s: the status was written again",
			again.ResourceVersion, stored.ResourceVersion)
	}
}

// A pass of work that changes the items and then waits for the same stopped
// restore Job as the pass before stores those items in that pass. Here the
// first volume failed and its Job's pod is still there, the second volume's
// Job is still restoring, and the Cluster waits for both. Then the second Job
// fails: the pass that finds it stores that volume's end, skips the Cluster
// because a volume failed, and waits for the first Job's pod with the same
// message as before. Before, the wait wrote only a changed Ready condition,
// so the skipped Cluster was stored only by a later pass.
func TestWorkStoresItsItemsWhenTheMoverWaitIsUnchanged(t *testing.T) {
	const other = "notes-cache"
	run, first := quiescedMidRestore(t)
	run.Status.Items[0].Phase = backupv1alpha1.ItemFailed
	run.Status.Items = append(run.Status.Items, runningOnJob(other, 1),
		backupv1alpha1.RestoreItem{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemPending, BaseBackup: saturday.ID})
	run.Status.Items[1].JobUID = "second-job-uid"
	second := restoreJobFor(t, run, other, monday.ID)
	second.Name, second.UID = jobName(restoreUID, 1), "second-job-uid"
	mover := jobPodOf(first, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), cluster(),
		stoppedDeployment(), kustomization(true), first, second, mover)

	restoreStep(t, r) // suspends the failed volume's Job
	markSuspended(t, c, first)
	restoreStep(t, r) // finds that Job's pod still there
	before := readRestoreRun(t, c)
	if readyReason(before.Status.Conditions) != backupv1alpha1.ReasonShutdown || !strings.Contains(readyMessage(before.Status.Conditions), mover.Name) {
		t.Fatalf("reason = %q, message = %q; want %s naming pod %s", readyReason(before.Status.Conditions),
			readyMessage(before.Status.Conditions), backupv1alpha1.ReasonShutdown, mover.Name)
	}
	endJob(t, c, second.Name, batchv1.JobFailed, "BackoffLimitExceeded", "Job has reached the specified backoff limit")
	restoreStep(t, r)

	stored := readRestoreRun(t, c)
	if readyMessage(stored.Status.Conditions) != readyMessage(before.Status.Conditions) {
		t.Fatalf("message = %q, want the wait unchanged: %q", readyMessage(stored.Status.Conditions), readyMessage(before.Status.Conditions))
	}
	if item := stored.Status.Items[1]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobFailed {
		t.Errorf("item %s = %+v, want it stored Failed with reason RestoreJobFailed", other, item)
	}
	if item := stored.Status.Items[2]; item.Phase != backupv1alpha1.ItemSkipped {
		t.Errorf("item %s = %+v in the pass that skipped it, want it stored Skipped", pgN, item)
	}
}

// An into restore whose claim is deleted while its Job writes stops the Job,
// and keeps its Leases until that Job's pod has ended. The run ends Failed
// only once the pod has.
func TestAnIntoRestoreWhoseClaimIsLostWaitsForItsStoppedMoversPod(t *testing.T) {
	run, job := intoOnJob(t)
	claim := intoClaim(run)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	lease := heldClaimLease(run, run.Spec.Into)
	r, c := restoreReconciler(t, nil, run, claim, sourceOnNode(), volumeRestore(), repository(), job, pod, lease)
	if err := c.Delete(context.Background(), claim); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r)
	markSuspended(t, c, job)
	restoreStep(t, r)

	stopping := readRestoreRun(t, c)
	if !suspendedJob(t, c, job.Name) || stopping.Status.Phase.Finished() {
		t.Errorf("Job suspended = %t, phase = %q while its pod ran; want the Job stopped and the run unfinished", suspendedJob(t, c, job.Name), stopping.Status.Phase)
	}
	if item := stopping.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonClaimLost {
		t.Errorf("item = %+v, want it Failed with reason ClaimLost", item)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while the Job's pod ran", err)
	}

	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q once the pod had ended, want Failed", done.Status.Phase)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Errorf("get the claim Lease = %v, want it released once the pod had ended", err)
	}
}

// An into restore past its timeout stops its restore Job and waits for the
// Job's pod. The pass that finds the pod ended ends the run with reason
// TimedOut, the reason the timeout gave it, and not with the Failed of an
// item that failed on its own. It goes by the ending the timeout recorded,
// so an item message edited during the wait changes nothing.
func TestATimedOutIntoRestoreEndsTimedOutAfterItsMoverWait(t *testing.T) {
	run, job := intoOnJob(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), job, pod)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStep(t, r)

	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() || !suspendedJob(t, c, job.Name) {
		t.Fatalf("phase = %q, Job suspended = %t with the Job's pod running; want the run unfinished and the Job stopped",
			waiting.Status.Phase, suspendedJob(t, c, job.Name))
	}
	if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut {
		t.Errorf("item = %+v, want it Failed with reason TimedOut", item)
	}
	waiting.Status.Items[0].Message = "edited between the passes"
	if err := c.Status().Update(context.Background(), waiting); err != nil {
		t.Fatal(err)
	}

	markSuspended(t, c, job)
	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Errorf("phase = %q, reason = %q, message = %q; want Failed, %s",
			done.Status.Phase, readyReason(done.Status.Conditions), readyMessage(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
	}
}

// An into restore whose item failed on its own before the timeout keeps
// reason Failed when the Job's pod outlives the deadline. Here the claim is
// deleted while the Job's pod runs: the item fails with reason ClaimLost,
// the run suspends the Job and waits for the pod past the deadline, and
// ends Failed once the pod has ended. A Job with Failed=True has no pod
// left that runs (Kubernetes 1.31 and later add a terminal condition only
// after every pod has ended), so a failed Job never waits here.
func TestAnIntoRestoreWhoseItemFailedKeepsFailedPastTheTimeout(t *testing.T) {
	run, job := intoOnJob(t)
	claim := intoClaim(run)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim, sourceOnNode(), volumeRestore(), repository(), job, pod)
	if err := c.Delete(context.Background(), claim); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r)
	markSuspended(t, c, job)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r)
	if waiting := readRestoreRun(t, c); waiting.Status.Phase.Finished() {
		t.Fatal("the run finished with the Job's pod still running, want it waiting")
	}

	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonFailed)
	}
	if item := done.Status.Items[0]; item.Reason != backupv1alpha1.ItemReasonClaimLost {
		t.Errorf("item = %+v, want reason ClaimLost", item)
	}
}

// A pass whose copy of the run is older than the stored run, as a pass
// started by a pod event before the run's own last write reached the
// informer, writes nothing when the stored status already equals its own.
// The stored status is what counts: a write with the older resourceVersion
// would only fail with a conflict and back off.
func TestAnUnchangedStatusIsNotWrittenFromAnOlderCopy(t *testing.T) {
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN })
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	r, c := restoreReconciler(t, nil, run)
	older := readRestoreRun(t, c)
	stored := older.DeepCopy()
	stored.Labels = map[string]string{"touched": "true"}
	if err := c.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}

	if err := r.writeChangedStatus(context.Background(), older); err != nil {
		t.Fatalf("writeChangedStatus from an older copy with the stored status: %v, want nil", err)
	}
	if again := readRestoreRun(t, c); again.ResourceVersion != stored.ResourceVersion {
		t.Errorf("resourceVersion = %s, want %s: an unchanged status was written", again.ResourceVersion, stored.ResourceVersion)
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
