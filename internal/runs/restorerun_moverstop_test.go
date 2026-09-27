package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	internalvolsync "github.com/walzen-group/backup-controller/internal/volsync"
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
)

// The tests in this file check that a restore stops a mover the way rule X2
// (rework-plan.md) requires: its ReplicationDestination goes first, its pod
// is waited for, and only then does the run give the app back, release the
// Leases or continue.

// intoRestoring returns the RestoreRun back-to-monday in the middle of an
// into restore of the claim notes-data-monday (see intoMonday), with its
// item Running and naming the ReplicationDestination the run created, and
// that destination, which the run's mover writes the claim through. The
// claim itself comes from intoClaim.
func intoRestoring() (*backupv1alpha1.RestoreRun, *volsyncv1alpha1.ReplicationDestination) {
	run := restoreRun(intoMonday, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Target = r.Spec.Into
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: r.Spec.Into,
			Phase: backupv1alpha1.ItemRunning, Destination: destinationName(restoreUID, 0), Snapshot: monday.ShortID()}}
	})
	return run, destinationFor(run, run.Spec.Into)
}

// destinationFor returns the ReplicationDestination the run back-to-monday
// created for its first item, writing into the claim named claim: the run's
// trigger, a Direct restic mover, and the claim's repository Secret.
func destinationFor(run *backupv1alpha1.RestoreRun, claim string) *volsyncv1alpha1.ReplicationDestination {
	return &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: destinationName(run.UID, 0), Namespace: run.Namespace},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: string(run.UID)},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodDirect, DestinationPVC: &claim,
				},
				Repository: repoN,
			},
		},
	}
}

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

// moverPod returns a pod of the VolSync mover that wrote through the
// ReplicationDestination named destination, in the phase given. VolSync runs
// that mover as the Job volsync-dst-<destination> in the run's namespace, and
// the Job's pods carry the label job-name.
func moverPod(destination string, phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "volsync-dst-" + destination + "-abcde", Namespace: ns,
			Labels: map[string]string{"job-name": "volsync-dst-" + destination},
		},
		Status: corev1.PodStatus{Phase: phase},
	}
}

// A mover that is still restoring has a pod that belongs there. The run does
// not wait for it: it would never finish the restore it started, and it goes
// on with the pass.
func TestARunningMoversPodDoesNotHoldTheRestore(t *testing.T) {
	run, destination := restoring()
	pod := moverPod(destination.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), destination, pod)

	restoreStep(t, r)

	after := readRestoreRun(t, c)
	if readyReason(after.Status.Conditions) != backupv1alpha1.ReasonRunning {
		t.Errorf("reason = %q, message = %q; want the run still restoring", readyReason(after.Status.Conditions), readyMessage(after.Status.Conditions))
	}
	if names := destinations(t, c); len(names) != 1 {
		t.Errorf("destinations = %v, want the mover's destination left alone", names)
	}
}

// quiescedInPlace is a mutate function for restoreRun that makes it a
// namespace restore which stops the app's Deployment (see quiescedRestore).
func quiescedInPlace(r *backupv1alpha1.RestoreRun) {
	r.Spec.All = true
	r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
}

// quiescedMidRestore returns the RestoreRun back-to-monday in the middle of a
// quiesced in-place restore of the claim: Running, holding its finalizer,
// with the app stopped (status.quiesced records the Deployment's 2 replicas
// and the Kustomization the run suspended), and one volume item Running that
// names the ReplicationDestination the run created for it. stoppedDeployment
// and suspendedKustomization are the app as the stopping pass left it.
func quiescedMidRestore() (*backupv1alpha1.RestoreRun, *volsyncv1alpha1.ReplicationDestination) {
	started := metav1.NewTime(frozen)
	run := restoreRun(quiescedInPlace, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = &started
		r.Status.QuiescedAt = &started
		r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
		r.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN,
			Phase: backupv1alpha1.ItemRunning, Destination: destinationName(restoreUID, 0), Snapshot: monday.ShortID()}}
	})
	return run, destinationFor(run, claimN)
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

// An into restore whose claim is gone records the item's end before finish
// deletes the ReplicationDestination. A pass that loses that write leaves the
// destination in place, and the pass after it deletes it, so the mover never
// starts again on a claim that is gone.
func TestAnIntoRestoreWhoseClaimIsLostRecordsTheEndBeforeTheMoverGoes(t *testing.T) {
	run, destination := intoRestoring()
	claim := intoClaim(run)
	r, c := restoreReconciler(t, nil, run, claim, destination)
	if err := c.Delete(context.Background(), claim); err != nil {
		t.Fatal(err)
	}

	r.Client = loseNextStatusWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose status write was lost succeeded, want the error returned")
	}
	if names := destinations(t, c); len(names) != 1 {
		t.Fatalf("destinations = %v after the lost write, want %s still there", names, destination.Name)
	}

	restoreStep(t, r)
	restoreStep(t, r) // the pass after the destination's delete finds its mover gone

	after := readRestoreRun(t, c)
	if after.Status.Phase != backupv1alpha1.RunPhaseFailed || after.Status.Items[0].Phase != backupv1alpha1.ItemFailed {
		t.Errorf("phase = %q, item = %+v; want Failed with the item Failed", after.Status.Phase, after.Status.Items[0])
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the run's deleted", names)
	}
}

// The same holds for an item whose mover failed: the end goes into the status
// before the destination is deleted, so a lost write leaves a record that
// stops the next pass from starting the mover again.
func TestAnIntoRestoreWhoseMoverFailedRecordsTheEndBeforeTheMoverGoes(t *testing.T) {
	run, destination := intoRestoring()
	destination.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "restic: repository is already locked"},
	}
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), destination)

	r.Client = loseNextStatusWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose status write was lost succeeded, want the error returned")
	}
	if names := destinations(t, c); len(names) != 1 {
		t.Fatalf("destinations = %v after the lost write, want %s still there", names, destination.Name)
	}

	restoreStep(t, r)
	restoreStep(t, r) // the pass after the destination's delete finds its mover gone

	after := readRestoreRun(t, c)
	if item := after.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "repository is already locked") {
		t.Errorf("item = %+v, want it Failed with the mover's log", item)
	}
	if after.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", after.Status.Phase)
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the run's deleted", names)
	}
}

// A run past its timeout stops its mover before it gives the app back: the
// ReplicationDestination goes, and while a pod of that mover is still there
// the run waits instead of restarting the app, however far past the timeout
// the clock has moved. Once the pod is gone the app comes back and the run
// ends TimedOut.
func TestATimedOutRestoreWaitsForItsStoppedMoversPod(t *testing.T) {
	run, destination := quiescedMidRestore()
	pod := moverPod(destination.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, pod)

	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r)
	restoreStep(t, r) // the pass after the delete looks for the mover

	if names := destinations(t, c); len(names) != 0 {
		t.Fatalf("destinations = %v at the timeout, want the mover's deleted", names)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while mover pod %s was still there, want the app still down", got, pod.Name)
	}
	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() {
		t.Errorf("phase = %q with the mover pod still there, want the run unfinished", waiting.Status.Phase)
	}
	if readyReason(waiting.Status.Conditions) != backupv1alpha1.ReasonShutdown ||
		!strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Errorf("reason = %q, message = %q; want %s naming pod %s",
			readyReason(waiting.Status.Conditions), readyMessage(waiting.Status.Conditions), backupv1alpha1.ReasonShutdown, pod.Name)
	}
	if !suspended(t, c) {
		t.Error("the Kustomization was resumed while the mover pod was still there")
	}

	// The timeout ends the restore once; the pod bounds the wait.
	restoreStep(t, r)
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d a pass later with the mover pod still there, want the app still down", got)
	}

	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the mover pod was gone, want the 2 the app had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization stayed suspended after the mover pod was gone")
	}
	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
	}
}

// A run deleted in the middle of a restore stops its mover, waits for that
// mover's pod, and only then releases its Leases and gives the app back. It
// keeps its finalizer until this is done.
func TestADeletedRestoreWaitsForItsStoppedMoversPod(t *testing.T) {
	run, destination := quiescedMidRestore()
	pod := moverPod(destination.Name, corev1.PodRunning)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, pod, lease)
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r)
	restoreStep(t, r) // the pass after the delete looks for the mover

	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the mover's deleted", names)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while mover pod %s was still there, want the app still down", got, pod.Name)
	}
	deleting := readRestoreRun(t, c)
	if len(deleting.Finalizers) == 0 {
		t.Error("the run dropped its finalizer while the mover pod was still there")
	}
	if !strings.Contains(readyMessage(deleting.Status.Conditions), pod.Name) {
		t.Errorf("ready message = %q, want it to name pod %s", readyMessage(deleting.Status.Conditions), pod.Name)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while the mover pod was still there", err)
	}

	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the mover pod was gone, want the 2 the app had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization stayed suspended after the mover pod was gone")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Errorf("get the claim Lease = %v, want it released once the mover pod was gone", err)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("RestoreRun = %v, want it gone once its finalizer was dropped", err)
	}
}

// A wait for a stopped mover's pod whose status write is lost is reported
// again on the next pass. The run holds what it must meanwhile: the pod, its
// finalizer and the Leases.
func TestALostStatusWriteInTheMoverWaitConverges(t *testing.T) {
	run, destination := quiescedMidRestore()
	pod := moverPod(destination.Name, corev1.PodRunning)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, pod, lease)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	r.Client = loseNextStatusWrite(c)

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the wait pass succeeded, want its lost status write returned")
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the mover's deleted", names)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d, want the app still down while the mover pod is there", got)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while the mover pod is there", err)
	}

	restoreStep(t, r)

	waiting := readRestoreRun(t, c)
	if len(waiting.Finalizers) == 0 {
		t.Error("the run dropped its finalizer while the mover pod was still there")
	}
	if readyReason(waiting.Status.Conditions) != backupv1alpha1.ReasonShutdown ||
		!strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Errorf("reason = %q, message = %q; want the wait reported again, naming pod %s",
			readyReason(waiting.Status.Conditions), readyMessage(waiting.Status.Conditions), pod.Name)
	}
}

// An into restore whose claim is deleted while its mover writes stops the
// mover, and keeps its Leases until that mover's pod is gone. The run ends
// Failed only once the pod has left.
func TestAnIntoRestoreWhoseClaimIsLostWaitsForItsStoppedMoversPod(t *testing.T) {
	run, destination := intoRestoring()
	claim := intoClaim(run)
	pod := moverPod(destination.Name, corev1.PodRunning)
	lease := heldClaimLease(run, run.Spec.Into)
	r, c := restoreReconciler(t, nil, run, claim, sourceOnNode(), volumeRestore(), repository(), destination, pod, lease)
	if err := c.Delete(context.Background(), claim); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r)

	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the mover's deleted", names)
	}
	stopping := readRestoreRun(t, c)
	if stopping.Status.Phase.Finished() {
		t.Errorf("phase = %q while the mover pod was still there, want the run unfinished", stopping.Status.Phase)
	}
	if item := stopping.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed {
		t.Errorf("item = %+v, want it Failed with its end recorded", item)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while the mover pod was still there", err)
	}

	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q once the mover pod was gone, want Failed", done.Status.Phase)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Errorf("get the claim Lease = %v, want it released once the mover pod was gone", err)
	}
}

// An into restore past its timeout stops its mover and waits for that mover's
// pod. The pass that finds the pod gone ends the run with reason TimedOut,
// the reason the timeout gave it, and not with the Failed of an item that
// failed on its own. It goes by the ending the timeout recorded, so an item
// message edited during the wait changes nothing.
func TestATimedOutIntoRestoreEndsTimedOutAfterItsMoverWait(t *testing.T) {
	run, destination := intoRestoring()
	started := metav1.NewTime(frozen)
	run.Status.StartedAt = &started
	pod := moverPod(destination.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), destination, pod)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStep(t, r)

	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() {
		t.Fatalf("phase = %q with the mover pod still there, want the run unfinished", waiting.Status.Phase)
	}
	if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut {
		t.Errorf("item = %+v, want it Failed with reason TimedOut", item)
	}
	waiting.Status.Items[0].Message = "edited between the passes"
	if err := c.Status().Update(context.Background(), waiting); err != nil {
		t.Fatal(err)
	}

	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Errorf("phase = %q, reason = %q, message = %q; want Failed, %s",
			done.Status.Phase, readyReason(done.Status.Conditions), readyMessage(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
	}
}

// An into restore whose mover failed before the timeout keeps reason Failed
// when its mover's pod outlives the deadline: the item failed on its own.
func TestAnIntoRestoreWhoseMoverFailedKeepsFailedPastTheTimeout(t *testing.T) {
	run, destination := intoRestoring()
	started := metav1.NewTime(frozen)
	run.Status.StartedAt = &started
	destination.Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "restic: repository is already locked"},
	}
	pod := moverPod(destination.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), destination, pod)

	restoreStep(t, r)
	if readRestoreRun(t, c).Status.Phase.Finished() {
		t.Fatal("the run finished with the mover pod still there, want it waiting")
	}

	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonFailed)
	}
}

// moverJob returns the Job VolSync runs for the mover of the
// ReplicationDestination named destination, in the run's namespace, with the
// name internalvolsync.MoverJobName gives it.
func moverJob(destination string) *batchv1.Job {
	return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: internalvolsync.MoverJobName(destination), Namespace: ns}}
}

// A mover Job whose pod is not there holds the run as a pod does. The Job
// may be between two pods (VolSync gives it a backoffLimit of 8) or may not
// have started its first, and either way it can still start a pod that
// writes. The app comes back only once the Job is gone too.
func TestAStoppedMoversJobWithoutAPodHoldsTheRestore(t *testing.T) {
	run, destination := quiescedMidRestore()
	job := moverJob(destination.Name)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, job)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStep(t, r)
	restoreStep(t, r)

	if names := destinations(t, c); len(names) != 0 {
		t.Fatalf("destinations = %v at the timeout, want the mover's deleted", names)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while mover Job %s was still there, want the app still down", got, job.Name)
	}
	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() || !strings.Contains(readyMessage(waiting.Status.Conditions), job.Name) {
		t.Errorf("phase = %q, message = %q; want the run waiting and naming Job %s",
			waiting.Status.Phase, readyMessage(waiting.Status.Conditions), job.Name)
	}

	if err := c.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the mover Job was gone, want the 2 the app had", got)
	}
	if done := readRestoreRun(t, c); readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Errorf("reason = %q, want %s", readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
	}
}

// Which of a stopped mover's pods hold the run. While the mover's Job is
// there, every pod counts, since the Job can start another after one that
// ended. Once the Job is gone, a pod that has Succeeded or Failed has no
// container left that writes and no Job to replace it, so it does not count;
// a pod in any other phase still does.
func TestWhichPodsOfAStoppedMoverHoldTheRestore(t *testing.T) {
	for _, tc := range []struct {
		phase   corev1.PodPhase
		withJob bool
		holds   bool
	}{
		{corev1.PodSucceeded, true, true},
		{corev1.PodFailed, true, true},
		{corev1.PodSucceeded, false, false},
		{corev1.PodFailed, false, false},
		{corev1.PodPending, false, true},
		{corev1.PodRunning, false, true},
		{corev1.PodUnknown, false, true},
	} {
		name := string(tc.phase) + " pod, no Job"
		if tc.withJob {
			name = string(tc.phase) + " pod of a Job still there"
		}
		t.Run(name, func(t *testing.T) {
			run, destination := quiescedMidRestore()
			objects := []client.Object{run, claim(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true),
				destination, moverPod(destination.Name, tc.phase)}
			if tc.withJob {
				objects = append(objects, moverJob(destination.Name))
			}
			r, c := restoreReconciler(t, nil, objects...)
			r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

			restoreStep(t, r)
			restoreStep(t, r)

			if back := replicasOf(t, c) == 2; back == tc.holds {
				t.Errorf("app back = %t, want %t", back, !tc.holds)
			}
		})
	}
}

// The pass that deletes a mover's ReplicationDestination does not go on to
// give the app back, even when it finds no pod and no Job. VolSync may be in
// the middle of a reconcile of that destination, and its Job may not be
// visible yet. The next pass looks again.
func TestThePassThatStopsAMoverDoesNotGiveTheAppBack(t *testing.T) {
	run, destination := quiescedMidRestore()
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	result := restoreStep(t, r)

	if names := destinations(t, c); len(names) != 0 {
		t.Fatalf("destinations = %v at the timeout, want the mover's deleted", names)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d in the pass that deleted the destination, want the app still down", got)
	}
	if result.RequeueAfter == 0 {
		t.Errorf("result = %+v, want a requeue to look for the mover again", result)
	}

	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d on the pass after, want the 2 the app had", got)
	}
}

// quiescedRestoreDone returns the run from quiescedMidRestore with its volume
// item Succeeded in an earlier pass, whose status write recorded the end
// before the run deleted the ReplicationDestination, and that destination.
func quiescedRestoreDone() (*backupv1alpha1.RestoreRun, *volsyncv1alpha1.ReplicationDestination) {
	run, destination := quiescedMidRestore()
	run.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	return run, destination
}

// A finished item whose stopped mover is still there keeps its Lease. work
// releases the Leases of finished items at the start of each pass, so a
// backup of the claim need not wait for the rest of the run, but a mover that
// may still write keeps the claim and the repository to its run (rule X2).
func TestAFinishedItemKeepsItsLeaseWhileItsStoppedMoverIsThere(t *testing.T) {
	run, destination := quiescedRestoreDone()
	pod := moverPod(destination.Name, corev1.PodRunning)
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, pod, lease)

	restoreStep(t, r)
	restoreStep(t, r)

	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while mover pod %s is there", err, pod.Name)
	}
}

// A run whose finished item's stopped mover is still there does not give the
// app back, however far the rest of the run has come: that mover may still
// write into the claim the app would mount (rule X2).
func TestARestoreDoesNotGiveTheAppBackWhileAFinishedItemsMoverIsThere(t *testing.T) {
	run, destination := quiescedRestoreDone()
	pod := moverPod(destination.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, pod)

	restoreStep(t, r)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d while mover pod %s is there, want the app still down", got, pod.Name)
	}
	if !suspended(t, c) {
		t.Error("the Kustomization was resumed while the mover pod was there")
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

// The status write of the pass that deletes a stopped mover's
// ReplicationDestination starts another reconcile at once. That pass comes
// milliseconds after the delete, while VolSync may still be in the middle of
// a reconcile of the destination that creates the mover's Job. So the run
// counts the mover gone only once pollInterval has passed since the delete:
// until then a finished item keeps its Lease and the app stays down, even
// when no Job and no pod are there. A Job VolSync creates late is found by
// the pass after that time, and holds the run until it is gone.
func TestAStoppedMoverCountsGoneOnlyAPollIntervalAfterTheDelete(t *testing.T) {
	run, destination := quiescedRestoreDone()
	lease := heldClaimLease(run, claimN)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, lease)

	restoreStepAtOnce(t, r) // deletes the destination
	restoreStepAtOnce(t, r) // the pass the status write starts, at the same instant

	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d on the pass right after the delete, want the app still down", got)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); err != nil {
		t.Errorf("get the claim Lease = %v on the pass right after the delete, want it held", err)
	}
	if reason := readyReason(readRestoreRun(t, c).Status.Conditions); reason != backupv1alpha1.ReasonShutdown {
		t.Errorf("reason = %q on the pass right after the delete, want %s", reason, backupv1alpha1.ReasonShutdown)
	}

	// VolSync's reconcile that was under way creates the mover's Job late.
	job := moverJob(destination.Name)
	if err := c.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	advance(r, pollInterval)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d with the late mover Job there, want the app still down", got)
	}
	if message := readyMessage(readRestoreRun(t, c).Status.Conditions); !strings.Contains(message, job.Name) {
		t.Errorf("ready message = %q, want it to name Job %s", message, job.Name)
	}

	if err := c.Delete(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the mover Job was gone, want the 2 the app had", got)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
		t.Errorf("get the claim Lease = %v, want it released once the mover was gone", err)
	}
}

// The same holds when the run ends: finish counts a mover it stopped gone
// only once pollInterval has passed since it deleted the destination.
func TestAnEndingRunCountsItsStoppedMoverGoneOnlyAPollIntervalAfterTheDelete(t *testing.T) {
	run, destination := quiescedMidRestore()
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStepAtOnce(t, r)
	restoreStepAtOnce(t, r)

	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d on the pass right after the delete, want the app still down", got)
	}
	if readRestoreRun(t, c).Status.Phase.Finished() {
		t.Error("the run finished on the pass right after the delete, want it waiting")
	}

	advance(r, pollInterval)
	restoreStep(t, r)

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d a poll interval after the delete, want the 2 the app had", got)
	}
}

// A run that times out while it waits for another run, and then waits for
// its own stopped mover, ends with the wait it timed out on in its Ready
// message. The pass that times out records that message in status.ending,
// in the write that fails the item with reason TimedOut. The mover wait
// replaces the SourceBusy condition and the item's message is edited before
// the last pass, and the run still ends with the ending as recorded.
func TestATimedOutRunKeepsTheWaitItTimedOutOn(t *testing.T) {
	run, destination := quiescedMidRestore()
	wait := "BackupRun manual-notes holds Lease backup-controller-repo-secret-uid for notes-data; this run starts once that run has finished with it"
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonSourceBusy, wait)
	pod := moverPod(destination.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, pod)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStep(t, r)
	want := "the run had not finished by " + frozen.Add(4*time.Hour).Format(time.RFC3339) + "; it was waiting: " + wait
	waiting := readRestoreRun(t, c)
	if waiting.Status.Phase.Finished() {
		t.Fatal("the run finished with its mover pod still there, want it waiting")
	}
	if got := waiting.Status.Ending; got == nil || *got != (backupv1alpha1.RunEnding{Reason: backupv1alpha1.ReasonTimedOut, Message: want}) {
		t.Fatalf("ending = %+v during the mover wait, want reason TimedOut and %q", got, want)
	}
	if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut || item.Message != want {
		t.Errorf("item = %+v, want it Failed with reason TimedOut and %q", item, want)
	}
	waiting.Status.Items[0].Message = "edited between the passes"
	if err := c.Status().Update(context.Background(), waiting); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut ||
		readyMessage(done.Status.Conditions) != want {
		t.Errorf("phase = %q, reason = %q, message = %q; want Failed, %s, %q", done.Status.Phase, readyReason(done.Status.Conditions),
			readyMessage(done.Status.Conditions), backupv1alpha1.ReasonTimedOut, want)
	}
}

// A restore whose items have all finished only waits for its stopped
// movers to go before it gives the app back, and a deadline that passes
// during that wait does not end it TimedOut: it ends as its items say. Here
// the item Succeeded before the deadline, and the run still waits for the
// mover's pod when the deadline passes. Before, the pass after the deadline
// timed the run out, and it ended Failed although it restored everything.
func TestAFinishedRestoreEndsAsItsItemsSayWhenTheDeadlinePassesInItsMoverWait(t *testing.T) {
	run, destination := quiescedRestoreDone()
	pod := moverPod(destination.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, pod)
	deadline := frozen.Add(4 * time.Hour)
	r.Now = func() time.Time { return deadline.Add(-30 * time.Second) }

	restoreStep(t, r) // deletes the destination
	restoreStep(t, r) // finds the mover's pod still there
	if reason := readyReason(readRestoreRun(t, c).Status.Conditions); reason != backupv1alpha1.ReasonShutdown {
		t.Fatalf("reason = %q before the deadline, want %s", reason, backupv1alpha1.ReasonShutdown)
	}
	r.Now = func() time.Time { return deadline.Add(time.Minute) }
	restoreStep(t, r)
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	restoreStep(t, r)

	done := readRestoreRun(t, c)
	if done.Status.Phase != backupv1alpha1.RunPhaseSucceeded || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonSucceeded {
		t.Errorf("phase = %q, reason = %q, message = %q; want Succeeded, %s", done.Status.Phase, readyReason(done.Status.Conditions),
			readyMessage(done.Status.Conditions), backupv1alpha1.ReasonSucceeded)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the mover was gone, want the 2 the app had", got)
	}
}

// The pass that decides to end a run records the ending, even when it then
// waits for the same stopped mover with the same message as the pass
// before. Here one item failed before the deadline and its mover's pod is
// still there, and the other waits for a pod that mounts its claim. The
// pass after the deadline fails the waiting item with reason TimedOut, and
// that item and the ending reach the stored status. Before, the wait wrote
// only a changed Ready condition, so both were lost.
func TestTheEndingIsStoredWhenTheMoverWaitIsUnchanged(t *testing.T) {
	const other = "notes-cache"
	run, destination := quiescedMidRestore()
	run.Status.Items[0].Phase = backupv1alpha1.ItemFailed
	run.Status.Items = append(run.Status.Items, backupv1alpha1.RestoreItem{Kind: "PersistentVolumeClaim", Name: other,
		Phase: backupv1alpha1.ItemPending})
	mover := moverPod(destination.Name, corev1.PodRunning)
	mounting := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-5d9f", Namespace: ns},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
			PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: other},
		}}}},
	}
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), destination, mover, mounting)
	r.Now = func() time.Time { return frozen.Add(time.Hour) }

	restoreStep(t, r) // deletes the failed item's destination
	restoreStep(t, r) // finds the mover's pod still there
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
// mover as the pass before stores those items in that pass. Here the first
// volume failed and its mover's pod is still there, the second volume's
// mover is still restoring, and the Cluster waits for both. Then the
// second mover fails: the pass that
// finds it stores that volume's end, skips the Cluster because a volume
// failed, and waits for the first mover's pod with the same message as
// before. Before, the wait wrote only a changed Ready condition, so the
// skipped Cluster was stored only by a later pass.
func TestWorkStoresItsItemsWhenTheMoverWaitIsUnchanged(t *testing.T) {
	const other = "notes-cache"
	run, first := quiescedMidRestore()
	run.Status.Items[0].Phase = backupv1alpha1.ItemFailed
	run.Status.Items = append(run.Status.Items,
		backupv1alpha1.RestoreItem{Kind: "PersistentVolumeClaim", Name: other, Phase: backupv1alpha1.ItemRunning,
			Destination: destinationName(restoreUID, 1), Snapshot: monday.ShortID()},
		backupv1alpha1.RestoreItem{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemPending, BaseBackup: saturday.ID})
	second := destinationFor(run, other)
	second.Name = destinationName(restoreUID, 1)
	mover := moverPod(first.Name, corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), cluster(),
		stoppedDeployment(), kustomization(true), first, second, mover)

	restoreStep(t, r) // deletes the failed volume's destination
	restoreStep(t, r) // finds that mover's pod still there
	before := readRestoreRun(t, c)
	if readyReason(before.Status.Conditions) != backupv1alpha1.ReasonShutdown || !strings.Contains(readyMessage(before.Status.Conditions), mover.Name) {
		t.Fatalf("reason = %q, message = %q; want %s naming pod %s", readyReason(before.Status.Conditions),
			readyMessage(before.Status.Conditions), backupv1alpha1.ReasonShutdown, mover.Name)
	}
	get(t, c, ns, second.Name, second)
	second.Status = &volsyncv1alpha1.ReplicationDestinationStatus{LatestMoverStatus: &volsyncv1alpha1.MoverStatus{
		Result: volsyncv1alpha1.MoverResultFailed, Logs: "Fatal: unable to open repository",
	}}
	if err := c.Status().Update(context.Background(), second); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	stored := readRestoreRun(t, c)
	if readyMessage(stored.Status.Conditions) != readyMessage(before.Status.Conditions) {
		t.Fatalf("message = %q, want the wait unchanged: %q", readyMessage(stored.Status.Conditions), readyMessage(before.Status.Conditions))
	}
	if item := stored.Status.Items[1]; item.Phase != backupv1alpha1.ItemFailed {
		t.Errorf("item %s = %+v, want it stored Failed", other, item)
	}
	if item := stored.Status.Items[2]; item.Phase != backupv1alpha1.ItemSkipped {
		t.Errorf("item %s = %+v in the pass that skipped it, want it stored Skipped", pgN, item)
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
