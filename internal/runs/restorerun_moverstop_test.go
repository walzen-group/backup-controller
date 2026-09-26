package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
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
