package runs

import (
	"context"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
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
	claimName := run.Spec.Into
	destination := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: destinationName(restoreUID, 0), Namespace: ns},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: string(restoreUID)},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodDirect, DestinationPVC: &claimName,
				},
				Repository: repoN,
			},
		},
	}
	return run, destination
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
