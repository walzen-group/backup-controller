package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// The tests in this file cover an in-place restore whose pass created the
// item's ReplicationDestination and lost the status write that recorded it
// (UFR1). The item stays Pending and names no destination, while the mover
// of that destination mounts the claim.

// mountingMoverPod returns the running pod of the mover that writes through
// the ReplicationDestination named destination, mounting the claim the way a
// Direct restic mover does. claimHolder sees it as a pod holding the claim.
func mountingMoverPod(destination string) *corev1.Pod {
	pod := moverPod(destination, corev1.PodRunning)
	pod.Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{
		PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: claimN},
	}}}
	return pod
}

// A quiesced in-place restore whose destination create went through and
// whose status write was lost stops that destination's mover when it is
// deleted or times out. It deletes the destination, keeps the app down, its
// finalizer and the claim Lease while the mover's pod is there, and gives the
// app back only once the pod is gone (rule X2). That holds when a pass ran
// between the lost write and the end, which saw the run's own mover pod on
// the claim, and when the end came on the very next pass.
func TestALostDestinationCreateIsStoppedAtTheEnd(t *testing.T) {
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
			name := destinationName(restoreUID, 0)
			if names := destinations(t, c); len(names) != 1 || names[0] != name {
				t.Fatalf("destinations = %v after the lost write, want %s", names, name)
			}
			if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemPending || item.Destination != "" {
				t.Fatalf("item = %+v after the lost write, want Pending with no destination", item)
			}
			pod := mountingMoverPod(name)
			if err := c.Create(context.Background(), pod); err != nil {
				t.Fatal(err)
			}

			if tc.passBetween {
				restoreStep(t, r)
				between := readRestoreRun(t, c)
				if item := between.Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Destination != name {
					t.Errorf("item = %+v, reason = %q; want Running naming %s, the destination the lost pass created",
						item, readyReason(between.Status.Conditions), name)
				}
			}

			if tc.timeout {
				r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
			} else if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r)
			restoreStep(t, r) // the pass after the delete looks for the mover

			if names := destinations(t, c); len(names) != 0 {
				t.Fatalf("destinations = %v, want the lost pass's deleted", names)
			}
			if got := replicasOf(t, c); got != 0 {
				t.Fatalf("replicas = %d while mover pod %s was still there, want the app still down", got, pod.Name)
			}
			waiting := readRestoreRun(t, c)
			if waiting.Status.Phase.Finished() || len(waiting.Finalizers) == 0 {
				t.Errorf("phase = %q, finalizers = %v with the mover pod still there; want the run unfinished and holding its finalizer",
					waiting.Status.Phase, waiting.Finalizers)
			}
			if !strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
				t.Errorf("ready message = %q, want it to name pod %s", readyMessage(waiting.Status.Conditions), pod.Name)
			}
			lease := types.NamespacedName{Namespace: ns, Name: claimLeaseName("claim-uid")}
			if err := c.Get(context.Background(), lease, &coordinationv1.Lease{}); err != nil {
				t.Errorf("get the claim Lease = %v, want it held while the mover pod was still there", err)
			}

			if err := c.Delete(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r)

			if got := replicasOf(t, c); got != 2 {
				t.Errorf("replicas = %d once the mover pod was gone, want the 2 the app had", got)
			}
			if err := c.Get(context.Background(), lease, &coordinationv1.Lease{}); !apierrors.IsNotFound(err) {
				t.Errorf("get the claim Lease = %v, want it released once the mover pod was gone", err)
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
