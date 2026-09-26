package runs

import (
	"context"
	"slices"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The tests in this file check that two restores never write one claim at
// once (finding F in the RestoreRun design): a second in-place restore of a
// claim waits while another ReplicationDestination writes into it, and while
// the mover of another run's finished item is not gone yet (rule X2).

// secondRestoreUID is the UID of the RestoreRun second, which the tests in
// this file run beside back-to-monday.
const secondRestoreUID = types.UID("5c3a1f07-0000-4000-8000-00000000000b")

// secondClaimRestore returns the RestoreRun second, an in-place restore of
// the claim notes-data, like back-to-monday's.
func secondClaimRestore() *backupv1alpha1.RestoreRun {
	return restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Name, r.UID, r.Spec.Claim = "second", secondRestoreUID, claimN
	})
}

// readRun reads the RestoreRun with the given name back from the client.
func readRun(t *testing.T, r *RestoreRunReconciler, name string) *backupv1alpha1.RestoreRun {
	t.Helper()
	run := &backupv1alpha1.RestoreRun{}
	get(t, r.Client, ns, name, run)
	return run
}

// Two in-place restores of one claim run one after the other. While the first
// run's ReplicationDestination writes into the claim, the second waits with
// reason ClaimInUse naming it. Once the first item has ended and its
// destination is deleted, the second keeps waiting, with reason SourceBusy,
// for as long as the first run's mover pod is there, since that mover may
// still write. Only once the first run has finished does the second create
// its own destination.
func TestTwoInPlaceRestoresOfOneClaimRunOneAtATime(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		secondClaimRestore(), claim(), volumeRestore(), repository())
	stepRestore(t, r, "back-to-monday") // plan
	stepRestore(t, r, "second")         // plan
	stepRestore(t, r, "back-to-monday") // create the destination
	stepRestore(t, r, "second")

	first := readRestoreRun(t, c).Status.Items[0].Destination
	if names := destinations(t, c); !slices.Equal(names, []string{first}) {
		t.Fatalf("destinations = %v, want only back-to-monday's %s", names, first)
	}
	second := readRun(t, r, "second")
	if readyReason(second.Status.Conditions) != backupv1alpha1.ReasonClaimInUse ||
		readyMessage(second.Status.Conditions) != "ReplicationDestination "+first+" is restoring into claim "+claimN {
		t.Errorf("second run: reason = %q, message = %q; want ClaimInUse naming %s",
			readyReason(second.Status.Conditions), readyMessage(second.Status.Conditions), first)
	}

	// The first item ends; its destination goes, and its mover pod stays for
	// a while.
	pod := moverPod(first, corev1.PodRunning)
	if err := c.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	completeVolume(t, c)
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "second")
	stepRestore(t, r, "second")

	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemSucceeded || item.Destination != first {
		t.Fatalf("first item = %+v, want Succeeded and still naming %s while its mover pod is there", item, first)
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v while mover pod %s is there, want none", names, pod.Name)
	}
	second = readRun(t, r, "second")
	if second.Status.Items[0].Phase != backupv1alpha1.ItemPending || readyReason(second.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(second.Status.Conditions), "RestoreRun back-to-monday") {
		t.Errorf("second run: item = %+v, reason = %q, message = %q; want Pending, SourceBusy naming back-to-monday",
			second.Status.Items[0], readyReason(second.Status.Conditions), readyMessage(second.Status.Conditions))
	}

	// The first run's mover is gone: it finishes, and the second starts.
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "second")

	if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("first run phase = %q, items = %+v; want Succeeded", run.Status.Phase, run.Status.Items)
	}
	if names := destinations(t, c); !slices.Equal(names, []string{destinationName(secondRestoreUID, 0)}) {
		t.Errorf("destinations = %v, want the second run's own", names)
	}
}

// A ReplicationDestination that no run of this controller created, whose
// restic mover writes into the claim, holds an in-place restore of that
// claim: the run waits with reason ClaimInUse naming it, and creates its own
// destination only once it is gone. No Lease guards such a destination, so
// the run looks at the destinations themselves.
func TestAnInPlaceRestoreWaitsForAnotherDestinationOfItsClaim(t *testing.T) {
	claimName := claimN
	foreign := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-data-by-hand", Namespace: ns},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: "by-hand"},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodDirect, DestinationPVC: &claimName,
				},
				Repository: repoN,
			},
		},
	}
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository(), foreign)
	restoreStep(t, r) // plan
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Items[0].Phase != backupv1alpha1.ItemPending || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonClaimInUse ||
		readyMessage(run.Status.Conditions) != "ReplicationDestination notes-data-by-hand is restoring into claim "+claimN {
		t.Fatalf("item = %+v, reason = %q, message = %q; want Pending, ClaimInUse naming notes-data-by-hand",
			run.Status.Items[0], readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if names := destinations(t, c); !slices.Equal(names, []string{foreign.Name}) {
		t.Fatalf("destinations = %v, want only %s", names, foreign.Name)
	}

	if err := c.Delete(context.Background(), foreign); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Errorf("item = %+v once the other destination is gone, want Running", item)
	}
}
