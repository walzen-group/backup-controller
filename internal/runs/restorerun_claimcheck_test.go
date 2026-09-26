package runs

import (
	"context"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

// inPlace is a mutate function for restoreRun that makes it an in-place
// restore of the claim notes-data.
func inPlace(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }

// expectVolumeFailed checks that the run's volume item failed with a message
// holding every string in want, and did not succeed.
func expectVolumeFailed(t *testing.T, run *backupv1alpha1.RestoreRun, want ...string) {
	t.Helper()
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed {
		t.Fatalf("item = %+v, want it Failed", item)
	}
	for _, w := range want {
		if !strings.Contains(item.Message, w) {
			t.Errorf("item message = %q, want it to hold %q", item.Message, w)
		}
	}
}

// An in-place item whose claim was deleted while the mover wrote fails once
// the mover completes. The claim is Terminating: pvc-protection keeps it
// while the mover's pod mounts it, and the mover finishes writing into a
// claim that is about to go. Before, the item succeeded on the mover's log
// alone.
func TestAnInPlaceRestoreIntoADeletedClaimFails(t *testing.T) {
	r, c := startedRestore(t, inPlace)
	pvc := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, claimN, pvc)
	pvc.Finalizers = append(pvc.Finalizers, "kubernetes.io/pvc-protection")
	if err := c.Update(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
	completeVolume(t, c)

	restoreStep(t, r)

	expectVolumeFailed(t, readRestoreRun(t, c), "claim "+claimN, "deleted")
}

// An in-place item whose claim was replaced by another of the same name
// while the mover wrote fails once the mover completes: the mover wrote into
// the claim the checks saw, and the claim there now is another one.
func TestAnInPlaceRestoreIntoAReplacedClaimFails(t *testing.T) {
	r, c := startedRestore(t, inPlace)
	if err := c.Delete(context.Background(), claim()); err != nil {
		t.Fatal(err)
	}
	replacement := claim()
	replacement.UID = "claim-uid-2"
	replacement.ResourceVersion = ""
	if err := c.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	completeVolume(t, c)

	restoreStep(t, r)

	expectVolumeFailed(t, readRestoreRun(t, c), "claim "+claimN, "replaced")
}

// An in-place item whose run no longer holds a claim Lease for it can't tell
// which claim the mover wrote into, and fails once the mover completes.
func TestAnInPlaceRestoreWithoutItsClaimLeaseFails(t *testing.T) {
	r, c := startedRestore(t, inPlace)
	lease := &coordinationv1.Lease{}
	get(t, c, ns, claimLeaseName("claim-uid"), lease)
	if err := c.Delete(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
	completeVolume(t, c)

	restoreStep(t, r)

	expectVolumeFailed(t, readRestoreRun(t, c), "claim Lease")
}

// An in-place item whose claim is the one the run took its Lease on
// succeeds.
func TestAnInPlaceRestoreIntoItsOwnClaimSucceeds(t *testing.T) {
	r, c := startedRestore(t, inPlace)
	completeVolume(t, c)

	restoreStep(t, r)

	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemSucceeded {
		t.Errorf("item = %+v, want it Succeeded", item)
	}
}

// A restore's ReplicationDestination gets the cache capacity of the claim's
// VolumeRestore, as a backup's ReplicationSource and the populator's
// destination do. Without it VolSync sizes the mover's cache at its default
// of 1Gi, which a large repository outgrows. A restore from a repository
// alone has no VolumeRestore to copy from and leaves the field unset.
func TestARestoreDestinationGetsTheCacheCapacity(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			vr := volumeRestore()
			capacity := resource.MustParse("4Gi")
			vr.Spec.CacheCapacity = &capacity
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate), claim(), vr, repository())
			restoreStep(t, r) // plan
			restoreStep(t, r) // create

			rd := &volsyncv1alpha1.ReplicationDestination{}
			get(t, c, ns, readRestoreRun(t, c).Status.Items[0].Destination, rd)
			got := rd.Spec.Restic.CacheCapacity
			if shape.into != "" && shape.name == "into from a repository" {
				if got != nil {
					t.Errorf("cacheCapacity = %v, want it unset with no VolumeRestore", got)
				}
				return
			}
			if got == nil || got.Cmp(capacity) != 0 {
				t.Errorf("cacheCapacity = %v, want %s", got, capacity.String())
			}
		})
	}
}
