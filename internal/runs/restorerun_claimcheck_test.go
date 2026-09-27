package runs

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
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
	completeJob(t, c)

	restoreStep(t, r)

	expectVolumeFailed(t, readRestoreRun(t, c), "claim "+claimN, "deleted")
}

// An in-place item whose claim was replaced by another of the same name
// while the mover wrote fails once the mover completes. The mover mounts the
// claim by name, so it may have written into the new claim as well; the
// message says so and asks for the claim's data to be checked.
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
	completeJob(t, c)

	restoreStep(t, r)

	expectVolumeFailed(t, readRestoreRun(t, c), "claim "+claimN, "replaced", "not the one the run checked and took its Lease on",
		"may have written into it", "check its data")
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
	completeJob(t, c)

	restoreStep(t, r)

	expectVolumeFailed(t, readRestoreRun(t, c), "claim Lease")
}

// An in-place item whose claim is the one the run took its Lease on
// succeeds.
func TestAnInPlaceRestoreIntoItsOwnClaimSucceeds(t *testing.T) {
	r, c := startedRestore(t, inPlace)
	completeJob(t, c)

	restoreStep(t, r)

	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemSucceeded {
		t.Errorf("item = %+v, want it Succeeded", item)
	}
}

// A restore's mover gets the cache capacity of the claim's VolumeRestore,
// as a backup's ReplicationSource and the populator's destination do: the
// restore Job, in place and into a new claim from the claim's backups,
// sizes its cache volume with it, together with the VolumeRestore's cache
// class and pod labels. Without it the cache would get the default of 1Gi,
// which a large repository outgrows. A restore from a repository alone has
// no VolumeRestore to copy from, and its Job's cache keeps the default size
// and no class.
func TestARestoreDestinationGetsTheCacheCapacity(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			vr := volumeRestore()
			capacity := resource.MustParse("4Gi")
			vr.Spec.CacheCapacity = &capacity
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate), claim(), vr, repository())
			restoreStep(t, r) // plan
			restoreStep(t, r) // create

			job := itemJob(t, c)
			if shape.name != "into from a repository" {
				expectJobCache(t, job, capacity)
				return
			}
			for _, v := range job.Spec.Template.Spec.Volumes {
				if v.Ephemeral == nil {
					continue
				}
				spec := v.Ephemeral.VolumeClaimTemplate.Spec
				if got := spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(resource.MustParse("1Gi")) != 0 || spec.StorageClassName != nil {
					t.Errorf("cache volume = %+v, want the default 1Gi and no class with no VolumeRestore", spec)
				}
			}
		})
	}
}

// expectJobCache checks that a restore Job's pod carries what the claim's
// VolumeRestore declares for its mover: the cache volume's class and
// capacity, and the Kueue queue label on the pod only.
func expectJobCache(t *testing.T, job *batchv1.Job, capacity resource.Quantity) {
	t.Helper()
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.Ephemeral == nil {
			continue
		}
		spec := v.Ephemeral.VolumeClaimTemplate.Spec
		if got := spec.Resources.Requests[corev1.ResourceStorage]; got.Cmp(capacity) != 0 ||
			spec.StorageClassName == nil || *spec.StorageClassName != "zfs-ephemeral" {
			t.Errorf("cache volume = %+v, want %s of class zfs-ephemeral", spec, capacity.String())
		}
	}
	if got := job.Spec.Template.Labels["kueue.x-k8s.io/queue-name"]; got != "backups" || job.Labels["kueue.x-k8s.io/queue-name"] != "" {
		t.Errorf("pod queue label = %q, Job labels = %v; want backups on the pod only", got, job.Labels)
	}
}
