package runs

import (
	"context"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// endedItem is what a case reads back from the run's first item once the
// run has refused it: its phase, its reason and its message.
type endedItem struct {
	phase   backupv1alpha1.ItemPhase
	reason  backupv1alpha1.ItemReason
	message string
}

// nothingWrittenEnd is the sentence a refused in-place restore item's
// message ends with, written out so a change to it fails the test.
const nothingWrittenEnd = ". Nothing was written to claim " + claimN + ". Create a new RestoreRun to select again"

// deleteClaim deletes the claim notes-data, which the case seeded.
func deleteClaim(t *testing.T, c client.Client) {
	t.Helper()
	pvc := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, claimN, pvc)
	if err := c.Delete(context.Background(), pvc); err != nil {
		t.Fatal(err)
	}
}

// deleteVolumeRestore deletes the VolumeRestore notes-data, which the case
// seeded.
func deleteVolumeRestore(t *testing.T, c client.Client) {
	t.Helper()
	if err := c.Delete(context.Background(), volumeRestore()); err != nil {
		t.Fatal(err)
	}
}

// backupItemAfter runs a BackupRun over the given objects for four passes,
// calling between after the plan, and returns its first item.
func backupItemAfter(t *testing.T, run *backupv1alpha1.BackupRun, between func(*testing.T, client.Client), objects ...client.Object) endedItem {
	t.Helper()
	r, c := backupReconciler(t, append([]client.Object{run}, objects...)...)
	step(t, r) // plan
	if between != nil {
		between(t, c)
	}
	step(t, r) // admit
	step(t, r) // quiesce or start
	step(t, r)
	item := readBackupRun(t, c).Status.Items[0]
	return endedItem{item.Phase, item.Reason, item.Message}
}

// restoreItemAfter runs a RestoreRun over the given objects for three
// passes, calling between after the plan, and returns its first item.
func restoreItemAfter(t *testing.T, run *backupv1alpha1.RestoreRun, between func(*testing.T, client.Client), objects ...client.Object) endedItem {
	t.Helper()
	r, c := restoreReconciler(t, prober{saturday}, append([]client.Object{run}, objects...)...)
	restoreStep(t, r) // plan
	if between != nil {
		between(t, c)
	}
	restoreStep(t, r)
	restoreStep(t, r)
	item := readRestoreRun(t, c).Status.Items[0]
	return endedItem{item.Phase, item.Reason, item.Message}
}

// An item a run refuses records why in its reason, a typed value that no
// one has to read from the message, and its message is the sentence it was
// before the reason existed. Each case goes through one of the start checks
// that return a typed refusal: startItem's claimGone, both startRefusals,
// sourceSettingsFor, volumeAffinity, the quiesce pre-check's foreign
// source, startItem's hibernated Cluster, checkVolume's repositoryFor and
// restoreDatabase's clustersRestoredElsewhere.
func TestARefusedItemRecordsItsReason(t *testing.T) {
	quiescing := backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true })
	unbound := claim()
	unbound.Spec.VolumeName, unbound.Status.Phase = "", corev1.ClaimPending
	zeroRetention := claim()
	zeroRetention.Annotations[backupv1alpha1.AnnotationRetainLast] = "0"
	foreign := idleSource()
	foreign.Labels = nil
	sleeping := cluster(func(u *unstructured.Unstructured) {
		annotations := u.GetAnnotations()
		annotations[hibernationAnnotation] = "on"
		u.SetAnnotations(annotations)
	})
	inPlace := func() *backupv1alpha1.RestoreRun {
		return restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN })
	}
	noVolumeRestore := "claim " + claimN + " has no VolumeRestore " + claimN + " to name its repository"

	for _, tc := range []struct {
		name string
		run  func(t *testing.T) endedItem
		want endedItem
	}{
		{"BackupRun, claim deleted after plan, no quiesce", func(t *testing.T) endedItem {
			return backupItemAfter(t, backupRun(), deleteClaim, claim(), volume(), volumeRestore(), repository())
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClaimMissing,
			"the claim " + claimN + " no longer exists"}},

		{"BackupRun with quiesce, claim deleted after plan", func(t *testing.T) endedItem {
			return backupItemAfter(t, quiescing, deleteClaim, claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClaimMissing,
			"the claim " + claimN + " no longer exists"}},

		{"BackupRun with quiesce, VolumeRestore deleted after plan", func(t *testing.T) endedItem {
			return backupItemAfter(t, quiescing, deleteVolumeRestore, claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonVolumeRestoreMissing, noVolumeRestore}},

		{"BackupRun, claim not bound", func(t *testing.T) endedItem {
			return backupItemAfter(t, backupRun(), nil, unbound, volumeRestore(), repository())
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClaimNotBound,
			"claim " + claimN + " is not bound to a volume yet"}},

		{"BackupRun, retention annotation 0", func(t *testing.T) endedItem {
			return backupItemAfter(t, backupRun(), nil, zeroRetention, volume(), volumeRestore(), repository())
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonSettingsInvalid,
			"claim " + claimN + " has " + backupv1alpha1.AnnotationRetainLast + ` "0", which is not a positive count`}},

		{"BackupRun with quiesce, ReplicationSource without the managed-by label", func(t *testing.T) endedItem {
			return backupItemAfter(t, quiescing, nil, claim(), volume(), volumeRestore(), repository(), foreign, deployment(), kustomization(false))
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonSourceNotManaged,
			"the ReplicationSource " + claimN + " exists and was not written by backup-controller; remove it so the controller can write its own"}},

		{"BackupRun, hibernated Cluster", func(t *testing.T) endedItem {
			return backupItemAfter(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), nil, sleeping)
		}, endedItem{backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonClusterHibernated,
			"the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"}},

		{"in-place RestoreRun, VolumeRestore missing at plan", func(t *testing.T) endedItem {
			return restoreItemAfter(t, inPlace(), nil, claim(), repository())
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonVolumeRestoreMissing, noVolumeRestore}},

		{"in-place RestoreRun, VolumeRestore deleted after plan", func(t *testing.T) endedItem {
			return restoreItemAfter(t, inPlace(), deleteVolumeRestore, claim(), volumeRestore(), repository())
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonVolumeRestoreMissing, noVolumeRestore + nothingWrittenEnd}},

		{"in-place RestoreRun with quiesce, claim being deleted after plan", func(t *testing.T) endedItem {
			return restoreItemAfter(t, quiescedRestore(), func(t *testing.T, c client.Client) {
				held := &corev1.PersistentVolumeClaim{}
				get(t, c, ns, claimN, held)
				held.Finalizers = append(held.Finalizers, "kubernetes.io/pvc-protection")
				if err := c.Update(context.Background(), held); err != nil {
					t.Fatal(err)
				}
				if err := c.Delete(context.Background(), held); err != nil {
					t.Fatal(err)
				}
			}, claim(), volumeRestore(), repository(), deployment(), kustomization(false))
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClaimDeleting,
			"claim " + claimN + " is being deleted" + nothingWrittenEnd}},

		{"RestoreRun of a Cluster another RestoreRun holds, found at restoreDatabase", func(t *testing.T) endedItem {
			started := metav1.NewTime(frozen)
			run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
				r.Spec.Database = pgN
				r.Finalizers = []string{Finalizer}
				r.Status.Phase, r.Status.StartedAt = backupv1alpha1.RunPhaseRunning, &started
				r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: backupv1alpha1.ItemKindCluster, Name: pgN, Phase: backupv1alpha1.ItemPending, BaseBackup: saturday.ID}}
			}, asOf("2026-09-22T00:00:00Z"))
			other := restoreRun(func(r *backupv1alpha1.RestoreRun) {
				r.Name, r.UID = "other-restore", "9b7d4e21-0000-4000-8000-000000000099"
				r.Spec.Database = pgN
				r.Status.Phase, r.Status.StartedAt = backupv1alpha1.RunPhaseRunning, &started
				r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: backupv1alpha1.ItemKindCluster, Name: pgN, Phase: backupv1alpha1.ItemDeleted}}
			})
			r, c := restoreReconciler(t, prober{saturday}, run, other, cluster(), objectStore(), storeSecret())
			restoreStep(t, r)
			item := readRestoreRun(t, c).Status.Items[0]
			return endedItem{item.Phase, item.Reason, item.Message}
		}, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClusterRestoredElsewhere,
			"RestoreRun other-restore is restoring Cluster " + pgN + ". Create this RestoreRun again once that run has finished. The run deleted nothing"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := tc.run(t); got != tc.want {
				t.Errorf("item = %+v\nwant   %+v", got, tc.want)
			}
		})
	}
}
