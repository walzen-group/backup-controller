package runs

import (
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// A run whose volume restore fails leaves its Pending Cluster running. The
// Cluster item is Skipped with reason OtherItemFailed, and its message is
// the sentence it was before the reason existed. The volume fails at its
// start checks: its VolumeRestore is deleted after plan.
func TestAVolumeFailureSkipsTheClusterWithOtherItemFailed(t *testing.T) {
	t.Parallel()
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true })
	r, c := restoreReconciler(t, prober{saturday},
		run, claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())
	restoreStep(t, r) // plan
	deleteVolumeRestore(t, c)
	restoreStep(t, r)

	items := map[string]endedItem{}
	for _, item := range readRestoreRun(t, c).Status.Items {
		items[item.Kind] = endedItem{item.Phase, item.Reason, item.Message}
	}
	if got := items[backupv1alpha1.ItemKindClaim]; got.phase != backupv1alpha1.ItemFailed {
		t.Fatalf("volume item = %+v, want Failed", got)
	}
	want := endedItem{backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonOtherItemFailed, "left running because a volume restore failed"}
	if got := items[backupv1alpha1.ItemKindCluster]; got != want {
		t.Errorf("Cluster item = %+v\nwant %+v", got, want)
	}
}
