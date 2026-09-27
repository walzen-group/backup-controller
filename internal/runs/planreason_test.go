package runs

import (
	"context"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// snapshotsBySecret is a SnapshotLister that holds one list of snapshots
// for each repository Secret, by the Secret's name.
type snapshotsBySecret map[string][]restic.Snapshot

// Snapshots returns the list of the repository Secret, or none for a
// Secret the map does not name.
func (s snapshotsBySecret) Snapshots(_ context.Context, secret *corev1.Secret) ([]restic.Snapshot, error) {
	return s[secret.Name], nil
}

// plannedItems runs the plan pass of a RestoreRun over the given objects,
// with the given snapshots and base backups, and returns every item by its
// name.
func plannedItems(t *testing.T, lister SnapshotLister, backups prober, run *backupv1alpha1.RestoreRun, objects ...client.Object) map[string]endedItem {
	t.Helper()
	r, c := restoreReconciler(t, backups, append([]client.Object{run}, objects...)...)
	r.Snapshots = lister
	restoreStep(t, r)
	items := map[string]endedItem{}
	for _, item := range readRestoreRun(t, c).Status.Items {
		items[item.Name] = endedItem{item.Phase, item.Reason, item.Message}
	}
	return items
}

// An item that plan finds out of reach records why in its reason, and its
// message is the sentence it was before the reason existed. Each case goes
// through one check that plan runs: selectSnapshot (no snapshot, no
// snapshot before the moment, previous past the oldest, no quiesced
// snapshot), the synced moment of two volumes and of a Cluster, the
// repository Secret, items and checkDatabase for a Cluster that archives
// nowhere, checkDatabase for a missing Cluster, a missing ObjectStore and a
// store with no base backup in reach. The items plan leaves alone because
// another item failed record OtherItemFailed.
func TestPlanRecordsWhyAnItemIsOutOfReach(t *testing.T) {
	t.Parallel()
	inPlace := func(mutate ...func(*backupv1alpha1.RestoreRun)) *backupv1alpha1.RestoreRun {
		return restoreRun(append([]func(*backupv1alpha1.RestoreRun){func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }}, mutate...)...)
	}
	synced := restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All, r.Spec.SyncDatabaseToVolume = true, true })
	all := restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true })
	database := restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN })
	previous := func(r *backupv1alpha1.RestoreRun) {
		two := int32(2)
		r.Spec.Previous = &two
	}
	later := quiet
	later.ID, later.Time = fullID("c0ffee01"), quiet.Time.Add(time.Hour)
	twoMoments := snapshotsBySecret{repoN: {later}, "notes-restic-cache": {quiet}}
	mondayOnly := snapshotsBySecret{repoN: {sunday, monday}}
	leftAlone := "left alone because another item has no backup the run's moment reaches"

	type want struct {
		name string
		item endedItem
	}
	for _, tc := range []struct {
		name    string
		lister  SnapshotLister
		backups prober
		run     *backupv1alpha1.RestoreRun
		objects []client.Object
		want    []want
	}{
		{"no snapshot", snapshots{}, nil, inPlace(), []client.Object{claim(), volumeRestore(), repository()},
			[]want{{claimN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
				"the repository holds no snapshot"}}}},

		{"no snapshot at or before the moment", mondayOnly, nil, inPlace(asOf("2026-09-01T00:00:00Z")),
			[]client.Object{claim(), volumeRestore(), repository()},
			[]want{{claimN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
				"no snapshot at or before 2026-09-01T00:00:00Z; the oldest, 2edf5bab, is from 2026-09-20T05:00:02Z"}}}},

		{"previous past the oldest", mondayOnly, nil, inPlace(previous), []client.Object{claim(), volumeRestore(), repository()},
			[]want{{claimN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
				"previous 2 reaches past the oldest snapshot"}}}},

		{"no quiesced snapshot", mondayOnly, prober{saturday}, synced,
			[]client.Object{claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret()},
			[]want{
				{claimN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
					"the repository holds no snapshot tagged " + restic.QuiescedTag + "; only a BackupRun that stopped the workloads writes one"}},
				{pgN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
					"no volume selected a quiesced snapshot, so there is no moment to recover the database to"}},
			}},

		{"two volumes at different quiesced moments", twoMoments, nil, synced,
			append([]client.Object{claim(), volumeRestore(), repository()}, cacheClaim()...),
			[]want{{claimN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
				"its quiesced snapshot c0ffee01 is from 2026-09-21T04:00:05Z and another volume's is from 2026-09-21T03:00:05Z; a synced restore needs one moment for every volume"}}}},

		{"repository Secret gone", mondayOnly, nil, inPlace(), []client.Object{claim(), volumeRestore()},
			[]want{{claimN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonRepositorySecretMissing,
				"no repository Secret " + repoN + " in this namespace"}}}},

		{"a Cluster that archives nowhere, found by items", mondayOnly, prober{saturday}, all,
			[]client.Object{claim(), volumeRestore(), repository(), cluster(archivingNowhere)},
			[]want{{pgN, endedItem{backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonClusterArchivesNowhere,
				"the Cluster archives nowhere, so it has no backup to restore"}}}},

		{"a Cluster that archives nowhere, found by checkDatabase", mondayOnly, prober{saturday}, database,
			[]client.Object{cluster(archivingNowhere)},
			[]want{{pgN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClusterArchivesNowhere,
				"the Cluster archives nowhere, so it has no backup to restore"}}}},

		{"a missing Cluster", mondayOnly, prober{saturday}, database, nil,
			[]want{{pgN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonClusterMissing,
				"no Cluster " + pgN + " in this namespace"}}}},

		{"a missing ObjectStore", mondayOnly, prober{saturday}, database, []client.Object{cluster(), storeSecret()},
			[]want{{pgN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
				"read ObjectStore " + ns + "/" + pgN + "-store: objectstores.barmancloud.cnpg.io \"" + pgN + "-store\" not found"}}}},

		{"a store with no base backup", mondayOnly, nil, database, []client.Object{cluster(), objectStore(), storeSecret()},
			[]want{{pgN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
				"cnpg/" + ns + "/" + pgN + "/base/ holds no completed base backup; deleting the Cluster would bring it back empty"}}}},

		{"no base backup finished by the moment", mondayOnly, prober{saturday},
			restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }, asOf("2026-09-01T00:00:00Z")),
			[]client.Object{cluster(), objectStore(), storeSecret()},
			[]want{{pgN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach,
				"no base backup finished by 2026-09-01T00:00:00Z; the oldest, 20260919T030000, finished at 2026-09-19T03:00:40Z"}}}},

		{"the other items", snapshots{}, prober{saturday}, all,
			[]client.Object{claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret()},
			[]want{
				{claimN, endedItem{backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonNoBackupInReach, "the repository holds no snapshot"}},
				{pgN, endedItem{backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonOtherItemFailed, leftAlone}},
			}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			items := plannedItems(t, tc.lister, tc.backups, tc.run, tc.objects...)
			for _, w := range tc.want {
				if got := items[w.name]; got != w.item {
					t.Errorf("item %s = %+v\nwant %+v", w.name, got, w.item)
				}
			}
		})
	}
}
