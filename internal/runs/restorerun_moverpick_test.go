package runs

import (
	"context"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// The tests in this file check that a RestoreRun's checks refuse a snapshot
// that VolSync's mover can't be pinned to. The run hands the mover the
// snapshot's time in whole seconds as restoreAsOf, and the mover restores the
// snapshot it picks for that second (restic.MoverPick), which is not always
// the one the checks selected.

// recordedSnapshots caches the snapshot lists read from the recorded
// repositories, keyed by directory, so each repository's key is derived once.
var recordedSnapshots sync.Map

// recordedRepository returns the snapshots of one repository that
// hack/fixtures/restic.sh recorded with the restic version VolSync's mover
// ships, read through the restic package as the controller reads a real
// repository.
//
// Parameters:
//   - kind names the fixture, such as same-second.
//
// It fails the test when the repository can't be opened or listed.
func recordedRepository(t *testing.T, kind string) snapshots {
	t.Helper()
	dir := filepath.Join("..", "restic", "testdata", "recorded", "restic-"+versions.Of(t, "restic-mover"), kind, "repo")
	if list, ok := recordedSnapshots.Load(dir); ok {
		return list.(snapshots)
	}
	repo, err := restic.Open(context.Background(), restic.DirStore(dir), "backup")
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	list, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	recordedSnapshots.Store(dir, snapshots(list))
	return list
}

// moverSnapshot returns a snapshot the way a VolSync mover writes one: host
// volsync and the one path /data, at the given time, with the given tags.
func moverSnapshot(short string, at time.Time, tags ...string) restic.Snapshot {
	return restic.Snapshot{ID: short + "00000000", Time: at, Hostname: "volsync", Paths: []string{"/data"}, Tags: tags}
}

// expectRefused checks that the run back-to-monday ended at its checks as
// Failed with reason NoBackupInReach, that the item's message holds every
// string in want, and that the run created no ReplicationDestination and no
// claim named scratch.
func expectRefused(t *testing.T, r *RestoreRunReconciler, want ...string) {
	t.Helper()
	c := r.Client
	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed, NoBackupInReach", run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	message := readyMessage(run.Status.Conditions)
	if len(run.Status.Items) > 0 {
		message = run.Status.Items[0].Message
	}
	for _, w := range want {
		if !strings.Contains(message, w) {
			t.Errorf("message = %q, want it to hold %q", message, w)
		}
	}
	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil || len(destinations.Items) != 0 {
		t.Errorf("destinations = %v (%v), want none", destinations.Items, err)
	}
	scratch := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "scratch"}, scratch); !apierrors.IsNotFound(err) {
		t.Errorf("claim scratch: %v, want it never created", err)
	}
}

// A restore whose previous lands on the earlier of two snapshots in one
// second fails at its checks and names the snapshot the mover would restore
// instead. The snapshots are the recorded same-second repository, where
// VolSync's mover, given 21:22:16, restored 2d35d9a8, the later one.
func TestARestoreRefusesASnapshotSharingItsSecond(t *testing.T) {
	for _, into := range []bool{false, true} {
		name := "in place"
		if into {
			name = "into from a repository"
		}
		t.Run(name, func(t *testing.T) {
			shape := func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }
			if into {
				shape = fromRepository
			}
			r, _ := restoreReconciler(t, nil, restoreRun(shape, asOf("2026-09-25T21:22:16Z"), previousOne),
				claim(), volumeRestore(), repository())
			r.Snapshots = recordedRepository(t, "same-second")

			restoreStep(t, r)

			expectRefused(t, r, "snapshot 763f53b1 (2026-09-25T21:22:16Z) shares its second with snapshot 2d35d9a8",
				"so it would restore 2d35d9a8", "Choose 2d35d9a8 or a snapshot in another second")
		})
	}
}

// A restore of a snapshot that is alone in its second, or the last in it,
// passes the checks and pins the mover to that second, from the same
// recorded repository.
func TestARestoreOfTheLastSnapshotInItsSecondGoesAhead(t *testing.T) {
	for _, c := range []struct {
		name     string
		mutate   []func(*backupv1alpha1.RestoreRun)
		snapshot string
		pin      string
	}{
		{"the last in its second", []func(*backupv1alpha1.RestoreRun){asOf("2026-09-25T21:22:16Z")}, "2d35d9a8", "2026-09-25T21:22:16Z"},
		{"alone in its second", []func(*backupv1alpha1.RestoreRun){asOf("2026-09-25T21:22:15Z")}, "2c2a4ea2", "2026-09-25T21:22:15Z"},
		{"the newest", nil, "2d35d9a8", "2026-09-25T21:22:16Z"},
	} {
		t.Run(c.name, func(t *testing.T) {
			mutate := append([]func(*backupv1alpha1.RestoreRun){func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }}, c.mutate...)
			r, cl := restoreReconciler(t, nil, restoreRun(mutate...), claim(), volumeRestore(), repository())
			r.Snapshots = recordedRepository(t, "same-second")
			restoreStep(t, r) // plan
			restoreStep(t, r) // restore

			run := readRestoreRun(t, cl)
			if run.Status.Phase != backupv1alpha1.RunPhaseRunning || run.Status.Items[0].Snapshot != c.snapshot {
				t.Fatalf("phase = %q, item = %+v; want Running with %s", run.Status.Phase, run.Status.Items[0], c.snapshot)
			}
			rd := &volsyncv1alpha1.ReplicationDestination{}
			get(t, cl, ns, run.Status.Items[0].Destination, rd)
			if rd.Spec.Restic.RestoreAsOf == nil || *rd.Spec.Restic.RestoreAsOf != c.pin {
				t.Errorf("destination restoreAsOf = %v, want %s", rd.Spec.Restic.RestoreAsOf, c.pin)
			}
		})
	}
}

// A synced restore refuses a quiesced snapshot when an untagged one was taken
// later in the same second. The run picks among quiesced snapshots, and the
// mover among all of them, so the mover would restore the untagged one. The
// Cluster is left alone.
func TestASyncedRestoreRefusesAQuiescedSnapshotAnUntaggedOneShadows(t *testing.T) {
	quiesced := moverSnapshot("c0ffee00", time.Date(2026, 9, 21, 3, 0, 5, 200e6, time.UTC), restic.QuiescedTag)
	untagged := moverSnapshot("7a11ce00", time.Date(2026, 9, 21, 3, 0, 5, 800e6, time.UTC))
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All, r.Spec.SyncDatabaseToVolume = true, true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())
	r.Snapshots = snapshots{moverSnapshot("2edf5bab", sunday.Time), quiesced, untagged}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot c0ffee00 (2026-09-21T03:00:05Z) shares its second with snapshot 7a11ce00, which is not tagged quiesced",
		"so it would restore 7a11ce00", "restoreAsOf before 2026-09-21T03:00:05Z")
	if _, ok := getUnstructured(t, c, ClusterGVK, ns, pgN); !ok {
		t.Error("the Cluster was deleted by a run that refused its volume")
	}
}

// A restore refuses a snapshot whose time another snapshot shares to the
// nanosecond. restic lists such snapshots in no set order, so the mover may
// restore either.
func TestARestoreRefusesASnapshotWithATwin(t *testing.T) {
	at := time.Date(2026, 9, 21, 5, 0, 2, 123456789, time.UTC)
	r, _ := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{moverSnapshot("2edf5bab", sunday.Time), moverSnapshot("aaaaaaaa", at), moverSnapshot("bbbbbbbb", at)}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot bbbbbbbb (2026-09-21T05:00:02Z) has the same time as snapshot aaaaaaaa", "may restore either",
		"Choose a snapshot in another second")
}

// A restore refuses a snapshot shadowed by two later ones in its second that
// share a time: the mover restores one of those two, and which is not set.
func TestARestoreRefusesASnapshotShadowedByTwins(t *testing.T) {
	second := time.Date(2026, 9, 21, 5, 0, 2, 0, time.UTC)
	two := int32(2)
	r, _ := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Previous = claimN, &two }),
		claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{moverSnapshot("11111111", second.Add(100e6)), moverSnapshot("aaaaaaaa", second.Add(900e6)), moverSnapshot("bbbbbbbb", second.Add(900e6))}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot 11111111 (2026-09-21T05:00:02Z) shares its second with snapshot aaaaaaaa and snapshot bbbbbbbb",
		"may restore either", "Choose a snapshot in another second")
}

// A restore refuses a snapshot with no path containing /data, which the
// mover's listing passes over. The mover would restore an older snapshot, or
// nothing.
func TestARestoreRefusesASnapshotTheMoverDoesNotList(t *testing.T) {
	other := moverSnapshot("5e1f0000", monday.Time)
	other.Paths = []string{"/srv/notes"}
	r, _ := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{moverSnapshot("2edf5bab", sunday.Time), other}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot 5e1f0000 (2026-09-21T05:00:02Z) has no path containing /data (its paths: /srv/notes)",
		"cannot restore 5e1f0000", "Choose a snapshot a VolSync mover took")
}

// A restore from a repository that holds a snapshot with /data after its
// first path is refused. The mover's listing reads that line as a snapshot
// of its own, whose time depends on the day the mover runs, so no pick can be
// predicted.
func TestARestoreRefusesARepositoryTheMoverMisreads(t *testing.T) {
	odd := moverSnapshot("0dd00000", sunday.Time.Add(-time.Hour))
	odd.Paths = []string{"/srv", "/data"}
	r, _ := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{odd, moverSnapshot("2edf5bab", sunday.Time), moverSnapshot("6e473100", monday.Time)}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot 0dd00000 lists /data after its first path", "cannot tell which snapshot")
}
