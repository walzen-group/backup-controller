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
	"github.com/walzen-group/backup-controller/internal/restorejob"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
)

// The tests in this file check that an into restore's checks refuse a
// snapshot that VolSync's mover can't be pinned to. The run hands the mover
// the snapshot's time in whole seconds as restoreAsOf, and the mover restores
// the snapshot it picks for that second (restic.MoverPick), which is not
// always the one the checks selected. An in-place restore restores by full ID
// through its own restore Job and refuses none of these.

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
	return restic.Snapshot{ID: fullID(short), Time: at, Hostname: "volsync", Paths: []string{"/data"}, Tags: tags}
}

// expectRefused checks that the run back-to-monday ended at its checks as
// Failed with reason NoBackupInReach, that the item's message holds every
// string in want, and that the run created no mover and no claim named
// scratch.
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
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("movers = %v, want none", names)
	}
	scratch := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "scratch"}, scratch); !apierrors.IsNotFound(err) {
		t.Errorf("claim scratch: %v, want it never created", err)
	}
}

// An into restore whose previous lands on the earlier of two snapshots in one
// second fails at its checks and names the snapshot the mover would restore
// instead. The snapshots are the recorded same-second repository, where
// VolSync's mover, given 21:22:16, restored 2d35d9a8, the later one. An
// in-place restore of the same snapshot goes ahead: its restore Job restores
// 763f53b1 by its full ID.
func TestARestoreRefusesASnapshotSharingItsSecond(t *testing.T) {
	r, _ := restoreReconciler(t, nil, restoreRun(fromRepository, asOf("2026-09-25T21:22:16Z"), previousOne),
		claim(), volumeRestore(), repository())
	r.Snapshots = recordedRepository(t, "same-second")

	restoreStep(t, r)

	expectRefused(t, r, "snapshot 763f53b1 (2026-09-25T21:22:16Z) shares its second with snapshot 2d35d9a8",
		"so it would restore 2d35d9a8", "Choose 2d35d9a8 or a snapshot in another second")

	inPlace, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN },
		asOf("2026-09-25T21:22:16Z"), previousOne), claim(), volumeRestore(), repository())
	inPlace.Snapshots = recordedRepository(t, "same-second")
	restoreStep(t, inPlace) // plan
	restoreStep(t, inPlace) // restore
	if job := itemJob(t, c); !strings.HasPrefix(job.Annotations[restorejob.AnnotationSnapshotID], "763f53b1") {
		t.Errorf("in place: restore Job snapshot = %s, want 763f53b1's full ID", job.Annotations[restorejob.AnnotationSnapshotID])
	}
}

// An into restore of a snapshot that is alone in its second, or the last in
// it, passes the checks and pins the mover to that second, from the same
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
			r, cl := restoreReconciler(t, nil, restoreRun(append([]func(*backupv1alpha1.RestoreRun){fromRepository}, c.mutate...)...), repository())
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

// A synced in-place restore takes a quiesced snapshot even when an untagged
// one was taken later in the same second. Its restore Job restores the
// quiesced snapshot by its full ID, so the untagged one can't take its
// place, as it could when VolSync's mover picked by the second.
func TestASyncedRestoreRestoresAQuiescedSnapshotAnUntaggedOneShares(t *testing.T) {
	quiesced := moverSnapshot("c0ffee00", time.Date(2026, 9, 21, 3, 0, 5, 200e6, time.UTC), restic.QuiescedTag)
	untagged := moverSnapshot("7a11ce00", time.Date(2026, 9, 21, 3, 0, 5, 800e6, time.UTC))
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All, r.Spec.SyncDatabaseToVolume = true, true }),
		claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret())
	r.Snapshots = snapshots{moverSnapshot("2edf5bab", sunday.Time), quiesced, untagged}

	restoreStep(t, r) // plan
	restoreStep(t, r) // restore the volume

	if got := itemJob(t, c).Annotations[restorejob.AnnotationSnapshotID]; got != quiesced.ID {
		t.Errorf("restore Job snapshot = %s, want the quiesced %s", got, quiesced.ID)
	}
}

// An into restore refuses a snapshot whose time another snapshot shares to
// the nanosecond. restic lists such snapshots in no set order, so the mover
// may restore either.
func TestARestoreRefusesASnapshotWithATwin(t *testing.T) {
	at := time.Date(2026, 9, 21, 5, 0, 2, 123456789, time.UTC)
	r, _ := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	r.Snapshots = snapshots{moverSnapshot("2edf5bab", sunday.Time), moverSnapshot("aaaaaaaa", at), moverSnapshot("bbbbbbbb", at)}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot bbbbbbbb (2026-09-21T05:00:02Z) has the same time as snapshot aaaaaaaa", "may restore either",
		"Choose a snapshot in another second")
}

// An into restore refuses a snapshot shadowed by two later ones in its
// second that share a time: the mover restores one of those two, and which
// is not set.
func TestARestoreRefusesASnapshotShadowedByTwins(t *testing.T) {
	second := time.Date(2026, 9, 21, 5, 0, 2, 0, time.UTC)
	two := int32(2)
	r, _ := restoreReconciler(t, nil, restoreRun(fromRepository, func(r *backupv1alpha1.RestoreRun) { r.Spec.Previous = &two }), repository())
	r.Snapshots = snapshots{moverSnapshot("11111111", second.Add(100e6)), moverSnapshot("aaaaaaaa", second.Add(900e6)), moverSnapshot("bbbbbbbb", second.Add(900e6))}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot 11111111 (2026-09-21T05:00:02Z) shares its second with snapshot aaaaaaaa and snapshot bbbbbbbb",
		"may restore either", "Choose a snapshot in another second")
}

// An into restore from a repository that holds a snapshot with /data after
// its first path is refused. The mover's listing reads that line as a
// snapshot of its own, whose time depends on the day the mover runs, so no
// pick can be predicted.
func TestARestoreRefusesARepositoryTheMoverMisreads(t *testing.T) {
	odd := moverSnapshot("0dd00000", sunday.Time.Add(-time.Hour))
	odd.Paths = []string{"/srv", "/data"}
	r, _ := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	r.Snapshots = snapshots{odd, moverSnapshot("2edf5bab", sunday.Time), moverSnapshot("6e473100", monday.Time)}

	restoreStep(t, r)

	expectRefused(t, r, "snapshot 0dd00000 lists /data after its first path", "cannot tell which snapshot")
}
