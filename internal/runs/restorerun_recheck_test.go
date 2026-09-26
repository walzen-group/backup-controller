package runs

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file check what a RestoreRun does about a repository that
// changes between its checks and its mover: it selects its snapshot only once
// a backup of the repository has finished (designs/restorerun.md C3), and it
// lists the repository again right before it creates anything, failing the
// item when the snapshot it selected was forgotten, retimed or shadowed since
// (C4, R3).

// restoreShapes are the three shapes of a volume restore: in place, into a
// new claim from a repository, and into a new claim from a claim's backups.
// into is the claim an into restore creates. Each writes through a
// ReplicationDestination the run creates.
var restoreShapes = []struct {
	name   string
	mutate func(*backupv1alpha1.RestoreRun)
	into   string
}{
	{"in place", func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, ""},
	{"into from a repository", fromRepository, "scratch"},
	{"into from a claim", func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "scratch" }, "scratch"},
}

// repositoryCopy is a restic.Lister over a copy of a recorded restic
// repository in a test's temporary directory. A test changes the copy the
// way a backup's mover or the controller's retime changes the real one, and
// the run lists what is there at each pass.
type repositoryCopy struct {
	dir  string
	repo *restic.Repository
}

// Snapshots lists the copy, oldest first, whatever Secret it is asked about.
func (r *repositoryCopy) Snapshots(ctx context.Context, _ *corev1.Secret) ([]restic.Snapshot, error) {
	return r.repo.Snapshots(ctx)
}

// forget removes the snapshot whose ID starts with short from the copy, as
// restic forget does: it deletes the snapshot's file and leaves the data.
func (r *repositoryCopy) forget(t *testing.T, short string) {
	t.Helper()
	list, err := r.repo.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	s, ok := restic.ByShortID(list, short)
	if !ok {
		t.Fatalf("no snapshot %s to forget", short)
	}
	if err := restic.DirStore(r.dir).Remove(context.Background(), path.Join("snapshots", s.ID)); err != nil {
		t.Fatal(err)
	}
}

// copyRecorded copies the recorded repository of the given kind, written by
// the restic version VolSync's mover ships, and opens the copy.
func copyRecorded(t *testing.T, kind string) *repositoryCopy {
	t.Helper()
	src := filepath.Join("..", "restic", "testdata", "recorded", "restic-"+versions.Of(t, "restic-mover"), kind, "repo")
	dir := filepath.Join(t.TempDir(), "repo")
	if err := os.CopyFS(dir, os.DirFS(src)); err != nil {
		t.Fatalf("copy %s: %v", src, err)
	}
	repo, err := restic.Open(context.Background(), restic.DirStore(dir), "backup")
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	return &repositoryCopy{dir: dir, repo: repo}
}

// The recorded timed repository holds three snapshots VolSync's mover took,
// each alone in its second; newestTimed is the newest.
const newestTimed = "f11fce99"

// expectNothingCreated checks that the run created no ReplicationDestination
// and, for an into restore, neither the claim nor a VolumeRestore named into.
func expectNothingCreated(t *testing.T, c client.Client, into string) {
	t.Helper()
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want none", names)
	}
	if into == "" {
		return
	}
	key := types.NamespacedName{Namespace: ns, Name: into}
	if err := c.Get(context.Background(), key, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Errorf("claim %s: %v, want it never created", into, err)
	}
	if err := c.Get(context.Background(), key, &backupv1alpha1.VolumeRestore{}); !apierrors.IsNotFound(err) {
		t.Errorf("VolumeRestore %s: %v, want it never created", into, err)
	}
}

// expectItemFailed checks that the run ended Failed with its first item
// Failed, and that the item's message holds every string in want.
func expectItemFailed(t *testing.T, c client.Client, want ...string) {
	t.Helper()
	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || len(run.Status.Items) == 0 || run.Status.Items[0].Phase != backupv1alpha1.ItemFailed {
		t.Fatalf("phase = %q, items = %+v (%s); want Failed with the item Failed", run.Status.Phase, run.Status.Items, readyMessage(run.Status.Conditions))
	}
	for _, w := range want {
		if !strings.Contains(run.Status.Items[0].Message, w) {
			t.Errorf("item message = %q, want it to hold %q", run.Status.Items[0].Message, w)
		}
	}
}

// A restore whose selected snapshot a backup's retention forgot between the
// checks and the create fails the item with that cause and creates nothing.
// Pinned to the snapshot's second, the mover would have restored the next
// older snapshot, or nothing, and reported success.
func TestARestoreWhoseSnapshotWasPrunedFailsBeforeWriting(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			repo := copyRecorded(t, "timed")
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate), claim(), volumeRestore(), repository())
			r.Snapshots = repo
			restoreStep(t, r) // plan
			if got := readRestoreRun(t, c).Status.Items[0].Snapshot; got != newestTimed {
				t.Fatalf("the checks selected %s, want %s", got, newestTimed)
			}

			repo.forget(t, newestTimed)
			restoreStep(t, r)

			expectItemFailed(t, c, "snapshot "+newestTimed+" (2026-09-25T21:21:02Z), which the checks selected, is no longer in the repository",
				"restic forget", "Create a new RestoreRun")
			expectNothingCreated(t, c, shape.into)
		})
	}
}

// A restore whose selected snapshot a quiesced backup retimed between the
// checks and the create fails the item and names the rewritten copy. The
// copy has another ID and time, and the run recorded no tree to prove it
// holds the same data, so it does not follow the copy (design R3).
func TestARestoreWhoseSnapshotWasRetimedFailsBeforeWriting(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			repo := copyRecorded(t, "timed")
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate), claim(), volumeRestore(), repository())
			r.Snapshots = repo
			restoreStep(t, r) // plan

			rewritten, err := repo.repo.Retime(context.Background(), newestTimed, time.Date(2026, 9, 25, 21, 21, 40, 0, time.UTC), restic.QuiescedTag)
			if err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r)

			expectItemFailed(t, c, "snapshot "+newestTimed+", which the checks selected, was rewritten as "+rewritten.ShortID()+
				" at 2026-09-25T21:21:40Z by a quiesced backup after the checks", "Create a new RestoreRun")
			expectNothingCreated(t, c, shape.into)
		})
	}
}

// A restore whose selected snapshot another one joined in its second after
// the checks fails the item: pinned to that second, the mover would restore
// the later one.
func TestARestoreWhoseSnapshotWasShadowedFailsBeforeWriting(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate), claim(), volumeRestore(), repository())
			restoreStep(t, r) // plan, which selects monday

			later := moverSnapshot("7a11ce00", monday.Time.Add(500*time.Millisecond))
			r.Snapshots = snapshots{sunday, monday, later}
			restoreStep(t, r)

			expectItemFailed(t, c, "the repository changed after the checks",
				"snapshot 6e473100 (2026-09-21T05:00:02Z) shares its second with snapshot 7a11ce00", "so it would restore 7a11ce00",
				"Create a new RestoreRun")
			expectNothingCreated(t, c, shape.into)
		})
	}
}

// A restore whose repository did not change since the checks creates its
// destination pinned to the selected snapshot, from the recorded repository.
// An into restore creates no VolumeRestore.
func TestARestoreWhoseSnapshotIsStillThereGoesAhead(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate), claim(), volumeRestore(), repository())
			r.Snapshots = copyRecorded(t, "timed")
			restoreStep(t, r) // plan
			restoreStep(t, r) // create

			run := readRestoreRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseRunning || run.Status.Items[0].Phase != backupv1alpha1.ItemRunning {
				t.Fatalf("phase = %q, item = %+v; want Running", run.Status.Phase, run.Status.Items[0])
			}
			want := "2026-09-25T21:21:02Z"
			if shape.into != "" {
				key := types.NamespacedName{Namespace: ns, Name: shape.into}
				if err := c.Get(context.Background(), key, &backupv1alpha1.VolumeRestore{}); !apierrors.IsNotFound(err) {
					t.Errorf("VolumeRestore %s: %v, want none created", shape.into, err)
				}
			}
			rd := &volsyncv1alpha1.ReplicationDestination{}
			get(t, c, ns, run.Status.Items[0].Destination, rd)
			if rd.Spec.Restic.RestoreAsOf == nil || *rd.Spec.Restic.RestoreAsOf != want {
				t.Errorf("destination restoreAsOf = %v, want %s", rd.Spec.Restic.RestoreAsOf, want)
			}
		})
	}
}

// A pass that created the item's destination and lost the status write
// leaves the item Pending. When the next pass finds the snapshot gone, it
// fails the item, says the destination from the lost pass may have written
// part of the claim, and deletes that destination: an item that named no
// destination would leave its mover running with nothing tracking it.
func TestARecheckAfterALostWriteDeletesTheDestinationItCreated(t *testing.T) {
	repo := copyRecorded(t, "timed")
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }), claim(), volumeRestore(), repository())
	r.Snapshots = repo
	restoreStep(t, r) // plan

	r.Client = loseNextStatusWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose status write was lost succeeded, want the error returned")
	}
	if names := destinations(t, c); len(names) != 1 {
		t.Fatalf("destinations = %v, want the one the lost pass created", names)
	}
	repo.forget(t, newestTimed)
	restoreStep(t, r)
	restoreStep(t, r) // the pass after the destination's delete finds its mover gone

	expectItemFailed(t, c, "is no longer in the repository", "ReplicationDestination "+destinationName(restoreUID, 0))
	expectNothingCreated(t, c, "")
}

// A pass that created the item's destination and lost the status write
// leaves the item Pending. When the repository Secret, or the claim's
// VolumeRestore that names it, is gone on the next pass, the run fails the
// item, says the destination from the lost pass may have written part of the
// claim, and deletes that destination. Before, the item named no destination
// and said nothing was written, or nothing about the claim, so the mover ran
// on with nothing tracking it (UF1).
func TestARefusalAfterALostWriteDeletesTheDestinationItCreated(t *testing.T) {
	for name, tc := range map[string]struct {
		gone client.Object
		want string
	}{
		"repository Secret": {gone: repository(), want: "repository Secret " + repoN},
		"VolumeRestore":     {gone: volumeRestore(), want: "VolumeRestore"},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }), claim(), volumeRestore(), repository())
			restoreStep(t, r) // plan

			r.Client = loseNextStatusWrite(c)
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
				t.Fatal("the pass whose status write was lost succeeded, want the error returned")
			}
			r.Client = c
			if names := destinations(t, c); len(names) != 1 {
				t.Fatalf("destinations = %v, want the one the lost pass created", names)
			}
			if err := c.Delete(context.Background(), tc.gone); err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r)
			restoreStep(t, r) // the pass after the destination's delete finds its mover gone

			expectItemFailed(t, c, tc.want, "ReplicationDestination "+destinationName(restoreUID, 0), "may have written part of claim "+claimN)
			expectNothingCreated(t, c, "")
		})
	}
}

// A restore selects its snapshot only once a backup of its repository has
// finished, so it selects after that backup's restic forget and retime. Until
// then the run stays unplanned with reason SourceBusy, names the backup, and
// records no snapshot. That holds while VolSync syncs the backup, and after
// the sync while the quiesced BackupRun has yet to retime its snapshot.
func TestARestoreSelectsItsSnapshotOnlyOnceABackupOfItsRepositoryHasFinished(t *testing.T) {
	for _, shape := range restoreShapes {
		for _, synced := range []bool{false, true} {
			name := shape.name + ", syncing"
			if synced {
				name = shape.name + ", synced and not yet retimed"
			}
			t.Run(name, func(t *testing.T) {
				source := backingUp()
				if synced {
					source.Status.LastManualSync = manualTag(source)
				}
				repo := copyRecorded(t, "timed")
				r, c := restoreReconciler(t, nil, restoreRun(shape.mutate, func(r *backupv1alpha1.RestoreRun) { r.CreationTimestamp = metav1.NewTime(frozen) }),
					claim(), volumeRestore(), repository(), otherRun(), source)
				r.Snapshots = repo
				result := restoreStep(t, r)

				run := readRestoreRun(t, c)
				if run.Status.Phase != "" || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
					!strings.Contains(readyMessage(run.Status.Conditions), "BackupRun manual-notes") {
					t.Fatalf("phase = %q, reason = %q, message = %q; want unplanned, SourceBusy naming BackupRun manual-notes",
						run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
				}
				if len(run.Status.Items) != 0 || result.RequeueAfter == 0 {
					t.Errorf("items = %+v, requeue = %v; want none recorded and a requeue", run.Status.Items, result.RequeueAfter)
				}
				expectNothingCreated(t, c, shape.into)

				// The backup retimes its snapshot and finishes.
				rewritten, err := repo.repo.Retime(context.Background(), newestTimed, time.Date(2026, 9, 25, 21, 21, 40, 0, time.UTC), restic.QuiescedTag)
				if err != nil {
					t.Fatal(err)
				}
				finishOtherRun(t, c)
				restoreStep(t, r)

				run = readRestoreRun(t, c)
				if run.Status.Phase != backupv1alpha1.RunPhaseRunning || run.Status.Items[0].Snapshot != rewritten.ShortID() {
					t.Fatalf("phase = %q, items = %+v; want Running with the retimed %s", run.Status.Phase, run.Status.Items, rewritten.ShortID())
				}
			})
		}
	}
}

// finishOtherRun marks the BackupRun manual-notes Succeeded and its trigger
// completed on the claim's ReplicationSource.
func finishOtherRun(t *testing.T, c client.Client) {
	t.Helper()
	backup := &backupv1alpha1.BackupRun{}
	get(t, c, ns, "manual-notes", backup)
	backup.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	backup.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	if err := c.Status().Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	source.Status.LastManualSync = manualTag(source)
	if err := c.Status().Update(context.Background(), source); err != nil {
		t.Fatal(err)
	}
}

// A restore that waits at its checks for a backup that outlasts spec.timeout,
// counted from the run's creation, ends TimedOut and names the backup.
func TestARestoreWaitingForABackupAtItsChecksTimesOut(t *testing.T) {
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate, func(r *backupv1alpha1.RestoreRun) { r.CreationTimestamp = metav1.NewTime(frozen) }),
				claim(), volumeRestore(), repository(), otherRun(), backingUp())
			restoreStep(t, r)

			r.Now = func() time.Time { return frozen.Add(4 * time.Hour) }
			restoreStep(t, r)

			run := readRestoreRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
				t.Fatalf("phase = %q, reason = %q; want Failed, TimedOut", run.Status.Phase, readyReason(run.Status.Conditions))
			}
			if msg := readyMessage(run.Status.Conditions); !strings.Contains(msg, "had not passed its checks by 2026-09-24T16:00:00Z") ||
				!strings.Contains(msg, "BackupRun manual-notes") {
				t.Errorf("message = %q, want the deadline and the BackupRun", msg)
			}
			expectNothingCreated(t, c, shape.into)
		})
	}
}

// An item that records a snapshot and no snapshotTime fails before its
// destination exists: the run has no second to pin the mover to, and a mover
// left to choose by spec.restoreAsOf could restore another snapshot. The run
// records both with its plan, so only a status the run did not write holds
// such an item. Before, the item took the time of the listed snapshot with
// its short ID.
func TestAnItemWithoutASnapshotTimeFailsBeforeItStarts(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = atFrozen(0)
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN,
			Phase: backupv1alpha1.ItemPending, Snapshot: sunday.ShortID()}}
	}), claim(), volumeRestore(), repository())
	restoreStep(t, r)

	expectItemFailed(t, c, "records no snapshot time")
	expectNothingCreated(t, c, "")
}
