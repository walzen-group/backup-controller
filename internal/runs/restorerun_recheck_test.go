package runs

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"slices"
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
// into is the claim an into restore creates. Each writes through a restore
// Job the run creates.
var restoreShapes = []struct {
	name   string
	mutate func(*backupv1alpha1.RestoreRun)
	into   string
}{
	{"in place", func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, ""},
	{"into from a repository", fromRepository, "scratch"},
	{"into from a claim", func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "scratch" }, "scratch"},
}

// repositoryCopy is a SnapshotLister over a copy of a recorded restic
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

// forget removes the snapshot with the given full ID from the copy, as
// restic forget does: it deletes the snapshot's file and leaves the data.
func (r *repositoryCopy) forget(t *testing.T, id string) {
	t.Helper()
	list, err := r.repo.Snapshots(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !slices.ContainsFunc(list, func(s restic.Snapshot) bool { return s.ID == id }) {
		t.Fatalf("no snapshot %s to forget", id)
	}
	if err := restic.DirStore(r.dir).Remove(context.Background(), path.Join("snapshots", id)); err != nil {
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

// expectNothingCreated checks that the run created no mover, neither a
// restore Job nor a ReplicationDestination, and, for an into restore,
// neither the claim nor a VolumeRestore named into.
func expectNothingCreated(t *testing.T, c client.Client, into string) {
	t.Helper()
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("movers = %v, want none", names)
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
	t.Parallel()
	for _, shape := range restoreShapes {
		t.Run(shape.name, func(t *testing.T) {
			repo := copyRecorded(t, "timed")
			r, c := restoreReconciler(t, nil, restoreRun(shape.mutate), claim(), volumeRestore(), repository())
			r.Snapshots = repo
			restoreStep(t, r) // plan
			selected := readRestoreRun(t, c).Status.Items[0]
			if selected.Snapshot != newestTimed {
				t.Fatalf("the checks selected %s, want %s", selected.Snapshot, newestTimed)
			}

			repo.forget(t, selected.SnapshotID)
			restoreStep(t, r)
			restoreStep(t, r) // the finished run only waits for its expiry

			expectItemFailed(t, c, "snapshot "+newestTimed+" (2026-09-25T21:21:02Z), which the checks selected, is no longer in the repository",
				"restic forget", "Create a new RestoreRun")
			expectNothingCreated(t, c, shape.into)
			expectItemReason(t, c, backupv1alpha1.ItemReasonSnapshotChanged)
		})
	}
}

// A pass that created the item's restore Job and lost the status write
// leaves the item Pending. The next pass takes that Job over: the item goes
// Running and names the Job and its UID, even when the snapshot is gone from
// the repository or the repository Secret or the claim's VolumeRestore is
// gone by then. The lost pass ran every check before the create, as a pass
// whose write went through does, and the Job's pod may already write. From
// then on the item is followed like any other Running item, and the run
// stops that Job when it ends (UFR1). No second Job is created.
func TestAPassAfterALostWriteTakesTheJobOver(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		forget bool
		gone   client.Object
	}{
		"snapshot forgotten": {forget: true},
		"repository Secret":  {gone: repository()},
		"VolumeRestore":      {gone: volumeRestore()},
	} {
		t.Run(name, func(t *testing.T) {
			repo := copyRecorded(t, "timed")
			r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }), claim(), volumeRestore(), repository())
			r.Snapshots = repo
			restoreStep(t, r) // plan

			r.Client = loseNextStatusWrite(c)
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
				t.Fatal("the pass whose status write was lost succeeded, want the error returned")
			}
			r.Client = c
			jobs := restoreJobs(t, c)
			if len(jobs) != 1 {
				t.Fatalf("restore Jobs = %v, want the one the lost pass created", jobs)
			}
			if tc.forget {
				repo.forget(t, readRestoreRun(t, c).Status.Items[0].SnapshotID)
			}
			if tc.gone != nil {
				if err := c.Delete(context.Background(), tc.gone); err != nil {
					t.Fatal(err)
				}
			}
			restoreStep(t, r)

			run := readRestoreRun(t, c)
			if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Job != jobs[0].Name || item.JobUID != jobs[0].UID {
				t.Errorf("item = %+v (%s), want Running naming %s with UID %s", item, readyMessage(run.Status.Conditions), jobs[0].Name, jobs[0].UID)
			}
			if again := restoreJobs(t, c); len(again) != 1 || again[0].UID != jobs[0].UID {
				t.Errorf("restore Jobs = %v, want only the lost pass's", again)
			}
		})
	}
}

// A restore selects its snapshot only once a backup of its repository has
// finished, so it selects after that backup's restic forget and retime. Until
// then the run stays unplanned with reason SourceBusy, names the backup, and
// records no snapshot. That holds while VolSync syncs the backup, and after
// the sync while the quiesced BackupRun has yet to retime its snapshot.
func TestARestoreSelectsItsSnapshotOnlyOnceABackupOfItsRepositoryHasFinished(t *testing.T) {
	t.Parallel()
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

// expectItemReason checks that the run's first item records the reason
// given.
func expectItemReason(t *testing.T, c client.Client, want backupv1alpha1.ItemReason) {
	t.Helper()
	if item := readRestoreRun(t, c).Status.Items[0]; item.Reason != want {
		t.Errorf("item reason = %q, want %s", item.Reason, want)
	}
}
