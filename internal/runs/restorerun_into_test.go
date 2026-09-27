package runs

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file check an into restore (spec.into), from a claim's
// backups or from a repository, through the controller's own restore Job
// (designs/restic-jobs.md J1, step 8). The run creates a plain claim and a
// Job that restores the full ID its checks recorded into that claim; the
// Job's terminal conditions alone decide the item's end, and the run stops
// the Job by the UID it recorded.

// sourceOnNode returns the app's claim, as claim does, with the node the
// scheduler selected for its volume.
func sourceOnNode() *corev1.PersistentVolumeClaim {
	source := claim()
	source.Annotations[selectedNodeAnnotation] = "worker-1"
	return source
}

// intoShapesOnJob are the two shapes of an into restore: from a claim's
// backups (see intoMonday) and from a repository alone (see fromRepository),
// with the claim each creates.
var intoShapesOnJob = []struct {
	name   string
	mutate func(*backupv1alpha1.RestoreRun)
	into   string
}{
	{"from a claim", intoMonday, "notes-data-monday"},
	{"from a repository", fromRepository, "scratch"},
}

// jobClaim returns the name of the claim the Job mounts as its data volume,
// and "" when it mounts none.
func jobClaim(job *batchv1.Job) string {
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			return v.PersistentVolumeClaim.ClaimName
		}
	}
	return ""
}

// startedInto returns a reconciler whose into restore of the given shape
// selected monday and created its claim and restore Job, and the client.
func startedInto(t *testing.T, mutate func(*backupv1alpha1.RestoreRun)) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	r, c := restoreReconciler(t, nil, restoreRun(mutate), sourceOnNode(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.JobUID == "" {
		t.Fatalf("item = %+v; want Running on its restore Job", item)
	}
	return r, c
}

// An into restore creates a plain claim, with no data source, so no
// populator is involved, and one restore Job that restores the snapshot the
// checks selected by its full ID into that claim, with --delete, controlled
// by the run. The item records the Job's name and UID in the same status
// write, and no ReplicationDestination exists. From a claim, the new claim
// takes the source claim's size, class and node, and the Job the source's
// cache class and queue label. Before, the run wrote through a
// ReplicationDestination pinned to the snapshot's second.
func TestAnIntoRestoreCreatesAJobForTheFullSnapshotID(t *testing.T) {
	t.Parallel()
	for _, shape := range intoShapesOnJob {
		t.Run(shape.name, func(t *testing.T) {
			_, c := startedInto(t, shape.mutate)

			run := readRestoreRun(t, c)
			scratch := &corev1.PersistentVolumeClaim{}
			get(t, c, ns, shape.into, scratch)
			if scratch.Spec.DataSourceRef != nil || scratch.Spec.DataSource != nil || !metav1.IsControlledBy(scratch, run) {
				t.Fatalf("claim = %+v, owners %v; want no data source and the run as controller", scratch.Spec, scratch.OwnerReferences)
			}
			jobs := restoreJobs(t, c)
			if len(jobs) != 1 {
				t.Fatalf("restore Jobs = %v, want one", jobs)
			}
			job := jobs[0]
			item := run.Status.Items[0]
			if item.SnapshotID != monday.ID || item.Job != job.Name || item.JobUID != job.UID || item.Job != jobName(restoreUID, 0) {
				t.Fatalf("item = %+v, Job %s (UID %s); want it naming the Job and its UID, with monday's full ID", item, job.Name, job.UID)
			}
			restore := job.Spec.Template.Spec.Containers[0]
			if !metav1.IsControlledBy(&job, run) || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID ||
				!slices.Contains(restore.Args, monday.ID) || !slices.Contains(restore.Args, "--delete") || jobClaim(&job) != shape.into {
				t.Errorf("Job owners = %v, annotations = %v, args = %v, claim %q; want the run's Job restoring %s into %s with --delete",
					job.OwnerReferences, job.Annotations, restore.Args, jobClaim(&job), monday.ID, shape.into)
			}
			if shape.into != "notes-data-monday" {
				return
			}
			request := scratch.Spec.Resources.Requests[corev1.ResourceStorage]
			if request.Cmp(resource.MustParse("1Gi")) != 0 || scratch.Spec.StorageClassName == nil || *scratch.Spec.StorageClassName != "zfs" ||
				scratch.Annotations[selectedNodeAnnotation] != "worker-1" {
				t.Errorf("claim = %+v, annotations %v; want 1Gi of zfs on worker-1", scratch.Spec, scratch.Annotations)
			}
			if job.Spec.Template.Labels["kueue.x-k8s.io/queue-name"] != "backups" {
				t.Errorf("pod labels = %v, want the source's queue label", job.Spec.Template.Labels)
			}
		})
	}
}

// plannedInto returns the RestoreRun back-to-monday as planIntoNewClaim
// leaves an into restore of the claim notes-data-monday: Running, holding
// its finalizer, with its item Running on monday's full ID and no Job yet.
func plannedInto() *backupv1alpha1.RestoreRun {
	started := metav1.NewTime(frozen)
	return restoreRun(intoMonday, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = &started
		r.Status.Target = r.Spec.Into
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: backupv1alpha1.ItemKindClaim, Name: r.Spec.Into, Phase: backupv1alpha1.ItemRunning,
			Snapshot: monday.ShortID(), SnapshotID: monday.ID, SnapshotTime: &metav1.Time{Time: monday.Time}}}
	})
}

// Two snapshots in one second restore by their full IDs, in place and into
// a new claim of either shape: with restoreAsOf at their second, previous 0
// records the later snapshot and previous 1 the earlier one, and the
// restore Job each run creates restores exactly the ID its item records.
// The first repository is the recorded same-second one, whose two snapshots
// differ in the nanoseconds; the second holds two snapshots with the same
// time to the nanosecond, which the lister orders by ID. Before, an into
// restore refused every one of them but the recorded later snapshot, since
// VolSync's mover pinned to their second could restore another.
func TestTwoSnapshotsInOneSecondRestoreByID(t *testing.T) {
	t.Parallel()
	twin := time.Date(2026, 9, 25, 21, 22, 16, 500e6, time.UTC)
	repositories := []struct {
		name  string
		list  func(t *testing.T) snapshots
		wants []string
	}{
		{"recorded", func(t *testing.T) snapshots { return recordedRepository(t, "same-second") }, []string{"2d35d9a8", "763f53b1"}},
		{"twins", func(*testing.T) snapshots {
			return snapshots{sunday, moverSnapshot("aaaaaaaa", twin), moverSnapshot("bbbbbbbb", twin)}
		}, []string{"bbbbbbbb", "aaaaaaaa"}},
	}
	shapes := append([]struct {
		name   string
		mutate func(*backupv1alpha1.RestoreRun)
		into   string
	}{{"in place", inPlace, claimN}}, intoShapesOnJob...)
	for _, repo := range repositories {
		for _, shape := range shapes {
			for previous, want := range repo.wants {
				t.Run(fmt.Sprintf("%s/%s/previous %d", repo.name, shape.name, previous), func(t *testing.T) {
					back := int32(previous)
					r, c := restoreReconciler(t, nil, restoreRun(shape.mutate, asOf("2026-09-25T21:22:16Z"),
						func(r *backupv1alpha1.RestoreRun) { r.Spec.Previous = &back }), sourceOnNode(), volumeRestore(), repository())
					r.Snapshots = repo.list(t)
					restoreStep(t, r) // plan
					restoreStep(t, r) // create the restore Job

					run := readRestoreRun(t, c)
					if len(run.Status.Items) == 0 {
						t.Fatalf("phase = %q with no items (%s); want Running on %s", run.Status.Phase, readyMessage(run.Status.Conditions), want)
					}
					item := run.Status.Items[0]
					if item.Phase != backupv1alpha1.ItemRunning || !strings.HasPrefix(item.SnapshotID, want) {
						t.Fatalf("item = %+v (%s); want Running on %s", item, readyMessage(run.Status.Conditions), want)
					}
					job := itemJob(t, c)
					if job.Annotations[restorejob.AnnotationSnapshotID] != item.SnapshotID || jobClaim(job) != shape.into {
						t.Errorf("restore Job restores %s into %s, want %s into %s",
							job.Annotations[restorejob.AnnotationSnapshotID], jobClaim(job), item.SnapshotID, shape.into)
					}
				})
			}
		}
	}
}
