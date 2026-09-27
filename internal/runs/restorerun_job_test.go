package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The tests in this file check an in-place restore through the controller's
// own restore Job (designs/restic-jobs.md J1, step 7a): the Job restores the
// full ID the checks recorded, its terminal conditions alone decide the
// item's end, and the run stops it by the UID it recorded.

// A restore Job that ends Failed fails the item with reason RestoreJobFailed
// and a message with restic's exit code, its meaning and restic's last
// lines, read from the Job's own pod. Nothing decides on that message.
func TestAFailedRestoreJobFailsTheItemWithTheExitCode(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, nil, restoreRun(inPlace), claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // restore
	job := itemJob(t, c)
	pod := jobPodOf(job, "restore-pod", corev1.PodFailed)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "restore", State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 12, Message: "Fatal: wrong password or no key found"},
	}}}
	createPod(t, c, pod)
	endJob(t, c, job.Name, batchv1.JobFailed, "PodFailurePolicy", "Container restore for pod notes/restore-pod exited with code 12 matching FailJob rule at index 0")
	restoreStep(t, r)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobFailed {
		t.Fatalf("item = %+v, want Failed with reason RestoreJobFailed", item)
	}
	for _, want := range []string{"restic exited 12 (wrong password)", "in container restore", "Fatal: wrong password or no key found"} {
		if !strings.Contains(item.Message, want) {
			t.Errorf("item message = %q, want it to hold %q", item.Message, want)
		}
	}
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", run.Status.Phase)
	}
}

// A restore Job the run controls under the item's name, left by a pass that
// lost the status write that recorded it, is taken over only when its
// snapshot-id annotation is the full ID the item recorded. One with another
// ID fails the item with reason RestoreJobFailed naming both IDs, and the run
// stops that Job like any other: the Job is suspended, and the app stays down
// until the Job controller reports it suspended.
func TestTakeoverChecksTheSnapshotIDAnnotation(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		snapshotID string
		takenOver  bool
	}{
		"the recorded ID": {monday.ID, true},
		"another ID":      {sunday.ID, false},
	} {
		t.Run(name, func(t *testing.T) {
			run := checkedRestore(quiescedInPlace, func(r *backupv1alpha1.RestoreRun) {
				r.Finalizers = []string{Finalizer}
				r.Status.QuiescedAt = atFrozen(0)
				r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
				r.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
			})
			lost := restoreJobFor(t, run, claimN, tc.snapshotID)
			r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true), lost)
			restoreStep(t, r)

			item := readRestoreRun(t, c).Status.Items[0]
			if item.Job != lost.Name || item.JobUID != lost.UID {
				t.Fatalf("item = %+v, want it to name Job %s with UID %s", item, lost.Name, lost.UID)
			}
			if jobs := restoreJobs(t, c); len(jobs) != 1 {
				t.Errorf("restore Jobs = %v, want only the lost pass's", jobs)
			}
			if tc.takenOver {
				if item.Phase != backupv1alpha1.ItemRunning || suspendedJob(t, c, lost.Name) {
					t.Errorf("item = %+v, Job suspended = %t; want Running with the Job left to run", item, suspendedJob(t, c, lost.Name))
				}
				return
			}
			if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobFailed ||
				!strings.Contains(item.Message, sunday.ID) || !strings.Contains(item.Message, monday.ID) {
				t.Fatalf("item = %+v, want Failed with reason RestoreJobFailed naming both IDs", item)
			}
			if !suspendedJob(t, c, lost.Name) || replicasOf(t, c) != 0 {
				t.Errorf("Job suspended = %t, replicas = %d; want the Job stopped and the app still down", suspendedJob(t, c, lost.Name), replicasOf(t, c))
			}
			markSuspended(t, c, lost)
			restoreStep(t, r)
			if got := replicasOf(t, c); got != 2 {
				t.Errorf("replicas = %d once the Job was stopped, want the 2 the app had", got)
			}
		})
	}
}

// A Running item whose restore Job someone deleted fails with reason
// RestoreJobDeleted, and the run never creates a second Job for it: Stop
// gates on the pods of the recorded UID only. The Job's pod, which the
// delete orphaned and which keeps that UID, holds the app down until it has
// ended.
func TestADeletedRestoreJobFailsTheItemAndHoldsForItsPods(t *testing.T) {
	t.Parallel()
	run, job := quiescedMidRestore(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod)
	if err := c.Delete(context.Background(), job, client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil {
		t.Fatal(err)
	}
	creates := 0
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				creates++
			}
			return cl.Create(ctx, obj, opts...)
		},
	})

	restoreStep(t, r)
	restoreStep(t, r)

	waiting := readRestoreRun(t, c)
	if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobDeleted ||
		!strings.Contains(item.Message, "was deleted before it finished") {
		t.Errorf("item = %+v, want Failed with reason RestoreJobDeleted saying the Job was deleted", item)
	}
	if creates != 0 || len(restoreJobs(t, c)) != 0 {
		t.Errorf("Job creates = %d, restore Jobs = %v; want no second Job", creates, restoreJobs(t, c))
	}
	if got := replicasOf(t, c); got != 0 || !strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Fatalf("replicas = %d, message = %q while pod %s runs; want the app down and the wait naming the pod",
			got, readyMessage(waiting.Status.Conditions), pod.Name)
	}

	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the pod had ended, want the 2 the app had", got)
	}
}

// A run that times out just as its restore Job completed records the item
// Succeeded, and one whose Job failed records restic's exit code: the
// timeout reads each Running item's Job before it fails what is left (P11).
// The run still ends TimedOut.
func TestAbortRecordsAJobThatCompleted(t *testing.T) {
	t.Parallel()
	for name, tc := range map[string]struct {
		condition batchv1.JobConditionType
		phase     backupv1alpha1.ItemPhase
		reason    backupv1alpha1.ItemReason
	}{
		"complete": {batchv1.JobComplete, backupv1alpha1.ItemSucceeded, ""},
		"failed":   {batchv1.JobFailed, backupv1alpha1.ItemFailed, backupv1alpha1.ItemReasonRestoreJobFailed},
	} {
		t.Run(name, func(t *testing.T) {
			run, job := quiescedMidRestore(t)
			job.Status.Conditions = []batchv1.JobCondition{{Type: tc.condition, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"}}
			r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
				stoppedDeployment(), kustomization(true), job, heldClaimLease(run, claimN))
			r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
			restoreStep(t, r)
			restoreStep(t, r)

			done := readRestoreRun(t, c)
			if item := done.Status.Items[0]; item.Phase != tc.phase || item.Reason != tc.reason {
				t.Errorf("item = %+v, want %s with reason %q", item, tc.phase, tc.reason)
			}
			if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
				t.Errorf("phase = %q, reason = %q; want Failed, %s", done.Status.Phase, readyReason(done.Status.Conditions), backupv1alpha1.ReasonTimedOut)
			}
		})
	}
}

// An abort stops a Running item's restore Job through the recorded UID and
// holds until the Job's pod has ended, as finish and finalize do. The item
// whose Job had not ended gets the abort's message, followed by why the
// Job's pod waits.
func TestAnAbortStopsTheJobAndSaysWhyItsPodWaited(t *testing.T) {
	t.Parallel()
	run, job := quiescedMidRestore(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodPending)
	pod.Status.InitContainerStatuses = []corev1.ContainerStatus{{Name: "unlock", State: corev1.ContainerState{
		Waiting: &corev1.ContainerStateWaiting{Reason: "ImagePullBackOff", Message: "Back-off pulling image"},
	}}}
	r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository(),
		stoppedDeployment(), kustomization(true), job, pod)

	stored := readRestoreRun(t, c)
	if _, err := r.abort(context.Background(), stored, backupv1alpha1.ReasonFailed, "Deployment notes was deleted"); err != nil {
		t.Fatal(err)
	}
	markSuspended(t, c, job)
	restoreStep(t, r)

	waiting := readRestoreRun(t, c)
	item := waiting.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || !strings.HasPrefix(item.Message, "Deployment notes was deleted") ||
		!strings.Contains(item.Message, "waiting: unlock: ImagePullBackOff") {
		t.Errorf("item = %+v, want Failed with the abort's message and the pod's waiting reason", item)
	}
	if !suspendedJob(t, c, job.Name) || replicasOf(t, c) != 0 || !strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Fatalf("Job suspended = %t, replicas = %d, message = %q; want the Job stopped and the app down while pod %s may start",
			suspendedJob(t, c, job.Name), replicasOf(t, c), readyMessage(waiting.Status.Conditions), pod.Name)
	}

	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the pod had ended, want the 2 the app had", got)
	}
}

// Only snapshots with the layout VolSync's backup mover gives one are
// candidates (D4): a newer snapshot of another host or path is passed over,
// and a repository with none of the mover's layout fails the item naming
// each snapshot it passed over, with its host and paths.
func TestOnlyMoverSnapshotsAreCandidates(t *testing.T) {
	t.Parallel()
	other := moverSnapshot("5e1f0000", monday.Time)
	other.Paths = []string{"/srv/notes"}
	stranger := moverSnapshot("77770000", monday.Time.Add(time.Hour))
	stranger.Hostname = "laptop"

	r, c := restoreReconciler(t, nil, restoreRun(inPlace), claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{sunday, other, stranger}
	restoreStep(t, r) // plan
	if item := readRestoreRun(t, c).Status.Items[0]; item.SnapshotID != sunday.ID {
		t.Errorf("item = %+v, want sunday's, the newest snapshot a mover wrote", item)
	}

	r, c = restoreReconciler(t, nil, restoreRun(inPlace), claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{other, stranger}
	restoreStep(t, r)
	expectRefused(t, r, "the repository holds 2 snapshots, none written by a VolSync mover (host volsync, paths [/data])",
		"5e1f0000 (host volsync, paths [/srv/notes])", "77770000 (host laptop, paths [/data])")
	if item := readRestoreRun(t, c).Status.Items[0]; item.SnapshotID != "" {
		t.Errorf("item = %+v, want no snapshot recorded", item)
	}

	r, _ = restoreReconciler(t, nil, restoreRun(inPlace), claim(), volumeRestore(), repository())
	r.Snapshots = snapshots{stranger}
	restoreStep(t, r)
	expectRefused(t, r, "the repository holds 1 snapshot, which no VolSync mover wrote (host volsync, paths [/data]): "+
		"77770000 (host laptop, paths [/data])")
}

// A synced in-place restore takes a quiesced snapshot even when an untagged
// one was taken later in the same second. Its restore Job restores the
// quiesced snapshot by its full ID, so the untagged one can't take its
// place, as it could when VolSync's mover picked by the second.
func TestASyncedRestoreRestoresAQuiescedSnapshotAnUntaggedOneShares(t *testing.T) {
	t.Parallel()
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
