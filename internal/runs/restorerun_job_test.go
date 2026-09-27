package runs

import (
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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
