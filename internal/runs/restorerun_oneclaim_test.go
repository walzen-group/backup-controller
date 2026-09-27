package runs

import (
	"slices"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
)

// The tests in this file check that two restores never write one claim at
// once (finding F in the RestoreRun design): a second in-place restore of a
// claim waits while a pod of another run's restore Job mounts it, and while
// the Job of another run's finished item may still write (rule X2).

// secondRestoreUID is the UID of the RestoreRun second, which the tests in
// this file run beside back-to-monday.
const secondRestoreUID = types.UID("5c3a1f07-0000-4000-8000-00000000000b")

// secondClaimRestore returns the RestoreRun second, an in-place restore of
// the claim notes-data, like back-to-monday's.
func secondClaimRestore() *backupv1alpha1.RestoreRun {
	return restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Name, r.UID, r.Spec.Claim = "second", secondRestoreUID, claimN
	})
}

// readRun reads the RestoreRun with the given name back from the client.
func readRun(t *testing.T, r *RestoreRunReconciler, name string) *backupv1alpha1.RestoreRun {
	t.Helper()
	run := &backupv1alpha1.RestoreRun{}
	get(t, r.Client, ns, name, run)
	return run
}

// Two in-place restores of one claim run one after the other. While the first
// run's restore Job writes into the claim, its pod mounts the claim and the
// second waits naming that pod. Once the first item has ended, the second
// keeps waiting for as long as the first Job's pod may still write, and the
// first run keeps naming its Job and holding the claim's Lease. Only once
// the first run has stopped its Job and finished does the second create its
// own Job.
func TestTwoInPlaceRestoresOfOneClaimRunOneAtATime(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		secondClaimRestore(), claim(), volumeRestore(), repository())
	stepRestore(t, r, "back-to-monday") // plan
	stepRestore(t, r, "second")         // plan
	stepRestore(t, r, "back-to-monday") // create the Job
	job := itemJob(t, c)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	createPod(t, c, pod)
	stepRestore(t, r, "second")

	if names := movers(t, c); !slices.Equal(names, []string{job.Name}) {
		t.Fatalf("movers = %v, want only back-to-monday's %s", names, job.Name)
	}
	second := readRun(t, r, "second")
	if readyReason(second.Status.Conditions) != backupv1alpha1.ReasonClaimInUse || !strings.Contains(readyMessage(second.Status.Conditions), pod.Name) {
		t.Errorf("second run: reason = %q, message = %q; want ClaimInUse naming pod %s",
			readyReason(second.Status.Conditions), readyMessage(second.Status.Conditions), pod.Name)
	}

	// The first item ends while its Job's pod is still there.
	completeJob(t, c)
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "second")
	stepRestore(t, r, "second")

	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemSucceeded || item.JobUID != job.UID {
		t.Fatalf("first item = %+v, want Succeeded and still naming Job %s while its pod is there", item, job.Name)
	}
	second = readRun(t, r, "second")
	if second.Status.Items[0].Phase != backupv1alpha1.ItemPending {
		t.Errorf("second run: item = %+v, reason = %q, message = %q; want it Pending while the first Job's pod is there",
			second.Status.Items[0], readyReason(second.Status.Conditions), readyMessage(second.Status.Conditions))
	}
	if names := movers(t, c); !slices.Equal(names, []string{job.Name}) {
		t.Errorf("movers = %v while pod %s is there, want only the first Job", names, pod.Name)
	}

	// The first Job's pod has ended: the first run finishes, and the second
	// starts.
	setPodPhase(t, c, pod, corev1.PodSucceeded)
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "second")

	if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("first run phase = %q, items = %+v; want Succeeded", run.Status.Phase, run.Status.Items)
	}
	if names := movers(t, c); !slices.Equal(names, []string{jobName(secondRestoreUID, 0)}) {
		t.Errorf("movers = %v, want the second run's own Job", names)
	}
}

// An in-place restore whose item is Pending waits with reason SourceBusy,
// naming the other run, while another live RestoreRun holds the claim's
// Lease and has not created its restore Job yet. Nothing else shows that
// run: no pod mounts the claim and no backup has its trigger on a
// ReplicationSource, so only the Lease keeps the two movers apart, and the
// waiting run creates no Job.
func TestARestoreWaitsForTheClaimLeaseOfARunThatHasNotStarted(t *testing.T) {
	other := secondClaimRestore()
	other.Status.Phase = backupv1alpha1.RunPhaseRunning
	other.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemPending}}
	r, c := restoreReconciler(t, nil, checkedRestore(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		other, claim(), volumeRestore(), repository(), heldClaimLease(other, claimN))

	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun second") {
		t.Errorf("reason = %q, message = %q; want SourceBusy naming RestoreRun second",
			readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemPending || item.JobUID != "" {
		t.Errorf("item = %+v, want Pending with no restore Job", item)
	}
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("movers = %v, want none while the other run holds the claim Lease", names)
	}
}
