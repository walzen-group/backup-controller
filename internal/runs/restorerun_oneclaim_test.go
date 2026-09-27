package runs

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
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
// second waits naming that pod. The first run then times out and suspends
// its Job, and the Job controller deletes the Job's pod before it reports the
// Job suspended. No pod mounts the claim then, but the Job may still get one,
// so the first run keeps naming its Job and holding the claim's Lease, and
// the second waits with reason SourceBusy naming the first. Only once the
// first run has stopped its Job and finished does the second create its own
// Job.
func TestTwoInPlaceRestoresOfOneClaimRunOneAtATime(t *testing.T) {
	t.Parallel()
	second := secondClaimRestore()
	second.Spec.Timeout = &metav1.Duration{Duration: 24 * time.Hour}
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		second, claim(), volumeRestore(), repository())
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
	second = readRun(t, r, "second")
	if readyReason(second.Status.Conditions) != backupv1alpha1.ReasonClaimInUse || !strings.Contains(readyMessage(second.Status.Conditions), pod.Name) {
		t.Errorf("second run: reason = %q, message = %q; want ClaimInUse naming pod %s",
			readyReason(second.Status.Conditions), readyMessage(second.Status.Conditions), pod.Name)
	}

	// The first run times out and suspends its Job. The Job controller
	// deletes the Job's pod and has not reported the Job suspended yet.
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	stepRestore(t, r, "back-to-monday")
	if !suspendedJob(t, c, job.Name) {
		t.Fatalf("restore Job %s is not suspended, want the timed-out run to stop it", job.Name)
	}
	if err := c.Delete(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "second")
	stepRestore(t, r, "second")

	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.JobUID != job.UID {
		t.Fatalf("first item = %+v, want Failed and still naming Job %s while it is not stopped", item, job.Name)
	}
	second = readRun(t, r, "second")
	if second.Status.Items[0].Phase != backupv1alpha1.ItemPending || readyReason(second.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(second.Status.Conditions), "RestoreRun back-to-monday") {
		t.Errorf("second run: item = %+v, reason = %q, message = %q; want it Pending with reason SourceBusy naming RestoreRun back-to-monday",
			second.Status.Items[0], readyReason(second.Status.Conditions), readyMessage(second.Status.Conditions))
	}
	if names := movers(t, c); !slices.Equal(names, []string{job.Name}) {
		t.Errorf("movers = %v while Job %s is not stopped, want only the first Job", names, job.Name)
	}

	// The Job controller reports the first Job suspended: the first run
	// finishes, and the second starts.
	markSuspended(t, c, job)
	stepRestore(t, r, "back-to-monday")
	stepRestore(t, r, "second")

	if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("first run phase = %q, items = %+v; want Failed", run.Status.Phase, run.Status.Items)
	}
	if names := movers(t, c); !slices.Equal(names, []string{jobName(secondRestoreUID, 0)}) {
		t.Errorf("movers = %v, want the second run's own Job", names)
	}
}

// A run whose finished item still names its restore Job keeps the claim's
// Lease, and an in-place restore of that claim waits with reason SourceBusy
// naming that run. The Job is suspended and the Job controller has not
// reported it suspended yet, so no pod mounts the claim, but the Job may
// still get one (rule X2). That holds for an in-place and for an into
// restore holding the Lease, and the waiting run creates no Job.
func TestAFinishedItemNamingAJobKeepsTheClaimLease(t *testing.T) {
	t.Parallel()
	for name, into := range map[string]string{"in place": "", "into": "notes-data-second"} {
		t.Run(name, func(t *testing.T) {
			other := secondClaimRestore()
			other.Spec.Into = into
			item := claimN
			if into != "" {
				item = into
			}
			other.Status.Phase = backupv1alpha1.RunPhaseRunning
			other.Status.Items = []backupv1alpha1.RestoreItem{{Kind: backupv1alpha1.ItemKindClaim, Name: item, Phase: backupv1alpha1.ItemFailed,
				Reason: backupv1alpha1.ItemReasonTimedOut, SnapshotID: monday.ID, Job: jobName(secondRestoreUID, 0), JobUID: jobUID}}
			job := restoreJobFor(t, other, item, monday.ID)
			job.Spec.Suspend = ptr.To(true)
			r, c := restoreReconciler(t, nil, checkedRestore(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
				other, claim(), volumeRestore(), repository(), heldClaimLease(other, item), job)

			restoreStep(t, r)

			run := readRestoreRun(t, c)
			if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
				!strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun second") {
				t.Errorf("reason = %q, message = %q; want SourceBusy naming RestoreRun second",
					readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
			}
			if names := movers(t, c); !slices.Equal(names, []string{job.Name}) {
				t.Errorf("movers = %v, want only the other run's Job %s", names, job.Name)
			}
		})
	}
}

// An in-place restore whose item is Pending waits with reason SourceBusy,
// naming the other run, while another live RestoreRun holds the claim's
// Lease and has not created its restore Job yet. Nothing else shows that
// run: no pod mounts the claim and no backup has its trigger on a
// ReplicationSource, so only the Lease keeps the two movers apart, and the
// waiting run creates no Job.
func TestARestoreWaitsForTheClaimLeaseOfARunThatHasNotStarted(t *testing.T) {
	t.Parallel()
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
