package runs

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// inPlace is a mutate function for restoreRun that makes it an in-place
// restore of the claim notes-data.
func inPlace(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }

// expectVolumeFailed checks that the run's volume item failed with a message
// holding every string in want, and did not succeed.
func expectVolumeFailed(t *testing.T, run *backupv1alpha1.RestoreRun, want ...string) {
	t.Helper()
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed {
		t.Fatalf("item = %+v, want it Failed", item)
	}
	for _, w := range want {
		if !strings.Contains(item.Message, w) {
			t.Errorf("item message = %q, want it to hold %q", item.Message, w)
		}
	}
}

// An in-place item whose claim was replaced by another of the same name
// while its restore Job wrote fails once the Job completes. The Job mounts the
// claim by name, so it may have written into the new claim as well; the
// message says so and asks for the claim's data to be checked.
func TestAnInPlaceRestoreIntoAReplacedClaimFails(t *testing.T) {
	t.Parallel()
	r, c := startedRestore(t, inPlace)
	if err := c.Delete(context.Background(), claim()); err != nil {
		t.Fatal(err)
	}
	replacement := claim()
	replacement.UID = "claim-uid-2"
	replacement.ResourceVersion = ""
	if err := c.Create(context.Background(), replacement); err != nil {
		t.Fatal(err)
	}
	completeJob(t, c)

	restoreStep(t, r)

	expectVolumeFailed(t, readRestoreRun(t, c), "claim "+claimN, "replaced", "not the one the run checked and took its Lease on",
		"The restore Job mounts claim "+claimN+" by name", "may have written into it", "check its data")
}

// startedRestore returns a reconciler whose run of the given shape selected
// monday and created its restore Job, and the client.
func startedRestore(t *testing.T, mutate func(*backupv1alpha1.RestoreRun)) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	r, c := restoreReconciler(t, nil, restoreRun(mutate), claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create
	item := readRestoreRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning || item.Snapshot != monday.ShortID() {
		t.Fatalf("item = %+v; want Running on monday's snapshot", item)
	}
	return r, c
}
