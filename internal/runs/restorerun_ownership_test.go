package runs

import (
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// intoMonday is a mutate function for restoreRun that restores the claim's
// backups into a new claim named notes-data-monday.
func intoMonday(r *backupv1alpha1.RestoreRun) {
	r.Spec.Claim, r.Spec.Into = claimN, "notes-data-monday"
}

// stepUntilFinished reconciles the run back-to-monday until it has finished,
// at most n times, and returns it.
func stepUntilFinished(t *testing.T, r *RestoreRunReconciler, c client.Client, n int) *backupv1alpha1.RestoreRun {
	t.Helper()
	for range n {
		if run := readRestoreRun(t, c); run.Status.Phase.Finished() {
			return run
		}
		restoreStep(t, r)
	}
	return readRestoreRun(t, c)
}
