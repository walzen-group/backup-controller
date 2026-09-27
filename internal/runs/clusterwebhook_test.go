package runs

import (
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
)

// The tests in this file check that a RestoreRun deletes a Cluster only
// while the API server serves Cluster at postgresql.cnpg.io/v1, the version
// the bootstrap webhook's rules name. Without it, the API server creates the
// Cluster again without calling the webhook, and the Cluster starts empty.

// checkClusterUnsupported fails the test unless message names Cluster, the
// version the webhook's rules name and the version served.
func checkClusterUnsupported(t *testing.T, message string) {
	t.Helper()
	for _, want := range []string{"Cluster", "postgresql.cnpg.io/v1", "postgresql.cnpg.io/" + nextVersion, "webhook"} {
		if !strings.Contains(message, want) {
			t.Errorf("message %q does not name %s", message, want)
		}
	}
}

// A database restore on a cluster whose CloudNativePG serves Cluster at a
// new version alone ends Failed with reason ClusterVersionUnsupported before
// it deletes anything: the bootstrap webhook would not see the Cluster's
// creation, and the Cluster would start as an empty database.
func TestADatabaseRestoreDeletesNoClusterTheWebhookCannotSee(t *testing.T) {
	t.Parallel()
	c := newClientWithCRDs(t, crdsServedAtNext(t),
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }, asOf("2026-09-22T00:00:00Z")),
		atNext(cluster()), atNext(objectStore()), storeSecret())
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday}, Now: frozenNow}

	restoreStep(t, r)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonClusterVersionUnsupported {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed with %s", run.Status.Phase, readyReason(run.Status.Conditions),
			readyMessage(run.Status.Conditions), backupv1alpha1.ReasonClusterVersionUnsupported)
	}
	checkClusterUnsupported(t, readyMessage(run.Status.Conditions))
	if _, ok := getUnstructured(t, c, nextGVK(cnpg.ClusterGVK), ns, pgN); !ok {
		t.Error("the Cluster was deleted")
	}
}
