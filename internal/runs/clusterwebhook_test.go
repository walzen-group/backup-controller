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

// A quiesced namespace restore planned while CloudNativePG no longer serves
// Cluster at v1 ends at its first step, before anything is stopped or
// suspended: the Cluster item fails, and the volume item, still Pending, is
// Skipped with the app left running.
func TestAPlannedNamespaceRestoreStopsNothingWhileTheWebhookCannotSee(t *testing.T) {
	t.Parallel()
	c := newClientWithCRDs(t, crdsServedAtNext(t), restoreRun(quiescedInPlace, asOf("2026-09-22T00:00:00Z")),
		claim(), volumeRestore(), repository(), atNext(cluster()), atNext(objectStore()), storeSecret(),
		deployment(), kustomization(false))
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday}, Now: frozenNow}

	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonClusterVersionUnsupported {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed with %s after the first step", run.Status.Phase,
			readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions), backupv1alpha1.ReasonClusterVersionUnsupported)
	}
	if len(run.Status.Quiesced) != 0 || run.Status.QuiescedAt != nil || replicasOf(t, c) != 2 || suspended(t, c) {
		t.Errorf("quiesced = %v, replicas = %d, suspended = %v; want nothing stopped or suspended",
			run.Status.Quiesced, replicasOf(t, c), suspended(t, c))
	}
	if len(run.Status.Items) != 2 {
		t.Fatalf("items = %+v, want the claim and the Cluster", run.Status.Items)
	}
	for _, item := range run.Status.Items {
		want := backupv1alpha1.ItemSkipped
		if item.Kind == "Cluster" {
			want = backupv1alpha1.ItemFailed
		}
		if item.Phase != want {
			t.Errorf("item %s %s = %q (%s), want %s", item.Kind, item.Name, item.Phase, item.Message, want)
		}
	}
}

// A restore whose Cluster item fails while CloudNativePG no longer serves
// Cluster at v1, and whose volume item still runs then, ends a pass later
// with reason ClusterVersionUnsupported. The failed Cluster item records
// the reason ClusterVersionUnsupported, which the run's end reason comes
// from. Before, the run ended with reason Failed, because only the pass
// that failed the item counted.
func TestABlindClusterFailedEarlierStillEndsClusterVersionUnsupported(t *testing.T) {
	t.Parallel()
	run, job := restoringOnJob(t)
	run.Spec.Claim, run.Spec.All = "", true
	run.Status.Items = append(run.Status.Items,
		backupv1alpha1.RestoreItem{Kind: backupv1alpha1.ItemKindCluster, Name: pgN, Phase: backupv1alpha1.ItemPending, BaseBackup: saturday.ID})
	c := newClientWithCRDs(t, crdsServedAtNext(t), run, job, claim(), volumeRestore(), repository(),
		atNext(cluster()), atNext(objectStore()), storeSecret())
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday},
		RestoreImage: testImage, Now: frozenNow}

	restoreStep(t, r) // the Cluster item fails, the volume item runs on
	if items := readRestoreRun(t, c).Status.Items; items[1].Phase != backupv1alpha1.ItemFailed || items[0].Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("items = %+v, want the Cluster item Failed and the volume item Running", items)
	}
	completeJob(t, c)
	done := stepUntilFinished(t, r, c, 3)

	if done.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(done.Status.Conditions) != backupv1alpha1.ReasonClusterVersionUnsupported {
		t.Errorf("phase = %q, reason = %q (%s); want Failed with %s", done.Status.Phase, readyReason(done.Status.Conditions),
			readyMessage(done.Status.Conditions), backupv1alpha1.ReasonClusterVersionUnsupported)
	}
	if item := done.Status.Items[1]; item.Reason != backupv1alpha1.ItemReasonClusterVersionUnsupported {
		t.Errorf("Cluster item = %+v, want reason %s", item, backupv1alpha1.ItemReasonClusterVersionUnsupported)
	}
}
