package runs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// The tests in this file check that a RestoreRun deletes a Cluster only
// while the API server serves Cluster at postgresql.cnpg.io/v1, the version
// the bootstrap webhook's rules name. Without it, the API server creates the
// Cluster again without calling the webhook, and the Cluster starts empty.

// crdsWithClusterAlsoAtNext returns the CRD files of newClient with the
// CloudNativePG Cluster served at v1 and at nextVersion, v1 staying the
// storage version, as after a CloudNativePG release that adds a version.
func crdsWithClusterAlsoAtNext(t *testing.T) []string {
	t.Helper()
	path := crdDir + "cloudnative-pg/postgresql.cnpg.io_clusters.yaml"
	crd := readCRD(t, path)
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if len(versions) != 1 {
		t.Fatalf("%s has %d versions, want v1 alone", path, len(versions))
	}
	next := runtime.DeepCopyJSONValue(versions[0]).(map[string]any)
	next["name"], next["storage"], next["served"] = nextVersion, false, true
	if err := unstructured.SetNestedSlice(crd.Object, append(versions, next), "spec", "versions"); err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(crd.Object)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), filepath.Base(path))
	if err := os.WriteFile(out, data, 0o600); err != nil {
		t.Fatal(err)
	}
	files := make([]string, 0, len(crds))
	for _, p := range crds {
		if p == path {
			p = out
		}
		files = append(files, p)
	}
	return files
}

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

// A CloudNativePG release that adds a version and keeps serving v1 changes
// nothing: the API server converts a creation at either version to v1 for
// the webhook, and the restore deletes the Cluster as before. The API server
// prefers the new version, so the run reads the Cluster there; the fake
// client converts nothing, so the Cluster is seeded at that version.
func TestADatabaseRestoreGoesOnWhileClusterIsStillServedAtV1(t *testing.T) {
	c := newClientWithCRDs(t, crdsWithClusterAlsoAtNext(t),
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }, asOf("2026-09-22T00:00:00Z")),
		atNext(cluster()), objectStore(), storeSecret())
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday}, Now: frozenNow}

	restoreStep(t, r) // plan
	restoreStep(t, r) // delete

	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemDeleted {
		t.Fatalf("item = %+v, want Deleted", item)
	}
}

// A run planned while v1 was served that reaches the delete after
// CloudNativePG stopped serving v1 fails the item and deletes nothing.
func TestAPlannedRestoreDeletesNoClusterTheWebhookCannotSee(t *testing.T) {
	started := metav1.NewTime(frozen)
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Database = pgN
		r.Finalizers = []string{Finalizer}
		r.Status.Phase, r.Status.StartedAt = backupv1alpha1.RunPhaseRunning, &started
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemPending, BaseBackup: saturday.ID}}
	}, asOf("2026-09-22T00:00:00Z"))
	c := newClientWithCRDs(t, crdsServedAtNext(t), run, atNext(cluster()), atNext(objectStore()), storeSecret())
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday}, Now: frozenNow}

	restoreStep(t, r)

	after := readRestoreRun(t, c)
	item := after.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "deleted nothing") {
		t.Errorf("item = %+v, want it Failed saying the run deleted nothing", item)
	}
	checkClusterUnsupported(t, item.Message)
	if readyReason(after.Status.Conditions) != backupv1alpha1.ReasonClusterVersionUnsupported {
		t.Errorf("reason = %q, want %s", readyReason(after.Status.Conditions), backupv1alpha1.ReasonClusterVersionUnsupported)
	}
	if _, ok := getUnstructured(t, c, nextGVK(cnpg.ClusterGVK), ns, pgN); !ok {
		t.Error("the Cluster was deleted")
	}
}

// A run that already deleted the Cluster and waits for its owner to create
// it again says, with reason ClusterVersionUnsupported, that the webhook
// would not see that creation. The run holds nothing back by then, so the
// message says what a creation then does and how to restore the Cluster,
// and asks nobody to hold the creation back.
func TestARestoreWaitingForAClusterTheWebhookCannotSeeSaysSo(t *testing.T) {
	started := metav1.NewTime(frozen)
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Database = pgN
		r.Finalizers = []string{Finalizer}
		r.Status.Phase, r.Status.StartedAt = backupv1alpha1.RunPhaseRunning, &started
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemDeleted,
			BaseBackup: saturday.ID, ClusterUID: "old-cluster-uid"}}
	}, asOf("2026-09-22T00:00:00Z"))
	c := newClientWithCRDs(t, crdsServedAtNext(t), run, atNext(objectStore()), storeSecret())
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday}, Now: frozenNow}

	restoreStep(t, r)

	after := readRestoreRun(t, c)
	if readyReason(after.Status.Conditions) != backupv1alpha1.ReasonClusterVersionUnsupported {
		t.Fatalf("reason = %q (%s), want %s", readyReason(after.Status.Conditions), readyMessage(after.Status.Conditions),
			backupv1alpha1.ReasonClusterVersionUnsupported)
	}
	checkClusterUnsupported(t, readyMessage(after.Status.Conditions))
	if message := readyMessage(after.Status.Conditions); strings.Contains(message, "Hold back") || !strings.Contains(message, "new RestoreRun") {
		t.Errorf("message = %q, want it to say how to restore the Cluster without asking to hold its creation back", message)
	}
	if after.Status.Phase.Finished() {
		t.Errorf("phase = %q, want the run still waiting for the Cluster", after.Status.Phase)
	}
}

// A run whose Cluster item failed for another reason before CloudNativePG
// stopped serving v1 ends with reason Failed: the check deleted nothing and
// failed no item of it, so ClusterVersionUnsupported would blame the wrong
// cause.
func TestARestoreWhoseClusterFailedEarlierEndsFailedWhileTheWebhookCannotSee(t *testing.T) {
	started := metav1.NewTime(frozen)
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.All = true
		r.Finalizers = []string{Finalizer}
		r.Status.Phase, r.Status.StartedAt = backupv1alpha1.RunPhaseRunning, &started
		r.Status.RestartedAt = &started
		r.Status.Items = []backupv1alpha1.RestoreItem{
			{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemFailed, BaseBackup: saturday.ID, ClusterUID: "old-cluster-uid",
				Message: "came back without this run's recovery"},
			{Kind: "Cluster", Name: "other-db", Phase: backupv1alpha1.ItemSucceeded, BaseBackup: saturday.ID, ClusterUID: "other-uid"},
		}
	}, asOf("2026-09-22T00:00:00Z"))
	c := newClientWithCRDs(t, crdsServedAtNext(t), run, atNext(objectStore()), storeSecret())
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Prober: prober{saturday}, Now: frozenNow}

	restoreStep(t, r)

	after := readRestoreRun(t, c)
	if after.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(after.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Errorf("phase = %q, reason = %q (%s); want Failed with %s", after.Status.Phase, readyReason(after.Status.Conditions),
			readyMessage(after.Status.Conditions), backupv1alpha1.ReasonFailed)
	}
}

// A quiesced namespace restore planned while CloudNativePG no longer serves
// Cluster at v1 ends at its first step, before anything is stopped or
// suspended: the Cluster item fails, and the volume item, still Pending, is
// Skipped with the app left running.
func TestAPlannedNamespaceRestoreStopsNothingWhileTheWebhookCannotSee(t *testing.T) {
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
