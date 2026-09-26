package runs

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"
)

// volsyncFiles are the pinned CRD files of VolSync's two kinds.
var volsyncFiles = []string{
	crdDir + "volsync/volsync.backube_replicationsources.yaml",
	crdDir + "volsync/volsync.backube_replicationdestinations.yaml",
}

// crdsWithVolSyncAt returns the CRD files of newClient with VolSync's
// ReplicationSource and ReplicationDestination served at version alone, as
// after a VolSync release that moves them off v1alpha1. The schema stays
// the pinned one.
func crdsWithVolSyncAt(t *testing.T, version string) []string {
	t.Helper()
	dir := t.TempDir()
	replaced := map[string]string{}
	for _, path := range volsyncFiles {
		crd := readCRD(t, path)
		versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
		if len(versions) != 1 {
			t.Fatalf("%s has %d versions, want v1alpha1 alone", path, len(versions))
		}
		versions[0].(map[string]any)["name"] = version
		if err := unstructured.SetNestedSlice(crd.Object, versions, "spec", "versions"); err != nil {
			t.Fatal(err)
		}
		data, err := yaml.Marshal(crd.Object)
		if err != nil {
			t.Fatal(err)
		}
		out := filepath.Join(dir, filepath.Base(path))
		if err := os.WriteFile(out, data, 0o600); err != nil {
			t.Fatal(err)
		}
		replaced[path] = out
	}
	files := make([]string, 0, len(crds))
	for _, path := range crds {
		if out, ok := replaced[path]; ok {
			path = out
		}
		files = append(files, path)
	}
	return files
}

// checkVolSyncUnsupported fails the test unless the run's Ready condition
// has reason VolSyncUnsupported and a message that names the kind, the
// version the controller needs and the version the API server serves.
func checkVolSyncUnsupported(t *testing.T, conditions []metav1.Condition, kind string) {
	t.Helper()
	reason, message := readyReason(conditions), readyMessage(conditions)
	if reason != backupv1alpha1.ReasonVolSyncUnsupported {
		t.Fatalf("Ready reason = %q (%s), want %s", reason, message, backupv1alpha1.ReasonVolSyncUnsupported)
	}
	for _, want := range []string{kind, "volsync.backube/v1alpha1", "volsync.backube/v1beta1"} {
		if !strings.Contains(message, want) {
			t.Errorf("Ready message %q does not name %s", message, want)
		}
	}
}

// A BackupRun on a cluster whose VolSync no longer serves v1alpha1 ends
// Failed with reason VolSyncUnsupported, whose message names
// ReplicationSource, the version the controller needs and the version
// served, and never creates a ReplicationSource or stops anything on a
// guess.
func TestABackupEndsWhenVolSyncNoLongerServesV1alpha1(t *testing.T) {
	c := newClientWithCRDs(t, crdsWithVolSyncAt(t, "v1beta1"), backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment())
	served := servingOnly(c)
	r := &BackupRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	for range 4 {
		_ = tryStep(r)
	}

	run := readBackupRun(t, c)
	checkVolSyncUnsupported(t, run.Status.Conditions, "ReplicationSource")
	if len(run.Status.Quiesced) != 0 || replicasOf(t, c) != 2 {
		t.Errorf("quiesced = %v, replicas = %d; want nothing stopped", run.Status.Quiesced, replicasOf(t, c))
	}
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", run.Status.Phase)
	}
}

// A RestoreRun on such a cluster that has created nothing ends the same
// way, naming ReplicationDestination, and creates nothing.
func TestARestoreEndsWhenVolSyncNoLongerServesV1alpha1(t *testing.T) {
	c := newClientWithCRDs(t, crdsWithVolSyncAt(t, "v1beta1"),
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	served := servingOnly(c)
	r := &RestoreRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

	for range 3 {
		_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}})
	}

	run := readRestoreRun(t, c)
	checkVolSyncUnsupported(t, run.Status.Conditions, "ReplicationDestination")
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", run.Status.Phase)
	}
}

// destinationGone wraps c so that the API server answers every get of a
// ReplicationDestination with the plain-text 404 of a version it no longer
// serves, as after a VolSync upgrade the client's mapper has not seen yet.
func destinationGone(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*volsyncv1alpha1.ReplicationDestination); ok {
				return apierrors.NewGenericServerResponse(http.StatusNotFound, "get",
					schema.GroupResource{Group: volsyncv1alpha1.GroupVersion.Group, Resource: "replicationdestinations"}, key.Name, "404 page not found", 0, true)
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
}

// A ReplicationDestination read at a version the API server has stopped
// serving never reads as a destination someone deleted: the run keeps its
// item Running and retries, since the mover may still be writing.
func TestARestoreNeverTakesAVersionGoneForADeletedDestination(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // restore: the destination exists, the item runs
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("item = %+v, want Running", item)
	}

	gone := destinationGone(c)
	r.Client, r.Reader = gone, gone
	r.serve()
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}})
	if err == nil {
		t.Error("the pass returned no error, want the unserved version retried")
	}
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Errorf("item = %+v, want it still Running", item)
	}
}
