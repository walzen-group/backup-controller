package runs

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"
)

// nextVersion is the version a future CloudNativePG or barman-cloud release
// is imagined to move its kinds to. The tests serve the kinds at it alone,
// with the fields of the pinned v1.
const nextVersion = "v2"

// cnpgFiles are the pinned CRD files of the CloudNativePG and barman-cloud
// kinds the controller reads and writes.
var cnpgFiles = []string{
	crdDir + "cloudnative-pg/postgresql.cnpg.io_clusters.yaml",
	crdDir + "cloudnative-pg/postgresql.cnpg.io_backups.yaml",
	crdDir + "plugin-barman-cloud/barmancloud.cnpg.io_objectstores.yaml",
}

// crdsServedAtNext returns the CRD files of newClient with the CloudNativePG
// Cluster and Backup and the barman-cloud ObjectStore served at nextVersion
// alone: each pinned CRD's one version is renamed, and its schema stays as
// it is. This stands in for a release that moves the kinds to a new version
// and stops serving v1, with the same fields. A pinned CRD with more than one
// version fails the test, since the rename would then drop a version.
func crdsServedAtNext(t *testing.T) []string {
	t.Helper()
	dir := t.TempDir()
	replaced := map[string]string{}
	for _, path := range cnpgFiles {
		crd := readCRD(t, path)
		versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
		if len(versions) != 1 {
			t.Fatalf("%s has %d versions, want v1 alone", path, len(versions))
		}
		versions[0].(map[string]any)["name"] = nextVersion
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

// atNext moves an unstructured object to nextVersion, for seeding a client
// whose CRDs crdsServedAtNext wrote.
func atNext(u *unstructured.Unstructured) *unstructured.Unstructured {
	u.SetGroupVersionKind(u.GroupVersionKind().GroupKind().WithVersion(nextVersion))
	return u
}

// nextGVK returns gvk at nextVersion.
func nextGVK(gvk schema.GroupVersionKind) schema.GroupVersionKind {
	return gvk.GroupKind().WithVersion(nextVersion)
}

// A database backup on a cluster whose CloudNativePG serves Cluster and
// Backup at a new version alone, with the same fields, works as before: the
// run finds the Cluster, creates its Backup at that version, and succeeds
// once the Backup completes.
func TestADatabaseIsBackedUpAtTheVersionCloudNativePGServes(t *testing.T) {
	c := newClientWithCRDs(t, crdsServedAtNext(t), backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), atNext(cluster()))
	served := servingOnly(c)
	r := &BackupRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // start

	backup, ok := getUnstructured(t, c, nextGVK(cnpg.BackupGVK), ns, cnpg.BackupName(pgN, runUID))
	if !ok {
		t.Fatalf("no Backup at %s; run = %+v", nextVersion, readBackupRun(t, c).Status)
	}
	_ = unstructured.SetNestedField(backup.Object, "completed", "status", "phase")
	if err := c.Status().Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	step(t, r)

	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded; status = %+v", run.Status.Phase, run.Status)
	}
}

// clusterListGone wraps c so that the API server answers every Cluster list
// at v1 with the plain-text 404 of a version it no longer serves, as after a
// CloudNativePG upgrade the client's mapper has not seen yet.
func clusterListGone(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if u, ok := list.(*unstructured.UnstructuredList); ok && u.GroupVersionKind() == cnpg.ClusterGVK.GroupVersion().WithKind("ClusterList") {
				return apierrors.NewGenericServerResponse(http.StatusNotFound, "list",
					schema.GroupResource{Group: cnpg.ClusterGVK.Group, Resource: "clusters"}, "", "404 page not found", 0, true)
			}
			return cl.List(ctx, list, opts...)
		},
	})
}

// A Cluster list at a version the API server has stopped serving never reads
// as a namespace without Clusters: the plan fails so it runs again, and the
// run is not planned without its database.
func TestAClusterListAtAVersionNoLongerServedIsRetried(t *testing.T) {
	c := newClient(t, backupRun(), claim(), volume(), volumeRestore(), repository(), cluster())
	gone := clusterListGone(c)
	r := &BackupRunReconciler{Client: gone, Reader: gone, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	err := tryStep(r)
	if err == nil {
		t.Error("the plan pass returned no error, want the unserved version retried")
	}
	if run := readBackupRun(t, c); len(run.Status.Items) != 0 {
		t.Errorf("items = %+v, want the run not planned without its Cluster", run.Status.Items)
	}
}
