package runs

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
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
