//go:build envtest

// The version-gone envtest checks, against envtest's kube-apiserver (the
// Kubernetes version versions.json pins), the answers served.VersionGone tells
// apart: a request at a Kustomization version the API server no longer
// serves, and a request for a Kustomization that doesn't exist at a version
// it serves. It also drives quiesce.SetSuspend through controller-runtime's lazy
// RESTMapper across such a change.
//
//	nix develop .#envtest -c go test -tags envtest ./internal/runs/ -run Envtest
package runs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/quiesce"
	"github.com/walzen-group/backup-controller/internal/served"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/discovery"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

// twoVersionKustomizationCRD writes the pinned Flux Kustomization CRD into a
// new folder with a second served version, v2, whose schema is v1's, and
// returns the folder. v1 stays the storage version. The API server prefers
// v2, as it would after a Flux release that adds a version.
func twoVersionKustomizationCRD(t *testing.T) string {
	t.Helper()
	crd := readCRD(t, crdDir+"flux/kustomize-controller.crds.yaml")
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	if len(versions) != 1 {
		t.Fatalf("the pinned Kustomization CRD has %d versions, want v1 alone", len(versions))
	}
	v2 := runtime.DeepCopyJSONValue(versions[0]).(map[string]any)
	v2["name"], v2["storage"], v2["served"] = "v2", false, true
	if err := unstructured.SetNestedSlice(crd.Object, append(versions, v2), "spec", "versions"); err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(crd.Object)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "kustomizations.yaml"), data, 0o600); err != nil {
		t.Fatal(err)
	}
	return dir
}

// kustomizationAt returns an empty Kustomization at the given version with
// the given name in the test namespace, ready for a Get or a Patch.
func kustomizationAt(version, name string) *unstructured.Unstructured {
	k := &unstructured.Unstructured{}
	k.SetGroupVersionKind(quiesce.KustomizationGVK.GroupKind().WithVersion(version))
	k.SetNamespace(ns)
	k.SetName(name)
	return k
}

// TestEnvtestAVersionNoLongerServedIsNotAMissingObject checks the assumption
// served.VersionGone rests on, on a real kube-apiserver. After the API server stops
// serving a Kustomization version, a get and a patch at that version fail
// with a NotFound that apierrors.IsUnexpectedServerError reports, while a get
// of a Kustomization that doesn't exist, at a version it serves, fails with
// a NotFound that it does not report. quiesce.SetSuspend through the lazy RESTMapper
// that cached the old version then fails once with a *served.VersionGoneError, for
// which apierrors.IsNotFound is false, and the next call suspends the
// Kustomization at the version the API server serves.
func TestEnvtestAVersionNoLongerServedIsNotAMissingObject(t *testing.T) {
	ctx := context.Background()
	env := &envtest.Environment{CRDDirectoryPaths: []string{twoVersionKustomizationCRD(t)}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	dc, err := discovery.NewDiscoveryClientForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := dc.ServerVersion()
	if want := "v" + versions.Of(t, "kubernetes"); err != nil || server.GitVersion != want {
		t.Fatalf("kube-apiserver version = %v, %v; want %s, the version versions.json pins", server, err, want)
	}
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	s.AddKnownTypeWithName(crdGVK, &unstructured.Unstructured{})
	// client.New gives the client controller-runtime's lazy RESTMapper, the
	// one the manager uses.
	c, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	app := kustomizationAt("v1", appN)
	app.Object["spec"] = map[string]any{
		"interval":  "10m",
		"path":      "./apps/notes",
		"prune":     true,
		"sourceRef": map[string]any{"kind": "GitRepository", "name": "flux-system"},
	}
	if err := c.Create(ctx, app); err != nil {
		t.Fatalf("create the Kustomization: %v", err)
	}

	gvk, err := served.Kind(c.RESTMapper(), quiesce.KustomizationGVK.GroupKind())
	if err != nil || gvk.Version != "v2" {
		t.Fatalf("served.Kind = %v, %v; want v2, the version the API server prefers", gvk, err)
	}
	if err := quiesce.SetSuspend(ctx, c, ns, appN, true); err != nil {
		t.Fatalf("suspend at v2 while it is served: %v", err)
	}

	// Flux stops serving v2.
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(crdGVK)
	if err := c.Get(ctx, types.NamespacedName{Name: "kustomizations." + quiesce.KustomizationGVK.Group}, crd); err != nil {
		t.Fatal(err)
	}
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	for _, v := range versions {
		if version := v.(map[string]any); version["name"] == "v2" {
			version["served"] = false
		}
	}
	if err := unstructured.SetNestedSlice(crd.Object, versions, "spec", "versions"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, crd); err != nil {
		t.Fatalf("stop serving v2: %v", err)
	}

	// The API server takes a moment to stop serving v2. The client's mapper
	// still maps v2, so the request goes to the v2 path.
	var getErr error
	for deadline := time.Now().Add(30 * time.Second); ; time.Sleep(200 * time.Millisecond) {
		getErr = c.Get(ctx, client.ObjectKeyFromObject(app), kustomizationAt("v2", appN))
		if getErr != nil || time.Now().After(deadline) {
			break
		}
	}
	if !apierrors.IsNotFound(getErr) || !apierrors.IsUnexpectedServerError(getErr) {
		t.Fatalf("get at the unserved v2 = %v (IsNotFound %t, IsUnexpectedServerError %t); want both true",
			getErr, apierrors.IsNotFound(getErr), apierrors.IsUnexpectedServerError(getErr))
	}
	patchErr := c.Patch(ctx, kustomizationAt("v2", appN), client.RawPatch(types.MergePatchType, []byte(`{"spec":{"suspend":false}}`)))
	if !apierrors.IsNotFound(patchErr) || !apierrors.IsUnexpectedServerError(patchErr) {
		t.Fatalf("patch at the unserved v2 = %v (IsNotFound %t, IsUnexpectedServerError %t); want both true",
			patchErr, apierrors.IsNotFound(patchErr), apierrors.IsUnexpectedServerError(patchErr))
	}
	missingErr := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: "no-such-app"}, kustomizationAt("v1", "no-such-app"))
	if !apierrors.IsNotFound(missingErr) || apierrors.IsUnexpectedServerError(missingErr) {
		t.Fatalf("get of a missing Kustomization at the served v1 = %v (IsNotFound %t, IsUnexpectedServerError %t); want NotFound alone",
			missingErr, apierrors.IsNotFound(missingErr), apierrors.IsUnexpectedServerError(missingErr))
	}

	err = quiesce.SetSuspend(ctx, c, ns, appN, false)
	var gone *served.VersionGoneError
	if !errors.As(err, &gone) || apierrors.IsNotFound(err) {
		t.Fatalf("resume at the cached v2 = %v; want a *served.VersionGoneError that is not NotFound", err)
	}
	if err := quiesce.SetSuspend(ctx, c, ns, appN, false); err != nil {
		t.Fatalf("resume on the next pass: %v; want it done at v1", err)
	}
	got := kustomizationAt("v1", appN)
	if err := c.Get(ctx, client.ObjectKeyFromObject(got), got); err != nil {
		t.Fatal(err)
	}
	if suspended, _, _ := unstructured.NestedBool(got.Object, "spec", "suspend"); suspended {
		t.Error("the Kustomization is still suspended after the resume at v1")
	}
}
