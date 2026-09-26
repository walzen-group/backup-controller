//go:build envtest

// The strict-validation envtest checks, against envtest's kube-apiserver
// (the Kubernetes version versions.json pins), what the API server does with
// a field its CRD does not declare, with and without the field validation
// the controller's clients ask for.
//
//	nix develop .#envtest -c go test -tags envtest ./cmd/backup-controller/ -run Envtest
package main

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// crdDir is the folder of pinned third-party CRDs.
const crdDir = "../../internal/testinfra/crds/"

// The kinds the test writes: a CloudNativePG Backup, which a BackupRun
// creates, and a Flux Kustomization, which a quiesce patches.
var (
	backupGVK        = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Backup"}
	kustomizationGVK = schema.GroupVersionKind{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization"}
)

// backupWith returns the Backup a BackupRun creates, in namespace app, with
// field set under spec to true. A field its CRD does not declare stands in
// for one a CloudNativePG release renamed or removed.
func backupWith(name, field string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{
		"method":              "plugin",
		"pluginConfiguration": map[string]any{"name": "barman-cloud.cloudnative-pg.io"},
		"cluster":             map[string]any{"name": "app-pg"},
		field:                 true,
	}}}
	u.SetGroupVersionKind(backupGVK)
	u.SetNamespace("app")
	u.SetName(name)
	return u
}

// A field the CRD does not declare is dropped without an error by a write
// that asks for no field validation: kube-apiserver defaults the directive to
// Warn (k8s.io/apiserver pkg/endpoints/handlers/rest.go, fieldValidation), and
// a custom resource's unknown fields are then pruned with a warning. A
// client built with clientOptions asks for Strict, so the same create, and a
// merge patch like setSuspend's with a misspelt field, fail with an error
// that names the field, while setSuspend's own patch still goes through.
func TestEnvtestTheControllersWritesRefuseUnknownFields(t *testing.T) {
	ctx := context.Background()
	env := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDir + "cloudnative-pg/postgresql.cnpg.io_backups.yaml", crdDir + "flux/kustomize-controller.crds.yaml"},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	plain, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	controller, err := client.New(cfg, clientOptions(s))
	if err != nil {
		t.Fatal(err)
	}
	if err := plain.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "app"}}); err != nil {
		t.Fatal(err)
	}

	// The server's default drops the field and succeeds.
	if err := plain.Create(ctx, backupWith("default", "renamedField")); err != nil {
		t.Fatalf("create with the server's default validation: %v", err)
	}
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(backupGVK)
	if err := plain.Get(ctx, types.NamespacedName{Namespace: "app", Name: "default"}, stored); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(stored.Object, "spec", "renamedField"); found {
		t.Fatal("the API server kept a field the CRD does not declare; the pruning this test documents is gone")
	}

	// The controller's client refuses it.
	err = controller.Create(ctx, backupWith("strict", "renamedField"))
	if err == nil || !strings.Contains(err.Error(), `unknown field "spec.renamedField"`) {
		t.Errorf("create through clientOptions = %v, want an error naming spec.renamedField", err)
	}

	kustomization := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{
		"interval":  "10m",
		"path":      "./apps/app",
		"prune":     true,
		"sourceRef": map[string]any{"kind": "GitRepository", "name": "flux-system"},
	}}}
	kustomization.SetGroupVersionKind(kustomizationGVK)
	kustomization.SetNamespace("app")
	kustomization.SetName("app")
	if err := plain.Create(ctx, kustomization); err != nil {
		t.Fatal(err)
	}
	patch := func(c client.Client, body string) error {
		k := &unstructured.Unstructured{}
		k.SetGroupVersionKind(kustomizationGVK)
		k.SetNamespace("app")
		k.SetName("app")
		return c.Patch(ctx, k, client.RawPatch(types.MergePatchType, []byte(body)))
	}
	if err := patch(plain, `{"spec":{"suspended":true}}`); err != nil {
		t.Errorf("a merge patch with a misspelt field and the server's default = %v, want it dropped without an error", err)
	}
	if err := patch(controller, `{"spec":{"suspended":true}}`); err == nil || !strings.Contains(err.Error(), `unknown field "spec.suspended"`) {
		t.Errorf("a merge patch with a misspelt field through clientOptions = %v, want an error naming spec.suspended", err)
	}
	if err := patch(controller, `{"spec":{"suspend":true}}`); err != nil {
		t.Errorf("setSuspend's patch through clientOptions = %v, want it applied", err)
	}
}
