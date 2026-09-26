package strictclient_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
)

// crdFiles lists the pinned backup-controller CRDs of one release.
func crdFiles(t *testing.T, version string) []string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join("..", "crds", "backup-controller", version, "*.yaml"))
	if err != nil || len(files) == 0 {
		t.Fatalf("no CRDs for %s: %v", version, err)
	}
	return files
}

func newCRDClient(t *testing.T, crds []string) *strictclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, backupv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return strictclient.Build(fake.NewClientBuilder(), scheme, strictclient.Options{
		Clock: func() time.Time { return serverTime },
		CRDs:  crds,
	})
}

// TestStatusWriteIsPrunedByTheInstalledCRD reproduces a v0.8.1 controller
// writing status.restartPending while the cluster still has the v0.7.2 CRD:
// the field is dropped on the way in and the read-back lacks it. Against the
// v0.8.1 CRD it is kept.
func TestStatusWriteIsPrunedByTheInstalledCRD(t *testing.T) {
	for _, tc := range []struct {
		crds string
		want bool
	}{
		{crds: "v0.7.2", want: false},
		{crds: "v0.8.1", want: true},
	} {
		t.Run(tc.crds, func(t *testing.T) {
			ctx := context.Background()
			c := newCRDClient(t, crdFiles(t, tc.crds))
			obj := run("r")
			if err := c.Create(ctx, obj); err != nil {
				t.Fatal(err)
			}

			obj.Status.Phase = backupv1alpha1.RunPhase("Running")
			obj.Status.RestartPending = true
			if err := c.Status().Update(ctx, obj); err != nil {
				t.Fatal(err)
			}
			if obj.Status.RestartPending != tc.want {
				t.Errorf("returned restartPending = %v, want %v", obj.Status.RestartPending, tc.want)
			}
			stored := run("r")
			get(t, c, stored)
			if stored.Status.RestartPending != tc.want {
				t.Errorf("stored restartPending = %v, want %v", stored.Status.RestartPending, tc.want)
			}
			if stored.Status.Phase != "Running" {
				t.Errorf("stored phase = %q, want the declared field kept", stored.Status.Phase)
			}

			// A status patch goes through the same pruning.
			stored.Status.RestartPending = false
			if err := c.Status().Update(ctx, stored); err != nil {
				t.Fatal(err)
			}
			patch := client.RawPatch(types.MergePatchType, []byte(`{"status":{"restartPending":true}}`))
			if err := c.Status().Patch(ctx, stored, patch); err != nil {
				t.Fatal(err)
			}
			get(t, c, stored)
			if stored.Status.RestartPending != tc.want {
				t.Errorf("after status patch restartPending = %v, want %v", stored.Status.RestartPending, tc.want)
			}
			if stored.Generation != 1 {
				t.Errorf("generation = %d, want 1 after status writes only", stored.Generation)
			}
		})
	}
}

// widgetCRD is a CRD whose spec keeps unknown fields and whose status does
// not, with a status subresource.
const widgetCRD = `apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  names: {kind: Widget, listKind: WidgetList, plural: widgets, singular: widget}
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
    subresources: {status: {}}
    schema:
      openAPIV3Schema:
        type: object
        properties:
          apiVersion: {type: string}
          kind: {type: string}
          metadata: {type: object}
          spec:
            type: object
            x-kubernetes-preserve-unknown-fields: true
            properties:
              size: {type: integer}
          status:
            type: object
            properties:
              ready: {type: boolean}
`

func widget(t *testing.T, content string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON([]byte(content)); err != nil {
		t.Fatal(err)
	}
	return u
}

func TestPruningKeepsPreserveUnknownFieldsAndDropsTheRest(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "widgets.yaml")
	if err := os.WriteFile(path, []byte(widgetCRD), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newCRDClient(t, []string{path})

	obj := widget(t, `{"apiVersion":"example.com/v1","kind":"Widget",
		"metadata":{"name":"w","namespace":"app"},
		"spec":{"size":3,"colour":"red"},"extra":"x","status":{"ready":true}}`)
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	stored := widget(t, `{"apiVersion":"example.com/v1","kind":"Widget","metadata":{"name":"w","namespace":"app"}}`)
	get(t, c, stored)
	if got, _, _ := unstructured.NestedString(stored.Object, "spec", "colour"); got != "red" {
		t.Errorf("spec.colour = %q, want it kept under x-kubernetes-preserve-unknown-fields", got)
	}
	if _, found := stored.Object["extra"]; found {
		t.Error("undeclared top-level field kept, want it pruned")
	}
	if _, found := stored.Object["status"]; found {
		t.Error("create kept status, want it dropped for a kind with a status subresource")
	}

	if err := unstructured.SetNestedField(stored.Object, map[string]any{"ready": true, "note": "n"}, "status"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	get(t, c, stored)
	if ready, _, _ := unstructured.NestedBool(stored.Object, "status", "ready"); !ready {
		t.Error("status.ready lost, want the declared field kept")
	}
	if _, found, _ := unstructured.NestedFieldNoCopy(stored.Object, "status", "note"); found {
		t.Error("status.note kept, want it pruned")
	}

	patch := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"shape":"round"},"extra":"y"}`))
	if err := c.Patch(ctx, stored, patch); err != nil {
		t.Fatal(err)
	}
	get(t, c, stored)
	if got, _, _ := unstructured.NestedString(stored.Object, "spec", "shape"); got != "round" {
		t.Errorf("spec.shape = %q after patch, want it kept", got)
	}
	if _, found := stored.Object["extra"]; found {
		t.Error("patch stored an undeclared top-level field, want it pruned")
	}
	if stored.GetGeneration() != 2 {
		t.Errorf("generation = %d, want 2 after one spec change", stored.GetGeneration())
	}
}

// TestStatusSubresourceComesFromTheCRD checks that a kind whose CRD declares
// subresources.status splits spec and status writes without a
// WithStatusSubresource call in the test.
func TestStatusSubresourceComesFromTheCRD(t *testing.T) {
	ctx := context.Background()
	c := newCRDClient(t, crdFiles(t, "v0.8.1"))
	obj := run("r")
	obj.Status.Phase = "Running"
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if obj.Status.Phase != "" {
		t.Errorf("create kept status.phase %q, want it dropped", obj.Status.Phase)
	}

	obj.Spec.Source = "changed-by-update"
	obj.Status.Phase = "Running"
	if err := c.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
	stored := run("r")
	get(t, c, stored)
	if stored.Spec.Source != "changed-by-update" || stored.Status.Phase != "" {
		t.Errorf("after update spec.source = %q, status.phase = %q; want the spec change and no status",
			stored.Spec.Source, stored.Status.Phase)
	}

	stored.Spec.Source = "changed-by-status"
	stored.Status.Phase = "Succeeded"
	if err := c.Status().Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	get(t, c, stored)
	if stored.Spec.Source != "changed-by-update" || stored.Status.Phase != "Succeeded" {
		t.Errorf("after status update spec.source = %q, status.phase = %q; want the old spec and the new status",
			stored.Spec.Source, stored.Status.Phase)
	}
}

func TestUpdateWithAnotherUIDConflicts(t *testing.T) {
	ctx := context.Background()
	c := newCRDClient(t, crdFiles(t, "v0.8.1"))
	obj := run("r")
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	uid := obj.UID

	for name, write := range map[string]func(client.Object) error{
		"update":        func(o client.Object) error { return c.Update(ctx, o) },
		"status update": func(o client.Object) error { return c.Status().Update(ctx, o) },
	} {
		t.Run(name, func(t *testing.T) {
			o := run("r")
			get(t, c, o)
			o.UID = "someone-else"
			o.Spec.Source = "other"
			err := write(o)
			if !apierrors.IsConflict(err) {
				t.Fatalf("err = %v, want Conflict", err)
			}
			want := "Precondition failed: UID in precondition: someone-else, UID in object meta: " + string(uid)
			if !strings.Contains(err.Error(), want) {
				t.Errorf("message = %q, want it to contain %q", err.Error(), want)
			}
			stored := run("r")
			get(t, c, stored)
			if stored.UID != uid || stored.Spec.Source != "data" {
				t.Errorf("stored uid = %q, source = %q; want the object unchanged", stored.UID, stored.Spec.Source)
			}
		})
	}

	// An update without a uid, or with the stored one, goes through.
	o := run("r")
	get(t, c, o)
	o.UID = ""
	o.Spec.Source = "other"
	if err := c.Update(ctx, o); err != nil {
		t.Fatalf("update without uid: %v", err)
	}
	if o.UID != uid {
		t.Errorf("uid = %q, want the stored %q", o.UID, uid)
	}
	o.Spec.Source = "again"
	if err := c.Update(ctx, o); err != nil {
		t.Fatalf("update with the stored uid: %v", err)
	}
}

// TestBuildTakesKindsRegisteredAsUnstructured registers BackupRun as
// unstructured, as tests do for kinds without Go types (CNPG, Flux, Kueue),
// and checks Build gives it the status subresource its CRD declares.
func TestBuildTakesKindsRegisteredAsUnstructured(t *testing.T) {
	ctx := context.Background()
	gvk := backupv1alpha1.GroupVersion.WithKind("BackupRun")
	scheme := runtime.NewScheme()
	scheme.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
	scheme.AddKnownTypeWithName(gvk.GroupVersion().WithKind("BackupRunList"), &unstructured.UnstructuredList{})
	c := strictclient.Build(fake.NewClientBuilder(), scheme, strictclient.Options{
		Clock: func() time.Time { return serverTime },
		CRDs:  crdFiles(t, "v0.8.1"),
	})

	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(gvk)
	obj.SetNamespace("app")
	obj.SetName("r")
	if err := unstructured.SetNestedField(obj.Object, "data", "spec", "source"); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(obj.Object, "Running", "status", "phase"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if phase, _, _ := unstructured.NestedString(obj.Object, "status", "phase"); phase != "" {
		t.Errorf("a plain update stored status.phase %q, want it dropped by the status subresource", phase)
	}
}

// TestCRDDefaultsAreApplied checks that RestoreRun's spec.timeout default
// (4h in the pinned v0.8.1 CRD) is filled in on create and on an update or
// patch that clears it, and that a value the writer sets is kept.
func TestCRDDefaultsAreApplied(t *testing.T) {
	ctx := context.Background()
	c := newCRDClient(t, crdFiles(t, "v0.8.1"))
	r := &backupv1alpha1.RestoreRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "r"},
		Spec:       backupv1alpha1.RestoreRunSpec{Claim: "data"},
	}
	if err := c.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Spec.Timeout == nil || r.Spec.Timeout.Duration != 4*time.Hour {
		t.Errorf("create: timeout = %v, want the 4h default", r.Spec.Timeout)
	}
	r.Spec.Timeout = &metav1.Duration{Duration: time.Minute}
	if err := c.Update(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Spec.Timeout == nil || r.Spec.Timeout.Duration != time.Minute {
		t.Errorf("update: timeout = %v, want the 1m the writer set", r.Spec.Timeout)
	}
	r.Spec.Timeout = nil
	if err := c.Update(ctx, r); err != nil {
		t.Fatal(err)
	}
	if r.Spec.Timeout == nil || r.Spec.Timeout.Duration != 4*time.Hour {
		t.Errorf("update clearing it: timeout = %v, want the 4h default", r.Spec.Timeout)
	}
	patch := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"timeout":null}}`))
	if err := c.Patch(ctx, r, patch); err != nil {
		t.Fatal(err)
	}
	stored := &backupv1alpha1.RestoreRun{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "r"}}
	get(t, c, stored)
	if stored.Spec.Timeout == nil || stored.Spec.Timeout.Duration != 4*time.Hour {
		t.Errorf("patch removing it: stored timeout = %v, want the 4h default", stored.Spec.Timeout)
	}
}

// TestBuildCoercesSeededObjects checks that Build brings seeded objects to
// a state the server could have stored: an unknown field is pruned, a
// default is filled in, the status is kept, and the uid, creationTimestamp
// and generation get server values when the test left them empty and keep
// the test's values otherwise.
func TestBuildCoercesSeededObjects(t *testing.T) {
	ctx := context.Background()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, backupv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	bare := &backupv1alpha1.RestoreRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "bare"},
		Spec:       backupv1alpha1.RestoreRunSpec{Claim: "data"},
		Status:     backupv1alpha1.RestoreRunStatus{Phase: "Running"},
	}
	old := metav1.NewTime(serverTime.Add(-time.Hour).Truncate(time.Second))
	set := &backupv1alpha1.RestoreRun{
		ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "set", UID: "fixed", Generation: 3, CreationTimestamp: old},
		Spec:       backupv1alpha1.RestoreRunSpec{Claim: "data"},
	}
	third := &unstructured.Unstructured{}
	third.SetGroupVersionKind(backupv1alpha1.GroupVersion.WithKind("BackupRun"))
	third.SetNamespace("app")
	third.SetName("unknown-field")
	third.Object["spec"] = map[string]any{"source": "data", "notInSchema": "dropped"}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "p"}}
	c := strictclient.Build(fake.NewClientBuilder().WithObjects(bare, set, third, pod), scheme, strictclient.Options{
		Clock: func() time.Time { return serverTime },
		CRDs:  crdFiles(t, "v0.8.1"),
	})

	got := &backupv1alpha1.RestoreRun{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "bare"}}
	get(t, c, got)
	if got.Generation != 1 || got.UID == "" || !got.CreationTimestamp.Time.Equal(serverTime.Truncate(time.Second)) {
		t.Errorf("bare: generation %d, uid %q, created %v; want 1, a fresh uid, the server clock",
			got.Generation, got.UID, got.CreationTimestamp)
	}
	if got.Spec.Timeout == nil || got.Spec.Timeout.Duration != 4*time.Hour {
		t.Errorf("bare: timeout = %v, want the 4h default", got.Spec.Timeout)
	}
	if got.Status.Phase != "Running" {
		t.Errorf("bare: status phase = %q, want the seeded Running kept", got.Status.Phase)
	}
	got = &backupv1alpha1.RestoreRun{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "set"}}
	get(t, c, got)
	if got.Generation != 3 || got.UID != "fixed" || !got.CreationTimestamp.Equal(&old) {
		t.Errorf("set: generation %d, uid %q, created %v; want the seeded 3, fixed, %v",
			got.Generation, got.UID, got.CreationTimestamp, old)
	}
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(backupv1alpha1.GroupVersion.WithKind("BackupRun"))
	if err := c.Get(ctx, client.ObjectKey{Namespace: "app", Name: "unknown-field"}, u); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := unstructured.NestedString(u.Object, "spec", "notInSchema"); found {
		t.Errorf("unknown-field: spec.notInSchema kept, want it pruned")
	}
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "app", Name: "p"}}
	get(t, c, p)
	if p.UID == "" {
		t.Errorf("pod: uid is empty, want a fresh one")
	}
}
