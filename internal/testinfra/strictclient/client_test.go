package strictclient_test

import (
	"context"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
)

// serverTime is the API server's clock in these tests. It has sub-second
// precision so the tests see the truncation a real server applies.
var serverTime = time.Date(2026, 9, 1, 12, 0, 0, 700_000_000, time.UTC)

func newClient(t *testing.T) *strictclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, backupv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	inner := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&backupv1alpha1.BackupRun{}).
		Build()
	return strictclient.New(inner, strictclient.Options{Clock: func() time.Time { return serverTime }})
}

func run(name string) *backupv1alpha1.BackupRun {
	return &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"},
		Spec:       backupv1alpha1.BackupRunSpec{Source: "data"},
	}
}

func get(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj); err != nil {
		t.Fatal(err)
	}
}

func TestCreateSetsServerFieldsIgnoringTheCallers(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := run("r")
	obj.CreationTimestamp = metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	obj.UID = types.UID("caller-chosen")
	obj.Generation = 7
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}

	stored := &backupv1alpha1.BackupRun{ObjectMeta: metav1.ObjectMeta{Name: "r", Namespace: "app"}}
	get(t, c, stored)
	for name, o := range map[string]*backupv1alpha1.BackupRun{"returned": obj, "stored": stored} {
		if want := serverTime.Truncate(time.Second); !o.CreationTimestamp.Time.Equal(want) {
			t.Errorf("%s creationTimestamp = %v, want %v", name, o.CreationTimestamp.Time, want)
		}
		if o.UID == "" || o.UID == "caller-chosen" {
			t.Errorf("%s uid = %q, want a server-generated uid", name, o.UID)
		}
		if o.Generation != 1 {
			t.Errorf("%s generation = %d, want 1", name, o.Generation)
		}
	}

	other := run("s")
	other.UID = obj.UID
	if err := c.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	if other.UID == obj.UID {
		t.Errorf("two creates got the same uid %q", other.UID)
	}
}

func TestCreateOfABuiltInKindLeavesGenerationAlone(t *testing.T) {
	c := newClient(t)
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "cm", Namespace: "app"}}
	if err := c.Create(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	if cm.Generation != 0 {
		t.Errorf("generation = %d, want 0", cm.Generation)
	}
	if cm.UID == "" || cm.CreationTimestamp.IsZero() {
		t.Errorf("uid %q, creationTimestamp %v: want both set", cm.UID, cm.CreationTimestamp)
	}
}

func TestUpdateKeepsServerFieldsAndBumpsGenerationOnSpecChange(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := run("r")
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	uid, created := obj.UID, obj.CreationTimestamp

	// A metadata-only change leaves the generation alone, and the caller cannot
	// move creationTimestamp, clear the uid or set the generation.
	obj.Labels = map[string]string{"a": "b"}
	obj.CreationTimestamp = metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	obj.UID = ""
	obj.Generation = 42
	if err := c.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
	stored := run("r")
	get(t, c, stored)
	if stored.Generation != 1 || obj.Generation != 1 {
		t.Errorf("generation after metadata update: stored %d, returned %d, want 1", stored.Generation, obj.Generation)
	}
	if stored.UID != uid || !stored.CreationTimestamp.Equal(&created) {
		t.Errorf("stored uid %q created %v, want %q %v", stored.UID, stored.CreationTimestamp, uid, created)
	}

	// A spec change bumps it by one.
	stored.Spec.Source = "other"
	if err := c.Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	if stored.Generation != 2 {
		t.Errorf("returned generation after spec update = %d, want 2", stored.Generation)
	}
	again := run("r")
	get(t, c, again)
	if again.Generation != 2 {
		t.Errorf("stored generation after spec update = %d, want 2", again.Generation)
	}

	// With a status subresource, a status change through the main resource is
	// dropped and does not count; one through the subresource does not either.
	again.Status.Phase = "Running"
	if err := c.Update(ctx, again); err != nil {
		t.Fatal(err)
	}
	again.Status.Phase = "Running"
	if err := c.Status().Update(ctx, again); err != nil {
		t.Fatal(err)
	}
	final := run("r")
	get(t, c, final)
	if final.Generation != 2 {
		t.Errorf("generation after status writes = %d, want 2", final.Generation)
	}
	if final.Status.Phase != "Running" {
		t.Errorf("status phase = %q, want Running", final.Status.Phase)
	}
}

func TestPatchKeepsServerFieldsAndBumpsGenerationOnSpecChange(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := run("r")
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	uid, created := obj.UID, obj.CreationTimestamp

	// A patch that changes the uid is refused, as the real server's
	// immutable-field check refuses it (differential_envtest_test.go).
	base := obj.DeepCopy()
	stale := obj.DeepCopy()
	stale.UID = "patched"
	stale.Spec.Source = "patched"
	if err := c.Patch(ctx, stale, client.MergeFrom(base)); !apierrors.IsInvalid(err) {
		t.Fatalf("patch changing the uid: got %v, want Invalid", err)
	}

	obj.Annotations = map[string]string{"x": "y"}
	obj.CreationTimestamp = metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	stored := run("r")
	get(t, c, stored)
	if stored.Generation != 1 || stored.UID != uid || !stored.CreationTimestamp.Equal(&created) {
		t.Errorf("after metadata patch: generation %d uid %q created %v, want 1 %q %v",
			stored.Generation, stored.UID, stored.CreationTimestamp, uid, created)
	}
	if obj.UID != uid || obj.Generation != 1 {
		t.Errorf("returned object: uid %q generation %d, want %q 1", obj.UID, obj.Generation, uid)
	}

	base = stored.DeepCopy()
	stored.Spec.Source = "other"
	if err := c.Patch(ctx, stored, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	if stored.Generation != 2 || stored.Spec.Source != "other" {
		t.Errorf("returned after spec patch: generation %d source %q, want 2 other", stored.Generation, stored.Spec.Source)
	}
	again := run("r")
	get(t, c, again)
	if again.Generation != 2 {
		t.Errorf("stored generation after spec patch = %d, want 2", again.Generation)
	}
}

func TestUnstructuredCustomResource(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(backupv1alpha1.GroupVersion.WithKind("BackupRun"))
	u.SetNamespace("app")
	u.SetName("u")
	u.SetUID("caller")
	if err := unstructured.SetNestedField(u.Object, "data", "spec", "source"); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(ctx, u); err != nil {
		t.Fatal(err)
	}
	if u.GetGeneration() != 1 || u.GetUID() == "caller" {
		t.Fatalf("after create: generation %d uid %q", u.GetGeneration(), u.GetUID())
	}
	if err := unstructured.SetNestedField(u.Object, "other", "spec", "source"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, u); err != nil {
		t.Fatal(err)
	}
	if u.GetGeneration() != 2 {
		t.Errorf("generation after spec update = %d, want 2", u.GetGeneration())
	}
}

func TestUpdateOfAMissingObjectIsNotFound(t *testing.T) {
	c := newClient(t)
	err := c.Update(context.Background(), run("missing"))
	if err == nil || !isNotFound(err) {
		t.Errorf("err = %v, want NotFound", err)
	}
}

func TestDefaultIsCustomResource(t *testing.T) {
	for group, want := range map[string]bool{
		"":                                false,
		"apps":                            false,
		"batch":                           false,
		"storage.k8s.io":                  false,
		"snapshot.storage.k8s.io":         true,
		"volsync.backube":                 true,
		"postgresql.cnpg.io":              true,
		backupv1alpha1.GroupVersion.Group: true,
	} {
		if got := strictclient.DefaultIsCustomResource(schema.GroupVersionKind{Group: group}); got != want {
			t.Errorf("DefaultIsCustomResource(%q) = %v, want %v", group, got, want)
		}
	}
}

func isNotFound(err error) bool { return apierrors.IsNotFound(err) }
