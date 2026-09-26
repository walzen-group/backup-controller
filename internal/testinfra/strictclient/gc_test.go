package strictclient_test

import (
	"context"
	"slices"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
)

func newGCClient(t *testing.T, collect bool) *strictclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, batchv1.AddToScheme, backupv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	inner := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&backupv1alpha1.BackupRun{}).
		Build()
	return strictclient.New(inner, strictclient.Options{
		Clock:          func() time.Time { return serverTime },
		GarbageCollect: collect,
	})
}

// ownedBy creates a ConfigMap whose ownerReferences name the given owners,
// with blockOwnerDeletion set as given, and the finalizers given.
func ownedBy(t *testing.T, c client.Client, name string, block bool, finalizers []string, owners ...client.Object) *corev1.ConfigMap {
	t.Helper()
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app", Finalizers: finalizers}}
	for _, o := range owners {
		gvk, err := apiutil.GVKForObject(o, c.Scheme())
		if err != nil {
			t.Fatal(err)
		}
		cm.OwnerReferences = append(cm.OwnerReferences, metav1.OwnerReference{
			APIVersion: gvk.GroupVersion().String(), Kind: gvk.Kind, Name: o.GetName(), UID: o.GetUID(),
			BlockOwnerDeletion: &block,
		})
	}
	if err := c.Create(context.Background(), cm); err != nil {
		t.Fatal(err)
	}
	return cm
}

func created(t *testing.T, c client.Client, obj client.Object) client.Object {
	t.Helper()
	if err := c.Create(context.Background(), obj); err != nil {
		t.Fatal(err)
	}
	return obj
}

func exists(t *testing.T, c client.Client, obj client.Object) bool {
	t.Helper()
	err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatal(err)
	}
	return true
}

func TestDeleteRecordsDependentsWithoutCollecting(t *testing.T) {
	ctx := context.Background()
	c := newGCClient(t, false)
	owner := created(t, c, run("r"))
	a := ownedBy(t, c, "a", true, nil, owner)
	b := ownedBy(t, c, "b", false, nil, owner)
	other := ownedBy(t, c, "other", false, nil)

	if err := c.Delete(ctx, run("r")); err != nil {
		t.Fatal(err)
	}
	got := c.Cascades()
	if len(got) != 1 {
		t.Fatalf("cascades = %+v, want one", got)
	}
	cs := got[0]
	if cs.Owner.UID != owner.GetUID() || cs.Owner.Kind != "BackupRun" || cs.Propagation != metav1.DeletePropagationBackground {
		t.Errorf("cascade = %+v, want the BackupRun, Background", cs)
	}
	if len(cs.Dependents) != 2 || cs.Dependents[0].Name != "a" || !cs.Dependents[0].BlockOwnerDeletion ||
		cs.Dependents[1].Name != "b" || cs.Dependents[1].BlockOwnerDeletion {
		t.Errorf("dependents = %+v, want a (blocking) and b", cs.Dependents)
	}
	for _, cm := range []*corev1.ConfigMap{a, b, other} {
		if !exists(t, c, cm) {
			t.Errorf("%s was deleted without GarbageCollect", cm.Name)
		}
	}
}

func TestBackgroundDeleteCollectsDependentsWithoutLiveOwners(t *testing.T) {
	ctx := context.Background()
	c := newGCClient(t, true)
	owner := created(t, c, run("r"))
	second := created(t, c, run("second"))
	only := ownedBy(t, c, "only", false, nil, owner)
	grandchild := ownedBy(t, c, "grandchild", false, nil, only)
	shared := ownedBy(t, c, "shared", true, nil, owner, second)

	if err := c.Delete(ctx, run("r")); err != nil {
		t.Fatal(err)
	}
	if exists(t, c, run("r")) || exists(t, c, only) || exists(t, c, grandchild) {
		t.Error("owner, its only dependent and that one's dependent should all be gone")
	}
	if !exists(t, c, shared) {
		t.Fatal("a dependent with a live owner left was deleted")
	}
	if len(shared.OwnerReferences) != 1 || shared.OwnerReferences[0].UID != second.GetUID() {
		t.Errorf("shared ownerReferences = %+v, want only the live owner", shared.OwnerReferences)
	}
	var names []string
	for _, cs := range c.Cascades() {
		names = append(names, cs.Owner.Name)
	}
	if !slices.Equal(names, []string{"r", "only", "grandchild"}) {
		t.Errorf("recorded deletes = %v, want r, only, grandchild", names)
	}
}

func TestBackgroundDeleteOfOwnerHeldByFinalizerKeepsDependents(t *testing.T) {
	c := newGCClient(t, true)
	obj := deleting(t, c, "r", "example.com/hold")
	dep := ownedBy(t, c, "dep", false, nil, obj)
	// A second delete call still leaves the owner in place.
	if err := c.Delete(context.Background(), run("r")); err != nil {
		t.Fatal(err)
	}
	if !exists(t, c, dep) {
		t.Error("dependent of an owner that still exists was deleted")
	}
}

func TestForegroundDeleteWaitsForBlockingDependentsOnly(t *testing.T) {
	ctx := context.Background()
	fg := client.PropagationPolicy(metav1.DeletePropagationForeground)

	t.Run("no dependent left", func(t *testing.T) {
		c := newGCClient(t, true)
		owner := created(t, c, run("r"))
		dep := ownedBy(t, c, "dep", true, nil, owner)
		if err := c.Delete(ctx, run("r"), fg); err != nil {
			t.Fatal(err)
		}
		if exists(t, c, run("r")) || exists(t, c, dep) {
			t.Error("owner and dependent should be gone")
		}
		got := c.Cascades()
		if len(got) != 2 || got[1].Owner.Name != "dep" || got[1].Propagation != metav1.DeletePropagationForeground {
			t.Errorf("cascades = %+v, want the dependent deleted with Foreground", got)
		}
	})

	t.Run("blocking dependent held by a finalizer", func(t *testing.T) {
		c := newGCClient(t, true)
		owner := created(t, c, run("r"))
		dep := ownedBy(t, c, "dep", true, []string{"example.com/hold"}, owner)
		if err := c.Delete(ctx, run("r"), fg); err != nil {
			t.Fatal(err)
		}
		stored := run("r")
		if !exists(t, c, stored) {
			t.Fatal("owner went while a blocking dependent is left")
		}
		if stored.DeletionTimestamp == nil || !slices.Contains(stored.Finalizers, metav1.FinalizerDeleteDependents) {
			t.Errorf("owner = %v %v, want a deletionTimestamp and the foregroundDeletion finalizer",
				stored.DeletionTimestamp, stored.Finalizers)
		}
		if !exists(t, c, dep) || dep.DeletionTimestamp == nil {
			t.Error("the blocking dependent should be left with a deletionTimestamp")
		}
	})

	t.Run("non-blocking dependent held by a finalizer", func(t *testing.T) {
		c := newGCClient(t, true)
		owner := created(t, c, run("r"))
		dep := ownedBy(t, c, "dep", false, []string{"example.com/hold"}, owner)
		if err := c.Delete(ctx, run("r"), fg); err != nil {
			t.Fatal(err)
		}
		if exists(t, c, run("r")) {
			t.Error("a dependent without blockOwnerDeletion held the owner")
		}
		if !exists(t, c, dep) || dep.DeletionTimestamp == nil {
			t.Error("the dependent should be left with a deletionTimestamp")
		}
	})
}

func TestOrphanDeleteRemovesOwnerReferences(t *testing.T) {
	c := newGCClient(t, true)
	owner := created(t, c, run("r"))
	dep := ownedBy(t, c, "dep", true, nil, owner)
	if err := c.Delete(context.Background(), run("r"), client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil {
		t.Fatal(err)
	}
	if exists(t, c, run("r")) {
		t.Error("owner should be gone")
	}
	if !exists(t, c, dep) || len(dep.OwnerReferences) != 0 {
		t.Errorf("dependent = %+v, want it kept without ownerReferences", dep.OwnerReferences)
	}
}

func TestBuiltInGeneration(t *testing.T) {
	ctx := context.Background()
	c := newGCClient(t, false)
	labels := map[string]string{"app": "x"}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "c", Image: "i"}}},
	}
	one := int32(1)
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "d", Namespace: "app"},
		Spec:       appsv1.DeploymentSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: template},
	}
	sts := &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: "s", Namespace: "app"},
		Spec:       appsv1.StatefulSetSpec{Replicas: &one, Selector: &metav1.LabelSelector{MatchLabels: labels}, Template: template},
	}
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "j", Namespace: "app"},
		Spec:       batchv1.JobSpec{Template: template},
	}
	for _, obj := range []client.Object{dep, sts, job} {
		created(t, c, obj)
	}
	if dep.Generation != 1 || sts.Generation != 1 || job.Generation != 0 {
		t.Fatalf("generations after create = %d %d %d, want 1 1 0", dep.Generation, sts.Generation, job.Generation)
	}

	step := func(name string, obj client.Object, mutate func(), want int64) {
		t.Helper()
		mutate()
		if err := c.Update(ctx, obj); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if obj.GetGeneration() != want {
			t.Errorf("%s: generation = %d, want %d", name, obj.GetGeneration(), want)
		}
	}
	two := int32(2)
	step("deployment status only", dep, func() { dep.Status.Replicas = 5 }, 1)
	step("deployment spec", dep, func() { dep.Spec.Replicas = &two }, 2)
	step("deployment annotation", dep, func() { dep.Annotations = map[string]string{"a": "b"} }, 3)
	step("deployment label", dep, func() { dep.Labels = map[string]string{"a": "b"} }, 3)
	step("statefulset spec", sts, func() { sts.Spec.Replicas = &two }, 2)
	step("statefulset annotation", sts, func() { sts.Annotations = map[string]string{"a": "b"} }, 2)
	step("job spec", job, func() { job.Spec.Parallelism = &two }, 0)

	patch := client.MergeFrom(sts.DeepCopy())
	sts.Spec.Replicas = &one
	if err := c.Patch(ctx, sts, patch); err != nil {
		t.Fatal(err)
	}
	if sts.Generation != 3 {
		t.Errorf("statefulset spec patch: generation = %d, want 3", sts.Generation)
	}
}
