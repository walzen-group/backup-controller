package strictclient_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// cnpgClusterCRD is the pinned CloudNativePG Cluster CRD: a third-party kind
// whose version declares a status subresource, written as an unstructured
// object, as the run manager writes it.
var cnpgClusterCRD = filepath.Join("..", "crds", "cloudnative-pg", "postgresql.cnpg.io_clusters.yaml")

var clusterGVK = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}

// unhealthyPhase and healthyPhase are phases of a CloudNativePG Cluster (the
// schema takes any string).
const (
	unhealthyPhase = "Cluster in recovery"
	healthyPhase   = "Cluster in healthy state"
)

// clusterObject returns an unstructured Cluster as a test writes it.
func clusterObject(name string) *unstructured.Unstructured {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(clusterGVK)
	u.SetNamespace("app")
	u.SetName(name)
	if err := unstructured.SetNestedField(u.Object, int64(1), "spec", "instances"); err != nil {
		panic(err)
	}
	return u
}

// readCluster reads the Cluster of the given name so the test sees the stored
// object.
func readCluster(t *testing.T, c client.Client, name string) *unstructured.Unstructured {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(clusterGVK)
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: "app", Name: name}, u); err != nil {
		t.Fatal(err)
	}
	return u
}

// wantNoStatusKey fails when obj carries a status key at all. On
// kube-apiserver 1.36.3 an object whose status was never written has none:
// the codec prunes a null status on every decode
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/apiserver/customresource_handler.go:1430-1440).
func wantNoStatusKey(t *testing.T, step string, obj *unstructured.Unstructured) {
	t.Helper()
	if status, found := obj.Object["status"]; found {
		t.Errorf("%s: status = %v, want no status key", step, status)
	}
}

// TestStatusOfAClusterBeingDeleted covers the status of an unstructured
// third-party object with a finalizer that is being deleted, which RR2's
// being_deleted test had to work around: the fake stored a null status on the
// first write, so unstructured.SetNestedField, markHealthy in that test,
// reported an error and the Cluster stayed unhealthy. kube-apiserver 1.36.3
// stores no status key at all and admits a status write while the finalizer
// holds the object.
func TestStatusOfAClusterBeingDeleted(t *testing.T) {
	ctx := context.Background()
	c := newCRDClient(t, []string{cnpgClusterCRD})

	cluster := clusterObject("held")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	wantNoStatusKey(t, "after create", cluster)

	cluster.SetFinalizers([]string{"example.com/hold"})
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	wantNoStatusKey(t, "after the finalizer write", cluster)
	if err := unstructured.SetNestedField(cluster.Object, unhealthyPhase, "status", "phase"); err != nil {
		t.Fatalf("writing a status on the object the update returned: %v", err)
	}

	stale := cluster.DeepCopy()
	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.GetDeletionTimestamp() != nil {
		t.Errorf("the delete set a deletionTimestamp on the caller's object, want it left alone")
	}
	stored := readCluster(t, c, "held")
	if stored.GetDeletionTimestamp() == nil {
		t.Fatal("the finalizer did not hold the Cluster in deletion")
	}
	wantNoStatusKey(t, "after the delete", stored)

	// A status write from the stored object is admitted, and the status is
	// readable afterwards.
	fresh := readCluster(t, c, "held")
	if err := unstructured.SetNestedField(fresh.Object, healthyPhase, "status", "phase"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, fresh); err != nil {
		t.Fatalf("status update while the finalizer holds the Cluster: %v", err)
	}
	stored = readCluster(t, c, "held")
	if phase, _, _ := unstructured.NestedString(stored.Object, "status", "phase"); phase != healthyPhase {
		t.Errorf("stored phase after the status update = %q, want %q", phase, healthyPhase)
	}

	// A status write from the object as it was before the delete carries the
	// resourceVersion from before it, which the server refuses.
	if err := unstructured.SetNestedField(stale.Object, unhealthyPhase, "status", "phase"); err != nil {
		t.Fatal(err)
	}
	err := c.Status().Update(ctx, stale)
	if !apierrors.IsConflict(err) {
		t.Fatalf("status update with the resourceVersion from before the delete: err = %v, want Conflict", err)
	}
	stored = readCluster(t, c, "held")
	if phase, _, _ := unstructured.NestedString(stored.Object, "status", "phase"); phase != healthyPhase {
		t.Errorf("stored phase after the refused status update = %q, want %q", phase, healthyPhase)
	}

	// A status the object already had stays readable through the delete.
	other := clusterObject("had-status")
	if err := c.Create(ctx, other); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(other.Object, unhealthyPhase, "status", "phase"); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(ctx, other); err != nil {
		t.Fatal(err)
	}
	other.SetFinalizers([]string{"example.com/hold"})
	if err := c.Update(ctx, other); err != nil {
		t.Fatal(err)
	}
	if phase, _, _ := unstructured.NestedString(other.Object, "status", "phase"); phase != unhealthyPhase {
		t.Errorf("phase on the object the update returned = %q, want %q", phase, unhealthyPhase)
	}
	if err := c.Delete(ctx, other); err != nil {
		t.Fatal(err)
	}
	stored = readCluster(t, c, "had-status")
	if stored.GetDeletionTimestamp() == nil {
		t.Fatal("the finalizer did not hold the second Cluster in deletion")
	}
	if phase, _, _ := unstructured.NestedString(stored.Object, "status", "phase"); phase != unhealthyPhase {
		t.Errorf("phase after the delete = %q, want the stored %q", phase, unhealthyPhase)
	}
}

// TestDeleteWithFinalizersBumpsTheGeneration checks that the delete which
// sets a deletionTimestamp raises the generation, as kube-apiserver 1.36.3
// does for every kind: markAsDeleting raises it for a custom resource
// (k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:1024-1029)
// and rest.BeforeDelete for the kinds that support graceful deletion
// (k8s.io/apiserver@v0.36.3/pkg/registry/rest/delete.go:168-172). The wrapper
// has one rule for both, tested here on custom resources, and a second delete
// of the same object leaves the generation alone.
func TestDeleteWithFinalizersBumpsTheGeneration(t *testing.T) {
	ctx := context.Background()
	c := newCRDClient(t, []string{cnpgClusterCRD})

	cluster := clusterObject("bumped")
	if err := c.Create(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.GetGeneration() != 1 {
		t.Fatalf("generation after create = %d, want 1", cluster.GetGeneration())
	}
	cluster.SetFinalizers([]string{"example.com/hold"})
	if err := c.Update(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	if cluster.GetGeneration() != 1 {
		t.Errorf("generation after a finalizer write = %d, want 1", cluster.GetGeneration())
	}
	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	stored := readCluster(t, c, "bumped")
	if stored.GetGeneration() != 2 {
		t.Errorf("generation after the delete = %d, want 2", stored.GetGeneration())
	}
	if err := c.Delete(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	stored = readCluster(t, c, "bumped")
	if stored.GetGeneration() != 2 {
		t.Errorf("generation after a second delete = %d, want 2", stored.GetGeneration())
	}

	// A typed custom resource of this repository takes the same path.
	backupRun := run("bumped")
	if err := c.Create(ctx, backupRun); err != nil {
		t.Fatal(err)
	}
	backupRun.Finalizers = []string{"example.com/hold"}
	if err := c.Update(ctx, backupRun); err != nil {
		t.Fatal(err)
	}
	if backupRun.Generation != 1 {
		t.Errorf("BackupRun generation after a finalizer write = %d, want 1", backupRun.Generation)
	}
	if err := c.Delete(ctx, backupRun); err != nil {
		t.Fatal(err)
	}
	storedRun := run("bumped")
	get(t, c, storedRun)
	if storedRun.Generation != 2 {
		t.Errorf("BackupRun generation after the delete = %d, want 2", storedRun.Generation)
	}
}

func TestDeleteChecksUIDAndResourceVersionPreconditions(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := run("r")
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}

	other := types.UID("someone-else")
	err := c.Delete(ctx, run("r"), client.Preconditions{UID: &other})
	if !apierrors.IsConflict(err) {
		t.Fatalf("delete with a wrong uid: err = %v, want Conflict", err)
	}
	if !strings.Contains(err.Error(), "Precondition failed: UID in precondition: someone-else, UID in object meta: "+string(obj.UID)) {
		t.Errorf("message = %q, want the apiserver's precondition message", err.Error())
	}

	staleRV := "1" + obj.ResourceVersion
	if err := c.Delete(ctx, run("r"), client.Preconditions{ResourceVersion: &staleRV}); !apierrors.IsConflict(err) {
		t.Fatalf("delete with a wrong resourceVersion: err = %v, want Conflict", err)
	}
	get(t, c, run("r"))

	uid, rv := obj.UID, obj.ResourceVersion
	if err := c.Delete(ctx, run("r"), client.Preconditions{UID: &uid, ResourceVersion: &rv}); err != nil {
		t.Fatalf("delete with matching preconditions: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), run("r")); !apierrors.IsNotFound(err) {
		t.Errorf("after delete: err = %v, want NotFound", err)
	}
}

// deleting creates a BackupRun with the given finalizers and deletes it, so it
// is left with a deletionTimestamp.
func deleting(t *testing.T, c client.Client, name string, finalizers ...string) *backupv1alpha1.BackupRun {
	t.Helper()
	ctx := context.Background()
	obj := run(name)
	obj.Finalizers = finalizers
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(ctx, obj); err != nil {
		t.Fatal(err)
	}
	stored := run(name)
	get(t, c, stored)
	if stored.DeletionTimestamp == nil {
		t.Fatal("deleted object with finalizers has no deletionTimestamp")
	}
	return stored
}

func wantNewFinalizerRefused(t *testing.T, err error, finalizer string) {
	t.Helper()
	if !apierrors.IsInvalid(err) {
		t.Fatalf("err = %v, want Invalid", err)
	}
	status, ok := err.(apierrors.APIStatus)
	if !ok || status.Status().Code != 422 {
		t.Fatalf("err = %#v, want a 422 status", err)
	}
	details := status.Status().Details
	if details == nil || len(details.Causes) != 1 || details.Causes[0].Field != "metadata.finalizers" ||
		details.Causes[0].Type != metav1.CauseTypeForbidden {
		t.Fatalf("details = %+v, want one Forbidden cause on metadata.finalizers", details)
	}
	want := `no new finalizers can be added if the object is being deleted, found new finalizers []string{"` + finalizer + `"}`
	if !strings.Contains(details.Causes[0].Message, want) {
		t.Errorf("message = %q, want it to contain %q", details.Causes[0].Message, want)
	}
}

func TestNoNewFinalizerOnAnObjectBeingDeleted(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := deleting(t, c, "r", "a", "b")

	upd := obj.DeepCopy()
	upd.Finalizers = append(upd.Finalizers, "new")
	wantNewFinalizerRefused(t, c.Update(ctx, upd), "new")

	patched := obj.DeepCopy()
	patched.Finalizers = append(patched.Finalizers, "other")
	wantNewFinalizerRefused(t, c.Patch(ctx, patched, client.MergeFrom(obj)), "other")

	stored := run("r")
	get(t, c, stored)
	if strings.Join(stored.Finalizers, ",") != "a,b" {
		t.Errorf("stored finalizers = %v, want [a b]", stored.Finalizers)
	}

	// Removing finalizers stays allowed.
	base := stored.DeepCopy()
	stored.Finalizers = []string{"b"}
	if err := c.Patch(ctx, stored, client.MergeFrom(base)); err != nil {
		t.Fatalf("patch removing a finalizer: %v", err)
	}
	stored.Finalizers = nil
	if err := c.Update(ctx, stored); err != nil {
		t.Fatalf("update removing the last finalizer: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), run("r")); !apierrors.IsNotFound(err) {
		t.Errorf("after the last finalizer went: err = %v, want NotFound", err)
	}
}

func TestPatchRemovingTheLastFinalizerDeletesTheObject(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := deleting(t, c, "r", "a")
	uid := obj.UID

	base := obj.DeepCopy()
	obj.Finalizers = nil
	if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
		t.Fatalf("patch removing the last finalizer: %v", err)
	}
	if len(obj.Finalizers) != 0 || obj.DeletionTimestamp == nil || obj.UID != uid {
		t.Errorf("returned object: finalizers %v deletionTimestamp %v uid %q", obj.Finalizers, obj.DeletionTimestamp, obj.UID)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), run("r")); !apierrors.IsNotFound(err) {
		t.Errorf("after the last finalizer went: err = %v, want NotFound", err)
	}
}

func TestAddingAFinalizerBeforeDeletionIsAllowed(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := run("r")
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	base := obj.DeepCopy()
	obj.Finalizers = []string{"a"}
	if err := c.Patch(ctx, obj, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	obj.Finalizers = append(obj.Finalizers, "b")
	if err := c.Update(ctx, obj); err != nil {
		t.Fatal(err)
	}
}

func TestStatusWritesKeepServerFields(t *testing.T) {
	ctx := context.Background()
	c := newClient(t)
	obj := run("r")
	if err := c.Create(ctx, obj); err != nil {
		t.Fatal(err)
	}
	uid, created := obj.UID, obj.CreationTimestamp

	check := func(step string, o *backupv1alpha1.BackupRun, phase backupv1alpha1.RunPhase) {
		t.Helper()
		if o.Generation != 1 || o.UID != uid || !o.CreationTimestamp.Equal(&created) {
			t.Errorf("%s: generation %d uid %q created %v, want 1 %q %v",
				step, o.Generation, o.UID, o.CreationTimestamp, uid, created)
		}
		if o.Spec.Source != "data" {
			t.Errorf("%s: spec.source = %q, want data", step, o.Spec.Source)
		}
		if o.Status.Phase != phase {
			t.Errorf("%s: status.phase = %q, want %q", step, o.Status.Phase, phase)
		}
	}

	upd := obj.DeepCopy()
	upd.Generation = 9
	upd.UID = ""
	upd.CreationTimestamp = metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	upd.Spec.Source = "ignored"
	upd.Status.Phase = "Running"
	if err := c.Status().Update(ctx, upd); err != nil {
		t.Fatal(err)
	}
	check("returned after status update", upd, "Running")
	stored := run("r")
	get(t, c, stored)
	check("stored after status update", stored, "Running")

	base := stored.DeepCopy()
	stored.Generation = 5
	stored.CreationTimestamp = metav1.NewTime(time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC))
	stored.Spec.Source = "ignored"
	stored.Status.Phase = "Succeeded"
	if err := c.Status().Patch(ctx, stored, client.MergeFrom(base)); err != nil {
		t.Fatal(err)
	}
	check("returned after status patch", stored, "Succeeded")
	again := run("r")
	get(t, c, again)
	check("stored after status patch", again, "Succeeded")
}
