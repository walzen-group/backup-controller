package strictclient_test

import (
	"context"
	"strings"
	"testing"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

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
