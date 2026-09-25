package strictclient

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"slices"

	jsonpatch "github.com/evanphx/json-patch/v5"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Delete deletes obj after checking a UID precondition the way
// kube-apiserver 1.36.3 does.
//
// The fake client checks a ResourceVersion precondition itself (with a
// Conflict) but ignores a UID one. On the real server the registry store
// copies both into storage.Preconditions
// (k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:1154-1158),
// Preconditions.Check refuses a mismatched UID with an invalid-object storage
// error, checking UID before ResourceVersion
// (pkg/storage/interfaces.go:138-163), and InterpretDeleteError turns that
// into a 409 Conflict (pkg/storage/errors/storage.go:103-104). Delete returns
// the same Conflict, with the message the real server builds, when the
// stored uid differs from the precondition. The storage key in the message
// is namespace/name, where the real server shows its etcd key. Other errors
// and the deletion itself are the fake client's.
func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	o := &client.DeleteOptions{}
	o.ApplyOptions(opts)
	if p := o.Preconditions; p != nil && p.UID != nil {
		old, err := c.stored(ctx, obj)
		if err != nil {
			return c.WithWatch.Delete(ctx, obj, opts...)
		}
		if *p.UID != old.GetUID() {
			return c.uidConflict(obj, *p.UID, old.GetUID())
		}
	}
	return c.WithWatch.Delete(ctx, obj, opts...)
}

// checkNoNewFinalizers refuses a write that would add a finalizer to an
// object that already has a deletionTimestamp.
//
// Parameters:
//   - obj is the object as the write would store it.
//   - old is the stored object.
//
// It returns nil when old is not being deleted or obj adds no finalizer, and
// otherwise the 422 Invalid error kube-apiserver 1.36.3 returns: a Forbidden
// field error on metadata.finalizers from ValidateNoNewFinalizers
// (k8s.io/apimachinery@v0.36.0/pkg/api/validation/objectmeta.go:121-127),
// which ValidateObjectMetaAccessorUpdate applies when the stored object has a
// deletionTimestamp (objectmeta.go:315-318), reached from rest.BeforeUpdate
// (k8s.io/apiserver@v0.36.3/pkg/registry/rest/update.go:97) and returned as
// errors.NewInvalid (update.go:156).
func (c *Client) checkNoNewFinalizers(obj, old client.Object) error {
	if old.GetDeletionTimestamp() == nil {
		return nil
	}
	var extra []string
	for _, f := range obj.GetFinalizers() {
		if !slices.Contains(old.GetFinalizers(), f) && !slices.Contains(extra, f) {
			extra = append(extra, f)
		}
	}
	if len(extra) == 0 {
		return nil
	}
	slices.Sort(extra)
	gvk, err := apiutil.GVKForObject(old, c.Scheme())
	if err != nil {
		return err
	}
	return apierrors.NewInvalid(gvk.GroupKind(), old.GetName(), field.ErrorList{
		field.Forbidden(field.NewPath("metadata", "finalizers"),
			fmt.Sprintf("no new finalizers can be added if the object is being deleted, found new finalizers %#v", extra)),
	})
}

// patchedObject applies patch to the stored object old and returns the
// result, so a patch can be checked before the fake client stores it. It
// returns nil for a server-side apply patch, which it does not model. Merge,
// JSON and strategic merge patches are applied as the real server applies
// them to the stored object.
func (c *Client) patchedObject(obj, old client.Object, patch client.Patch) (client.Object, error) {
	if patch.Type() == types.ApplyPatchType || patch.Type() == types.ApplyCBORPatchType {
		return nil, nil
	}
	data, err := patch.Data(obj)
	if err != nil {
		return nil, err
	}
	orig, err := json.Marshal(old)
	if err != nil {
		return nil, err
	}
	var out []byte
	switch patch.Type() {
	case types.MergePatchType:
		out, err = jsonpatch.MergePatch(orig, data)
	case types.JSONPatchType:
		var p jsonpatch.Patch
		if p, err = jsonpatch.DecodePatch(data); err == nil {
			out, err = p.Apply(orig)
		}
	case types.StrategicMergePatchType:
		if _, ok := old.(*unstructured.Unstructured); ok {
			out, err = jsonpatch.MergePatch(orig, data)
		} else {
			out, err = strategicpatch.StrategicMergePatch(orig, data, old.DeepCopyObject())
		}
	default:
		return nil, nil
	}
	if err != nil {
		return nil, apierrors.NewBadRequest(err.Error())
	}
	res, ok := old.DeepCopyObject().(client.Object)
	if !ok {
		return nil, fmt.Errorf("strictclient: %T is not a client.Object", old)
	}
	if err := decodeInto(res, out); err != nil {
		return nil, err
	}
	return res, nil
}

// decodeInto replaces obj's content with the JSON in data.
func decodeInto(obj client.Object, data []byte) error {
	if u, ok := obj.(*unstructured.Unstructured); ok {
		u.Object = nil
		return u.UnmarshalJSON(data)
	}
	v := reflect.ValueOf(obj).Elem()
	v.Set(reflect.Zero(v.Type()))
	return json.Unmarshal(data, obj)
}

// goneAfterLastFinalizer reports whether a write removed the last finalizer
// of an object being deleted, so that the object no longer exists. The real
// server then answers the write with the object it was about to store; the
// registry store deletes it right after (store.go:806).
func goneAfterLastFinalizer(err error, old, written client.Object) bool {
	return apierrors.IsNotFound(err) && old.GetDeletionTimestamp() != nil && len(written.GetFinalizers()) == 0
}

// Status returns a writer for the status subresource that keeps the stored
// generation, uid and creationTimestamp on every write.
//
// On kube-apiserver 1.36.3 a status write starts from the stored object and
// takes only status from the request: customResource statusStrategy
// PrepareForUpdate copies the old object over the new one and puts the
// request's status back
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/registry/customresource/status_strategy.go:67-91),
// and rest.BeforeUpdate keeps creationTimestamp and generation
// (k8s.io/apiserver@v0.36.3/pkg/registry/rest/update.go:121-137). The fake
// client does the same for kinds registered with WithStatusSubresource; this
// writer checks the stored copy after each write and restores those three
// fields if the write changed them. A kind without a status subresource gets
// NotFound, from the fake, as on a real server.
func (c *Client) Status() client.SubResourceWriter {
	return &statusWriter{SubResourceWriter: c.WithWatch.Status(), c: c}
}

type statusWriter struct {
	client.SubResourceWriter

	c *Client
}

// Update writes obj's status, pruned against its CRD. Errors are a Conflict
// when obj carries a uid other than the stored one (see checkUID), the fake
// client's, or from the follow-up update that restores the server-owned
// fields.
func (w *statusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	old, err := w.c.stored(ctx, obj)
	if err != nil {
		return w.SubResourceWriter.Update(ctx, obj, opts...)
	}
	if err := w.c.checkUID(obj, old); err != nil {
		return err
	}
	if err := w.c.coerce(obj, false); err != nil {
		return err
	}
	if err := w.SubResourceWriter.Update(ctx, obj, opts...); err != nil {
		return err
	}
	o := &client.SubResourceUpdateOptions{}
	o.ApplyOptions(opts)
	return w.c.restoreServerFields(ctx, obj, old, len(o.DryRun) > 0)
}

// Patch patches obj's status and prunes the result against its CRD. Errors
// are the fake client's, or from the follow-up updates that prune and
// restore the server-owned fields.
func (w *statusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	old, err := w.c.stored(ctx, obj)
	if err != nil {
		return w.SubResourceWriter.Patch(ctx, obj, patch, opts...)
	}
	if err := w.SubResourceWriter.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	o := &client.SubResourcePatchOptions{}
	o.ApplyOptions(opts)
	if err := w.c.prunePatched(ctx, obj, len(o.DryRun) > 0); err != nil {
		return err
	}
	return w.c.restoreServerFields(ctx, obj, old, len(o.DryRun) > 0)
}

// restoreServerFields puts old's generation, uid and creationTimestamp back
// on the stored copy of obj when a status write changed them, and leaves obj
// holding the stored object. A dry run stores nothing, so only obj is
// corrected.
func (c *Client) restoreServerFields(ctx context.Context, obj, old client.Object, dryRun bool) error {
	cur := obj
	if !dryRun {
		var err error
		if cur, err = c.stored(ctx, obj); err != nil {
			return err
		}
	}
	same := cur.GetGeneration() == old.GetGeneration() && cur.GetUID() == old.GetUID() &&
		ptr(cur.GetCreationTimestamp()).Equal(ptr(old.GetCreationTimestamp()))
	cur.SetGeneration(old.GetGeneration())
	cur.SetUID(old.GetUID())
	cur.SetCreationTimestamp(old.GetCreationTimestamp())
	if dryRun {
		return nil
	}
	if !same {
		if err := c.WithWatch.Update(ctx, cur); err != nil {
			return err
		}
	}
	return c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
}

// decodeFrom copies src's content into dst through JSON, which works for any
// pair of typed or unstructured objects of the same kind.
func decodeFrom(dst, src client.Object) error {
	data, err := json.Marshal(src)
	if err != nil {
		return err
	}
	return decodeInto(dst, data)
}

// checkUID refuses an update whose object carries a uid other than the
// stored one.
//
// Parameters:
//   - obj is the object the update sends.
//   - old is the stored object.
//
// It returns nil when obj has no uid or the stored one, and otherwise the
// 409 Conflict kube-apiserver 1.36.3 returns. The update handler turns the
// uid of the request's object into a UID precondition
// (k8s.io/apiserver@v0.36.3/pkg/registry/rest/update.go:186-202), the
// registry store passes it to GuaranteedUpdate
// (pkg/registry/generic/registry/store.go:637-649), which checks it against
// the stored object before it calls the update function
// (pkg/storage/etcd3/store.go:501, pkg/storage/interfaces.go:138-163),
// and InterpretUpdateError turns the invalid-object storage error into a
// Conflict (pkg/storage/errors/storage.go:78-81). Status updates take the same
// path. A patch sends no precondition, so Patch does not call this.
func (c *Client) checkUID(obj, old client.Object) error {
	if obj.GetUID() == "" || obj.GetUID() == old.GetUID() {
		return nil
	}
	return c.uidConflict(obj, obj.GetUID(), old.GetUID())
}

// uidConflict builds the Conflict a failed UID precondition returns. The
// storage key in the message is namespace/name, where the real server shows
// its etcd key.
func (c *Client) uidConflict(obj client.Object, want, stored types.UID) error {
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return err
	}
	key := client.ObjectKeyFromObject(obj).String()
	gvr, _ := meta.UnsafeGuessKindToResource(gvk)
	return apierrors.NewConflict(gvr.GroupResource(), obj.GetName(),
		fmt.Errorf("StorageError: invalid object, Code: 4, Key: %s, ResourceVersion: 0, AdditionalErrorMsg: "+
			"Precondition failed: UID in precondition: %v, UID in object meta: %v", key, want, stored))
}
