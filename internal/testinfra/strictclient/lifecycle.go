package strictclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"

	jsonpatch "github.com/evanphx/json-patch/v5"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/validation"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Delete deletes obj after checking its preconditions the way
// kube-apiserver 1.36.3 does.
//
// The fake client checks a ResourceVersion precondition itself but ignores a
// UID one. On the real server rest.BeforeDelete checks both against the
// stored object before anything else, the UID first, and refuses a mismatch
// with a 409 Conflict on the group and kind
// (k8s.io/apiserver@v0.36.3/pkg/registry/rest/delete.go:83-91). Delete
// returns the same Conflict, with the same message, also for a dry run.
// Other errors and the deletion itself are the fake client's.
//
// Every delete that is not a dry run is recorded with the dependents of the
// object (see Cascades), and with Options.GarbageCollect those dependents are
// handled the way the garbage collector would; see deleteCascading. A delete
// that a finalizer holds in place raises the object's generation by one and
// sets deletionGracePeriodSeconds to 0, as the server does; see
// markAsDeleting. A further delete of an object that is already being
// deleted and still has finalizers stores nothing, so its deletionTimestamp
// stays; see deleteStored.
func (c *Client) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	o := &client.DeleteOptions{}
	o.ApplyOptions(opts)
	owner, err := c.stored(ctx, obj)
	if err != nil {
		return c.WithWatch.Delete(ctx, obj, opts...)
	}
	if err := c.checkPreconditions(owner, o.Preconditions); err != nil {
		return err
	}
	if len(o.DryRun) > 0 {
		return c.WithWatch.Delete(ctx, obj, opts...)
	}
	if err := c.deleteCascading(ctx, owner, o, opts); err != nil {
		return err
	}
	return c.markAsDeleting(ctx, owner)
}

// DeleteAllOf deletes every object of obj's kind that the options select, one
// by one through Delete.
//
// Parameters:
//   - obj gives the kind. Its name and content are not used.
//   - opts select the objects (namespace, labels, fields) and carry the
//     delete options for each object.
//
// It returns the error of the list or of the first delete that fails with an
// error other than NotFound. The real server lists the objects and deletes
// each one through the same Delete as a single delete, and ignores NotFound
// (k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:1331-1389).
// The fake client instead stamps a new deletionTimestamp on every object that
// a finalizer holds in place. Through Delete, such an object keeps its first
// deletionTimestamp and its resourceVersion, as on the server.
func (c *Client) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	o := &client.DeleteAllOfOptions{}
	o.ApplyOptions(opts)
	list, err := c.listFor(obj)
	if err != nil {
		return err
	}
	if err := c.List(ctx, list, &o.ListOptions); err != nil {
		return err
	}
	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	for _, item := range items {
		cur, ok := item.(client.Object)
		if !ok {
			return fmt.Errorf("list item %T is not a client.Object", item)
		}
		if err := c.Delete(ctx, cur, &o.DeleteOptions); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// listFor returns an empty list of obj's kind, typed when the scheme has a
// typed list and unstructured otherwise. Its error is the scheme's when the
// kind of obj is not known.
func (c *Client) listFor(obj client.Object) (client.ObjectList, error) {
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return nil, err
	}
	listGVK := gvk.GroupVersion().WithKind(gvk.Kind + "List")
	if _, unstructuredObj := obj.(*unstructured.Unstructured); !unstructuredObj {
		if l, err := c.Scheme().New(listGVK); err == nil {
			if list, ok := l.(client.ObjectList); ok {
				if u, isU := list.(*unstructured.UnstructuredList); isU {
					u.SetGroupVersionKind(listGVK)
				}
				return list, nil
			}
		}
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(listGVK)
	return list, nil
}

// checkPreconditions refuses a delete whose preconditions do not match the
// stored object.
//
// Parameters:
//   - owner is the stored object.
//   - p are the delete's preconditions. Nil means none.
//
// It returns nil when every precondition matches, and otherwise the 409
// Conflict that rest.BeforeDelete of kube-apiserver 1.36.3 returns, with its
// message. The resource in that Conflict is the kind of the object and its
// group (k8s.io/apiserver@v0.36.3/pkg/registry/rest/delete.go:83-91).
func (c *Client) checkPreconditions(owner client.Object, p *metav1.Preconditions) error {
	if p == nil {
		return nil
	}
	var cause error
	switch {
	case p.UID != nil && *p.UID != owner.GetUID():
		cause = fmt.Errorf("the UID in the precondition (%s) does not match the UID in record (%s). "+
			"The object might have been deleted and then recreated", *p.UID, owner.GetUID())
	case p.ResourceVersion != nil && *p.ResourceVersion != owner.GetResourceVersion():
		cause = fmt.Errorf("the ResourceVersion in the precondition (%s) does not match the ResourceVersion in record (%s). "+
			"The object might have been modified", *p.ResourceVersion, owner.GetResourceVersion())
	default:
		return nil
	}
	gvk, err := apiutil.GVKForObject(owner, c.Scheme())
	if err != nil {
		return err
	}
	return apierrors.NewConflict(schema.GroupResource{Group: gvk.Group, Resource: gvk.Kind}, owner.GetName(), cause)
}

// deleteStored deletes owner, the stored object, as kube-apiserver 1.36.3
// would.
//
// Parameters:
//   - owner is the stored object, as Delete read it before the delete.
//   - opts are the caller's delete options, passed to the fake client.
//
// It calls the fake client for every delete except one that stores nothing on
// the server: a delete of an object that already has a deletionTimestamp and
// still has finalizers. There markAsDeleting keeps the deletionTimestamp that
// is already set and not in the future
// (k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:1030-1034),
// and the guaranteed update that follows serialises the stored object, so the
// etcd3 store returns without a write
// (k8s.io/apiserver@v0.36.3/pkg/storage/etcd3/store.go:553-570). The fake
// client stamps a new deletionTimestamp on every delete of such an object
// (deleteObjectLocked, sigs.k8s.io/controller-runtime@v0.24.1/pkg/client/fake/client.go:1198-1200),
// and its tracker refuses the update that would put the older timestamp back
// (versioned_tracker.go:296-298), so the wrapper answers without calling it
// and the first timestamp stays. Its errors are the fake client's.
func (c *Client) deleteStored(ctx context.Context, owner client.Object, opts []client.DeleteOption) error {
	if owner.GetDeletionTimestamp() == nil || len(owner.GetFinalizers()) == 0 {
		return c.WithWatch.Delete(ctx, owner, opts...)
	}
	return nil
}

// markAsDeleting gives an object that the delete left in place with a
// deletionTimestamp the generation and the deletionGracePeriodSeconds that
// kube-apiserver 1.36.3 gives it.
//
// Parameters:
//   - owner is the stored object as it was before the delete, from Delete.
//
// It returns nil when the delete removed the object, when the object was
// already being deleted, when nothing changes, and when the follow-up write
// succeeds. Its errors are those of the read and of that write.
//
// The server raises the generation once, on the delete that sets the
// deletionTimestamp, when the generation is above 0. markAsDeleting does it
// for an object whose kind does not support graceful deletion, which is every
// custom resource
// (k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:1015-1037),
// and rest.BeforeDelete does it for the other kinds
// (k8s.io/apiserver@v0.36.3/pkg/registry/rest/delete.go:168-172). The same
// markAsDeleting sets deletionGracePeriodSeconds to 0. A Pod supports
// graceful deletion and gets another grace period on the server, so the
// wrapper leaves the grace period of a Pod as the fake client stores it. The
// fake client changes neither field, so the wrapper writes the stored object
// back with both, as it does for the generation of an update (see
// settleGeneration).
func (c *Client) markAsDeleting(ctx context.Context, owner client.Object) error {
	if owner.GetDeletionTimestamp() != nil {
		return nil
	}
	// The delete removed an owner that the fake cannot find.
	stored, err := c.stored(ctx, owner)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if stored.GetDeletionTimestamp() == nil {
		return nil
	}
	changed := false
	if g := stored.GetGeneration(); g != 0 {
		stored.SetGeneration(g + 1)
		changed = true
	}
	if _, pod := stored.(*corev1.Pod); !pod {
		if p := stored.GetDeletionGracePeriodSeconds(); p == nil || *p != 0 {
			zero := int64(0)
			stored.SetDeletionGracePeriodSeconds(&zero)
			changed = true
		}
	}
	if !changed {
		return nil
	}
	return c.WithWatch.Update(ctx, stored)
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
func (*Client) patchedObject(obj, old client.Object, patch client.Patch) (client.Object, error) {
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
// NotFound, from the fake, as on a real server. Status returns the same
// client as SubResource("status").
func (c *Client) Status() client.SubResourceWriter {
	return c.SubResource(statusSubresource)
}

// statusSubresource is the name of the status subresource.
const statusSubresource = "status"

// statusWriter serves the status subresource with the rules of Status. Get
// and Create go to the fake client unchanged.
type statusWriter struct {
	client.SubResourceClient

	c *Client
}

// Update writes obj's status, pruned against its CRD. Errors are a Conflict
// when obj carries a uid other than the stored one (see checkUID), the fake
// client's, or from the follow-up update that restores the server-owned
// fields.
func (w *statusWriter) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	old, err := w.c.stored(ctx, obj)
	if err != nil {
		return w.SubResourceClient.Update(ctx, obj, opts...)
	}
	if err := w.c.checkUID(obj, old); err != nil {
		return err
	}
	if err := w.c.checkResourceVersion(obj, old); err != nil {
		return err
	}
	if err := w.c.coerce(obj, false); err != nil {
		return err
	}
	if err := w.SubResourceClient.Update(ctx, obj, opts...); err != nil {
		return err
	}
	o := &client.SubResourceUpdateOptions{}
	o.ApplyOptions(opts)
	return w.c.restoreServerFields(ctx, obj, old, len(o.DryRun) > 0)
}

// Patch patches obj's status and prunes the result against its CRD. Errors
// are an Invalid when the patch changes the uid of a built-in kind (see
// checkPatchedUID), the fake client's, or from the follow-up updates that
// prune and restore the server-owned fields.
func (w *statusWriter) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	old, err := w.c.stored(ctx, obj)
	if err != nil {
		return w.SubResourceClient.Patch(ctx, obj, patch, opts...)
	}
	result, err := w.c.patchedObject(obj, old, patch)
	if err != nil {
		return err
	}
	if result != nil {
		if err := w.c.checkPatchedUID(result, old, true); err != nil {
			return err
		}
	}
	if err := w.SubResourceClient.Patch(ctx, obj, patch, opts...); err != nil {
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

// checkResourceVersion refuses an update or status update whose object
// carries a resourceVersion other than the stored one.
//
// Parameters:
//   - obj is the object the write sends.
//   - old is the stored object.
//
// It returns nil when obj carries no resourceVersion or the stored one, and
// otherwise the 409 Conflict kube-apiserver 1.36.3 returns, with the message
// the registry store builds. The store compares the resourceVersion of the
// object to update with the stored one and answers a mismatch with a
// Conflict carrying OptimisticLockErrorMsg, for a status update through the
// same path
// (k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:743-745,
// pkg/registry/generic/registry/store.go:263). The fake client compares the
// two itself, but for an unstructured object it copies the stored object over
// the request object before that comparison, which loses the request's
// resourceVersion and admits a stale write; this check runs first and refuses
// it. A patch is not checked: its body carries no resourceVersion unless the
// test sets one there.
func (c *Client) checkResourceVersion(obj, old client.Object) error {
	rv := obj.GetResourceVersion()
	if rv == "" || rv == old.GetResourceVersion() {
		return nil
	}
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return err
	}
	gvr, _ := meta.UnsafeGuessKindToResource(gvk)
	return apierrors.NewConflict(gvr.GroupResource(), obj.GetName(),
		errors.New("the object has been modified; please apply your changes to the latest version and try again"))
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
// path. A patch sends no precondition; checkPatchedUID covers it.
func (c *Client) checkUID(obj, old client.Object) error {
	if obj.GetUID() == "" || obj.GetUID() == old.GetUID() {
		return nil
	}
	return c.uidConflict(obj, obj.GetUID(), old.GetUID())
}

// uidConflict builds the Conflict that an update or status update with
// another uid gets from the storage layer. The storage key in the message is
// namespace/name, where the real server shows its etcd key.
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

// checkPatchedUID refuses a patch whose result carries a uid other than the
// stored one.
//
// Parameters:
//   - result is the stored object with the patch applied (see
//     patchedObject).
//   - old is the stored object.
//   - status is true for a patch of the status subresource.
//
// It returns nil when the uid is empty or unchanged, and for a status patch
// of a custom resource, and otherwise the 422 Invalid error kube-apiserver
// 1.36.3 returns. A patch sends no UID precondition, so the patched object
// reaches rest.BeforeUpdate, which keeps the stored uid only when the
// patched one is empty (k8s.io/apiserver@v0.36.3/pkg/registry/rest/update.go:131-133)
// and then validates metadata.uid as immutable
// (k8s.io/apimachinery@v0.36.0/pkg/api/validation/objectmeta.go:332). The
// status strategy of a custom resource starts from a copy of the stored
// object and takes only status from the request
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/registry/customresource/status_strategy.go:67-91),
// so there the patched uid is dropped and the write succeeds. The built-in
// status strategies keep the request's metadata, so a status patch of a
// built-in kind is refused like a plain patch.
func (c *Client) checkPatchedUID(result, old client.Object, status bool) error {
	if result.GetUID() == "" || result.GetUID() == old.GetUID() {
		return nil
	}
	gvk, err := apiutil.GVKForObject(old, c.Scheme())
	if err != nil {
		return err
	}
	if status && c.opts.IsCustomResource(gvk) {
		return nil
	}
	return apierrors.NewInvalid(gvk.GroupKind(), old.GetName(), field.ErrorList{
		field.Invalid(field.NewPath("metadata", "uid"), result.GetUID(), validation.FieldImmutableErrorMsg),
	})
}
