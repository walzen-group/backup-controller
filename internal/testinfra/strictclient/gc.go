package strictclient

import (
	"context"
	"slices"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Cascade records one delete and the dependents the garbage collector of
// kube-controller-manager 1.36.3 would act on because of it.
type Cascade struct {
	// Owner is the deleted object.
	Owner Ref
	// Propagation is the policy the delete resolved to: the request's
	// propagationPolicy, else the orphan or foregroundDeletion finalizer the
	// object already had, else Background.
	Propagation metav1.DeletionPropagation
	// Dependents are the objects whose ownerReferences named Owner's uid when
	// it was deleted, sorted by kind, namespace and name.
	Dependents []Ref
}

// Ref names a stored object.
type Ref struct {
	schema.GroupVersionKind
	types.NamespacedName
	UID types.UID
	// BlockOwnerDeletion is the dependent's ownerReference's
	// blockOwnerDeletion. It is false in Cascade.Owner.
	BlockOwnerDeletion bool
}

// Cascades returns every delete the client has recorded, oldest first,
// including the deletes of dependents the modelled garbage collector made.
func (c *Client) Cascades() []Cascade {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Clone(c.cascades)
}

// propagation resolves the policy of a delete the way the registry store
// does: the request's propagationPolicy wins, then the orphan or
// foregroundDeletion finalizer the stored object has, then the default,
// Background (shouldOrphanDependents and shouldDeleteDependents,
// k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:891-975).
// Per-kind defaults other than Background (DefaultGarbageCollectionPolicy of
// the built-in strategies) are left out.
func propagation(obj client.Object, requested *metav1.DeletionPropagation) metav1.DeletionPropagation {
	if requested != nil {
		return *requested
	}
	for _, f := range obj.GetFinalizers() {
		switch f {
		case metav1.FinalizerOrphanDependents:
			return metav1.DeletePropagationOrphan
		case metav1.FinalizerDeleteDependents:
			return metav1.DeletePropagationForeground
		}
	}
	return metav1.DeletePropagationBackground
}

// deleteCascading records the dependents of owner and, with
// Options.GarbageCollect, deletes owner and treats them as the garbage
// collector would. owner is the stored object; opts are the request's.
//
// The rules follow the garbage collection page
// (https://kubernetes.io/docs/concepts/architecture/garbage-collection/) and
// the DeletionPropagation and OwnerReference.BlockOwnerDeletion comments in
// k8s.io/apimachinery@v0.36.0/pkg/apis/meta/v1/types.go:330-339,530-547;
// kube-controller-manager's garbagecollector package (k8s.io/kubernetes) is
// not in the module cache, so its code was not read:
//
//   - Orphan: the collector removes the owner's reference from every
//     dependent, then the owner goes.
//   - Background: the owner goes at once; each dependent is then deleted,
//     unless it still has an owner that exists, in which case only the
//     references to missing owners are removed from it.
//   - Foreground: the server puts the foregroundDeletion finalizer on the
//     owner and sets its deletionTimestamp; the collector deletes the
//     dependents with Foreground, and removes the finalizer once no dependent
//     with blockOwnerDeletion=true is left. Dependents without it do not hold
//     the owner. blockOwnerDeletion has no effect in the other two policies.
//
// The collector's work happens inside the Delete call, so a test sees the end
// state at once. An owner held by an ordinary finalizer keeps its dependents
// in Background (the collector only acts once the owner is gone), and
// nothing re-runs the collector when that finalizer, or a blocking
// dependent's own finalizer, is later removed.
func (c *Client) deleteCascading(ctx context.Context, owner client.Object, o *client.DeleteOptions, opts []client.DeleteOption) error {
	policy := propagation(owner, o.PropagationPolicy)
	ownerRef, err := c.ref(owner)
	if err != nil {
		return err
	}
	deps, err := c.dependents(ctx, owner)
	if err != nil {
		return err
	}
	cascade := Cascade{Owner: ownerRef, Propagation: policy}
	for _, d := range deps {
		r, err := c.ref(d)
		if err != nil {
			return err
		}
		for _, or := range d.GetOwnerReferences() {
			if or.UID == owner.GetUID() && or.BlockOwnerDeletion != nil && *or.BlockOwnerDeletion {
				r.BlockOwnerDeletion = true
			}
		}
		cascade.Dependents = append(cascade.Dependents, r)
	}
	c.mu.Lock()
	c.cascades = append(c.cascades, cascade)
	c.mu.Unlock()

	if !c.opts.GarbageCollect {
		return c.WithWatch.Delete(ctx, owner, opts...)
	}
	switch policy {
	case metav1.DeletePropagationOrphan:
		for _, d := range deps {
			if err := c.dropOwnerRefs(ctx, d, func(r metav1.OwnerReference) bool { return r.UID == owner.GetUID() }); err != nil {
				return err
			}
		}
		return c.WithWatch.Delete(ctx, owner, opts...)
	case metav1.DeletePropagationForeground:
		if !slices.Contains(owner.GetFinalizers(), metav1.FinalizerDeleteDependents) {
			owner.SetFinalizers(append(owner.GetFinalizers(), metav1.FinalizerDeleteDependents))
			if err := c.WithWatch.Update(ctx, owner); err != nil {
				return err
			}
		}
		if err := c.WithWatch.Delete(ctx, owner, opts...); err != nil {
			return err
		}
		for _, d := range deps {
			if err := c.collect(ctx, d); err != nil {
				return err
			}
		}
		return c.finishForeground(ctx, owner)
	default:
		if err := c.WithWatch.Delete(ctx, owner, opts...); err != nil {
			return err
		}
		if _, err := c.stored(ctx, owner); err == nil {
			return nil // a finalizer keeps the owner; its dependents stay
		}
		for _, d := range deps {
			if err := c.collect(ctx, d); err != nil {
				return err
			}
		}
		return nil
	}
}

// collect handles one dependent after an owner of it was deleted. When the
// dependent still has an owner that exists and is not waiting on its
// dependents, only its references to missing or waiting owners are removed.
// Otherwise it is deleted: with Foreground when an owner is waiting for it,
// else with the policy its own finalizers ask for (Background by default).
func (c *Client) collect(ctx context.Context, dep client.Object) error {
	cur, err := c.stored(ctx, dep)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	solid, gone := false, map[types.UID]bool{}
	waiting := false
	for _, r := range cur.GetOwnerReferences() {
		state, err := c.ownerState(ctx, cur, r)
		if err != nil {
			return err
		}
		switch state {
		case ownerSolid:
			solid = true
		case ownerWaiting:
			waiting = true
			gone[r.UID] = true
		default:
			gone[r.UID] = true
		}
	}
	if solid {
		return c.dropOwnerRefs(ctx, cur, func(r metav1.OwnerReference) bool { return gone[r.UID] })
	}
	var requested *metav1.DeletionPropagation
	if waiting {
		p := metav1.DeletePropagationForeground
		requested = &p
	}
	uid := cur.GetUID()
	o := &client.DeleteOptions{PropagationPolicy: requested, Preconditions: &metav1.Preconditions{UID: &uid}}
	opts := []client.DeleteOption{client.Preconditions{UID: &uid}}
	if requested != nil {
		opts = append(opts, client.PropagationPolicy(*requested))
	}
	if err := c.deleteCascading(ctx, cur, o, opts); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

// finishForeground removes the foregroundDeletion finalizer from owner once
// no dependent that blocks it is left, which deletes it when that was its
// last finalizer.
func (c *Client) finishForeground(ctx context.Context, owner client.Object) error {
	cur, err := c.stored(ctx, owner)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	deps, err := c.dependents(ctx, cur)
	if err != nil {
		return err
	}
	for _, d := range deps {
		for _, r := range d.GetOwnerReferences() {
			if r.UID == cur.GetUID() && r.BlockOwnerDeletion != nil && *r.BlockOwnerDeletion {
				return nil
			}
		}
	}
	cur.SetFinalizers(slices.DeleteFunc(cur.GetFinalizers(), func(f string) bool { return f == metav1.FinalizerDeleteDependents }))
	if err := c.WithWatch.Update(ctx, cur); err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

type ownerStateKind int

const (
	ownerMissing ownerStateKind = iota
	ownerSolid
	ownerWaiting
)

// ownerState looks up the owner r names for dependent dep: missing when no
// object of that kind, name and uid exists (in dep's namespace, or cluster
// scoped), waiting when it is being deleted with the foregroundDeletion
// finalizer, and solid otherwise.
func (c *Client) ownerState(ctx context.Context, dep client.Object, r metav1.OwnerReference) (ownerStateKind, error) {
	gv, err := schema.ParseGroupVersion(r.APIVersion)
	if err != nil {
		return ownerMissing, nil //nolint:nilerr // an unparsable reference names no owner
	}
	for _, ns := range []string{dep.GetNamespace(), ""} {
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gv.WithKind(r.Kind))
		if err := c.Get(ctx, types.NamespacedName{Namespace: ns, Name: r.Name}, u); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return ownerMissing, err
		}
		if u.GetUID() != r.UID {
			continue
		}
		if u.GetDeletionTimestamp() != nil && slices.Contains(u.GetFinalizers(), metav1.FinalizerDeleteDependents) {
			return ownerWaiting, nil
		}
		return ownerSolid, nil
	}
	return ownerMissing, nil
}

// dropOwnerRefs removes the ownerReferences drop selects from dep's stored
// copy, as the collector's patch does.
func (c *Client) dropOwnerRefs(ctx context.Context, dep client.Object, drop func(metav1.OwnerReference) bool) error {
	cur, err := c.stored(ctx, dep)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return err
	}
	refs := slices.DeleteFunc(slices.Clone(cur.GetOwnerReferences()), drop)
	if len(refs) == len(cur.GetOwnerReferences()) {
		return nil
	}
	cur.SetOwnerReferences(refs)
	return c.WithWatch.Update(ctx, cur)
}

// dependents lists every stored object, of every kind in the scheme that has
// a list kind, whose ownerReferences name owner's uid. A namespaced owner
// only has dependents in its own namespace, since the collector does not
// follow references across namespaces.
func (c *Client) dependents(ctx context.Context, owner client.Object) ([]client.Object, error) {
	var out []client.Object
	seen := map[types.UID]bool{}
	for _, gvk := range c.listableKinds() {
		list := &unstructured.UnstructuredList{}
		list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
		var lo []client.ListOption
		if owner.GetNamespace() != "" {
			lo = append(lo, client.InNamespace(owner.GetNamespace()))
		}
		if err := c.List(ctx, list, lo...); err != nil {
			continue // a kind the fake cannot list holds no objects
		}
		for i := range list.Items {
			item := &list.Items[i]
			if seen[item.GetUID()] {
				continue // the same object served in another version
			}
			if slices.ContainsFunc(item.GetOwnerReferences(), func(r metav1.OwnerReference) bool { return r.UID == owner.GetUID() }) {
				seen[item.GetUID()] = true
				item.SetGroupVersionKind(gvk)
				out = append(out, item)
			}
		}
	}
	slices.SortFunc(out, func(a, b client.Object) int {
		ka := a.GetObjectKind().GroupVersionKind().String() + "/" + a.GetNamespace() + "/" + a.GetName()
		kb := b.GetObjectKind().GroupVersionKind().String() + "/" + b.GetNamespace() + "/" + b.GetName()
		return strings.Compare(ka, kb)
	})
	return out, nil
}

// listableKinds returns the kinds of the scheme that have a matching list
// kind, leaving out internal versions and the meta.k8s.io kinds.
func (c *Client) listableKinds() []schema.GroupVersionKind {
	s := c.Scheme()
	var out []schema.GroupVersionKind
	for gvk := range s.AllKnownTypes() {
		if gvk.Version == "__internal" || gvk.Group == "meta.k8s.io" || strings.HasSuffix(gvk.Kind, "List") {
			continue
		}
		if s.Recognizes(gvk.GroupVersion().WithKind(gvk.Kind + "List")) {
			out = append(out, gvk)
		}
	}
	slices.SortFunc(out, func(a, b schema.GroupVersionKind) int { return strings.Compare(a.String(), b.String()) })
	return out
}

// ref names obj.
func (c *Client) ref(obj client.Object) (Ref, error) {
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return Ref{}, err
	}
	return Ref{GroupVersionKind: gvk, NamespacedName: client.ObjectKeyFromObject(obj), UID: obj.GetUID()}, nil
}
