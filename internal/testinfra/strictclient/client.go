// Package strictclient wraps controller-runtime's fake client so that the
// metadata the API server owns behaves as it does on a real cluster.
//
// It stands in for kube-apiserver 1.36.3 (k8s.io/apiserver v0.36.3, with the
// custom resource strategy of k8s.io/apiextensions-apiserver v0.36.0) behind
// the fake client of sigs.k8s.io/controller-runtime v0.24.1. The fake stores
// whatever metadata the caller sends and never sets metadata.generation (its
// package documentation lists this as a known limitation). The wrapper models
// the server-owned fields:
//
//   - metadata.creationTimestamp and metadata.uid are set on every create from
//     the test's server clock and a fresh UUID, replacing whatever the caller
//     sent. The registry store calls rest.FillObjectMetaSystemFields on create
//     (k8s.io/apiserver@v0.36.3/pkg/registry/generic/registry/store.go:492),
//     which sets both unconditionally (pkg/registry/rest/meta.go:39-42).
//   - An update or patch keeps the stored creationTimestamp, keeps the stored
//     uid when the request leaves it empty, and cannot set metadata.generation
//     (pkg/registry/rest/update.go:121-137).
//   - For custom resources metadata.generation is 1 after create and goes up
//     by one on every write that changes anything outside metadata. That is
//     customResourceStrategy.PrepareForCreate and PrepareForUpdate
//     (k8s.io/apiextensions-apiserver@v0.36.0/pkg/registry/customresource/strategy.go:145-186).
//     With a status subresource the fake keeps the stored status on a plain
//     update, so only spec changes count, as on the real server.
//   - Delete honours a UID precondition with a Conflict (the fake already
//     honours a ResourceVersion one). See Client.Delete.
//   - An update or patch cannot add a finalizer to an object that has a
//     deletionTimestamp; removing the last one deletes the object and the
//     write still succeeds. See checkNoNewFinalizers.
//   - Status subresource writes keep the stored generation, uid and
//     creationTimestamp. See Client.Status.
//   - An update or status update whose object carries a uid other than the
//     stored one fails with a Conflict. See checkUID.
//   - Objects of a kind defined by a CRD in Options.CRDs are pruned against
//     that CRD's schema on every write, with the apiextensions pruning code.
//     See Client.coerce. Build registers the status subresources those CRDs
//     declare, and Create drops the status of such a kind.
//
// It leaves out: generation for built-in kinds (each built-in strategy has
// its own rule; the wrapper keeps the stored value and never bumps it), the
// finalizer check for server-side apply patches, CRD defaulting and
// metadata coercion, garbage collection and blockOwnerDeletion. Those are separate behaviours; each one is added as a
// method or an Options field of Client, the way Create, Update and Patch are
// here.
//
// A write that changes the generation after a patch or update is stored in two
// steps (the fake's write, then one update that sets the generation), so the
// resourceVersion goes up by two where a real server would raise it by one,
// and a watcher sees two events. Nothing in this package depends on
// resourceVersion values beyond their ordering.
//
// It is test support: only tests import it.
package strictclient

import (
	"context"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Options configures a Client.
type Options struct {
	// Clock is the API server's clock. It sets creationTimestamp on create.
	// Tests keep it separate from the controller's own clock so the two can
	// disagree, as they do on a real cluster. Required.
	Clock func() time.Time

	// IsCustomResource reports whether objects of a kind are served by the
	// custom resource strategy, which owns metadata.generation. Nil means
	// DefaultIsCustomResource.
	IsCustomResource func(schema.GroupVersionKind) bool

	// CRDs are CustomResourceDefinition manifests, normally the pinned copies
	// in internal/testinfra/crds. Objects of a kind one of them defines are
	// pruned against that version's schema on every write (see
	// Client.coerce). Build also registers their status subresources.
	CRDs []string
}

// Client is a client.WithWatch that sets the server-owned metadata the way
// kube-apiserver does and passes everything else to the fake client it wraps.
type Client struct {
	client.WithWatch

	opts Options
	crds *crdSet
}

// New wraps a fake client.
//
// Parameters:
//   - inner is the client to wrap, normally built with fake.NewClientBuilder.
//     Objects seeded into it through the builder keep the metadata the test
//     gave them, as objects restored from etcd would.
//   - opts sets the server clock, the custom resource test and the CRD
//     files to prune against.
//
// New panics when opts.Clock is nil, since every create needs it, and when a
// CRD file cannot be loaded. It does not register status subresources with
// the fake; Build does.
func New(inner client.WithWatch, opts Options) *Client {
	if opts.Clock == nil {
		panic("strictclient: Options.Clock is required")
	}
	if opts.IsCustomResource == nil {
		opts.IsCustomResource = DefaultIsCustomResource
	}
	c := &Client{WithWatch: inner, opts: opts}
	if len(opts.CRDs) > 0 {
		set, err := loadCRDs(opts.CRDs)
		if err != nil {
			panic(err)
		}
		c.crds = set
	}
	return c
}

// DefaultIsCustomResource reports whether a group looks like a custom
// resource group: it is not the core group, it has a dot in its name, and it
// is not one of the built-in *.k8s.io groups. snapshot.storage.k8s.io is a
// custom resource group despite its name, since the external snapshotter
// installs it as CRDs.
func DefaultIsCustomResource(gvk schema.GroupVersionKind) bool {
	g := gvk.Group
	switch {
	case g == "snapshot.storage.k8s.io":
		return true
	case g == "" || !strings.Contains(g, "."):
		return false
	case g == "k8s.io" || strings.HasSuffix(g, ".k8s.io"):
		return false
	}
	return true
}

// Create stores obj with a server-set creationTimestamp and uid and, for a
// custom resource, generation 1. The values the caller set in those fields
// are discarded. On success obj holds the stored object. Errors are the fake
// client's.
func (c *Client) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	isCR, err := c.isCustomResource(obj)
	if err != nil {
		return err
	}
	// The stored timestamp has whole seconds, since metav1.Time serialises as
	// RFC 3339, and the object a real create returns is the stored one.
	obj.SetCreationTimestamp(metav1.NewTime(c.opts.Clock().Truncate(time.Second)))
	obj.SetUID(uuid.NewUUID())
	if isCR {
		obj.SetGeneration(1)
	}
	if err := c.coerce(obj, true); err != nil {
		return err
	}
	return c.WithWatch.Create(ctx, obj, opts...)
}

// Update stores obj with the stored creationTimestamp and generation, and the
// stored uid when obj has none; for a custom resource whose content outside
// metadata changed, the generation goes up by one. obj is pruned against its
// CRD first (see coerce). On success obj holds the stored object. Errors are
// a Conflict when obj carries a uid other than the stored one (see
// checkUID), and otherwise the fake client's, including NotFound for an
// object that does not exist.
func (c *Client) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	old, err := c.stored(ctx, obj)
	if err != nil {
		return c.WithWatch.Update(ctx, obj, opts...)
	}
	if err := c.checkUID(obj, old); err != nil {
		return err
	}
	if err := c.checkNoNewFinalizers(obj, old); err != nil {
		return err
	}
	keepServerFields(obj, old)
	if err := c.coerce(obj, false); err != nil {
		return err
	}
	if err := c.WithWatch.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if err := c.settleGeneration(ctx, obj, old, isDryRunUpdate(opts)); err != nil {
		// The write removed the last finalizer and the object is gone; the
		// real server answers with the object it was about to store.
		if goneAfterLastFinalizer(err, old, obj) {
			return nil
		}
		return err
	}
	return nil
}

// Patch applies patch through the fake client, then restores the server-owned
// fields the patch may have changed and, for a custom resource whose content
// outside metadata changed, raises the generation by one. The patched object
// is pruned against its CRD (see prunePatched). On success obj holds
// the stored object. Errors are the fake client's, or from the follow-up
// update that sets the server-owned fields.
func (c *Client) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	old, err := c.stored(ctx, obj)
	if err != nil {
		return c.WithWatch.Patch(ctx, obj, patch, opts...)
	}
	result, err := c.patchedObject(obj, old, patch)
	if err != nil {
		return err
	}
	if result != nil {
		if err := c.checkNoNewFinalizers(result, old); err != nil {
			return err
		}
	}
	err = c.WithWatch.Patch(ctx, obj, patch, opts...)
	if err == nil {
		err = c.prunePatched(ctx, obj, isDryRunPatch(opts))
	}
	if err == nil {
		err = c.settleGeneration(ctx, obj, old, isDryRunPatch(opts))
	}
	// When the patch removed the last finalizer the object is gone and the
	// fake, or the read-back after it, fails with NotFound; the real server
	// answers with the object it was about to store.
	if err != nil && result != nil && goneAfterLastFinalizer(err, old, result) {
		keepServerFields(result, old)
		return decodeFrom(obj, result)
	}
	return err
}

// stored reads the current stored copy of obj.
func (c *Client) stored(ctx context.Context, obj client.Object) (client.Object, error) {
	old, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return nil, fmt.Errorf("strictclient: %T is not a client.Object", obj)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(obj), old); err != nil {
		return nil, err
	}
	return old, nil
}

// settleGeneration compares the object the fake just stored (in obj) with
// old and, when the server-owned fields in obj differ from what the real
// server would have stored, writes them with one more update. obj then holds
// the stored object. The stored uid is kept even when the write sent another
// one: a real server never changes a stored uid. A dry run stores nothing, so
// only obj is corrected.
func (c *Client) settleGeneration(ctx context.Context, obj, old client.Object, dryRun bool) error {
	cur := obj
	if !dryRun {
		var err error
		if cur, err = c.stored(ctx, obj); err != nil {
			return err
		}
	}
	isCR, err := c.isCustomResource(cur)
	if err != nil {
		return err
	}
	want := old.GetGeneration()
	if isCR {
		changed, err := contentChanged(old, cur)
		if err != nil {
			return err
		}
		if changed {
			want++
		}
	}
	generation, uid, created := cur.GetGeneration(), cur.GetUID(), cur.GetCreationTimestamp()
	cur.SetUID(old.GetUID())
	keepServerFields(cur, old)
	cur.SetGeneration(want)
	if dryRun {
		return nil
	}
	if generation != want || uid != cur.GetUID() || !created.Equal(ptr(cur.GetCreationTimestamp())) {
		if err := c.WithWatch.Update(ctx, cur); err != nil {
			return err
		}
	}
	return c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
}

// keepServerFields copies the fields an update cannot change from old into
// obj, as rest.BeforeUpdate does.
func keepServerFields(obj, old client.Object) {
	obj.SetGeneration(old.GetGeneration())
	if obj.GetUID() == "" {
		obj.SetUID(old.GetUID())
	}
	if created := old.GetCreationTimestamp(); !created.IsZero() {
		obj.SetCreationTimestamp(created)
	}
}

// contentChanged reports whether anything outside metadata differs between
// two stored objects. apiVersion and kind are left out because the fake does
// not always fill TypeMeta on typed objects; on a real server they are equal
// for the same stored object.
func contentChanged(old, cur client.Object) (bool, error) {
	a, err := nonMetadata(old)
	if err != nil {
		return false, err
	}
	b, err := nonMetadata(cur)
	if err != nil {
		return false, err
	}
	return !equality.Semantic.DeepEqual(a, b), nil
}

func nonMetadata(obj client.Object) (map[string]any, error) {
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return nil, fmt.Errorf("strictclient: convert %T: %w", obj, err)
	}
	out := make(map[string]any, len(content))
	for k, v := range content {
		switch k {
		case "metadata", "apiVersion", "kind":
			continue
		}
		out[k] = v
	}
	return out, nil
}

func (c *Client) isCustomResource(obj client.Object) (bool, error) {
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return false, err
	}
	return c.opts.IsCustomResource(gvk), nil
}

func isDryRunUpdate(opts []client.UpdateOption) bool {
	o := &client.UpdateOptions{}
	o.ApplyOptions(opts)
	return len(o.DryRun) > 0
}

func isDryRunPatch(opts []client.PatchOption) bool {
	o := &client.PatchOptions{}
	o.ApplyOptions(opts)
	return len(o.DryRun) > 0
}

func ptr(t metav1.Time) *metav1.Time { return &t }
