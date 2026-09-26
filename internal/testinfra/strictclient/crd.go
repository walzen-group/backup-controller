package strictclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"

	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralpruning "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// crdSet holds what the client takes from the CRD files in Options.CRDs:
// the structural schema of every served version and the versions that
// declare a status subresource.
type crdSet struct {
	schemas map[schema.GroupVersionKind]*structuralschema.Structural
	status  map[schema.GroupVersionKind]bool
}

// crdCache holds the CRD sets loaded so far in this process, keyed by
// crdCacheKey of the file list. Every client built from the same list shares
// one crdSet, so the files are read and parsed once per test binary. A
// crdSet is never changed after loadCRDs returns it: Build and coerce only
// read its maps, and the pruning functions only read the schemas
// (structuralpruning.PruneWithOptions copies the root before it sets
// XEmbeddedResource and descends through copies of the properties).
var crdCache sync.Map // string -> *crdCacheEntry

// crdCacheEntry is one crdCache slot. once makes concurrent first callers
// wait for a single load, whose result every caller then shares.
type crdCacheEntry struct {
	once sync.Once
	set  *crdSet
	err  error
}

// cachedCRDs returns the crdSet for paths, loading it with loadCRDs on first
// use and handing the same set to every later caller with the same list.
//
// The key is the ordered list of absolute paths, so a relative path names
// the same file for every test of a package. The order is part of the key
// because a later file's definition of a kind replaces an earlier one's. A
// load error is cached with the set, since the files are pinned test data.
// It is safe for concurrent use.
func cachedCRDs(paths []string) (*crdSet, error) {
	e, _ := crdCache.LoadOrStore(crdCacheKey(paths), &crdCacheEntry{})
	entry := e.(*crdCacheEntry)
	entry.once.Do(func() {
		entry.set, entry.err = loadCRDs(paths)
	})
	return entry.set, entry.err
}

// crdCacheKey joins the absolute form of each path with NUL bytes, which
// cannot appear in a path. A path that cannot be made absolute is used as
// given.
func crdCacheKey(paths []string) string {
	abs := make([]string, len(paths))
	for i, p := range paths {
		if a, err := filepath.Abs(p); err == nil {
			abs[i] = a
		} else {
			abs[i] = p
		}
	}
	return strings.Join(abs, "\x00")
}

// loadCRDs reads CustomResourceDefinition manifests (YAML or JSON, one or
// more documents per file) and builds their structural schemas.
//
// It returns an error when a file cannot be read or decoded, or when a
// version's schema is not structural. Documents of other kinds are skipped.
//
// The schemas are built as kube-apiserver 1.36.3 builds them when it starts
// serving a CRD: the v1 validation is converted to the internal version and
// passed to structuralschema.NewStructural
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/apiserver/customresource_handler.go:655-684).
// The server also prunes the schema's defaults there; defaults do not affect
// pruning of objects, so that step is left out.
func loadCRDs(paths []string) (*crdSet, error) {
	set := &crdSet{
		schemas: map[schema.GroupVersionKind]*structuralschema.Structural{},
		status:  map[schema.GroupVersionKind]bool{},
	}
	for _, path := range paths {
		if err := set.addFile(path); err != nil {
			return nil, fmt.Errorf("strictclient: CRD file %s: %w", path, err)
		}
	}
	return set, nil
}

func (s *crdSet) addFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read-only file
	dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
	for {
		var crd apiextensionsv1.CustomResourceDefinition
		if err := dec.Decode(&crd); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if crd.Kind != "CustomResourceDefinition" {
			continue
		}
		if err := s.add(&crd); err != nil {
			return err
		}
	}
}

func (s *crdSet) add(crd *apiextensionsv1.CustomResourceDefinition) error {
	for _, v := range crd.Spec.Versions {
		gvk := schema.GroupVersionKind{Group: crd.Spec.Group, Version: v.Name, Kind: crd.Spec.Names.Kind}
		if v.Subresources != nil && v.Subresources.Status != nil {
			s.status[gvk] = true
		}
		if v.Schema == nil || crd.Spec.PreserveUnknownFields {
			continue
		}
		internal := &apiextensionsinternal.CustomResourceValidation{}
		if err := apiextensionsv1.Convert_v1_CustomResourceValidation_To_apiextensions_CustomResourceValidation(v.Schema, internal, nil); err != nil {
			return fmt.Errorf("%s: convert schema: %w", gvk, err)
		}
		st, err := structuralschema.NewStructural(internal.OpenAPIV3Schema)
		if err != nil {
			return fmt.Errorf("%s: schema is not structural: %w", gvk, err)
		}
		s.schemas[gvk] = st
	}
	return nil
}

// Build registers a status subresource with the fake client builder for every
// kind whose CRD in opts.CRDs declares one, builds the fake client and wraps
// it with New.
//
// Parameters:
//   - b is the builder, with the test's objects and options already set.
//   - scheme is the scheme the fake client uses; Build sets it on b. A kind
//     the scheme does not know is registered as unstructured.
//   - opts is passed to New.
//
// Build panics when a CRD file cannot be loaded, as New does.
//
// On kube-apiserver 1.36.3 a CRD version with subresources.status gets a
// /status endpoint served by the status strategy, and the main endpoint's
// strategy stops accepting status: the handler routes /status only when the
// version declares it
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/apiserver/customresource_handler.go:336-347)
// and gives the version's strategy the status spec (customresource_handler.go:815-867).
// The fake client splits Update and Status().Update the same way for kinds
// registered with WithStatusSubresource; Client.Create drops the status on
// create for these kinds, as customResourceStrategy.PrepareForCreate does
// (pkg/registry/customresource/strategy.go:144-151).
func Build(b *fake.ClientBuilder, scheme *runtime.Scheme, opts Options) *Client {
	set, err := cachedCRDs(opts.CRDs)
	if err != nil {
		panic(err)
	}
	for gvk := range set.status {
		var obj client.Object
		if scheme.Recognizes(gvk) {
			o, err := scheme.New(gvk)
			if err != nil {
				panic(err)
			}
			obj = o.(client.Object)
			// A kind registered as unstructured comes back without its
			// kind, which the fake builder needs.
			if u, ok := obj.(*unstructured.Unstructured); ok {
				u.SetGroupVersionKind(gvk)
			}
		} else {
			u := &unstructured.Unstructured{}
			u.SetGroupVersionKind(gvk)
			obj = u
		}
		b = b.WithStatusSubresource(obj)
	}
	return newClient(b.WithScheme(scheme).Build(), opts, set)
}

// coerce prunes obj the way kube-apiserver 1.36.3 prunes a custom resource
// it decodes, and on create drops the status of a kind with a status
// subresource.
//
// Parameters:
//   - obj is the object the write would store. It is changed in place.
//   - create is true for a create.
//
// It returns an error when obj's kind cannot be found in the scheme or obj
// cannot be converted. A kind without a CRD in Options.CRDs is left alone.
//
// Pruning uses the apiextensions code itself: the server's
// unstructuredSchemaCoercer.apply calls pruning.PruneWithOptions with the
// version's structural schema at the resource root, then
// defaulting.PruneNonNullableNullsWithoutDefaults
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/apiserver/customresource_handler.go:1430-1440).
// The second function is copied here because its package
// (pkg/apiserver/schema/defaulting) imports the CEL libraries, whose modules
// are not in this repository's go.sum. A field the schema does not declare is dropped unless an enclosing schema
// sets x-kubernetes-preserve-unknown-fields; apiVersion, kind and metadata
// are kept (pkg/apiserver/schema/pruning/algorithm.go:29-113). The coercer
// runs on every decode of a request body and of the stored object, so the
// same pruning applies to create, update, patch and status writes. The
// metadata coercion that follows it (schemaobjectmeta.CoerceWithOptions) and
// defaulting are left out.
func (c *Client) coerce(obj client.Object, create bool) error {
	if c.crds == nil {
		return nil
	}
	gvk, err := apiutil.GVKForObject(obj, c.Scheme())
	if err != nil {
		return err
	}
	s := c.crds.schemas[gvk]
	dropStatus := create && c.crds.status[gvk]
	if s == nil && !dropStatus {
		return nil
	}
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(obj)
	if err != nil {
		return fmt.Errorf("strictclient: convert %T: %w", obj, err)
	}
	if dropStatus {
		delete(content, "status")
	}
	if s != nil {
		structuralpruning.Prune(content, s, true)
		pruneNonNullableNullsWithoutDefaults(content, s)
	}
	if u, ok := obj.(*unstructured.Unstructured); ok {
		u.Object = content
		return nil
	}
	v := reflect.ValueOf(obj).Elem()
	v.Set(reflect.Zero(v.Type()))
	return runtime.DefaultUnstructuredConverter.FromUnstructured(content, obj)
}

// prunePatched prunes the object a patch stored, since the fake client
// applies a patch without a schema. A real server prunes the patched object
// before it stores it; the wrapper stores the pruned object with one more
// update, and one more status update when the fake kept the status apart.
// A dry run stores nothing, so only obj is pruned. On success obj holds the
// stored object. Errors are the fake client's.
func (c *Client) prunePatched(ctx context.Context, obj client.Object, dryRun bool) error {
	if c.crds == nil {
		return nil
	}
	if dryRun {
		return c.coerce(obj, false)
	}
	cur, err := c.stored(ctx, obj)
	if err != nil {
		return err
	}
	pruned, ok := cur.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("strictclient: %T is not a client.Object", cur)
	}
	if err := c.coerce(pruned, false); err != nil {
		return err
	}
	a, err := runtime.DefaultUnstructuredConverter.ToUnstructured(cur)
	if err != nil {
		return err
	}
	b, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pruned)
	if err != nil {
		return err
	}
	if reflect.DeepEqual(a, b) {
		return nil
	}
	want := b["status"]
	target, ok := pruned.DeepCopyObject().(client.Object)
	if !ok {
		return fmt.Errorf("strictclient: %T is not a client.Object", pruned)
	}
	if err := c.WithWatch.Update(ctx, pruned); err != nil {
		return err
	}
	got, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pruned)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(got["status"], want) {
		target.SetResourceVersion(pruned.GetResourceVersion())
		if err := c.WithWatch.Status().Update(ctx, target); err != nil {
			return err
		}
	}
	return c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
}

// pruneNonNullableNullsWithoutDefaults removes null values the schema does
// not allow and has no default for. It is a copy of
// defaulting.PruneNonNullableNullsWithoutDefaults
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/apiserver/schema/defaulting/prunenulls.go:21-67);
// see coerce for why it is copied.
func pruneNonNullableNullsWithoutDefaults(x any, s *structuralschema.Structural) {
	switch x := x.(type) {
	case map[string]any:
		for k, v := range x {
			sub := schemaForField(k, s)
			if v == nil && sub != nil && !sub.Nullable && sub.Default.Object == nil {
				delete(x, k)
			} else {
				pruneNonNullableNullsWithoutDefaults(v, sub)
			}
		}
	case []any:
		var items *structuralschema.Structural
		if s != nil {
			items = s.Items
		}
		for i := range x {
			pruneNonNullableNullsWithoutDefaults(x[i], items)
		}
	}
}

func schemaForField(field string, s *structuralschema.Structural) *structuralschema.Structural {
	if s == nil {
		return nil
	}
	if sub, ok := s.Properties[field]; ok {
		return &sub
	}
	if s.AdditionalProperties != nil {
		return s.AdditionalProperties.Structural
	}
	return nil
}
