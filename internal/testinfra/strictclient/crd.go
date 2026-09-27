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
	"time"
	"unsafe"

	apiextensionsinternal "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	structuralpruning "k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/uuid"
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
	entry, ok := e.(*crdCacheEntry)
	if !ok {
		panic(fmt.Sprintf("strictclient: the CRD cache holds a %T", e))
	}
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
// Before the fake stores the objects seeded into b, Build prunes and
// defaults them and fills in the server fields the test left empty (see
// Client.seed), so a seeded custom resource never has generation 0 or a
// missing default.
//
// Build panics when a CRD file cannot be loaded, as New does, when
// opts.Clock is nil, and when a seeded object cannot be coerced.
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
			co, ok := o.(client.Object)
			if !ok {
				panic(fmt.Sprintf("strictclient: the scheme makes %s as a %T, which is not a client.Object", gvk, o))
			}
			obj = co
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
	// The seeded objects are coerced before the fake stores them, by a
	// client that has the scheme but no fake yet.
	pre := &Client{opts: opts, scheme: scheme}
	if pre.opts.Clock == nil {
		panic("strictclient: Options.Clock is required")
	}
	if pre.opts.IsCustomResource == nil {
		pre.opts.IsCustomResource = DefaultIsCustomResource
	}
	if len(opts.CRDs) > 0 {
		pre.crds = set
	}
	pre.seed(b)
	return newClient(b.WithScheme(scheme).Build(), opts, set)
}

// coerce prunes and defaults obj the way kube-apiserver 1.36.3 prunes and
// defaults a custom resource it decodes, and on create drops the status of a
// kind with a status subresource.
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
// metadata coercion that follows it (schemaobjectmeta.CoerceWithOptions) is
// left out.
//
// Defaulting follows the pruning, as on the server: the codec that decodes
// request bodies and the storage codec both wrap the coercing decoder in a
// versioning codec whose defaulter is unstructuredDefaulter
// (customresource_handler.go:1189-1199 and 1290-1305), and the versioning
// codec calls the defaulter after the decoder returns
// (k8s.io/apimachinery@v0.36.0/pkg/runtime/serializer/versioning/versioning.go:139-170).
// unstructuredDefaulter.Default calls structuraldefaulting.Default with the
// version's structural schema (customresource_handler.go:1240-1249), so a
// create, update, patch and every read return the object with the schema's
// defaults filled in. applyDefaults is a copy of that function, for the same
// reason as the null pruning. The server also prunes the default values
// themselves when it builds the schema (structuraldefaulting.PruneDefaults,
// customresource_handler.go:674-682); that is left out because CRD
// validation already refuses a v1 default with unknown fields
// (pkg/apiserver/schema/defaulting/validation.go:122-128), so it only matters
// for a default that reaches into metadata or an embedded resource.
func (c *Client) coerce(obj client.Object, create bool) error {
	if c.crds == nil {
		return nil
	}
	gvk, err := apiutil.GVKForObject(obj, c.scheme)
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
	if s != nil {
		structuralpruning.Prune(content, s, true)
		pruneNonNullableNullsWithoutDefaults(content, s)
		applyDefaults(content, s)
	}
	if dropStatus {
		delete(content, "status")
		// The stored object is decoded, and so defaulted, again on every
		// read; without a status only a default for status itself shows.
		applyDefaults(content, s)
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

// applyDefaults fills in the schema's default values in x. It is a copy of
// structuraldefaulting.Default
// (k8s.io/apiextensions-apiserver@v0.36.0/pkg/apiserver/schema/defaulting/algorithm.go:24-69);
// see coerce for why it is copied.
//
// A field that is missing, or null where the schema does not allow null,
// gets a deep copy of its default; then every field the schema describes is
// defaulted in turn. Scalars are left as they are.
func applyDefaults(x any, s *structuralschema.Structural) {
	if s == nil {
		return
	}
	switch x := x.(type) {
	case map[string]any:
		for k, prop := range s.Properties {
			if prop.Default.Object == nil {
				continue
			}
			if _, found := x[k]; !found || isNonNullableNull(x[k], &prop) {
				x[k] = runtime.DeepCopyJSONValue(prop.Default.Object)
			}
		}
		for k := range x {
			if prop, found := s.Properties[k]; found {
				applyDefaults(x[k], &prop)
			} else if s.AdditionalProperties != nil {
				if isNonNullableNull(x[k], s.AdditionalProperties.Structural) {
					x[k] = runtime.DeepCopyJSONValue(s.AdditionalProperties.Structural.Default.Object)
				}
				applyDefaults(x[k], s.AdditionalProperties.Structural)
			}
		}
	case []any:
		for i := range x {
			if isNonNullableNull(x[i], s.Items) {
				x[i] = runtime.DeepCopyJSONValue(s.Items.Default.Object)
			}
			applyDefaults(x[i], s.Items)
		}
	}
}

// isNonNullableNull reports whether x is null where s does not allow null.
func isNonNullableNull(x any, s *structuralschema.Structural) bool {
	return x == nil && s != nil && !s.Nullable
}

// seed brings the objects seeded into b (WithObjects, WithRuntimeObjects and
// WithLists) to the state a real server would have stored them in, before
// b.Build adds them to the fake's tracker. Each object is changed in place,
// as a create changes the object it is given:
//
//   - it is pruned and defaulted against its CRD (see Client.coerce); its
//     status is kept, since a seeded object stands for one already stored;
//   - an empty uid gets a fresh UUID and a zero creationTimestamp the server
//     clock, since no stored object lacks either (rest.FillObjectMetaSystemFields,
//     k8s.io/apiserver@v0.36.3/pkg/registry/rest/meta.go:39-42);
//   - generation 0 becomes 1 for a kind whose strategy sets it on create (see
//     Client.generationRule), since the smallest stored value is 1.
//
// Values the test set in those fields are kept, so a test can still seed an
// old object or one whose generation is ahead of its observedGeneration.
//
// The builder keeps the seeded objects in unexported fields, which seed
// reads with reflect. It panics when a field is missing, which means
// controller-runtime's fake changed and seed needs updating, and when an
// object cannot be coerced.
func (c *Client) seed(b *fake.ClientBuilder) {
	v := reflect.ValueOf(b).Elem()
	for _, o := range builderField[[]client.Object](v, "initObject") {
		c.seedObject(o)
	}
	for _, o := range builderField[[]runtime.Object](v, "initRuntimeObjects") {
		c.seedObject(o)
	}
	for _, l := range builderField[[]client.ObjectList](v, "initLists") {
		if err := apimeta.EachListItem(l, func(o runtime.Object) error {
			c.seedObject(o)
			return nil
		}); err != nil {
			panic(fmt.Errorf("strictclient: seeded list %T: %w", l, err))
		}
	}
}

// builderField reads an unexported field of the fake client builder.
//
// Parameters:
//   - v is the builder struct, from reflect.ValueOf(b).Elem().
//   - name is the name of the field that seed reads.
//
// It returns the value of the field as a T. It panics when the builder has
// no field with that name, or when the field is not a T. Both mean that
// controller-runtime's fake changed and seed needs updating.
func builderField[T any](v reflect.Value, name string) T {
	f := v.FieldByName(name)
	if !f.IsValid() {
		panic(fmt.Sprintf("strictclient: fake.ClientBuilder has no field %s; update seed for this controller-runtime version", name))
	}
	value := reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem().Interface()
	typed, ok := value.(T)
	if !ok {
		panic(fmt.Sprintf("strictclient: fake.ClientBuilder field %s is a %T; update seed for this controller-runtime version", name, value))
	}
	return typed
}

// seedObject is seed for one object. An object without object metadata is
// left alone.
func (c *Client) seedObject(o runtime.Object) {
	obj, ok := o.(client.Object)
	if !ok {
		return
	}
	if obj.GetUID() == "" {
		obj.SetUID(uuid.NewUUID())
	}
	if ts := obj.GetCreationTimestamp(); ts.IsZero() {
		obj.SetCreationTimestamp(metav1.NewTime(c.opts.Clock().Truncate(time.Second)))
	}
	rule, err := c.generationRule(obj)
	if err != nil {
		panic(fmt.Errorf("strictclient: seeded %T: %w", obj, err))
	}
	if rule != generationKept && obj.GetGeneration() == 0 {
		obj.SetGeneration(1)
	}
	if err := c.coerce(obj, false); err != nil {
		panic(fmt.Errorf("strictclient: seeded %T: %w", obj, err))
	}
}
