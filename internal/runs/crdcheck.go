package runs

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The names of the CustomResourceDefinitions a run checks before it changes
// anything. The controller's ClusterRole grants get on exactly these.
const (
	backupRunsCRD  = "backupruns.backup.wlz.li"
	restoreRunsCRD = "restoreruns.backup.wlz.li"
)

// crdGVK is the kind of a CustomResourceDefinition. The check reads it as an
// unstructured object, so the controller's scheme needs no apiextensions
// types.
var crdGVK = schema.GroupVersionKind{Group: "apiextensions.k8s.io", Version: "v1", Kind: "CustomResourceDefinition"}

// schemaCache remembers the result of the last schema check of each CRD,
// keyed by the CRD's name and resourceVersion. A CRD changes rarely, so most
// checks cost one read and no walk; any update to the CRD gives it a new
// resourceVersion, and the next check walks the new schema. The zero value is
// ready to use, and it is safe for concurrent use.
type schemaCache struct {
	mu      sync.Mutex
	entries map[string]schemaCacheEntry
}

// schemaCacheEntry is the result of one walk of a CRD's schema.
type schemaCacheEntry struct {
	resourceVersion string
	gaps            []string
}

// crdOutdated reports the fields the installed CRD crdName lacks and that
// the controller writes on the kind of sample, as a message for a run's Ready
// condition.
//
// Parameters:
//   - reader reads the CRD straight from the API server.
//   - crdName is the CRD's metadata.name, such as backupruns.backup.wlz.li.
//   - kind is the kind's name as the message shows it, such as BackupRun.
//   - sample is a value of the Go type the controller writes, such as
//     backupv1alpha1.BackupRun{}.
//   - harm says what a dropped field would do to the run, for the message.
//
// It returns an empty message when the schema declares every field. It
// returns a message and no error when the schema lacks a field, and when the
// controller may not read the CRD or the CRD does not exist, because a run
// that cannot be checked must not go on (it fails closed). Any other failed
// read comes back as an error, for a retry.
func (c *schemaCache) crdOutdated(ctx context.Context, reader client.Reader, crdName, kind string, sample any, harm string) (string, error) {
	crd := &unstructured.Unstructured{}
	crd.SetGroupVersionKind(crdGVK)
	if err := reader.Get(ctx, types.NamespacedName{Name: crdName}, crd); err != nil {
		switch {
		case apierrors.IsForbidden(err):
			return fmt.Sprintf("the controller may not read the CustomResourceDefinition %s, so it cannot check that the "+
				"installed %s CRD stores every field it writes: grant get on customresourcedefinitions.apiextensions.k8s.io "+
				"named %s (the ClusterRole of this release does): %v", crdName, kind, crdName, err), nil
		case apierrors.IsNotFound(err):
			return fmt.Sprintf("the CustomResourceDefinition %s is not installed: apply the CRDs of this release", crdName), nil
		}
		return "", fmt.Errorf("read the CustomResourceDefinition %s: %w", crdName, err)
	}
	gaps, ok := c.lookup(crdName, crd.GetResourceVersion())
	if !ok {
		var err error
		if gaps, err = schemaGaps(crd, sample); err != nil {
			return fmt.Sprintf("the installed %s CRD cannot be checked: %v; apply the CRDs of this release", kind, err), nil
		}
		c.store(crdName, crd.GetResourceVersion(), gaps)
	}
	if len(gaps) == 0 {
		return "", nil
	}
	return fmt.Sprintf("the installed %s CRD does not declare %s, which this controller writes; the API server would drop it "+
		"and %s. Apply the CRDs of this release (kubectl apply --server-side -f the release's CRD manifest, or config/crd). "+
		"Helm does not upgrade CRDs on its own", kind, strings.Join(gaps, ", "), harm), nil
}

// lookup returns the cached gaps of crdName when the cache holds a walk of
// the same resourceVersion.
func (c *schemaCache) lookup(crdName, resourceVersion string) ([]string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[crdName]
	if !ok || resourceVersion == "" || e.resourceVersion != resourceVersion {
		return nil, false
	}
	return e.gaps, true
}

// store records the gaps of crdName at resourceVersion.
func (c *schemaCache) store(crdName, resourceVersion string, gaps []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = map[string]schemaCacheEntry{}
	}
	c.entries[crdName] = schemaCacheEntry{resourceVersion: resourceVersion, gaps: gaps}
}

// schemaGaps returns the JSON path of every field the Go type of sample has
// and the v1alpha1 schema of crd does not declare, sorted, such as
// status.restartPending.
//
// Parameters:
//   - crd is a CustomResourceDefinition as an unstructured object.
//   - sample is a value of the Go type whose fields are checked.
//
// It returns an error when crd serves no v1alpha1 version with a schema.
//
// It walks the type by reflection over its json tags: a struct's fields map
// to the schema's properties, a slice's element to items, a map's value to
// additionalProperties, a pointer to its element. metav1.Time,
// metav1.Duration, resource.Quantity and any type that implements
// json.Marshaler are leaves, as are strings, numbers and booleans. The
// top-level metadata is skipped, since the API server handles it itself. A
// schema node with x-kubernetes-preserve-unknown-fields: true covers
// everything below it.
func schemaGaps(crd *unstructured.Unstructured, sample any) ([]string, error) {
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	for _, v := range versions {
		version, _ := v.(map[string]any)
		if version["name"] != "v1alpha1" {
			continue
		}
		root, found, _ := unstructured.NestedMap(version, "schema", "openAPIV3Schema")
		if !found {
			break
		}
		var gaps []string
		walkSchema(reflect.TypeOf(sample), root, "", map[reflect.Type]bool{}, &gaps)
		sort.Strings(gaps)
		return gaps, nil
	}
	return nil, fmt.Errorf("the CustomResourceDefinition %s serves no v1alpha1 version with a schema", crd.GetName())
}

// leafTypes are the struct types that serialize as a single JSON value.
var leafTypes = map[reflect.Type]bool{
	reflect.TypeOf(metav1.Time{}):       true,
	reflect.TypeOf(metav1.MicroTime{}):  true,
	reflect.TypeOf(metav1.Duration{}):   true,
	reflect.TypeOf(resource.Quantity{}): true,
}

// marshaler is the json.Marshaler interface type.
var marshaler = reflect.TypeOf((*json.Marshaler)(nil)).Elem()

// walkSchema adds to gaps the path of every field of t that node does not
// declare. path is the JSON path of node, empty at the root. onPath holds
// the struct types being walked above node, so a type that contains itself
// ends the walk there.
func walkSchema(t reflect.Type, node map[string]any, path string, onPath map[reflect.Type]bool, gaps *[]string) {
	if preserve, _ := node["x-kubernetes-preserve-unknown-fields"].(bool); preserve {
		return
	}
	for t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if leafTypes[t] || t.Implements(marshaler) || reflect.PointerTo(t).Implements(marshaler) {
		return
	}
	switch t.Kind() {
	case reflect.Struct:
		if onPath[t] {
			return
		}
		onPath[t] = true
		defer delete(onPath, t)
		properties, _ := node["properties"].(map[string]any)
		walkFields(t, properties, path, onPath, gaps)
	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			return
		}
		items, ok := node["items"].(map[string]any)
		if !ok {
			*gaps = append(*gaps, path+"[]")
			return
		}
		walkSchema(t.Elem(), items, path+"[]", onPath, gaps)
	case reflect.Map:
		values, ok := node["additionalProperties"].(map[string]any)
		if !ok {
			*gaps = append(*gaps, path+"{}")
			return
		}
		walkSchema(t.Elem(), values, path+"{}", onPath, gaps)
	default:
		// Any other kind, a scalar or an interface, has no fields or
		// elements the walk could check.
	}
}

// walkFields checks each field of the struct type t against properties, the
// properties of the schema node at path. An embedded struct with an inline
// or empty json name adds its fields to the same node.
func walkFields(t reflect.Type, properties map[string]any, path string, onPath map[reflect.Type]bool, gaps *[]string) {
	for i := range t.NumField() {
		f := t.Field(i)
		if !f.IsExported() {
			continue
		}
		tag := f.Tag.Get("json")
		name, _, _ := strings.Cut(tag, ",")
		if name == "-" {
			continue
		}
		if name == "" {
			ft := f.Type
			for ft.Kind() == reflect.Pointer {
				ft = ft.Elem()
			}
			if f.Anonymous && ft.Kind() == reflect.Struct {
				walkFields(ft, properties, path, onPath, gaps)
				continue
			}
			name = f.Name
		}
		if path == "" && name == "metadata" {
			continue
		}
		fieldPath := name
		if path != "" {
			fieldPath = path + "." + name
		}
		child, ok := properties[name].(map[string]any)
		if !ok {
			*gaps = append(*gaps, fieldPath)
			continue
		}
		walkSchema(f.Type, child, fieldPath, onPath, gaps)
	}
}
