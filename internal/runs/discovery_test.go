package runs

import (
	"context"
	"errors"
	"io"
	"os"
	"sort"
	"strings"
	"testing"

	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/restmapper"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// servedKinds is the RESTMapper of a test client: what the API server's
// discovery would report. It serves the built-in kinds of the scheme and each
// served version of the CRDs the client was built with. A test deletes a
// group's CRDs with remove, after which a lookup of any kind in that group
// fails with the no-match error the real mapper gives.
type servedKinds struct {
	meta.RESTMapper

	t        *testing.T
	scheme   *runtime.Scheme
	crdFiles []string
	removed  map[string]bool
}

// newServedKinds returns a servedKinds for the scheme s and the CRD manifests
// in crdFiles, with nothing removed.
func newServedKinds(t *testing.T, s *runtime.Scheme, crdFiles []string) *servedKinds {
	t.Helper()
	k := &servedKinds{t: t, scheme: s, crdFiles: crdFiles, removed: map[string]bool{}}
	k.RESTMapper = discoveryMapper(t, s, crdFiles, k.removed)
	return k
}

// remove stands in for deleting every CRD of the given API groups: from then
// on the mapper serves no kind in them. Deleting a CRD also deletes its
// objects on a real cluster; the test deletes those itself.
func (k *servedKinds) remove(groups ...string) {
	k.t.Helper()
	for _, g := range groups {
		k.removed[g] = true
	}
	k.RESTMapper = discoveryMapper(k.t, k.scheme, k.crdFiles, k.removed)
}

// clusterScoped are the built-in kinds in the test scheme that have no
// namespace.
var clusterScoped = map[schema.GroupKind]bool{
	{Kind: "Namespace"}:        true,
	{Kind: "Node"}:             true,
	{Kind: "PersistentVolume"}: true,
	{Group: "apiextensions.k8s.io", Kind: "CustomResourceDefinition"}: true,
}

// discoveryMapper builds a RESTMapper the way controller-runtime builds one
// from the API server's discovery documents, with
// restmapper.NewDiscoveryRESTMapper.
//
// Parameters:
//   - s supplies the built-in kinds: every kind whose group
//     strictclient.DefaultIsCustomResource does not count as a custom
//     resource group.
//   - crdFiles are CRD manifests. Each served version of each CRD in them is
//     served.
//   - removed are API groups the mapper leaves out.
//
// Each group's versions are ordered by Kubernetes version priority (v2, v1,
// v1beta2, v1beta1), which is the order the API server's discovery gives, so
// a lookup by group and kind with no version gets the version the API server
// prefers. A file that can't be read or decoded fails the test.
func discoveryMapper(t *testing.T, s *runtime.Scheme, crdFiles []string, removed map[string]bool) meta.RESTMapper {
	t.Helper()
	groups := map[string]*restmapper.APIGroupResources{}
	add := func(gvk schema.GroupVersionKind, plural string, namespaced bool) {
		if removed[gvk.Group] {
			return
		}
		g, ok := groups[gvk.Group]
		if !ok {
			g = &restmapper.APIGroupResources{Group: metav1.APIGroup{Name: gvk.Group}, VersionedResources: map[string][]metav1.APIResource{}}
			groups[gvk.Group] = g
		}
		if _, ok := g.VersionedResources[gvk.Version]; !ok {
			g.Group.Versions = append(g.Group.Versions, metav1.GroupVersionForDiscovery{GroupVersion: gvk.GroupVersion().String(), Version: gvk.Version})
		}
		g.VersionedResources[gvk.Version] = append(g.VersionedResources[gvk.Version], metav1.APIResource{Name: plural, Kind: gvk.Kind, Namespaced: namespaced})
	}

	for gvk := range s.AllKnownTypes() {
		if gvk.Version == runtime.APIVersionInternal || strings.HasSuffix(gvk.Kind, "List") || strictclient.DefaultIsCustomResource(gvk) {
			continue
		}
		plural, _ := meta.UnsafeGuessKindToResource(gvk)
		add(gvk, plural.Resource, !clusterScoped[gvk.GroupKind()])
	}
	for _, path := range crdFiles {
		f, err := os.Open(path)
		if err != nil {
			t.Fatalf("open %s: %v", path, err)
		}
		dec := utilyaml.NewYAMLOrJSONDecoder(f, 4096)
		for {
			var crd apiextensionsv1.CustomResourceDefinition
			if err := dec.Decode(&crd); err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				t.Fatalf("decode %s: %v", path, err)
			}
			for _, v := range crd.Spec.Versions {
				if v.Served {
					gvk := schema.GroupVersionKind{Group: crd.Spec.Group, Version: v.Name, Kind: crd.Spec.Names.Kind}
					add(gvk, crd.Spec.Names.Plural, crd.Spec.Scope == apiextensionsv1.NamespaceScoped)
				}
			}
		}
		_ = f.Close()
	}

	resources := make([]*restmapper.APIGroupResources, 0, len(groups))
	for _, g := range groups {
		sort.Slice(g.Group.Versions, func(i, j int) bool {
			return version.CompareKubeAwareVersionStrings(g.Group.Versions[i].Version, g.Group.Versions[j].Version) > 0
		})
		g.Group.PreferredVersion = g.Group.Versions[0]
		resources = append(resources, g)
	}
	return restmapper.NewDiscoveryRESTMapper(resources)
}

// servingOnly wraps c so that every call first looks up the object's group,
// version and kind in c's RESTMapper, as controller-runtime's client does
// before it sends a request, and fails with the mapper's error when the kind
// is not served at that version. A call for a kind whose CRD is not installed
// then fails with the no-match error a real client returns, where the fake
// alone would answer from its scheme.
func servingOnly(c client.Client) client.Client {
	served := func(cl client.Client, obj runtime.Object) error {
		gvk, err := apiutil.GVKForObject(obj, cl.Scheme())
		if err != nil {
			return err
		}
		if _, list := obj.(client.ObjectList); list {
			gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
		}
		_, err = cl.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version)
		return err
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := served(cl, obj); err != nil {
				return err
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := served(cl, list); err != nil {
				return err
			}
			return cl.List(ctx, list, opts...)
		},
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := served(cl, obj); err != nil {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := served(cl, obj); err != nil {
				return err
			}
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if err := served(cl, obj); err != nil {
				return err
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err := served(cl, obj); err != nil {
				return err
			}
			return cl.Delete(ctx, obj, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if err := served(cl, obj); err != nil {
				return err
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			if err := served(cl, obj); err != nil {
				return err
			}
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
	})
}
