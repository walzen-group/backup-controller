package runs

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strings"
	"sync"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	apidiscoveryv2 "k8s.io/api/apidiscovery/v2"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/apimachinery/pkg/version"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"
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

// fluxDiscovery is an API server's discovery endpoint, on an httptest
// server, that serves Flux's Kustomization kind at one version at a time. It
// also serves VolSync at volsync.backube/v1alpha1. A
// test switches the version mid-run with serve, which stands in for a Flux
// upgrade that stops serving the version the controller cached.
//
// It answers /apis/<group>/<version> for a version it does not serve with a
// plain-text "404 page not found", as kube-apiserver's not-found handler
// does (checked against Kubernetes 1.36.4 on docker-desktop). With
// aggregated set, /api and /apis answer with aggregated discovery
// (apidiscovery.k8s.io/v2), which Kubernetes 1.36 serves; otherwise with the
// legacy APIVersions and APIGroupList.
type fluxDiscovery struct {
	mu         sync.Mutex
	version    string
	aggregated bool
	server     *httptest.Server
}

// newFluxDiscovery starts a fluxDiscovery that serves Kustomization at
// version, and stops it when the test ends.
func newFluxDiscovery(t *testing.T, version string, aggregated bool) *fluxDiscovery {
	t.Helper()
	d := &fluxDiscovery{version: version, aggregated: aggregated}
	d.server = httptest.NewServer(http.HandlerFunc(d.handle))
	t.Cleanup(d.server.Close)
	return d
}

// serve makes the endpoint serve Kustomization at version only.
func (d *fluxDiscovery) serve(version string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.version = version
}

// served returns the version the endpoint serves Kustomization at.
func (d *fluxDiscovery) served() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.version
}

// mapper returns controller-runtime's lazy RESTMapper, the one the manager
// gives every client, over this endpoint. It caches what it has looked up.
func (d *fluxDiscovery) mapper(t *testing.T) meta.RESTMapper {
	t.Helper()
	m, err := apiutil.NewDynamicRESTMapper(&rest.Config{Host: d.server.URL}, d.server.Client())
	if err != nil {
		t.Fatal(err)
	}
	return m
}

// volsyncGV is the VolSync API version the discovery endpoint serves beside
// Flux, as every cluster the controller runs on does. volsyncUnsupported looks
// it up on every BackupRun pass.
var volsyncGV = volsyncv1alpha1.GroupVersion

// handle answers one discovery request.
func (d *fluxDiscovery) handle(w http.ResponseWriter, req *http.Request) {
	version := d.served()
	gv := fluxGroup + "/" + version
	write := func(contentType string, body any) {
		w.Header().Set("Content-Type", contentType)
		_ = json.NewEncoder(w).Encode(body)
	}
	aggregated := func(items []apidiscoveryv2.APIGroupDiscovery) {
		write(discovery.AcceptV2, apidiscoveryv2.APIGroupDiscoveryList{
			TypeMeta: metav1.TypeMeta{Kind: "APIGroupDiscoveryList", APIVersion: "apidiscovery.k8s.io/v2"},
			Items:    items,
		})
	}
	verbs := []string{"get", "list", "watch", "create", "update", "patch", "delete"}
	switch req.URL.Path {
	case "/api":
		if d.aggregated {
			aggregated([]apidiscoveryv2.APIGroupDiscovery{{Versions: []apidiscoveryv2.APIVersionDiscovery{{Version: "v1", Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent}}}})
			return
		}
		write("application/json", metav1.APIVersions{TypeMeta: metav1.TypeMeta{Kind: "APIVersions"}, Versions: []string{"v1"}})
	case "/apis":
		if d.aggregated {
			volsyncResource := func(resource, singular, kind string) apidiscoveryv2.APIResourceDiscovery {
				return apidiscoveryv2.APIResourceDiscovery{
					Resource: resource, SingularResource: singular, Scope: apidiscoveryv2.ScopeNamespace, Verbs: verbs,
					ResponseKind: &metav1.GroupVersionKind{Group: volsyncGV.Group, Version: volsyncGV.Version, Kind: kind},
				}
			}
			aggregated([]apidiscoveryv2.APIGroupDiscovery{{
				ObjectMeta: metav1.ObjectMeta{Name: fluxGroup},
				Versions: []apidiscoveryv2.APIVersionDiscovery{{
					Version: version,
					Resources: []apidiscoveryv2.APIResourceDiscovery{{
						Resource: "kustomizations", SingularResource: "kustomization", Scope: apidiscoveryv2.ScopeNamespace, Verbs: verbs,
						ResponseKind: &metav1.GroupVersionKind{Group: fluxGroup, Version: version, Kind: "Kustomization"},
					}},
					Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
				}},
			}, {
				ObjectMeta: metav1.ObjectMeta{Name: volsyncGV.Group},
				Versions: []apidiscoveryv2.APIVersionDiscovery{{
					Version: volsyncGV.Version,
					Resources: []apidiscoveryv2.APIResourceDiscovery{
						volsyncResource("replicationsources", "replicationsource", "ReplicationSource"),
						volsyncResource("replicationdestinations", "replicationdestination", "ReplicationDestination"),
					},
					Freshness: apidiscoveryv2.DiscoveryFreshnessCurrent,
				}},
			}})
			return
		}
		served := metav1.GroupVersionForDiscovery{GroupVersion: gv, Version: version}
		volsync := metav1.GroupVersionForDiscovery{GroupVersion: volsyncGV.String(), Version: volsyncGV.Version}
		write("application/json", metav1.APIGroupList{TypeMeta: metav1.TypeMeta{Kind: "APIGroupList", APIVersion: "v1"},
			Groups: []metav1.APIGroup{
				{Name: fluxGroup, Versions: []metav1.GroupVersionForDiscovery{served}, PreferredVersion: served},
				{Name: volsyncGV.Group, Versions: []metav1.GroupVersionForDiscovery{volsync}, PreferredVersion: volsync},
			}})
	case "/apis/" + volsyncGV.String():
		write("application/json", metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: volsyncGV.String(),
			APIResources: []metav1.APIResource{
				{Name: "replicationsources", SingularName: "replicationsource", Namespaced: true, Kind: "ReplicationSource", Verbs: verbs},
				{Name: "replicationdestinations", SingularName: "replicationdestination", Namespaced: true, Kind: "ReplicationDestination", Verbs: verbs},
			}})
	case "/apis/" + gv:
		write("application/json", metav1.APIResourceList{TypeMeta: metav1.TypeMeta{Kind: "APIResourceList", APIVersion: "v1"}, GroupVersion: gv,
			APIResources: []metav1.APIResource{{Name: "kustomizations", SingularName: "kustomization", Namespaced: true, Kind: "Kustomization", Verbs: verbs}}})
	default:
		http.NotFound(w, req)
	}
}

// fluxServedAt wraps c, a strict client, for a cluster whose Flux serves
// Kustomization at the version d serves, and whose client looks versions up
// through mapper, which RESTMapper returns.
//
// A call for a Kustomization at a version d does not serve fails the way
// client-go fails a request the API server answered with a plain-text 404:
// a NotFound whose details carry an UnexpectedServerResponse cause (checked
// against Kubernetes 1.36.4). A call at the served version reaches the
// Kustomization c stores at v1, as the API server converts between the
// versions of one kind; this stands in for a Kustomization whose fields did
// not change between the versions.
func fluxServedAt(c client.Client, d *fluxDiscovery, mapper meta.RESTMapper) client.Client {
	kustomization := func(obj runtime.Object) (*unstructured.Unstructured, bool) {
		u, ok := obj.(*unstructured.Unstructured)
		return u, ok && u.GroupVersionKind().GroupKind() == KustomizationGVK.GroupKind()
	}
	// call runs do on obj stored at v1 and gives obj back its version. name
	// is the object's name, which client-go puts in the error's details.
	call := func(verb, name string, obj client.Object, do func() error) error {
		u, ok := kustomization(obj)
		if !ok {
			return do()
		}
		gvk := u.GroupVersionKind()
		if gvk.Version != d.served() {
			return apierrors.NewGenericServerResponse(http.StatusNotFound, verb,
				schema.GroupResource{Group: fluxGroup, Resource: "kustomizations"}, name, "404 page not found", 0, true)
		}
		u.SetGroupVersionKind(KustomizationGVK)
		err := do()
		u.SetGroupVersionKind(gvk)
		return err
	}
	intercepted := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			return call("get", key.Name, obj, func() error { return cl.Get(ctx, key, obj, opts...) })
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return call("patch", obj.GetName(), obj, func() error { return cl.Patch(ctx, obj, patch, opts...) })
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			return call("put", obj.GetName(), obj, func() error { return cl.Update(ctx, obj, opts...) })
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			return call("delete", obj.GetName(), obj, func() error { return cl.Delete(ctx, obj, opts...) })
		},
	})
	return withMapper{WithWatch: intercepted, mapper: mapper}
}

// withMapper is a client whose RESTMapper is mapper.
type withMapper struct {
	client.WithWatch
	mapper meta.RESTMapper
}

// RESTMapper returns the wrapped mapper.
func (c withMapper) RESTMapper() meta.RESTMapper { return c.mapper }
