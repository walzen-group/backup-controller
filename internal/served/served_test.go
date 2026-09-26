package served

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"testing"

	corev1 "k8s.io/api/core/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A kind of which no version is served reads as NotFound and as a no-match
// error, the two answers callers already take for "not installed", and
// IsNotServed tells it apart from a missing object.
func TestANotServedKindIsNotFoundAndNoMatch(t *testing.T) {
	gk := schema.GroupKind{Group: "postgresql.cnpg.io", Kind: "Cluster"}
	mapper := meta.NewDefaultRESTMapper(nil)
	_, err := Kind(mapper, gk)
	wrapped := fmt.Errorf("get Cluster: %w", err)
	if !apierrors.IsNotFound(wrapped) || !meta.IsNoMatchError(wrapped) || !IsNotServed(wrapped) {
		t.Errorf("Kind error %v: NotFound %t, no-match %t, not served %t; want all three",
			err, apierrors.IsNotFound(wrapped), meta.IsNoMatchError(wrapped), IsNotServed(wrapped))
	}
	if Transient(err) {
		t.Error("a kind that is not served is transient, want it final")
	}
	if IsNotServed(apierrors.NewNotFound(schema.GroupResource{Group: gk.Group, Resource: "clusters"}, "pg")) {
		t.Error("a missing object reads as a kind that is not served")
	}
}

// A 404 for a version the API server no longer serves becomes a
// *VersionGoneError that is transient and never NotFound; a NotFound for a
// missing object is returned as it is.
func TestOnlyAPlainTextNotFoundIsAVersionGone(t *testing.T) {
	gvk := schema.GroupVersionKind{Group: "barmancloud.cnpg.io", Version: "v1", Kind: "ObjectStore"}
	mapper := meta.NewDefaultRESTMapper(nil)
	gr := schema.GroupResource{Group: gvk.Group, Resource: "objectstores"}
	gone := VersionGone(mapper, gvk, apierrors.NewGenericServerResponse(http.StatusNotFound, "get", gr, "store", "404 page not found", 0, true))
	var versionGone *VersionGoneError
	if !errors.As(gone, &versionGone) || apierrors.IsNotFound(gone) || !Transient(gone) {
		t.Errorf("VersionGone = %v, want a transient *VersionGoneError that is not NotFound", gone)
	}
	missing := apierrors.NewNotFound(gr, "store")
	if got := VersionGone(mapper, gvk, missing); !errors.Is(got, missing) || !apierrors.IsNotFound(got) {
		t.Errorf("VersionGone of a missing object = %v, want it unchanged", got)
	}
}

// Client turns the plain-text 404 of a version no longer served into a
// *VersionGoneError on a typed object, whose version comes from the scheme,
// for reads, lists, writes and status writes, and leaves a missing object's
// NotFound alone.
func TestTheClientNeverReadsAVersionGoneAsAMissingObject(t *testing.T) {
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	pageNotFound := func(name string) error {
		return apierrors.NewGenericServerResponse(http.StatusNotFound, "get", schema.GroupResource{Resource: "configmaps"}, name, "404 page not found", 0, true)
	}
	inner := fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if key.Name == "gone" {
				return pageNotFound(key.Name)
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return pageNotFound("")
		},
		SubResourceUpdate: func(_ context.Context, _ client.Client, _ string, obj client.Object, _ ...client.SubResourceUpdateOption) error {
			return pageNotFound(obj.GetName())
		},
	}).Build()
	c := Client(inner)
	ctx := context.Background()
	var versionGone *VersionGoneError

	err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "gone"}, &corev1.ConfigMap{})
	if !errors.As(err, &versionGone) || apierrors.IsNotFound(err) || versionGone.GVK != corev1.SchemeGroupVersion.WithKind("ConfigMap") {
		t.Errorf("Get = %v, want a *VersionGoneError for v1 ConfigMap", err)
	}
	if err := Reader(inner, c).List(ctx, &corev1.ConfigMapList{}); !errors.As(err, &versionGone) || versionGone.GVK.Kind != "ConfigMap" {
		t.Errorf("List = %v, want a *VersionGoneError for ConfigMap", err)
	}
	if err := c.Status().Update(ctx, &corev1.ConfigMap{}); !errors.As(err, &versionGone) {
		t.Errorf("Status().Update = %v, want a *VersionGoneError", err)
	}
	if err := c.Get(ctx, client.ObjectKey{Namespace: "ns", Name: "missing"}, &corev1.ConfigMap{}); !apierrors.IsNotFound(err) {
		t.Errorf("Get of a missing object = %v, want NotFound", err)
	}
	if Client(c) != c {
		t.Error("Client wrapped an already wrapped client again")
	}
}
