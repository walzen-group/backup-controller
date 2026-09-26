package served

import (
	"errors"
	"fmt"
	"net/http"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
	if got := VersionGone(mapper, gvk, missing); got != missing {
		t.Errorf("VersionGone of a missing object = %v, want it unchanged", got)
	}
}
