// Package served looks up the version at which the API server serves a kind
// of another project, and tells a request at a version the API server has
// stopped serving apart from a request for an object that doesn't exist.
//
// The controller reads and writes the kinds of CloudNativePG, the barman-cloud
// plugin, Flux and Kueue as unstructured objects, by group and kind. A
// release of one of those projects that adds a version, or moves its kinds to
// a new one, keeps the controller working as long as the fields it uses stay
// the same. The internal/runs and internal/bootstrap packages share this
// package so that the RestoreRun controller and the bootstrap webhook read an
// ObjectStore the same way.
//
// Every lookup goes through the RESTMapper the caller passes. The manager's
// mapper (controller-runtime's lazy mapper) caches what it has looked up, so a
// lookup costs a discovery call only the first time a group is seen, and again
// after VersionGone has made it forget the group.
package served

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// Kind returns the group, version and kind at which the API server serves
// the kind gk, as the RESTMapper looks it up by group and kind with no
// version.
//
// Parameters:
//   - mapper is the client's RESTMapper, from client.Client.RESTMapper. The
//     manager's mapper asks the API server's discovery when it has not seen
//     the group yet, and answers from its cache after that.
//   - gk is the kind to look up.
//
// It returns a *NotServedError when the API server serves no version of the
// kind. That means the kind's CRD is not installed, and deleting a CRD
// deletes its objects, so no object of the kind exists: apierrors.IsNotFound
// and meta.IsNoMatchError are both true for it, and callers treat it like an
// object that is gone. A discovery call that failed, even one that failed for
// only some versions of the group, returns a *LookupError, which the caller
// retries with nothing skipped.
func Kind(mapper meta.RESTMapper, gk schema.GroupKind) (schema.GroupVersionKind, error) {
	mapping, err := mapper.RESTMapping(gk)
	if err == nil {
		return mapping.GroupVersionKind, nil
	}
	var partial *apiutil.ErrResourceDiscoveryFailed
	if meta.IsNoMatchError(err) && !errors.As(err, &partial) {
		return schema.GroupVersionKind{}, &NotServedError{GroupKind: gk}
	}
	return schema.GroupVersionKind{}, &LookupError{GroupKind: gk, Err: err}
}

// LookupError is a lookup of the served version of a kind that failed, such
// as a discovery call the API server did not answer. A retry may succeed,
// and nothing may be skipped on it.
type LookupError struct {
	// GroupKind is the kind that was looked up.
	GroupKind schema.GroupKind

	// Err is the RESTMapper's error.
	Err error
}

// Error names the kind and the RESTMapper's error.
func (e *LookupError) Error() string {
	return fmt.Sprintf("look up the served version of %s: %v", e.GroupKind, e.Err)
}

// Unwrap returns the RESTMapper's error.
func (e *LookupError) Unwrap() error { return e.Err }

// Transient reports whether err, or an error it wraps, is one of this
// package's errors that a retry may clear: a *LookupError or a
// *VersionGoneError. A caller that sorts errors into "retry" and "give up"
// checks this first, since a *LookupError can wrap a no-match error and a
// *VersionGoneError stands for a NotFound.
func Transient(err error) bool {
	var lookup *LookupError
	var gone *VersionGoneError
	return errors.As(err, &lookup) || errors.As(err, &gone)
}

// NotServedError is a kind of which the API server serves no version. It
// counts as NotFound (apierrors.IsNotFound) and as a no-match error
// (meta.IsNoMatchError), the two answers a caller already treats as "the
// project is not installed".
type NotServedError struct {
	// GroupKind is the kind that was looked up.
	GroupKind schema.GroupKind
}

// Error says that no version of the kind is served.
func (e *NotServedError) Error() string {
	return fmt.Sprintf("the API server serves no version of %s", e.GroupKind)
}

// Status returns the NotFound Status that makes apierrors.IsNotFound true.
func (e *NotServedError) Status() metav1.Status {
	return metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    http.StatusNotFound,
		Reason:  metav1.StatusReasonNotFound,
		Message: e.Error(),
	}
}

// Is makes errors.Is, and with it meta.IsNoMatchError, match a
// *meta.NoKindMatchError, the error the RESTMapper returns for the same case.
func (e *NotServedError) Is(target error) bool {
	_, ok := target.(*meta.NoKindMatchError)
	return ok
}

// Versions returns the versions at which the API server serves the kind gk,
// as the RESTMapper knows them, sorted, for an error message that says what
// is served instead of the version the controller needs. It returns nil when
// no version is served or the lookup fails.
func Versions(mapper meta.RESTMapper, gk schema.GroupKind) []string {
	mappings, err := mapper.RESTMappings(gk)
	if err != nil {
		return nil
	}
	var versions []string
	for _, m := range mappings {
		versions = append(versions, m.GroupVersionKind.GroupVersion().String())
	}
	sort.Strings(versions)
	return versions
}

// VersionGone tells a request the API server refused because it no longer
// serves the version the request was sent at apart from a request for an
// object that doesn't exist. Both come back as NotFound, and only the second
// means the object is gone.
//
// Parameters:
//   - mapper is the RESTMapper the version came from. The manager's mapper
//     caches every version it has looked up and keeps serving it after an
//     upgrade of the kind's project stops serving it.
//   - gvk is the group, version and kind the request was sent at.
//   - err is the request's error.
//
// kube-apiserver answers a request for an object that doesn't exist with a
// NotFound Status of its own, and a request at a version it doesn't serve
// with a plain-text "404 page not found" from its not-found handler.
// client-go turns the text answer into a NotFound whose details carry an
// UnexpectedServerResponse cause, which apierrors.IsUnexpectedServerError
// reports. This was checked against Kubernetes 1.36.4, and
// TestEnvtestAVersionNoLongerServedIsNotAMissingObject in internal/runs checks
// it against the kube-apiserver versions.json pins on every envtest run. For
// that error VersionGone makes mapper look the kind's versions up again (see
// Rediscover) and returns a *VersionGoneError, for which apierrors.IsNotFound
// is false, so the caller retries with nothing skipped. It returns any other
// error, and nil, as it is.
func VersionGone(mapper meta.RESTMapper, gvk schema.GroupVersionKind, err error) error {
	if !apierrors.IsNotFound(err) || !apierrors.IsUnexpectedServerError(err) {
		return err
	}
	Rediscover(mapper, gvk.Group)
	return &VersionGoneError{GVK: gvk, Err: err}
}

// VersionGoneError is a request the API server refused because it no longer
// serves the version the request was sent at. It deliberately has no Unwrap
// method, so that apierrors.IsNotFound doesn't take it for an object that is
// gone.
type VersionGoneError struct {
	// GVK is the group, version and kind the request was sent at.
	GVK schema.GroupVersionKind

	// Err is the NotFound client-go returned for it.
	Err error
}

// Error names the version and says that the controller looks it up again.
func (e *VersionGoneError) Error() string {
	return fmt.Sprintf("the API server no longer serves %s at %s (%v); the controller looks up the served version again and retries, "+
		"and a restart of the controller also clears the versions it has cached", e.GVK.Kind, e.GVK.GroupVersion(), e.Err)
}

// noSuchKind is a kind name no API group serves. Rediscover looks it up to
// make controller-runtime's mapper read a group's discovery again.
const noSuchKind = "BackupControllerNoSuchKind"

// Rediscover makes mapper forget the versions of group it has cached, so its
// next lookup asks the API server's discovery again.
//
// A mapper that implements meta.ResettableRESTMapper is reset. The manager's
// mapper from controller-runtime v0.24 has no Reset. For it, Rediscover looks
// up a kind the group doesn't have: the mapper answers a kind it can't find
// by reading the discovery of every version of the group it has cached, and
// drops the group from its cache when one of those versions answers 404
// (controller-runtime pkg/client/apiutil/restmapper.go,
// fetchGroupVersionResourcesLocked). The lookup's own error is ignored; a
// failed discovery call leaves the cache as it was, and the caller's retry
// comes back here.
func Rediscover(mapper meta.RESTMapper, group string) {
	if resettable, ok := mapper.(meta.ResettableRESTMapper); ok {
		resettable.Reset()
		return
	}
	_, _ = mapper.RESTMapping(schema.GroupKind{Group: group, Kind: noSuchKind})
}

// Get reads one object of the kind gk at the version the API server serves.
//
// Parameters:
//   - reader reads the object. The caller picks the cached client or the
//     uncached API reader.
//   - mapper looks up the served version (see Kind).
//   - gk is the kind.
//   - key names the object.
//
// It returns an error for which apierrors.IsNotFound is true when the object
// doesn't exist or no version of the kind is served; a *NotServedError tells
// the second apart. A read at a version the API server has stopped serving
// since mapper cached it returns a *VersionGoneError (see VersionGone). Any
// other failed lookup or read is returned as it is.
func Get(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, gk schema.GroupKind, key types.NamespacedName) (*unstructured.Unstructured, error) {
	gvk, err := Kind(mapper, gk)
	if err != nil {
		return nil, err
	}
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(gvk)
	if err := reader.Get(ctx, key, object); err != nil {
		return nil, VersionGone(mapper, gvk, err)
	}
	return object, nil
}

// List lists the objects of the kind gk at the version the API server
// serves.
//
// Parameters:
//   - reader lists the objects.
//   - mapper looks up the served version (see Kind).
//   - gk is the kind of the items, such as Cluster; List asks for its list
//     kind, ClusterList.
//   - opts are the list options, such as client.InNamespace.
//
// It returns a *NotServedError when no version of the kind is served, and a
// *VersionGoneError when the API server has stopped serving the version
// mapper cached (see VersionGone). A caller that treats "not served" as "no
// objects" checks for the first alone: the second never means that. Any
// other failed lookup or list is returned as it is.
func List(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, gk schema.GroupKind, opts ...client.ListOption) (*unstructured.UnstructuredList, error) {
	gvk, err := Kind(mapper, gk)
	if err != nil {
		return nil, err
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err := reader.List(ctx, list, opts...); err != nil {
		return nil, VersionGone(mapper, gvk, err)
	}
	return list, nil
}

// IsNotServed reports whether err, or an error it wraps, is a
// *NotServedError: the API server serves no version of the kind.
func IsNotServed(err error) bool {
	var notServed *NotServedError
	return errors.As(err, &notServed)
}

// Describe returns "at v1, v2" for the versions the API server serves the
// kind gk at, or "at no version" when it serves none, for an error message.
func Describe(mapper meta.RESTMapper, gk schema.GroupKind) string {
	versions := Versions(mapper, gk)
	if len(versions) == 0 {
		return "at no version"
	}
	return "at " + strings.Join(versions, ", ")
}
