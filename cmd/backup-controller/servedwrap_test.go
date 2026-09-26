package main

import (
	"context"
	"net/http"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// versionGoneClient returns a fake client that answers every read of a
// ReplicationDestination with the plain-text 404 kube-apiserver gives for a
// version it no longer serves, as after a VolSync release that drops
// v1alpha1 while the client's mapper still caches it. client-go turns that
// answer into a NotFound, the same as for a destination that is gone.
func versionGoneClient(t *testing.T) client.WithWatch {
	t.Helper()
	s := runtime.NewScheme()
	if err := volsyncv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	gone := func(key client.ObjectKey) error {
		return apierrors.NewGenericServerResponse(http.StatusNotFound, "get",
			schema.GroupResource{Group: volsyncv1alpha1.GroupVersion.Group, Resource: "replicationdestinations"}, key.Name, "404 page not found", 0, true)
	}
	return fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*volsyncv1alpha1.ReplicationDestination); ok {
				return gone(key)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()
}

// checkNotMissing fails the test unless err is an error that does not read
// as a missing object.
func checkNotMissing(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || apierrors.IsNotFound(err) {
		t.Errorf("%s = %v, want an error that is not NotFound", what, err)
	}
}

// The populator reads a ReplicationDestination at a version the API server
// no longer serves as an error it retries, never as a destination that is
// gone.
func TestThePopulatorNeverTakesAVersionGoneForAMissingDestination(t *testing.T) {
	_, err := operationsFor(versionGoneClient(t)).GetReplicationDestination(context.Background(), "backup-system", "restore-9b7d4e21-0")
	checkNotMissing(t, "GetReplicationDestination", err)
}

// The orphan reconciler reads it the same way through both its client and
// its uncached reader.
func TestTheOrphanReconcilerNeverTakesAVersionGoneForAMissingDestination(t *testing.T) {
	c := versionGoneClient(t)
	orphans := orphanReconciler(c, c, nil, "backup-system")
	key := types.NamespacedName{Namespace: "backup-system", Name: "restore-9b7d4e21-0"}

	checkNotMissing(t, "the client's Get", orphans.Client.Get(context.Background(), key, &volsyncv1alpha1.ReplicationDestination{}))
	checkNotMissing(t, "the reader's Get", orphans.Reader.Get(context.Background(), key, &volsyncv1alpha1.ReplicationDestination{}))
}
