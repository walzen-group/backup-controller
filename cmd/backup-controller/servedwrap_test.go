package main

import (
	"context"
	"net/http"
	"testing"

	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// versionGoneClient returns a fake client that answers every read of a Job
// with the plain-text 404 kube-apiserver gives for a version it no longer
// serves, as after an upgrade that drops batch/v1 while the client's mapper
// still caches it. client-go turns that answer into a NotFound, the same as
// for a Job that is gone.
func versionGoneClient(t *testing.T) client.WithWatch {
	t.Helper()
	s := runtime.NewScheme()
	if err := batchv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	gone := func(key client.ObjectKey) error {
		return apierrors.NewGenericServerResponse(http.StatusNotFound, "get",
			schema.GroupResource{Group: batchv1.GroupName, Resource: "jobs"}, key.Name, "404 page not found", 0, true)
	}
	return fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
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

// The populator reads a restore Job at a version the API server no longer
// serves as an error it retries, never as a Job that is gone, which would
// let it create a second Job or bind the claim.
func TestThePopulatorNeverTakesAVersionGoneForAMissingJob(t *testing.T) {
	_, err := operationsFor(versionGoneClient(t)).GetJob(context.Background(), types.NamespacedName{Namespace: "backup-system", Name: "restore-9b7d4e21"})
	checkNotMissing(t, "GetJob", err)
}

// The orphan reconciler reads it the same way through both its client and
// its uncached reader.
func TestTheOrphanReconcilerNeverTakesAVersionGoneForAMissingJob(t *testing.T) {
	c := versionGoneClient(t)
	orphans := orphanReconciler(c, c, nil, "backup-system")
	key := types.NamespacedName{Namespace: "backup-system", Name: "restore-9b7d4e21"}

	checkNotMissing(t, "the client's Get", orphans.Client.Get(context.Background(), key, &batchv1.Job{}))
	checkNotMissing(t, "the reader's Get", orphans.Reader.Get(context.Background(), key, &batchv1.Job{}))
}
