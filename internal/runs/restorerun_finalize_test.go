package runs

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A run deleted while it left its Cluster deleted reads the Cluster only to
// choose whether it records the ClusterLeftDeleted event. A read of the
// Cluster that fails, because CloudNativePG is no longer installed
// (NoKindMatch) or because the controller lost its RBAC for Clusters
// (Forbidden), does not keep the finalizer. The run skips the event, and
// its deletion completes. Before, the finalizer stayed and every retry
// failed the same way.
func TestAFailedClusterReadDoesNotBlockTheFinalizer(t *testing.T) {
	t.Parallel()
	for name, readErr := range map[string]error{
		"NoKindMatch": &meta.NoKindMatchError{GroupKind: cnpg.ClusterGVK.GroupKind(), SearchedVersions: []string{"v1"}},
		"Forbidden": apierrors.NewForbidden(schema.GroupResource{Group: cnpg.ClusterGVK.Group, Resource: "clusters"}, pgN,
			nil),
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, prober{saturday}, restoreRun(deletedDatabaseRun), objectStore(), storeSecret())
			recorder := events.NewFakeRecorder(10)
			r.Recorder = recorder
			r.Reader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == cnpg.ClusterGVK.Kind {
						return readErr
					}
					return cl.Get(ctx, key, obj, opts...)
				},
			})
			if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
				t.Fatal(err)
			}

			if err := tryRestoreStep(r); err != nil {
				t.Fatalf("the pass returned %v, want the finalizer removed without an error", err)
			}

			run := &backupv1alpha1.RestoreRun{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, run); !apierrors.IsNotFound(err) {
				t.Errorf("run finalizers = %v (get error %v), want the run gone", run.Finalizers, err)
			}
			for _, event := range recorded(recorder) {
				if strings.Contains(event, "ClusterLeftDeleted") {
					t.Errorf("event %q recorded, want none for a Cluster the run could not read", event)
				}
			}
		})
	}
}
