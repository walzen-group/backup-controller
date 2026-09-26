package populator

import (
	"context"
	"errors"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// clientOperations sends the callbacks' API calls to one client, as
// cmd/backup-controller's clientOperations sends them to the API server.
type clientOperations struct{ client client.Client }

func (o clientOperations) GetReplicationDestination(ctx context.Context, namespace, name string) (*volsyncv1alpha1.ReplicationDestination, error) {
	rd := &volsyncv1alpha1.ReplicationDestination{}
	return rd, o.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, rd)
}

func (o clientOperations) CreateReplicationDestination(ctx context.Context, rd *volsyncv1alpha1.ReplicationDestination) error {
	return o.client.Create(ctx, rd)
}

func (o clientOperations) DeleteReplicationDestination(ctx context.Context, namespace, name string) error {
	return o.client.Delete(ctx, &volsyncv1alpha1.ReplicationDestination{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
}

func (o clientOperations) GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	secret := &corev1.Secret{}
	return secret, o.client.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, secret)
}

func (o clientOperations) CreateSecret(ctx context.Context, secret *corev1.Secret) error {
	return o.client.Create(ctx, secret)
}

func (o clientOperations) DeleteSecret(ctx context.Context, namespace, name string) error {
	return o.client.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}})
}

func (o clientOperations) SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	return o.client.Status().Update(ctx, vr)
}

func (o clientOperations) UpdateVolumeRestore(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	return o.client.Update(ctx, vr)
}

// Window 2 of designs/populator.md finding A: Cleanup releases the
// VolumeRestore's finalizer before the library deletes the prime and removes
// its claim finalizer. For a VolumeRestore already being deleted, it goes the
// moment the finalizer does. When the library's prime delete then fails, its
// next sync finds no data source and returns, so the claim would stay
// Terminating for good. OrphanReconciler completes the cleanup instead.
func TestAFailedPrimeDeleteAfterCleanupIsCompletedByTheOrphanReconciler(t *testing.T) {
	deleting := metav1.NewTime(time.Date(2026, 9, 26, 6, 30, 0, 0, time.UTC))
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name: orphanVolumeRestore, Namespace: orphanNamespace, Generation: 1,
			DeletionTimestamp: &deleting, Finalizers: []string{Finalizer},
		},
		Spec: backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"},
		Status: backupv1alpha1.VolumeRestoreStatus{Claims: []backupv1alpha1.ClaimRestoreStatus{
			{Name: "data", UID: orphanClaimUID, Phase: backupv1alpha1.RestorePhaseRestoring},
		}},
	}
	c := newOrphanClient(t, append(leftovers(), stuckClaim(), vr)...)
	ctx := context.Background()

	// The library hands Cleanup the VolumeRestore from its cache.
	stored := &backupv1alpha1.VolumeRestore{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(vr), stored); err != nil {
		t.Fatalf("get VolumeRestore: %v", err)
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(stored)
	if err != nil {
		t.Fatal(err)
	}
	params := populatormachinery.PopulatorParams{Pvc: stuckClaim(), Unstructured: &unstructured.Unstructured{Object: object}}
	if err := New(clientOperations{client: c}, orphanControllerNS, nil).Cleanup(ctx, params); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if present(t, c, &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: orphanVolumeRestore, Namespace: orphanNamespace}}) {
		t.Fatal("the VolumeRestore is still there after Cleanup released its finalizer; the window needs it gone")
	}

	// The library then deletes the prime (lib controller.go:998-1001), and
	// that delete fails, so the claim keeps ClaimFinalizer.
	lost := errors.New("the API server is unavailable")
	failing := interceptor.NewClient(c, interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && obj.GetName() == PrimeClaimName(orphanClaimUID) {
				return lost
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
	prime := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: PrimeClaimName(orphanClaimUID), Namespace: orphanControllerNS}}
	if err := failing.Delete(ctx, prime); !errors.Is(err, lost) {
		t.Fatalf("library's prime delete: got %v, want %v", err, lost)
	}
	if got := claimFinalizers(t, c); !containsString(got, ClaimFinalizer) {
		t.Fatalf("claim finalizers after the failed prime delete = %v, want %s still there", got, ClaimFinalizer)
	}

	r, recorder := newOrphanReconciler(c, c)
	if _, err := reconcileClaim(t, r); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if present(t, c, prime) {
		t.Error("the prime is still there after the orphan reconciler ran")
	}
	if got := claimFinalizers(t, c); containsString(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s removed", got, ClaimFinalizer)
	}
	events := drain(recorder)
	if len(events) != 1 || !containsPrefix(events[0], "Warning DataSourceGone") {
		t.Errorf("events = %q, want one Warning DataSourceGone", events)
	}
	if !apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(vr), &backupv1alpha1.VolumeRestore{})) {
		t.Error("the VolumeRestore came back")
	}
}

func containsString(list []string, s string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

func containsPrefix(s, prefix string) bool {
	return len(s) >= len(prefix) && s[:len(prefix)] == prefix
}
