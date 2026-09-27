package main

import (
	"context"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/populator"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clientOperations implements populator.Operations with a real Kubernetes
// client. The populator callbacks reach the API server only through that
// interface, which lets their tests run against a fake. The client is built
// with client.New and has no cache, so every read goes to the API server,
// which the restore Job's stop gate needs. Each method is a thin call on the
// client and returns the client's error unchanged.
type clientOperations struct {
	populator.Jobs
	client client.Client
}

// newOperations returns the populator's operations over one uncached client.
//
// Parameters:
//   - c is the populator's client, built with client.New, which reads from
//     the API server directly. It serves the reads and the writes of the
//     restore Jobs too.
func newOperations(c client.Client) *clientOperations {
	return &clientOperations{Jobs: populator.NewJobs(c, c), client: c}
}

// GetNamespace reads the named namespace.
func (o *clientOperations) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	ns := new(corev1.Namespace)
	if err := o.client.Get(ctx, types.NamespacedName{Name: name}, ns); err != nil {
		return nil, err
	}
	return ns, nil
}

// GetClaim reads the named claim.
func (o *clientOperations) GetClaim(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	claim := new(corev1.PersistentVolumeClaim)
	if err := o.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, claim); err != nil {
		return nil, err
	}
	return claim, nil
}

// GetVolume reads the named PersistentVolume.
func (o *clientOperations) GetVolume(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	volume := new(corev1.PersistentVolume)
	if err := o.client.Get(ctx, types.NamespacedName{Name: name}, volume); err != nil {
		return nil, err
	}
	return volume, nil
}

// PatchClaim sends the patch to the claim.
func (o *clientOperations) PatchClaim(ctx context.Context, claim *corev1.PersistentVolumeClaim, patch client.Patch) error {
	return o.client.Patch(ctx, claim, patch)
}

// GetSecret reads the named Secret.
func (o *clientOperations) GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	secret := new(corev1.Secret)
	if err := o.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// CreateSecret creates the Secret it is given.
func (o *clientOperations) CreateSecret(ctx context.Context, secret *corev1.Secret) error {
	return o.client.Create(ctx, secret)
}

// DeleteSecret deletes the named Secret.
func (o *clientOperations) DeleteSecret(ctx context.Context, namespace, name string) error {
	return o.client.Delete(ctx, &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name}})
}

// SetStatus writes the VolumeRestore's status subresource. The callbacks pass
// an object decoded from the data source the library cached, so it already
// carries the resource version the update needs.
func (o *clientOperations) SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	return o.client.Status().Update(ctx, vr)
}

// UpdateVolumeRestore writes the VolumeRestore's metadata and spec, which is
// how the callbacks add and remove their finalizer. The object carries the
// resource version it was read at, so a stale write fails with a conflict.
func (o *clientOperations) UpdateVolumeRestore(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	return o.client.Update(ctx, vr)
}

// ListVolumeRestores lists the VolumeRestores of the namespace.
func (o *clientOperations) ListVolumeRestores(ctx context.Context, namespace string) ([]backupv1alpha1.VolumeRestore, error) {
	list := &backupv1alpha1.VolumeRestoreList{}
	if err := o.client.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	return list.Items, nil
}
