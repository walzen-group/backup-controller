package main

import (
	"context"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clientOperations implements populator.Operations with a real Kubernetes
// client. The populator callbacks reach the API server only through that
// interface, which lets their tests run against a fake. Each method is a thin
// call on the client and returns the client's error unchanged.
type clientOperations struct {
	client client.Client
}

// GetReplicationDestination reads the named ReplicationDestination.
func (o *clientOperations) GetReplicationDestination(ctx context.Context, namespace, name string) (*volsyncv1alpha1.ReplicationDestination, error) {
	destination := new(volsyncv1alpha1.ReplicationDestination)
	if err := o.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, destination); err != nil {
		return nil, err
	}
	return destination, nil
}

// CreateReplicationDestination creates the ReplicationDestination it is given.
func (o *clientOperations) CreateReplicationDestination(ctx context.Context, rd *volsyncv1alpha1.ReplicationDestination) error {
	return o.client.Create(ctx, rd)
}

// DeleteReplicationDestination deletes the named ReplicationDestination.
func (o *clientOperations) DeleteReplicationDestination(ctx context.Context, namespace, name string) error {
	return o.client.Delete(ctx, &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: objectMeta(namespace, name),
	})
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
	return o.client.Delete(ctx, &corev1.Secret{ObjectMeta: objectMeta(namespace, name)})
}

// SetStatus writes the VolumeRestore's status subresource. The callbacks pass
// an object decoded from the data source the library cached, so it already
// carries the resource version the update needs.
func (o *clientOperations) SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	return o.client.Status().Update(ctx, vr)
}

// objectMeta returns object metadata holding only a namespace and a name,
// which is all a delete needs.
func objectMeta(namespace, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace, Name: name}
}
