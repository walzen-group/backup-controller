package main

import (
	"context"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// clientOperations implements populator.Operations with a real Kubernetes
// client. The populator callbacks make their writes through that interface.
// Each method is a thin call on the client and returns the client's error
// unchanged.
type clientOperations struct {
	client client.Client
}

// GetJob reads one Job.
//
// Parameters:
//   - namespace and name identify the Job.
//
// It returns the Job, or the client's error, which is NotFound when there is
// no such Job.
func (o *clientOperations) GetJob(ctx context.Context, namespace, name string) (*batchv1.Job, error) {
	job := new(batchv1.Job)
	if err := o.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, job); err != nil {
		return nil, err
	}
	return job, nil
}

// CreateJob creates one Job.
//
// Parameters:
//   - job is the Job to create, with its namespace and name set.
//
// It returns the client's error, which is AlreadyExists when a Job of that
// name exists.
func (o *clientOperations) CreateJob(ctx context.Context, job *batchv1.Job) error {
	return o.client.Create(ctx, job)
}

// DeleteJob deletes one Job and, in the background, its pods.
//
// Parameters:
//   - namespace and name identify the Job.
//
// It returns the client's error, which is NotFound when there is no such
// Job. Without background propagation, the API server would leave the
// Job's pods behind.
func (o *clientOperations) DeleteJob(ctx context.Context, namespace, name string) error {
	return o.client.Delete(ctx, &batchv1.Job{ObjectMeta: objectMeta(namespace, name)},
		client.PropagationPolicy(metav1.DeletePropagationBackground))
}

// GetSecret reads one Secret.
//
// Parameters:
//   - namespace and name identify the Secret.
//
// It returns the Secret, or the client's error, which is NotFound when there
// is no such Secret.
func (o *clientOperations) GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error) {
	secret := new(corev1.Secret)
	if err := o.client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
		return nil, err
	}
	return secret, nil
}

// CreateSecret creates one Secret.
//
// Parameters:
//   - secret is the Secret to create, with its namespace and name set.
//
// It returns the client's error, which is AlreadyExists when a Secret of
// that name exists.
func (o *clientOperations) CreateSecret(ctx context.Context, secret *corev1.Secret) error {
	return o.client.Create(ctx, secret)
}

// DeleteSecret deletes one Secret.
//
// Parameters:
//   - namespace and name identify the Secret.
//
// It returns the client's error, which is NotFound when there is no such
// Secret.
func (o *clientOperations) DeleteSecret(ctx context.Context, namespace, name string) error {
	return o.client.Delete(ctx, &corev1.Secret{ObjectMeta: objectMeta(namespace, name)})
}

// SetStatus writes the status subresource of one VolumeRestore.
//
// Parameters:
//   - vr is the VolumeRestore with the new status. The callbacks decode it
//     from the object the library cached, so it has the resource version
//     that the update needs. An update from an old copy fails with Conflict.
//
// It returns the client's error.
func (o *clientOperations) SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	return o.client.Status().Update(ctx, vr)
}

// objectMeta returns object metadata with only a namespace and a name, which
// is all that a delete needs.
//
// Parameters:
//   - namespace and name identify the object.
func objectMeta(namespace, name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{Namespace: namespace, Name: name}
}
