package restorejob

import (
	"context"
	"encoding/json"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// API is what the package needs of the Kubernetes API to create, read and
// stop a restore Job. Every read must be fresh from the API server: the stop
// gate decides that no pod can still write from what it reads.
type API interface {
	// GetJob reads the Job of the given namespace and name.
	GetJob(ctx context.Context, key types.NamespacedName) (*batchv1.Job, error)
	// CreateJob creates the Job.
	CreateJob(ctx context.Context, job *batchv1.Job) error
	// SuspendJob sets spec.suspend on the Job, and only on the Job with the
	// given one's UID.
	SuspendJob(ctx context.Context, job *batchv1.Job) error
	// DeleteJob deletes the Job with Foreground propagation, and only the
	// Job with the given one's UID.
	DeleteJob(ctx context.Context, job *batchv1.Job) error
	// ListJobPods lists the pods whose batch.kubernetes.io/controller-uid
	// label is the Job's UID.
	ListJobPods(ctx context.Context, job *batchv1.Job) ([]corev1.Pod, error)
}

// clientAPI is the API over controller-runtime clients.
type clientAPI struct {
	reader client.Reader
	writer client.Client
}

// NewAPI returns the API over controller-runtime clients.
//
// Parameters:
//   - reader serves every read. The caller passes the manager's
//     APIReader, which reads the API server directly. It must never be the
//     cached client: the stop gate lets the app have its volume back once
//     every pod it reads has ended, and a cache that lags behind would show
//     a pod as ended, or miss a new one, while it still writes.
//   - writer serves the create, the suspend and the delete. The caller
//     passes the manager's client.
func NewAPI(reader client.Reader, writer client.Client) API {
	return &clientAPI{reader: reader, writer: writer}
}

// GetJob reads the Job through the reader.
func (a *clientAPI) GetJob(ctx context.Context, key types.NamespacedName) (*batchv1.Job, error) {
	job := &batchv1.Job{}
	if err := a.reader.Get(ctx, key, job); err != nil {
		return nil, err
	}
	return job, nil
}

// CreateJob creates the Job through the writer.
func (a *clientAPI) CreateJob(ctx context.Context, job *batchv1.Job) error {
	return a.writer.Create(ctx, job)
}

// SuspendJob sends a merge patch that sets spec.suspend and carries the
// Job's UID. The API server refuses a patch that would change a stored
// object's UID, so a Job created under the same name after the given one
// was read is left alone.
func (a *clientAPI) SuspendJob(ctx context.Context, job *batchv1.Job) error {
	patch, err := json.Marshal(map[string]any{
		"metadata": map[string]any{"uid": job.UID},
		"spec":     map[string]any{"suspend": true},
	})
	if err != nil {
		return fmt.Errorf("build the suspend patch: %w", err)
	}
	return a.writer.Patch(ctx, job, client.RawPatch(types.MergePatchType, patch))
}

// DeleteJob deletes the Job with Foreground propagation and a UID
// precondition. The Job then stays, with the foregroundDeletion finalizer,
// until the garbage collector has deleted its pods.
func (a *clientAPI) DeleteJob(ctx context.Context, job *batchv1.Job) error {
	uid := job.UID
	return a.writer.Delete(ctx, job,
		client.PropagationPolicy(metav1.DeletePropagationForeground),
		client.Preconditions{UID: &uid})
}

// ListJobPods lists the Job's pods through the reader, by the label the Job
// controller puts on every pod it creates.
func (a *clientAPI) ListJobPods(ctx context.Context, job *batchv1.Job) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := a.reader.List(ctx, pods, client.InNamespace(job.Namespace),
		client.MatchingLabels{batchv1.ControllerUidLabel: string(job.UID)}); err != nil {
		return nil, err
	}
	return pods.Items, nil
}
