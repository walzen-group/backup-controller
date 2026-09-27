package populator

import (
	"context"
	"fmt"

	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Jobs is what the populator needs of the Kubernetes API to create, resume,
// read and stop the restore Jobs of its claims. Every read must come fresh
// from the API server, since the stop gate decides from what it reads that no
// pod can still write.
type Jobs interface {
	restorejob.API
	// ListClaimPods lists the pods of the given namespace whose
	// backup.wlz.li/restore-claim label is the given claim UID: the pods of
	// every restore Job that ever filled that claim, also after their Job is
	// gone.
	ListClaimPods(ctx context.Context, namespace string, claimUID types.UID) ([]corev1.Pod, error)
}

// clientJobs is Jobs over controller-runtime clients.
type clientJobs struct {
	restorejob.API
	reader client.Reader
}

// NewJobs returns Jobs over controller-runtime clients.
//
// Parameters:
//   - reader serves every read. The caller passes a reader that goes to the
//     API server directly, such as the manager's APIReader or a client built
//     with client.New, and never a cached client: the stop gate lets the app
//     claim have its volume once every pod it reads has ended, and a cache
//     that lags behind would show a pod as ended, or miss a new one, while
//     it still writes.
//   - writer serves the create, the suspend, the resume and the delete.
func NewJobs(reader client.Reader, writer client.Client) Jobs {
	return &clientJobs{API: restorejob.NewAPI(reader, writer), reader: reader}
}

// ListClaimPods lists a claim's restore pods through the reader, by the
// label restorejob.Build puts on every pod of a Job that fills a claim.
func (j *clientJobs) ListClaimPods(ctx context.Context, namespace string, claimUID types.UID) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := j.reader.List(ctx, pods, client.InNamespace(namespace),
		client.MatchingLabels{restorejob.LabelRestoreClaim: string(claimUID)}); err != nil {
		return nil, err
	}
	return pods.Items, nil
}

// stoppingError is a claim whose earlier restore Job is not stopped yet, so
// nothing else may happen to the claim or its prime. It carries what the stop
// waits for, for the message.
type stoppingError struct {
	// state is what restorejob.Stop last reported.
	state restorejob.StopState
}

// Error renders what the stop waits for.
func (e *stoppingError) Error() string {
	return e.state.String()
}

// stopRestore stops every restore Job that ever filled a claim, and reports
// whether one of them may still write.
//
// Parameters:
//   - ctx bounds the API calls.
//   - jobs makes the calls, fresh from the API server.
//   - namespace is the controller namespace, where the Jobs run.
//   - claimUID is the UID of the app claim. It names the claim's Job
//     (JobName) and selects its pods by the backup.wlz.li/restore-claim
//     label.
//
// It returns a StopState whose Stopped field is true once no Job of the claim
// can start a pod and no pod of one may still write, and an error from any
// API call or from restorejob.Stop. A caller that gets Stopped false waits and
// calls again.
//
// The populator keeps no record of its Jobs that outlives the claim's prime,
// so it stops by what it can always find. It reads the Job of the claim by
// name and stops it with its own UID, so a Job that has no pod yet starts
// none later. It then lists the claim's pods by label and passes each
// distinct Job UID they carry through restorejob.Stop's gate. That finds the
// pods of a Job a person deleted, with any propagation, and of every earlier
// Job of the claim, since each pod keeps both labels until it is gone.
// restorejob.Build sets the claim label after the settings' pod labels, so no
// setting can take it off.
func stopRestore(ctx context.Context, jobs Jobs, namespace string, claimUID types.UID) (restorejob.StopState, error) {
	name := JobName(claimUID)
	var refs []restorejob.Ref
	job, err := jobs.GetJob(ctx, types.NamespacedName{Namespace: namespace, Name: name})
	switch {
	case apierrors.IsNotFound(err):
	case err != nil:
		return restorejob.StopState{Job: name}, fmt.Errorf("read restore Job %s/%s: %w", namespace, name, err)
	default:
		refs = append(refs, restorejob.RefOf(job))
	}
	pods, err := jobs.ListClaimPods(ctx, namespace, claimUID)
	if err != nil {
		return restorejob.StopState{Job: name}, fmt.Errorf("list the restore pods of claim %s: %w", claimUID, err)
	}
	refs = appendPodJobs(refs, pods, namespace, name)
	for _, ref := range refs {
		state, err := restorejob.Stop(ctx, jobs, ref)
		if err != nil || !state.Stopped {
			return state, err
		}
	}
	return restorejob.StopState{Stopped: true, Job: name}, nil
}

// appendPodJobs adds a Ref for each Job UID the pods carry that refs lacks.
//
// Parameters:
//   - refs are the Jobs already found.
//   - pods are the claim's restore pods.
//   - namespace and name are the claim Job's namespace and name, which every
//     Job of the claim had.
//
// A pod without the batch.kubernetes.io/controller-uid label gives a Ref
// with no UID, which restorejob.Stop refuses with an error, so such a pod
// holds the stop.
func appendPodJobs(refs []restorejob.Ref, pods []corev1.Pod, namespace, name string) []restorejob.Ref {
	seen := make(map[types.UID]bool, len(refs))
	for _, ref := range refs {
		seen[ref.UID] = true
	}
	for i := range pods {
		uid := types.UID(pods[i].Labels[batchv1.ControllerUidLabel])
		if seen[uid] {
			continue
		}
		seen[uid] = true
		refs = append(refs, restorejob.Ref{Namespace: namespace, Name: name, UID: uid})
	}
	return refs
}
