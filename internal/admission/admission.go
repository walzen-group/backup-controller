// Package admission lets work of the controller wait for Kueue before it
// starts. Each BackupRun, each RestoreRun and each restore the populator
// starts is one Kueue Workload that asks for one RunResource, so one
// ClusterQueue quota bounds how much of that work runs at once.
package admission

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"
)

// AddToScheme registers the Kueue kinds this package reads and writes. Every
// client that calls this package needs them in its scheme.
var AddToScheme = kueuev1beta2.AddToScheme

// admissionImage is the image named in the pod template of a Workload. Kueue
// reads the template to count the Workload against its quota, and it never
// runs the image.
const admissionImage = "registry.k8s.io/pause:3.10"

// RunResource is the resource each Workload asks for, one per unit of work. A
// ClusterQueue that covers it with a quota of 5 admits five units at once, so
// the quota bounds how many runs and populator restores work at the same
// time, and so the controller's own work and memory.
const RunResource corev1.ResourceName = "backup-controller.wlz.li/run"

// NoQueueError reports that Kueue can't admit work in a namespace, because
// the namespace has no LocalQueue or the cluster serves no Kueue. The work
// then waits and does nothing, so nothing escapes the ClusterQueue's quota.
type NoQueueError struct {
	message string
}

// Error returns the message that the waiting object shows on its Ready
// condition.
func (e NoQueueError) Error() string {
	return e.message
}

// LocalQueue returns the name of the LocalQueue that admits a namespace's
// work.
//
// Parameters:
//   - namespace is where the Workload goes: the run's namespace, or the
//     controller namespace for a populator restore.
//
// A namespace normally holds one LocalQueue. When it holds several, the one
// named backups is used, or else the first by name, so the choice stays the
// same from one pass to the next.
//
// It returns a NoQueueError when the namespace has no LocalQueue, or when the
// cluster serves no kueue.x-k8s.io/v1beta2 LocalQueue, and any other error
// when the list fails.
func LocalQueue(ctx context.Context, c client.Reader, namespace string) (string, error) {
	queues := &kueuev1beta2.LocalQueueList{}
	if err := c.List(ctx, queues, client.InNamespace(namespace)); err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return "", NoQueueError{message: fmt.Sprintf("the cluster serves no %s LocalQueue; the work waits until Kueue is installed", kueuev1beta2.SchemeGroupVersion)}
		}
		return "", fmt.Errorf("list the LocalQueues in %s: %w", namespace, err)
	}
	var names []string
	for _, q := range queues.Items {
		if q.Name == "backups" {
			return "backups", nil
		}
		names = append(names, q.Name)
	}
	if len(names) == 0 {
		return "", NoQueueError{message: fmt.Sprintf("namespace %s has no Kueue LocalQueue; the work waits until one exists", namespace)}
	}
	sort.Strings(names)
	return names[0], nil
}

// Name returns the name of a Workload: the lower-case kind of what waits for
// admission, a dash, and its UID, such as backuprun-<UID>. Because the name
// comes from the UID, a restarted controller finds the Workload it made.
//
// Parameters:
//   - kind is the kind of what waits, such as BackupRun, RestoreRun or
//     populator.
//   - uid is its UID: the run's, or the claim's that the populator fills.
func Name(kind string, uid types.UID) string {
	return strings.ToLower(kind) + "-" + string(uid)
}

// EnsureWorkload creates the Kueue Workload through which work waits for
// admission, and returns the Workload as it stands.
//
// Parameters:
//   - reader reads the Workload from the API server. A cached client would
//     start a cluster-wide informer on Workloads, which needs list and watch
//     and holds every Workload in memory.
//   - c creates the Workload.
//   - namespace and name place the Workload; name comes from Name.
//   - queue is the name of the LocalQueue to submit to, from LocalQueue.
//   - owner is the owner reference the Workload carries, so the garbage
//     collector deletes it with what it admits: the run, or the prime claim
//     of a populator restore.
//
// The Workload has one pod set with a count of 1, whose pod asks for one
// RunResource, so Kueue counts it as one unit of work against the queue's
// quota. A Workload that already exists is returned unchanged, so calling
// this on every pass is safe.
func EnsureWorkload(ctx context.Context, reader client.Reader, c client.Client, namespace, name, queue string, owner metav1.OwnerReference) (*kueuev1beta2.Workload, error) {
	workload := &kueuev1beta2.Workload{}
	err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, workload)
	if err == nil {
		return workload, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get Workload %s/%s: %w", namespace, name, err)
	}

	one := corev1.ResourceList{RunResource: resource.MustParse("1")}
	workload = &kueuev1beta2.Workload{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name, OwnerReferences: []metav1.OwnerReference{owner}},
		Spec: kueuev1beta2.WorkloadSpec{
			QueueName: kueuev1beta2.LocalQueueName(queue),
			PodSets: []kueuev1beta2.PodSet{{
				Name:  "run",
				Count: 1,
				Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{{
						Name:      "run",
						Image:     admissionImage,
						Resources: corev1.ResourceRequirements{Requests: one, Limits: one},
					}},
				}},
			}},
		},
	}
	if err := c.Create(ctx, workload); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create Workload %s/%s: %w", namespace, name, err)
	}
	return workload, nil
}

// Admitted reports whether Kueue has admitted the Workload, which it shows
// with an Admitted condition set to True.
func Admitted(workload *kueuev1beta2.Workload) bool {
	return meta.IsStatusConditionTrue(workload.Status.Conditions, kueuev1beta2.WorkloadAdmitted)
}

// Evicted reports whether Kueue has evicted an admitted Workload, which it
// shows with an Evicted condition set to True. Kueue then expects the work to
// stop.
func Evicted(workload *kueuev1beta2.Workload) bool {
	return meta.IsStatusConditionTrue(workload.Status.Conditions, kueuev1beta2.WorkloadEvicted)
}

// MarkPodsReady adds a PodsReady condition set to True to the Workload's
// status, which tells Kueue that the admitted work is running. The
// condition's lastTransitionTime is the time given in now. When the condition
// is already True, it does nothing.
//
// Kueue's waitForPodsReady evicts an admitted Workload whose PodsReady
// condition stays false past its timeout. Kueue's job integrations set that
// condition from their pods. This Workload has no pods, so the caller sets it.
func MarkPodsReady(ctx context.Context, c client.Client, workload *kueuev1beta2.Workload, now metav1.Time) error {
	if meta.IsStatusConditionTrue(workload.Status.Conditions, kueuev1beta2.WorkloadPodsReady) {
		return nil
	}
	meta.SetStatusCondition(&workload.Status.Conditions, metav1.Condition{
		Type:               kueuev1beta2.WorkloadPodsReady,
		Status:             metav1.ConditionTrue,
		Reason:             "Started",
		Message:            "backup-controller started the work",
		LastTransitionTime: now,
	})
	if err := c.Status().Update(ctx, workload); err != nil {
		return fmt.Errorf("mark Workload %s ready: %w", workload.Name, err)
	}
	return nil
}

// Get reads a Workload.
//
// Parameters:
//   - namespace and name identify the Workload, such as a run's
//     status.workload.
//
// It returns nil and no error when the Workload is gone, and an error when
// the read fails for another reason.
func Get(ctx context.Context, c client.Reader, namespace, name string) (*kueuev1beta2.Workload, error) {
	workload := &kueuev1beta2.Workload{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, workload); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get Workload %s: %w", name, err)
	}
	return workload, nil
}

// Delete deletes a Workload, which gives its quota back to the queue.
//
// Parameters:
//   - namespace is the Workload's namespace.
//   - kind and uid name the Workload (see Name).
//
// A Workload that is already gone counts as deleted.
func Delete(ctx context.Context, c client.Client, namespace, kind string, uid types.UID) error {
	workload := &kueuev1beta2.Workload{ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: Name(kind, uid)}}
	if err := c.Delete(ctx, workload); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Workload %s: %w", workload.Name, err)
	}
	return nil
}
