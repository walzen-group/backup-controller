package runs

import (
	"context"
	"fmt"
	"sort"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkloadGVK and LocalQueueListGVK are the Kueue kinds a run uses. The
// controller reads and writes them as unstructured objects, so it doesn't
// depend on Kueue's Go module.
var (
	WorkloadGVK       = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	LocalQueueListGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "LocalQueueList"}
)

// admissionImage is the image named in the pod template of a run's Workload.
// Kueue reads the template to count the Workload against its quota, and it
// never runs the image.
const admissionImage = "registry.k8s.io/pause:3.10"

// noQueueError reports that Kueue can't admit a namespace's runs, because the
// namespace has no LocalQueue or the cluster serves no Kueue. The run then
// waits in Queued and does no work, so no namespace escapes the ClusterQueue's
// quota.
type noQueueError struct {
	message string
}

// Error returns the message that the run shows on its Ready condition.
func (e noQueueError) Error() string {
	return e.message
}

// localQueue returns the name of the LocalQueue that admits a namespace's
// runs.
//
// A namespace normally holds one LocalQueue. When it holds several, the one
// named backups is used, or else the first by name, so the choice stays the
// same from one reconcile to the next.
//
// It returns a noQueueError when the namespace has no LocalQueue, or when the
// cluster serves no kueue.x-k8s.io/v1beta2 LocalQueue, and any other error
// when the list fails.
func localQueue(ctx context.Context, c client.Reader, namespace string) (string, error) {
	queues := &unstructured.UnstructuredList{}
	queues.SetGroupVersionKind(LocalQueueListGVK)
	if err := c.List(ctx, queues, client.InNamespace(namespace)); err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return "", noQueueError{message: fmt.Sprintf("the cluster serves no %s LocalQueue; the run waits until Kueue is installed", LocalQueueListGVK.GroupVersion())}
		}
		return "", fmt.Errorf("list the LocalQueues in %s: %w", namespace, err)
	}
	var names []string
	for _, q := range queues.Items {
		if q.GetName() == "backups" {
			return "backups", nil
		}
		names = append(names, q.GetName())
	}
	if len(names) == 0 {
		return "", noQueueError{message: fmt.Sprintf("namespace %s has no Kueue LocalQueue; the run waits until one exists", namespace)}
	}
	sort.Strings(names)
	return names[0], nil
}

// queuedMessage is the Ready message of a run that waits for Kueue to admit
// its Workload.
const queuedMessage = "waiting for the backup queue to admit the run"

// setQueued sets a run's Ready condition to False with reason Queued and the
// given message.
//
// Parameters:
//   - conditions is the run's status.conditions.
//   - generation is the run's metadata.generation, for observedGeneration.
//   - message says what the run waits for: queuedMessage, or the message of a
//     noQueueError.
//
// It returns true when the condition changed, so the caller writes the status
// only then.
func setQueued(conditions *[]metav1.Condition, generation int64, message string) bool {
	current := meta.FindStatusCondition(*conditions, backupv1alpha1.ConditionReady)
	if current != nil && current.Reason == backupv1alpha1.ReasonQueued && current.Message == message {
		return false
	}
	backupv1alpha1.SetReady(conditions, generation, metav1.ConditionFalse, backupv1alpha1.ReasonQueued, message)
	return true
}

// RunResource is the resource each run's Workload asks for, one per run. A
// ClusterQueue that covers it with a quota of 5 admits five runs at once, so
// the quota bounds how many runs back up or restore at the same time, and so
// the controller's own work and memory.
const RunResource = "backup-controller.wlz.li/run"

// workloadName returns the name of a run's Workload: the lower-case kind of
// the run, a dash, and the run's UID, such as backuprun-<UID>. Because the
// name comes from the UID, a restarted controller finds the Workload it made.
func workloadName(kind string, uid types.UID) string {
	return strings.ToLower(kind) + "-" + string(uid)
}

// ensureWorkload creates the Kueue Workload through which a run waits for
// admission, and returns the Workload as it stands.
//
// Parameters:
//   - owner is the run. The Workload is named after its UID and carries a
//     controller reference to it.
//   - ownerKind is the run's kind. A typed object read through the client
//     carries no kind, so the caller passes it for the owner reference.
//   - queue is the name of the LocalQueue to submit to, from localQueue.
//
// The Workload has one pod set with a count of 1, whose pod asks for one
// RunResource, so Kueue counts the run as one run against the queue's quota.
// A Workload that already exists is returned unchanged, so calling this on
// every reconcile is safe.
func ensureWorkload(ctx context.Context, c client.Client, owner client.Object, ownerKind schema.GroupVersionKind, queue string) (*unstructured.Unstructured, error) {
	name := workloadName(ownerKind.Kind, owner.GetUID())
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(WorkloadGVK)
	err := c.Get(ctx, types.NamespacedName{Namespace: owner.GetNamespace(), Name: name}, workload)
	if err == nil {
		return workload, nil
	}
	if !apierrors.IsNotFound(err) {
		return nil, fmt.Errorf("get Workload %s: %w", name, err)
	}

	workload = &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"queueName": queue,
			"podSets": []any{map[string]any{
				"name":  "run",
				"count": int64(1),
				"template": map[string]any{
					"spec": map[string]any{
						"restartPolicy": "Never",
						"containers": []any{map[string]any{
							"name":  "run",
							"image": admissionImage,
							"resources": map[string]any{
								"requests": map[string]any{RunResource: "1"},
								"limits":   map[string]any{RunResource: "1"},
							},
						}},
					},
				},
			}},
		},
	}}
	workload.SetGroupVersionKind(WorkloadGVK)
	workload.SetName(name)
	workload.SetNamespace(owner.GetNamespace())
	workload.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(owner, ownerKind)})
	if err := c.Create(ctx, workload); err != nil && !apierrors.IsAlreadyExists(err) {
		return nil, fmt.Errorf("create Workload %s: %w", name, err)
	}
	return workload, nil
}

// admitted reports whether Kueue has admitted the Workload, which it shows
// with an Admitted condition set to True.
func admitted(workload *unstructured.Unstructured) bool {
	return conditionTrue(workload, "Admitted")
}

// markPodsReady adds a PodsReady condition set to True to the Workload's
// status, which tells Kueue that the admitted work is running. The
// condition's lastTransitionTime is the time given in now. When the condition
// is already True, it does nothing.
//
// Kueue's waitForPodsReady evicts an admitted Workload whose PodsReady
// condition stays false past its timeout. Kueue's job integrations set that
// condition from their pods. This Workload has no pods, so the run sets it.
func markPodsReady(ctx context.Context, c client.Client, workload *unstructured.Unstructured, now metav1.Time) error {
	if conditionTrue(workload, "PodsReady") {
		return nil
	}
	conditions, _, _ := unstructured.NestedSlice(workload.Object, "status", "conditions")
	conditions = append(conditions, map[string]any{
		"type":               "PodsReady",
		"status":             "True",
		"reason":             "Started",
		"message":            "backup-controller started the run",
		"lastTransitionTime": now.UTC().Format("2006-01-02T15:04:05Z"),
	})
	if err := unstructured.SetNestedSlice(workload.Object, conditions, "status", "conditions"); err != nil {
		return err
	}
	if err := c.Status().Update(ctx, workload); err != nil {
		return fmt.Errorf("mark Workload %s ready: %w", workload.GetName(), err)
	}
	return nil
}

// getWorkload reads a run's Workload.
//
// Parameters:
//   - namespace is the run's namespace.
//   - name is the Workload's name, from the run's status.workload.
//
// It returns nil and no error when the Workload is gone, and an error when
// the read fails for another reason.
func getWorkload(ctx context.Context, c client.Reader, namespace, name string) (*unstructured.Unstructured, error) {
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(WorkloadGVK)
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, workload); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("get Workload %s: %w", name, err)
	}
	return workload, nil
}

// evicted reports whether Kueue has evicted an admitted Workload, which it
// shows with an Evicted condition set to True. Kueue then expects the work
// to stop.
func evicted(workload *unstructured.Unstructured) bool {
	return conditionTrue(workload, "Evicted")
}

// deleteWorkload deletes a run's Workload, which gives the run's quota back
// to the queue.
//
// Parameters:
//   - namespace is the run's namespace.
//   - kind and uid are the run's kind and UID, which name the Workload (see
//     workloadName).
//
// A Workload that is already gone counts as deleted.
func deleteWorkload(ctx context.Context, c client.Client, namespace, kind string, uid types.UID) error {
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(WorkloadGVK)
	workload.SetNamespace(namespace)
	workload.SetName(workloadName(kind, uid))
	if err := c.Delete(ctx, workload); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Workload %s: %w", workload.GetName(), err)
	}
	return nil
}

// conditionTrue reports whether an unstructured object's status.conditions
// holds a condition of the type given in kind with status "True".
func conditionTrue(object *unstructured.Unstructured, kind string) bool {
	conditions, _, _ := unstructured.NestedSlice(object.Object, "status", "conditions")
	for _, entry := range conditions {
		condition, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if condition["type"] == kind && condition["status"] == "True" {
			return true
		}
	}
	return false
}
