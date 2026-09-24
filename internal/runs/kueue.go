package runs

import (
	"context"
	"fmt"
	"sort"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The Kueue kinds a run uses. Read as unstructured, so the controller carries
// no dependency on Kueue's Go module and runs in a cluster without Kueue.
var (
	WorkloadGVK       = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	LocalQueueListGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "LocalQueueList"}
)

// admissionImage is the image the Workload's pod template names. Kueue reads
// the template to count the Workload against its quota and never runs it.
const admissionImage = "registry.k8s.io/pause:3.10"

// localQueue returns the LocalQueue a namespace's runs are admitted through,
// or empty when the namespace has none and runs start without admission.
//
// A namespace normally holds one. With several, the one named backups is
// used, and otherwise the first by name, so the choice is stable.
func localQueue(ctx context.Context, c client.Reader, namespace string) (string, error) {
	queues := &unstructured.UnstructuredList{}
	queues.SetGroupVersionKind(LocalQueueListGVK)
	if err := c.List(ctx, queues, client.InNamespace(namespace)); err != nil {
		if meta.IsNoMatchError(err) || apierrors.IsNotFound(err) {
			return "", nil
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
		return "", nil
	}
	sort.Strings(names)
	return names[0], nil
}

// workloadName is the Workload a run creates, derived from its UID so a
// restarted controller finds the one it made.
func workloadName(uid types.UID) string {
	return "backuprun-" + string(uid)
}

// ensureWorkload creates the run's Workload, counted as one pod, and returns
// it as it stands. ownerKind is the run's kind, which a typed object read
// through the client does not carry.
func ensureWorkload(ctx context.Context, c client.Client, owner client.Object, ownerKind schema.GroupVersionKind, queue string) (*unstructured.Unstructured, error) {
	name := workloadName(owner.GetUID())
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

// admitted reports whether Kueue admitted the Workload.
func admitted(workload *unstructured.Unstructured) bool {
	return conditionTrue(workload, "Admitted")
}

// markPodsReady tells Kueue the admitted work is running.
//
// Kueue's waitForPodsReady evicts an admitted Workload whose PodsReady
// condition stays false past its timeout. The job integrations set that
// condition from their pods; this Workload has no pods, so the run sets it.
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

// deleteWorkload removes the run's Workload, which gives its quota back.
func deleteWorkload(ctx context.Context, c client.Client, namespace string, uid types.UID) error {
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(WorkloadGVK)
	workload.SetNamespace(namespace)
	workload.SetName(workloadName(uid))
	if err := c.Delete(ctx, workload); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Workload %s: %w", workload.GetName(), err)
	}
	return nil
}

// conditionTrue reads one condition off an unstructured object's status.
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
