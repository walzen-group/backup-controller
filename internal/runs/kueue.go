package runs

import (
	"context"
	"fmt"
	"sort"

	"github.com/walzen-group/backup-controller/internal/served"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// WorkloadGVK and LocalQueueGVK name the Kueue kinds a run uses. The
// controller reads and writes them as unstructured objects, so it doesn't
// depend on Kueue's Go module, and it runs in a cluster without Kueue. The
// version is the one Kueue 0.19 prefers; a run sends every request at the
// version the API server serves instead (see served.Kind).
var (
	WorkloadGVK   = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "Workload"}
	LocalQueueGVK = schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "LocalQueue"}
)

// admissionImage is the image named in the pod template of a run's Workload.
// Kueue reads the template to count the Workload against its quota, and it
// never runs the image.
const admissionImage = "registry.k8s.io/pause:3.10"

// localQueue returns the name of the LocalQueue that admits a namespace's
// runs, or an empty name when the namespace has none, and the run then starts
// without admission.
//
// Parameters:
//   - c lists the LocalQueues. Callers pass the uncached Reader.
//   - mapper looks up the version at which the API server serves
//     LocalQueues (see served.Kind).
//   - namespace is the run's namespace.
//
// A cluster that serves no version of LocalQueue has no Kueue, and gives an
// empty name. Every other failure is an error, and the caller retries with
// the run still queued: a failed lookup, and a list the API server refuses,
// including one at a version it has stopped serving since the mapper cached
// it (see served.VersionGone). Taking any of those for a cluster without Kueue
// would start the run past its queue.
//
// A namespace normally holds one LocalQueue. When it holds several, the one
// named backups is used, or else the first by name, so the choice stays the
// same from one reconcile to the next.
func localQueue(ctx context.Context, c client.Reader, mapper meta.RESTMapper, namespace string) (string, error) {
	gvk, err := served.Kind(mapper, LocalQueueGVK.GroupKind())
	if apierrors.IsNotFound(err) {
		return "", nil
	}
	if err != nil {
		return "", fmt.Errorf("list the LocalQueues in %s: %w", namespace, err)
	}
	queues := &unstructured.UnstructuredList{}
	queues.SetGroupVersionKind(gvk.GroupVersion().WithKind(gvk.Kind + "List"))
	if err := served.VersionGone(mapper, gvk, c.List(ctx, queues, client.InNamespace(namespace))); err != nil {
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

// workloadName returns the name of a run's Workload: "backuprun-" followed by
// the run's UID. Because the name comes from the UID, a restarted controller
// finds the Workload it made.
func workloadName(uid types.UID) string {
	return "backuprun-" + string(uid)
}

// ensureWorkload creates the Kueue Workload through which a run waits for
// admission, and returns the Workload as it stands.
//
// Parameters:
//   - c reads and creates the Workload. Its RESTMapper gives the version at
//     which the API server serves Workloads (see served.Kind).
//   - owner is the run. The Workload is named after its UID and carries a
//     controller reference to it.
//   - ownerKind is the run's kind. A typed object read through the client
//     carries no kind, so the caller passes it for the owner reference.
//   - queue is the name of the LocalQueue to submit to, from localQueue.
//
// The Workload has one pod set with a count of 1, so Kueue counts the run as
// one pod against the queue's quota. A Workload that already exists is
// returned unchanged, so calling this on every reconcile is safe.
//
// It returns an error when the lookup of the served version fails, even
// when the cluster serves no version of Workload at all: the namespace has a
// LocalQueue, so the run must go through Kueue. A get at a version the API
// server has stopped serving is an error as well (see served.VersionGone), and no
// Workload is created then, since one may exist at another version.
func ensureWorkload(ctx context.Context, c client.Client, owner client.Object, ownerKind schema.GroupVersionKind, queue string) (*unstructured.Unstructured, error) {
	name := workloadName(owner.GetUID())
	gvk, err := served.Kind(c.RESTMapper(), WorkloadGVK.GroupKind())
	if err != nil {
		return nil, fmt.Errorf("get Workload %s: %w", name, err)
	}
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(gvk)
	err = served.VersionGone(c.RESTMapper(), gvk, c.Get(ctx, types.NamespacedName{Namespace: owner.GetNamespace(), Name: name}, workload))
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
	workload.SetGroupVersionKind(gvk)
	workload.SetName(name)
	workload.SetNamespace(owner.GetNamespace())
	workload.SetOwnerReferences([]metav1.OwnerReference{*metav1.NewControllerRef(owner, ownerKind)})
	if err := served.VersionGone(c.RESTMapper(), gvk, c.Create(ctx, workload)); err != nil && !apierrors.IsAlreadyExists(err) {
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
//
// The status is written at the Workload's own version, the one
// ensureWorkload read or created it at. A write the API server refuses, one
// at a version it has stopped serving included (see served.VersionGone), comes back
// as an error.
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
	if err := served.VersionGone(c.RESTMapper(), workload.GroupVersionKind(), c.Status().Update(ctx, workload)); err != nil {
		return fmt.Errorf("mark Workload %s ready: %w", workload.GetName(), err)
	}
	return nil
}

// deleteWorkload deletes the Workload of the run whose UID is given, which
// gives the run's quota back to the queue. It deletes at the version of
// Workload the API server serves (see served.Kind). A Workload that is
// already gone counts as deleted, and so does every Workload on a cluster
// that serves no version of the kind, which has no Kueue installed.
//
// It returns an error when the lookup of the served version fails for
// another reason, such as a failed discovery call, or when the delete fails
// with an error other than NotFound. A delete at a version the API server
// has stopped serving since the client's mapper cached it is such an error
// (see served.VersionGone), because the Workload may still exist at another
// version.
func deleteWorkload(ctx context.Context, c client.Client, namespace string, uid types.UID) error {
	gvk, err := served.Kind(c.RESTMapper(), WorkloadGVK.GroupKind())
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("delete Workload %s: %w", workloadName(uid), err)
	}
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(gvk)
	workload.SetNamespace(namespace)
	workload.SetName(workloadName(uid))
	if err := served.VersionGone(c.RESTMapper(), gvk, c.Delete(ctx, workload)); err != nil && !apierrors.IsNotFound(err) {
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
