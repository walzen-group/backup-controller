package quiesce

import (
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Plan works out what stopping the target workloads changes, and changes
// nothing yet. A run records the plan in its status before it calls
// Apply, so a pass that stops the workloads and then loses its status
// write is retried from the plan. Reading the workloads again at that point
// would find them at zero replicas and their Kustomizations suspended, and
// the run would give back nothing.
//
// Parameters:
//   - reader reads each Kustomization's spec.suspend and inventory.
//   - mapper looks up the version at which the API server serves
//     Kustomizations (see served.Kind).
//   - runNamespace is the run's namespace, which holds the targets.
//   - targets are the workloads to stop, from Targets or Named.
//
// It returns each workload with the replica count it has now, which is the
// count Restart gives back, and the Kustomizations to suspend as
// "namespace/name" keys. It returns a *CrossNamespaceError when a
// Kustomization that applies a target also applies a Deployment or a
// StatefulSet in another namespace (see OtherNamespaces). It returns the
// *InventoryError from inventoryIDs when the status.inventory.entries of such
// a Kustomization is missing or does not parse. Both errors end the run. It
// returns a wrapped error when it cannot read a Kustomization.
// On a cluster that serves no version of Kustomization, every Kustomization
// counts as gone.
//
// The kustomize-controller labels on a target name its Kustomization, and
// that Kustomization is suspended only when its status.inventory lists the
// target. Anyone who can edit a workload can set its labels, and suspending a
// Kustomization is a cluster-wide write, so the labels alone would let a
// workload suspend a Kustomization in any namespace. A target without the
// labels, whose Kustomization no longer exists, or whose Kustomization
// doesn't list it is stopped with nothing suspended. A Kustomization that is
// already suspended is left out, because the run didn't suspend it and must
// not resume it; it is refused all the same when it applies workloads of
// another namespace. A Kustomization that applies several targets is listed
// once.
func Plan(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, runNamespace string, targets []Workload) ([]backupv1alpha1.QuiescedWorkload, []string, error) {
	var suspend []string
	read := map[string]*unstructured.Unstructured{}
	for _, t := range targets {
		name := t.object.GetLabels()[FluxNameLabel]
		namespace := t.object.GetLabels()[FluxNamespaceLabel]
		if name == "" || namespace == "" {
			continue
		}
		key := namespace + "/" + name
		kustomization, done := read[key]
		if !done {
			var err error
			kustomization, err = getKustomization(ctx, reader, mapper, namespace, name)
			if err != nil {
				if !apierrors.IsNotFound(err) {
					return nil, nil, fmt.Errorf("get Kustomization %s: %w", key, err)
				}
				kustomization = nil
			}
			read[key] = kustomization
		}
		if kustomization == nil || slices.Contains(suspend, key) {
			continue
		}
		if _, err := inventoryIDs(kustomization); err != nil {
			return nil, nil, err
		}
		if !inventoryLists(kustomization, t) {
			continue
		}
		if namespaces := OtherNamespaces(kustomization, runNamespace); len(namespaces) > 1 {
			return nil, nil, &CrossNamespaceError{Kustomization: key, Namespaces: namespaces}
		}
		if already, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend"); already {
			continue
		}
		suspend = append(suspend, key)
	}

	stop := make([]backupv1alpha1.QuiescedWorkload, 0, len(targets))
	for _, t := range targets {
		stop = append(stop, backupv1alpha1.QuiescedWorkload{Kind: t.kind, Name: t.object.GetName(), Replicas: t.replicas})
	}
	return stop, suspend, nil
}

// OtherNamespaces lists the namespaces a Kustomization applies workloads in
// when they go beyond the run's namespace.
//
// Parameters:
//   - kustomization is the Kustomization as read, with its
//     status.inventory.entries.
//   - namespace is the run's namespace.
//
// It returns the run's namespace and the namespaces of the Deployments and
// StatefulSets the inventory lists, sorted, when at least one of them is
// another namespace, and nil when every such workload is in the run's
// namespace.
//
// Each entry's id is "<namespace>_<name>_<group>_<kind>" (see
// inventoryLists), and only apps Deployments and StatefulSets count, since
// those are what a run stops. A Kustomization that applies workloads of two
// namespaces is one Plan refuses: suspending it while one run stops its
// workloads leaves the workloads of the other namespace unmanaged by Flux,
// and a run there would resume it while this run holds its workloads at 0.
func OtherNamespaces(kustomization *unstructured.Unstructured, namespace string) []string {
	seen := map[string]bool{namespace: true}
	entries, _, _ := unstructured.NestedSlice(kustomization.Object, "status", "inventory", "entries")
	for _, entry := range entries {
		fields, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		id, _ := fields["id"].(string)
		parts := strings.Split(id, "_")
		if len(parts) != 4 || parts[2] != appsv1.GroupName || (parts[3] != "Deployment" && parts[3] != "StatefulSet") || parts[0] == "" {
			continue
		}
		seen[parts[0]] = true
	}
	if len(seen) < 2 {
		return nil
	}
	namespaces := make([]string, 0, len(seen))
	for n := range seen {
		namespaces = append(namespaces, n)
	}
	sort.Strings(namespaces)
	return namespaces
}

// joinAnd returns the given names listed the way a sentence lists them: "a",
// "a and b", or "a, b and c".
func joinAnd(names []string) string {
	if len(names) < 2 {
		return strings.Join(names, "")
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
}

// inventoryIDs returns the ids of a Kustomization's status.inventory.entries.
// kustomize-controller records each object it applied as an entry whose id
// is "<namespace>_<name>_<group>_<kind>", such as
// notes_notes_apps_Deployment; the namespace of a cluster-scoped object and
// the group of a core object are empty.
//
// The caller reads the inventory of a Kustomization whose
// kustomize-controller labels name it on a workload the run stops, so
// kustomize-controller applied that workload and recorded it. It returns an
// *InventoryError with Problem InventoryMissing when status.inventory.entries
// is missing or is not a list. It returns an *InventoryError with Problem
// InventoryEntryMalformed when an entry has no string id of four parts. A
// Flux release that moves or reshapes the field then fails the run loudly:
// read as an empty list, it would make the run stop the workload
// with its Kustomization still reconciling, and Flux would scale it back up
// in the middle of the backup.
func inventoryIDs(kustomization *unstructured.Unstructured) ([]string, error) {
	key := kustomization.GetNamespace() + "/" + kustomization.GetName()
	entries, found, err := unstructured.NestedSlice(kustomization.Object, "status", "inventory", "entries")
	if err != nil || !found {
		return nil, &InventoryError{Kustomization: key, Problem: InventoryMissing}
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		fields, _ := entry.(map[string]any)
		id, _ := fields["id"].(string)
		if strings.Count(id, "_") != 3 {
			return nil, &InventoryError{Kustomization: key, Problem: InventoryEntryMalformed, Entry: fmt.Sprintf("%v", entry)}
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// inventoryLists reports whether a Kustomization's status.inventory.entries
// lists a workload. kustomize-controller records each object it applied as
// an entry whose id is "<namespace>_<name>_<group>_<kind>", such as
// notes_notes_apps_Deployment.
func inventoryLists(kustomization *unstructured.Unstructured, t Workload) bool {
	id := t.object.GetNamespace() + "_" + t.object.GetName() + "_apps_" + t.kind
	entries, _, _ := unstructured.NestedSlice(kustomization.Object, "status", "inventory", "entries")
	for _, entry := range entries {
		if fields, ok := entry.(map[string]any); ok && fields["id"] == id {
			return true
		}
	}
	return false
}
