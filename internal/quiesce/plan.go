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
	suspend, err := toSuspend(ctx, reader, mapper, runNamespace, targets)
	if err != nil {
		return nil, nil, err
	}
	stop := make([]backupv1alpha1.QuiescedWorkload, 0, len(targets))
	for _, t := range targets {
		stop = append(stop, backupv1alpha1.QuiescedWorkload{Kind: t.kind, Name: t.object.GetName(), Replicas: t.replicas})
	}
	return stop, suspend, nil
}

// toSuspend lists the Kustomizations that Plan suspends for the targets.
//
// Parameters:
//   - reader and mapper read each Kustomization, as in Plan.
//   - runNamespace is the run's namespace, which holds the targets.
//   - targets are the workloads to stop.
//
// It returns the "namespace/name" keys of the Kustomizations to suspend, in
// the order of the targets, with each key one time. It returns the errors
// that Plan documents.
//
// The function reads each Kustomization one time, also when several targets
// name it. A target with no Kustomization, or with a Kustomization that
// suspendable refuses to suspend, adds no key.
func toSuspend(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, runNamespace string, targets []Workload) ([]string, error) {
	var suspend []string
	read := map[string]*unstructured.Unstructured{}
	for _, t := range targets {
		key, kustomization, err := labelledKustomization(ctx, reader, mapper, read, t)
		if err != nil {
			return nil, err
		}
		if kustomization == nil || slices.Contains(suspend, key) {
			continue
		}
		add, err := suspendable(kustomization, key, runNamespace, t)
		if err != nil {
			return nil, err
		}
		if add {
			suspend = append(suspend, key)
		}
	}
	return suspend, nil
}

// labelledKustomization gets the Kustomization that the kustomize-controller
// labels on a target name.
//
// Parameters:
//   - reader and mapper read the Kustomization, as in Plan.
//   - read keeps each Kustomization that the function got before, by its
//     "namespace/name" key. A nil value is a Kustomization that does not
//     exist. The function adds each new read to it.
//   - t is the target.
//
// It returns the key and the Kustomization. It returns a nil Kustomization
// when the target has no labels or the Kustomization does not exist. It
// returns a wrapped error when the read fails for a different cause.
func labelledKustomization(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, read map[string]*unstructured.Unstructured, t Workload) (string, *unstructured.Unstructured, error) {
	name := t.object.GetLabels()[FluxNameLabel]
	namespace := t.object.GetLabels()[FluxNamespaceLabel]
	if name == "" || namespace == "" {
		return "", nil, nil
	}
	key := namespace + "/" + name
	if kustomization, done := read[key]; done {
		return key, kustomization, nil
	}
	kustomization, err := getKustomization(ctx, reader, mapper, namespace, name)
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return "", nil, fmt.Errorf("get Kustomization %s: %w", key, err)
		}
		kustomization = nil
	}
	read[key] = kustomization
	return key, kustomization, nil
}

// suspendable reports whether Plan suspends a Kustomization for a target.
//
// Parameters:
//   - kustomization is the Kustomization that the labels on t name.
//   - key is its "namespace/name" key, for the errors.
//   - runNamespace is the run's namespace.
//   - t is the target.
//
// It returns true when the inventory of the Kustomization lists t and the
// spec.suspend of the Kustomization is not true. It returns false when the
// inventory does not list t, or when spec.suspend is true already. It returns
// the *InventoryError from inventoryIDs when the inventory is missing or does
// not parse. It returns a *CrossNamespaceError when the inventory lists t and
// also lists a workload in a different namespace.
func suspendable(kustomization *unstructured.Unstructured, key, runNamespace string, t Workload) (bool, error) {
	if err := inventoryIDs(kustomization); err != nil {
		return false, err
	}
	if !inventoryLists(kustomization, t) {
		return false, nil
	}
	if namespaces := OtherNamespaces(kustomization, runNamespace); len(namespaces) > 1 {
		return false, &CrossNamespaceError{Kustomization: key, Namespaces: namespaces}
	}
	already, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend")
	return !already, nil
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
		parsed, ok := parseInventoryID(id)
		if !ok || parsed.Group != appsv1.GroupName || (parsed.Kind != backupv1alpha1.WorkloadKindDeployment && parsed.Kind != backupv1alpha1.WorkloadKindStatefulSet) || parsed.Namespace == "" {
			continue
		}
		seen[parsed.Namespace] = true
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

// inventoryIDs checks the ids of a Kustomization's status.inventory.entries.
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
// InventoryEntryMalformed when an entry has no string id that
// parseInventoryID reads. A
// Flux release that moves or reshapes the field then fails the run loudly:
// read as an empty list, it would make the run stop the workload
// with its Kustomization still reconciling, and Flux would scale it back up
// in the middle of the backup.
func inventoryIDs(kustomization *unstructured.Unstructured) error {
	key := kustomization.GetNamespace() + "/" + kustomization.GetName()
	entries, found, err := unstructured.NestedSlice(kustomization.Object, "status", "inventory", "entries")
	if err != nil || !found {
		return &InventoryError{Kustomization: key, Problem: InventoryMissing}
	}
	for _, entry := range entries {
		fields, _ := entry.(map[string]any)
		id, _ := fields["id"].(string)
		if _, ok := parseInventoryID(id); !ok {
			return &InventoryError{Kustomization: key, Problem: InventoryEntryMalformed, Entry: fmt.Sprintf("%v", entry)}
		}
	}
	return nil
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

// inventoryID is one id of status.inventory.entries, split into its fields.
type inventoryID struct {
	// Namespace is empty for a cluster-scoped object.
	Namespace string
	// Name has ":" where the id has "__".
	Name string
	// Group is empty for a core object.
	Group string
	Kind  string
}

// parseInventoryID splits an inventory id with the rule of ParseObjMetadata
// in fluxcd/cli-utils v1.2.3 (pkg/object/objmetadata.go:69-102), which
// kustomize-controller uses to write the id (internal/inventory/inventory.go:
// 44-47). The namespace ends at the first "_". The kind starts after the last
// "_", and the group after the "_" before it. The rest is the name, in which
// "__" is ":", because ObjMetadata.String writes ":" in an RBAC name as "__"
// (objmetadata.go:32-35, 115-128).
//
// It returns false when the id has fewer "_" than the three fields need.
func parseInventoryID(id string) (inventoryID, bool) {
	namespace, rest, ok := strings.Cut(id, "_")
	if !ok {
		return inventoryID{}, false
	}
	k := strings.LastIndex(rest, "_")
	if k < 0 {
		return inventoryID{}, false
	}
	rest, kind := rest[:k], rest[k+1:]
	g := strings.LastIndex(rest, "_")
	if g < 0 {
		return inventoryID{}, false
	}
	name, group := rest[:g], rest[g+1:]
	return inventoryID{Namespace: namespace, Name: strings.ReplaceAll(name, "__", ":"), Group: group, Kind: kind}, true
}
