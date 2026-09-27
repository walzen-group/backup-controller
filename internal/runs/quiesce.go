package runs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KustomizationGVK is the kind of a Flux Kustomization, the object that
// applies a workload. A run suspends the Kustomization while the workload is
// stopped, so that Flux doesn't scale the workload back up. The run reads
// and writes Kustomizations at the version the API server serves, which
// served.Kind looks up; v1 is the version this code was written against.
var KustomizationGVK = schema.GroupVersionKind{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization"}

// getKustomization reads one Kustomization at the version the API server
// serves (see served.Kind).
//
// Parameters:
//   - reader reads the Kustomization.
//   - mapper looks up the served version.
//   - namespace and name name the Kustomization.
//
// It returns an error for which apierrors.IsNotFound is true when the
// Kustomization doesn't exist or no version of the kind is served. A read at
// a version the API server has stopped serving since mapper cached it
// returns a *served.VersionGoneError (see served.VersionGone). Any other failed lookup or
// read is returned as it is.
func getKustomization(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, namespace, name string) (*unstructured.Unstructured, error) {
	gvk, err := served.Kind(mapper, KustomizationGVK.GroupKind())
	if err != nil {
		return nil, err
	}
	kustomization := &unstructured.Unstructured{}
	kustomization.SetGroupVersionKind(gvk)
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, kustomization); err != nil {
		return nil, served.VersionGone(mapper, gvk, err)
	}
	return kustomization, nil
}

// fluxNameLabel and fluxNamespaceLabel are the labels kustomize-controller
// writes on every object it applies. Together they name the Kustomization
// that applied the object. planStop suspends that Kustomization when its
// inventory lists the object.
const (
	fluxNameLabel      = "kustomize.toolkit.fluxcd.io/name"
	fluxNamespaceLabel = "kustomize.toolkit.fluxcd.io/namespace"
)

// durablyRestarted reports whether the run's stored status shows that it gave
// the workloads back: status.restartedAt is set and, on a BackupRun,
// status.restartPending is cleared. A RestoreRun writes restartedAt only
// after its restart succeeded.
func durablyRestarted(run client.Object) bool {
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		return r.Status.RestartedAt != nil && !r.Status.RestartPending
	case *backupv1alpha1.RestoreRun:
		return r.Status.RestartedAt != nil
	}
	return false
}

// readStop reads a run again straight from the API server and puts the
// stored record of its stop and restart on the copy the caller holds. A
// caller that is about to start the workloads again calls it first, so that
// it decides on what the API server holds. stopOwed calls it before a stop
// for the same reason.
//
// Parameters:
//   - reader is the uncached Reader. The reconcilers read their run through
//     the informer cache, which can lag behind the run's own last writes: a
//     pass that reads a copy from before the restart was stored would start
//     the workloads again, under the stop of a run that took the namespace's
//     quiesce Lease since.
//   - run is the BackupRun or RestoreRun as the pass read it. Its
//     status.quiescedAt, status.quiesced, status.suspendedKustomizations,
//     status.restartedAt and, on a BackupRun, status.restartPending are
//     replaced with the stored ones when the stored run is newer.
//
// It returns the stored run's status.phase, which is the phase of run when
// run is as new as the stored one. It returns an error, and changes nothing,
// when the read fails, when the run is gone, or when the stored run with
// that name is another object (a different UID). The caller then changes no
// workload and the pass runs again.
//
// A copy whose resourceVersion matches the stored one is left as it is. A
// newer stored run changes only the fields above, so the caller's later
// status write still carries the old resourceVersion and fails with a
// conflict, and the next pass works from the stored run.
func readStop(ctx context.Context, reader client.Reader, run client.Object) (backupv1alpha1.RunPhase, error) {
	var stored client.Object
	kind := ""
	switch run.(type) {
	case *backupv1alpha1.BackupRun:
		stored, kind = &backupv1alpha1.BackupRun{}, backupv1alpha1.KindBackupRun
	case *backupv1alpha1.RestoreRun:
		stored, kind = &backupv1alpha1.RestoreRun{}, backupv1alpha1.KindRestoreRun
	default:
		return "", fmt.Errorf("read the stop of %T: not a run", run)
	}
	if err := reader.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
		return "", fmt.Errorf("read %s %s/%s again before changing its workloads: %w", kind, run.GetNamespace(), run.GetName(), err)
	}
	if stored.GetUID() != run.GetUID() {
		return "", fmt.Errorf("%s %s/%s is now another object (UID %s, was %s); no workload is changed for the old one",
			kind, run.GetNamespace(), run.GetName(), stored.GetUID(), run.GetUID())
	}
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		s := stored.(*backupv1alpha1.BackupRun) //nolint:forcetypeassert // stored is built above as the run's own type; H1 rewrites readStop
		if s.ResourceVersion != r.ResourceVersion {
			r.Status.QuiescedAt, r.Status.Quiesced, r.Status.SuspendedKustomizations = s.Status.QuiescedAt, s.Status.Quiesced, s.Status.SuspendedKustomizations
			r.Status.RestartedAt, r.Status.RestartPending = s.Status.RestartedAt, s.Status.RestartPending
		}
		return s.Status.Phase, nil
	case *backupv1alpha1.RestoreRun:
		s := stored.(*backupv1alpha1.RestoreRun) //nolint:forcetypeassert // stored is built above as the run's own type; H1 rewrites readStop
		if s.ResourceVersion != r.ResourceVersion {
			r.Status.QuiescedAt, r.Status.Quiesced, r.Status.SuspendedKustomizations = s.Status.QuiescedAt, s.Status.Quiesced, s.Status.SuspendedKustomizations
			r.Status.RestartedAt = s.Status.RestartedAt
		}
		return s.Status.Phase, nil
	}
	return "", nil
}

// stopOwed reports whether a run whose cached copy shows a recorded plan
// without status.quiescedAt still owes the stop, as the API server holds the
// run. quiesce calls it before applyStop when it took the plan from the
// run's status.
//
// Parameters:
//   - reader is the uncached Reader. A cached copy can lag behind the pass
//     that stopped the workloads, recorded status.quiescedAt, gave the
//     workloads back and ended the run; a stop from that copy would leave the
//     app at 0 with no run left to start it again.
//   - run is the BackupRun or RestoreRun as the pass read it. readStop puts
//     the stored record of its stop and restart on it.
//
// It returns true when the stored run is not finished and still shows the
// plan with neither status.quiescedAt nor status.restartedAt set. A failed
// read comes back as the error from readStop, and the caller stops nothing.
func stopOwed(ctx context.Context, reader client.Reader, run client.Object) (bool, error) {
	phase, err := readStop(ctx, reader, run)
	if err != nil {
		return false, err
	}
	plan, quiescedAt, restartedAt := 0, (*metav1.Time)(nil), (*metav1.Time)(nil)
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		plan, quiescedAt, restartedAt = len(r.Status.Quiesced), r.Status.QuiescedAt, r.Status.RestartedAt
	case *backupv1alpha1.RestoreRun:
		plan, quiescedAt, restartedAt = len(r.Status.Quiesced), r.Status.QuiescedAt, r.Status.RestartedAt
	}
	return !phase.Finished() && plan > 0 && quiescedAt == nil && restartedAt == nil, nil
}

// waitingOn returns a message naming another run in the run's namespace that
// this run must wait for before it stops the namespace's workloads, or ""
// when there is none. A run calls it before it takes the namespace's quiesce
// Lease, with nothing stopped.
//
// Parameters:
//   - reader lists the namespace's RestoreRuns, uncached. A failed list comes
//     back as an error, and nothing is decided on it.
//   - run is the asking run; a RestoreRun with its UID is skipped.
//
// The run it names is an unfinished RestoreRun with a Cluster item in phase
// Deleted. That RestoreRun waits for its Cluster to be created again, and a
// Kustomization this run suspends may be the one Flux needs to create it.
func waitingOn(ctx context.Context, reader client.Reader, run metav1.Object) (string, error) {
	restores := &backupv1alpha1.RestoreRunList{}
	if err := reader.List(ctx, restores, client.InNamespace(run.GetNamespace())); err != nil {
		return "", fmt.Errorf("list RestoreRuns in %s: %w", run.GetNamespace(), err)
	}
	for i := range restores.Items {
		other := &restores.Items[i]
		if other.UID == run.GetUID() || other.Status.Phase.Finished() {
			continue
		}
		if cluster := deletedCluster(other); cluster != "" {
			return fmt.Sprintf("RestoreRun %s has deleted Cluster %s and waits for it to be created again; this run stops the workloads once that Cluster is back",
				other.Name, cluster), nil
		}
	}
	return "", nil
}

// deletedCluster returns the name of a Cluster the run deleted and waits for
// its owner to create again, or "" when it has none.
func deletedCluster(run *backupv1alpha1.RestoreRun) string {
	for _, item := range run.Status.Items {
		if item.Kind == "Cluster" && item.Phase == backupv1alpha1.ItemDeleted {
			return item.Name
		}
	}
	return ""
}

// acquireQuiesceLease takes the namespace's quiesce Lease for the run, so
// that one run at a time stops that namespace's workloads. A run calls it
// right before it plans a stop and holds the Lease until its stored status
// shows the workloads back (see releaseQuiesceLeases).
//
// Parameters:
//   - kind is BackupRun or RestoreRun.
//   - run is the run that takes the Lease.
//
// It returns "" once the run holds the Lease, a message for the Ready
// condition naming the run that holds it when another live run does, and an
// error for a failed API call. The caller waits with reason SourceBusy and
// tries again on a later pass, with nothing stopped.
//
// Runs of both kinds take the same Lease, so a backup and a restore of
// different claims in one namespace wait for each other even when their
// workloads do not overlap. The one Lease covers every overlap: a namespace
// run stops every marked workload, and a restore's spec.quiesce usually
// lists some of them.
func acquireQuiesceLease(ctx context.Context, c client.Client, reader client.Reader, run metav1.Object, kind string) (string, error) {
	holder := leaseHolder{kind: kind, run: run, scope: scopeQuiesce}
	return acquireLease(ctx, c, reader, holder, run.GetNamespace(), quiesceLeaseName)
}

// timedOutMessage returns the Ready message of a run that ends because its
// deadline passed.
//
// Parameters:
//   - deadline is the moment the run had to finish by, which the message
//     gives.
//   - conditions are the run's status.conditions before it ends.
//
// When the run was waiting for another run (reason SourceBusy), the message
// adds that wait's own message, so it says what the run waited for.
func timedOutMessage(deadline time.Time, conditions []metav1.Condition) string {
	message := fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339))
	if ready := meta.FindStatusCondition(conditions, backupv1alpha1.ConditionReady); ready != nil && ready.Reason == backupv1alpha1.ReasonSourceBusy {
		message += "; it was waiting: " + ready.Message
	}
	return message
}

// workload is one Deployment or StatefulSet that a run stops while it works.
type workload struct {
	// kind is "Deployment" or "StatefulSet".
	kind string

	// object is the workload as read from the API server.
	object client.Object

	// replicas is the count to give back when the run restarts the workload.
	// An unset spec.replicas counts as 1, which is the Kubernetes default.
	replicas int32

	// selector matches the workload's pods, so that podsGone can wait for
	// them to exit.
	selector *metav1.LabelSelector
}

// quiesceTargets lists the Deployments and StatefulSets in a namespace that
// carry the annotation backup.wlz.li/quiesce: "true". A BackupRun stops these
// while VolSync clones the volumes. The list is sorted by kind, then by name.
//
// It returns an error when either list call fails.
func quiesceTargets(ctx context.Context, c client.Reader, namespace string) ([]workload, error) {
	marked := func(o metav1.Object) bool { return o.GetAnnotations()[backupv1alpha1.AnnotationQuiesce] == "true" }
	replicas := func(r *int32) int32 {
		if r == nil {
			return 1
		}
		return *r
	}

	var targets []workload
	deployments := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployments, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the Deployments in %s: %w", namespace, err)
	}
	for i := range deployments.Items {
		d := &deployments.Items[i]
		if marked(d) {
			targets = append(targets, workload{kind: "Deployment", object: d, replicas: replicas(d.Spec.Replicas), selector: d.Spec.Selector})
		}
	}
	statefulSets := &appsv1.StatefulSetList{}
	if err := c.List(ctx, statefulSets, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the StatefulSets in %s: %w", namespace, err)
	}
	for i := range statefulSets.Items {
		s := &statefulSets.Items[i]
		if marked(s) {
			targets = append(targets, workload{kind: "StatefulSet", object: s, replicas: replicas(s.Spec.Replicas), selector: s.Spec.Selector})
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].kind+"/"+targets[i].object.GetName() < targets[j].kind+"/"+targets[j].object.GetName()
	})
	return targets, nil
}

// namedTargets reads the workloads that a RestoreRun's spec.quiesce lists, in
// the order the list gives them.
//
// Parameters:
//   - namespace is the RestoreRun's namespace. Each workload is looked up there.
//   - refs is the run's spec.quiesce.
//
// It returns a *quiesceSpecError when an entry has a kind other than
// Deployment or StatefulSet, or names a workload the namespace doesn't hold.
// The error names the entry. Any other failed read comes back wrapped as it
// is, and the caller retries it. A RestoreRun calls this while it plans,
// before it stops anything, so a mistake in spec.quiesce fails the run with
// nothing changed.
func namedTargets(ctx context.Context, c client.Reader, namespace string, refs []backupv1alpha1.WorkloadRef) ([]workload, error) {
	replicas := func(r *int32) int32 {
		if r == nil {
			return 1
		}
		return *r
	}
	var targets []workload
	for _, ref := range refs {
		key := types.NamespacedName{Namespace: namespace, Name: ref.Name}
		switch ref.Kind {
		case "Deployment":
			d := &appsv1.Deployment{}
			if err := c.Get(ctx, key, d); err != nil {
				return nil, missingWorkload(ref, err)
			}
			targets = append(targets, workload{kind: ref.Kind, object: d, replicas: replicas(d.Spec.Replicas), selector: d.Spec.Selector})
		case "StatefulSet":
			s := &appsv1.StatefulSet{}
			if err := c.Get(ctx, key, s); err != nil {
				return nil, missingWorkload(ref, err)
			}
			targets = append(targets, workload{kind: ref.Kind, object: s, replicas: replicas(s.Spec.Replicas), selector: s.Spec.Selector})
		default:
			return nil, &quiesceSpecError{fmt.Sprintf("spec.quiesce lists %s %s; only a Deployment or a StatefulSet can be stopped", ref.Kind, ref.Name)}
		}
	}
	return targets, nil
}

// quiesceSpecError is an entry in a RestoreRun's spec.quiesce that the run
// can't act on: a kind it can't stop, or a workload the namespace doesn't
// hold. Reading again won't change it, so the run ends. A failed read of the
// API server is returned as a plain error, and the run retries it.
type quiesceSpecError struct{ message string }

func (e *quiesceSpecError) Error() string { return e.message }

// isQuiesceSpecError reports whether err, or an error it wraps, is a
// *quiesceSpecError.
func isQuiesceSpecError(err error) bool {
	var bad *quiesceSpecError
	return errors.As(err, &bad)
}

// missingWorkload turns the error from reading a spec.quiesce entry into the
// error the RestoreRun reports. A NotFound error becomes a *quiesceSpecError
// saying the namespace holds no such workload. Any other error is wrapped as
// it is.
func missingWorkload(ref backupv1alpha1.WorkloadRef, err error) error {
	if apierrors.IsNotFound(err) {
		return &quiesceSpecError{fmt.Sprintf("spec.quiesce lists %s %s, which this namespace does not hold", ref.Kind, ref.Name)}
	}
	return fmt.Errorf("get %s %s: %w", ref.Kind, ref.Name, err)
}

// planStop works out what stopping the target workloads changes, and changes
// nothing yet. A run records the plan in its status before it calls
// applyStop, so a pass that stops the workloads and then loses its status
// write is retried from the plan. Reading the workloads again at that point
// would find them at zero replicas and their Kustomizations suspended, and
// the run would give back nothing.
//
// Parameters:
//   - reader reads each Kustomization's spec.suspend and inventory.
//   - mapper looks up the version at which the API server serves
//     Kustomizations (see served.Kind).
//   - runNamespace is the run's namespace, which holds the targets.
//   - targets are the workloads to stop, from quiesceTargets or namedTargets.
//
// It returns each workload with the replica count it has now, which is the
// count restartWorkloads gives back, and the Kustomizations to suspend as
// "namespace/name" keys. It returns an *invalidSpecError, which ends the run
// (see asRunRefusal), when a Kustomization that applies a target also
// applies a Deployment or a StatefulSet in another namespace (see
// otherNamespaces), or whose status.inventory.entries is missing or does not
// parse (see inventoryIDs), and an error when a Kustomization can't be read.
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
func planStop(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, runNamespace string, targets []workload) ([]backupv1alpha1.QuiescedWorkload, []string, error) {
	var suspend []string
	read := map[string]*unstructured.Unstructured{}
	for _, t := range targets {
		name := t.object.GetLabels()[fluxNameLabel]
		namespace := t.object.GetLabels()[fluxNamespaceLabel]
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
			return nil, nil, invalidSpec("%v", err)
		}
		if !inventoryLists(kustomization, t) {
			continue
		}
		if namespaces := otherNamespaces(kustomization, runNamespace); len(namespaces) > 1 {
			return nil, nil, invalidSpec("Kustomization %s applies workloads in namespaces %s; a run suspends the Kustomization while it stops workloads, "+
				"which would leave the other namespace's workloads unmanaged by Flux, so it refuses. Give each namespace its own Kustomization",
				key, joinAnd(namespaces))
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

// otherNamespaces lists the namespaces a Kustomization applies workloads in
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
// namespaces is one planStop refuses: suspending it while one run stops its
// workloads leaves the workloads of the other namespace unmanaged by Flux,
// and a run there would resume it while this run holds its workloads at 0.
func otherNamespaces(kustomization *unstructured.Unstructured, namespace string) []string {
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
// error that names the field when status.inventory.entries is missing or is
// not a list, and one that names the entry when an entry has no string id of
// four parts. A Flux release that moves or reshapes the field then fails the
// run loudly: read as an empty list, it would make the run stop the workload
// with its Kustomization still reconciling, and Flux would scale it back up
// in the middle of the backup.
func inventoryIDs(kustomization *unstructured.Unstructured) ([]string, error) {
	key := kustomization.GetNamespace() + "/" + kustomization.GetName()
	entries, found, err := unstructured.NestedSlice(kustomization.Object, "status", "inventory", "entries")
	if err != nil || !found {
		return nil, fmt.Errorf("the Kustomization %s has no list at status.inventory.entries, where kustomize-controller records every object it "+
			"applies; either it has not applied yet or a Flux release moved the field (see docs/compatibility.md)", key)
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		fields, _ := entry.(map[string]any)
		id, _ := fields["id"].(string)
		if strings.Count(id, "_") != 3 {
			return nil, fmt.Errorf("the Kustomization %s has an entry %v in status.inventory.entries whose id is not "+
				"\"<namespace>_<name>_<group>_<kind>\"; a Flux release may have changed the format (see docs/compatibility.md)", key, entry)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// inventoryLists reports whether a Kustomization's status.inventory.entries
// lists a workload. kustomize-controller records each object it applied as
// an entry whose id is "<namespace>_<name>_<group>_<kind>", such as
// notes_notes_apps_Deployment.
func inventoryLists(kustomization *unstructured.Unstructured, t workload) bool {
	id := t.object.GetNamespace() + "_" + t.object.GetName() + "_apps_" + t.kind
	entries, _, _ := unstructured.NestedSlice(kustomization.Object, "status", "inventory", "entries")
	for _, entry := range entries {
		if fields, ok := entry.(map[string]any); ok && fields["id"] == id {
			return true
		}
	}
	return false
}

// applyStop carries out a plan from planStop. It suspends the Kustomizations
// first, so that Flux can't scale the workloads back up, then scales each
// workload to zero. Both patches set a fixed value, so calling it again with
// the same plan changes nothing more.
//
// Parameters:
//   - namespace is the run's namespace, which holds the workloads.
//   - stop is the run's status.quiesced.
//   - suspend is the run's status.suspendedKustomizations.
//
// It returns the first error it meets and leaves the rest undone. The caller
// then narrows the plan with appliedPart before it restarts the workloads.
func applyStop(ctx context.Context, c client.Client, namespace string, stop []backupv1alpha1.QuiescedWorkload, suspend []string) error {
	for _, key := range suspend {
		ns, name, _ := strings.Cut(key, "/")
		if err := setSuspend(ctx, c, ns, name, true); err != nil {
			return err
		}
	}
	for _, w := range stop {
		object := workloadObject(namespace, w)
		if object == nil {
			continue
		}
		if err := scale(ctx, c, object, 0); err != nil {
			return err
		}
	}
	return nil
}

// appliedPart returns the part of a stop plan that is in effect now. A run
// calls it after applyStop failed, so that the restart that follows only
// touches what the run stopped. A workload whose scale-down was refused
// would refuse the scale-up too, and the run could then never finish.
//
// Parameters:
//   - reader reads each workload and Kustomization as it stands.
//   - mapper looks up the version at which the API server serves
//     Kustomizations (see served.Kind).
//   - namespace is the run's namespace, which holds the workloads.
//   - stop and suspend are the plan, from the run's status.quiesced and
//     status.suspendedKustomizations.
//
// It keeps a workload that stands at zero replicas and a Kustomization that
// is suspended, since an earlier pass that lost its status write may have
// stopped them. It drops what still runs and what is gone, and a
// Kustomization of a kind the API server no longer serves counts as gone.
// An entry that can't be read is kept, so the restart tries to put it back.
func appliedPart(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, namespace string, stop []backupv1alpha1.QuiescedWorkload, suspend []string) ([]backupv1alpha1.QuiescedWorkload, []string) {
	var stopped []backupv1alpha1.QuiescedWorkload
	for _, w := range stop {
		object := workloadObject(namespace, w)
		if object == nil {
			continue
		}
		err := reader.Get(ctx, client.ObjectKeyFromObject(object), object)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			stopped = append(stopped, w)
		default:
			if replicas := specReplicas(object); replicas != nil && *replicas == 0 {
				stopped = append(stopped, w)
			}
		}
	}
	var suspended []string
	for _, key := range suspend {
		ns, name, _ := strings.Cut(key, "/")
		kustomization, err := getKustomization(ctx, reader, mapper, ns, name)
		switch {
		case apierrors.IsNotFound(err):
		case err != nil:
			suspended = append(suspended, key)
		default:
			if on, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend"); on {
				suspended = append(suspended, key)
			}
		}
	}
	return stopped, suspended
}

// specReplicas returns the spec.replicas of a Deployment or StatefulSet, or
// nil for any other object.
func specReplicas(object client.Object) *int32 {
	switch o := object.(type) {
	case *appsv1.Deployment:
		return o.Spec.Replicas
	case *appsv1.StatefulSet:
		return o.Spec.Replicas
	}
	return nil
}

// workloadObject returns an empty Deployment or StatefulSet that carries the
// namespace and name of a recorded workload, ready for a patch. It returns
// nil for any other kind.
func workloadObject(namespace string, w backupv1alpha1.QuiescedWorkload) client.Object {
	var object client.Object
	switch w.Kind {
	case "Deployment":
		object = &appsv1.Deployment{}
	case "StatefulSet":
		object = &appsv1.StatefulSet{}
	default:
		return nil
	}
	object.SetNamespace(namespace)
	object.SetName(w.Name)
	return object
}

// podsGone reports whether every pod of the target workloads has gone. A
// terminating pod still counts, because a pod that is shutting down can still
// write to the volume. A pod in phase Succeeded or Failed doesn't count: its
// containers have ended, and an evicted pod stays in that phase until
// something deletes it.
//
// While a pod is left, it returns false and that pod's name, which the caller
// puts in the message it waits with. It returns an error when a target's
// selector is invalid or its pods can't be listed.
func podsGone(ctx context.Context, c client.Reader, namespace string, targets []workload) (bool, string, error) {
	for _, t := range targets {
		selector, err := metav1.LabelSelectorAsSelector(t.selector)
		if err != nil {
			return false, "", fmt.Errorf("%s %s has an invalid selector: %w", t.kind, t.object.GetName(), err)
		}
		pods := &corev1.PodList{}
		if err := c.List(ctx, pods, client.InNamespace(namespace), client.MatchingLabelsSelector{Selector: selector}); err != nil {
			return false, "", fmt.Errorf("list the pods of %s %s: %w", t.kind, t.object.GetName(), err)
		}
		for _, pod := range pods.Items {
			if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
				return false, pod.Name, nil
			}
		}
	}
	return true, "", nil
}

// restartWorkloads gives each stopped workload its replica count back, then
// resumes the Kustomizations the run suspended.
//
// Parameters:
//   - namespace is the run's namespace, which holds the stopped workloads.
//   - stopped is the run's status.quiesced, as planStop returned it.
//   - suspended is the run's status.suspendedKustomizations, as
//     "namespace/name" keys.
//
// A workload or Kustomization that has been deleted since is skipped, so a
// second call after a partial failure is safe. So is every Kustomization
// when the API server serves no version of the kind, because Flux's CRDs
// have been removed (see served.Kind). A workload that already stands at its
// recorded count, and a Kustomization that is no longer suspended, are
// skipped as well, so a person who puts the app back by hand while the API
// server refuses the run's own patches lets the run go on. A read that fails
// leaves the patch to decide. It returns the first other error it meets as
// a *restartError, which names the object it could not put back.
//
// The reads are of unstructured objects, which the manager's client sends
// to the API server rather than to its cache (controller-runtime's
// client.CacheOptions.Unstructured is false by default), so they need only
// the get verb and start no informer.
func restartWorkloads(ctx context.Context, c client.Client, namespace string, stopped []backupv1alpha1.QuiescedWorkload, suspended []string) error {
	for _, w := range stopped {
		object := workloadObject(namespace, w)
		if object == nil {
			continue
		}
		if atCount(ctx, c, namespace, w) {
			continue
		}
		if err := scale(ctx, c, object, w.Replicas); err != nil && !apierrors.IsNotFound(err) {
			return &restartError{action: fmt.Sprintf("give %s %s its %d replicas back", w.Kind, w.Name, w.Replicas), err: err}
		}
	}
	for _, key := range suspended {
		ns, name, _ := strings.Cut(key, "/")
		if kustomization, err := getKustomization(ctx, c, c.RESTMapper(), ns, name); err == nil {
			if on, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend"); !on {
				continue
			}
		}
		if err := setSuspend(ctx, c, ns, name, false); err != nil && !apierrors.IsNotFound(err) {
			return &restartError{action: "resume Kustomization " + key, err: err}
		}
	}
	return nil
}

// atCount reports whether the recorded workload w already stands at the
// replica count the run recorded for it. It reads the workload as an
// unstructured object (see restartWorkloads for why) and returns false when
// the read fails or the workload has no spec.replicas.
func atCount(ctx context.Context, c client.Reader, namespace string, w backupv1alpha1.QuiescedWorkload) bool {
	object := &unstructured.Unstructured{}
	object.SetGroupVersionKind(appsv1.SchemeGroupVersion.WithKind(w.Kind))
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: w.Name}, object); err != nil {
		return false
	}
	replicas, found, err := unstructured.NestedInt64(object.Object, "spec", "replicas")
	return err == nil && found && replicas == int64(w.Replicas)
}

// restartError is a failure of restartWorkloads. It says which workload or
// Kustomization the run could not put back, so the run can name it on its
// Ready condition.
type restartError struct {
	// action is what the run could not do, such as "give Deployment notes
	// its 2 replicas back".
	action string

	// err is the error from the API server or the RESTMapper.
	err error
}

// Error returns "could not ", the action, and the error.
func (e *restartError) Error() string { return "could not " + e.action + ": " + e.err.Error() }

// Unwrap returns the error from the API server or the RESTMapper.
func (e *restartError) Unwrap() error { return e.err }

// releaseError is a failure of a step that releases or deletes something a
// run holds: its Leases, its Kueue Workload, or a restore Job it stops (the
// suspend, the delete, and the wait until no pod of the Job can write). The
// step can come before the workloads are back, as a RestoreRun stops its
// movers first, or after them; releasePlan.appDown tells releaseFailure
// which. The error says what the run could not do and what a person can do
// about it, so the run can put both on its Ready condition.
type releaseError struct {
	// action is what the run could not do, such as "release the Leases it
	// holds on its claims and repositories".
	action string

	// advice is one or more sentences that say what a person can do.
	advice string

	// err is the error from the API server or the RESTMapper.
	err error
}

// Error returns "could not ", the action, and the error.
func (e *releaseError) Error() string { return "could not " + e.action + ": " + e.err.Error() }

// Unwrap returns the error from the API server or the RESTMapper.
func (e *releaseError) Unwrap() error { return e.err }

// releasePlan holds what releaseFailure needs from a BackupRun or a
// RestoreRun to pick the Ready reason and write the advice for a failed
// restart or release. The run's releaseFailed fills it from the run's
// status and deletion timestamp.
type releasePlan struct {
	// stopped is the run's status.quiesced, the workloads it may still hold
	// stopped.
	stopped []backupv1alpha1.QuiescedWorkload

	// suspended is the run's status.suspendedKustomizations.
	suspended []string

	// kind is "BackupRun" or "RestoreRun", which the advice names.
	kind string

	// working is true when the run is still working, and false when it is
	// ending or being deleted.
	working bool

	// deleting is true when the run is being deleted.
	deleting bool

	// appDown is true when the run still holds workloads stopped or
	// Kustomizations suspended. A step that fails before the restart, such as
	// a RestoreRun stopping its movers, then reports RestartFailed with the
	// steps that give the app back by hand, because the app is still down.
	appDown bool

	// scheduled is true when a namespace's schedule starts no new run until
	// this one has finished, which is a BackupRun with spec.all set.
	scheduled bool
}

// releaseFailure returns the Ready reason and message for a run that could
// not put back what it changed or release what it holds.
//
// Parameters:
//   - err is the *restartError from restartWorkloads, the *releaseError of a
//     step that releases or deletes something the run holds, both joined
//     with errors.Join, or another error from that work, such as a failed
//     read.
//   - plan is the part of the run's status the message is built from, and
//     says whether the app is still down (see releasePlan).
//
// It returns the reason and the message. The reason is RestartFailed while
// the app is still down, which is after a failed restart and after any
// failure while plan.appDown is set, and also when the run could not tell
// what failed. It is ReleaseFailed when only a release step failed and the
// app is back.
//
// The message names every step that failed, says that the run keeps trying,
// and gives advice that fits. A failed restart names each workload with the
// count it is owed and each Kustomization to resume, which a person can do
// while the run tries again, and restartWorkloads skips what is already
// back. A release step carries its own advice. While the app is down after a
// release step failed, the message adds the same scaling steps, to be taken
// once no mover of the run still writes. A run that is still working says it
// must not be deleted; only a run that is ending or being deleted says that a
// person can delete it, or remove its finalizer, once the app runs again.
// When err holds both a restart and a release failure, as BackupRun.release
// returns them, the message names both and gives both pieces of advice.
func releaseFailure(err error, plan releasePlan) (string, string) {
	reason := backupv1alpha1.ReasonRestartFailed
	var failed, advice []string
	var restart *restartError
	var step *releaseError
	if errors.As(err, &restart) {
		failed = append(failed, restart.Error())
		switch {
		case plan.working:
			advice = append(advice, fmt.Sprintf("The run is still %s; do not delete it. To give the app back now, %s yourself; "+
				"the run then goes on by itself.", stillDoing(plan.kind), byHand(plan.stopped, plan.suspended)))
		case plan.deleting:
			advice = append(advice, fmt.Sprintf("Fix the cause, or %s yourself; the deletion then completes by itself. "+
				"If it still does not once the app runs again, remove the finalizer %s from this %s.",
				byHand(plan.stopped, plan.suspended), Finalizer, plan.kind))
		default:
			advice = append(advice, fmt.Sprintf("Fix the cause, or %s yourself; the run then finishes by itself. "+
				"If it still does not once the app runs again, delete this %s and remove its finalizer %s.",
				byHand(plan.stopped, plan.suspended), plan.kind, Finalizer))
		}
	}
	if errors.As(err, &step) {
		if restart == nil && !plan.appDown {
			reason = backupv1alpha1.ReasonReleaseFailed
		}
		failed = append(failed, step.Error())
		advice = append(advice, step.advice)
	}
	if len(failed) == 0 {
		failed = []string{"could not put back what the run changed: " + err.Error()}
		advice = []string{"Fixing the cause lets the run finish by itself."}
	}
	if restart == nil && plan.appDown {
		advice = append(advice, fmt.Sprintf("The app stays stopped until the run gets past this. To give it back sooner, "+
			"make sure no mover of the run still writes to its claims, then %s yourself.", byHand(plan.stopped, plan.suspended)))
	}
	message := strings.Join(failed, "; it also ") + ". The run retries until it can"
	if plan.scheduled {
		message += ", and this namespace's schedule waits for it"
	}
	message += ". " + strings.Join(advice, " ")
	return reason, message
}

// stillDoing returns what a run of the given kind is still doing while it
// owes the app its workloads, as the advice for a failed restart says it.
func stillDoing(kind string) string {
	if kind == "RestoreRun" {
		return "restoring"
	}
	return "backing up"
}

// byHand returns what a person does to give a run's app back by hand.
//
// Parameters:
//   - stopped is the run's status.quiesced. Each workload is named with the
//     replica count the run recorded for it.
//   - suspended is the run's status.suspendedKustomizations. Each
//     Kustomization is named by its namespace/name key.
//
// It returns a phrase that starts with a verb, such as "scale Deployment
// notes to 2 and resume Kustomization flux-system/notes", and "put the
// workloads back" when the run recorded neither.
func byHand(stopped []backupv1alpha1.QuiescedWorkload, suspended []string) string {
	var steps []string
	var counts []string
	for _, w := range stopped {
		counts = append(counts, fmt.Sprintf("%s %s to %d", w.Kind, w.Name, w.Replicas))
	}
	if len(counts) > 0 {
		steps = append(steps, "scale "+strings.Join(counts, ", "))
	}
	switch len(suspended) {
	case 0:
	case 1:
		steps = append(steps, "resume Kustomization "+suspended[0])
	default:
		steps = append(steps, "resume the Kustomizations "+strings.Join(suspended, ", "))
	}
	if len(steps) == 0 {
		return "put the workloads back"
	}
	return strings.Join(steps, " and ")
}

// scale sets a workload's spec.replicas. It sends a merge patch that names
// only that field, so every other field of the object stays as it is.
func scale(ctx context.Context, c client.Client, object client.Object, replicas int32) error {
	body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	if err := c.Patch(ctx, object, client.RawPatch(types.MergePatchType, []byte(body)), FieldOwner); err != nil {
		return fmt.Errorf("scale %s to %d: %w", object.GetName(), replicas, err)
	}
	return nil
}

// setSuspend sets spec.suspend on one Kustomization with a merge patch, at
// the version the API server serves (see served.Kind).
//
// It returns an error for which apierrors.IsNotFound is true when the
// Kustomization doesn't exist or no version of the kind is served. A patch
// at a version the API server has stopped serving since the client's mapper
// cached it returns a *served.VersionGoneError (see served.VersionGone). Any other failed
// lookup or patch is returned as it is. A patch the API server answers with
// a Kustomization whose spec.suspend is not the value set is an error that
// names the field.
func setSuspend(ctx context.Context, c client.Client, namespace, name string, suspend bool) error {
	gvk, err := served.Kind(c.RESTMapper(), KustomizationGVK.GroupKind())
	if err != nil {
		return fmt.Errorf("set suspend %t on Kustomization %s/%s: %w", suspend, namespace, name, err)
	}
	kustomization := &unstructured.Unstructured{}
	kustomization.SetGroupVersionKind(gvk)
	kustomization.SetNamespace(namespace)
	kustomization.SetName(name)
	body := fmt.Sprintf(`{"spec":{"suspend":%t}}`, suspend)
	if err := c.Patch(ctx, kustomization, client.RawPatch(types.MergePatchType, []byte(body)), FieldOwner); err != nil {
		return fmt.Errorf("set suspend %t on Kustomization %s/%s: %w", suspend, namespace, name, served.VersionGone(c.RESTMapper(), gvk, err))
	}
	// The patch's answer is the Kustomization as stored. A schema without
	// spec.suspend drops the field without an error when the write is not
	// strict, and Flux would then go on reconciling behind a stopped app.
	if stored, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend"); stored != suspend {
		return fmt.Errorf("set suspend %t on Kustomization %s/%s: the Kustomization reads spec.suspend %t after the patch; "+
			"a Flux release may have moved the field (see docs/compatibility.md)", suspend, namespace, name, stored)
	}
	return nil
}
