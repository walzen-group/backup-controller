package runs

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// KustomizationGVK is the kind of a Flux Kustomization, the object that
// applies a workload. A run suspends the Kustomization while the workload is
// stopped, so that Flux doesn't scale the workload back up.
var KustomizationGVK = schema.GroupVersionKind{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization"}

// fluxNameLabel and fluxNamespaceLabel are the labels kustomize-controller
// writes on every object it applies. Together they name the Kustomization
// that applied the object, which is the one stopWorkloads suspends.
const (
	fluxNameLabel      = "kustomize.toolkit.fluxcd.io/name"
	fluxNamespaceLabel = "kustomize.toolkit.fluxcd.io/namespace"
)

// workload is one Deployment or StatefulSet that a run stops while it works.
type workload struct {
	// kind is "Deployment" or "StatefulSet".
	kind string

	// object is the workload as read from the API server.
	object client.Object

	// replicas is the count to give back when the run resumes the workload.
	// An unset spec.replicas counts as 1, which is the Kubernetes default.
	replicas int32

	// selector matches the workload's pods, so that podsGone can wait for
	// them to exit.
	selector *metav1.LabelSelector
}

// pauseTargets lists the Deployments and StatefulSets in a namespace that carry
// the annotation backup.wlz.li/pause-during-backup: "true". A BackupRun stops
// these while VolSync clones the volumes. The list is sorted by kind, then by
// name.
//
// It returns an error when either list call fails.
func pauseTargets(ctx context.Context, c client.Reader, namespace string) ([]workload, error) {
	marked := func(o metav1.Object) bool {
		return o.GetAnnotations()[backupv1alpha1.AnnotationPauseDuringBackup] == "true"
	}
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

// namedTargets reads the workloads that a RestoreRun's spec.pauseDuringRestore
// lists, in the order the list gives them.
//
// Parameters:
//   - namespace is the RestoreRun's namespace. Each workload is looked up there.
//   - refs is the run's spec.pauseDuringRestore.
//
// It returns a *pauseSpecError when an entry has a kind other than Deployment
// or StatefulSet, or names a workload the namespace doesn't hold. The error
// names the entry. Any other failed read comes back wrapped as it is, and the
// caller retries it. A RestoreRun calls this while it plans, before it stops
// anything, so a mistake in spec.pauseDuringRestore fails the run with nothing
// changed.
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
			return nil, &pauseSpecError{fmt.Sprintf("spec.pauseDuringRestore lists %s %s; only a Deployment or a StatefulSet can be stopped", ref.Kind, ref.Name)}
		}
	}
	return targets, nil
}

// pauseSpecError is an entry in a RestoreRun's spec.pauseDuringRestore that the
// run can't act on: a kind it can't stop, or a workload the namespace doesn't
// hold. Reading again won't change it, so the run ends. A failed read of the
// API server is returned as a plain error, and the run retries it.
type pauseSpecError struct{ message string }

func (e *pauseSpecError) Error() string { return e.message }

// isPauseSpecError reports whether err, or an error it wraps, is a
// *pauseSpecError.
func isPauseSpecError(err error) bool {
	var bad *pauseSpecError
	return errors.As(err, &bad)
}

// missingWorkload turns the error from reading a spec.pauseDuringRestore entry
// into the error the RestoreRun reports. A NotFound error becomes a
// *pauseSpecError saying the namespace holds no such workload. Any other error
// is wrapped as it is.
func missingWorkload(ref backupv1alpha1.WorkloadRef, err error) error {
	if apierrors.IsNotFound(err) {
		return &pauseSpecError{fmt.Sprintf("spec.pauseDuringRestore lists %s %s, which this namespace does not hold", ref.Kind, ref.Name)}
	}
	return fmt.Errorf("get %s %s: %w", ref.Kind, ref.Name, err)
}

// pausePlan works out what pausing the target workloads changes, without
// changing anything, so the run can record it before it acts.
//
// Parameters:
//   - reader reads each Kustomization's current spec.suspend.
//   - targets are the workloads to pause, from pauseTargets or namedTargets.
//
// It returns each workload with the replica count it has now, and the Flux
// Kustomizations to suspend as "namespace/name" keys, so Flux can't scale the
// workloads back up. A target without the kustomize-controller labels, or
// whose Kustomization no longer exists, gets nothing suspended. A
// Kustomization that is already suspended is left out, because the run did
// not suspend it and must not resume it. A Kustomization that applies several
// targets is listed once. It returns an error when a Kustomization can't be
// read.
func pausePlan(ctx context.Context, reader client.Reader, targets []workload) ([]backupv1alpha1.PausedWorkload, []string, error) {
	var suspend []string
	seen := map[string]bool{}
	for _, t := range targets {
		name := t.object.GetLabels()[fluxNameLabel]
		namespace := t.object.GetLabels()[fluxNamespaceLabel]
		if name == "" || namespace == "" {
			continue
		}
		key := namespace + "/" + name
		if seen[key] {
			continue
		}
		seen[key] = true

		kustomization := &unstructured.Unstructured{}
		kustomization.SetGroupVersionKind(KustomizationGVK)
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, kustomization); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return nil, nil, fmt.Errorf("get Kustomization %s: %w", key, err)
		}
		if already, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend"); already {
			continue
		}
		suspend = append(suspend, key)
	}

	paused := make([]backupv1alpha1.PausedWorkload, 0, len(targets))
	for _, t := range targets {
		paused = append(paused, backupv1alpha1.PausedWorkload{Kind: t.kind, Name: t.object.GetName(), Replicas: t.replicas})
	}
	return paused, suspend, nil
}

// applyPause suspends the Kustomizations and scales the workloads to zero
// that the run recorded from pausePlan.
//
// Parameters:
//   - namespace is the run's namespace, which holds the workloads.
//   - paused is the run's status.paused.
//   - suspend is the run's status.suspendedKustomizations.
//
// It suspends first, so Flux can't scale a workload back up. Every patch sets
// a fixed value, so a pass that repeats applyPause after a crash changes
// nothing that is already paused. A workload or Kustomization that is gone is
// skipped. It returns the first other error.
func applyPause(ctx context.Context, c client.Client, namespace string, paused []backupv1alpha1.PausedWorkload, suspend []string) error {
	for _, key := range suspend {
		ns, name, _ := strings.Cut(key, "/")
		if err := setSuspend(ctx, c, ns, name, true); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	for _, w := range paused {
		object, ok := workloadObject(namespace, w)
		if !ok {
			continue
		}
		if err := scale(ctx, c, object, 0); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// workloadObject returns an empty object of a paused workload's kind with its
// namespace and name set, for a patch. It returns false for a kind other than
// Deployment and StatefulSet.
func workloadObject(namespace string, w backupv1alpha1.PausedWorkload) (client.Object, bool) {
	var object client.Object
	switch w.Kind {
	case "Deployment":
		object = &appsv1.Deployment{}
	case "StatefulSet":
		object = &appsv1.StatefulSet{}
	default:
		return nil, false
	}
	object.SetNamespace(namespace)
	object.SetName(w.Name)
	return object, true
}

// podsGone reports whether every pod of the target workloads has gone. A
// terminating pod still counts, because a pod that is shutting down can still
// write to the volume.
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
		if len(pods.Items) > 0 {
			return false, pods.Items[0].Name, nil
		}
	}
	return true, "", nil
}

// resumeWorkloads gives each paused workload its replica count back, then
// resumes the Kustomizations the run suspended.
//
// Parameters:
//   - namespace is the run's namespace, which holds the paused workloads.
//   - paused is the run's status.paused. resumeWorkloads marks each workload
//     Resumed once it is scaled back, and skips a workload already marked,
//     so it never scales a workload again that its owner scaled since.
//   - suspended is the run's status.suspendedKustomizations.
//
// The caller records status.resumedAt before it calls resumeWorkloads, and
// writes the marks afterwards. A workload or Kustomization that has been
// deleted since is skipped, so a second call after a partial failure is safe.
// It returns the first other error.
func resumeWorkloads(ctx context.Context, c client.Client, namespace string, paused []backupv1alpha1.PausedWorkload, suspended []string) error {
	for i := range paused {
		w := &paused[i]
		if w.Resumed {
			continue
		}
		object, ok := workloadObject(namespace, *w)
		if ok {
			if err := scale(ctx, c, object, w.Replicas); err != nil && !apierrors.IsNotFound(err) {
				return err
			}
		}
		w.Resumed = true
	}
	for _, key := range suspended {
		ns, name, _ := strings.Cut(key, "/")
		if err := setSuspend(ctx, c, ns, name, false); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
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

// setSuspend sets spec.suspend on one Kustomization with a merge patch.
func setSuspend(ctx context.Context, c client.Client, namespace, name string, suspend bool) error {
	kustomization := &unstructured.Unstructured{}
	kustomization.SetGroupVersionKind(KustomizationGVK)
	kustomization.SetNamespace(namespace)
	kustomization.SetName(name)
	body := fmt.Sprintf(`{"spec":{"suspend":%t}}`, suspend)
	if err := c.Patch(ctx, kustomization, client.RawPatch(types.MergePatchType, []byte(body)), FieldOwner); err != nil {
		return fmt.Errorf("set suspend %t on Kustomization %s/%s: %w", suspend, namespace, name, err)
	}
	return nil
}

// pausedPodsGone reports whether every pod of the workloads a run paused is
// gone, so nothing writes to their volumes any more.
//
// Parameters:
//   - namespace is the run's namespace.
//   - paused is the run's status.paused.
//
// It reads each workload for its pod selector and lists the pods that match.
// A workload deleted since the pause has no pods to wait for. It returns
// false and the name of a pod that is still there, or an error when a read
// fails.
func pausedPodsGone(ctx context.Context, c client.Reader, namespace string, paused []backupv1alpha1.PausedWorkload) (bool, string, error) {
	var targets []workload
	for _, w := range paused {
		found, err := namedTargets(ctx, c, namespace, []backupv1alpha1.WorkloadRef{{Kind: w.Kind, Name: w.Name}})
		if isPauseSpecError(err) {
			continue
		}
		if err != nil {
			return false, "", err
		}
		targets = append(targets, found...)
	}
	return podsGone(ctx, c, namespace, targets)
}
