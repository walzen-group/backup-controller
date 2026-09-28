// Package quiesce finds, stops and starts again the workloads of a run, and
// suspends and resumes the Flux Kustomizations that apply them. The
// reconcilers in internal/runs keep every decision about a run: the plan they
// record, the quiesce Lease and the reason a run ends with.
package quiesce

import (
	"context"
	"fmt"
	"sort"

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

// fieldOwner is the field manager name, backupv1alpha1.FieldManager, that
// the package sends with its writes to workloads and Kustomizations.
const fieldOwner = client.FieldOwner(backupv1alpha1.FieldManager)

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

// FluxNameLabel and FluxNamespaceLabel are the labels kustomize-controller
// writes on every object it applies. Together they name the Kustomization
// that applied the object. Plan suspends that Kustomization when its
// inventory lists the object.
const (
	FluxNameLabel      = "kustomize.toolkit.fluxcd.io/name"
	FluxNamespaceLabel = "kustomize.toolkit.fluxcd.io/namespace"
)

// Workload is one Deployment or StatefulSet that a run stops while it works.
type Workload struct {
	// kind is "Deployment" or "StatefulSet".
	kind string

	// object is the workload as read from the API server.
	object client.Object

	// replicas is the count to give back when the run restarts the workload.
	// An unset spec.replicas counts as 1, which is the Kubernetes default.
	replicas int32

	// selector matches the workload's pods, so that PodsGone can wait for
	// them to exit.
	selector *metav1.LabelSelector
}

// Targets lists the Deployments and StatefulSets in a namespace that
// carry the annotation backup.wlz.li/quiesce: "true". A BackupRun stops these
// while VolSync clones the volumes. The list is sorted by kind, then by name.
//
// It returns an error when either list call fails.
func Targets(ctx context.Context, c client.Reader, namespace string) ([]Workload, error) {
	deployments := &appsv1.DeploymentList{}
	if err := c.List(ctx, deployments, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the Deployments in %s: %w", namespace, err)
	}
	statefulSets := &appsv1.StatefulSetList{}
	if err := c.List(ctx, statefulSets, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the StatefulSets in %s: %w", namespace, err)
	}
	var objects []client.Object
	for i := range deployments.Items {
		objects = append(objects, &deployments.Items[i])
	}
	for i := range statefulSets.Items {
		objects = append(objects, &statefulSets.Items[i])
	}

	var targets []Workload
	for _, object := range objects {
		if object.GetAnnotations()[backupv1alpha1.AnnotationQuiesce] == "true" {
			targets = append(targets, workloadOf(object))
		}
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].kind+"/"+targets[i].object.GetName() < targets[j].kind+"/"+targets[j].object.GetName()
	})
	return targets, nil
}

// Named reads the workloads that a RestoreRun's spec.quiesce lists, in
// the order the list gives them.
//
// Parameters:
//   - namespace is the RestoreRun's namespace. Each workload is looked up there.
//   - refs is the run's spec.quiesce.
//
// It returns a *SpecError when an entry has a kind other than
// Deployment or StatefulSet, or names a workload the namespace doesn't hold.
// The error names the entry. Any other failed read comes back wrapped as it
// is, and the caller retries it. A RestoreRun calls this while it plans,
// before it stops anything, so a mistake in spec.quiesce fails the run with
// nothing changed.
func Named(ctx context.Context, c client.Reader, namespace string, refs []backupv1alpha1.WorkloadRef) ([]Workload, error) {
	var targets []Workload
	for _, ref := range refs {
		object := workloadObject(namespace, backupv1alpha1.QuiescedWorkload{Kind: ref.Kind, Name: ref.Name})
		if object == nil {
			return nil, &SpecError{Ref: ref, problem: specKindUnsupported}
		}
		if err := c.Get(ctx, client.ObjectKeyFromObject(object), object); err != nil {
			return nil, missingWorkload(ref, err)
		}
		targets = append(targets, workloadOf(object))
	}
	return targets, nil
}

// workloadOf returns the Workload of a Deployment or a StatefulSet that was
// read from the API server. An unset spec.replicas counts as 1, which is the
// Kubernetes default.
func workloadOf(object client.Object) Workload {
	w := Workload{object: object}
	var replicas *int32
	switch o := object.(type) {
	case *appsv1.Deployment:
		w.kind = backupv1alpha1.WorkloadKindDeployment
		replicas = o.Spec.Replicas
		w.selector = o.Spec.Selector
	case *appsv1.StatefulSet:
		w.kind = backupv1alpha1.WorkloadKindStatefulSet
		replicas = o.Spec.Replicas
		w.selector = o.Spec.Selector
	}
	w.replicas = 1
	if replicas != nil {
		w.replicas = *replicas
	}
	return w
}

// missingWorkload turns the error from reading a spec.quiesce entry into the
// error the RestoreRun reports. A NotFound error becomes a *SpecError
// saying the namespace holds no such workload. Any other error is wrapped as
// it is.
func missingWorkload(ref backupv1alpha1.WorkloadRef, err error) error {
	if apierrors.IsNotFound(err) {
		return &SpecError{Ref: ref, problem: specWorkloadMissing}
	}
	return fmt.Errorf("get %s %s: %w", ref.Kind, ref.Name, err)
}

// PodsGone reports whether every pod of the target workloads has gone. A
// terminating pod still counts, because a pod that is shutting down can still
// write to the volume. A pod in phase Succeeded or Failed doesn't count: its
// containers have ended, and an evicted pod stays in that phase until
// something deletes it.
//
// While a pod is left, it returns false and that pod's name, which the caller
// puts in the message it waits with. It returns an error when a target's
// selector is invalid or its pods can't be listed.
func PodsGone(ctx context.Context, c client.Reader, namespace string, targets []Workload) (bool, string, error) {
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
