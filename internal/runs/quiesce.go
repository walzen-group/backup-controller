package runs

import (
	"context"
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

// KustomizationGVK is the Flux kind that applies a workload. The run suspends
// it for the length of a quiesce, so Flux does not put the replicas back.
var KustomizationGVK = schema.GroupVersionKind{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization"}

// The labels kustomize-controller writes on every object it applies.
const (
	fluxNameLabel      = "kustomize.toolkit.fluxcd.io/name"
	fluxNamespaceLabel = "kustomize.toolkit.fluxcd.io/namespace"
)

// workload is one Deployment or StatefulSet marked for quiesce.
type workload struct {
	kind     string
	object   client.Object
	replicas int32
	selector *metav1.LabelSelector
}

// quiesceTargets lists the workloads in a namespace marked
// backup.wlz.li/quiesce, by kind and name.
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

// stopWorkloads suspends the Flux Kustomizations that apply the targets, then
// scales each target to zero, and returns what it changed so it can be put
// back. A Kustomization already suspended is left out of the list: the run
// did not suspend it, so it must not resume it.
func stopWorkloads(ctx context.Context, c client.Client, reader client.Reader, targets []workload) ([]backupv1alpha1.QuiescedWorkload, []string, error) {
	var suspended []string
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
			return nil, suspended, fmt.Errorf("get Kustomization %s: %w", key, err)
		}
		if already, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend"); already {
			continue
		}
		if err := setSuspend(ctx, c, namespace, name, true); err != nil {
			return nil, suspended, err
		}
		suspended = append(suspended, key)
	}

	var stopped []backupv1alpha1.QuiescedWorkload
	for _, t := range targets {
		if err := scale(ctx, c, t.object, 0); err != nil {
			return stopped, suspended, err
		}
		stopped = append(stopped, backupv1alpha1.QuiescedWorkload{Kind: t.kind, Name: t.object.GetName(), Replicas: t.replicas})
	}
	return stopped, suspended, nil
}

// podsGone reports whether no pod of the targets is left, terminating ones
// included: a pod shutting down can still write to the volume.
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

// restartWorkloads gives each stopped workload its replicas back and resumes
// the Kustomizations the run suspended.
func restartWorkloads(ctx context.Context, c client.Client, namespace string, stopped []backupv1alpha1.QuiescedWorkload, suspended []string) error {
	for _, w := range stopped {
		var object client.Object
		switch w.Kind {
		case "Deployment":
			object = &appsv1.Deployment{}
		case "StatefulSet":
			object = &appsv1.StatefulSet{}
		default:
			continue
		}
		object.SetNamespace(namespace)
		object.SetName(w.Name)
		if err := scale(ctx, c, object, w.Replicas); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	for _, key := range suspended {
		ns, name, _ := strings.Cut(key, "/")
		if err := setSuspend(ctx, c, ns, name, false); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}

// scale sets spec.replicas with a merge patch naming that field alone.
func scale(ctx context.Context, c client.Client, object client.Object, replicas int32) error {
	body := fmt.Sprintf(`{"spec":{"replicas":%d}}`, replicas)
	if err := c.Patch(ctx, object, client.RawPatch(types.MergePatchType, []byte(body)), FieldOwner); err != nil {
		return fmt.Errorf("scale %s to %d: %w", object.GetName(), replicas, err)
	}
	return nil
}

// setSuspend sets a Kustomization's spec.suspend.
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
