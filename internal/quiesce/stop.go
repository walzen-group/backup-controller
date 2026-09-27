package quiesce

import (
	"context"
	"fmt"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Apply carries out a plan from Plan. It suspends the Kustomizations
// first, so that Flux can't scale the workloads back up, then scales each
// workload to zero. Both writes set a fixed value, so calling it again with
// the same plan changes nothing more.
//
// Parameters:
//   - namespace is the run's namespace, which holds the workloads.
//   - stop is the run's status.quiesced.
//   - suspend is the run's status.suspendedKustomizations.
//
// It returns the first error it meets and leaves the rest undone. The caller
// then narrows the plan with Applied before it restarts the workloads.
func Apply(ctx context.Context, c client.Client, namespace string, stop []backupv1alpha1.QuiescedWorkload, suspend []string) error {
	for _, key := range suspend {
		ns, name, _ := strings.Cut(key, "/")
		if err := SetSuspend(ctx, c, ns, name, true); err != nil {
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

// Applied returns the part of a stop plan that is in effect now. A run
// calls it after Apply failed, so that the restart that follows only
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
func Applied(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, namespace string, stop []backupv1alpha1.QuiescedWorkload, suspend []string) ([]backupv1alpha1.QuiescedWorkload, []string) {
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
// namespace and name of a recorded workload, ready for a scale. It returns
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

// scale sets a workload's replica count through its scale subresource, the
// only write the controller makes to a Deployment or a StatefulSet.
//
// Parameters:
//   - c is the client that reads and writes the subresource. The manager's
//     client sends subresource requests straight to the API server.
//   - object is an empty Deployment or StatefulSet that carries the
//     workload's namespace and name (see workloadObject). It is only read.
//   - replicas is the count to set: 0 to stop the workload, or the count
//     the run recorded to give it back.
//
// It returns nil once the API server has stored the count. A workload that
// does not exist is an error for which apierrors.IsNotFound is true, so a
// restart can skip it. Any other refusal is returned wrapped, with the
// workload's name and the count.
//
// It reads the Scale, sets spec.replicas, clears the resourceVersion and
// writes the Scale back with the controller's field manager. A Scale
// without a resourceVersion is an unconditional update, which both kinds
// allow (AllowUnconditionalUpdate in the Deployment and StatefulSet
// strategies of Kubernetes 1.36), so a status write by the workload's
// controller between the read and the write does not fail the scale. The
// API server copies only spec.replicas from the Scale into the object it
// holds, so no other field can be overwritten. The Scale keeps the UID it
// read, and a workload deleted and created again in between is a Conflict.
// The subresource can change nothing but the replica count, which is why
// the ClusterRole grants no write verb on the workloads themselves.
func scale(ctx context.Context, c client.Client, object client.Object, replicas int32) error {
	current := &autoscalingv1.Scale{}
	err := c.SubResource("scale").Get(ctx, object, current)
	if err == nil {
		current.Spec.Replicas = replicas
		current.ResourceVersion = ""
		err = c.SubResource("scale").Update(ctx, object, client.WithSubResourceBody(current), fieldOwner)
	}
	if err != nil {
		return fmt.Errorf("scale %s to %d: %w", object.GetName(), replicas, err)
	}
	return nil
}

// SetSuspend sets spec.suspend on one Kustomization with a merge patch, at
// the version the API server serves (see served.Kind).
//
// It returns an error for which apierrors.IsNotFound is true when the
// Kustomization doesn't exist or no version of the kind is served. A patch
// at a version the API server has stopped serving since the client's mapper
// cached it returns a *served.VersionGoneError (see served.VersionGone). Any other failed
// lookup or patch is returned as it is. A patch the API server answers with
// a Kustomization whose spec.suspend is not the value set is an error that
// names the field.
func SetSuspend(ctx context.Context, c client.Client, namespace, name string, suspend bool) error {
	gvk, err := served.Kind(c.RESTMapper(), KustomizationGVK.GroupKind())
	if err != nil {
		return fmt.Errorf("set suspend %t on Kustomization %s/%s: %w", suspend, namespace, name, err)
	}
	kustomization := &unstructured.Unstructured{}
	kustomization.SetGroupVersionKind(gvk)
	kustomization.SetNamespace(namespace)
	kustomization.SetName(name)
	body := fmt.Sprintf(`{"spec":{"suspend":%t}}`, suspend)
	if err := c.Patch(ctx, kustomization, client.RawPatch(types.MergePatchType, []byte(body)), fieldOwner); err != nil {
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
