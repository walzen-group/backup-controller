package runs

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sort"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// KustomizationGVK is the kind of a Flux Kustomization, the object that
// applies a workload. A run suspends the Kustomization while the workload is
// stopped, so that Flux doesn't scale the workload back up. The run reads
// and writes Kustomizations at the version the API server serves, which
// servedKind looks up; v1 is the version this code was written against.
var KustomizationGVK = schema.GroupVersionKind{Group: "kustomize.toolkit.fluxcd.io", Version: "v1", Kind: "Kustomization"}

// servedKind returns the group, version and kind at which the API server
// serves the kind gk, as the client's RESTMapper looks it up by group and
// kind with no version. A Flux or Kueue release that moves its kinds to a
// new version therefore keeps working.
//
// Parameters:
//   - mapper is the client's RESTMapper, from client.Client.RESTMapper. The
//     manager's mapper asks the API server's discovery when it has not seen
//     the group yet.
//   - gk is the kind to look up.
//
// It returns an error for which apierrors.IsNotFound is true when the API
// server serves no version of the kind. That means the kind's CRD is not
// installed, and deleting a CRD deletes its objects, so no object of the
// kind exists: callers treat it like an object that is gone. A discovery
// call that failed, even one that failed for only some versions, returns
// any other error, which the caller retries with nothing skipped.
func servedKind(mapper meta.RESTMapper, gk schema.GroupKind) (schema.GroupVersionKind, error) {
	mapping, err := mapper.RESTMapping(gk)
	if err == nil {
		return mapping.GroupVersionKind, nil
	}
	var partial *apiutil.ErrResourceDiscoveryFailed
	if meta.IsNoMatchError(err) && !errors.As(err, &partial) {
		return schema.GroupVersionKind{}, &apierrors.StatusError{ErrStatus: metav1.Status{
			Status:  metav1.StatusFailure,
			Code:    http.StatusNotFound,
			Reason:  metav1.StatusReasonNotFound,
			Message: fmt.Sprintf("the API server serves no version of %s", gk),
		}}
	}
	return schema.GroupVersionKind{}, fmt.Errorf("look up the served version of %s: %w", gk, err)
}

// versionGone tells a request the API server refused because it no longer
// serves the version the request was sent at apart from a request for an
// object that doesn't exist. Both come back as NotFound, and only the second
// means the object is gone.
//
// Parameters:
//   - mapper is the RESTMapper the version came from. The manager's mapper
//     caches every version it has looked up and keeps serving it after a
//     Flux or Kueue upgrade stops serving it.
//   - gvk is the group, version and kind the request was sent at.
//   - err is the request's error.
//
// kube-apiserver answers a request for an object that doesn't exist with a
// NotFound Status of its own, and a request at a version it doesn't serve
// with a plain-text "404 page not found" from its not-found handler.
// client-go turns the text answer into a NotFound whose details carry an
// UnexpectedServerResponse cause, which apierrors.IsUnexpectedServerError
// reports (checked against Kubernetes 1.36.4). For that error versionGone
// makes mapper look the kind's versions up again (see rediscover) and returns
// a *versionGoneError, for which apierrors.IsNotFound is false, so the caller
// retries with nothing skipped. It returns any other error, and nil, as it
// is.
func versionGone(mapper meta.RESTMapper, gvk schema.GroupVersionKind, err error) error {
	if !apierrors.IsNotFound(err) || !apierrors.IsUnexpectedServerError(err) {
		return err
	}
	rediscover(mapper, gvk.Group)
	return &versionGoneError{gvk: gvk, err: err}
}

// versionGoneError is a request the API server refused because it no longer
// serves the version the request was sent at. It deliberately has no Unwrap
// method, so that apierrors.IsNotFound doesn't take it for an object that is
// gone.
type versionGoneError struct {
	// gvk is the group, version and kind the request was sent at.
	gvk schema.GroupVersionKind

	// err is the NotFound client-go returned for it.
	err error
}

// Error names the version and says that the controller looks it up again.
func (e *versionGoneError) Error() string {
	return fmt.Sprintf("the API server no longer serves %s at %s (%v); the controller looks up the served version again and retries, "+
		"and a restart of the controller also clears the versions it has cached", e.gvk.Kind, e.gvk.GroupVersion(), e.err)
}

// noSuchKind is a kind name no API group serves. rediscover looks it up to
// make controller-runtime's mapper read a group's discovery again.
const noSuchKind = "BackupControllerNoSuchKind"

// rediscover makes mapper forget the versions of group it has cached, so its
// next lookup asks the API server's discovery again.
//
// A mapper that implements meta.ResettableRESTMapper is reset. The manager's
// mapper from controller-runtime v0.24 has no Reset. For it, rediscover looks
// up a kind the group doesn't have: the mapper answers a kind it can't find
// by reading the discovery of every version of the group it has cached, and
// drops the group from its cache when one of those versions answers 404
// (controller-runtime pkg/client/apiutil/restmapper.go,
// fetchGroupVersionResourcesLocked). The lookup's own error is ignored; a
// failed discovery call leaves the cache as it was, and the caller's retry
// comes back here.
func rediscover(mapper meta.RESTMapper, group string) {
	if resettable, ok := mapper.(meta.ResettableRESTMapper); ok {
		resettable.Reset()
		return
	}
	_, _ = mapper.RESTMapping(schema.GroupKind{Group: group, Kind: noSuchKind})
}

// getKustomization reads one Kustomization at the version the API server
// serves (see servedKind).
//
// Parameters:
//   - reader reads the Kustomization.
//   - mapper looks up the served version.
//   - namespace and name name the Kustomization.
//
// It returns an error for which apierrors.IsNotFound is true when the
// Kustomization doesn't exist or no version of the kind is served. A read at
// a version the API server has stopped serving since mapper cached it
// returns a *versionGoneError (see versionGone). Any other failed lookup or
// read is returned as it is.
func getKustomization(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, namespace, name string) (*unstructured.Unstructured, error) {
	gvk, err := servedKind(mapper, KustomizationGVK.GroupKind())
	if err != nil {
		return nil, err
	}
	kustomization := &unstructured.Unstructured{}
	kustomization.SetGroupVersionKind(gvk)
	if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, kustomization); err != nil {
		return nil, versionGone(mapper, gvk, err)
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
//     Kustomizations (see servedKind).
//   - targets are the workloads to stop, from quiesceTargets or namedTargets.
//
// It returns each workload with the replica count it has now, which is the
// count restartWorkloads gives back, and the Kustomizations to suspend as
// "namespace/name" keys. It returns an error when a Kustomization can't be
// read. On a cluster that serves no version of Kustomization, every
// Kustomization counts as gone.
//
// The kustomize-controller labels on a target name its Kustomization, and
// that Kustomization is suspended only when its status.inventory lists the
// target. Anyone who can edit a workload can set its labels, and suspending a
// Kustomization is a cluster-wide write, so the labels alone would let a
// workload suspend a Kustomization in any namespace. A target without the
// labels, whose Kustomization no longer exists, or whose Kustomization
// doesn't list it is stopped with nothing suspended. A Kustomization that is
// already suspended is left out, because the run didn't suspend it and must
// not resume it. A Kustomization that applies several targets is listed once.
func planStop(ctx context.Context, reader client.Reader, mapper meta.RESTMapper, targets []workload) ([]backupv1alpha1.QuiescedWorkload, []string, error) {
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
		if kustomization == nil || !inventoryLists(kustomization, t) || slices.Contains(suspend, key) {
			continue
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
//     Kustomizations (see servedKind).
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
// have been removed (see servedKind). It returns the first other error it
// meets as a *restartError, which names the object it could not put back.
func restartWorkloads(ctx context.Context, c client.Client, namespace string, stopped []backupv1alpha1.QuiescedWorkload, suspended []string) error {
	for _, w := range stopped {
		object := workloadObject(namespace, w)
		if object == nil {
			continue
		}
		if err := scale(ctx, c, object, w.Replicas); err != nil && !apierrors.IsNotFound(err) {
			return &restartError{action: fmt.Sprintf("give %s %s its %d replicas back", w.Kind, w.Name, w.Replicas), err: err}
		}
	}
	for _, key := range suspended {
		ns, name, _ := strings.Cut(key, "/")
		if err := setSuspend(ctx, c, ns, name, false); err != nil && !apierrors.IsNotFound(err) {
			return &restartError{action: "resume Kustomization " + key, err: err}
		}
	}
	return nil
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
// the version the API server serves (see servedKind).
//
// It returns an error for which apierrors.IsNotFound is true when the
// Kustomization doesn't exist or no version of the kind is served. A patch
// at a version the API server has stopped serving since the client's mapper
// cached it returns a *versionGoneError (see versionGone). Any other failed
// lookup or patch is returned as it is.
func setSuspend(ctx context.Context, c client.Client, namespace, name string, suspend bool) error {
	gvk, err := servedKind(c.RESTMapper(), KustomizationGVK.GroupKind())
	if err != nil {
		return fmt.Errorf("set suspend %t on Kustomization %s/%s: %w", suspend, namespace, name, err)
	}
	kustomization := &unstructured.Unstructured{}
	kustomization.SetGroupVersionKind(gvk)
	kustomization.SetNamespace(namespace)
	kustomization.SetName(name)
	body := fmt.Sprintf(`{"spec":{"suspend":%t}}`, suspend)
	if err := c.Patch(ctx, kustomization, client.RawPatch(types.MergePatchType, []byte(body)), FieldOwner); err != nil {
		return fmt.Errorf("set suspend %t on Kustomization %s/%s: %w", suspend, namespace, name, versionGone(c.RESTMapper(), gvk, err))
	}
	return nil
}
