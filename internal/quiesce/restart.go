package quiesce

import (
	"context"
	"fmt"
	"strings"

	appsv1 "k8s.io/api/apps/v1"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Restart gives each stopped workload its replica count back, then
// resumes the Kustomizations the run suspended.
//
// Parameters:
//   - namespace is the run's namespace, which holds the stopped workloads.
//   - stopped is the run's status.quiesced, as Plan returned it.
//   - suspended is the run's status.suspendedKustomizations, as
//     "namespace/name" keys.
//
// A workload or Kustomization that has been deleted since is skipped, so a
// second call after a partial failure is safe. So is every Kustomization
// when the API server serves no version of the kind, because Flux's CRDs
// have been removed (see served.Kind). A workload that already stands at its
// recorded count, and a Kustomization that is no longer suspended, are
// skipped as well, so a person who puts the app back by hand while the API
// server refuses the run's own writes lets the run go on. A read that fails
// leaves the write to decide. It returns the first other error it meets as
// a *RestartError, which names the object it could not put back.
//
// The reads are of unstructured objects, which the manager's client sends
// to the API server rather than to its cache (controller-runtime's
// client.CacheOptions.Unstructured is false by default), so they need only
// the get verb and start no informer.
func Restart(ctx context.Context, c client.Client, namespace string, stopped []backupv1alpha1.QuiescedWorkload, suspended []string) error {
	if err := giveBack(ctx, c, namespace, stopped); err != nil {
		return err
	}
	return resume(ctx, c, suspended)
}

// giveBack gives each stopped workload its recorded replica count back.
//
// Parameters:
//   - namespace is the run's namespace, which holds the stopped workloads.
//   - stopped is the run's status.quiesced.
//
// It returns nil when each workload has its count or does not exist. It
// returns a *RestartError for the first scale that fails for a different
// cause. It skips a workload of an unknown kind and a workload that has its
// count already (see atCount).
func giveBack(ctx context.Context, c client.Client, namespace string, stopped []backupv1alpha1.QuiescedWorkload) error {
	for _, w := range stopped {
		object := workloadObject(namespace, w)
		if object == nil {
			continue
		}
		if atCount(ctx, c, namespace, w) {
			continue
		}
		if err := scale(ctx, c, object, w.Replicas); err != nil && !apierrors.IsNotFound(err) {
			return &RestartError{action: fmt.Sprintf("give %s %s its %d replicas back", w.Kind, w.Name, w.Replicas), err: err}
		}
	}
	return nil
}

// resume resumes each Kustomization that the run suspended.
//
// Parameters:
//   - suspended is the run's status.suspendedKustomizations, as
//     "namespace/name" keys.
//
// It returns nil when it resumes each Kustomization or the Kustomization does
// not exist. It returns a *RestartError for the first resume that fails for a
// different cause. It skips a Kustomization that it reads with spec.suspend
// false. When the read fails, the write decides.
func resume(ctx context.Context, c client.Client, suspended []string) error {
	for _, key := range suspended {
		ns, name, _ := strings.Cut(key, "/")
		if kustomization, err := getKustomization(ctx, c, c.RESTMapper(), ns, name); err == nil {
			if on, _, _ := unstructured.NestedBool(kustomization.Object, "spec", "suspend"); !on {
				continue
			}
		}
		if err := SetSuspend(ctx, c, ns, name, false); err != nil && !apierrors.IsNotFound(err) {
			return &RestartError{action: "resume Kustomization " + key, err: err}
		}
	}
	return nil
}

// atCount reports whether the recorded workload w already stands at the
// replica count the run recorded for it. It reads the workload as an
// unstructured object (see Restart for why) and returns false when
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
