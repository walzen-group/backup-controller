package main

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeinformers "k8s.io/client-go/informers"
	ctrlcache "sigs.k8s.io/controller-runtime/pkg/cache"
)

// populatorInformerOptions returns the options main hands the populator
// library for its shared informer factory: the transform
// populatorCacheTransform, set on every informer the factory builds.
//
// lib-volume-populator v3.3.0 offers VolumePopulatorConfig's
// SharedInformerOptions for this, to keep its caches small, and leaves it to
// the caller to hold what the library reads (populator-machinery/controller.go
// lines 153-160 and 284).
func populatorInformerOptions() []kubeinformers.SharedInformerOption {
	return []kubeinformers.SharedInformerOption{kubeinformers.WithTransform(populatorCacheTransform)}
}

// populatorCacheTransform returns what the populator library's informers
// keep of an object the API server lists or sends: a Pod as its name,
// namespace, UID and resourceVersion, and any other object without its
// managedFields. It never returns an error.
//
// Parameters:
//   - in is the object the informer received, or anything else, which it
//     returns as it came.
//
// The library watches every Pod, claim, PersistentVolume and StorageClass in
// the cluster (lib-volume-populator v3.3.0 populator-machinery/controller.go
// lines 286-289). In the provider-function mode this binary runs it in, it
// looks a Pod up only when a PodConfig is set (lines 709-711), and its Pod
// handler reads only the name and namespace (lines 492-501 and 527), so the
// rest of each Pod in the cluster held memory for nothing. It reads claims,
// PersistentVolumes and StorageClasses whole, apart from managedFields.
func populatorCacheTransform(in any) (any, error) {
	if pod, ok := in.(*corev1.Pod); ok {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name:            pod.Name,
			Namespace:       pod.Namespace,
			UID:             pod.UID,
			ResourceVersion: pod.ResourceVersion,
		}}, nil
	}
	return ctrlcache.TransformStripManagedFields()(in)
}

// managerCacheOptions returns the cache options of the controller-runtime
// manager: every object it caches drops its managedFields.
//
// The manager watches every Namespace and every claim in the cluster (see
// runs.Scheduler and populator.OrphanReconciler), and no reconciler reads
// managedFields. An update or patch that carries an empty managedFields
// leaves the API server's record of them as it was (the Server-Side Apply
// documentation, "Clearing managedFields"), and controller-runtime offers
// TransformStripManagedFields as a default transform for this (v0.24.1
// pkg/cache/cache.go lines 470-482). The reconcilers read every kind they
// don't watch through the uncached API reader or as unstructured, so those
// reads start no informer and cache nothing.
func managerCacheOptions() ctrlcache.Options {
	return ctrlcache.Options{DefaultTransform: ctrlcache.TransformStripManagedFields()}
}
