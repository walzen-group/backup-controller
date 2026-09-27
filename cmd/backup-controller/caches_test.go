package main

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	kubeinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	toolscache "k8s.io/client-go/tools/cache"
)

// managedBy is a managedFields entry of the size a real object carries a few
// of, which neither cache reads.
var managedBy = []metav1.ManagedFieldsEntry{{
	Manager:    "kubectl",
	Operation:  metav1.ManagedFieldsOperationApply,
	APIVersion: "v1",
	FieldsType: "FieldsV1",
	FieldsV1:   metav1.NewFieldsV1(`{"f:spec":{"f:containers":{}}}`),
}}

// cachedPod returns a Pod the way the API server lists one: labels,
// annotations, managedFields, a spec and a status.
func cachedPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "app-0", Namespace: "notes", UID: types.UID("pod-uid"), ResourceVersion: "42",
			Labels:        map[string]string{"app": "notes"},
			Annotations:   map[string]string{"note": "kept by the API server"},
			ManagedFields: managedBy,
		},
		Spec:   corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "notes:1"}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

// cachedClaim returns a claim the way the API server lists one, with the
// annotations, spec and status the populator library reads.
func cachedClaim() *corev1.PersistentVolumeClaim {
	class := "e2e-hostpath"
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data", Namespace: "notes", ResourceVersion: "7",
			Annotations:   map[string]string{"volume.kubernetes.io/selected-node": "worker"},
			ManagedFields: managedBy,
		},
		Spec:   corev1.PersistentVolumeClaimSpec{StorageClassName: &class, VolumeName: "pv-1"},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

// transformed runs a transform over an object and fails the test when it
// returns an error.
func transformed(t *testing.T, transform toolscache.TransformFunc, in runtime.Object) any {
	t.Helper()
	out, err := transform(in)
	if err != nil {
		t.Fatalf("transform: %v", err)
	}
	return out
}

// TestThePopulatorCachesPodsByNameOnly checks that the populator library's
// informers keep a Pod as its name, namespace, UID and resourceVersion.
//
// The library watches every Pod in the cluster. In provider-function mode
// it reads none of them: it looks a Pod up only with a PodConfig, and its
// Pod handler uses the name and namespace alone. The rest of each Pod held
// memory for nothing.
func TestThePopulatorCachesPodsByNameOnly(t *testing.T) {
	got, _ := populatorCached(t)
	want := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "app-0", Namespace: "notes", UID: "pod-uid", ResourceVersion: "42"}}
	if !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("the populator caches the Pod as %+v, want %+v", got, want)
	}
}

// TestThePopulatorCachesClaimsWhole checks that the populator library's
// informers keep a claim's labels, annotations, spec and status, and drop
// only its managedFields, which the library never reads.
func TestThePopulatorCachesClaimsWhole(t *testing.T) {
	_, got := populatorCached(t)
	want := cachedClaim()
	want.ManagedFields = nil
	if !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("the populator caches the claim as %+v, want %+v", got, want)
	}
}

// TestTheManagersCacheDropsManagedFields checks that the manager's cache
// drops managedFields from every object it holds and keeps the rest.
//
// The manager watches every Namespace and every claim in the cluster, and no
// reconciler reads managedFields. An update sent with no managedFields
// leaves the API server's record of them as it was.
func TestTheManagersCacheDropsManagedFields(t *testing.T) {
	transform := managerOptions(nil, "0", "0", BootstrapWebhook{}).Cache.DefaultTransform
	if transform == nil {
		t.Fatal("the manager's cache has no default transform")
	}
	got := transformed(t, transform, cachedPod())
	want := cachedPod()
	want.ManagedFields = nil
	if !equality.Semantic.DeepEqual(got, want) {
		t.Errorf("the manager caches the Pod as %+v, want %+v", got, want)
	}
}

// populatorCached returns the Pod and the claim as the populator library's
// informers hold them: it runs a shared informer factory built with
// populatorInformerOptions, as the library builds its own, over a fake
// clientset that serves cachedPod and cachedClaim, and reads both back from
// the listers.
func populatorCached(t *testing.T) (*corev1.Pod, *corev1.PersistentVolumeClaim) {
	t.Helper()
	clientset := fake.NewClientset(cachedPod(), cachedClaim())
	factory := kubeinformers.NewSharedInformerFactoryWithOptions(clientset, 0, populatorInformerOptions()...)
	pods := factory.Core().V1().Pods().Lister()
	claims := factory.Core().V1().PersistentVolumeClaims().Lister()
	stop := make(chan struct{})
	t.Cleanup(func() { close(stop) })
	factory.Start(stop)
	for informer, synced := range factory.WaitForCacheSync(stop) {
		if !synced {
			t.Fatalf("the %v informer did not sync", informer)
		}
	}
	pod, err := pods.Pods("notes").Get("app-0")
	if err != nil {
		t.Fatalf("read the cached Pod: %v", err)
	}
	claim, err := claims.PersistentVolumeClaims("notes").Get("data")
	if err != nil {
		t.Fatalf("read the cached claim: %v", err)
	}
	return pod, claim
}
