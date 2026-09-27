package bootstrap

import (
	"context"
	"encoding/json"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// nextVersion is the version a future CloudNativePG and barman-cloud release
// is imagined to move Cluster and ObjectStore to, with the same fields.
const nextVersion = "v2"

// servedAtNext returns a scheme and a RESTMapper for a cluster that serves
// Cluster and ObjectStore at nextVersion alone. The fake client knows no
// other version of either kind, so a request at v1 fails as it would on
// such a cluster.
func servedAtNext(t *testing.T) (*runtime.Scheme, meta.RESTMapper) {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	if err := backupv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	cnpg := schema.GroupVersion{Group: ClusterListGVK.Group, Version: nextVersion}
	barman := schema.GroupVersion{Group: ObjectStoreGVK.Group, Version: nextVersion}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{corev1.SchemeGroupVersion, backupv1alpha1.GroupVersion, cnpg, barman})
	for _, gvk := range []schema.GroupVersionKind{cnpg.WithKind("Cluster"), barman.WithKind("ObjectStore")} {
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	for gvk := range s.AllKnownTypes() {
		if gvk.Group == "" || gvk.Group == backupv1alpha1.GroupVersion.Group {
			mapper.Add(gvk, meta.RESTScopeNamespace)
		}
	}
	return s, mapper
}

// decideAtNext sends a create of the Cluster app/app-pg to a Decider on a
// cluster that serves Cluster and ObjectStore at nextVersion alone, with
// store() and secret() and the objects in existing, all moved to
// nextVersion where they are Clusters or ObjectStores.
func decideAtNext(t *testing.T, prober ArchiveProber, existing ...*unstructured.Unstructured) admission.Response {
	t.Helper()
	s, mapper := servedAtNext(t)
	objects := []runtime.Object{secret()}
	for _, u := range append([]*unstructured.Unstructured{store()}, existing...) {
		u.SetGroupVersionKind(u.GroupVersionKind().GroupKind().WithVersion(nextVersion))
		objects = append(objects, u)
	}
	c := fake.NewClientBuilder().WithScheme(s).WithRESTMapper(mapper).WithRuntimeObjects(objects...).Build()
	raw, err := json.Marshal(cluster(t, nil))
	if err != nil {
		t.Fatal(err)
	}
	decider := &Decider{Client: c, Mapper: c.RESTMapper(), Prober: prober}
	return decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
}

// On a cluster whose CloudNativePG and barman-cloud serve Cluster and
// ObjectStore at a new version alone, the webhook reads the Cluster's
// ObjectStore and lists the Clusters at that version, and admits a new
// Cluster over an empty prefix as before.
func TestTheWebhookReadsKindsAtTheVersionTheyAreServedAt(t *testing.T) {
	response := decideAtNext(t, stubProber{has: false})
	if !response.Allowed {
		t.Fatalf("the Cluster was refused: %v", response.Result)
	}
}
