package cnpg

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// TestInstanceLeftIgnoresAPoolerPod checks that InstanceLeft counts only the
// instance pods of a Cluster. A pod of a Pooler for the Cluster also carries
// cnpg.io/cluster, with cnpg.io/podRole=pooler (CloudNativePG v1.30.0
// pkg/specs/pgbouncer/deployments.go:55-58). The Pooler owns it, so it stays
// when the Cluster is deleted.
func TestInstanceLeftIgnoresAPoolerPod(t *testing.T) {
	pooler := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "notes", Name: "notes-pooler-rw-7d9f8-abcde", Labels: map[string]string{
			"cnpg.io/poolerName": "notes-pooler-rw",
			"cnpg.io/cluster":    "notes-pg",
			"cnpg.io/podRole":    "pooler",
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(pooler).Build()
	left, err := InstanceLeft(context.Background(), c, "notes", "notes-pg")
	if err != nil {
		t.Fatal(err)
	}
	if left != "" {
		t.Fatalf("InstanceLeft = %q, want none: a pooler pod is not an instance", left)
	}
}

// TestInstanceLeftSeesAnInstancePod checks that InstanceLeft reports a
// running pod with the labels CloudNativePG v1.30.0 puts on an instance
// (pkg/specs/pods.go:544-548).
func TestInstanceLeftSeesAnInstancePod(t *testing.T) {
	instance := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "notes", Name: "notes-pg-1", Labels: map[string]string{
			"cnpg.io/cluster":      "notes-pg",
			"cnpg.io/instanceName": "notes-pg-1",
			"cnpg.io/podRole":      "instance",
		}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	c := fake.NewClientBuilder().WithScheme(scheme.Scheme).WithObjects(instance).Build()
	left, err := InstanceLeft(context.Background(), c, "notes", "notes-pg")
	if err != nil {
		t.Fatal(err)
	}
	if left != "pod notes-pg-1" {
		t.Fatalf("InstanceLeft = %q, want pod notes-pg-1", left)
	}
}
