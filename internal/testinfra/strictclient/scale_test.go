package strictclient_test

import (
	"context"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
)

// newAppsClient returns a strict client whose scheme knows the apps kinds.
func newAppsClient(t *testing.T) *strictclient.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, appsv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	return strictclient.New(fake.NewClientBuilder().WithScheme(scheme).Build(),
		strictclient.Options{Clock: func() time.Time { return serverTime }})
}

// scaledDeployment returns a valid Deployment with two replicas, a selector
// and a pod template, the shape kube-apiserver accepts.
func scaledDeployment(name string) *appsv1.Deployment {
	replicas := int32(2)
	labels := map[string]string{"app": name}
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "nginx:1.27"}}},
			},
		},
	}
}

// A scale Update through an object that carries only a name changes the
// stored spec.replicas and nothing else, raises the generation, and hands
// back the stored Scale; a Scale read before a later write is refused with a
// Conflict, as kube-apiserver refuses it.
func TestScaleUpdateChangesOnlyTheReplicas(t *testing.T) {
	ctx := context.Background()
	c := newAppsClient(t)
	if err := c.Create(ctx, scaledDeployment("app")); err != nil {
		t.Fatal(err)
	}
	named := &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "default"}}
	scale := &autoscalingv1.Scale{}
	if err := c.SubResource("scale").Get(ctx, named, scale); err != nil {
		t.Fatal(err)
	}
	if scale.Spec.Replicas != 2 || scale.Status.Selector != "app=app" || scale.ResourceVersion == "" {
		t.Fatalf("Scale = %+v, want 2 replicas, selector app=app and a resourceVersion", scale)
	}
	stale := scale.DeepCopy()

	scale.Spec.Replicas = 0
	if err := c.SubResource("scale").Update(ctx, named, client.WithSubResourceBody(scale)); err != nil {
		t.Fatal(err)
	}
	stored := &appsv1.Deployment{ObjectMeta: named.ObjectMeta}
	get(t, c, stored)
	if *stored.Spec.Replicas != 0 || stored.Generation != 2 || stored.Spec.Template.Spec.Containers[0].Image != "nginx:1.27" {
		t.Errorf("stored Deployment: replicas %d, generation %d, template %+v; want 0, 2 and the template kept",
			*stored.Spec.Replicas, stored.Generation, stored.Spec.Template.Spec.Containers)
	}
	if scale.ResourceVersion != stored.ResourceVersion || scale.Spec.Replicas != 0 {
		t.Errorf("returned Scale at %s with %d replicas, want the stored %s with 0", scale.ResourceVersion, scale.Spec.Replicas, stored.ResourceVersion)
	}

	stale.Spec.Replicas = 5
	if err := c.SubResource("scale").Update(ctx, named, client.WithSubResourceBody(stale)); !apierrors.IsConflict(err) {
		t.Errorf("update with a stale Scale: %v, want a Conflict", err)
	}
	missing := &appsv1.StatefulSet{ObjectMeta: metav1.ObjectMeta{Name: "gone", Namespace: "default"}}
	if err := c.SubResource("scale").Get(ctx, missing, &autoscalingv1.Scale{}); !apierrors.IsNotFound(err) {
		t.Errorf("scale of a missing StatefulSet: %v, want NotFound", err)
	}
}
