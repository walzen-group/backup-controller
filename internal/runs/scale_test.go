package runs

import (
	"context"
	"fmt"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// dbN is the StatefulSet that stands beside the app's Deployment in the scale
// tests.
const dbN = "notes-db"

// statefulSet returns a StatefulSet in the run's namespace with three
// replicas and a pod template, so a test can see that a scale leaves the
// rest of the spec alone.
func statefulSet() *appsv1.StatefulSet {
	replicas := int32(3)
	labels := map[string]string{"app": dbN}
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Name: dbN, Namespace: ns},
		Spec: appsv1.StatefulSetSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "db", Image: "postgres:17"}}},
			},
		},
	}
}

// workloadWrites wraps c so that it records every write to a Deployment or a
// StatefulSet: "patch", "update" or "delete" for a write to the object
// itself, and "<subresource> <replicas>" with the field manager for a
// subresource update. It returns the wrapped client and the record.
func workloadWrites(c client.Client) (client.Client, *[]string) {
	var writes []string
	workload := func(obj client.Object) string {
		switch obj.(type) {
		case *appsv1.Deployment:
			return "Deployment " + obj.GetName()
		case *appsv1.StatefulSet:
			return "StatefulSet " + obj.GetName()
		}
		return ""
	}
	record := func(obj client.Object, what string) {
		if name := workload(obj); name != "" {
			writes = append(writes, name+" "+what)
		}
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			record(obj, "patch")
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			record(obj, "update")
			return cl.Update(ctx, obj, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			record(obj, "delete")
			return cl.Delete(ctx, obj, opts...)
		},
		SubResourcePatch: func(ctx context.Context, cl client.Client, sub string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			record(obj, sub+" patch")
			return cl.SubResource(sub).Patch(ctx, obj, patch, opts...)
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			o := &client.SubResourceUpdateOptions{}
			o.ApplyOptions(opts)
			replicas := "no Scale body"
			if s, ok := o.SubResourceBody.(*autoscalingv1.Scale); ok {
				replicas = fmt.Sprint(s.Spec.Replicas)
			}
			record(obj, fmt.Sprintf("%s %s by %s", sub, replicas, o.FieldManager))
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}), &writes
}

// Quiesce changes a workload's replica count only through its scale
// subresource, with the controller's field manager, and never writes the
// Deployment or StatefulSet itself, so the controller needs no write verb on
// those objects. The stop and the restart change spec.replicas and nothing
// else in the spec, and each raises the generation as the API server does.
func TestScaleUsesTheScaleSubresource(t *testing.T) {
	c := newClient(t, deployment(), statefulSet())
	watched, writes := workloadWrites(c)
	ctx := context.Background()
	stop := []backupv1alpha1.QuiescedWorkload{
		{Kind: backupv1alpha1.WorkloadKindDeployment, Name: appN, Replicas: 2},
		{Kind: backupv1alpha1.WorkloadKindStatefulSet, Name: dbN, Replicas: 3},
	}

	if err := quiesce.Apply(ctx, watched, ns, stop, nil); err != nil {
		t.Fatalf("stop: %v", err)
	}
	checkWorkloads(t, c, 0, 0, 2)
	if err := quiesce.Restart(ctx, watched, ns, stop, nil); err != nil {
		t.Fatalf("restart: %v", err)
	}
	checkWorkloads(t, c, 2, 3, 3)

	want := []string{
		"Deployment notes scale 0 by " + string(FieldOwner),
		"StatefulSet notes-db scale 0 by " + string(FieldOwner),
		"Deployment notes scale 2 by " + string(FieldOwner),
		"StatefulSet notes-db scale 3 by " + string(FieldOwner),
	}
	if fmt.Sprint(*writes) != fmt.Sprint(want) {
		t.Errorf("workload writes = %q, want %q", *writes, want)
	}
}

// checkWorkloads fails the test unless the app's Deployment stands at
// deploymentReplicas and the StatefulSet at setReplicas, both at generation,
// with the rest of their specs as the fixtures made them.
func checkWorkloads(t *testing.T, c client.Client, deploymentReplicas, setReplicas int32, generation int64) {
	t.Helper()
	ctx := context.Background()
	d := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: appN}, d); err != nil {
		t.Fatal(err)
	}
	s := &appsv1.StatefulSet{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: dbN}, s); err != nil {
		t.Fatal(err)
	}
	if got := *d.Spec.Replicas; got != deploymentReplicas || d.Generation != generation {
		t.Errorf("Deployment replicas %d at generation %d, want %d at %d", got, d.Generation, deploymentReplicas, generation)
	}
	if got := *s.Spec.Replicas; got != setReplicas || s.Generation != generation {
		t.Errorf("StatefulSet replicas %d at generation %d, want %d at %d", got, s.Generation, setReplicas, generation)
	}
	if d.Spec.Selector.MatchLabels["app"] != appN || d.Annotations[backupv1alpha1.AnnotationQuiesce] != "true" {
		t.Errorf("Deployment lost its selector or annotations: %+v", d.Annotations)
	}
	if len(s.Spec.Template.Spec.Containers) != 1 || s.Spec.Template.Spec.Containers[0].Image != "postgres:17" {
		t.Errorf("StatefulSet template changed: %+v", s.Spec.Template.Spec.Containers)
	}
}

// changeBetweenReadAndWrite wraps c so that another writer changes the app's
// Deployment right after each of the first changes reads of its Scale, the
// way a status or label write can land between quiesce's read and its
// write. It returns the wrapped client and the outcome of every scale
// update: "ok", or the reason the API server gave.
func changeBetweenReadAndWrite(t *testing.T, c client.Client, changes int) (client.Client, *[]string) {
	t.Helper()
	var outcomes []string
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceGet: func(ctx context.Context, cl client.Client, sub string, obj, subResource client.Object, opts ...client.SubResourceGetOption) error {
			if err := cl.SubResource(sub).Get(ctx, obj, subResource, opts...); err != nil {
				return err
			}
			if changes == 0 {
				return nil
			}
			changes--
			d := &appsv1.Deployment{}
			if err := cl.Get(ctx, client.ObjectKey{Namespace: ns, Name: appN}, d); err != nil {
				t.Fatal(err)
			}
			d.Labels["touched"] = fmt.Sprint(changes)
			if err := cl.Update(ctx, d); err != nil {
				t.Fatal(err)
			}
			return nil
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			err := cl.SubResource(sub).Update(ctx, obj, opts...)
			outcome := "ok"
			if err != nil {
				outcome = string(apierrors.ReasonForError(err))
			}
			outcomes = append(outcomes, outcome)
			return err
		},
	}), &outcomes
}

// A change to a workload between scale's read of its Scale and its write,
// such as the status writes a Deployment's controller makes while its pods
// change, never fails the scale: the write carries no resourceVersion, so
// the API server applies it to whatever it holds then. The count is the
// only field the write sets, and the change it raced stays as it was.
func TestAChangeBetweenReadAndScaleDoesNotFailIt(t *testing.T) {
	ctx := context.Background()
	stop := []backupv1alpha1.QuiescedWorkload{{Kind: backupv1alpha1.WorkloadKindDeployment, Name: appN, Replicas: 2}}

	c := newClient(t, deployment())
	racing, outcomes := changeBetweenReadAndWrite(t, c, 100)
	if err := quiesce.Apply(ctx, racing, ns, stop, nil); err != nil {
		t.Fatalf("stop against a Deployment that keeps changing: %v", err)
	}
	if got := replicasOf(t, c); got != 0 || fmt.Sprint(*outcomes) != "[ok]" {
		t.Errorf("replicas %d after scale updates %v, want 0 after [ok]", got, *outcomes)
	}
	d := &appsv1.Deployment{}
	if err := c.Get(ctx, client.ObjectKey{Namespace: ns, Name: appN}, d); err != nil {
		t.Fatal(err)
	}
	if d.Labels["touched"] != "99" {
		t.Errorf("Deployment labels %v after the scale, want the racing change touched=99 kept", d.Labels)
	}
}
