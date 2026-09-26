//go:build e2e

// The differential suite runs each operation twice, once through the strict
// client and once against the real cluster (kubectl context docker-desktop,
// Kubernetes 1.36.4 with its kube-controller-manager and garbage collector),
// and compares what a client can observe afterwards. The strict client's
// rules are the ones this suite pins; a difference fails the test.
//
// It works in one temporary namespace, which it deletes at the end. Objects
// are ConfigMaps, workloads scaled to zero or suspended, a Pending claim of
// 10Mi on local-path (WaitForFirstConsumer, so no volume is made) and one
// pause Pod.
//
//	nix develop -c go test -tags e2e ./internal/testinfra/strictclient/
package strictclient_test

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/rand"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/retry"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
)

// obs is what one operation left behind, as strings a test can compare.
type obs map[string]string

// diffCase is one operation. run does it with c in namespace ns and returns
// the observation. It may be called again on the real cluster while the
// garbage collector catches up, so everything it creates must be created
// only by setup.
type diffCase struct {
	name  string
	setup func(t *testing.T, c client.Client, ns string) obs
	// settle, when set, reads the state again; the real cluster is polled
	// with it until it matches the strict client's observation.
	settle func(t *testing.T, c client.Client, ns string) obs
}

func realClient(t *testing.T) client.Client {
	t.Helper()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: "docker-desktop"},
	).ClientConfig()
	if err != nil {
		t.Fatalf("kubeconfig for docker-desktop: %v", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: clientgoscheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func strictClient() client.Client {
	scheme := runtime.NewScheme()
	_ = clientgoscheme.AddToScheme(scheme)
	b := fake.NewClientBuilder().WithStatusSubresource(
		&appsv1.Deployment{}, &appsv1.StatefulSet{}, &batchv1.Job{},
		&corev1.PersistentVolumeClaim{}, &corev1.Pod{})
	return strictclient.Build(b, scheme, strictclient.Options{Clock: time.Now, GarbageCollect: true})
}

func TestDifferentialAgainstCluster(t *testing.T) {
	ctx := context.Background()
	real := realClient(t)
	ns := "strictclient-diff-" + rand.String(5)
	if err := real.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = real.Delete(context.Background(), &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}})
	})

	for _, tc := range diffCases() {
		t.Run(tc.name, func(t *testing.T) {
			strict := strictClient()
			want := tc.setup(t, strict, ns)
			if tc.settle != nil {
				maps.Copy(want, tc.settle(t, strict, ns))
			}
			got := tc.setup(t, real, ns)
			if tc.settle != nil {
				deadline := time.Now().Add(90 * time.Second)
				for {
					s := tc.settle(t, real, ns)
					merged := maps.Clone(got)
					maps.Copy(merged, s)
					if maps.Equal(merged, want) || time.Now().After(deadline) {
						got = merged
						break
					}
					time.Sleep(time.Second)
				}
			}
			for _, k := range slices.Sorted(maps.Keys(merge(want, got))) {
				if want[k] != got[k] {
					t.Errorf("%s: strict client %q, cluster %q", k, want[k], got[k])
				}
			}
		})
	}
}

func merge(a, b obs) obs {
	out := maps.Clone(a)
	maps.Copy(out, b)
	return out
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

// errReason reduces an error to what the suite compares: its status reason.
func errReason(err error) string {
	if err == nil {
		return "ok"
	}
	return string(apierrors.ReasonForError(err))
}

func objExists(ctx context.Context, c client.Client, obj client.Object) string {
	err := c.Get(ctx, client.ObjectKeyFromObject(obj), obj)
	switch {
	case apierrors.IsNotFound(err):
		return "gone"
	case err != nil:
		return err.Error()
	case obj.GetDeletionTimestamp() != nil:
		return "deleting " + strings.Join(obj.GetFinalizers(), ",")
	}
	return fmt.Sprintf("present owners=%d", len(obj.GetOwnerReferences()))
}

func labels() map[string]string { return map[string]string{"app": "diff"} }

func deployment(ns, name string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptrTo(int32(0)),
			Selector: &metav1.LabelSelector{MatchLabels: labels()},
			Template: podTemplate(),
		},
	}
}

func statefulSet(ns, name string) *appsv1.StatefulSet {
	return &appsv1.StatefulSet{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: appsv1.StatefulSetSpec{
			Replicas: ptrTo(int32(0)),
			Selector: &metav1.LabelSelector{MatchLabels: labels()},
			Template: podTemplate(),
		},
	}
}

func job(ns, name string) *batchv1.Job {
	tpl := podTemplate()
	tpl.Labels = nil
	tpl.Spec.RestartPolicy = corev1.RestartPolicyNever
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec:       batchv1.JobSpec{Suspend: ptrTo(true), Parallelism: ptrTo(int32(1)), Template: tpl},
	}
}

func claim(ns, name string) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: ptrTo("local-path"),
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("10Mi")},
			},
		},
	}
}

func podTemplate() corev1.PodTemplateSpec {
	return corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{Labels: labels()},
		Spec:       podSpec(),
	}
}

func podSpec() corev1.PodSpec {
	return corev1.PodSpec{
		TerminationGracePeriodSeconds: ptrTo(int64(0)),
		Containers: []corev1.Container{{
			Name: "pause", Image: "registry.k8s.io/pause:3.10", ImagePullPolicy: corev1.PullIfNotPresent,
		}},
	}
}

func ptrTo[T any](v T) *T { return &v }

func configMap(ns, name string, owners ...metav1.OwnerReference) *corev1.ConfigMap {
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, OwnerReferences: owners}}
}

func ownerRef(owner client.Object, kind string, block bool) metav1.OwnerReference {
	return metav1.OwnerReference{
		APIVersion: "v1", Kind: kind, Name: owner.GetName(), UID: owner.GetUID(),
		BlockOwnerDeletion: ptrTo(block),
	}
}

// generationCases builds the create, spec, annotation and status cases for
// one kind. mutate changes the spec; status changes the status.
func generationCases[T client.Object](kind string, build func(ns, name string) T, mutate, status func(T)) []diffCase {
	name := strings.ToLower(kind) + "-gen"
	fresh := func(ns string) T { return build(ns, name) }
	get := func(t *testing.T, c client.Client, obj T) {
		t.Helper()
		must(t, c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj))
	}
	write := func(t *testing.T, c client.Client, ns string, change func(T), sub bool) obs {
		ctx := context.Background()
		obj := fresh(ns)
		_ = c.Delete(ctx, obj)
		waitGone(t, c, obj)
		obj = fresh(ns)
		must(t, c.Create(ctx, obj))
		o := obs{"create": fmt.Sprint(obj.GetGeneration())}
		if change == nil {
			return o
		}
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			get(t, c, obj)
			change(obj)
			if sub {
				return c.Status().Update(ctx, obj)
			}
			return c.Update(ctx, obj)
		})
		o["write"] = errReason(err)
		get(t, c, obj)
		o["after"] = fmt.Sprint(obj.GetGeneration())
		return o
	}
	annotate := func(o T) { o.SetAnnotations(map[string]string{"diff/note": rand.String(4)}) }
	return []diffCase{
		{name: kind + "/generation on create", setup: func(t *testing.T, c client.Client, ns string) obs { return write(t, c, ns, nil, false) }},
		{name: kind + "/generation on spec change", setup: func(t *testing.T, c client.Client, ns string) obs { return write(t, c, ns, mutate, false) }},
		{name: kind + "/generation on annotation change", setup: func(t *testing.T, c client.Client, ns string) obs { return write(t, c, ns, annotate, false) }},
		{name: kind + "/generation on status change", setup: func(t *testing.T, c client.Client, ns string) obs { return write(t, c, ns, status, true) }},
	}
}

func waitGone(t *testing.T, c client.Client, obj client.Object) {
	t.Helper()
	for i := 0; i < 60; i++ {
		err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
		if apierrors.IsNotFound(err) {
			return
		}
		time.Sleep(time.Second)
	}
	t.Fatalf("%s still present", obj.GetName())
}

func diffCases() []diffCase {
	var cases []diffCase
	cases = append(cases, generationCases("Deployment", deployment,
		func(d *appsv1.Deployment) { d.Spec.MinReadySeconds++ },
		func(d *appsv1.Deployment) { d.Status.ObservedGeneration = 42 })...)
	cases = append(cases, generationCases("StatefulSet", statefulSet,
		func(s *appsv1.StatefulSet) { s.Spec.MinReadySeconds++ },
		func(s *appsv1.StatefulSet) { s.Status.ObservedGeneration = 42 })...)
	cases = append(cases, generationCases("Job", job,
		func(j *batchv1.Job) { j.Spec.ActiveDeadlineSeconds = ptrTo(int64(600)) },
		func(j *batchv1.Job) { j.Status.Conditions = nil; j.Status.Active = 0 })...)
	// A Pending claim's spec is immutable (the cluster answers Invalid, which
	// the strict client does not model), so the claim has no spec case.
	pvc := generationCases("PersistentVolumeClaim", claim, nil,
		func(p *corev1.PersistentVolumeClaim) { p.Status.Phase = corev1.ClaimPending })
	cases = append(cases, pvc[0], pvc[2], pvc[3])
	return append(cases, deleteCases()...)
}

func deleteCases() []diffCase {
	ctx := context.Background()
	// ownerWithDependents makes an owner ConfigMap, a dependent that blocks
	// owner deletion and one that does not. The blocking one may carry a
	// finalizer that keeps it from going away.
	ownerWithDependents := func(t *testing.T, c client.Client, ns, prefix string, hold bool) *corev1.ConfigMap {
		owner := configMap(ns, prefix+"-owner")
		must(t, c.Create(ctx, owner))
		blocking := configMap(ns, prefix+"-blocking", ownerRef(owner, "ConfigMap", true))
		if hold {
			blocking.Finalizers = []string{"diff.example.com/hold"}
		}
		must(t, c.Create(ctx, blocking))
		must(t, c.Create(ctx, configMap(ns, prefix+"-plain", ownerRef(owner, "ConfigMap", false))))
		return owner
	}
	state := func(prefix string) func(t *testing.T, c client.Client, ns string) obs {
		return func(t *testing.T, c client.Client, ns string) obs {
			o := obs{}
			for _, n := range []string{"owner", "blocking", "plain"} {
				o[n] = objExists(ctx, c, configMap(ns, prefix+"-"+n))
			}
			return o
		}
	}
	release := func(t *testing.T, c client.Client, ns, name string) {
		cm := configMap(ns, name)
		if err := c.Get(ctx, client.ObjectKeyFromObject(cm), cm); err == nil {
			cm.Finalizers = nil
			_ = c.Update(ctx, cm)
		}
	}
	policyCase := func(prefix string, policy metav1.DeletionPropagation, hold bool) diffCase {
		return diffCase{
			name: "delete " + string(policy) + " with blockOwnerDeletion dependents",
			setup: func(t *testing.T, c client.Client, ns string) obs {
				owner := ownerWithDependents(t, c, ns, prefix, hold)
				t.Cleanup(func() { release(t, c, ns, prefix+"-blocking") })
				return obs{"delete": errReason(c.Delete(ctx, owner, client.PropagationPolicy(policy)))}
			},
			settle: state(prefix),
		}
	}

	return []diffCase{
		policyCase("bg", metav1.DeletePropagationBackground, false),
		policyCase("orphan", metav1.DeletePropagationOrphan, false),
		// The blocking dependent's finalizer holds it, so the owner waits
		// in foreground deletion.
		policyCase("fg", metav1.DeletePropagationForeground, true),
		{
			name: "Job delete with default propagation and its pod",
			setup: func(t *testing.T, c client.Client, ns string) obs {
				j := job(ns, "orphaning")
				must(t, c.Create(ctx, j))
				pod := &corev1.Pod{
					ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "orphaning-pod", OwnerReferences: []metav1.OwnerReference{{
						APIVersion: "batch/v1", Kind: "Job", Name: j.Name, UID: j.UID,
						Controller: ptrTo(true), BlockOwnerDeletion: ptrTo(true),
					}}},
					Spec: podSpec(),
				}
				must(t, c.Create(ctx, pod))
				t.Cleanup(func() { _ = c.Delete(ctx, pod, client.GracePeriodSeconds(0)) })
				return obs{"delete": errReason(c.Delete(ctx, j))}
			},
			settle: func(t *testing.T, c client.Client, ns string) obs {
				return obs{
					"job": objExists(ctx, c, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "orphaning"}}),
					"pod": objExists(ctx, c, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: "orphaning-pod"}}),
				}
			},
		},
		{
			name: "delete with a mismatched UID precondition",
			setup: func(t *testing.T, c client.Client, ns string) obs {
				cm := configMap(ns, "uid-precondition")
				must(t, c.Create(ctx, cm))
				err := c.Delete(ctx, cm, client.Preconditions{UID: ptrTo(types.UID("00000000-0000-0000-0000-000000000000"))})
				return obs{"delete": errReason(err), "after": objExists(ctx, c, cm)}
			},
		},
		{
			name: "add a finalizer to an object being deleted",
			setup: func(t *testing.T, c client.Client, ns string) obs {
				cm := configMap(ns, "add-finalizer")
				cm.Finalizers = []string{"diff.example.com/a"}
				must(t, c.Create(ctx, cm))
				t.Cleanup(func() { release(t, c, ns, cm.Name) })
				must(t, c.Delete(ctx, cm))
				must(t, c.Get(ctx, client.ObjectKeyFromObject(cm), cm))
				cm.Finalizers = append(cm.Finalizers, "diff.example.com/b")
				err := c.Update(ctx, cm)
				return obs{"update": errReason(err), "after": objExists(ctx, c, configMap(ns, cm.Name))}
			},
		},
		{
			name: "remove the last finalizer",
			setup: func(t *testing.T, c client.Client, ns string) obs {
				cm := configMap(ns, "last-finalizer")
				cm.Finalizers = []string{"diff.example.com/a"}
				must(t, c.Create(ctx, cm))
				must(t, c.Delete(ctx, cm))
				must(t, c.Get(ctx, client.ObjectKeyFromObject(cm), cm))
				cm.Finalizers = nil
				err := c.Update(ctx, cm)
				return obs{
					"update":   errReason(err),
					"returned": fmt.Sprint(cm.DeletionTimestamp != nil),
					"after":    objExists(ctx, c, configMap(ns, cm.Name)),
				}
			},
		},
	}
}
