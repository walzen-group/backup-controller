package quiesce

import (
	"context"
	"errors"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// testNS is the namespace of the run in these tests.
const testNS = "notes"

// labeled returns the Deployment notes in testNS, with the labels of the
// Kustomization flux-system/notes.
func labeled() *appsv1.Deployment {
	replicas := int32(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: "notes", Namespace: testNS,
			Labels: map[string]string{FluxNameLabel: "notes", FluxNamespaceLabel: "flux-system"},
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "notes"}}},
	}
}

// kustomizationOf returns the Kustomization flux-system/notes. A nil
// entries gives it no status.inventory.entries.
func kustomizationOf(entries []any) *unstructured.Unstructured {
	k := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"suspend": false}}}
	if entries != nil {
		k.Object["status"] = map[string]any{"inventory": map[string]any{"entries": entries}}
	}
	k.SetGroupVersionKind(KustomizationGVK)
	k.SetNamespace("flux-system")
	k.SetName("notes")
	return k
}

// newTestClient returns a fake client that holds the objects and serves
// Deployments and Kustomizations, and the RESTMapper it uses.
func newTestClient(t *testing.T, funcs interceptor.Funcs, objects ...client.Object) (client.Client, meta.RESTMapper) {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{appsv1.SchemeGroupVersion, KustomizationGVK.GroupVersion()})
	mapper.Add(appsv1.SchemeGroupVersion.WithKind("Deployment"), meta.RESTScopeNamespace)
	mapper.Add(KustomizationGVK, meta.RESTScopeNamespace)
	c := fake.NewClientBuilder().WithScheme(scheme).WithRESTMapper(mapper).WithObjects(objects...).WithInterceptorFuncs(funcs).Build()
	return c, mapper
}

// Each refusal of the package is its own type, which errors.As finds, and
// its Error keeps the sentence the run shows in its Ready message.
func TestTheRefusalsAreTyped(t *testing.T) {
	ctx := context.Background()
	named := func(ref backupv1alpha1.WorkloadRef) func(t *testing.T) error {
		return func(t *testing.T) error {
			c, _ := newTestClient(t, interceptor.Funcs{}, labeled())
			_, err := Named(ctx, c, testNS, []backupv1alpha1.WorkloadRef{ref})
			return err
		}
	}
	plan := func(objects ...client.Object) func(t *testing.T) error {
		return func(t *testing.T) error {
			c, mapper := newTestClient(t, interceptor.Funcs{}, objects...)
			targets, err := Targets(ctx, c, testNS)
			if err != nil {
				t.Fatal(err)
			}
			if len(targets) == 0 {
				targets, err = Named(ctx, c, testNS, []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: "notes"}})
				if err != nil {
					t.Fatal(err)
				}
			}
			_, _, err = Plan(ctx, c, mapper, testNS, targets)
			return err
		}
	}
	restart := func(t *testing.T) error {
		refused := interceptor.Funcs{SubResourceGet: func(context.Context, client.Client, string, client.Object, client.Object, ...client.SubResourceGetOption) error {
			return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments/scale"}, "notes", errors.New("denied"))
		}}
		c, _ := newTestClient(t, refused, labeled())
		return Restart(ctx, c, testNS, []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: "notes", Replicas: 3}}, nil)
	}
	wiki := labeled()
	wiki.Namespace = "wiki"
	own := map[string]any{"id": testNS + "_notes_apps_Deployment", "v": "v1"}

	for name, tc := range map[string]struct {
		run   func(t *testing.T) error
		check func(err error) bool
		text  string
	}{
		"a kind the run cannot stop": {
			run:   named(backupv1alpha1.WorkloadRef{Kind: "DaemonSet", Name: "notes"}),
			check: func(err error) bool { var e *SpecError; return errors.As(err, &e) && e.Ref.Kind == "DaemonSet" },
			text:  "spec.quiesce lists DaemonSet notes; only a Deployment or a StatefulSet can be stopped",
		},
		"a workload the namespace does not hold": {
			run:   named(backupv1alpha1.WorkloadRef{Kind: "StatefulSet", Name: "notes-worker"}),
			check: func(err error) bool { var e *SpecError; return errors.As(err, &e) && e.Ref.Name == "notes-worker" },
			text:  "spec.quiesce lists StatefulSet notes-worker, which this namespace does not hold",
		},
		"a Kustomization of two namespaces": {
			run: plan(labeled(), wiki, kustomizationOf([]any{own, map[string]any{"id": "wiki_notes_apps_Deployment", "v": "v1"}})),
			check: func(err error) bool {
				var e *CrossNamespaceError
				return errors.As(err, &e) && e.Kustomization == "flux-system/notes" && len(e.Namespaces) == 2
			},
			text: "Kustomization flux-system/notes applies workloads in namespaces notes and wiki; a run suspends the Kustomization while it stops workloads, " +
				"which would leave the other namespace's workloads unmanaged by Flux, so it refuses. Give each namespace its own Kustomization",
		},
		"no inventory": {
			run: plan(labeled(), kustomizationOf(nil)),
			check: func(err error) bool {
				var e *InventoryError
				return errors.As(err, &e) && e.Problem == InventoryMissing && e.Kustomization == "flux-system/notes"
			},
			text: "the Kustomization flux-system/notes has no list at status.inventory.entries, where kustomize-controller records every object it " +
				"applies; either it has not applied yet or a Flux release moved the field (see docs/compatibility.md)",
		},
		"an inventory entry of another format": {
			run: plan(labeled(), kustomizationOf([]any{map[string]any{"id": "notes"}})),
			check: func(err error) bool {
				var e *InventoryError
				return errors.As(err, &e) && e.Problem == InventoryEntryMalformed
			},
			text: "the Kustomization flux-system/notes has an entry map[id:notes] in status.inventory.entries whose id is not " +
				"\"<namespace>_<name>_<group>_<kind>\"; a Flux release may have changed the format (see docs/compatibility.md)",
		},
		"a refused restart": {
			run: restart,
			check: func(err error) bool {
				var e *RestartError
				return errors.As(err, &e) && apierrors.IsForbidden(err)
			},
			text: "could not give Deployment notes its 3 replicas back: scale notes to 3: deployments/scale.apps \"notes\" is forbidden: denied",
		},
	} {
		t.Run(name, func(t *testing.T) {
			err := tc.run(t)
			if !tc.check(err) {
				t.Fatalf("err = %T %v; want the typed refusal", err, err)
			}
			if err.Error() != tc.text {
				t.Errorf("Error() = %q\nwant      %q", err.Error(), tc.text)
			}
		})
	}
}
