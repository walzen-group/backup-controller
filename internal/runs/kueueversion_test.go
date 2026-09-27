package runs

import (
	"context"
	"net/http"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// kueueAt is the RESTMapper of a cluster whose Kueue serves its kinds at one
// version only, as an older Kueue that serves v1beta1 does. Every other
// group comes from the wrapped mapper, the discovery-backed one of the test
// client (see discoveryMapper).
type kueueAt struct {
	meta.RESTMapper
	version string
}

// RESTMapping looks a Kueue kind up at the one version k serves, and fails
// with the no-match error of the real mapper for any other version asked for.
func (k kueueAt) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if gk.Group != kueueGroup {
		return k.RESTMapper.RESTMapping(gk, versions...)
	}
	for _, v := range versions {
		if v != k.version {
			return nil, &meta.NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
		}
	}
	return k.RESTMapper.RESTMapping(gk, k.version)
}

// kueueObject returns a Kueue object of the given kind and version in the
// test namespace, with the given spec.
func kueueObject(version, kind, name string, spec map[string]any) *unstructured.Unstructured {
	u := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	u.SetGroupVersionKind(schema.GroupVersionKind{Group: kueueGroup, Version: version, Kind: kind})
	u.SetNamespace(ns)
	u.SetName(name)
	return u
}

// A run on a cluster whose Kueue serves only v1beta1 is admitted through
// Kueue at that version: it finds the namespace's LocalQueue, creates its
// Workload at v1beta1, and waits for admission. It never takes the version
// it can't use for a cluster without Kueue, which would start the run past
// the queue.
func TestARunIsAdmittedAtTheVersionKueueServes(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(),
		kueueObject("v1beta1", "LocalQueue", "backups", map[string]any{"clusterQueue": "backups"}))
	served := servingOnly(withMapper{WithWatch: c.(client.WithWatch), mapper: kueueAt{RESTMapper: c.RESTMapper(), version: "v1beta1"}})
	r := &BackupRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	step(t, r) // plan
	step(t, r) // admit

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseQueued || run.Status.StartedAt != nil {
		t.Fatalf("phase = %q, startedAt = %v; want the run queued for admission", run.Status.Phase, run.Status.StartedAt)
	}
	workload, ok := getUnstructured(t, c, schema.GroupVersionKind{Group: kueueGroup, Version: "v1beta1", Kind: "Workload"}, ns, kueue.WorkloadName(runUID))
	if !ok {
		t.Fatal("the run created no Workload at v1beta1")
	}
	if queue, _, _ := unstructured.NestedString(workload.Object, "spec", "queueName"); queue != "backups" {
		t.Errorf("the Workload's queueName = %q, want backups", queue)
	}
}

// versionGoneAt wraps c so that the API server answers every call for the
// Kueue kind given in kind at version with the plain-text 404 of a version
// it no longer serves, as after a Kueue upgrade the client's mapper has not
// seen. It counts the create calls for that kind in creates.
func versionGoneAt(c client.Client, kind, version string, creates *int) client.Client {
	gone := func(obj any, verb, name string) error {
		u, ok := obj.(interface {
			GroupVersionKind() schema.GroupVersionKind
		})
		if !ok {
			return nil
		}
		gvk := u.GroupVersionKind()
		if gvk.Group != kueueGroup || gvk.Version != version || (gvk.Kind != kind && gvk.Kind != kind+"List") {
			return nil
		}
		return apierrors.NewGenericServerResponse(http.StatusNotFound, verb,
			schema.GroupResource{Group: kueueGroup, Resource: "workloads"}, name, "404 page not found", 0, true)
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := gone(obj, "get", key.Name); err != nil {
				return err
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := gone(list, "list", ""); err != nil {
				return err
			}
			return cl.List(ctx, list, opts...)
		},
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := gone(obj, "post", obj.GetName()); err != nil {
				*creates++
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
}

// A LocalQueue list or a Workload get at a version the API server has
// stopped serving answers NotFound, which never reads as a cluster without
// Kueue or a missing Workload: the run stays queued, the pass fails so it
// runs again, and no Workload is created at that version.
func TestAKueueVersionNoLongerServedKeepsTheRunQueued(t *testing.T) {
	for _, kind := range []string{"LocalQueue", "Workload"} {
		t.Run(kind, func(t *testing.T) {
			c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository(), localQueueObject())
			creates := 0
			gone := versionGoneAt(c, kind, "v1beta2", &creates)
			r := &BackupRunReconciler{Client: gone, Reader: gone, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

			step(t, r) // plan
			err := tryStep(r)
			if err == nil {
				t.Error("the admit pass returned no error, want the unserved version retried")
			}
			if apierrors.IsNotFound(err) {
				t.Errorf("the admit pass error %v reads as NotFound, want a version no longer served", err)
			}
			run := readBackupRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseQueued || run.Status.StartedAt != nil {
				t.Errorf("phase = %q, startedAt = %v; want the run still queued", run.Status.Phase, run.Status.StartedAt)
			}
			if creates != 0 {
				t.Errorf("%d Workload creates at the unserved version, want none", creates)
			}
		})
	}
}
