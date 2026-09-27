package runs

import (
	"errors"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
	"github.com/walzen-group/backup-controller/internal/served"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// kueueLookupFails is the RESTMapper of a cluster whose discovery of the
// Kueue group fails for the kind given in kind, as when the API server does
// not answer. Every other kind comes from the wrapped mapper.
type kueueLookupFails struct {
	meta.RESTMapper
	kind string
}

// RESTMapping fails the lookup of k's kind with the error that the
// controller-runtime mapper gives for a discovery call that failed.
func (k kueueLookupFails) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if gk.Group == kueueGroup && gk.Kind == k.kind {
		return nil, &apiutil.ErrResourceDiscoveryFailed{
			schema.GroupVersion{Group: kueueGroup, Version: "v1beta2"}: apierrors.NewServiceUnavailable("the server is currently unable to handle the request"),
		}
	}
	return k.RESTMapper.RESTMapping(gk, versions...)
}

// A lookup of the served LocalQueue or Workload version that fails is not a
// cluster without Kueue. The run stays queued with no Workload, and the pass
// fails so that it runs again. If the run started, it would start past its
// queue.
func TestAFailedKueueLookupKeepsTheRunQueued(t *testing.T) {
	for _, kind := range []string{"LocalQueue", "Workload"} {
		t.Run(kind, func(t *testing.T) {
			c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository(), localQueueObject())
			failing := withMapper{WithWatch: c.(client.WithWatch), mapper: kueueLookupFails{RESTMapper: c.RESTMapper(), kind: kind}}
			r := &BackupRunReconciler{Client: failing, Reader: failing, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

			step(t, r) // plan
			var lookup *served.LookupError
			if err := tryStep(r); !errors.As(err, &lookup) {
				t.Errorf("the admit pass returned %v, want the failed lookup retried", err)
			}
			run := readBackupRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseQueued || run.Status.StartedAt != nil {
				t.Errorf("phase = %q, startedAt = %v; want the run still queued", run.Status.Phase, run.Status.StartedAt)
			}
			if _, ok := getUnstructured(t, c, kueue.WorkloadGVK, ns, kueue.WorkloadName(runUID)); ok {
				t.Error("the run created a Workload at a version it could not look up")
			}
		})
	}
}
