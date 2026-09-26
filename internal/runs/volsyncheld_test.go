package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file check what a run does when VolSync, in the middle
// of the run, stops serving v1alpha1, the one version the controller's
// VolSync types come from. Giving the app back needs no VolSync object, so
// a BackupRun ends and restarts what it stopped. A RestoreRun can't stop
// its mover through an API it has no types for, so each pass fails loudly
// and retries, with the app down, until v1alpha1 is served again. VolSync
// is upgraded after the controller, so a supported cluster never gets here.

// volsyncMovedMapper serves VolSync's kinds at v1beta1 alone, as the API
// server's discovery does after a VolSync release that drops v1alpha1. Every
// other kind is looked up in the wrapped mapper.
type volsyncMovedMapper struct{ meta.RESTMapper }

// RESTMapping answers a VolSync kind at v1beta1, and a lookup of it at any
// other version with the no-match error the real mapper gives.
func (m volsyncMovedMapper) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if gk.Group != volsyncv1alpha1.GroupVersion.Group {
		return m.RESTMapper.RESTMapping(gk, versions...)
	}
	if len(versions) > 0 && versions[0] != "v1beta1" {
		return nil, &meta.NoKindMatchError{GroupKind: gk, SearchedVersions: versions}
	}
	return &meta.RESTMapping{GroupVersionKind: gk.WithVersion("v1beta1"), Scope: meta.RESTScopeNamespace}, nil
}

// RESTMappings answers a VolSync kind with its v1beta1 mapping alone.
func (m volsyncMovedMapper) RESTMappings(gk schema.GroupKind, versions ...string) ([]*meta.RESTMapping, error) {
	if gk.Group != volsyncv1alpha1.GroupVersion.Group {
		return m.RESTMapper.RESTMappings(gk, versions...)
	}
	mapping, err := m.RESTMapping(gk, versions...)
	if err != nil {
		return nil, err
	}
	return []*meta.RESTMapping{mapping}, nil
}

// volsyncMovedClient is a client whose RESTMapper is a volsyncMovedMapper:
// a BackupRun's pass sees the VolSync upgrade the way the manager's mapper
// sees it once it has read the group's discovery again.
type volsyncMovedClient struct{ client.Client }

// RESTMapper returns the wrapped client's mapper behind volsyncMovedMapper.
func (c volsyncMovedClient) RESTMapper() meta.RESTMapper {
	return volsyncMovedMapper{c.Client.RESTMapper()}
}

// A quiesced BackupRun whose VolSync stops serving v1alpha1 while the app is
// down ends at once with reason VolSyncUnsupported, and gives the app its
// replicas back and resumes its Kustomization, rather than holding the app
// down past spec.maxQuiesce and the run's timeout.
func TestABackupEndsAndGivesTheAppBackWhenVolSyncDropsV1alpha1(t *testing.T) {
	r, c := quiescedVolumeRun(t, annotatedNamespace(nil))
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the quiesce, want 0", got)
	}
	r.Client = volsyncMovedClient{r.Client}

	if err := tryStep(r); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	run := readBackupRun(t, c)
	checkVolSyncUnsupported(t, run.Status.Conditions, "ReplicationSource")
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", run.Status.Phase)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d, want the app's 2 back", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization is still suspended")
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "v1alpha1") {
		t.Errorf("item = %+v, want it Failed with the VolSync message", item)
	}
	if len(run.Finalizers) != 0 {
		t.Errorf("finalizers = %v, want the run's removed", run.Finalizers)
	}
}

// A quiesced BackupRun deleted while VolSync serves only v1alpha1's
// successor gives the app back and lets the deletion complete.
func TestADeletedBackupGivesTheAppBackWhenVolSyncDropsV1alpha1(t *testing.T) {
	r, c := quiescedVolumeRun(t, annotatedNamespace(nil))
	if err := c.Delete(context.Background(), readBackupRun(t, c)); err != nil {
		t.Fatal(err)
	}
	r.Client = volsyncMovedClient{r.Client}

	if err := tryStep(r); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d, want the app's 2 back", got)
	}
	err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "before-upgrade"}, &backupv1alpha1.BackupRun{})
	if !apierrors.IsNotFound(err) {
		t.Errorf("get the deleted run: %v, want it gone", err)
	}
}

// destinationAt returns the run's ReplicationDestination for its first item
// as an object of VolSync's kind at version, with the same content: the
// object as the API server serves it after VolSync moved to that version.
func destinationAt(t *testing.T, run *backupv1alpha1.RestoreRun, claim, version string) *unstructured.Unstructured {
	t.Helper()
	content, err := runtime.DefaultUnstructuredConverter.ToUnstructured(destinationFor(run, claim))
	if err != nil {
		t.Fatal(err)
	}
	destination := &unstructured.Unstructured{Object: content}
	destination.SetGroupVersionKind(volsyncv1alpha1.GroupVersion.WithKind("ReplicationDestination"))
	destination.SetAPIVersion(volsyncv1alpha1.GroupVersion.Group + "/" + version)
	return destination
}

// movedRestoreReconciler returns a RestoreRunReconciler over a client whose
// API server serves VolSync's kinds at v1beta1 alone, holding the given
// objects, and the client itself. The clock stands at frozen.
func movedRestoreReconciler(t *testing.T, objects ...client.Object) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	c := newClientWithCRDs(t, crdsWithVolSyncAt(t, "v1beta1"), objects...)
	serving := servingOnly(c)
	return &RestoreRunReconciler{Client: serving, Reader: serving, Snapshots: snapshots{sunday, monday}, Now: frozenNow}, c
}

// tryRestoreStep reconciles the RestoreRun back-to-monday once and returns
// the reconcile's error, moving the clock forward by the result's
// RequeueAfter.
func tryRestoreStep(r *RestoreRunReconciler) error {
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}})
	if result.RequeueAfter > 0 {
		advance(r, result.RequeueAfter)
	}
	return err
}

// movedDestinationExists reports whether the run's ReplicationDestination
// for its first item is still there at v1beta1.
func movedDestinationExists(t *testing.T, c client.Client) bool {
	t.Helper()
	destination := &unstructured.Unstructured{}
	destination.SetAPIVersion(volsyncv1alpha1.GroupVersion.Group + "/v1beta1")
	destination.SetKind("ReplicationDestination")
	err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: destinationName(restoreUID, 0)}, destination)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatal(err)
	}
	return err == nil
}

// checkVolSyncRefused fails the test unless err is the error of a request
// for a VolSync kind at v1alpha1, which the API server no longer serves: an
// error the reconcile returns for a retry, naming the kind and the version.
func checkVolSyncRefused(t *testing.T, err error, kind string) {
	t.Helper()
	if err == nil {
		t.Fatal("the pass returned no error, want the unserved VolSync version refused and retried")
	}
	if apierrors.IsNotFound(err) {
		t.Errorf("error = %v, want one that never reads as a missing object", err)
	}
	for _, want := range []string{kind, "volsync.backube/v1alpha1"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not name %s", err, want)
		}
	}
}

// A RestoreRun whose ReplicationDestination is still there when VolSync
// stops serving v1alpha1 can neither read nor stop its mover. Each pass
// fails with an error naming ReplicationDestination and v1alpha1, and the
// run stays unfinished with the app down and the destination left alone,
// however far past its timeout, until v1alpha1 is served again.
func TestARestoreFailsLoudlyAndKeepsTheAppDownWhenVolSyncDropsV1alpha1(t *testing.T) {
	run, _ := quiescedMidRestore()
	r, c := movedRestoreReconciler(t, run, claim(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true),
		destinationAt(t, run, claimN, "v1beta1"))
	r.Now = func() time.Time { return frozen.Add(48 * time.Hour) }

	for range 3 {
		checkVolSyncRefused(t, tryRestoreStep(r), "ReplicationDestination")
	}

	after := readRestoreRun(t, c)
	if after.Status.Phase.Finished() || replicasOf(t, c) != 0 || !suspended(t, c) || !movedDestinationExists(t, c) {
		t.Errorf("phase = %q, replicas = %d, suspended = %v, destination there = %v; "+
			"want the run unfinished, the app down and the destination left alone",
			after.Status.Phase, replicasOf(t, c), suspended(t, c), movedDestinationExists(t, c))
	}
}

// A RestoreRun deleted while VolSync serves only v1alpha1's successor keeps
// its finalizer and the app down, and each pass fails with an error naming
// ReplicationDestination: its mover may still write into the claim.
func TestADeletedRestoreKeepsItsFinalizerWhenVolSyncDropsV1alpha1(t *testing.T) {
	run, _ := quiescedMidRestore()
	r, c := movedRestoreReconciler(t, run, claim(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true),
		destinationAt(t, run, claimN, "v1beta1"))
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}

	for range 3 {
		checkVolSyncRefused(t, tryRestoreStep(r), "ReplicationDestination")
	}

	after := readRestoreRun(t, c)
	if len(after.Finalizers) == 0 || replicasOf(t, c) != 0 || !movedDestinationExists(t, c) {
		t.Errorf("finalizers = %v, replicas = %d, destination there = %v; want the finalizer kept, the app down and the destination left alone",
			after.Finalizers, replicasOf(t, c), movedDestinationExists(t, c))
	}
}
