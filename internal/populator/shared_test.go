package populator

import (
	"context"
	"strings"
	"testing"
	"time"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// strictOperations is a fakeOperations whose VolumeRestore writes go to the
// strict fake client that holds its Jobs and claims, as clientOperations in
// cmd/backup-controller sends them: SetStatus updates the status subresource
// and UpdateVolumeRestore the object. The client keeps the stored
// VolumeRestore, bumps its resourceVersion on every write, refuses a write
// whose resourceVersion is stale with a Conflict, and prunes against the
// pinned CRD. Every successful status write is also recorded in statuses.
type strictOperations struct {
	*fakeOperations
}

func (s *strictOperations) SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if err := s.cluster.Status().Update(ctx, vr); err != nil {
		return err
	}
	s.statuses = append(s.statuses, vr.DeepCopy())
	return nil
}

func (s *strictOperations) UpdateVolumeRestore(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if err := s.cluster.Update(ctx, vr); err != nil {
		return err
	}
	s.updates = append(s.updates, vr.DeepCopy())
	return nil
}

// stored reads the VolumeRestore notes-data back from the strict client.
func (s *strictOperations) stored(t *testing.T) *backupv1alpha1.VolumeRestore {
	t.Helper()
	vr := new(backupv1alpha1.VolumeRestore)
	if err := s.cluster.Get(context.Background(), client.ObjectKey{Namespace: appNS, Name: "notes-data"}, vr); err != nil {
		t.Fatalf("get VolumeRestore: %v", err)
	}
	return vr
}

// paramsFor returns the library's params for one claim in apps, with the
// VolumeRestore and the prime claim as the strict client holds them now,
// which is what the library's informer cache hands the callbacks.
func (s *strictOperations) paramsFor(t *testing.T, name string, uid types.UID) populatormachinery.PopulatorParams {
	t.Helper()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(s.stored(t))
	if err != nil {
		t.Fatal(err)
	}
	return populatormachinery.PopulatorParams{
		Pvc:          &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: appNS, UID: uid}},
		PvcPrime:     s.prime(t, uid),
		Unstructured: &unstructured.Unstructured{Object: object},
	}
}

// newStrictOperations returns a fake cluster that holds the repository
// Secret, the Secret copies of the claims notes (claim-123) and photos
// (claim-456), a running restore Job for photos, and the VolumeRestore
// notes-data with the populator's finalizer and the status given.
func newStrictOperations(t *testing.T, status backupv1alpha1.VolumeRestoreStatus) *strictOperations {
	t.Helper()
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: appNS, Generation: 1, ResourceVersion: "7", Finalizers: []string{Finalizer}},
		Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"},
		Status:     status,
	}
	ops := fakeOperationsOn(newCluster(t, vr))
	addRepository(ops)
	for _, uid := range []string{"claim-123", "claim-456"} {
		ops.secrets[namespacedName(controllerNS, uid)] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: uid, Namespace: controllerNS}}
	}
	ops.createJobFor(t, 1)
	ops.runJob(t, "claim-456")
	return &strictOperations{fakeOperations: ops}
}

// failedNotes is a status in which the claim notes failed with its restore
// Job's failure and the claim photos is being filled.
func failedNotes() backupv1alpha1.VolumeRestoreStatus {
	started := metav1.NewTime(time.Date(2026, 9, 26, 6, 0, 0, 0, time.UTC))
	return backupv1alpha1.VolumeRestoreStatus{
		Claims: []backupv1alpha1.ClaimRestoreStatus{
			{Name: "notes", UID: "claim-123", Phase: backupv1alpha1.RestorePhaseFailed, StartedAt: &started},
			{Name: "photos", UID: "claim-456", Phase: backupv1alpha1.RestorePhaseRestoring, StartedAt: &started},
		},
		Conditions: []metav1.Condition{{
			Type: backupv1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: backupv1alpha1.ReasonRestoreFailed,
			Message: "claim notes: restic exited 1", ObservedGeneration: 1, LastTransitionTime: started,
		}},
	}
}

// assertReadyFailed checks that the stored VolumeRestore still reports the
// failure of the claim notes.
func assertReadyFailed(t *testing.T, ops *strictOperations) {
	t.Helper()
	ready := findCondition(ops.stored(t).Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || ready.Reason != backupv1alpha1.ReasonRestoreFailed || ready.Message != "claim notes: restic exited 1" {
		t.Fatalf("Ready = %#v, want RestoreFailed with the failure of claim notes", ready)
	}
}

// assertEntry checks the stored entry of claim notes and the Ready
// condition: the phase, the reason and a part of the message.
func assertEntry(t *testing.T, ops *strictOperations, phase backupv1alpha1.RestorePhase, reason, message string) {
	t.Helper()
	stored := ops.stored(t)
	if len(stored.Status.Claims) == 0 || stored.Status.Claims[0].Phase != phase {
		t.Fatalf("claims = %+v, want notes %s", stored.Status.Claims, phase)
	}
	ready := findCondition(stored.Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || ready.Reason != reason || !strings.Contains(ready.Message, message) {
		t.Fatalf("Ready = %#v, want %s with %q", ready, reason, message)
	}
}

// TestCleanupOfARestoredClaimKeepsAnotherClaimsFailure checks that Cleanup of
// a filled claim leaves Ready alone while another claim's entry is Failed.
// The library calls Cleanup on every resync for the life of a filled claim,
// and it used to write Restoring "1 claim(s) still restoring" over the other
// claim's RestoreFailed.
func TestCleanupOfARestoredClaimKeepsAnotherClaimsFailure(t *testing.T) {
	status := failedNotes()
	status.Claims = status.Claims[:1]
	ops := newStrictOperations(t, status)
	ops.setJob(t, "claim-456", complete)

	if err := newCallbacks(ops, monday).Cleanup(context.Background(), ops.paramsFor(t, "photos", "claim-456")); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 0 {
		t.Errorf("status writes = %d, want none", len(ops.statuses))
	}
	assertReadyFailed(t, ops)
}
