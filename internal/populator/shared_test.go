package populator

import (
	"context"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// strictOperations is a fakeOperations whose VolumeRestore writes go to the
// strict fake client, as clientOperations in cmd/backup-controller sends
// them: SetStatus updates the status subresource and UpdateVolumeRestore the
// object. The client keeps the stored VolumeRestore, bumps its
// resourceVersion on every write, refuses a write whose resourceVersion is
// stale with a Conflict, and prunes against the pinned CRD. Every
// successful status write is also recorded in statuses.
type strictOperations struct {
	*fakeOperations
	client client.Client
}

func (s *strictOperations) SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if err := s.client.Status().Update(ctx, vr); err != nil {
		return err
	}
	s.statuses = append(s.statuses, vr.DeepCopy())
	return nil
}

func (s *strictOperations) UpdateVolumeRestore(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if err := s.client.Update(ctx, vr); err != nil {
		return err
	}
	s.updates = append(s.updates, vr.DeepCopy())
	return nil
}

// stored reads the VolumeRestore notes-data back from the strict client.
func (s *strictOperations) stored(t *testing.T) *backupv1alpha1.VolumeRestore {
	t.Helper()
	vr := new(backupv1alpha1.VolumeRestore)
	if err := s.client.Get(context.Background(), client.ObjectKey{Namespace: "apps", Name: "notes-data"}, vr); err != nil {
		t.Fatalf("get VolumeRestore: %v", err)
	}
	return vr
}

// paramsFor returns the library's params for one claim in apps, with the
// VolumeRestore as the strict client holds it now, which is what the
// library's informer cache hands the callbacks.
func (s *strictOperations) paramsFor(t *testing.T, name string, uid types.UID) populatormachinery.PopulatorParams {
	t.Helper()
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(s.stored(t))
	if err != nil {
		t.Fatal(err)
	}
	return populatormachinery.PopulatorParams{
		Pvc:          &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "apps", UID: uid}},
		PvcPrime:     &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "prime-" + string(uid), Namespace: "backup-system"}},
		Unstructured: &unstructured.Unstructured{Object: object},
	}
}

// newStrictOperations returns a fake cluster that holds the repository
// Secret, the Secret copies and healthy destinations of the claims notes
// (claim-123) and photos (claim-456), and the VolumeRestore notes-data with
// the populator's finalizer and the status given.
func newStrictOperations(t *testing.T, status backupv1alpha1.VolumeRestoreStatus) *strictOperations {
	t.Helper()
	s := runtime.NewScheme()
	if err := backupv1alpha1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: "apps", Generation: 1, ResourceVersion: "7", Finalizers: []string{Finalizer}},
		Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"},
		Status:     status,
	}
	c := strictclient.Build(fake.NewClientBuilder().WithObjects(vr), s, strictclient.Options{
		Clock: func() time.Time { return time.Date(2026, 9, 26, 7, 0, 0, 0, time.UTC) },
		CRDs:  []string{"../testinfra/crds/backup-controller/v0.8.1/backup.wlz.li_volumerestores.yaml"},
	})
	ops := restoringOperations()
	ops.secrets[namespacedName("backup-system", "claim-456")] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "claim-456", Namespace: "backup-system"}}
	ops.destinations[namespacedName("backup-system", "restore-claim-456")] = &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-claim-456", Namespace: "backup-system"},
	}
	return &strictOperations{fakeOperations: ops, client: c}
}

// failedNotes is a status in which the claim notes failed with the mover's
// logs and the claim photos is being filled.
func failedNotes() backupv1alpha1.VolumeRestoreStatus {
	started := metav1.NewTime(time.Date(2026, 9, 26, 6, 0, 0, 0, time.UTC))
	return backupv1alpha1.VolumeRestoreStatus{
		Claims: []backupv1alpha1.ClaimRestoreStatus{
			{Name: "notes", UID: "claim-123", Phase: backupv1alpha1.RestorePhaseFailed, StartedAt: &started},
			{Name: "photos", UID: "claim-456", Phase: backupv1alpha1.RestorePhaseRestoring, StartedAt: &started},
		},
		Conditions: []metav1.Condition{{
			Type: backupv1alpha1.ConditionReady, Status: metav1.ConditionFalse, Reason: backupv1alpha1.ReasonRestoreFailed,
			Message: "claim notes: mover logs", ObservedGeneration: 1, LastTransitionTime: started,
		}},
	}
}

// assertReadyFailed checks that the stored VolumeRestore still reports the
// failure of the claim notes with its mover logs.
func assertReadyFailed(t *testing.T, ops *strictOperations) {
	t.Helper()
	ready := findCondition(ops.stored(t).Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || ready.Reason != backupv1alpha1.ReasonRestoreFailed || ready.Message != "claim notes: mover logs" {
		t.Fatalf("Ready = %#v, want RestoreFailed with the logs of claim notes", ready)
	}
}

// TestCleanupOfARestoredClaimKeepsAnotherClaimsFailure checks that Cleanup of
// a filled claim leaves Ready alone while another claim's entry is Failed.
// The library calls Cleanup on every resync for the life of a filled claim,
// and it used to write Restoring "1 claim(s) still restoring" over the other
// claim's RestoreFailed and its mover logs.
func TestCleanupOfARestoredClaimKeepsAnotherClaimsFailure(t *testing.T) {
	status := failedNotes()
	status.Claims = status.Claims[:1]
	ops := newStrictOperations(t, status)
	callbacks := New(ops, "backup-system", nil)

	if err := callbacks.Cleanup(context.Background(), ops.paramsFor(t, "photos", "claim-456")); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 0 {
		t.Errorf("status writes = %d, want none", len(ops.statuses))
	}
	assertReadyFailed(t, ops)
}

// TestPopulateOfAHealthyClaimKeepsAnotherClaimsFailure checks that Populate
// of a claim whose destination is healthy leaves Ready alone while another
// claim's entry is Failed.
func TestPopulateOfAHealthyClaimKeepsAnotherClaimsFailure(t *testing.T) {
	ops := newStrictOperations(t, failedNotes())
	callbacks := New(ops, "backup-system", nil)

	if err := callbacks.Populate(context.Background(), ops.paramsFor(t, "photos", "claim-456")); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	assertReadyFailed(t, ops)
}

// TestTwoRestoringClaimsWriteTheStatusOnce checks that two claims filled from
// one VolumeRestore compute the same Ready message, so their passes write the
// status once between them. Each used to write its own destination's name,
// and every pass of either claim changed the status.
func TestTwoRestoringClaimsWriteTheStatusOnce(t *testing.T) {
	status := failedNotes()
	status.Claims[0].Phase = backupv1alpha1.RestorePhaseRestoring
	status.Conditions[0].Reason = backupv1alpha1.ReasonRestoring
	status.Conditions[0].Message = "waiting for ReplicationDestination restore-claim-123 in backup-system"
	ops := newStrictOperations(t, status)
	callbacks := New(ops, "backup-system", nil)

	for _, claim := range []struct {
		name string
		uid  types.UID
	}{{"notes", "claim-123"}, {"photos", "claim-456"}, {"notes", "claim-123"}, {"photos", "claim-456"}} {
		if err := callbacks.Populate(context.Background(), ops.paramsFor(t, claim.name, claim.uid)); err != nil {
			t.Fatalf("Populate(%s) error = %v", claim.name, err)
		}
	}
	if len(ops.statuses) != 1 {
		t.Errorf("status writes = %d, want 1", len(ops.statuses))
	}
	ready := findCondition(ops.stored(t).Status.Conditions, backupv1alpha1.ConditionReady)
	want := "waiting for ReplicationDestinations restore-claim-123, restore-claim-456 in backup-system"
	if ready == nil || ready.Reason != backupv1alpha1.ReasonRestoring || ready.Message != want {
		t.Fatalf("Ready = %#v, want Restoring %q", ready, want)
	}
}

// TestCleanupOfTheOnlyFailedClaimReportsRestored checks that Cleanup of a
// deleted failed claim, the only entry left, retires it and sets Ready to
// Restored.
func TestCleanupOfTheOnlyFailedClaimReportsRestored(t *testing.T) {
	status := failedNotes()
	status.Claims = status.Claims[:1]
	ops := newStrictOperations(t, status)
	callbacks := New(ops, "backup-system", nil)

	if err := callbacks.Cleanup(context.Background(), ops.paramsFor(t, "notes", "claim-123")); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	stored := ops.stored(t)
	ready := findCondition(stored.Status.Conditions, backupv1alpha1.ConditionReady)
	if len(stored.Status.Claims) != 0 || ready == nil || ready.Status != metav1.ConditionTrue || ready.Reason != backupv1alpha1.ReasonRestored {
		t.Fatalf("claims %v, Ready %#v, want none and Restored", stored.Status.Claims, ready)
	}
}

// TestARecoveredClaimReportsEveryDestination checks that once a failed
// claim's mover succeeds, its Populate marks it Restoring and Ready names the
// destinations of both claims being filled.
func TestARecoveredClaimReportsEveryDestination(t *testing.T) {
	ops := newStrictOperations(t, failedNotes())
	ops.destinations[namespacedName("backup-system", "restore-claim-123")].Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultSuccessful},
	}
	callbacks := New(ops, "backup-system", nil)

	if err := callbacks.Populate(context.Background(), ops.paramsFor(t, "notes", "claim-123")); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	stored := ops.stored(t)
	if stored.Status.Claims[0].Phase != backupv1alpha1.RestorePhaseRestoring {
		t.Errorf("notes phase = %s, want Restoring", stored.Status.Claims[0].Phase)
	}
	ready := findCondition(stored.Status.Conditions, backupv1alpha1.ConditionReady)
	want := "waiting for ReplicationDestinations restore-claim-123, restore-claim-456 in backup-system"
	if ready == nil || ready.Reason != backupv1alpha1.ReasonRestoring || ready.Message != want {
		t.Fatalf("Ready = %#v, want Restoring %q", ready, want)
	}
}

// TestACompleteThatChangesNothingWritesNothing checks that Complete prefixes
// the mover's logs with the claim's name, and skips the write on a later
// pass when the status already says so.
func TestACompleteThatChangesNothingWritesNothing(t *testing.T) {
	status := failedNotes()
	status.Conditions[0].Message = "mover logs"
	ops := newStrictOperations(t, status)
	ops.destinations[namespacedName("backup-system", "restore-claim-123")].Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "mover logs"},
	}
	callbacks := New(ops, "backup-system", nil)

	for range 2 {
		if _, err := callbacks.Complete(context.Background(), ops.paramsFor(t, "notes", "claim-123")); err != nil {
			t.Fatalf("Complete() error = %v", err)
		}
	}
	if len(ops.statuses) != 1 {
		t.Errorf("status writes = %d, want 1", len(ops.statuses))
	}
	assertReadyFailed(t, ops)
}
