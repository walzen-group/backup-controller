package populator

import (
	"context"
	"reflect"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

type fakeOperations struct {
	destinations map[string]*volsyncv1alpha1.ReplicationDestination
	secrets      map[string]*corev1.Secret
	createdRD    []*volsyncv1alpha1.ReplicationDestination
	createdSec   []*corev1.Secret
	deletedRD    []string
	deletedSec   []string
	statuses     []*backupv1alpha1.VolumeRestore
}

func newFakeOperations() *fakeOperations {
	return &fakeOperations{
		destinations: make(map[string]*volsyncv1alpha1.ReplicationDestination),
		secrets:      make(map[string]*corev1.Secret),
	}
}

func (f *fakeOperations) GetReplicationDestination(_ context.Context, namespace, name string) (*volsyncv1alpha1.ReplicationDestination, error) {
	if rd, ok := f.destinations[namespacedName(namespace, name)]; ok {
		return rd.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Group: "volsync.backube", Resource: "replicationdestinations"}, name)
}

func (f *fakeOperations) CreateReplicationDestination(_ context.Context, rd *volsyncv1alpha1.ReplicationDestination) error {
	key := namespacedName(rd.Namespace, rd.Name)
	if _, ok := f.destinations[key]; ok {
		return apierrors.NewAlreadyExists(schema.GroupResource{Group: "volsync.backube", Resource: "replicationdestinations"}, rd.Name)
	}
	f.destinations[key] = rd.DeepCopy()
	f.createdRD = append(f.createdRD, rd.DeepCopy())
	return nil
}

func (f *fakeOperations) DeleteReplicationDestination(_ context.Context, namespace, name string) error {
	key := namespacedName(namespace, name)
	if _, ok := f.destinations[key]; !ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: "volsync.backube", Resource: "replicationdestinations"}, name)
	}
	delete(f.destinations, key)
	f.deletedRD = append(f.deletedRD, key)
	return nil
}

func (f *fakeOperations) GetSecret(_ context.Context, namespace, name string) (*corev1.Secret, error) {
	if secret, ok := f.secrets[namespacedName(namespace, name)]; ok {
		return secret.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
}

func (f *fakeOperations) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	key := namespacedName(secret.Namespace, secret.Name)
	if _, ok := f.secrets[key]; ok {
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, secret.Name)
	}
	f.secrets[key] = secret.DeepCopy()
	f.createdSec = append(f.createdSec, secret.DeepCopy())
	return nil
}

func (f *fakeOperations) DeleteSecret(_ context.Context, namespace, name string) error {
	key := namespacedName(namespace, name)
	if _, ok := f.secrets[key]; !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	delete(f.secrets, key)
	f.deletedSec = append(f.deletedSec, key)
	return nil
}

func (f *fakeOperations) SetStatus(_ context.Context, vr *backupv1alpha1.VolumeRestore) error {
	f.statuses = append(f.statuses, vr.DeepCopy())
	return nil
}

func TestPopulateCopiesRepositorySecret(t *testing.T) {
	ops := newFakeOperations()
	ops.secrets[namespacedName("apps", "repo-secret")] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-secret", Namespace: "apps"},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"repository": []byte("s3://bucket"), "password": []byte("secret")},
	}
	callbacks := New(ops, "backup-system")

	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}

	copied, ok := ops.secrets[namespacedName("backup-system", "claim-123")]
	if !ok || !reflect.DeepEqual(copied.Data, ops.secrets[namespacedName("apps", "repo-secret")].Data) {
		t.Fatalf("copied repository secret = %#v, want same data in backup-system", copied)
	}
}

func TestPopulateNamesFromClaimUID(t *testing.T) {
	ops := populatedOperations()
	callbacks := New(ops, "backup-system")
	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if _, ok := ops.destinations[namespacedName("backup-system", "restore-claim-123")]; !ok {
		t.Fatalf("destination names = %v, want restore-claim-123", mapKeys(ops.destinations))
	}
	if _, ok := ops.secrets[namespacedName("backup-system", "claim-123")]; !ok {
		t.Fatalf("secret names = %v, want claim-123", mapKeys(ops.secrets))
	}
}

func TestPopulateReusesExistingDestination(t *testing.T) {
	ops := populatedOperations()
	existing := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-claim-123", Namespace: "backup-system", Labels: map[string]string{"keep": "exactly"}},
		Spec:       volsyncv1alpha1.ReplicationDestinationSpec{Paused: true},
	}
	ops.destinations[namespacedName(existing.Namespace, existing.Name)] = existing.DeepCopy()
	callbacks := New(ops, "backup-system")

	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if len(ops.createdRD) != 0 {
		t.Fatalf("created destinations = %d, want 0", len(ops.createdRD))
	}
	if got := ops.destinations[namespacedName(existing.Namespace, existing.Name)]; !reflect.DeepEqual(got, existing) {
		t.Fatalf("existing destination changed: got %#v, want %#v", got, existing)
	}
}

func TestPopulateReportsRestoring(t *testing.T) {
	ops := populatedOperations()
	callbacks := New(ops, "backup-system")
	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Fatalf("status writes = %d, want 1", len(ops.statuses))
	}
	status := ops.statuses[0]
	condition := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != backupv1alpha1.ReasonRestoring {
		t.Fatalf("Ready condition = %#v, want False/Restoring", condition)
	}
	// The destination lives in the controller's namespace, so the message says
	// which namespace to look in as well as what to look for.
	if condition.Message != "waiting for ReplicationDestination restore-claim-123 in backup-system" {
		t.Errorf("Ready message = %q, want the destination and its namespace", condition.Message)
	}
	if len(status.Status.Claims) != 1 || status.Status.Claims[0].Name != "notes" || status.Status.Claims[0].UID != types.UID("claim-123") || status.Status.Claims[0].Phase != backupv1alpha1.RestorePhaseRestoring || status.Status.Claims[0].StartedAt == nil {
		t.Fatalf("claim status = %#v, want restoring claim with start time", status.Status.Claims)
	}
}

func TestCompleteTriggerMatching(t *testing.T) {
	for name, test := range map[string]struct {
		last string
		want bool
	}{
		"equal":     {last: "claim-123", want: true},
		"different": {last: "other", want: false},
		"empty":     {last: "", want: false},
	} {
		t.Run(name, func(t *testing.T) {
			ops := restoringOperations()
			ops.destinations[namespacedName("backup-system", "restore-claim-123")].Status = &volsyncv1alpha1.ReplicationDestinationStatus{LastManualSync: test.last}
			callbacks := New(ops, "backup-system")
			got, err := callbacks.Complete(context.Background(), params())
			if err != nil || got != test.want {
				t.Fatalf("Complete() = %t, %v, want %t, nil", got, err, test.want)
			}
		})
	}
}

func TestCompleteReportsFailedMover(t *testing.T) {
	ops := restoringOperations()
	ops.destinations[namespacedName("backup-system", "restore-claim-123")].Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "mover logs"},
	}
	callbacks := New(ops, "backup-system")
	complete, err := callbacks.Complete(context.Background(), params())
	if err != nil || complete {
		t.Fatalf("Complete() = %t, %v, want false, nil", complete, err)
	}
	if len(ops.statuses) != 1 {
		t.Fatalf("status writes = %d, want 1", len(ops.statuses))
	}
	status := ops.statuses[0]
	condition := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != backupv1alpha1.ReasonRestoreFailed || condition.Message != "mover logs" {
		t.Fatalf("Ready condition = %#v, want False/RestoreFailed with mover logs", condition)
	}
	if len(status.Status.Claims) != 1 || status.Status.Claims[0].Phase != backupv1alpha1.RestorePhaseFailed {
		t.Fatalf("claim status = %#v, want Failed", status.Status.Claims)
	}
}

func TestCleanupDeletesDestinationAndSecret(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system")
	if err := callbacks.Cleanup(context.Background(), params()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.deletedRD) != 1 || len(ops.deletedSec) != 1 {
		t.Fatalf("deleted destinations/secrets = %v/%v, want one each", ops.deletedRD, ops.deletedSec)
	}
}

func TestCleanupToleratesMissingObjects(t *testing.T) {
	callbacks := New(newFakeOperations(), "backup-system")
	if err := callbacks.Cleanup(context.Background(), params()); err != nil {
		t.Fatalf("Cleanup() error = %v, want nil for missing objects", err)
	}
}

func params() populatormachinery.PopulatorParams {
	return populatormachinery.PopulatorParams{
		Pvc:          &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "notes", Namespace: "apps", UID: types.UID("claim-123"), Generation: 3}},
		PvcPrime:     &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "prime-claim-123", Namespace: "backup-system"}},
		Unstructured: volumeRestoreUnstructured(),
	}
}

func volumeRestoreUnstructured() *unstructured.Unstructured {
	vr := &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: "apps", Generation: 3}, Spec: backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"}}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vr)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: object}
}

func populatedOperations() *fakeOperations {
	ops := newFakeOperations()
	ops.secrets[namespacedName("apps", "repo-secret")] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "repo-secret", Namespace: "apps"}, Data: map[string][]byte{"repository": []byte("s3://bucket")}}
	return ops
}

// restoringOperations holds what a completed Populate leaves behind: the
// repository Secret copied into the controller namespace and the destination
// for claim-123. Complete and Cleanup read those objects, so their tests start
// from this state rather than from an empty cluster.
func restoringOperations() *fakeOperations {
	ops := populatedOperations()
	ops.secrets[namespacedName("backup-system", "claim-123")] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "claim-123", Namespace: "backup-system"},
		Data:       map[string][]byte{"repository": []byte("s3://bucket")},
	}
	ops.destinations[namespacedName("backup-system", "restore-claim-123")] = &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-claim-123", Namespace: "backup-system"},
	}
	return ops
}

func namespacedName(namespace, name string) string { return namespace + "/" + name }

func findCondition(conditions []metav1.Condition, typ string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == typ {
			return &conditions[i]
		}
	}
	return nil
}

func mapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

var _ Operations = (*fakeOperations)(nil)

// api.md's status table says the Ready condition is True when no claim is being
// filled from the object, and that claims[] holds only the claims being
// populated now. Cleanup is the only callback that runs when a restore ends, so
// it is the one place that can retire the entry and report the object Ready.
// The library has no early return for a claim it has already populated: every
// resync walks the whole path, reaches the completion branch and calls Cleanup
// again, for the life of the claim. Writing status each time would be one
// update per volume every resync interval, forever, none of them changing
// anything. Observed as a PopulatorFinished event repeating on a long-bound
// claim, which is the library's own and harmless; the writes behind it were not.
func TestCleanupWritesNothingWhenThereIsNothingToChange(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system")
	params := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{
		Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring,
	})

	if err := callbacks.Cleanup(context.Background(), params); err != nil {
		t.Fatalf("first Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Fatalf("status writes after the first call = %d, want 1", len(ops.statuses))
	}

	// Feed back what the first call wrote, which is what the next resync reads
	// off the cluster: the claim already retired and the condition already
	// saying so, leaving the second call nothing to write.
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ops.statuses[0])
	if err != nil {
		t.Fatalf("convert the written status back: %v", err)
	}
	retired := params
	retired.Unstructured = &unstructured.Unstructured{Object: object}
	if err := callbacks.Cleanup(context.Background(), retired); err != nil {
		t.Fatalf("second Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Errorf("status writes after the second call = %d, want the second to write nothing", len(ops.statuses))
	}
}

func TestCleanupRetiresTheClaimAndReportsRestored(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system")
	params := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{
		Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring,
	})

	if err := callbacks.Cleanup(context.Background(), params); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.statuses) != 1 {
		t.Fatalf("status writes = %d, want 1", len(ops.statuses))
	}
	status := ops.statuses[0]
	if len(status.Status.Claims) != 0 {
		t.Errorf("claims = %#v, want none once the restore has ended", status.Status.Claims)
	}
	condition := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != backupv1alpha1.ReasonRestored {
		t.Fatalf("Ready condition = %#v, want True/Restored", condition)
	}
}

// A VolumeRestore can populate more than one claim at a time, so retiring one
// claim must leave the others reported and the object not Ready.
func TestCleanupLeavesOtherClaimsRestoring(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system")
	params := paramsWithClaims(
		backupv1alpha1.ClaimRestoreStatus{Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring},
		backupv1alpha1.ClaimRestoreStatus{Name: "photos", UID: types.UID("claim-456"), Phase: backupv1alpha1.RestorePhaseRestoring},
	)

	if err := callbacks.Cleanup(context.Background(), params); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	status := ops.statuses[0]
	if len(status.Status.Claims) != 1 || status.Status.Claims[0].UID != types.UID("claim-456") {
		t.Fatalf("claims = %#v, want only photos still restoring", status.Status.Claims)
	}
	condition := findCondition(status.Status.Conditions, backupv1alpha1.ConditionReady)
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != backupv1alpha1.ReasonRestoring {
		t.Fatalf("Ready condition = %#v, want False/Restoring while photos is filling", condition)
	}
}

func paramsWithClaims(claims ...backupv1alpha1.ClaimRestoreStatus) populatormachinery.PopulatorParams {
	p := params()
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: "apps", Generation: 3},
		Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"},
		Status:     backupv1alpha1.VolumeRestoreStatus{Claims: claims},
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vr)
	if err != nil {
		panic(err)
	}
	p.Unstructured = &unstructured.Unstructured{Object: object}
	return p
}
