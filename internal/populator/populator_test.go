package populator

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
)

// fakeOperations is an in-memory Operations. It keeps objects by
// namespace/name, and records every create, delete and status write so the
// tests can check them.
type fakeOperations struct {
	destinations map[string]*volsyncv1alpha1.ReplicationDestination
	secrets      map[string]*corev1.Secret
	createdRD    []*volsyncv1alpha1.ReplicationDestination
	createdSec   []*corev1.Secret
	deletedRD    []string
	deletedSec   []string
	statuses     []*backupv1alpha1.VolumeRestore
}

// newFakeOperations returns a fakeOperations that holds no objects.
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

// TestPopulateCopiesRepositorySecret checks that Populate copies the repository
// Secret from the app's namespace into the controller namespace with the same
// data.
func TestPopulateCopiesRepositorySecret(t *testing.T) {
	ops := newFakeOperations()
	ops.secrets[namespacedName("apps", "repo-secret")] = &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-secret", Namespace: "apps"},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"repository": []byte("s3://bucket"), "password": []byte("secret")},
	}
	callbacks := New(ops, "backup-system", nil)

	if err := callbacks.Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}

	copied, ok := ops.secrets[namespacedName("backup-system", "claim-123")]
	if !ok || !reflect.DeepEqual(copied.Data, ops.secrets[namespacedName("apps", "repo-secret")].Data) {
		t.Fatalf("copied repository secret = %#v, want same data in backup-system", copied)
	}
}

// TestPopulateNamesFromClaimUID checks that Populate names the destination
// restore-<claim UID> and names the Secret copy after the claim UID.
func TestPopulateNamesFromClaimUID(t *testing.T) {
	ops := populatedOperations()
	callbacks := New(ops, "backup-system", nil)
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

// TestPopulateReusesExistingDestination checks that Populate leaves an existing
// ReplicationDestination exactly as it is and creates no second one.
func TestPopulateReusesExistingDestination(t *testing.T) {
	ops := populatedOperations()
	existing := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: "restore-claim-123", Namespace: "backup-system", Labels: map[string]string{"keep": "exactly"}},
		Spec:       volsyncv1alpha1.ReplicationDestinationSpec{Paused: true},
	}
	ops.destinations[namespacedName(existing.Namespace, existing.Name)] = existing.DeepCopy()
	callbacks := New(ops, "backup-system", nil)

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

// TestPopulateReportsRestoring checks that Populate writes the status once,
// with Ready False and reason Restoring, and a Restoring entry for the claim
// that has a start time.
func TestPopulateReportsRestoring(t *testing.T) {
	ops := populatedOperations()
	callbacks := New(ops, "backup-system", nil)
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

// TestCompleteTriggerMatching checks that Complete reports the restore finished
// only when status.lastManualSync equals the claim's trigger.
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
			callbacks := New(ops, "backup-system", nil)
			got, err := callbacks.Complete(context.Background(), params())
			if err != nil || got != test.want {
				t.Fatalf("Complete() = %t, %v, want %t, nil", got, err, test.want)
			}
		})
	}
}

// TestCompleteReportsFailedMover checks that after a failed mover run, Complete
// returns false and writes a Failed entry for the claim and a Ready condition
// with reason RestoreFailed and the mover's logs as the message.
func TestCompleteReportsFailedMover(t *testing.T) {
	ops := restoringOperations()
	ops.destinations[namespacedName("backup-system", "restore-claim-123")].Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "mover logs"},
	}
	callbacks := New(ops, "backup-system", nil)
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

// versionedStatus is a fakeOperations whose status writes behave like the API
// server's: a write whose resourceVersion is older than the stored one fails
// with a conflict, and a write that changes the status bumps the version.
type versionedStatus struct {
	*fakeOperations
	stored  *backupv1alpha1.VolumeRestore
	version int
}

func (v *versionedStatus) SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if vr.ResourceVersion != v.stored.ResourceVersion {
		return apierrors.NewConflict(schema.GroupResource{Group: "backup.wlz.li", Resource: "volumerestores"}, vr.Name, nil)
	}
	// The API server keeps times to the second, so the comparison and the
	// stored copy go through the same serialisation.
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vr)
	if err != nil {
		return err
	}
	written := new(backupv1alpha1.VolumeRestore)
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(object, written); err != nil {
		return err
	}
	if !reflect.DeepEqual(written.Status, v.stored.Status) {
		v.version++
		v.stored = written
		v.stored.ResourceVersion = fmt.Sprint(v.version)
	}
	vr.ResourceVersion = v.stored.ResourceVersion
	return v.fakeOperations.SetStatus(ctx, vr)
}

// TestAFailingMoverKeepsTheStatusFailed checks that while the mover keeps
// failing, the VolumeRestore reports RestoreFailed with the mover's logs on
// every pass. The library calls Populate and then Complete on each pass with
// the same cached object. When Populate wrote Restoring over the Failed entry,
// Complete's write of Failed conflicted, and the reason flipped between
// Restoring and RestoreFailed from one pass to the next.
func TestAFailingMoverKeepsTheStatusFailed(t *testing.T) {
	ops := &versionedStatus{fakeOperations: restoringOperations(), version: 1}
	ops.destinations[namespacedName("backup-system", "restore-claim-123")].Status = &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "mover logs"},
	}
	ops.stored = &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: "apps", Generation: 3, ResourceVersion: "1"},
		Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"},
	}
	callbacks := New(ops, "backup-system", nil)

	var reasons []string
	for range 4 {
		object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(ops.stored)
		if err != nil {
			t.Fatal(err)
		}
		p := params()
		p.Unstructured = &unstructured.Unstructured{Object: object}
		// Errors are the library's cue to requeue, so the pass goes on to the
		// next one the way the library's would.
		_ = callbacks.Populate(context.Background(), p)
		_, _ = callbacks.Complete(context.Background(), p)
		reasons = append(reasons, findCondition(ops.stored.Status.Conditions, backupv1alpha1.ConditionReady).Reason)
	}
	for i, reason := range reasons[1:] {
		if reason != backupv1alpha1.ReasonRestoreFailed {
			t.Fatalf("Ready reason after each pass = %v, want %s from pass %d on", reasons, backupv1alpha1.ReasonRestoreFailed, i+2)
		}
	}
}

// TestCleanupDeletesDestinationAndSecret checks that Cleanup deletes the
// destination and the Secret copy.
func TestCleanupDeletesDestinationAndSecret(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system", nil)
	if err := callbacks.Cleanup(context.Background(), params()); err != nil {
		t.Fatalf("Cleanup() error = %v", err)
	}
	if len(ops.deletedRD) != 1 || len(ops.deletedSec) != 1 {
		t.Fatalf("deleted destinations/secrets = %v/%v, want one each", ops.deletedRD, ops.deletedSec)
	}
}

// TestCleanupToleratesMissingObjects checks that Cleanup succeeds when the
// destination and the Secret copy are already gone.
func TestCleanupToleratesMissingObjects(t *testing.T) {
	callbacks := New(newFakeOperations(), "backup-system", nil)
	if err := callbacks.Cleanup(context.Background(), params()); err != nil {
		t.Fatalf("Cleanup() error = %v, want nil for missing objects", err)
	}
}

// fixedSnapshots is a restic.Lister that returns the same snapshots for every
// Secret.
type fixedSnapshots []restic.Snapshot

func (f fixedSnapshots) Snapshots(context.Context, *corev1.Secret) ([]restic.Snapshot, error) {
	return f, nil
}

// monday is the one snapshot in the repository of the pinned-claim tests,
// taken at 05:00 UTC on Monday 21 September 2026.
var monday = restic.Snapshot{ID: "6e473100aaaa", Time: time.Date(2026, 9, 21, 5, 0, 0, 0, time.UTC)}

// pinned returns the default params with the claim pinned by the
// backup.wlz.li/restore-as-of annotation to the time given in at.
func pinned(at string) populatormachinery.PopulatorParams {
	p := params()
	p.Pvc.Annotations = map[string]string{backupv1alpha1.AnnotationRestoreAsOf: at}
	return p
}

// TestAPinnedClaimRestoresItsMoment checks that a claim pinned to a moment gets
// a destination whose restoreAsOf is that moment. The VolumeRestore alone
// would restore the newest snapshot.
func TestAPinnedClaimRestoresItsMoment(t *testing.T) {
	ops := populatedOperations()
	callbacks := New(ops, "backup-system", fixedSnapshots{monday})

	if err := callbacks.Populate(context.Background(), pinned("2026-09-22T00:00:00Z")); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	rd := ops.destinations[namespacedName("backup-system", "restore-claim-123")]
	if rd == nil || rd.Spec.Restic.RestoreAsOf == nil || *rd.Spec.Restic.RestoreAsOf != "2026-09-22T00:00:00Z" {
		t.Fatalf("restoreAsOf = %v, want the claim's annotation", rd)
	}
}

// TestAPinnedClaimNoSnapshotReachesStaysPending checks that Populate returns an
// error and creates no destination for a claim pinned before every snapshot,
// and that it reports NoBackupInReach with the oldest snapshot's ID. VolSync
// would restore nothing and report success, and the claim would bind an empty
// volume.
func TestAPinnedClaimNoSnapshotReachesStaysPending(t *testing.T) {
	ops := populatedOperations()
	callbacks := New(ops, "backup-system", fixedSnapshots{monday})

	err := callbacks.Populate(context.Background(), pinned("2026-09-01T00:00:00Z"))
	if err == nil {
		t.Fatal("Populate() filled a claim pinned before every snapshot")
	}
	if len(ops.createdRD) != 0 {
		t.Fatalf("a destination was created: %v", ops.createdRD)
	}
	last := ops.statuses[len(ops.statuses)-1]
	if cond := last.Status.Conditions[0]; cond.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(cond.Message, "6e473100") {
		t.Fatalf("condition = %+v, want NoBackupInReach naming the oldest snapshot", cond)
	}
}

// TestAVolumeRestorePinnedBeforeEverySnapshotStaysPending checks that
// Populate checks the VolumeRestore's own spec.restoreAsOf when the claim has
// no annotation. The destination takes that time as its restoreAsOf, and when
// it is before every snapshot VolSync prints "No eligible snapshots found",
// exits 0, and the claim binds an empty volume. Populate returns an error,
// creates no destination, and reports NoBackupInReach.
func TestAVolumeRestorePinnedBeforeEverySnapshotStaysPending(t *testing.T) {
	ops := populatedOperations()
	callbacks := New(ops, "backup-system", fixedSnapshots{monday})
	p := params()
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: "apps", Generation: 3},
		Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret", RestoreAsOf: new("2026-09-01T00:00:00Z")},
	}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vr)
	if err != nil {
		t.Fatal(err)
	}
	p.Unstructured = &unstructured.Unstructured{Object: object}

	if err := callbacks.Populate(context.Background(), p); err == nil {
		t.Fatal("Populate() filled a claim whose VolumeRestore is pinned before every snapshot")
	}
	if len(ops.createdRD) != 0 {
		t.Fatalf("a destination was created: %v", ops.createdRD)
	}
	last := ops.statuses[len(ops.statuses)-1]
	if cond := last.Status.Conditions[0]; cond.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(cond.Message, "spec.restoreAsOf") {
		t.Fatalf("condition = %+v, want NoBackupInReach naming spec.restoreAsOf", cond)
	}
}

// params returns the library's parameters for the claim notes (UID claim-123)
// in apps, its prime claim in backup-system, and a VolumeRestore that names
// the Secret repo-secret.
func params() populatormachinery.PopulatorParams {
	return populatormachinery.PopulatorParams{
		Pvc:          &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "notes", Namespace: "apps", UID: types.UID("claim-123"), Generation: 3}},
		PvcPrime:     &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "prime-claim-123", Namespace: "backup-system"}},
		Unstructured: volumeRestoreUnstructured(),
	}
}

// volumeRestoreUnstructured returns the VolumeRestore notes-data as an
// unstructured object, the form in which the library passes it.
func volumeRestoreUnstructured() *unstructured.Unstructured {
	vr := &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: "apps", Generation: 3}, Spec: backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"}}
	object, err := runtime.DefaultUnstructuredConverter.ToUnstructured(vr)
	if err != nil {
		panic(err)
	}
	return &unstructured.Unstructured{Object: object}
}

// populatedOperations returns a fake cluster that holds only the repository
// Secret repo-secret in apps.
func populatedOperations() *fakeOperations {
	ops := newFakeOperations()
	ops.secrets[namespacedName("apps", "repo-secret")] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "repo-secret", Namespace: "apps"}, Data: map[string][]byte{"repository": []byte("s3://bucket")}}
	return ops
}

// restoringOperations returns a fake cluster in the state a finished Populate
// leaves: the repository Secret copied into the controller namespace and the
// destination for claim-123. Complete and Cleanup read those objects, so their
// tests start from this state.
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

// namespacedName returns the namespace/name key the fake stores objects under.
func namespacedName(namespace, name string) string { return namespace + "/" + name }

// findCondition returns the condition of the given type, or nil when there is
// none.
func findCondition(conditions []metav1.Condition, typ string) *metav1.Condition {
	for i := range conditions {
		if conditions[i].Type == typ {
			return &conditions[i]
		}
	}
	return nil
}

// mapKeys returns a map's keys, for failure messages.
func mapKeys[T any](values map[string]T) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	return keys
}

var _ Operations = (*fakeOperations)(nil)

// TestCleanupToleratesTheAlreadyDeletedPrimeClaim checks that Cleanup succeeds
// without a prime claim and still writes the status that ends the restore.
//
// api.md's status table says the Ready condition is True when no claim is
// being filled from the VolumeRestore, and that claims[] holds only the claims
// being populated now. Cleanup is the only callback that runs when a restore
// ends, so it's the one place that can remove the entry and report the
// VolumeRestore Ready. The library deletes the prime claim after it calls
// PopulateCleanupFn, so every pass after the one that finished the restore
// arrives without it. Rejecting those passes failed the sync, and the library
// requeues on error, so one restored claim errored several times a second for
// as long as it existed:
//
//	error syncing 'pvc/canary-backup/canary-backup': populator parameters
//	have no prime PVC, requeuing
//
// Cleanup exists to remove things, and the prime claim being gone is the state
// it works towards.
func TestCleanupToleratesTheAlreadyDeletedPrimeClaim(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system", nil)
	params := paramsWithClaims(backupv1alpha1.ClaimRestoreStatus{
		Name: "notes", UID: types.UID("claim-123"), Phase: backupv1alpha1.RestorePhaseRestoring,
	})
	params.PvcPrime = nil

	if err := callbacks.Cleanup(context.Background(), params); err != nil {
		t.Fatalf("Cleanup() without a prime claim = %v, want it tolerated", err)
	}
	if len(ops.statuses) != 1 {
		t.Errorf("status writes = %d, want the restore still reported as ended", len(ops.statuses))
	}
}

// TestCleanupWritesNothingWhenThereIsNothingToChange checks that a second
// Cleanup with nothing to change writes no status.
//
// The library has no early return for a claim it has already populated. Every
// resync walks the whole path, reaches the completion branch and calls Cleanup
// again, for the life of the claim. Writing status each time would send one
// update per volume every resync interval, forever, and none of them would
// change anything. It was seen as a PopulatorFinished event that repeated on a
// claim bound long before. The event is the library's own and harmless. The
// status writes behind it were the problem.
func TestCleanupWritesNothingWhenThereIsNothingToChange(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system", nil)
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
	// from the cluster. The claim is already retired and the condition already
	// says so, so the second call has nothing to write.
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

// TestCleanupRetiresTheClaimAndReportsRestored checks that Cleanup removes the
// entry of the last claim being filled and sets Ready to True with reason
// Restored.
func TestCleanupRetiresTheClaimAndReportsRestored(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system", nil)
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

// TestCleanupLeavesOtherClaimsRestoring checks that Cleanup removes only the
// finished claim's entry. A VolumeRestore can populate more than one claim at
// a time, so the other claims stay reported, and Ready stays False with reason
// Restoring.
func TestCleanupLeavesOtherClaimsRestoring(t *testing.T) {
	ops := restoringOperations()
	callbacks := New(ops, "backup-system", nil)
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

// paramsWithClaims returns the default params with a VolumeRestore whose
// status.claims holds the given entries.
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
