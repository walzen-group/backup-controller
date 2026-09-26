package runs

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/populator"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file check an into restore from a claim (spec.claim with
// spec.into). It restores through a ReplicationDestination the run creates,
// as an into restore from a repository does, so the mover's log confirms the
// snapshot (designs/restorerun.md C-pop-1, R1). A run that a v0.8.1 or older
// controller started through the VolumeRestore populator ends Failed, because
// the run cannot confirm which snapshot the populator restored (R2).

// sourceOnNode returns the app's claim, as claim does, with the node the
// scheduler selected for its volume.
func sourceOnNode() *corev1.PersistentVolumeClaim {
	source := claim()
	source.Annotations[selectedNodeAnnotation] = "worker-1"
	return source
}

// An into restore from a claim creates a plain claim with the source claim's
// size, class and node and no data source, and a ReplicationDestination in
// the app's namespace whose mover writes the snapshot the checks selected
// into it, with the source's repository, cache class and queue label. No
// VolumeRestore is created. The run succeeds once the mover's log names that
// snapshot, and deletes its destination. Before, the run created a
// VolumeRestore and a claim naming it, and reported Succeeded once the claim
// was Bound, with no evidence of which snapshot the populator restored.
func TestAnIntoRestoreFromAClaimWritesThroughItsOwnDestination(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(intoMonday), sourceOnNode(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create

	run := readRestoreRun(t, c)
	scratch := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "notes-data-monday", scratch)
	request := scratch.Spec.Resources.Requests[corev1.ResourceStorage]
	if request.Cmp(resource.MustParse("1Gi")) != 0 || scratch.Spec.StorageClassName == nil || *scratch.Spec.StorageClassName != "zfs" ||
		scratch.Annotations[selectedNodeAnnotation] != "worker-1" || scratch.Spec.DataSourceRef != nil || scratch.Spec.DataSource != nil {
		t.Fatalf("claim = %+v, annotations %v; want 1Gi of zfs on worker-1 with no data source", scratch.Spec, scratch.Annotations)
	}
	if !metav1.IsControlledBy(scratch, run) {
		t.Errorf("claim owners = %v, want the run as controller", scratch.OwnerReferences)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}, &backupv1alpha1.VolumeRestore{}); !apierrors.IsNotFound(err) {
		t.Errorf("VolumeRestore notes-data-monday: %v, want none created", err)
	}

	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning || item.Destination != destinationName(restoreUID, 0) {
		t.Fatalf("item = %+v; want Running with destination %s", item, destinationName(restoreUID, 0))
	}
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, item.Destination, rd)
	spec := rd.Spec.Restic
	if rd.Spec.Trigger == nil || rd.Spec.Trigger.Manual != string(restoreUID) || spec == nil ||
		spec.CopyMethod != volsyncv1alpha1.CopyMethodDirect || spec.DestinationPVC == nil || *spec.DestinationPVC != "notes-data-monday" ||
		spec.Repository != repoN || spec.RestoreAsOf == nil || *spec.RestoreAsOf != "2026-09-21T05:00:02Z" || spec.Previous != nil ||
		spec.CacheStorageClassName == nil || *spec.CacheStorageClassName != "zfs-ephemeral" || !spec.EnableFileDeletion ||
		spec.MoverPodLabels["kueue.x-k8s.io/queue-name"] != "backups" {
		t.Fatalf("destination = %+v, restic %+v; want monday's snapshot written into notes-data-monday from %s", rd.Spec, spec, repoN)
	}

	completeVolume(t, c)
	run = stepUntilFinished(t, r, c, 2)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded {
		t.Fatalf("phase = %q, item = %+v (%s); want Succeeded", run.Status.Phase, run.Status.Items[0], readyMessage(run.Status.Conditions))
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want the run's deleted", names)
	}
}

// populatorClaim is the claim a v0.8.1 controller created for an into
// restore from a claim: named after spec.into, controlled by the run, on the
// source claim's node, with the run's VolumeRestore as its data source.
func populatorClaim(run *backupv1alpha1.RestoreRun, phase corev1.PersistentVolumeClaimPhase, finalizers ...string) *corev1.PersistentVolumeClaim {
	class := "zfs"
	group := backupv1alpha1.GroupVersion.Group
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: run.Spec.Into, Namespace: ns, Finalizers: finalizers,
			Annotations:     map[string]string{selectedNodeAnnotation: "worker-1"},
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind("RestoreRun"))},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
			DataSourceRef: &corev1.TypedObjectReference{APIGroup: &group, Kind: "VolumeRestore", Name: run.Spec.Into},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: phase},
	}
}

// populatorRestore is the VolumeRestore a v0.8.1 controller created for an
// into restore from a claim: named after spec.into, controlled by the run,
// pinned to the time of the snapshot the checks selected, with the given
// finalizers. The controller created it with populator.Finalizer, and the
// populator's Cleanup removes that once the claim is filled.
func populatorRestore(run *backupv1alpha1.RestoreRun, finalizers ...string) *backupv1alpha1.VolumeRestore {
	class := "zfs-ephemeral"
	moment := "2026-09-21T05:00:02Z"
	return &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name: run.Spec.Into, Namespace: ns, Finalizers: finalizers,
			OwnerReferences: []metav1.OwnerReference{*metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind("RestoreRun"))},
		},
		Spec: backupv1alpha1.VolumeRestoreSpec{
			Repository: repoN, RestoreAsOf: &moment, CacheStorageClassName: &class,
			MoverPodLabels: map[string]backupv1alpha1.MoverPodLabelValue{"kueue.x-k8s.io/queue-name": "backups"},
		},
	}
}

// populatorRun returns the RestoreRun back-to-monday, an into restore from a
// claim into notes-data-monday, as a v0.8.1 controller left it after its
// checks: Running, with the run's finalizer, and one Running item that
// records monday's snapshot and names no ReplicationDestination.
func populatorRun() *backupv1alpha1.RestoreRun {
	started := metav1.NewTime(frozen)
	return restoreRun(intoMonday, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = &started
		r.Status.Target = r.Spec.Into
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: r.Spec.Into,
			Phase: backupv1alpha1.ItemRunning, Snapshot: monday.ShortID(), SnapshotTime: &metav1.Time{Time: monday.Time}}}
	})
}

// snapshotVersions reads each of objects by its name and returns its
// resourceVersion now, for expectUnchanged.
func snapshotVersions(t *testing.T, c client.Client, objects ...client.Object) map[client.Object]string {
	t.Helper()
	versions := map[client.Object]string{}
	for _, object := range objects {
		get(t, c, ns, object.GetName(), object)
		versions[object] = object.GetResourceVersion()
	}
	return versions
}

// expectUnchanged checks that every object in versions is still there with
// the resourceVersion recorded for it.
func expectUnchanged(t *testing.T, c client.Client, versions map[client.Object]string) {
	t.Helper()
	for object, version := range versions {
		key := client.ObjectKeyFromObject(object)
		if err := c.Get(context.Background(), key, object); err != nil {
			t.Errorf("%T %s: %v, want it left in place", object, key, err)
			continue
		}
		if object.GetResourceVersion() != version || object.GetDeletionTimestamp() != nil {
			t.Errorf("%T %s changed: resourceVersion %s -> %s, deletionTimestamp %v", object, key, version, object.GetResourceVersion(), object.GetDeletionTimestamp())
		}
	}
}

// expectPopulatorRunFailed checks that the run ended Failed with reason
// Failed, its item Failed, and that the item's and the Ready message hold
// every string in want. It also checks that the run's finalizer is gone.
func expectPopulatorRunFailed(t *testing.T, c client.Client, want ...string) {
	t.Helper()
	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed ||
		run.Status.Items[0].Phase != backupv1alpha1.ItemFailed {
		t.Fatalf("phase = %q, reason = %q, item = %+v (%s); want Failed", run.Status.Phase, readyReason(run.Status.Conditions),
			run.Status.Items[0], readyMessage(run.Status.Conditions))
	}
	for _, w := range want {
		if !strings.Contains(run.Status.Items[0].Message, w) || !strings.Contains(readyMessage(run.Status.Conditions), w) {
			t.Errorf("item message = %q, Ready message = %q; want both to hold %q", run.Status.Items[0].Message, readyMessage(run.Status.Conditions), w)
		}
	}
	if slices.Contains(run.Finalizers, Finalizer) {
		t.Errorf("finalizers = %v, want %s dropped", run.Finalizers, Finalizer)
	}
}

// A run a v0.8.1 controller started through the populator whose claim is
// Bound ends Failed: the populator's mover log went with its destination, so
// nothing confirms which snapshot the claim holds. The message says so and
// what to do, and the run leaves the claim and the VolumeRestore as they
// are. Before, the run reported Succeeded.
func TestAPopulatorRestoreWhoseClaimIsBoundEndsFailed(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run), populatorClaim(run, corev1.ClaimBound))
	left := snapshotVersions(t, c, &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday"}})
	restoreStep(t, r)

	expectPopulatorRunFailed(t, c, "started by v0.8.1 or older through the VolumeRestore populator",
		"cannot confirm which snapshot", "The checks selected snapshot 6e473100",
		"Claim notes-data-monday is Bound", "leaves it and VolumeRestore notes-data-monday in place",
		"a new RestoreRun", "delete this RestoreRun first")
	expectUnchanged(t, c, left)
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want none", names)
	}
}

// A run on the populator path whose claim has not bound waits for the
// populator and creates nothing of its own; once the claim binds, it ends
// Failed as above. Past spec.timeout it ends TimedOut. Neither touches the
// claim or the VolumeRestore.
func TestAPopulatorRestoreWaitsForItsClaimAndCreatesNothing(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run, populator.Finalizer), populatorClaim(run, corev1.ClaimPending, populator.ClaimFinalizer))
	restoreStep(t, r)

	if got := readRestoreRun(t, c); got.Status.Phase.Finished() || got.Status.Items[0].Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("phase = %q, item = %+v; want the run waiting for the populator", got.Status.Phase, got.Status.Items[0])
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Fatalf("destinations = %v, want none", names)
	}

	scratch := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "notes-data-monday", scratch)
	scratch.Finalizers = nil
	if err := c.Update(context.Background(), scratch); err != nil {
		t.Fatal(err)
	}
	scratch.Status.Phase = corev1.ClaimBound
	if err := c.Status().Update(context.Background(), scratch); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	expectPopulatorRunFailed(t, c, "cannot confirm which snapshot", "Claim notes-data-monday is Bound")
	get(t, c, ns, "notes-data-monday", &backupv1alpha1.VolumeRestore{})
}

// A run on the populator path whose claim has not bound by spec.timeout ends
// TimedOut, and leaves the claim and the VolumeRestore to the populator,
// which is still at work on them.
func TestAPopulatorRestoreWhoseClaimDoesNotBindTimesOut(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run, populator.Finalizer), populatorClaim(run, corev1.ClaimPending, populator.ClaimFinalizer))
	left := snapshotVersions(t, c, &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday"}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday"}})
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	restoreStep(t, r)

	got := readRestoreRun(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(got.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Fatalf("phase = %q, reason = %q; want Failed, TimedOut", got.Status.Phase, readyReason(got.Status.Conditions))
	}
	expectUnchanged(t, c, left)
}

// A run on the populator path whose VolumeRestore exists and whose claim
// does not (a v0.8.1 pass created the one and stopped before the other, or
// the claim was deleted) ends Failed and creates nothing: a claim it created
// now would be filled by the populator, which it can't check. Before, it
// created the claim.
func TestAPopulatorRestoreWithoutItsClaimFails(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(), populatorRestore(run, populator.Finalizer))
	restoreStep(t, r)

	expectPopulatorRunFailed(t, c, "started by v0.8.1 or older through the VolumeRestore populator",
		"claim notes-data-monday, which the populator fills, does not exist", "Create a new RestoreRun")
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Errorf("claim notes-data-monday: %v, want none created", err)
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want none", names)
	}
	// The populator never took the VolumeRestore on, so the run releases it
	// and the garbage collector can delete it with the run.
	vr := &backupv1alpha1.VolumeRestore{}
	get(t, c, ns, "notes-data-monday", vr)
	if slices.Contains(vr.Finalizers, populator.Finalizer) {
		t.Errorf("VolumeRestore finalizers = %v, want %s released", vr.Finalizers, populator.Finalizer)
	}
}

// A run on the populator path whose claim name is taken by a claim it did
// not create ends Failed naming it, and leaves that claim alone.
func TestAPopulatorRestoreLeavesAClaimItDidNotCreate(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run), boundClaim("notes-data-monday"))
	left := snapshotVersions(t, c, &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday"}})
	restoreStep(t, r)

	expectPopulatorRunFailed(t, c, "claim notes-data-monday already exists and this run did not create it")
	expectUnchanged(t, c, left)
}

// A run a v0.8.1 controller planned for the populator that had created
// nothing yet goes through a ReplicationDestination of its own, like a new
// run: nothing was written, and the snapshot it recorded is checked again
// before the claim is created. Before, it created a VolumeRestore.
func TestARestorePlannedForThePopulatorGoesThroughADestination(t *testing.T) {
	r, c := restoreReconciler(t, nil, populatorRun(), sourceOnNode(), volumeRestore(), repository())
	stepUntil(t, r, c, 3, func(run *backupv1alpha1.RestoreRun) bool { return len(destinations(t, c)) > 0 })

	run := readRestoreRun(t, c)
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Destination != destinationName(restoreUID, 0) {
		t.Fatalf("item = %+v (%s); want Running with destination %s", item, readyMessage(run.Status.Conditions), destinationName(restoreUID, 0))
	}
	scratch := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "notes-data-monday", scratch)
	if scratch.Spec.DataSourceRef != nil || scratch.Annotations[selectedNodeAnnotation] != "worker-1" {
		t.Errorf("claim dataSourceRef = %v, annotations %v; want none, on worker-1", scratch.Spec.DataSourceRef, scratch.Annotations)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}, &backupv1alpha1.VolumeRestore{}); !apierrors.IsNotFound(err) {
		t.Errorf("VolumeRestore notes-data-monday: %v, want none created", err)
	}

	completeVolume(t, c)
	if run := stepUntilFinished(t, r, c, 2); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, message = %q; want Succeeded", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}

// stepUntil reconciles the run back-to-monday until done reports true for
// it, at most n times, and fails the test when it never does.
func stepUntil(t *testing.T, r *RestoreRunReconciler, c client.Client, n int, done func(*backupv1alpha1.RestoreRun) bool) {
	t.Helper()
	for range n {
		restoreStep(t, r)
		if done(readRestoreRun(t, c)) {
			return
		}
	}
	t.Fatalf("run = %+v after %d passes", readRestoreRun(t, c).Status, n)
}

// A run on the populator path deleted before the populator started on its
// claim takes the populator's finalizer off its VolumeRestore, so the
// VolumeRestore does not hang in deletion once the run is gone. It deletes
// neither the claim nor the VolumeRestore itself; the garbage collector does
// that after the run is gone.
func TestADeletedPopulatorRestoreReleasesItsVolumeRestore(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run, populator.Finalizer), populatorClaim(run, corev1.ClaimPending))
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	vr := &backupv1alpha1.VolumeRestore{}
	get(t, c, ns, "notes-data-monday", vr)
	if slices.Contains(vr.Finalizers, populator.Finalizer) || vr.DeletionTimestamp != nil {
		t.Errorf("VolumeRestore finalizers = %v, deletion %v; want %s released and the object left to the garbage collector",
			vr.Finalizers, vr.DeletionTimestamp, populator.Finalizer)
	}
	scratch := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "notes-data-monday", scratch)
	if scratch.DeletionTimestamp != nil {
		t.Error("the run deleted its claim itself")
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("RestoreRun: %v, want it gone once its finalizer is dropped", err)
	}
}
