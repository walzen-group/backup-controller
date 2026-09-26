package runs

import (
	"context"
	"slices"
	"strings"
	"testing"

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
// controller started through the VolumeRestore populator ends with reason
// Upgraded (see upgrade_test.go), and its deletion releases the VolumeRestore
// that release created.

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
// checks: Running, with the run's finalizer, no status.plannedBy, and one
// Running item that records monday's snapshot and names no
// ReplicationDestination.
func populatorRun() *backupv1alpha1.RestoreRun {
	started := metav1.NewTime(frozen)
	return restoreRun(intoMonday, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.PlannedBy = ""
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

// A run v0.8.1 started through the populator whose VolumeRestore exists and
// whose claim does not (that release created the one and stopped before the
// other, or the claim was deleted) ends with reason Upgraded and creates
// nothing. The populator never took the VolumeRestore on, and neither its
// Cleanup nor its OrphanReconciler takes populator.Finalizer off a
// VolumeRestore with no claim, so the run keeps its own finalizer, and
// deleting the run releases the VolumeRestore (see releaseVolumeRestore).
func TestAnUpgradedPopulatorRestoreWithoutItsClaimReleasesItsVolumeRestoreWhenDeleted(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(), populatorRestore(run, populator.Finalizer))
	restoreStep(t, r)

	ended := readRestoreRun(t, c)
	if ended.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(ended.Status.Conditions) != backupv1alpha1.ReasonUpgraded {
		t.Fatalf("phase = %q, reason = %q; want Failed, %s", ended.Status.Phase, readyReason(ended.Status.Conditions), backupv1alpha1.ReasonUpgraded)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
		t.Errorf("claim notes-data-monday: %v, want none created", err)
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want none", names)
	}
	// The release waits only for the claim, which is gone here. The run
	// keeps its finalizer until it is deleted, and that deletion releases the
	// VolumeRestore, so the garbage collector can delete it with the run.
	held := readRestoreRun(t, c)
	if !slices.Contains(held.Finalizers, Finalizer) {
		t.Errorf("finalizers = %v, want %s kept until the run is deleted", held.Finalizers, Finalizer)
	}
	if err := c.Delete(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	vr := &backupv1alpha1.VolumeRestore{}
	get(t, c, ns, "notes-data-monday", vr)
	if slices.Contains(vr.Finalizers, populator.Finalizer) {
		t.Errorf("VolumeRestore finalizers = %v, want %s released", vr.Finalizers, populator.Finalizer)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("RestoreRun: %v, want it gone once its VolumeRestore is released", err)
	}
}

// A run on the populator path deleted before the populator started on its
// claim takes the populator's finalizer off its VolumeRestore only once its
// claim is gone. While the claim is there the library can still start on it,
// so the VolumeRestore has to stay: the run deletes the claim it created,
// waits for the library to let it go, and only then releases the
// VolumeRestore and drops its own finalizer.
func TestADeletedPopulatorRestoreReleasesItsVolumeRestoreAfterItsClaim(t *testing.T) {
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run, populator.Finalizer), populatorClaim(run, corev1.ClaimPending, populator.ClaimFinalizer))
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r) // the claim goes first

	claim := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "notes-data-monday", claim)
	if claim.DeletionTimestamp == nil {
		t.Fatal("the run did not delete the claim it created")
	}
	if !slices.Contains(claim.Finalizers, populator.ClaimFinalizer) {
		t.Error("the run removed the populator's finalizer from the claim")
	}
	vr := &backupv1alpha1.VolumeRestore{}
	get(t, c, ns, "notes-data-monday", vr)
	if !slices.Contains(vr.Finalizers, populator.Finalizer) {
		t.Error("the VolumeRestore was released while its claim was still there")
	}
	deleting := readRestoreRun(t, c)
	if len(deleting.Finalizers) == 0 {
		t.Error("the run dropped its finalizer while its VolumeRestore was still held")
	}
	if msg := readyMessage(deleting.Status.Conditions); !strings.Contains(msg, "notes-data-monday") || !strings.Contains(msg, "VolumeRestore") {
		t.Errorf("ready message = %q, want it to name the claim its VolumeRestore waits for", msg)
	}

	// The library's cleanup removes its finalizer from the claim, which lets
	// the claim go.
	claim.Finalizers = slices.DeleteFunc(claim.Finalizers, func(f string) bool { return f == populator.ClaimFinalizer })
	if err := c.Update(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	get(t, c, ns, "notes-data-monday", vr)
	if slices.Contains(vr.Finalizers, populator.Finalizer) || vr.DeletionTimestamp != nil {
		t.Errorf("VolumeRestore finalizers = %v, deletion %v; want %s released and the object left to the garbage collector",
			vr.Finalizers, vr.DeletionTimestamp, populator.Finalizer)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("RestoreRun: %v, want it gone once its finalizer is dropped", err)
	}
}

// The populator library's finalizer on a claim has one exact name, and a
// claim whose finalizer merely ends the same way belongs to something else.
// It must not hold the run's VolumeRestore: a match on the ending alone kept
// the VolumeRestore and the run in deletion for good.
func TestAStrangersFinalizerOnTheClaimDoesNotHoldTheRunsVolumeRestore(t *testing.T) {
	stranger := "example.com/populate-target-protection"
	run := populatorRun()
	r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(),
		populatorRestore(run, populator.Finalizer), populatorClaim(run, corev1.ClaimPending, stranger))
	claim := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "notes-data-monday", claim)
	if err := c.Delete(context.Background(), claim); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, r)

	vr := &backupv1alpha1.VolumeRestore{}
	get(t, c, ns, "notes-data-monday", vr)
	if slices.Contains(vr.Finalizers, populator.Finalizer) {
		t.Errorf("VolumeRestore finalizers = %v, want %s released: claim finalizer %s is not the library's",
			vr.Finalizers, populator.Finalizer, stranger)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("RestoreRun: %v, want it gone once its VolumeRestore is released", err)
	}
}
