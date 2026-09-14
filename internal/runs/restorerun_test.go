package runs

import (
	"context"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

const restoreUID = types.UID("9b7d4e21")

func volumeRestore() *backupv1alpha1.VolumeRestore {
	class := "zfs-ephemeral"
	return &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-backup", Namespace: namespace},
		Spec: backupv1alpha1.VolumeRestoreSpec{
			Repository:            "canary-backup-restic",
			CacheStorageClassName: &class,
			MoverPodLabels: map[string]backupv1alpha1.MoverPodLabelValue{
				"kueue.x-k8s.io/queue-name": "backups",
			},
		},
	}
}

// populatedClaim is the ordinary shape: a dynamic claim naming its VolumeRestore,
// which is where a run reads the repository and the mover's settings from.
func populatedClaim() *corev1.PersistentVolumeClaim {
	class := "zfs"
	group := backupv1alpha1.GroupVersion.Group
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "canary-backup", Namespace: namespace,
			Annotations: map[string]string{selectedNodeAnnotation: "talos-unraid-ums-w-1"},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
			DataSourceRef: &corev1.TypedObjectReference{
				APIGroup: &group, Kind: "VolumeRestore", Name: "canary-backup",
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

func restoreRun(mutate ...func(*backupv1alpha1.RestoreRun)) *backupv1alpha1.RestoreRun {
	run := &backupv1alpha1.RestoreRun{
		ObjectMeta: metav1.ObjectMeta{
			Name: "back-to-friday", Namespace: namespace, UID: restoreUID, Generation: 1,
			Finalizers: []string{Finalizer},
		},
		Spec: backupv1alpha1.RestoreRunSpec{
			Claim:   "canary-backup",
			Timeout: &metav1.Duration{Duration: 4 * time.Hour},
		},
	}
	for _, m := range mutate {
		m(run)
	}
	return run
}

func restoreReconciler(t *testing.T, objects ...client.Object) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	c := fake.NewClientBuilder().
		WithScheme(scheme(t)).
		WithObjects(objects...).
		WithStatusSubresource(&backupv1alpha1.RestoreRun{}, &volsyncv1alpha1.ReplicationDestination{}).
		Build()
	return &RestoreRunReconciler{Client: c, Now: func() time.Time { return frozen }}, c
}

func runRestore(t *testing.T, r *RestoreRunReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{
		NamespacedName: types.NamespacedName{Namespace: namespace, Name: "back-to-friday"},
	})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

func readRestore(t *testing.T, c client.Client) *backupv1alpha1.RestoreRun {
	t.Helper()
	got := &backupv1alpha1.RestoreRun{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: "back-to-friday"}, got); err != nil {
		t.Fatalf("read the run back: %v", err)
	}
	return got
}

// The claim is ReadWriteOnce, which restricts it to one node rather than to one
// pod, so Kubernetes would let the app mount it beside the mover. Two writers on
// one filesystem is how the volume being restored is corrupted.
func TestInPlaceWaitsWhileAPodHoldsTheClaim(t *testing.T) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "canary-backup-58789985b7-k5lvz", Namespace: namespace},
		Spec: corev1.PodSpec{Volumes: []corev1.Volume{{
			Name: "data",
			VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
				ClaimName: "canary-backup",
			}},
		}}},
	}
	r, c := restoreReconciler(t, restoreRun(), populatedClaim(), volumeRestore(), pod)

	runRestore(t, r)

	got := readRestore(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseWaiting {
		t.Fatalf("phase = %q, want Waiting", got.Status.Phase)
	}
	if reason := readyReason(got.Status.Conditions); reason != backupv1alpha1.ReasonClaimInUse {
		t.Errorf("reason = %q, want ClaimInUse", reason)
	}

	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil {
		t.Fatalf("list destinations: %v", err)
	}
	if len(destinations.Items) != 0 {
		t.Errorf("destinations = %d, want none while a pod holds the claim", len(destinations.Items))
	}
}

func TestInPlaceStartsOnceTheClaimIsFree(t *testing.T) {
	r, c := restoreReconciler(t, restoreRun(func(rr *backupv1alpha1.RestoreRun) {
		asOf := "2026-09-13T00:00:00Z"
		rr.Spec.RestoreAsOf = &asOf
	}), populatedClaim(), volumeRestore())

	runRestore(t, r)

	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil {
		t.Fatalf("list destinations: %v", err)
	}
	if len(destinations.Items) != 1 {
		t.Fatalf("destinations = %d, want 1", len(destinations.Items))
	}

	spec := destinations.Items[0].Spec.Restic
	if spec.CopyMethod != volsyncv1alpha1.CopyMethodDirect {
		t.Errorf("copy method = %q, want Direct", spec.CopyMethod)
	}
	if spec.DestinationPVC == nil || *spec.DestinationPVC != "canary-backup" {
		t.Errorf("destinationPVC = %#v, want the claim the app uses", spec.DestinationPVC)
	}
	// Read off the claim's VolumeRestore rather than restated on the run, so a
	// restore cannot disagree with the backups it restores from.
	if spec.Repository != "canary-backup-restic" {
		t.Errorf("repository = %q, want it taken from the VolumeRestore", spec.Repository)
	}
	if spec.CacheStorageClassName == nil || *spec.CacheStorageClassName != "zfs-ephemeral" {
		t.Errorf("cache class = %#v, want zfs-ephemeral from the VolumeRestore", spec.CacheStorageClassName)
	}
	if spec.MoverPodLabels["kueue.x-k8s.io/queue-name"] != "backups" {
		t.Errorf("mover labels = %#v, want the queue label from the VolumeRestore", spec.MoverPodLabels)
	}
	if spec.RestoreAsOf == nil || *spec.RestoreAsOf != "2026-09-13T00:00:00Z" {
		t.Errorf("restoreAsOf = %#v, want the run's point in time", spec.RestoreAsOf)
	}
	if !spec.EnableFileDeletion {
		t.Error("enableFileDeletion is off, so the volume would hold a merge of the snapshot and what was there")
	}

	if phase := readRestore(t, c).Status.Phase; phase != backupv1alpha1.RunPhaseRunning {
		t.Errorf("phase = %q, want Running", phase)
	}
}

func TestInPlaceRemovesTheDestinationWhenItFinishes(t *testing.T) {
	started := metav1.NewTime(frozen.Add(-time.Minute))
	destination := directDestination(restoreRun(), restoreSettings{Secret: "canary-backup-restic"}, "restore-"+string(restoreUID))
	destination.Status = &volsyncv1alpha1.ReplicationDestinationStatus{LastManualSync: string(restoreUID)}

	r, c := restoreReconciler(t,
		restoreRun(func(rr *backupv1alpha1.RestoreRun) {
			rr.Status.Phase = backupv1alpha1.RunPhaseRunning
			rr.Status.Destination = "restore-" + string(restoreUID)
			rr.Status.StartedAt = &started
		}),
		populatedClaim(), volumeRestore(), destination,
	)

	runRestore(t, r)

	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil {
		t.Fatalf("list destinations: %v", err)
	}
	if len(destinations.Items) != 0 {
		t.Errorf("destinations = %d, want the run to remove its own", len(destinations.Items))
	}
	got := readRestore(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Errorf("phase = %q, want Succeeded", got.Status.Phase)
	}
	if len(got.Finalizers) != 0 {
		t.Errorf("finalizers = %v, want none once the run is finished", got.Finalizers)
	}
}

// Deleting a run mid-restore must take its mover with it, or a Direct-mode
// restore keeps writing into a claim with nothing tracking it.
func TestDeletingARestoreRemovesItsDestination(t *testing.T) {
	deleted := metav1.NewTime(frozen)
	destination := directDestination(restoreRun(), restoreSettings{Secret: "canary-backup-restic"}, "restore-"+string(restoreUID))

	r, c := restoreReconciler(t,
		restoreRun(func(rr *backupv1alpha1.RestoreRun) {
			rr.DeletionTimestamp = &deleted
			rr.Status.Phase = backupv1alpha1.RunPhaseRunning
			rr.Status.Destination = "restore-" + string(restoreUID)
		}),
		populatedClaim(), volumeRestore(), destination,
	)

	runRestore(t, r)

	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil {
		t.Fatalf("list destinations: %v", err)
	}
	if len(destinations.Items) != 0 {
		t.Errorf("destinations = %d, want deletion to remove the mover", len(destinations.Items))
	}
	if err := c.Get(context.Background(),
		types.NamespacedName{Namespace: namespace, Name: "back-to-friday"},
		&backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("reading the run back gave %v, want NotFound once the finalizer is dropped", err)
	}
}

// The Into mode writes a VolumeRestore carrying the point in time and a claim
// naming it, so the populator path does the work and the app's volume is never
// touched. Nothing has to stop for it.
func TestIntoCreatesASecondVolumeAndTouchesNothingElse(t *testing.T) {
	asOf := "2026-09-13T00:00:00Z"
	r, c := restoreReconciler(t,
		restoreRun(func(rr *backupv1alpha1.RestoreRun) {
			rr.Spec.Into = "canary-backup-friday"
			rr.Spec.RestoreAsOf = &asOf
		}),
		populatedClaim(), volumeRestore(),
	)

	runRestore(t, r)

	created := &backupv1alpha1.VolumeRestore{}
	key := types.NamespacedName{Namespace: namespace, Name: "canary-backup-friday"}
	if err := c.Get(context.Background(), key, created); err != nil {
		t.Fatalf("read the new VolumeRestore: %v", err)
	}
	if created.Spec.RestoreAsOf == nil || *created.Spec.RestoreAsOf != asOf {
		t.Errorf("restoreAsOf = %#v, want the run's point in time", created.Spec.RestoreAsOf)
	}
	if created.Spec.Repository != "canary-backup-restic" {
		t.Errorf("repository = %q, want the source claim's", created.Spec.Repository)
	}

	claim := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), key, claim); err != nil {
		t.Fatalf("read the scratch claim: %v", err)
	}
	if claim.Spec.DataSourceRef == nil || claim.Spec.DataSourceRef.Name != "canary-backup-friday" {
		t.Errorf("dataSourceRef = %#v, want the new VolumeRestore", claim.Spec.DataSourceRef)
	}
	if got := claim.Spec.Resources.Requests[corev1.ResourceStorage]; got.String() != "1Gi" {
		t.Errorf("size = %s, want the source claim's 1Gi", got.String())
	}
	// Without this the claim is never filled at all. On a WaitForFirstConsumer
	// class the populator library waits for selected-node before it creates
	// anything, and that annotation is written by the scheduler when a pod
	// using the claim is scheduled. A scratch claim has no pod, so it stayed
	// Pending for as long as it existed: observed on the walzen test cluster,
	// eight minutes with no prime claim and nothing in the controller's log.
	if got := claim.Annotations[selectedNodeAnnotation]; got != "talos-unraid-ums-w-1" {
		t.Errorf("selected node = %q, want the source claim's, or nothing ever provisions this", got)
	}

	// No ReplicationDestination at all: this mode never mounts the app's claim.
	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil {
		t.Fatalf("list destinations: %v", err)
	}
	if len(destinations.Items) != 0 {
		t.Errorf("destinations = %d, want none for an Into restore", len(destinations.Items))
	}
}

func TestAFixedNameClaimNeedsARepositoryNamed(t *testing.T) {
	fixed := populatedClaim()
	fixed.Spec.DataSourceRef = nil
	fixed.Spec.VolumeName = "canary-backup"

	r, c := restoreReconciler(t, restoreRun(), fixed)

	runRestore(t, r)

	got := readRestore(t, c)
	if got.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", got.Status.Phase)
	}
	if reason := readyReason(got.Status.Conditions); reason != backupv1alpha1.ReasonInvalid {
		t.Errorf("reason = %q, want Invalid", reason)
	}
}
