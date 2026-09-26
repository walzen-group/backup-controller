package runs

import (
	"context"
	"strings"
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
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// oldRestoreUID is the UID of the RestoreRun old-restore, an earlier run
// that restored into the same claim name.
const oldRestoreUID = types.UID("9b7d4e21-0000-4000-8000-000000000009")

// oldRestoreRef returns the controller reference the RestoreRun old-restore
// put on the objects it created.
func oldRestoreRef() metav1.OwnerReference {
	old := &backupv1alpha1.RestoreRun{ObjectMeta: metav1.ObjectMeta{Name: "old-restore", Namespace: ns, UID: oldRestoreUID}}
	return *metav1.NewControllerRef(old, backupv1alpha1.GroupVersion.WithKind("RestoreRun"))
}

// intoApp is a mutate function for restoreRun that restores the repository
// Secret into the app's own claim, notes-data, the way a user who read the
// old refusal ("spec.into is required when spec.repository names the
// source") would write it.
func intoApp(r *backupv1alpha1.RestoreRun) {
	size := resource.MustParse("1Gi")
	r.Spec.Repository, r.Spec.Into, r.Spec.IntoSize = repoN, claimN, &size
}

// intoMonday is a mutate function for restoreRun that restores the claim's
// backups into a new claim named notes-data-monday, through the populator.
func intoMonday(r *backupv1alpha1.RestoreRun) {
	r.Spec.Claim, r.Spec.Into = claimN, "notes-data-monday"
}

// boundClaim returns a Bound claim with the given name and owner references,
// which no run created.
func boundClaim(name string, owners ...metav1.OwnerReference) *corev1.PersistentVolumeClaim {
	class := "zfs"
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, OwnerReferences: owners},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			VolumeName:       "pvc-" + name,
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

// foreignDestination returns a ReplicationDestination with the name the run
// back-to-monday gives its first item's destination, whose manual trigger
// is another run's UID.
func foreignDestination(claimName string) *volsyncv1alpha1.ReplicationDestination {
	return &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: destinationName(restoreUID, 0), Namespace: ns},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: string(oldRestoreUID)},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodDirect, DestinationPVC: &claimName,
				},
				Repository: repoN,
			},
		},
	}
}

// stepUntilFinished reconciles the run back-to-monday until it has finished,
// at most n times, and returns it.
func stepUntilFinished(t *testing.T, r *RestoreRunReconciler, c client.Client, n int) *backupv1alpha1.RestoreRun {
	t.Helper()
	for range n {
		if run := readRestoreRun(t, c); run.Status.Phase.Finished() {
			return run
		}
		restoreStep(t, r)
	}
	return readRestoreRun(t, c)
}

// An into restore from a repository whose spec.into names the app's live
// claim ends Invalid at its checks. It writes nothing: no destination, and
// the claim is as it was. Before, the run created a Direct destination with
// enableFileDeletion on the mounted claim and ended Succeeded.
func TestAnIntoRestoreRefusesAnExistingClaim(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(intoApp), claim(), volumeRestore(), repository(), writerPod())
	before := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, claimN, before)

	restoreStep(t, r) // plan
	if names := destinations(t, c); len(names) == 0 {
		restoreStep(t, r)
	}
	if names := destinations(t, c); len(names) > 0 {
		completeVolume(t, c)
	}
	run := stepUntilFinished(t, r, c, 3)

	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
		!strings.Contains(readyMessage(run.Status.Conditions), "claim notes-data already exists and this run did not create it") {
		t.Fatalf("phase = %q, reason = %q, message = %q; want Invalid saying the claim exists",
			run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if names := destinations(t, c); len(names) > 0 {
		t.Errorf("destinations = %v, want none", names)
	}
	after := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, claimN, after)
	if after.ResourceVersion != before.ResourceVersion || len(after.OwnerReferences) != 0 {
		t.Errorf("claim changed: resourceVersion %s -> %s, owners %v", before.ResourceVersion, after.ResourceVersion, after.OwnerReferences)
	}
}

// An into restore through the populator whose spec.into names an existing
// Bound claim ends Invalid at its checks, and creates no VolumeRestore. The
// same holds for a VolumeRestore of that name, which may be the app's own.
// Before, the run found the claim Bound and ended Succeeded with nothing
// restored.
func TestAnIntoRestoreThroughThePopulatorRefusesAnExistingClaim(t *testing.T) {
	for name, existing := range map[string]client.Object{
		"claim": boundClaim("notes-data-monday"),
		"VolumeRestore": &backupv1alpha1.VolumeRestore{
			ObjectMeta: metav1.ObjectMeta{Name: "notes-data-monday", Namespace: ns},
			Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: repoN},
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(intoMonday), claim(), volumeRestore(), repository(), existing)
			run := stepUntilFinished(t, r, c, 4)

			want := name + " notes-data-monday already exists and this run did not create it"
			if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid || !strings.Contains(readyMessage(run.Status.Conditions), want) {
				t.Fatalf("phase = %q, reason = %q, message = %q; want Invalid saying %q",
					run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions), want)
			}
			if name == "claim" {
				vr := &backupv1alpha1.VolumeRestore{}
				if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}, vr); err == nil {
					t.Error("a VolumeRestore was created for a claim the run did not create")
				}
			}
		})
	}
}

// A claim with the spec.into name that appears between the checks and the
// create ends the run Failed, with no destination and no VolumeRestore
// written against it. Before, the run adopted it.
func TestAnIntoRestoreRefusesAClaimCreatedAfterItsChecks(t *testing.T) {
	for name, mutate := range map[string]func(*backupv1alpha1.RestoreRun){
		"from a repository": fromRepository,
		"from a claim":      func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "scratch" },
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(mutate), claim(), volumeRestore(), repository())
			restoreStep(t, r) // plan
			if err := c.Create(context.Background(), boundClaim("scratch")); err != nil {
				t.Fatal(err)
			}
			if names := destinations(t, c); len(names) == 0 {
				restoreStep(t, r)
			}
			run := stepUntilFinished(t, r, c, 3)

			if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed ||
				!strings.Contains(readyMessage(run.Status.Conditions), "claim scratch already exists and this run did not create it") {
				t.Fatalf("phase = %q, reason = %q, message = %q; want Failed saying the claim exists",
					run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
			}
			if names := destinations(t, c); len(names) > 0 {
				t.Errorf("destinations = %v, want none", names)
			}
			vr := &backupv1alpha1.VolumeRestore{}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "scratch"}, vr); err == nil {
				t.Error("a VolumeRestore was created for a claim the run did not create")
			}
		})
	}
}

// A run created again with the same spec.into, while the old run still
// exists, does not adopt the claim the old run created: the garbage
// collector deletes that claim with the old run. The new run ends Invalid
// and names the old run.
func TestARetriedIntoRestoreDoesNotAdoptTheOldRunsClaim(t *testing.T) {
	for name, mutate := range map[string]func(*backupv1alpha1.RestoreRun){
		"from a repository": fromRepository,
		"from a claim":      func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "scratch" },
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(mutate), claim(), volumeRestore(), repository(),
				boundClaim("scratch", oldRestoreRef()))
			restoreStep(t, r)
			if names := destinations(t, c); len(names) == 0 {
				restoreStep(t, r)
			}
			if names := destinations(t, c); len(names) > 0 {
				completeVolume(t, c)
			}
			run := stepUntilFinished(t, r, c, 3)

			if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
				!strings.Contains(readyMessage(run.Status.Conditions), "it belongs to RestoreRun old-restore") {
				t.Fatalf("phase = %q, reason = %q, message = %q; want Invalid naming RestoreRun old-restore",
					run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
			}
			if names := destinations(t, c); len(names) > 0 {
				t.Errorf("destinations = %v, want none", names)
			}
		})
	}
}

// A pass whose claim create went through but came back as an error is
// retried, and the next pass finds the claim the run created and goes on
// to create the destination. The ownership check must not refuse the run's
// own claim.
func TestAnIntoRestoreWhoseClaimCreateWasLostContinues(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan

	lost := false
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := cl.Create(ctx, obj, opts...); err != nil {
				return err
			}
			if _, ok := obj.(*corev1.PersistentVolumeClaim); ok && !lost {
				lost = true
				return apierrors.NewServerTimeout(corev1.Resource("persistentvolumeclaims"), "create", 1)
			}
			return nil
		},
	})
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose claim create came back as an error succeeded, want the error returned")
	}
	restoreStep(t, r)

	if names := destinations(t, c); len(names) != 1 {
		t.Fatalf("destinations = %v, want the run's one (run %+v)", names, readRestoreRun(t, c).Status)
	}
	completeVolume(t, c)
	run := stepUntilFinished(t, r, c, 2)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, message = %q; want Succeeded", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}

// A ReplicationDestination with the name the run would give its own, whose
// trigger is another run's UID, ends the item Failed at once, in place and
// into a new claim. The run never deletes it: it did not create it.
// Before, the run adopted it and waited for it until its timeout, and then
// deleted it.
func TestARestoreRefusesADestinationItDidNotCreate(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*backupv1alpha1.RestoreRun)
		claim  string
	}{
		"in place":          {func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, claimN},
		"from a repository": {fromRepository, "scratch"},
	} {
		t.Run(name, func(t *testing.T) {
			foreign := foreignDestination(tc.claim)
			r, c := restoreReconciler(t, nil, restoreRun(tc.mutate), claim(), volumeRestore(), repository(), foreign)
			restoreStep(t, r) // plan
			restoreStep(t, r) // create
			run := stepUntilFinished(t, r, c, 2)

			if run.Status.Phase != backupv1alpha1.RunPhaseFailed ||
				!strings.Contains(readyMessage(run.Status.Conditions), "does not carry this run's trigger") {
				t.Fatalf("phase = %q, message = %q; want Failed naming the foreign destination",
					run.Status.Phase, readyMessage(run.Status.Conditions))
			}
			kept := &volsyncv1alpha1.ReplicationDestination{}
			get(t, c, ns, foreign.Name, kept)
			if kept.Spec.Trigger.Manual != string(oldRestoreUID) {
				t.Errorf("destination trigger = %q, want the other run's untouched", kept.Spec.Trigger.Manual)
			}
		})
	}
}

// A destination the run's status names but whose trigger is another run's
// is never deleted when the run ends: removeDestinations reads the trigger
// first. Before, a run that timed out deleted it.
func TestRemovingDestinationsLeavesAnotherRunsDestination(t *testing.T) {
	restore, _ := restoring()
	restore.Status.StartedAt = &metav1.Time{Time: frozen}
	foreign := foreignDestination(claimN)
	r, c := restoreReconciler(t, nil, restore, claim(), volumeRestore(), repository(), foreign)
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
	if run := stepUntilFinished(t, r, c, 3); run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want the run timed out", run.Status.Phase)
	}

	kept := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, foreign.Name, kept)
}

// An into restore from a repository whose claim is deleted, or replaced by
// one the run did not create, while the mover writes fails. Before, the run
// ended Succeeded once the mover completed its trigger.
func TestAnIntoRestoreWhoseClaimIsReplacedFails(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create
	scratch := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, "scratch", scratch)
	if err := c.Delete(context.Background(), scratch); err != nil {
		t.Fatal(err)
	}
	if err := c.Create(context.Background(), boundClaim("scratch")); err != nil {
		t.Fatal(err)
	}
	completeVolume(t, c)
	run := stepUntilFinished(t, r, c, 2)

	if run.Status.Phase != backupv1alpha1.RunPhaseFailed ||
		!strings.Contains(readyMessage(run.Status.Conditions), "claim scratch was deleted (or replaced) while the mover wrote into it") {
		t.Fatalf("phase = %q, message = %q; want Failed saying the claim was replaced", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}

// A restore from spec.repository without spec.into is refused with a message
// that sends the user to spec.claim for an existing claim, and says spec.into
// must name a claim that does not exist yet.
func TestAnIntoRestoreFromARepositoryNamesTheInPlaceShape(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Repository = repoN }), repository())
	restoreStep(t, r)

	message := readyMessage(readRestoreRun(t, c).Status.Conditions)
	for _, want := range []string{"must name a claim that does not exist yet", "set spec.claim to it as well"} {
		if !strings.Contains(message, want) {
			t.Errorf("message = %q, want it to say %q", message, want)
		}
	}
}
