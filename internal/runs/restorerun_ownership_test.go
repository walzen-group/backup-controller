package runs

import (
	"context"
	"strings"
	"testing"

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
// backups into a new claim named notes-data-monday.
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
// claim ends Invalid at its checks. It writes nothing: no mover, and
// the claim is as it was. Before, the run created a Direct destination with
// enableFileDeletion on the mounted claim and ended Succeeded.
func TestAnIntoRestoreRefusesAnExistingClaim(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(intoApp), claim(), volumeRestore(), repository(), writerPod())
	before := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, claimN, before)

	restoreStep(t, r) // plan
	if names := movers(t, c); len(names) == 0 {
		restoreStep(t, r)
	}
	if names := movers(t, c); len(names) > 0 {
		completeJob(t, c)
	}
	run := stepUntilFinished(t, r, c, 3)

	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
		!strings.Contains(readyMessage(run.Status.Conditions), "claim notes-data already exists and this run did not create it") {
		t.Fatalf("phase = %q, reason = %q, message = %q; want Invalid saying the claim exists",
			run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if names := movers(t, c); len(names) > 0 {
		t.Errorf("movers = %v, want none", names)
	}
	after := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, claimN, after)
	if after.ResourceVersion != before.ResourceVersion || len(after.OwnerReferences) != 0 {
		t.Errorf("claim changed: resourceVersion %s -> %s, owners %v", before.ResourceVersion, after.ResourceVersion, after.OwnerReferences)
	}
}

// An into restore from a claim whose spec.into names an existing Bound claim
// ends Invalid at its checks and creates nothing. So does one whose spec.into
// names an existing VolumeRestore: a VolumeRestore describes the backups of
// the claim of its name, so that name is taken by a claim of the app's.
// Before, the run found the claim Bound and ended Succeeded with nothing
// restored.
func TestAnIntoRestoreFromAClaimRefusesAnExistingClaimOrVolumeRestore(t *testing.T) {
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
			if names := movers(t, c); len(names) != 0 {
				t.Errorf("movers = %v, want none", names)
			}
			key := types.NamespacedName{Namespace: ns, Name: "notes-data-monday"}
			if name == "claim" {
				if err := c.Get(context.Background(), key, &backupv1alpha1.VolumeRestore{}); err == nil {
					t.Error("a VolumeRestore was created for a claim the run did not create")
				}
			} else if err := c.Get(context.Background(), key, &corev1.PersistentVolumeClaim{}); err == nil {
				t.Error("a claim was created under the name of an existing VolumeRestore")
			}
		})
	}
}

// A claim with the spec.into name that appears between the checks and the
// create ends the run Failed, with no mover and no VolumeRestore
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
			if names := movers(t, c); len(names) == 0 {
				restoreStep(t, r)
			}
			run := stepUntilFinished(t, r, c, 3)

			if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed ||
				!strings.Contains(readyMessage(run.Status.Conditions), "claim scratch already exists and this run did not create it") {
				t.Fatalf("phase = %q, reason = %q, message = %q; want Failed saying the claim exists",
					run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
			}
			expectItemReason(t, c, backupv1alpha1.ItemReasonIntoClaimTaken)
			if names := movers(t, c); len(names) > 0 {
				t.Errorf("movers = %v, want none", names)
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
			if names := movers(t, c); len(names) == 0 {
				restoreStep(t, r)
			}
			if names := movers(t, c); len(names) > 0 {
				completeJob(t, c)
			}
			run := stepUntilFinished(t, r, c, 3)

			if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
				!strings.Contains(readyMessage(run.Status.Conditions), "it belongs to RestoreRun old-restore") {
				t.Fatalf("phase = %q, reason = %q, message = %q; want Invalid naming RestoreRun old-restore",
					run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
			}
			if names := movers(t, c); len(names) > 0 {
				t.Errorf("movers = %v, want none", names)
			}
		})
	}
}

// A pass whose claim create went through but came back as an error is
// retried, and the next pass finds the claim the run created and goes on
// to create the restore Job. The ownership check must not refuse the run's
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

	if jobs := restoreJobs(t, c); len(jobs) != 1 {
		t.Fatalf("restore Jobs = %v, want the run's one (run %+v)", jobs, readRestoreRun(t, c).Status)
	}
	completeJob(t, c)
	run := stepUntilFinished(t, r, c, 2)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, message = %q; want Succeeded", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
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
	completeJob(t, c)
	run := stepUntilFinished(t, r, c, 2)

	if run.Status.Phase != backupv1alpha1.RunPhaseFailed ||
		!strings.Contains(readyMessage(run.Status.Conditions), "claim scratch was deleted (or replaced) while its restore Job wrote into it") {
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

// A claim with the spec.into name that someone else creates between the
// run's last read and its own create ends the run Failed: the create finds
// the name taken, and the run creates no restore Job, whose --delete would
// empty that claim. The foreign claim is left as it was.
func TestAnIntoRestoreRefusesAClaimCreatedAtItsCreate(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan
	foreign := boundClaim("scratch")
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if pvc, ok := obj.(*corev1.PersistentVolumeClaim); ok && pvc.Name == foreign.Name {
				if err := cl.Create(ctx, foreign); err != nil {
					t.Fatal(err)
				}
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	restoreStep(t, r)
	r.Client = c
	run := stepUntilFinished(t, r, c, 3)

	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %d, want none on a claim the run did not create", len(jobs))
	}
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed ||
		!strings.Contains(readyMessage(run.Status.Conditions), "claim scratch already exists and this run did not create it") {
		t.Fatalf("phase = %q, message = %q; want Failed naming the claim", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
	expectItemReason(t, c, backupv1alpha1.ItemReasonIntoClaimTaken)
	after := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, foreign.Name, after)
	if after.ResourceVersion != foreign.ResourceVersion || len(after.OwnerReferences) != 0 || after.DeletionTimestamp != nil {
		t.Errorf("claim = %+v, want the foreign claim untouched", after.ObjectMeta)
	}
}
