package runs

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

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

// An into restore from a claim whose spec.into names an existing Bound claim
// ends Invalid at its checks and creates nothing. So does one whose spec.into
// names an existing VolumeRestore: a VolumeRestore describes the backups of
// the claim of its name, so that name is taken by a claim of the app's.
// Before, the run found the claim Bound and ended Succeeded with nothing
// restored.
func TestAnIntoRestoreFromAClaimRefusesAnExistingClaimOrVolumeRestore(t *testing.T) {
	t.Parallel()
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

// A claim with the spec.into name that someone else creates between the
// run's last read and its own create ends the run Failed: the create finds
// the name taken, and the run creates no restore Job, whose --delete would
// empty that claim. The foreign claim is left as it was.
func TestAnIntoRestoreRefusesAClaimCreatedAtItsCreate(t *testing.T) {
	t.Parallel()
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
