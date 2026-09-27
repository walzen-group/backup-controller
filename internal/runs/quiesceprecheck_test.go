package runs

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A RestoreRun whose claim or repository a live backup holds waits before it
// stops the app, rather than stopping it and waiting for the backup with the
// app down.
func TestAQuiescedRestoreWaitsForABackupBeforeStopping(t *testing.T) {
	t.Parallel()
	c := newClient(t, otherRun(), quiescedRestoreOf(), idleSource(),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}
	if err := writeOtherTag(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, rr) // quiesce: waits, the app stays up
	restore := readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 0 {
		t.Fatalf("the restore recorded %+v; want no plan while the backup holds the claim", restore.Status.Quiesced)
	}
	if readyReason(restore.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(restore.Status.Conditions), "BackupRun manual-notes") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the backup",
			readyReason(restore.Status.Conditions), readyMessage(restore.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d while the restore waits, want the app still up", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization was suspended before the restore planned anything")
	}

	// The backup ends, so the restore stops the app and records its count.
	finished := &backupv1alpha1.BackupRun{}
	get(t, c, ns, "manual-notes", finished)
	finished.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	for i := range finished.Status.Items {
		finished.Status.Items[i].Phase = backupv1alpha1.ItemSucceeded
	}
	if err := c.Status().Update(context.Background(), finished); err != nil {
		t.Fatal(err)
	}
	complete(t, c)
	restoreStep(t, rr)
	restore = readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 1 || restore.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the restore recorded %+v, want Deployment %s at 2", restore.Status.Quiesced, appN)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d after the restore stopped the app, want 0", got)
	}
}

// countDeploymentScales wraps c so that it counts the writes to a
// Deployment's scale subresource, which is how quiesce and restart scale
// the app, and returns the wrapped client and the counter.
func countDeploymentScales(c client.Client) (client.Client, *int) {
	scales := 0
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := deploymentScale(sub, obj, opts); ok {
				scales++
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}), &scales
}

// A namespace BackupRun whose every volume item startItem would refuse fails
// those items in its pre-check, with the message startItem gives, and never
// stops the app: a ReplicationSource of the claim's name that the controller
// did not write, a retention annotation that does not parse or none at all,
// a claim not bound yet, a claim without its VolumeRestore, a volume without
// node affinity, and a claim deleted since the plan. Before, the pre-check
// left those to startItem, so the run stopped the app for a backup that
// could not start (AB4).
func TestAnItemStartItemWouldRefuseFailsBeforeTheAppStops(t *testing.T) {
	t.Parallel()
	foreign := idleSource()
	foreign.Labels = nil
	unbound := claim()
	unbound.Spec.VolumeName, unbound.Status.Phase = "", corev1.ClaimPending
	badRetention, noRetention := claim(), claim()
	badRetention.Annotations[backupv1alpha1.AnnotationRetainLast] = "zero"
	delete(noRetention.Annotations, backupv1alpha1.AnnotationRetainLast)
	noAffinity := volume()
	noAffinity.Spec.NodeAffinity = nil
	for _, tc := range []struct {
		name    string
		objects []client.Object
		gone    bool
		want    string
	}{
		{"source not the controller's", []client.Object{claim(), volume(), volumeRestore(), foreign}, false, "was not written by backup-controller"},
		{"retention that does not parse", []client.Object{badRetention, volume(), volumeRestore()}, false, backupv1alpha1.AnnotationRetainLast},
		{"no retention", []client.Object{noRetention, volume(), volumeRestore()}, false, "retain"},
		{"claim not bound", []client.Object{unbound, volumeRestore()}, false, "is not bound to a volume yet"},
		{"no VolumeRestore", []client.Object{claim(), volume()}, false, "VolumeRestore"},
		{"volume without node affinity", []client.Object{claim(), noAffinity, volumeRestore()}, false, "declares no node affinity"},
		{"claim deleted since the plan", []client.Object{claim(), volume(), volumeRestore()}, true, "no longer exists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				repository(), deployment(), kustomization(false)}, tc.objects...)
			c := newClient(t, objects...)
			watching, scaled := countDeploymentScales(c)
			br := &BackupRunReconciler{Client: watching, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

			step(t, br) // plan
			if tc.gone {
				pvc := &corev1.PersistentVolumeClaim{}
				get(t, c, ns, claimN, pvc)
				if err := c.Delete(context.Background(), pvc); err != nil {
					t.Fatal(err)
				}
			}
			step(t, br) // admit
			step(t, br) // quiesce: the pre-check fails the item
			step(t, br) // finish

			run := readBackupRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseFailed || len(run.Status.Items) != 1 ||
				run.Status.Items[0].Phase != backupv1alpha1.ItemFailed || !strings.Contains(run.Status.Items[0].Message, tc.want) {
				t.Fatalf("phase = %q (%s), items = %+v; want Failed with the item's message holding %q",
					run.Status.Phase, readyMessage(run.Status.Conditions), run.Status.Items, tc.want)
			}
			if *scaled != 0 || len(run.Status.Quiesced) != 0 || suspended(t, c) {
				t.Errorf("Deployment scale writes = %d, quiesced = %+v, suspended = %t; want the app never stopped", *scaled, run.Status.Quiesced, suspended(t, c))
			}
		})
	}
}
