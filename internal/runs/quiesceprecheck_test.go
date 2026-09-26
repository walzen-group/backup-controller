package runs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// seedLease creates the named Lease in namespace, stamped for holder, and
// returns the stored copy.
func seedLease(t *testing.T, c client.Client, namespace, name string, holder leaseHolder) *coordinationv1.Lease {
	t.Helper()
	lease := &coordinationv1.Lease{}
	lease.SetNamespace(namespace)
	lease.SetName(name)
	stamp(lease, holder, []string{holder.item})
	if err := c.Create(context.Background(), lease); err != nil {
		t.Fatalf("create Lease %s/%s: %v", namespace, name, err)
	}
	return lease
}

// failingReads wraps c so that a read of an object of the given kind comes
// back as a 500, the way an API server that cannot reach its store answers.
func failingReads(c client.Client, kind string) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			named := func(name string) bool {
				switch o := obj.(type) {
				case *corev1.PersistentVolumeClaim:
					return name == "PersistentVolumeClaim"
				case *volsyncv1alpha1.ReplicationSource:
					return name == "ReplicationSource"
				case *backupv1alpha1.VolumeRestore:
					return name == "VolumeRestore"
				case *unstructured.Unstructured:
					return o.GroupVersionKind() == schema.GroupVersionKind{
						Group: backupv1alpha1.GroupVersion.Group, Version: backupv1alpha1.GroupVersion.Version, Kind: name,
					}
				}
				return false
			}
			if named(kind) {
				return apierrors.NewInternalError(errors.New("the API server cannot read " + kind))
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
}

// A namespace BackupRun that stopped the app and a RestoreRun that has not
// stopped it never stop it together: the restore waits with the app running,
// and once the backup is done it records the count the backup gave back.
func TestANamespaceBackupAndARestoreNeverStopTheAppTogether(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }), quiescedRestoreOf(),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce: stops the app at 2 recorded
	step(t, br) // start: the source, the claim's Lease and the repository's
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the backup stopped the app, want 0", got)
	}

	// The backup's upload holds the claim's Lease while the app runs again.
	cutClone(t, c)
	step(t, br) // restart: the app is back at 2
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d after the backup's restart, want 2", got)
	}
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("item = %+v after the restart, want it Running while the mover uploads", item)
	}

	restoreStep(t, rr) // quiesce: waits, the app stays up
	restore := readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 0 {
		t.Fatalf("the restore recorded %+v; want no plan while the backup's upload holds the claim", restore.Status.Quiesced)
	}
	if readyReason(restore.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(restore.Status.Conditions), "BackupRun before-upgrade") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the backup",
			readyReason(restore.Status.Conditions), readyMessage(restore.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d while the restore waits, want 2", got)
	}

	// The backup's upload ends and the backup ends, so the restore may stop
	// the app and records the count the backup gave back.
	complete(t, c, "snapshot 6e473100 saved")
	step(t, br)
	if backup := readBackupRun(t, c); backup.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("the backup ended %s: %s", backup.Status.Phase, readyMessage(backup.Status.Conditions))
	}
	restoreStep(t, rr)
	restore = readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 1 || restore.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the restore recorded %+v, want Deployment %s at 2", restore.Status.Quiesced, appN)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the restore stopped the app, want 0", got)
	}

	// The restore gives the app back when its volume is restored.
	restoreStep(t, rr)
	restore = readRestoreRun(t, c)
	destination := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, restore.Status.Items[0].Destination, destination)
	destination.Status = restoredStatus(restore.Status.Items[0].Snapshot)
	if err := c.Status().Update(context.Background(), destination); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, rr)
	restoreStep(t, rr) // the pass after the destination's delete finds its mover gone
	if restore := readRestoreRun(t, c); restore.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("the restore ended %s: %s", restore.Status.Phase, readyMessage(restore.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d after the restore, want 2", got)
	}
}

// A read that fails in a quiesce pre-check is not evidence that nothing is in
// use: the pass comes back as an error and the app keeps running, for a read
// of the claim, of its VolumeRestore, and of the ReplicationSource.
func TestThePreCheckFailsClosed(t *testing.T) {
	for _, kind := range []string{"PersistentVolumeClaim", "VolumeRestore", "ReplicationSource"} {
		t.Run(kind, func(t *testing.T) {
			c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
			br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
			step(t, br) // plan
			step(t, br) // admit
			healthy := br.Reader
			br.Reader = failingReads(c, kind)
			if err := tryStep(br); err == nil {
				t.Fatal("the quiesce pass succeeded although a pre-check read failed")
			}

			run := readBackupRun(t, c)
			if len(run.Status.Quiesced) != 0 || run.Status.QuiescedAt != nil {
				t.Fatalf("the run recorded %+v with quiescedAt %v; want no plan on a failed read",
					run.Status.Quiesced, run.Status.QuiescedAt)
			}
			if got := replicasOf(t, c); got != 2 {
				t.Fatalf("replicas = %d after the failed pass, want the app untouched", got)
			}
			if suspended(t, c) {
				t.Error("the Kustomization was suspended on a failed read")
			}

			// With the read working, the same pass plans the true count.
			br.Reader = healthy
			step(t, br)
			run = readBackupRun(t, c)
			if len(run.Status.Quiesced) != 1 || run.Status.Quiesced[0].Replicas != 2 {
				t.Fatalf("the run recorded %+v after the read came back, want Deployment %s at 2", run.Status.Quiesced, appN)
			}
		})
	}
}

// A claim whose Lease a live RestoreRun holds, and no ReplicationDestination
// yet, makes a namespace BackupRun wait before it stops anything.
func TestThePreCheckSeesAClaimLeaseHeldByARestore(t *testing.T) {
	restoring := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemPending}}
	})
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }), restoring,
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	claimLease, _ := leaseNames(t, c)
	seedLease(t, c, ns, claimLease, leaseHolder{kind: "RestoreRun", run: restoring, item: claimN})

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce: waits for the restore's Lease
	run := readBackupRun(t, c)
	if len(run.Status.Quiesced) != 0 {
		t.Fatalf("the run recorded %+v; want no plan while the restore holds the claim", run.Status.Quiesced)
	}
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun back-to-monday") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the restore",
			readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d while the run waits, want 2", got)
	}
}

// A RestoreRun whose claim or repository a live backup holds waits before it
// stops the app, rather than stopping it and waiting for the backup with the
// app down.
func TestAQuiescedRestoreWaitsForABackupBeforeStopping(t *testing.T) {
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
	complete(t, c, "snapshot 6e473100 saved")
	restoreStep(t, rr)
	restore = readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 1 || restore.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the restore recorded %+v, want Deployment %s at 2", restore.Status.Quiesced, appN)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d after the restore stopped the app, want 0", got)
	}
}

// A RestoreRun that times out while it waits ends Failed naming the wait,
// with nothing stopped.
func TestARestoreWaitThatOutlastsTheTimeoutNamesTheWait(t *testing.T) {
	c := newClient(t, otherRun(), quiescedRestoreOf(), idleSource(),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}
	if err := writeOtherTag(context.Background(), c); err != nil {
		t.Fatal(err)
	}

	restoreStep(t, rr) // quiesce: waits for the live backup
	restore := readRestoreRun(t, c)
	if readyReason(restore.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("reason = %q, want SourceBusy while the live backup holds the claim", readyReason(restore.Status.Conditions))
	}
	rr.Now = func() time.Time { return frozen.Add(4*time.Hour + time.Second) }
	restoreStep(t, rr)

	restore = readRestoreRun(t, c)
	message := readyMessage(restore.Status.Conditions)
	if restore.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", restore.Status.Phase)
	}
	if !strings.Contains(message, "manual-notes") {
		t.Errorf("message = %q; want it to name the backup it waited for", message)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d after the timed-out restore, want 2", got)
	}
}

// A namespace BackupRun whose claim's repository Secret is gone fails that
// item in its pre-check, before it stops anything, and with no item left to
// back up it never stops the app. Before, the pre-check left the refusal to
// the item's start, so the run stopped the app, failed the item, and gave
// the app back (UF7).
func TestAMissingRepositorySecretFailsTheItemBeforeTheAppStops(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), deployment(), kustomization(false))
	scaled := 0
	watching := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				scaled++
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	br := &BackupRunReconciler{Client: watching, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce: the pre-check fails the item
	step(t, br) // finish

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || len(run.Status.Items) != 1 ||
		run.Status.Items[0].Phase != backupv1alpha1.ItemFailed || !strings.Contains(run.Status.Items[0].Message, "repository Secret "+repoN) {
		t.Fatalf("phase = %q, items = %+v; want Failed with the item naming repository Secret %s", run.Status.Phase, run.Status.Items, repoN)
	}
	if scaled != 0 || len(run.Status.Quiesced) != 0 || suspended(t, c) {
		t.Errorf("Deployment patches = %d, quiesced = %+v, suspended = %t; want the app never stopped", scaled, run.Status.Quiesced, suspended(t, c))
	}
}
