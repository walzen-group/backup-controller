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
	completeJob(t, c)
	restoreStep(t, rr)
	restoreStep(t, rr) // the pass after the stop finds the restore Job stopped
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

// countDeploymentPatches wraps c so that it counts the patches of a
// Deployment, which is how quiesce and restart scale the app, and returns the
// wrapped client and the counter.
func countDeploymentPatches(c client.Client) (client.Client, *int) {
	patches := 0
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok {
				patches++
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}), &patches
}

// A quiesced RestoreRun whose claim's repository Secret is gone by the time
// it would stop the app fails that item in its pre-check, before it stops
// anything, and with no item left to restore it never stops the app. Before,
// the pre-check left the missing Secret to restoreVolume, so the run stopped
// the app, failed the item, and gave the app back (UFX, like UF7 on a
// BackupRun).
func TestARestoreWhoseRepositorySecretIsGoneFailsBeforeTheAppStops(t *testing.T) {
	c := newClient(t, quiescedRestore(), claim(), volumeRestore(), repository(), deployment(), kustomization(false))
	watching, patches := countDeploymentPatches(c)
	r := &RestoreRunReconciler{Client: watching, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

	restoreStep(t, r) // plan
	if err := c.Delete(context.Background(), repository()); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r) // quiesce: the pre-check fails the item
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || run.Status.Items[0].Phase != backupv1alpha1.ItemFailed ||
		!strings.Contains(run.Status.Items[0].Message, "repository Secret "+repoN) {
		t.Fatalf("phase = %q, items = %+v; want Failed with the item naming repository Secret %s", run.Status.Phase, run.Status.Items, repoN)
	}
	if *patches != 0 || len(run.Status.Quiesced) != 0 || suspended(t, c) {
		t.Errorf("Deployment patches = %d, quiesced = %+v, suspended = %t; want the app never stopped", *patches, run.Status.Quiesced, suspended(t, c))
	}
}

// A quiesced RestoreRun that has no Pending item after its checks, here a
// namespace whose only marked object is an opted-out Cluster, stops nothing
// and ends Failed saying nothing was restored.
func TestAQuiescedRestoreWithNothingToRestoreStopsNothing(t *testing.T) {
	c := newClient(t, quiescedRestore(), cluster(optedOut), objectStore(), storeSecret(), deployment(), kustomization(false))
	watching, patches := countDeploymentPatches(c)
	r := &RestoreRunReconciler{Client: watching, Reader: c, Prober: prober{saturday}, Now: frozenNow}

	restoreStep(t, r) // plan: the Cluster is Skipped
	restoreStep(t, r)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("phase = %q, reason = %q; want Failed, NoBackupInReach", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if *patches != 0 || len(run.Status.Quiesced) != 0 || suspended(t, c) {
		t.Errorf("Deployment patches = %d, quiesced = %+v, suspended = %t; want the app never stopped", *patches, run.Status.Quiesced, suspended(t, c))
	}
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
			watching, scaled := countDeploymentPatches(c)
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
				t.Errorf("Deployment patches = %d, quiesced = %+v, suspended = %t; want the app never stopped", *scaled, run.Status.Quiesced, suspended(t, c))
			}
		})
	}
}

// A quiesced RestoreRun fails an item restoreVolume would refuse before its
// ReplicationDestination exists in its pre-check, with the message
// restoreVolume gives, and with no item left to restore it never stops the
// app. The refusals it can know before the stop are a claim that is gone or
// being deleted, and a claim whose VolumeRestore is gone, which repositoryFor
// refuses. Before, the pre-check left them to restoreVolume, so the run
// stopped the app, failed the item and gave the app back (AB9, like AB4 on a
// BackupRun).
func TestAnItemRestoreVolumeWouldRefuseFailsBeforeTheAppStops(t *testing.T) {
	for _, tc := range []struct {
		name    string
		breakIt func(t *testing.T, c client.Client)
		want    string
	}{
		{"claim gone", func(t *testing.T, c client.Client) {
			if err := c.Delete(context.Background(), claim()); err != nil {
				t.Fatal(err)
			}
		}, "no PersistentVolumeClaim " + claimN + " in this namespace"},
		{"claim being deleted", func(t *testing.T, c client.Client) {
			held := &corev1.PersistentVolumeClaim{}
			get(t, c, ns, claimN, held)
			held.Finalizers = append(held.Finalizers, "kubernetes.io/pvc-protection")
			if err := c.Update(context.Background(), held); err != nil {
				t.Fatal(err)
			}
			if err := c.Delete(context.Background(), held); err != nil {
				t.Fatal(err)
			}
		}, "claim " + claimN + " is being deleted"},
		{"VolumeRestore gone", func(t *testing.T, c client.Client) {
			if err := c.Delete(context.Background(), volumeRestore()); err != nil {
				t.Fatal(err)
			}
		}, "claim " + claimN + " has no VolumeRestore " + claimN + " to name its repository"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, quiescedRestore(), claim(), volumeRestore(), repository(), deployment(), kustomization(false))
			watching, patches := countDeploymentPatches(c)
			r := &RestoreRunReconciler{Client: watching, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

			restoreStep(t, r) // plan
			tc.breakIt(t, c)
			restoreStep(t, r) // quiesce: the pre-check fails the item
			restoreStep(t, r)

			run := readRestoreRun(t, c)
			if want := tc.want + nothingWritten(claimN); run.Status.Phase != backupv1alpha1.RunPhaseFailed ||
				run.Status.Items[0].Phase != backupv1alpha1.ItemFailed || run.Status.Items[0].Message != want {
				t.Fatalf("phase = %q, items = %+v; want Failed with the item message %q", run.Status.Phase, run.Status.Items, want)
			}
			if *patches != 0 || len(run.Status.Quiesced) != 0 || suspended(t, c) {
				t.Errorf("Deployment patches = %d, quiesced = %+v, suspended = %t; want the app never stopped", *patches, run.Status.Quiesced, suspended(t, c))
			}
		})
	}
}

// A RestoreRun without spec.quiesce fails an item whose claim is being
// deleted before it creates a ReplicationDestination, with the message the
// pre-check gives: the mover would write into a claim that is about to go,
// and the scheduler does not place a pod whose claim is being deleted.
func TestARestoreIntoAClaimBeingDeletedFailsBeforeTheMoverStarts(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
		claim(), volumeRestore(), repository())

	restoreStep(t, r) // plan
	held := &corev1.PersistentVolumeClaim{}
	get(t, c, ns, claimN, held)
	held.Finalizers = append(held.Finalizers, "kubernetes.io/pvc-protection")
	if err := c.Update(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), held); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	want := "claim " + claimN + " is being deleted" + nothingWritten(claimN)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || run.Status.Items[0].Phase != backupv1alpha1.ItemFailed ||
		run.Status.Items[0].Message != want || run.Status.Items[0].Destination != "" {
		t.Fatalf("phase = %q, items = %+v; want Failed with the item message %q and no destination", run.Status.Phase, run.Status.Items, want)
	}
	destinations := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), destinations); err != nil || len(destinations.Items) != 0 {
		t.Errorf("ReplicationDestinations = %d (err %v); want none", len(destinations.Items), err)
	}
}
