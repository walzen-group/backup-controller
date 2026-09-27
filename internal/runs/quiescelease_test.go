package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// atFrozen returns the frozen clock's time, moved on by after, as the pointer
// a run's status holds.
func atFrozen(after time.Duration) *metav1.Time {
	t := metav1.NewTime(frozen.Add(after))
	return &t
}

// quiesceLeaseIn returns the namespace's quiesce Lease as stored, or nil when
// the namespace holds none.
func quiesceLeaseIn(t *testing.T, c client.Client, namespace string) *coordinationv1.Lease {
	t.Helper()
	lease := &coordinationv1.Lease{}
	err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: quiesceLeaseName}, lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("get the quiesce Lease in %s: %v", namespace, err)
	}
	return lease
}

// checkedRestore returns the RestoreRun back-to-monday with its checks
// already passed: it has a phase, a start, and one Pending volume item whose
// snapshot the checks recorded. A test about what quiesce does uses it to
// reach quiesce without driving the checks first.
func checkedRestore(mutate ...func(*backupv1alpha1.RestoreRun)) *backupv1alpha1.RestoreRun {
	return restoreRun(append([]func(*backupv1alpha1.RestoreRun){
		func(r *backupv1alpha1.RestoreRun) {
			r.Status.Phase = backupv1alpha1.RunPhaseRunning
			r.Status.StartedAt = atFrozen(0)
			r.Status.Items = []backupv1alpha1.RestoreItem{
				{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemPending, Snapshot: monday.ShortID(), SnapshotID: monday.ID, SnapshotTime: &metav1.Time{Time: monday.Time}},
			}
		},
	}, mutate...)...)
}

// quiescedRestoreOf returns the RestoreRun back-to-monday, checked, with
// spec.quiesce naming the app's Deployment.
func quiescedRestoreOf() *backupv1alpha1.RestoreRun {
	return checkedRestore(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
	})
}

// quiescedBackup returns the BackupRun before-upgrade stopped the app:
// spec.all set, the plan recorded, and its volume item not started.
func quiescedBackup(mutate ...func(*backupv1alpha1.BackupRun)) *backupv1alpha1.BackupRun {
	return backupRun(append([]func(*backupv1alpha1.BackupRun){
		func(b *backupv1alpha1.BackupRun) {
			b.Spec.All = true
			b.Status.Phase = backupv1alpha1.RunPhaseRunning
			b.Status.QuiescedAt = atFrozen(0)
			b.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
			b.Status.Items = []backupv1alpha1.BackupItem{{Kind: "ReplicationSource", Name: claimN, Phase: backupv1alpha1.ItemPending}}
		},
	}, mutate...)...)
}

// secondBackupRun returns the BackupRun manual-notes, the run a person
// creates beside the scheduled one.
func secondBackupRun(mutate ...func(*backupv1alpha1.BackupRun)) *backupv1alpha1.BackupRun {
	run := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "manual-notes", Namespace: ns, UID: otherRunUID, Generation: 1},
		Spec:       backupv1alpha1.BackupRunSpec{Timeout: &metav1.Duration{Duration: time.Hour}},
	}
	for _, m := range mutate {
		m(run)
	}
	return run
}

// A RestoreRun that has stopped the app and a namespace BackupRun never stop
// it together: the backup waits with reason SourceBusy for the restore,
// records no plan, and only plans once the restore has given the app back.
// Were it to plan while the app stands at 0, it would record 0 as the count to
// give back and leave the app down.
func TestARestoreAndANamespaceBackupNeverStopTheAppTogether(t *testing.T) {
	t.Parallel()
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		restoreRun(func(r *backupv1alpha1.RestoreRun) {
			r.Spec.Claim = claimN
			r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
		}, asOf("2026-09-21T04:00:00Z")),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

	restoreStep(t, rr) // plan
	restoreStep(t, rr) // quiesce: stops the app and records its count
	restore := readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 1 || restore.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the restore recorded %+v, want Deployment %s at 2", restore.Status.Quiesced, appN)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the restore stopped the app, want 0", got)
	}

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce: waits for the restore
	backup := readBackupRun(t, c)
	if len(backup.Status.Quiesced) != 0 || backup.Status.QuiescedAt != nil {
		t.Fatalf("the backup recorded %+v with quiescedAt %v; want no plan while the restore holds the app",
			backup.Status.Quiesced, backup.Status.QuiescedAt)
	}
	if readyReason(backup.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(backup.Status.Conditions), "RestoreRun back-to-monday") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the restore",
			readyReason(backup.Status.Conditions), readyMessage(backup.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d while the backup waits, want the restore's 0", got)
	}

	// Drive the restore to its restart: its mover finishes, so the run gives
	// the app back and ends.
	restoreStep(t, rr)
	completeJob(t, c)
	restoreStep(t, rr)
	restoreStep(t, rr) // the pass after the stop finds the restore Job stopped
	restore = readRestoreRun(t, c)
	if restore.Status.Phase != backupv1alpha1.RunPhaseSucceeded || restore.Status.RestartedAt == nil {
		t.Fatalf("the restore ended %+v; want it Succeeded after giving the app back", restore.Status)
	}

	step(t, br) // quiesce: the restore is done, so the backup plans
	backup = readBackupRun(t, c)
	if len(backup.Status.Quiesced) != 1 || backup.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the backup recorded %+v, want Deployment %s at 2", backup.Status.Quiesced, appN)
	}

	cutClone(t, c)
	step(t, br) // restart
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d after the backup's restart, want 2", got)
	}
	complete(t, c)
	step(t, br) // find the snapshot
	step(t, br) // move it, and finish
	if backup := readBackupRun(t, c); backup.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("the backup ended %s: %s", backup.Status.Phase, readyMessage(backup.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d after both runs, want 2", got)
	}
}

// A quiesce Lease is taken over only when its holder has durably given the
// workloads back: a holder that is gone, finished, or whose stored status
// shows the restart done is stale, whatever the workloads stand at now; a
// holder that still owes a restart, has no plan yet, or is being deleted is
// live.
func TestAQuiesceLeaseIsTakenOverOnlyWhenItsHolderRestarted(t *testing.T) {
	t.Parallel()
	restarted := func(b *backupv1alpha1.BackupRun) {
		b.Status.RestartedAt = atFrozen(0)
		b.Status.RestartPending = false
	}
	noPlan := func(b *backupv1alpha1.BackupRun) {
		b.Spec.All = true
		b.Status.Phase = backupv1alpha1.RunPhaseRunning
		b.Status.Items = []backupv1alpha1.BackupItem{{Kind: "ReplicationSource", Name: claimN, Phase: backupv1alpha1.ItemPending}}
	}
	for name, tc := range map[string]struct {
		holder  func() *backupv1alpha1.BackupRun
		store   bool
		stopped int32
		live    bool
	}{
		"holder gone": {stopped: 2},
		"finished": {holder: func() *backupv1alpha1.BackupRun {
			return quiescedBackup(func(b *backupv1alpha1.BackupRun) { b.Status.Phase = backupv1alpha1.RunPhaseSucceeded })
		}, store: true, stopped: 0},
		"restarted, plan back":         {holder: func() *backupv1alpha1.BackupRun { return quiescedBackup(restarted) }, store: true, stopped: 2},
		"restarted, app stopped since": {holder: func() *backupv1alpha1.BackupRun { return quiescedBackup(restarted) }, store: true, stopped: 0},
		"restart pending": {
			holder: func() *backupv1alpha1.BackupRun {
				return quiescedBackup(func(b *backupv1alpha1.BackupRun) {
					b.Status.RestartedAt = atFrozen(0)
					b.Status.RestartPending = true
				})
			},
			store: true, stopped: 0, live: true,
		},
		"no plan yet": {holder: func() *backupv1alpha1.BackupRun { return backupRun(noPlan) }, store: true, stopped: 2, live: true},
	} {
		t.Run(name, func(t *testing.T) {
			var holder *backupv1alpha1.BackupRun
			if tc.holder != nil {
				holder = tc.holder()
			} else {
				holder = backupRun()
			}
			objects := []client.Object{deployment()}
			if tc.store {
				objects = append(objects, holder)
			}
			c := newClient(t, objects...)
			d := &appsv1.Deployment{}
			get(t, c, ns, appN, d)
			d.Spec.Replicas = &tc.stopped
			if err := c.Update(context.Background(), d); err != nil {
				t.Fatal(err)
			}
			lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: quiesceLeaseName, Namespace: ns}}
			stamp(lease, leaseHolder{kind: "BackupRun", run: holder, scope: scopeQuiesce}, nil)
			live, err := holderLive(context.Background(), c, lease)
			if err != nil || live != tc.live {
				t.Errorf("holderLive = %t, %v; want %t", live, err, tc.live)
			}
		})
	}
}

// A namespace BackupRun waits, with the app running and no quiesce Lease
// taken, while a RestoreRun in its namespace has deleted a Cluster and waits
// for it to be created again: the Kustomization the backup would suspend may
// be the one Flux needs to create that Cluster (see waitingOn).
func TestANamespaceBackupWaitsForARestoreThatDeletedItsCluster(t *testing.T) {
	t.Parallel()
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		restoreRun(func(r *backupv1alpha1.RestoreRun) {
			r.Finalizers = []string{Finalizer}
			r.Spec.Database = pgN
			r.Status.Phase = backupv1alpha1.RunPhaseWaiting
			r.Status.StartedAt = atFrozen(-time.Minute)
			r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemDeleted, ClusterUID: "old-cluster-uid"}}
		}),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce: waits for the restore

	backup := readBackupRun(t, c)
	want := "RestoreRun back-to-monday has deleted Cluster " + pgN + " and waits for it to be created again"
	if readyReason(backup.Status.Conditions) != backupv1alpha1.ReasonSourceBusy || !strings.Contains(readyMessage(backup.Status.Conditions), want) {
		t.Fatalf("reason = %q, message = %q; want SourceBusy holding %q",
			readyReason(backup.Status.Conditions), readyMessage(backup.Status.Conditions), want)
	}
	if len(backup.Status.Quiesced) != 0 || quiesceLeaseIn(t, c, ns) != nil {
		t.Errorf("quiesced = %+v, quiesce Lease = %v; want no plan and no Lease while the restore waits", backup.Status.Quiesced, quiesceLeaseIn(t, c, ns))
	}
	if got := replicasOf(t, c); got != 2 || suspended(t, c) {
		t.Errorf("replicas = %d, suspended = %t; want the app running and its Kustomization left alone", got, suspended(t, c))
	}
}

// seedQuiesceLease creates the namespace's quiesce Lease, stamped for run.
func seedQuiesceLease(t *testing.T, c client.Client, kind string, run metav1.Object) {
	t.Helper()
	lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: quiesceLeaseName, Namespace: ns}}
	stamp(lease, leaseHolder{kind: kind, run: run, scope: scopeQuiesce}, nil)
	if err := c.Create(context.Background(), lease); err != nil {
		t.Fatal(err)
	}
}
