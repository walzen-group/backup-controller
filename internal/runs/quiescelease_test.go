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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

// stepIn reconciles the named BackupRun once and returns the result.
func stepIn(t *testing.T, r *BackupRunReconciler, namespace, name string) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: namespace, Name: name}})
	if err != nil {
		t.Fatalf("reconcile BackupRun %s/%s: %v", namespace, name, err)
	}
	return result
}

// backupIn returns the named BackupRun as stored.
func backupIn(t *testing.T, c client.Client, namespace, name string) *backupv1alpha1.BackupRun {
	t.Helper()
	run := &backupv1alpha1.BackupRun{}
	get(t, c, namespace, name, run)
	return run
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
				{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemPending, Snapshot: monday.ShortID(), SnapshotTime: &metav1.Time{Time: monday.Time}},
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
	restore = readRestoreRun(t, c)
	destination := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, restore.Status.Items[0].Destination, destination)
	destination.Status = restoredStatus(restore.Status.Items[0].Snapshot)
	if err := c.Status().Update(context.Background(), destination); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, rr)
	restoreStep(t, rr) // the pass after the destination's delete finds its mover gone
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
	complete(t, c, "snapshot 6e473100 saved")
	step(t, br) // collect and finish
	if backup := readBackupRun(t, c); backup.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("the backup ended %s: %s", backup.Status.Phase, readyMessage(backup.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d after both runs, want 2", got)
	}
}

// Two namespace BackupRuns never stop the app together: the second waits for
// the first's quiesce Lease and records the count the first gave back.
func TestTwoNamespaceBackupsNeverStopTheAppTogether(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		secondBackupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	const second = "manual-notes"

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce: stops the app at 2 recorded
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the first run stopped the app, want 0", got)
	}

	stepIn(t, br, ns, second) // plan
	stepIn(t, br, ns, second) // admit
	stepIn(t, br, ns, second) // quiesce: waits for the first run
	other := backupIn(t, c, ns, second)
	if len(other.Status.Quiesced) != 0 {
		t.Fatalf("the second run recorded %+v; want no plan while the first holds the namespace", other.Status.Quiesced)
	}
	if readyReason(other.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(other.Status.Conditions), "BackupRun before-upgrade") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the first run",
			readyReason(other.Status.Conditions), readyMessage(other.Status.Conditions))
	}

	// The first run gives the app back and ends.
	cutClone(t, c)
	step(t, br) // restart
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d after the first run's restart, want 2", got)
	}
	complete(t, c, "snapshot 6e473100 saved")
	step(t, br) // collect and finish, releasing the first run's Leases

	stepIn(t, br, ns, second) // quiesce: plans now that the first run is done
	other = backupIn(t, c, ns, second)
	if len(other.Status.Quiesced) != 1 || other.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the second run recorded %+v, want Deployment %s at 2", other.Status.Quiesced, appN)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the second run stopped the app, want 0", got)
	}
	stepIn(t, br, ns, second) // restart
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d after the second run's restart, want 2", got)
	}
}

// A pass that loses its plan status write keeps the quiesce Lease it created,
// and the retry plans with that same Lease: nothing is written onto it and its
// holder does not change. A restore that runs in between waits.
func TestALostPlanWriteAfterTheLeaseRetriesWithItsOwnLease(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }), quiescedRestoreOf(),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

	step(t, br) // plan
	step(t, br) // admit
	healthy := br.Client
	br.Client = loseStatusWriteAt(c, 2)
	if err := tryStep(br); err == nil {
		t.Fatal("the quiesce pass succeeded although its plan status write was lost")
	}
	held := quiesceLeaseIn(t, c, ns)
	if held == nil || holderUID(held) != string(runUID) {
		t.Fatalf("the quiesce Lease after the lost plan write = %+v; want it held by the run", held)
	}
	if run := readBackupRun(t, c); len(run.Status.Quiesced) != 0 {
		t.Fatalf("the run recorded %+v although the plan write was lost", run.Status.Quiesced)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d after the lost plan write, want the app untouched", got)
	}
	version := held.ResourceVersion

	restoreStep(t, rr) // the restore waits: the backup's Lease is live
	restore := readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 0 || readyReason(restore.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("the restore recorded %+v with reason %q; want it waiting with no plan",
			restore.Status.Quiesced, readyReason(restore.Status.Conditions))
	}

	br.Client = healthy
	step(t, br) // quiesce: the plan write goes through
	if got := readBackupRun(t, c); len(got.Status.Quiesced) != 1 {
		t.Fatalf("the retry recorded %+v, want the plan", got.Status.Quiesced)
	}
	held = quiesceLeaseIn(t, c, ns)
	if held == nil || holderUID(held) != string(runUID) {
		t.Fatalf("the quiesce Lease after the retry = %+v; want the run still holding it", held)
	}
	if held.ResourceVersion != version {
		t.Errorf("the run wrote its own quiesce Lease again: resourceVersion %s, want %s", held.ResourceVersion, version)
	}
}

// A pass that loses the status write that clears restartPending keeps the
// quiesce Lease: the stored status still shows a restart the run owes, so
// another run waits until the retry has stored it. Without that, the other run
// would stop the app and the retry would scale it back up under it.
func TestALostWriteAfterTheRestartKeepsTheQuiesceLease(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }), quiescedRestoreOf(),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce
	step(t, br) // start
	cutClone(t, c)
	healthy := br.Client
	br.Client = loseStatusWriteAt(c, 2)
	if err := tryStep(br); err == nil {
		t.Fatal("the restart pass succeeded although the status write that clears restartPending was lost")
	}
	stored := readBackupRun(t, c)
	if stored.Status.RestartedAt == nil || !stored.Status.RestartPending {
		t.Fatalf("stored restartedAt = %v, restartPending = %t; want the lost write to leave the flag set",
			stored.Status.RestartedAt, stored.Status.RestartPending)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d after the lost write, want the restart's 2", got)
	}

	restoreStep(t, rr) // waits: the holder may still repeat its restart
	restore := readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 0 {
		t.Fatalf("the restore recorded %+v; want it waiting with no plan", restore.Status.Quiesced)
	}
	if readyReason(restore.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(restore.Status.Conditions), "before-upgrade") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the backup",
			readyReason(restore.Status.Conditions), readyMessage(restore.Status.Conditions))
	}

	// The retry stores the cleared flag and finishes the backup; only then
	// does the restore stop the app, and nothing scales it back up.
	br.Client = healthy
	complete(t, c, "snapshot 6e473100 saved")
	step(t, br) // retry the restart, collect, and finish
	backup := readBackupRun(t, c)
	if backup.Status.Phase != backupv1alpha1.RunPhaseSucceeded || backup.Status.RestartPending {
		t.Fatalf("the backup ended %s with restartPending = %t", backup.Status.Phase, backup.Status.RestartPending)
	}
	step(t, br) // the finished run writes nothing more

	restoreStep(t, rr)
	restore = readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 1 || restore.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the restore recorded %+v, want Deployment %s at 2", restore.Status.Quiesced, appN)
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the restore stopped the app, want 0", got)
	}
	step(t, br)
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d after another backup pass, want the restore's stop kept", got)
	}
}

// A quiesce Lease is taken over only when its holder has durably given the
// workloads back: a holder that is gone, finished, or whose stored status
// shows the restart done is stale, whatever the workloads stand at now; a
// holder that still owes a restart, has no plan yet, or is being deleted is
// live.
func TestAQuiesceLeaseIsTakenOverOnlyWhenItsHolderRestarted(t *testing.T) {
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

// A run being deleted with a plan keeps others waiting: its finalizer may
// still restart the app, so its quiesce Lease stays live until it has. The
// second run's volume item fails at its pre-check, since the deleted run's
// tag stays open on the source; its Cluster item is what it still stops the
// app for.
func TestADeletedQuiescedRunKeepsOthersWaitingUntilItGaveTheAppBack(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		secondBackupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	const second = "manual-notes"

	step(t, br) // plan
	step(t, br) // admit
	step(t, br) // quiesce
	step(t, br) // start
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the run stopped the app, want 0", got)
	}

	// The run is deleted while the app is down, and its restart is refused.
	healthy := br.Client
	br.Client = refuseDeploymentPatches(c)
	if err := c.Delete(context.Background(), backupRun()); err != nil {
		t.Fatal(err)
	}
	_ = tryStep(br) // finalize: the restart fails, so the finalizer stays

	stepIn(t, br, ns, second) // plan
	stepIn(t, br, ns, second) // admit
	stepIn(t, br, ns, second) // quiesce: waits for the deleted run
	other := backupIn(t, c, ns, second)
	if len(other.Status.Quiesced) != 0 {
		t.Fatalf("the second run recorded %+v; want no plan while the deleted run owes a restart", other.Status.Quiesced)
	}
	if readyReason(other.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(readyMessage(other.Status.Conditions), "before-upgrade") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the deleted run",
			readyReason(other.Status.Conditions), readyMessage(other.Status.Conditions))
	}

	// The restart goes through, the finalizer is dropped, and the second run
	// plans the count the first gave back.
	br.Client = healthy
	step(t, br)
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d after the deleted run gave the app back, want 2", got)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "before-upgrade"}, backupRun()); !apierrors.IsNotFound(err) {
		t.Fatalf("get the deleted run = %v, want it gone once its finalizer was dropped", err)
	}
	stepIn(t, br, ns, second) // quiesce
	other = backupIn(t, c, ns, second)
	if len(other.Status.Quiesced) != 1 || other.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the second run recorded %+v, want Deployment %s at 2", other.Status.Quiesced, appN)
	}
}

// A run that is about to stop the app takes the namespace's quiesce Lease and
// keeps it while its plan stands; a source BackupRun never takes one, so it
// can start beside a quiesced namespace.
func TestOnlyARunThatIsAboutToStopTheAppTakesTheQuiesceLease(t *testing.T) {
	objects := append([]client.Object{quiescedRestoreOf(),
		secondBackupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = cacheN }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false)}, cacheClaim()...)
	c := newClient(t, objects...)
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}
	const second = "manual-notes"

	restoreStep(t, rr) // quiesce: takes the Lease and stops the app
	restore := readRestoreRun(t, c)
	if held := quiesceLeaseIn(t, c, ns); held == nil || holderUID(held) != string(restoreUID) {
		t.Fatalf("the quiesce Lease after quiesce = %+v, restore = %+v; want the restore holding it", held, restore.Status)
	}

	stepIn(t, br, ns, second) // plan
	stepIn(t, br, ns, second) // admit
	stepIn(t, br, ns, second) // start: a source run never quiesces
	other := backupIn(t, c, ns, second)
	if len(other.Status.Items) != 1 || other.Status.Items[0].Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("the source run's items = %+v, want its claim's item Running beside the held quiesce Lease", other.Status.Items)
	}
	held := quiesceLeaseIn(t, c, ns)
	if held == nil || holderUID(held) != string(restoreUID) {
		t.Errorf("the quiesce Lease after the source run started = %+v, want the restore still holding it", held)
	}
}

// The early Lease release that runs before every pass and the release at the
// end of a run both leave the quiesce Lease alone: only a durable restart
// gives it up.
func TestReleaseLeasesLeavesTheQuiesceLease(t *testing.T) {
	stopped := quiescedBackup(func(b *backupv1alpha1.BackupRun) {
		b.Status.Items = []backupv1alpha1.BackupItem{{Kind: "ReplicationSource", Name: claimN, Phase: backupv1alpha1.ItemSucceeded}}
	})
	app := deployment()
	zero := int32(0)
	app.Spec.Replicas = &zero
	c := newClient(t, stopped, claim(), volume(), volumeRestore(), repository(), app, kustomization(true))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	claimLease, repoLease := leaseNames(t, c)
	for _, name := range []string{claimLease, repoLease, quiesceLeaseName} {
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
		scope := ""
		if name == quiesceLeaseName {
			scope = scopeQuiesce
		}
		stamp(lease, leaseHolder{kind: "BackupRun", run: stopped, item: claimN, scope: scope}, []string{claimN})
		if err := c.Create(context.Background(), lease); err != nil {
			t.Fatal(err)
		}
	}

	// The restart is refused, so the pass ends after the early release and
	// nothing has removed the quiesce Lease yet.
	br.Client = refuseDeploymentPatches(c)
	_ = tryStep(br)
	if got := leaseHolderOf(t, c, claimLease); got != "" {
		t.Errorf("the claim Lease holder = %q after the early release, want it gone", got)
	}
	if got := leaseHolderOf(t, c, repoLease); got != "" {
		t.Errorf("the repository Lease holder = %q after the early release, want it gone", got)
	}
	if got := leaseHolderOf(t, c, quiesceLeaseName); got != string(runUID) {
		t.Fatalf("the quiesce Lease holder = %q after the early release, want the run still holding it", got)
	}

	// A run that ends after its restart gives the quiesce Lease up.
	br.Client = c
	step(t, br) // retry the restart, then finish: the terminal status, then the quiesce Lease goes
	if got := leaseHolderOf(t, c, quiesceLeaseName); got != "" {
		t.Errorf("the quiesce Lease holder = %q after the run finished, want it released", got)
	}
}

// A run that times out while it waits ends Failed naming the wait, with
// nothing stopped, in both directions.
func TestAWaitThatOutlastsTheTimeoutNamesTheWait(t *testing.T) {
	t.Run("backup behind a restore", func(t *testing.T) {
		c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
			restoreRun(func(r *backupv1alpha1.RestoreRun) {
				r.Spec.Claim = claimN
				r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
			}, asOf("2026-09-21T04:00:00Z")),
			claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
		br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
		rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

		restoreStep(t, rr) // plan
		restoreStep(t, rr) // quiesce
		step(t, br)        // plan
		step(t, br)        // admit
		step(t, br)        // quiesce: waits
		br.Now = func() time.Time { return frozen.Add(time.Hour + time.Second) }
		step(t, br) // the timeout passes while the run waits

		backup := readBackupRun(t, c)
		message := readyMessage(backup.Status.Conditions)
		if backup.Status.Phase != backupv1alpha1.RunPhaseFailed {
			t.Fatalf("phase = %q, want Failed", backup.Status.Phase)
		}
		if !strings.Contains(message, frozen.Add(time.Hour).Format(time.RFC3339)) || !strings.Contains(message, "RestoreRun back-to-monday") {
			t.Errorf("message = %q; want the deadline and the restore it waited for", message)
		}
		if got := replicasOf(t, c); got != 0 {
			t.Errorf("replicas = %d after the timed-out backup, want the restore's 0", got)
		}
	})
}

// A run whose stored status shows its restart done never gives the app back
// again, even when the app stands at 0 once more, as it does when another run
// has stopped it since. Before, a restart whose plan did not read back was
// repeated, which scaled the app up under the other run's stop (BRF1F1).
func TestARunNeverRepeatsARestartItsStatusShowsDone(t *testing.T) {
	source := idleSource()
	source.Spec.Trigger.Manual = TriggerFor(runUID)
	r, c := backupReconciler(t, quiescedBackup(func(b *backupv1alpha1.BackupRun) {
		b.Status.StartedAt = atFrozen(-time.Minute)
		b.Status.RestartedAt = atFrozen(0)
		b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		b.Status.Items[0].Phase, b.Status.Items[0].Trigger = backupv1alpha1.ItemRunning, TriggerFor(runUID)
	}), claim(), volume(), volumeRestore(), repository(), source, stoppedDeployment(), kustomization(true))

	step(t, r) // the upload goes on
	if got := replicasOf(t, c); got != 0 || !suspended(t, c) {
		t.Fatalf("replicas = %d, suspended = %t; want the other stop left alone", got, suspended(t, c))
	}
	complete(t, c, "snapshot 6e473100 saved")
	step(t, r) // collect and finish
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 0 || !suspended(t, c) {
		t.Errorf("replicas = %d, suspended = %t after the run finished; want the other stop left alone", got, suspended(t, c))
	}
}

// A RestoreRun whose stored status shows its restart done never gives the
// app back again while it waits for its Cluster to be created, even when the
// app stands at 0 once more.
func TestARestoreNeverRepeatsARestartItsStatusShowsDone(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Database = pgN
		r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
		r.Status.Phase = backupv1alpha1.RunPhaseWaiting
		r.Status.StartedAt = atFrozen(-time.Minute)
		r.Status.QuiescedAt, r.Status.RestartedAt = atFrozen(-time.Minute), atFrozen(0)
		r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
		r.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemDeleted, ClusterUID: "old-cluster-uid"}}
	}), stoppedDeployment(), kustomization(true))

	restoreStep(t, r)

	if got := readRestoreRun(t, c); readyReason(got.Status.Conditions) != backupv1alpha1.ReasonRecreate {
		t.Fatalf("reason = %q (%s), want %s", readyReason(got.Status.Conditions), readyMessage(got.Status.Conditions), backupv1alpha1.ReasonRecreate)
	}
	if got := replicasOf(t, c); got != 0 || !suspended(t, c) {
		t.Errorf("replicas = %d, suspended = %t; want the other stop left alone", got, suspended(t, c))
	}
}

// A namespace BackupRun waits, with the app running and no quiesce Lease
// taken, while a RestoreRun in its namespace has deleted a Cluster and waits
// for it to be created again: the Kustomization the backup would suspend may
// be the one Flux needs to create that Cluster (see waitingOn).
func TestANamespaceBackupWaitsForARestoreThatDeletedItsCluster(t *testing.T) {
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

// pendingRestartBackup returns the BackupRun before-upgrade as a pass whose
// restart failed left it: the app recorded at 2, restartedAt chosen with
// restartPending still set, its Kustomization suspended, and the run past
// its one-hour timeout under the given clock.
func pendingRestartBackup() *backupv1alpha1.BackupRun {
	return quiescedBackup(func(b *backupv1alpha1.BackupRun) {
		b.Finalizers = []string{Finalizer}
		b.Status.StartedAt = atFrozen(0)
		b.Status.RestartedAt, b.Status.RestartPending = atFrozen(0), true
		b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
	})
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

// A BackupRun whose restart is still pending when its timeout ends it gives
// the app back: its stored restartedAt says only that the restart moment was
// chosen, and restartPending says the workloads may still be down, so
// release starts them again.
func TestATimeoutDuringAPendingRestartGivesTheAppBack(t *testing.T) {
	c := newClient(t, pendingRestartBackup(), claim(), volume(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true))
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{},
		Now: func() time.Time { return frozen.Add(time.Hour + time.Second) }}

	step(t, br)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || run.Status.RestartPending {
		t.Fatalf("phase = %q, restartPending = %t (%s); want Failed with the restart done",
			run.Status.Phase, run.Status.RestartPending, readyMessage(run.Status.Conditions))
	}
	if got := replicasOf(t, c); got != 2 || suspended(t, c) {
		t.Errorf("replicas = %d, suspended = %t; want the app back at 2 and its Kustomization resumed", got, suspended(t, c))
	}
}

// A run keeps the namespace's quiesce Lease when the status write that ends
// it is lost: the stored status still shows a restart the run owes, so
// another run that took the Lease then could stop the app and see the retry
// scale it back up. The retry stores the end and releases the Lease.
func TestAFinishWhoseStatusWriteIsLostKeepsTheQuiesceLease(t *testing.T) {
	t.Run("BackupRun", func(t *testing.T) {
		c := newClient(t, pendingRestartBackup(), claim(), volume(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true))
		br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{},
			Now: func() time.Time { return frozen.Add(time.Hour + time.Second) }}
		seedQuiesceLease(t, c, "BackupRun", pendingRestartBackup())

		br.Client = loseStatusWriteAt(c, 2)
		if err := tryStep(br); err == nil {
			t.Fatal("the pass succeeded although the status write that ends the run was lost")
		}
		stored := readBackupRun(t, c)
		if stored.Status.Phase.Finished() || !stored.Status.RestartPending || replicasOf(t, c) != 2 {
			t.Fatalf("phase = %q, restartPending = %t, replicas = %d; want the app back and the lost end unstored",
				stored.Status.Phase, stored.Status.RestartPending, replicasOf(t, c))
		}
		if got := leaseHolderOf(t, c, quiesceLeaseName); got != string(runUID) {
			t.Fatalf("the quiesce Lease holder = %q after the lost end, want the run still holding it", got)
		}

		br.Client = c
		step(t, br)
		if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseFailed {
			t.Fatalf("phase = %q after the retry, want Failed", run.Status.Phase)
		}
		if got := leaseHolderOf(t, c, quiesceLeaseName); got != "" {
			t.Errorf("the quiesce Lease holder = %q after the run ended, want it released", got)
		}
	})

	t.Run("RestoreRun", func(t *testing.T) {
		stopping := func() *backupv1alpha1.RestoreRun {
			return checkedRestore(func(r *backupv1alpha1.RestoreRun) {
				r.Finalizers = []string{Finalizer}
				r.Spec.Claim = claimN
				r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
				r.Status.QuiescedAt = atFrozen(0)
				r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
			})
		}
		c := newClient(t, stopping(), claim(), volumeRestore(), repository(), stoppedDeployment())
		rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday},
			Now: func() time.Time { return frozen.Add(4*time.Hour + time.Second) }}
		seedQuiesceLease(t, c, "RestoreRun", stopping())

		rr.Client = loseNextStatusWrite(c)
		if _, err := rr.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
			t.Fatal("the pass succeeded although the status write that ends the run was lost")
		}
		stored := readRestoreRun(t, c)
		if stored.Status.Phase.Finished() || stored.Status.RestartedAt != nil || replicasOf(t, c) != 2 {
			t.Fatalf("phase = %q, restartedAt = %v, replicas = %d; want the app back and the lost end unstored",
				stored.Status.Phase, stored.Status.RestartedAt, replicasOf(t, c))
		}
		if got := leaseHolderOf(t, c, quiesceLeaseName); got != string(restoreUID) {
			t.Fatalf("the quiesce Lease holder = %q after the lost end, want the run still holding it", got)
		}

		rr.Client = c
		restoreStep(t, rr)
		if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseFailed {
			t.Fatalf("phase = %q after the retry, want Failed", run.Status.Phase)
		}
		if got := leaseHolderOf(t, c, quiesceLeaseName); got != "" {
			t.Errorf("the quiesce Lease holder = %q after the run ended, want it released", got)
		}
	})
}

// A BackupRun that times out while it waits, and whose restart then fails
// for a pass, still ends with the wait it timed out on: the failed restart
// replaces the SourceBusy condition with RestartFailed, and the end reads
// the wait back from the message its items already carry. A Pending item
// whose start failed keeps its last error in its own message only.
func TestATimedOutBackupKeepsItsWaitAfterAFailedRestart(t *testing.T) {
	wait := "RestoreRun back-to-monday is restoring claim " + claimN + "; this run starts once that restore has finished"
	c := newClient(t, quiescedBackup(func(b *backupv1alpha1.BackupRun) {
		b.Finalizers = []string{Finalizer}
		b.Status.StartedAt = atFrozen(0)
		b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		b.Status.Items = append(b.Status.Items, backupv1alpha1.BackupItem{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemPending,
			Message: notStartedYet + "the webhook refused the Backup"})
		backupv1alpha1.SetReady(&b.Status.Conditions, b.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonSourceBusy, wait)
	}), claim(), volume(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true))
	refused := true
	refusing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && refused {
				return apierrors.NewInternalError(errors.New("the API server cannot scale the Deployment"))
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	br := &BackupRunReconciler{Client: refusing, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{},
		Now: func() time.Time { return frozen.Add(time.Hour + time.Second) }}

	if err := tryStep(br); err == nil {
		t.Fatal("the pass whose restart failed returned no error")
	}
	if got := readBackupRun(t, c); readyReason(got.Status.Conditions) != backupv1alpha1.ReasonRestartFailed {
		t.Fatalf("reason = %q after the failed restart, want RestartFailed", readyReason(got.Status.Conditions))
	}
	refused = false
	step(t, br)

	run := readBackupRun(t, c)
	message := readyMessage(run.Status.Conditions)
	want := "the run had not finished by " + frozen.Add(time.Hour).Format(time.RFC3339) + "; it was waiting: " + wait
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || message != want {
		t.Fatalf("phase = %q, message = %q; want Failed with %q", run.Status.Phase, message, want)
	}
	if got := run.Status.Items[1].Message; got != want+"; last error: the webhook refused the Backup" {
		t.Errorf("the Cluster item's message = %q, want the end with its last error", got)
	}
}

// backupTimedOut reads the end back from a failed item's message without the
// sentence syncGoesOn added to a Running item's message.
func TestBackupTimedOutDropsTheSyncNote(t *testing.T) {
	deadline := frozen.Add(time.Hour)
	end := timedOutMessage(deadline, nil) + "; it was waiting: BackupRun manual-notes holds the claim"
	for _, note := range []string{
		"VolSync keeps retrying the sync it started at 2026-09-24T12:00:00Z with the clone it cut then, so a snapshot this sync saves later holds the data of 2026-09-24T12:00:00Z, whatever time restic stamps on it.",
		"VolSync goes on with the sync it started at 2026-09-24T12:00:00Z and has not cut its clone yet, so a snapshot this sync saves later holds the data of the moment it cuts the clone, whatever time restic stamps on it.",
	} {
		run := backupRun(func(b *backupv1alpha1.BackupRun) {
			b.Status.Items = []backupv1alpha1.BackupItem{{Kind: "ReplicationSource", Name: claimN, Phase: backupv1alpha1.ItemFailed, Message: end + ". " + note}}
		})
		if got := backupTimedOut(run, deadline); got != end {
			t.Errorf("backupTimedOut = %q, want %q", got, end)
		}
	}
}
