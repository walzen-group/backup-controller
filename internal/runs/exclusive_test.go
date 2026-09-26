package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/populator"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restoring returns the RestoreRun back-to-monday in the middle of an
// in-place restore of the claim, and the ReplicationDestination its mover
// writes the claim from.
func restoring() (*backupv1alpha1.RestoreRun, *volsyncv1alpha1.ReplicationDestination) {
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN,
			Phase: backupv1alpha1.ItemRunning, Destination: destinationName(restoreUID, 0)}}
	})
	claimName := claimN
	destination := &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: destinationName(restoreUID, 0), Namespace: ns},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: string(restoreUID)},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodDirect, DestinationPVC: &claimName,
				},
				Repository: repoN,
			},
		},
	}
	return run, destination
}

// backingUp returns the claim's ReplicationSource with the open trigger of
// the BackupRun manual-notes (see otherRun), which VolSync has not completed.
func backingUp() *volsyncv1alpha1.ReplicationSource {
	source := idleSource()
	source.Spec.Trigger.Manual = TriggerFor(otherRunUID)
	source.Spec.Restic = &volsyncv1alpha1.ReplicationSourceResticSpec{Repository: repoN}
	return source
}

// ownSourceTag returns the manual tag on the claim's ReplicationSource, or
// "" when there is no source.
func ownSourceTag(t *testing.T, c client.Client) string {
	t.Helper()
	sources := &volsyncv1alpha1.ReplicationSourceList{}
	if err := c.List(context.Background(), sources, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	for _, s := range sources.Items {
		if s.Name == claimN {
			return manualTag(&s)
		}
	}
	return ""
}

// destinations returns the names of the ReplicationDestinations in the
// namespace.
func destinations(t *testing.T, c client.Client) []string {
	t.Helper()
	list := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := c.List(context.Background(), list, client.InNamespace(ns)); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, d := range list.Items {
		names = append(names, d.Name)
	}
	return names
}

// A backup started while a restore writes the claim waits with reason
// SourceBusy, names the restore, and writes no trigger: its clone would cut
// a half-restored volume, and its forget would fail on the restore's lock.
func TestABackupWaitsWhileARestoreOfTheClaimRuns(t *testing.T) {
	restore, destination := restoring()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(), restore, destination)
	step(t, r)
	step(t, r)
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if msg := readyMessage(run.Status.Conditions); !strings.Contains(msg, "RestoreRun back-to-monday") {
		t.Errorf("message = %q, want it to name the RestoreRun back-to-monday", msg)
	}
	if tag := ownSourceTag(t, c); tag == TriggerFor(runUID) {
		t.Errorf("the backup wrote its trigger while the restore ran")
	}
}

// startOtherBackup stands in for the BackupRun manual-notes (see otherRun)
// starting after a restore's checks: it creates the run and the claim's
// ReplicationSource with the run's open trigger (see backingUp), each with
// its status. The API server gives the created run a UID of its own, and the
// trigger is made from it.
func startOtherBackup(t *testing.T, c client.Client) {
	t.Helper()
	ctx := context.Background()
	run, source := otherRun(), backingUp()
	if err := c.Create(ctx, run); err != nil {
		t.Fatal(err)
	}
	run.Status = otherRun().Status
	run.Status.Items[0].Trigger = TriggerFor(run.UID)
	if err := c.Status().Update(ctx, run); err != nil {
		t.Fatal(err)
	}
	source.Spec.Trigger.Manual = TriggerFor(run.UID)
	if err := c.Create(ctx, source); err != nil {
		t.Fatal(err)
	}
	source.Status = backingUp().Status
	if err := c.Status().Update(ctx, source); err != nil {
		t.Fatal(err)
	}
}

// A restore past its checks whose claim's repository a backup started on
// since waits with reason SourceBusy, names the backup, and creates no
// ReplicationDestination. (A backup already running at the checks holds the
// checks themselves; see
// TestARestoreSelectsItsSnapshotOnlyOnceABackupOfItsRepositoryHasFinished.)
func TestARestoreWaitsWhileABackupRuns(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	startOtherBackup(t, c)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if msg := readyMessage(run.Status.Conditions); !strings.Contains(msg, "BackupRun manual-notes") {
		t.Errorf("message = %q, want it to name the BackupRun manual-notes", msg)
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Errorf("destinations = %v, want none while the backup runs", names)
	}
}

// A backup and a restore of the same claim never wait on each other:
// whichever writes its mover object first goes on, and the other waits for
// it and names it. That holds when one starts well before the other, and when
// both are reconciled in turn from the start, as in one pass of the manager.
func TestABackupAndARestoreStartedTogetherNeverDeadlock(t *testing.T) {
	orders := map[string][]string{
		"backup long before":     {"b", "b", "b", "r", "r", "b", "r"},
		"restore long before":    {"r", "r", "b", "b", "b", "r", "b"},
		"in turn, backup first":  {"b", "r", "b", "r", "b", "r", "b", "r"},
		"in turn, restore first": {"r", "b", "r", "b", "r", "b", "r", "b"},
	}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
				claim(), volume(), volumeRestore(), repository())
			br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
			rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}
			for _, who := range order {
				if who == "b" {
					step(t, br)
				} else {
					restoreStep(t, rr)
				}
			}

			backup, restore := readBackupRun(t, c), readRestoreRun(t, c)
			backupWaits := readyReason(backup.Status.Conditions) == backupv1alpha1.ReasonSourceBusy
			restoreWaits := readyReason(restore.Status.Conditions) == backupv1alpha1.ReasonSourceBusy
			if backupWaits == restoreWaits {
				t.Fatalf("backup waits = %v, restore waits = %v; want exactly one to wait", backupWaits, restoreWaits)
			}
			triggered, restoring := ownSourceTag(t, c) == TriggerFor(runUID), len(destinations(t, c)) == 1
			if triggered == restoring {
				t.Fatalf("trigger written = %v, destination created = %v; want exactly one mover object", triggered, restoring)
			}
			if restoring {
				if !backupWaits || !strings.Contains(readyMessage(backup.Status.Conditions), "RestoreRun back-to-monday") {
					t.Errorf("backup reason = %q, message = %q; want it to wait for the RestoreRun",
						readyReason(backup.Status.Conditions), readyMessage(backup.Status.Conditions))
				}
			} else if !restoreWaits || !strings.Contains(readyMessage(restore.Status.Conditions), "BackupRun before-upgrade") {
				t.Errorf("restore reason = %q, message = %q; want it to wait for the BackupRun",
					readyReason(restore.Status.Conditions), readyMessage(restore.Status.Conditions))
			}
			if strings.HasSuffix(name, "long before") && strings.HasPrefix(name, "backup") != triggered {
				t.Errorf("triggered = %v; the run that started long before must go first", triggered)
			}
		})
	}
}

// A destination left by a RestoreRun that finished or no longer exists does
// not hold a backup, and a source whose trigger a finished or deleted
// BackupRun completed does not hold a restore.
func TestAFinishedOrDeletedRunDoesNotBlock(t *testing.T) {
	finishedRestore, destination := restoring()
	finishedRestore.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	finishedRestore.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	for name, objects := range map[string][]client.Object{
		"finished restore": {finishedRestore, destination},
		"deleted restore":  {destination},
	} {
		t.Run(name, func(t *testing.T) {
			objects = append(objects, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository())
			r, c := backupReconciler(t, objects...)
			step(t, r)
			step(t, r)
			step(t, r)
			run := readBackupRun(t, c)
			if run.Status.Items[0].Phase != backupv1alpha1.ItemRunning || ownSourceTag(t, c) != TriggerFor(runUID) {
				t.Fatalf("item = %+v, reason = %q; want Running with the run's trigger written", run.Status.Items[0], readyReason(run.Status.Conditions))
			}
		})
	}

	finishedBackup := otherRun()
	finishedBackup.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	finishedBackup.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	completed := backingUp()
	completed.Status.LastManualSync = TriggerFor(otherRunUID)
	for name, objects := range map[string][]client.Object{
		"finished backup": {finishedBackup, completed},
		"deleted backup":  {completed.DeepCopy()},
	} {
		t.Run(name, func(t *testing.T) {
			objects = append(objects, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
				claim(), volumeRestore(), repository())
			r, c := restoreReconciler(t, nil, objects...)
			restoreStep(t, r)
			restoreStep(t, r)
			run := readRestoreRun(t, c)
			if run.Status.Items[0].Phase != backupv1alpha1.ItemRunning || len(destinations(t, c)) != 1 {
				t.Fatalf("item = %+v, reason = %q; want Running with its destination", run.Status.Items[0], readyReason(run.Status.Conditions))
			}
		})
	}
}

// frozenNow returns the frozen clock's time.
func frozenNow() time.Time { return frozen }

// A restore into a new claim from a repository that a backup started writing
// after the checks waits, creates neither the claim nor the destination, and
// goes on once the backup has finished.
func TestAnIntoRestoreWaitsWhileItsRepositoryIsBackedUp(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan
	startOtherBackup(t, c)
	restoreStep(t, r) // waits

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || !strings.Contains(readyMessage(run.Status.Conditions), "BackupRun manual-notes") {
		t.Fatalf("phase = %q, message = %q; want Waiting for the BackupRun manual-notes", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
	if names := destinations(t, c); len(names) != 0 {
		t.Fatalf("destinations = %v, want none while the backup runs", names)
	}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: "scratch"}, &corev1.PersistentVolumeClaim{}); err == nil {
		t.Fatal("the claim was created while the backup ran")
	}

	backup := &backupv1alpha1.BackupRun{}
	get(t, c, ns, "manual-notes", backup)
	backup.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	backup.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	if err := c.Status().Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	source.Status.LastManualSync = manualTag(source)
	if err := c.Status().Update(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)
	run = readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseRunning || len(destinations(t, c)) != 1 {
		t.Fatalf("phase = %q, destinations = %v; want Running with its destination", run.Status.Phase, destinations(t, c))
	}
}

// A namespace run waits before it stops anything while a restore writes one
// of its claims, so the app is not held down for the length of the restore.
func TestANamespaceRunWaitsForARestoreBeforeItQuiesces(t *testing.T) {
	restore, destination := restoring()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false), localQueueObject(),
		restore, destination)
	step(t, r) // plan
	step(t, r) // admit
	admitAll(t, c)
	step(t, r) // admitted
	step(t, r) // would quiesce

	run := readBackupRun(t, c)
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy || !strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun back-to-monday") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the RestoreRun", readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 || run.Status.QuiescedAt != nil {
		t.Errorf("replicas = %d, quiescedAt = %v; want the app left running", *d.Spec.Replicas, run.Status.QuiescedAt)
	}
}

// A restore of the claim's backups into a new claim writes through its own
// ReplicationDestination, which names the claim's repository: a backup of the
// claim waits while it is there.
func TestABackupWaitsWhileAnIntoRestoreFromTheClaimRuns(t *testing.T) {
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "scratch" }, asOf("2026-09-21T04:00:00Z")),
		claim(), volume(), volumeRestore(), repository())
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}
	restoreStep(t, rr) // plan
	restoreStep(t, rr) // creates the claim and the destination
	if names := destinations(t, c); len(names) != 1 {
		t.Fatalf("destinations = %v, want the restore's", names)
	}
	step(t, br)
	step(t, br)
	step(t, br)

	run := readBackupRun(t, c)
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy || !strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun back-to-monday") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the RestoreRun", readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if ownSourceTag(t, c) == TriggerFor(runUID) {
		t.Error("the backup wrote its trigger while the restore ran")
	}
}

// A restore a v0.8.1 controller started through the populator has its mover
// in the controller's namespace. Its VolumeRestore stands for that mover: a
// backup of the claim waits while the run is live and the VolumeRestore is
// there.
func TestABackupWaitsWhileAPopulatorRestoreOfTheClaimRuns(t *testing.T) {
	restore := populatorRun()
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		restore, populatorRestore(restore, populator.Finalizer), populatorClaim(restore, corev1.ClaimPending, populator.ClaimFinalizer),
		claim(), volume(), volumeRestore(), repository())
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	step(t, br)
	step(t, br)
	step(t, br)

	run := readBackupRun(t, c)
	if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy || !strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun back-to-monday") {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the RestoreRun", readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if ownSourceTag(t, c) == TriggerFor(runUID) {
		t.Error("the backup wrote its trigger while the restore ran")
	}
}
