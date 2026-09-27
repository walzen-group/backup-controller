package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restoring returns the RestoreRun back-to-monday in the middle of an
// in-place restore of the claim, and the restore Job its item names, which
// writes the claim from the repository. The Job has no conditions yet, so
// the Job controller may start a pod for it at any time.
func restoring(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job) {
	t.Helper()
	return restoringInto(t, claimN)
}

// restoringInto returns the RestoreRun back-to-monday in the middle of an
// in-place restore, and the restore Job its item names.
//
// Parameters:
//   - t fails the test when the Job cannot be built.
//   - claimName is the claim the run restores and its Job writes. A test
//     that wants the Job to share only the repository with a backup of
//     claimN passes another claim.
//
// The Job restores monday from the repository repoN and has the UID jobUID,
// which the run's item records beside the Job's name.
func restoringInto(t *testing.T, claimName string) (*backupv1alpha1.RestoreRun, *batchv1.Job) {
	t.Helper()
	run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimName
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
	})
	job := restoreJobFor(t, run, claimName, monday.ID)
	run.Status.Items = []backupv1alpha1.RestoreItem{{Kind: backupv1alpha1.ItemKindClaim, Name: claimName,
		Phase: backupv1alpha1.ItemRunning, SnapshotID: monday.ID, Job: job.Name, JobUID: job.UID}}
	return run, job
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

// A backup started while a restore writes the claim waits with reason
// SourceBusy, names the restore, and writes no trigger: its clone would cut
// a half-restored volume, and its forget would fail on the restore's lock.
func TestABackupWaitsWhileARestoreOfTheClaimRuns(t *testing.T) {
	t.Parallel()
	restore, job := restoring(t)
	expectBackupWaitsForRestore(t, restore, job)
}

// A backup started while a restore of another claim writes from the same
// repository waits with reason SourceBusy, names the restore, and writes no
// trigger: its forget needs the exclusive lock, which fails while the
// restore holds its read lock.
func TestABackupWaitsWhileARestoreFromItsRepositoryRuns(t *testing.T) {
	t.Parallel()
	restore, job := restoringInto(t, "other-data")
	expectBackupWaitsForRestore(t, restore, job)
}

// expectBackupWaitsForRestore runs the BackupRun before-upgrade of claimN
// beside a RestoreRun whose restore Job writes the claim or reads its
// repository, and checks that the backup waits with reason SourceBusy,
// names the RestoreRun, and writes no trigger.
//
// Parameters:
//   - t reports the failures.
//   - restore is the RestoreRun, stored as given with no Lease of its own,
//     as a run that started before the controller took Leases.
//   - job is the restore Job the run's item names, stored as given.
func expectBackupWaitsForRestore(t *testing.T, restore *backupv1alpha1.RestoreRun, job *batchv1.Job) {
	t.Helper()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(), restore, job)
	step(t, r)
	step(t, r)
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if msg := readyMessage(run.Status.Conditions); !strings.Contains(msg, "RestoreRun back-to-monday") || !strings.Contains(msg, job.Name) {
		t.Errorf("message = %q, want it to name the RestoreRun back-to-monday and its restore Job %s", msg, job.Name)
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
// restore Job. (A backup already running at the checks holds the
// checks themselves; see
// TestARestoreSelectsItsSnapshotOnlyOnceABackupOfItsRepositoryHasFinished.)
func TestARestoreWaitsWhileABackupRuns(t *testing.T) {
	t.Parallel()
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
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("restore Jobs = %v, want none while the backup runs", names)
	}
}

// A backup and a restore of the same claim never wait on each other:
// whichever writes its mover object first goes on, and the other waits for
// it and names it. That holds when one starts well before the other, and when
// both are reconciled in turn from the start, as in one pass of the manager.
func TestABackupAndARestoreStartedTogetherNeverDeadlock(t *testing.T) {
	t.Parallel()
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
			triggered, restoring := ownSourceTag(t, c) == TriggerFor(runUID), len(movers(t, c)) == 1
			if triggered == restoring {
				t.Fatalf("trigger written = %v, mover created = %v; want exactly one mover object", triggered, restoring)
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

// A Complete restore Job with no pod left, of a RestoreRun that finished or
// no longer exists, does not hold a backup, and a source whose trigger a
// finished or deleted BackupRun completed does not hold a restore.
func TestAFinishedOrDeletedRunDoesNotBlock(t *testing.T) {
	t.Parallel()
	finishedRestore, job := restoring(t)
	finishedRestore.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	finishedRestore.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
	for name, objects := range map[string][]client.Object{
		"finished restore": {finishedRestore, job.DeepCopy()},
		"deleted restore":  {job.DeepCopy()},
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
			if run.Status.Items[0].Phase != backupv1alpha1.ItemRunning || len(movers(t, c)) != 1 {
				t.Fatalf("item = %+v, reason = %q; want Running with its restore Job", run.Status.Items[0], readyReason(run.Status.Conditions))
			}
		})
	}
}

// frozenNow returns the frozen clock's time.
func frozenNow() time.Time { return frozen }

// A restore into a new claim from a repository that a backup started writing
// after the checks waits, creates neither the claim nor the restore Job, and
// goes on once the backup has finished.
func TestAnIntoRestoreWaitsWhileItsRepositoryIsBackedUp(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan
	startOtherBackup(t, c)
	restoreStep(t, r) // waits

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || !strings.Contains(readyMessage(run.Status.Conditions), "BackupRun manual-notes") {
		t.Fatalf("phase = %q, message = %q; want Waiting for the BackupRun manual-notes", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
	if names := movers(t, c); len(names) != 0 {
		t.Fatalf("movers = %v, want none while the backup runs", names)
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
	if run.Status.Phase != backupv1alpha1.RunPhaseRunning || len(restoreJobs(t, c)) != 1 {
		t.Fatalf("phase = %q, restore Jobs = %v; want Running with its restore Job", run.Status.Phase, restoreJobs(t, c))
	}
}

// A restore of the claim's backups into a new claim holds the Leases of the
// claim and its repository while its restore Job writes: a backup of the
// claim waits while it runs.
func TestABackupWaitsWhileAnIntoRestoreFromTheClaimRuns(t *testing.T) {
	t.Parallel()
	c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim, r.Spec.Into = claimN, "scratch" }, asOf("2026-09-21T04:00:00Z")),
		claim(), volume(), volumeRestore(), repository())
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, RestoreImage: testImage, Now: frozenNow}
	restoreStep(t, rr) // plan
	restoreStep(t, rr) // creates the claim and the restore Job
	if jobs := restoreJobs(t, c); len(jobs) != 1 {
		t.Fatalf("restore Jobs = %v, want the restore's", jobs)
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

// restoreJobCase is a state of a restore Job and its RestoreRun, for the
// test of which restore Jobs hold a claim and its repository.
type restoreJobCase struct {
	// name names the subtest.
	name string
	// item is the phase of the run's item that names the Job.
	item backupv1alpha1.ItemPhase
	// noRun leaves the RestoreRun out, as after it was deleted.
	noRun bool
	// run changes the RestoreRun before the test stores it.
	run func(run *backupv1alpha1.RestoreRun)
	// deleting deletes the stored RestoreRun, which a finalizer keeps.
	deleting bool
	// job changes the Job before the test stores it.
	job func(job *batchv1.Job)
	// pod is the phase of the Job's one pod, or "" for a Job whose pods are
	// gone.
	pod corev1.PodPhase
	// holds is whether a backup of the claim must wait.
	holds bool
}

// completeCondition marks a Job Complete, as the Job controller does once
// restic exited 0.
func completeCondition(job *batchv1.Job) {
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue}}
}

// suspendedCondition marks a Job suspended, as a run's stop leaves it once
// the Job controller took its pods down.
func suspendedCondition(job *batchv1.Job) {
	job.Spec.Suspend = ptr.To(true)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuspended, Status: corev1.ConditionTrue}}
}

// A restore Job holds the claim and the repository while its run's item has
// not finished, and while it or its pods may still write: the Job controller
// may start a pod for a Job that is neither finished nor suspended, and a pod
// that has not ended may run restic. A Job whose item has finished and whose
// pods have all ended, or are gone, holds nothing, even while it is still
// there: a run records the item's end before it stops and deletes the Job.
func TestWhichRestoreJobsHoldABackup(t *testing.T) {
	t.Parallel()
	for _, tc := range []restoreJobCase{
		{name: "running", item: backupv1alpha1.ItemRunning, pod: corev1.PodRunning, holds: true},
		{name: "not started", item: backupv1alpha1.ItemRunning, holds: true},
		{name: "complete, item not yet written", item: backupv1alpha1.ItemRunning, job: completeCondition, holds: true},
		{name: "complete, pods gone", item: backupv1alpha1.ItemSucceeded, job: completeCondition},
		{name: "complete, pod succeeded", item: backupv1alpha1.ItemSucceeded, job: completeCondition, pod: corev1.PodSucceeded},
		{name: "failed item, Job not yet suspended", item: backupv1alpha1.ItemFailed, pod: corev1.PodRunning, holds: true},
		{name: "suspended, pod still running", item: backupv1alpha1.ItemFailed, job: suspendedCondition, pod: corev1.PodRunning, holds: true},
		{name: "suspended, pod failed", item: backupv1alpha1.ItemFailed, job: suspendedCondition, pod: corev1.PodFailed},
		{name: "suspended, pods gone", item: backupv1alpha1.ItemFailed, job: suspendedCondition},
		{name: "run deleted, Job not finished", noRun: true, holds: true},
		{name: "run deleted, pod still running", noRun: true, job: suspendedCondition, pod: corev1.PodRunning, holds: true},
		{name: "run deleted, Job complete, pods gone", noRun: true, job: completeCondition},
		{name: "run being deleted, item Running, Job complete, pods gone", item: backupv1alpha1.ItemRunning, deleting: true, job: completeCondition},
		{name: "run finished, item Running, Job complete, pods gone", item: backupv1alpha1.ItemRunning, job: completeCondition,
			run: func(run *backupv1alpha1.RestoreRun) { run.Status.Phase = backupv1alpha1.RunPhaseFailed }},
		{name: "item failed without naming its Job, Job suspended since its create", item: backupv1alpha1.ItemFailed, job: suspendedCondition,
			run: func(run *backupv1alpha1.RestoreRun) { run.Status.Items[0].Job, run.Status.Items[0].JobUID = "", "" }, holds: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			busy := restoreJobHold(t, tc)
			if held := busy != ""; held != tc.holds {
				t.Errorf("restoreInProgress = %q, want held = %t", busy, tc.holds)
			}
		})
	}
}

// restoreJobHold stores the state a restoreJobCase describes and returns
// what restoreInProgress says about a backup of claimN to repoN.
func restoreJobHold(t *testing.T, tc restoreJobCase) string {
	t.Helper()
	run, job := restoring(t)
	run.Status.Items[0].Phase = tc.item
	if tc.job != nil {
		tc.job(job)
	}
	if tc.run != nil {
		tc.run(run)
	}
	objects := []client.Object{job}
	if !tc.noRun {
		objects = append(objects, run)
	}
	c := newClient(t, objects...)
	if tc.deleting {
		deleteKept(t, c, run.Name)
	}
	if tc.pod != "" {
		createPod(t, c, jobPodOf(job, job.Name+"-x7k2p", tc.pod))
	}
	busy, err := restoreInProgress(context.Background(), c, ns, claimN, repoN)
	if err != nil {
		t.Fatal(err)
	}
	return busy.text
}

// deleteKept gives the stored RestoreRun of the given name a finalizer and
// deletes it, so the run is being deleted and stays.
func deleteKept(t *testing.T, c client.Client, name string) {
	t.Helper()
	stored := &backupv1alpha1.RestoreRun{}
	get(t, c, ns, name, stored)
	stored.Finalizers = []string{Finalizer}
	if err := c.Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	if err := c.Delete(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
}

// A restore Job holds only the claim it writes and the repository it reads,
// and only a Job the controller labelled as its own restore Job counts.
func TestARestoreJobHoldsOnlyItsClaimAndRepository(t *testing.T) {
	t.Parallel()
	run, job := restoring(t)
	c := newClient(t, run, job)
	for _, tc := range []struct {
		claim, repository string
		holds             bool
	}{
		{claimN, "", true},
		{"", repoN, true},
		{"other-data", "other-restic-data", false},
		{"", "", false},
	} {
		busy, err := restoreInProgress(context.Background(), c, ns, tc.claim, tc.repository)
		if err != nil {
			t.Fatal(err)
		}
		if held := busy.held(); held != tc.holds {
			t.Errorf("claim %q, repository %q: restoreInProgress = %q, want held = %t", tc.claim, tc.repository, busy.text, tc.holds)
		}
	}
	job.Labels = nil
	c = newClient(t, run, job)
	if busy, err := restoreInProgress(context.Background(), c, ns, claimN, repoN); err != nil || busy.held() {
		t.Errorf("restoreInProgress = %q, %v; want a Job without the controller's labels to hold nothing", busy.text, err)
	}
}

// A BackupRun that has finished can leave its trigger open on the claim's
// ReplicationSource while VolSync retries the sync it started
// (status.lastSyncStartTime set): a mover pod may be writing the repository.
// A restore of the claim waits with reason SourceBusy, creates no
// restore Job, and its message says how the retrying sync ends
// and when the source may be deleted.
func TestARestoreWaitsWhileVolSyncRetriesTheSyncOfAFinishedBackup(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	restoreStep(t, r) // plan

	ctx := context.Background()
	startOtherBackup(t, c)
	failed := &backupv1alpha1.BackupRun{}
	get(t, c, ns, "manual-notes", failed)
	failed.Status.Phase = backupv1alpha1.RunPhaseFailed
	failed.Status.Items[0].Phase = backupv1alpha1.ItemFailed
	if err := c.Status().Update(ctx, failed); err != nil {
		t.Fatal(err)
	}
	retrying := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, retrying)
	started := metav1.NewTime(frozen.Add(-time.Hour))
	retrying.Status.LastSyncStartTime = &started
	if err := c.Status().Update(ctx, retrying); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("restore Jobs = %v, want none while VolSync retries the sync", names)
	}
	msg := readyMessage(run.Status.Conditions)
	for _, want := range []string{"ReplicationSource " + claimN + " is still syncing", "delete the ReplicationSource " + claimN,
		"no pod of Job volsync-src-" + claimN + " is running"} {
		if !strings.Contains(msg, want) {
			t.Errorf("message = %q, want it to contain %q", msg, want)
		}
	}
}
