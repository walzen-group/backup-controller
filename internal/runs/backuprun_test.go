package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func backupRun(mutate ...func(*backupv1alpha1.BackupRun)) *backupv1alpha1.BackupRun {
	run := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "before-upgrade", Namespace: ns, UID: runUID, Generation: 1},
		Spec:       backupv1alpha1.BackupRunSpec{Timeout: &metav1.Duration{Duration: time.Hour}},
	}
	for _, m := range mutate {
		m(run)
	}
	return run
}

func backupReconciler(t *testing.T, objects ...client.Object) (*BackupRunReconciler, client.Client) {
	t.Helper()
	c := newClient(t, objects...)
	return &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: func() time.Time { return frozen }}, c
}

// step reconciles the run once and returns what the reconcile asked for.
func step(t *testing.T, r *BackupRunReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

func readBackupRun(t *testing.T, c client.Client) *backupv1alpha1.BackupRun {
	t.Helper()
	run := &backupv1alpha1.BackupRun{}
	get(t, c, ns, "before-upgrade", run)
	return run
}

// complete stands in for VolSync finishing the tag the run wrote.
func complete(t *testing.T, c client.Client, logs string) {
	t.Helper()
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	source.Status = &volsyncv1alpha1.ReplicationSourceStatus{
		LastManualSync:    manualTag(source),
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultSuccessful, Logs: logs},
	}
	if err := c.Status().Update(context.Background(), source); err != nil {
		t.Fatalf("complete the source: %v", err)
	}
}

// A volume run writes the claim's ReplicationSource itself, placed by the
// volume's node, and reports the time restic stamped on the snapshot.
func TestAVolumeRunWritesTheSourceAndReportsResticsTime(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())

	step(t, r) // plan
	step(t, r) // admit: no LocalQueue in the namespace, so it starts at once
	step(t, r) // start

	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	if got := manualTag(source); got != TriggerFor(runUID) {
		t.Errorf("manual tag = %q, want the run's", got)
	}
	if source.Labels[backupv1alpha1.LabelManagedBy] != backupv1alpha1.ManagedByValue {
		t.Error("the source is not marked as the controller's")
	}
	if source.Spec.Restic.Repository != repoN || *source.Spec.Restic.Retain.Last != "10" {
		t.Errorf("restic = %+v, want the VolumeRestore's repository and the claim's retention", source.Spec.Restic)
	}
	terms := source.Spec.Restic.MoverAffinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms
	if terms[0].MatchExpressions[0].Values[0] != "worker-1" {
		t.Errorf("mover affinity = %+v, want the volume's node", terms)
	}
	if len(source.Spec.Restic.MoverPodLabels) != 0 {
		t.Errorf("mover labels = %v; a run is admitted as a whole, so its movers carry no queue label", source.Spec.Restic.MoverPodLabels)
	}
	if len(source.OwnerReferences) != 1 || source.OwnerReferences[0].Name != claimN {
		t.Errorf("owners = %v, want the claim", source.OwnerReferences)
	}

	complete(t, c, "using parent snapshot 2edf5bab\nsnapshot 6e473100 saved\nRestic completed in 2s")
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	item := run.Status.Items[0]
	if item.Snapshot != "6e473100" || item.SnapshotTime == nil || !item.SnapshotTime.Equal(&metav1.Time{Time: monday.Time}) {
		t.Errorf("item = %+v, want snapshot 6e473100 at %s", item, monday.Time)
	}
	if calls := r.Retimer.(*retimer).calls; len(calls) != 0 {
		t.Errorf("retimed %+v; a run that stopped nothing has no quiesce moment to move the snapshot to", calls)
	}
	if len(run.Finalizers) != 0 {
		t.Error("the finished run kept its finalizer")
	}

	// The spent tag stays: a source with no trigger at all syncs in a loop.
	get(t, c, ns, claimN, source)
	if manualTag(source) == "" {
		t.Error("the tag was cleared, which leaves VolSync syncing continuously")
	}
}

// startVolumeRun runs a volume run on the claim up to the source being written,
// and returns the client.
func startVolumeRun(t *testing.T, annotations map[string]string) client.Client {
	t.Helper()
	pvc := claim()
	pvc.Annotations = annotations
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		pvc, volume(), volumeRestore(), repository())
	step(t, r)
	step(t, r)
	step(t, r)
	return c
}

// A claim keeping snapshots by age gets restic's tiers on its source, the way
// VolSync's own retain block takes them.
func TestTieredRetentionReachesTheSource(t *testing.T) {
	c := startVolumeRun(t, map[string]string{
		backupv1alpha1.AnnotationEnabled:       "true",
		backupv1alpha1.AnnotationRetainHourly:  "24",
		backupv1alpha1.AnnotationRetainDaily:   "7",
		backupv1alpha1.AnnotationRetainWeekly:  "4",
		backupv1alpha1.AnnotationRetainMonthly: "6",
		backupv1alpha1.AnnotationRetainYearly:  "2",
		backupv1alpha1.AnnotationRetainWithin:  "3d",
	})

	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	retain := source.Spec.Restic.Retain
	if retain.Last != nil {
		t.Errorf("last = %q, want unset on a claim that names no retain-last", *retain.Last)
	}
	for field, got := range map[string]*int32{"hourly": retain.Hourly, "daily": retain.Daily, "weekly": retain.Weekly, "monthly": retain.Monthly, "yearly": retain.Yearly} {
		if got == nil {
			t.Errorf("%s is unset", field)
		}
	}
	if t.Failed() {
		return
	}
	if *retain.Hourly != 24 || *retain.Daily != 7 || *retain.Weekly != 4 || *retain.Monthly != 6 || *retain.Yearly != 2 {
		t.Errorf("retain = hourly %d daily %d weekly %d monthly %d yearly %d, want 24 7 4 6 2",
			*retain.Hourly, *retain.Daily, *retain.Weekly, *retain.Monthly, *retain.Yearly)
	}
	if retain.Within == nil || *retain.Within != "3d" {
		t.Errorf("within = %v, want 3d", retain.Within)
	}
}

// A claim with no retention would keep every snapshot forever.
func TestAClaimWithNoRetentionFails(t *testing.T) {
	c := startVolumeRun(t, map[string]string{backupv1alpha1.AnnotationEnabled: "true"})

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", run.Status.Phase)
	}
	message := run.Status.Items[0].Message
	for _, name := range []string{"retain-last", "retain-daily", "retain-within"} {
		if !strings.Contains(message, name) {
			t.Errorf("message %q does not name %s", message, name)
		}
	}
}

func TestAnUnparseableRetentionFails(t *testing.T) {
	for annotation, value := range map[string]string{
		backupv1alpha1.AnnotationRetainWeekly: "four",
		backupv1alpha1.AnnotationRetainDaily:  "0",
		backupv1alpha1.AnnotationRetainWithin: "3 days",
	} {
		t.Run(annotation, func(t *testing.T) {
			c := startVolumeRun(t, map[string]string{backupv1alpha1.AnnotationEnabled: "true", annotation: value})

			run := readBackupRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(run.Status.Items[0].Message, annotation) {
				t.Fatalf("run = %+v, want the item failed naming %s", run.Status, annotation)
			}
		})
	}
}

func TestAClaimNotMarkedEnabledIsRefused(t *testing.T) {
	unmarked := claim()
	unmarked.Annotations = nil
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }), unmarked)

	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid {
		t.Fatalf("phase = %q, reason = %q; want Failed, Invalid", run.Status.Phase, readyReason(run.Status.Conditions))
	}
}

func TestAnEmptyVolumeSucceedsWithoutASnapshot(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r)
	step(t, r)
	step(t, r)

	complete(t, c, "== Directory is empty skipping backup ===")
	step(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSucceeded || !item.Empty || item.Snapshot != "" {
		t.Fatalf("item = %+v, want Succeeded and Empty", item)
	}
}

// A source still completing another run's tag keeps it: writing a second tag
// would leave the first run waiting for a backup that is never taken.
func TestABusySourceMakesTheRunWait(t *testing.T) {
	busySource := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: claimN, Namespace: ns,
			Labels: map[string]string{backupv1alpha1.LabelManagedBy: backupv1alpha1.ManagedByValue}},
		Spec:   volsyncv1alpha1.ReplicationSourceSpec{Trigger: &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: "backuprun-other"}},
		Status: &volsyncv1alpha1.ReplicationSourceStatus{LastManualSync: "backuprun-older"},
	}
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(), busySource)
	step(t, r)
	step(t, r)
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	if manualTag(source) != "backuprun-other" {
		t.Errorf("the other run's tag was overwritten with %q", manualTag(source))
	}
}

func TestASourceSomethingElseWroteIsLeftAlone(t *testing.T) {
	foreign := &volsyncv1alpha1.ReplicationSource{ObjectMeta: metav1.ObjectMeta{Name: claimN, Namespace: ns}}
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(), foreign)
	step(t, r)
	step(t, r)
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(run.Status.Items[0].Message, "not written by backup-controller") {
		t.Fatalf("run = %+v, want the item failed naming the foreign source", run.Status)
	}
}

func TestADatabaseRunTakesABaseBackup(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), cluster())
	step(t, r)
	step(t, r)
	step(t, r)

	backup, ok := getUnstructured(t, c, BackupGVK, ns, backupName(pgN, runUID))
	if !ok {
		t.Fatal("no Backup was created")
	}
	method, _, _ := unstructured.NestedString(backup.Object, "spec", "method")
	if method != "plugin" {
		t.Errorf("method = %q, want plugin", method)
	}

	_ = unstructured.SetNestedField(backup.Object, "completed", "status", "phase")
	if err := c.Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	step(t, r)

	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", run.Status.Phase)
	}
}

func TestAHibernatedDatabaseIsSkipped(t *testing.T) {
	sleeping := cluster(func(u *unstructured.Unstructured) {
		annotations := u.GetAnnotations()
		annotations[hibernationAnnotation] = "on"
		u.SetAnnotations(annotations)
	})
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), sleeping)
	step(t, r)
	step(t, r)
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Items[0].Phase != backupv1alpha1.ItemSkipped || run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("run = %+v, want the Cluster skipped and the run Succeeded", run.Status)
	}
	if _, ok := getUnstructured(t, c, BackupGVK, ns, backupName(pgN, runUID)); ok {
		t.Error("a Backup was created for a hibernated Cluster")
	}
}

// admitAll lets Kueue admit the run's Workload.
func admitAll(t *testing.T, c client.Client) {
	t.Helper()
	workload, ok := getUnstructured(t, c, WorkloadGVK, ns, workloadName(runUID))
	if !ok {
		t.Fatal("the run created no Workload")
	}
	_ = unstructured.SetNestedSlice(workload.Object, []any{map[string]any{
		"type": "Admitted", "status": "True", "reason": "Admitted", "message": "", "lastTransitionTime": "2026-09-24T12:00:00Z",
	}}, "status", "conditions")
	if err := c.Status().Update(context.Background(), workload); err != nil {
		t.Fatal(err)
	}
}

// The whole namespace: admitted as one Workload, the app stopped while the
// clones are cut and started again before the uploads finish.
func TestANamespaceRunQuiescesAroundTheClones(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false), localQueueObject())

	step(t, r) // plan
	step(t, r) // admit: creates the Workload and waits
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Fatalf("phase = %q before admission, want Queued", run.Status.Phase)
	}
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 {
		t.Fatal("the app was stopped before the run was admitted")
	}

	admitAll(t, c)
	step(t, r) // admitted: PodsReady, Running
	workload, _ := getUnstructured(t, c, WorkloadGVK, ns, workloadName(runUID))
	if !conditionTrue(workload, "PodsReady") {
		t.Error("the Workload was not marked PodsReady, and waitForPodsReady would evict it")
	}

	step(t, r) // quiesce
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 0 {
		t.Fatalf("replicas = %d while the clones are cut, want 0", *d.Spec.Replicas)
	}
	k, _ := getUnstructured(t, c, KustomizationGVK, "flux-system", appN)
	if suspended, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend"); !suspended {
		t.Error("the Kustomization was not suspended, and Flux would put the replicas back")
	}

	step(t, r) // start: the source is triggered and the base backup requested
	if run := readBackupRun(t, c); run.Status.RestartedAt != nil {
		t.Fatal("the app was restarted before its clone was cut")
	}

	// VolSync cuts the clone.
	clone := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "volsync-" + claimN + "-src", Namespace: ns},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	if err := c.Create(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
	step(t, r)

	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 {
		t.Fatalf("replicas = %d once the clone is cut, want 2 back", *d.Spec.Replicas)
	}
	k, _ = getUnstructured(t, c, KustomizationGVK, "flux-system", appN)
	if suspended, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend"); suspended {
		t.Error("the Kustomization the run suspended was not resumed")
	}

	complete(t, c, "snapshot 6e473100 saved")
	backup, _ := getUnstructured(t, c, BackupGVK, ns, backupName(pgN, runUID))
	_ = unstructured.SetNestedField(backup.Object, "completed", "status", "phase")
	if err := c.Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if _, ok := getUnstructured(t, c, WorkloadGVK, ns, workloadName(runUID)); ok {
		t.Error("the Workload outlived the run and holds its queue slot")
	}
}

// quiescedRunToUpload drives a namespace run with the app marked for quiesce
// through the clone and the restart, and completes the upload and the base
// backup. The next step collects the results.
func quiescedRunToUpload(t *testing.T) (*BackupRunReconciler, client.Client) {
	t.Helper()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false))
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start

	clone := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "volsync-" + claimN + "-src", Namespace: ns},
		Status:     corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	if err := c.Create(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
	step(t, r) // restart
	if run := readBackupRun(t, c); run.Status.RestartedAt == nil {
		t.Fatal("the app was not restarted once the clone was cut")
	}

	complete(t, c, "snapshot 6e473100 saved")
	backup, _ := getUnstructured(t, c, BackupGVK, ns, backupName(pgN, runUID))
	_ = unstructured.SetNestedField(backup.Object, "completed", "status", "phase")
	if err := c.Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	return r, c
}

// A run that stopped the app moves the volume's snapshot to the moment it
// started the app again. Nothing wrote the volume or the database between the
// last pod stopping and that moment, so a restore can recover the database to
// the snapshot's time and the two agree.
func TestAQuiescedRunMovesTheSnapshotToItsRestartMoment(t *testing.T) {
	r, c := quiescedRunToUpload(t)
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	calls := r.Retimer.(*retimer).calls
	if len(calls) != 1 || calls[0].short != "6e473100" || !calls[0].at.Equal(run.Status.RestartedAt.Time) || calls[0].tag != "quiesced" {
		t.Fatalf("retime calls = %+v, want snapshot 6e473100 moved to restartedAt %s and tagged quiesced", calls, run.Status.RestartedAt)
	}
	item := run.Status.Items[0]
	if item.Snapshot != "c0ffee00" || item.SnapshotTime == nil || !item.SnapshotTime.Equal(run.Status.RestartedAt) {
		t.Errorf("item = %+v, want the rewritten snapshot c0ffee00 at restartedAt %s", item, run.Status.RestartedAt)
	}
}

// The rewrite needs restic's exclusive lock. While another process holds a
// lock the volume's item waits, and the run finishes once the rewrite goes
// through, so a run never reports success for a snapshot left untagged.
func TestAQuiescedRunWaitsForTheRepositoryLock(t *testing.T) {
	r, c := quiescedRunToUpload(t)
	fake := r.Retimer.(*retimer)
	fake.err = &restic.LockedError{Hostname: "volsync-dst-restore-1a2b", Time: frozen.Add(-time.Minute)}
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase.Finished() {
		t.Fatalf("phase = %q while the repository was locked, want the run still going", run.Status.Phase)
	}
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning || !strings.Contains(item.Message, "volsync-dst-restore-1a2b") {
		t.Errorf("item = %+v, want it Running with a message naming the lock's host", item)
	}

	fake.err = nil
	step(t, r)
	run = readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || run.Status.Items[0].Snapshot != "c0ffee00" {
		t.Errorf("phase = %q, item = %+v; want Succeeded with the rewritten snapshot", run.Status.Phase, run.Status.Items[0])
	}
}

// A Kustomization someone else suspended stays suspended after the run.
func TestAKustomizationAlreadySuspendedIsNotResumed(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(true))
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce

	run := readBackupRun(t, c)
	if len(run.Status.SuspendedKustomizations) != 0 {
		t.Fatalf("suspended = %v; the run did not suspend it, so it must not resume it", run.Status.SuspendedKustomizations)
	}
}

// A run that times out with the app stopped starts it again before it fails.
func TestATimedOutRunRestartsTheApp(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	step(t, r)
	step(t, r)
	step(t, r) // quiesce
	step(t, r) // start

	r.Now = func() time.Time { return frozen.Add(2 * time.Hour) }
	step(t, r)

	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 {
		t.Fatalf("replicas = %d after the timeout, want 2 back", *d.Spec.Replicas)
	}
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", run.Status.Phase)
	}
}
