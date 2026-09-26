package runs

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// backupRun returns the BackupRun before-upgrade with a one-hour timeout,
// after applying each of the mutate functions to it.
func backupRun(mutate ...func(*backupv1alpha1.BackupRun)) *backupv1alpha1.BackupRun {
	run := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "before-upgrade", Namespace: ns, UID: runUID, Generation: 1},
		Spec:       backupv1alpha1.BackupRunSpec{Timeout: &metav1.Duration{Duration: time.Hour}},
		Status:     backupv1alpha1.BackupRunStatus{PlannedBy: runFormat},
	}
	for _, m := range mutate {
		m(run)
	}
	return run
}

// backupReconciler returns a BackupRunReconciler over a fake client that
// holds the given objects, and the client itself. The reconciler runs on the
// frozen clock, its lister holds sunday's and monday's snapshots, and its
// Retimer is a fake that records each call.
func backupReconciler(t *testing.T, objects ...client.Object) (*BackupRunReconciler, client.Client) {
	t.Helper()
	c := newClient(t, objects...)
	return &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: func() time.Time { return frozen }}, c
}

// step reconciles the BackupRun before-upgrade once and returns the result.
// An error from the reconcile fails the test.
func step(t *testing.T, r *BackupRunReconciler) ctrl.Result {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	return result
}

// readBackupRun reads the BackupRun before-upgrade back from the client.
func readBackupRun(t *testing.T, c client.Client) *backupv1alpha1.BackupRun {
	t.Helper()
	run := &backupv1alpha1.BackupRun{}
	get(t, c, ns, "before-upgrade", run)
	return run
}

// complete stands in for VolSync finishing a backup. It marks the claim's
// ReplicationSource as having completed its current trigger tag, with a
// successful mover that printed the given logs.
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

// A volume run writes the claim's ReplicationSource itself, with the
// VolumeRestore's repository, the claim's retention and a mover pinned to the
// volume's node. Once the mover finishes, the run reports the snapshot and the
// time restic stamped on it.
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

	// The spent tag stays on the source, because VolSync syncs a source with
	// no trigger at all in a loop.
	get(t, c, ns, claimN, source)
	if manualTag(source) == "" {
		t.Error("the tag was cleared, which leaves VolSync syncing continuously")
	}
}

// startVolumeRun gives the claim the given annotations, runs a volume run on
// it up to the point where the run writes the ReplicationSource, and returns
// the client.
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

// A claim that keeps snapshots by age, through the retain-hourly to
// retain-yearly and retain-within annotations, gets those values in its
// source's retain block, and no retain-last.
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

// A claim with no retention annotation fails its item, because its repository
// would keep every snapshot forever. The message names the annotations that
// would fix it.
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

// A retention annotation that does not parse, or asks to keep zero
// snapshots, fails the item with a message naming the annotation.
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

// A run that names a claim not marked backup.wlz.li/enabled fails at once
// with reason Invalid.
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

// A volume with no files, which VolSync skips without taking a snapshot,
// succeeds with Empty set and no snapshot ID.
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

// A run waits with reason SourceBusy while the claim's source is still
// completing the tag of another run that waits for it, and leaves that tag in
// place. The message names that run. Writing a second tag would leave the
// first run waiting for a backup that is never taken.
func TestABusySourceMakesTheRunWait(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(), busySource(TriggerFor(otherRunUID)), otherRun())
	step(t, r)
	step(t, r)
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if msg := readyMessage(run.Status.Conditions); !strings.Contains(msg, "manual-notes") {
		t.Errorf("message = %q, want it to name the BackupRun manual-notes", msg)
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	if manualTag(source) != TriggerFor(otherRunUID) {
		t.Errorf("the other run's tag was overwritten with %q", manualTag(source))
	}
}

// busySource returns the claim's ReplicationSource with the manual tag open
// and VolSync retrying its sync after a failed mover.
func busySource(tag string) *volsyncv1alpha1.ReplicationSource {
	source := idleSource()
	source.Spec.Trigger.Manual = tag
	at := metav1.NewTime(frozen.Add(-time.Hour))
	source.Status.LastSyncStartTime = &at
	source.Status.LatestMoverStatus = &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed,
		Logs: "Fatal: unable to open config file: Stat: The Access Key Id you provided does not exist in our records."}
	return source
}

// deadTag is a source's open tag that belongs to no run waiting for it, with
// the objects that make it so and the name the item's message must give.
type deadTag struct {
	tag     string
	objects []client.Object
	names   string
}

// deadTags are the ways a source's open tag can belong to no run that waits
// for it: a finished run, a run that no longer exists (as for a tag written
// by v0.7.x or v0.8.x whose run is gone), a run being deleted, and an
// unfinished run whose own item for the claim already failed.
func deadTags() map[string]deadTag {
	finished := otherRun()
	finished.Status.Phase = backupv1alpha1.RunPhaseFailed
	deleting := otherRun()
	deleting.DeletionTimestamp = &metav1.Time{Time: frozen}
	deleting.Finalizers = []string{Finalizer}
	itemFailed := otherRun()
	itemFailed.Status.Items[0].Phase = backupv1alpha1.ItemFailed
	gone := TriggerFor("0d1e2f3a-0000-4000-8000-000000000009")
	return map[string]deadTag{
		"a finished run":                      {TriggerFor(otherRunUID), []client.Object{finished}, "manual-notes"},
		"no run":                              {gone, nil, gone},
		"a run being deleted":                 {TriggerFor(otherRunUID), []client.Object{deleting}, "manual-notes"},
		"a running run whose item has failed": {TriggerFor(otherRunUID), []client.Object{itemFailed}, "manual-notes"},
	}
}

// A source whose open tag no run waits for fails the run's item at once,
// with a message that names the tag's run and says what a person can do. The
// source is left alone: VolSync is still retrying that sync with the clone it
// cut for it, and a new tag would be completed by that older backup.
func TestADeadTriggerFailsTheItemAtOnce(t *testing.T) {
	for name, tc := range deadTags() {
		t.Run(name, func(t *testing.T) {
			objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository(), busySource(tc.tag)}, tc.objects...)
			r, c := backupReconciler(t, objects...)
			r.Client = neverWriteSyncing(t, c)
			step(t, r) // plan
			step(t, r) // admit, no queue
			step(t, r) // start

			run := readBackupRun(t, c)
			item := run.Status.Items[0]
			if item.Phase != backupv1alpha1.ItemFailed {
				t.Fatalf("item = %+v, run reason %q; want the item Failed", item, readyReason(run.Status.Conditions))
			}
			for _, want := range []string{tc.names, "no run waits for", "Access Key Id", "delete the ReplicationSource " + claimN, "volsync-src-" + claimN,
				"the next backup unlocks the repository first"} {
				if !strings.Contains(item.Message, want) {
					t.Errorf("item message %q does not say %q", item.Message, want)
				}
			}
			if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
				t.Errorf("phase = %q, want the run Failed with its only item", run.Status.Phase)
			}
			source := &volsyncv1alpha1.ReplicationSource{}
			get(t, c, ns, claimN, source)
			if manualTag(source) != tc.tag {
				t.Errorf("source trigger = %q, want %q left in place", manualTag(source), tc.tag)
			}
		})
	}
}

// In a namespace run, a source busy with a tag no run waits for fails its
// item before the workloads stop, and the rest of the namespace is backed up.
// Waiting would keep every item of the namespace from being backed up until
// the run's timeout.
func TestADeadTriggerFailsOnlyItsItem(t *testing.T) {
	for name, tc := range deadTags() {
		t.Run(name, func(t *testing.T) {
			objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), busySource(tc.tag)}, tc.objects...)
			r, c := backupReconciler(t, objects...)
			r.Client = neverWriteSyncing(t, c)
			step(t, r) // plan
			step(t, r) // admit, no queue

			step(t, r) // quiesce
			run := readBackupRun(t, c)
			if run.Status.QuiescedAt == nil {
				t.Fatalf("the run did not quiesce: phase %q, reason %q: %s", run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
			}
			for _, item := range run.Status.Items {
				if item.Kind == "ReplicationSource" && (item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, tc.names)) {
					t.Fatalf("volume item = %+v, want it Failed naming %q before the workloads stop", item, tc.names)
				}
			}

			step(t, r) // start: the base backup is requested, the app starts again
			step(t, r)
			run = readBackupRun(t, c)
			for _, item := range run.Status.Items {
				if item.Kind == "Cluster" && item.Phase != backupv1alpha1.ItemRunning {
					t.Errorf("database item = %+v, want it Running", item)
				}
			}
			d := &appsv1.Deployment{}
			get(t, c, ns, appN, d)
			if *d.Spec.Replicas != 2 {
				t.Errorf("replicas = %d, want the app back at 2", *d.Spec.Replicas)
			}
		})
	}
}

// cacheN is a second claim of the namespace, with its own volume,
// VolumeRestore and repository.
const cacheN = "notes-cache"

// cacheClaim returns the claim cacheN and the objects it needs to be backed
// up, built like claim, volume, volumeRestore and repository.
func cacheClaim() []client.Object {
	pvc := claim()
	pvc.Name, pvc.UID, pvc.Spec.VolumeName, pvc.Spec.DataSourceRef.Name = cacheN, "cache-claim-uid", "pvc-"+cacheN, cacheN
	pv := volume()
	pv.Name = "pvc-" + cacheN
	vr := volumeRestore()
	vr.Name, vr.Spec.Repository = cacheN, "notes-restic-cache"
	secret := repository()
	secret.Name = "notes-restic-cache"
	return []client.Object{pvc, pv, vr, secret}
}

// In a namespace run with two claims, a claim whose source is busy with a
// tag no run waits for fails its item, and the other claim is backed up as
// usual: its source gets the run's trigger, and the run records its snapshot.
func TestADeadTriggerOnOneClaimLeavesTheOtherBackedUp(t *testing.T) {
	dead := busySource(TriggerFor("0d1e2f3a-0000-4000-8000-000000000009"))
	dead.Name, dead.Spec.SourcePVC = cacheN, cacheN
	objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), dead}, cacheClaim()...)
	r, c := backupReconciler(t, objects...)
	r.Client = neverWriteSyncing(t, c)
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start
	cutClone(t, c)
	step(t, r) // restart
	complete(t, c, "snapshot 6e473100 saved")
	step(t, r)

	run := readBackupRun(t, c)
	byName := map[string]backupv1alpha1.BackupItem{}
	for _, item := range run.Status.Items {
		byName[item.Name] = item
	}
	if item := byName[cacheN]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "no run waits for") {
		t.Errorf("dead claim's item = %+v, want it Failed as abandoned", item)
	}
	if item := byName[claimN]; item.Phase != backupv1alpha1.ItemSucceeded || item.Snapshot == "" {
		t.Errorf("healthy claim's item = %+v, want it Succeeded with a snapshot", item)
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, cacheN, source)
	if manualTag(source) != manualTag(dead) || source.Spec.Restic != nil && source.Spec.Restic.Unlock != "" {
		t.Errorf("dead source = %+v, want it left as it was", source.Spec)
	}
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 {
		t.Errorf("replicas = %d, want the app back at 2", *d.Spec.Replicas)
	}
}

// A ReplicationSource of the claim's name that the controller did not write
// is left alone, and the item fails saying so.
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

// otherRunUID is the UID of the BackupRun otherRun, whose tag races this
// package's run for the claim's source.
const otherRunUID = types.UID("3f2a1c7e-0000-4000-8000-000000000002")

// otherRun returns a second BackupRun of the claim, Running, with its item
// for the claim Running under its own tag. It is the run a busy source waits
// for.
func otherRun() *backupv1alpha1.BackupRun {
	return &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "manual-notes", Namespace: ns, UID: otherRunUID, Generation: 1},
		Spec:       backupv1alpha1.BackupRunSpec{Source: claimN, Timeout: &metav1.Duration{Duration: time.Hour}},
		Status: backupv1alpha1.BackupRunStatus{
			Phase: backupv1alpha1.RunPhaseRunning, PlannedBy: runFormat,
			Items: []backupv1alpha1.BackupItem{{Kind: "ReplicationSource", Name: claimN,
				Phase: backupv1alpha1.ItemRunning, Trigger: TriggerFor(otherRunUID)}},
		},
	}
}

// idleSource returns the claim's ReplicationSource as the controller wrote
// it for an earlier run whose tag VolSync has completed.
func idleSource() *volsyncv1alpha1.ReplicationSource {
	return &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: claimN, Namespace: ns,
			Labels: map[string]string{backupv1alpha1.LabelManagedBy: backupv1alpha1.ManagedByValue}},
		Spec:   volsyncv1alpha1.ReplicationSourceSpec{SourcePVC: claimN, Trigger: &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: "backuprun-older"}},
		Status: &volsyncv1alpha1.ReplicationSourceStatus{LastManualSync: "backuprun-older"},
	}
}

// writeOtherTag stands in for the run otherRun writing its trigger onto the
// claim's source, and VolSync starting that sync.
func writeOtherTag(ctx context.Context, cl client.Client) error {
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: claimN}, source); err != nil {
		return err
	}
	source.Spec.Trigger = &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: TriggerFor(otherRunUID)}
	if err := cl.Update(ctx, source); err != nil {
		return err
	}
	at := metav1.NewTime(frozen)
	source.Status = &volsyncv1alpha1.ReplicationSourceStatus{LastManualSync: "backuprun-older", LastSyncStartTime: &at}
	return cl.Status().Update(ctx, source)
}

// Two runs of the same claim never overwrite each other's trigger. The other
// run writes its tag onto the idle source in the middle of this run's
// ensureSource: after a read that found the source idle, or after the read
// the write is based on. Either way this run leaves the other run's tag in
// place and waits with reason SourceBusy. An overwritten tag would be
// completed by the other run's sync, with a clone cut before this run's
// quiesce.
func TestTwoRunsDoNotOverwriteEachOthersTrigger(t *testing.T) {
	for name, race := range map[string]interceptor.Funcs{
		// The other run writes just before this run's client reads the
		// source for its write, after any check through the Reader.
		"before the read the write is based on": {
			Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*volsyncv1alpha1.ReplicationSource); ok && key.Name == claimN {
					if err := writeOtherTag(ctx, cl); err != nil {
						return err
					}
				}
				return cl.Get(ctx, key, obj, opts...)
			},
		},
		// The other run writes after this run's client read the source and
		// just before its write reaches the API server.
		"between the read and the write": {
			Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
				if _, ok := obj.(*volsyncv1alpha1.ReplicationSource); ok && obj.GetName() == claimN {
					if err := writeOtherTag(ctx, cl); err != nil {
						return err
					}
				}
				return cl.Update(ctx, obj, opts...)
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository(), idleSource(), otherRun())
			step(t, r) // plan
			step(t, r) // admit, no queue
			raced := false
			once := interceptor.Funcs{
				Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if race.Get != nil && !raced {
						if _, ok := obj.(*volsyncv1alpha1.ReplicationSource); ok {
							raced = true
							return race.Get(ctx, cl, key, obj, opts...)
						}
					}
					return cl.Get(ctx, key, obj, opts...)
				},
				Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
					if race.Update != nil && !raced {
						if _, ok := obj.(*volsyncv1alpha1.ReplicationSource); ok {
							raced = true
							return race.Update(ctx, cl, obj, opts...)
						}
					}
					return cl.Update(ctx, obj, opts...)
				},
			}
			r.Client = interceptor.NewClient(c.(client.WithWatch), once)

			// The pass that loses the race may return the conflict for a
			// retry; the next pass decides again from the stored source.
			_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}})
			if !raced {
				t.Fatal("the race never ran; the run did not write the source")
			}
			step(t, r)

			source := &volsyncv1alpha1.ReplicationSource{}
			get(t, c, ns, claimN, source)
			if got := manualTag(source); got != TriggerFor(otherRunUID) {
				t.Fatalf("source trigger = %q, want the other run's %q left in place", got, TriggerFor(otherRunUID))
			}
			run := readBackupRun(t, c)
			if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
				t.Errorf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
			}
			if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemPending {
				t.Errorf("item = %+v, want it Pending", item)
			}
		})
	}
}

// neverWriteSyncing returns a client over c that fails the test on any
// update, patch or delete of a ReplicationSource whose stored
// status.lastSyncStartTime is set. VolSync is syncing such a source: a new
// tag would be completed by the running sync, and any change to the mover's
// spec makes VolSync replace the running mover Job, which kills restic and
// leaves its lock in the repository.
func neverWriteSyncing(t *testing.T, c client.Client) client.Client {
	check := func(ctx context.Context, cl client.Client, verb string, obj client.Object) {
		if _, ok := obj.(*volsyncv1alpha1.ReplicationSource); !ok {
			return
		}
		stored := &volsyncv1alpha1.ReplicationSource{}
		if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
			return
		}
		if stored.Status != nil && stored.Status.LastSyncStartTime != nil {
			t.Errorf("%s of ReplicationSource %s while VolSync syncs tag %q", verb, obj.GetName(), manualTag(stored))
		}
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			check(ctx, cl, "update", obj)
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			check(ctx, cl, "patch", obj)
			return cl.Patch(ctx, obj, patch, opts...)
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			check(ctx, cl, "delete", obj)
			return cl.Delete(ctx, obj, opts...)
		},
	})
}

// The controller never writes a ReplicationSource that VolSync is syncing,
// whoever's tag it syncs. With this run's own tag on it (a pass whose status
// write was lost after the source write), the item goes Running and the
// source keeps its spec, even when the claim's retention changed since. With
// another live run's tag, the run waits with SourceBusy.
func TestTheControllerNeverWritesASourceVolSyncIsSyncing(t *testing.T) {
	syncing := func(tag string) *volsyncv1alpha1.ReplicationSource {
		source := idleSource()
		source.Spec.Trigger.Manual = tag
		// The claim says retain-last 10; the source still has the 5 the
		// claim said when the source was written.
		five := "5"
		source.Spec.Restic = &volsyncv1alpha1.ReplicationSourceResticSpec{Repository: repoN,
			Retain: &volsyncv1alpha1.ResticRetainPolicy{Last: &five}}
		at := metav1.NewTime(frozen)
		source.Status.LastSyncStartTime = &at
		return source
	}
	cases := map[string]struct {
		source *volsyncv1alpha1.ReplicationSource
		check  func(t *testing.T, run *backupv1alpha1.BackupRun)
	}{
		"this run's tag": {
			source: syncing(TriggerFor(runUID)),
			check: func(t *testing.T, run *backupv1alpha1.BackupRun) {
				if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Trigger != TriggerFor(runUID) {
					t.Errorf("item = %+v, want it Running under the run's tag", item)
				}
			},
		},
		"another live run's tag": {
			source: syncing(TriggerFor(otherRunUID)),
			check: func(t *testing.T, run *backupv1alpha1.BackupRun) {
				if run.Status.Phase != backupv1alpha1.RunPhaseWaiting || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
					t.Errorf("phase = %q, reason = %q; want Waiting, SourceBusy", run.Status.Phase, readyReason(run.Status.Conditions))
				}
			},
		},
		// No BackupRun of the namespace has this UID, so no run waits for
		// the tag.
		"a dead tag": {
			source: syncing(TriggerFor("0d1e2f3a-0000-4000-8000-000000000009")),
			check: func(t *testing.T, run *backupv1alpha1.BackupRun) {
				if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "no run waits for") {
					t.Errorf("item = %+v, want it Failed as abandoned", item)
				}
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository(), tc.source, otherRun())
			r.Client = neverWriteSyncing(t, c)
			step(t, r) // plan
			step(t, r) // admit, no queue
			step(t, r) // start

			tc.check(t, readBackupRun(t, c))
			source := &volsyncv1alpha1.ReplicationSource{}
			get(t, c, ns, claimN, source)
			if got := manualTag(source); got != manualTag(tc.source) {
				t.Errorf("source trigger = %q, want %q", got, manualTag(tc.source))
			}
			if last := source.Spec.Restic.Retain.Last; last == nil || *last != "5" {
				t.Error("retain-last changed from 5; the syncing source's spec was rewritten")
			}
			if unlock := source.Spec.Restic.Unlock; unlock != "" {
				t.Errorf("unlock = %q on a syncing source; the new field would make VolSync replace the running mover", unlock)
			}
		})
	}
}

// A database run creates a CloudNativePG Backup with method plugin, and
// succeeds once the Backup completes.
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
	if err := c.Status().Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	step(t, r)

	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", run.Status.Phase)
	}
}

// A hibernated Cluster gets no Backup. Its item is Skipped and the run still
// succeeds.
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

// admitAll stands in for Kueue admitting the run: it sets the Admitted
// condition on the run's Workload.
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

// A namespace run waits for Kueue to admit its one Workload, then stops the
// app and suspends its Kustomization while the clones are cut. It starts the
// app again as soon as the clone exists, before the upload finishes, and
// deletes the Workload when it succeeds.
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

	cutClone(t, c) // VolSync cuts the clone.
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
	if err := c.Status().Update(context.Background(), backup); err != nil {
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
	cutClone(t, c)
	step(t, r) // restart
	if run := readBackupRun(t, c); run.Status.RestartedAt == nil {
		t.Fatal("the app was not restarted once the clone was cut")
	}

	complete(t, c, "snapshot 6e473100 saved")
	backup, _ := getUnstructured(t, c, BackupGVK, ns, backupName(pgN, runUID))
	_ = unstructured.SetNestedField(backup.Object, "completed", "status", "phase")
	if err := c.Status().Update(context.Background(), backup); err != nil {
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
// lock, the volume's item stays Running with a message naming the lock's
// host. The run finishes once the rewrite goes through, so it never reports
// success for a snapshot left untagged.
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

// A Kustomization someone else suspended is left out of the run's
// suspendedKustomizations, so the run does not resume it afterwards.
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

// A run that times out while the app is stopped starts the app again and
// fails.
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

// A database whose Backup the API server refuses on every create does not
// keep the stopped app down. CloudNativePG's webhook is unreachable, and
// under failurePolicy: Fail the API server answers each create with a 500.
// The pass goes on past the database item: once the clone is cut, the app
// gets its replicas back, and the database item stays Pending with the error
// in its message. A later pass whose create goes through starts the Backup.
func TestADatabaseThatCannotStartDoesNotHoldTheApp(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false))
	webhook := `failed calling webhook "vbackup.cnpg.io": failed to call webhook: Post "https://cnpg-webhook-service.cnpg-system.svc:443/validate-postgresql-cnpg-io-v1-backup": dial tcp 10.96.12.7:443: connect: connection refused`
	healthy := r.Client
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == BackupGVK {
				return apierrors.NewInternalError(errors.New(webhook))
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	pass := func() {
		t.Helper()
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}}); err != nil {
			t.Fatalf("reconcile: %v", err)
		}
	}
	pass() // plan
	pass() // admit, no queue
	pass() // quiesce
	pass() // start: the source is triggered, the Backup create fails
	cutClone(t, c)
	pass()

	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 {
		t.Fatalf("replicas = %d once the clone is cut, want 2 back while the database waits", *d.Spec.Replicas)
	}
	run := readBackupRun(t, c)
	if run.Status.RestartedAt == nil {
		t.Fatal("restartedAt is unset after the app was started again")
	}
	var db backupv1alpha1.BackupItem
	for _, item := range run.Status.Items {
		if item.Kind == "Cluster" {
			db = item
		}
	}
	if db.Phase != backupv1alpha1.ItemPending || !strings.Contains(db.Message, "connection refused") {
		t.Errorf("database item = %+v, want it Pending with the webhook error in its message", db)
	}
	if reason, message := readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions); reason != backupv1alpha1.ReasonRetrying || !strings.Contains(message, pgN) {
		t.Errorf("Ready = %s: %s, want Retrying naming the Cluster %s", reason, message, pgN)
	}

	r.Client = healthy
	pass()
	if _, ok := getUnstructured(t, c, BackupGVK, ns, backupName(pgN, runUID)); !ok {
		t.Fatal("no Backup after the webhook came back")
	}
	for _, item := range readBackupRun(t, c).Status.Items {
		if item.Kind == "Cluster" && (item.Phase != backupv1alpha1.ItemRunning || item.Message != "") {
			t.Errorf("database item = %+v, want it Running with the old error cleared", item)
		}
	}
}

// cutClone stands in for VolSync cutting the clone of the claim: it creates
// the Bound claim volsync-<claim>-src, five seconds after the frozen clock's
// time, which is after the run stopped the app.
func cutClone(t *testing.T, c client.Client) {
	t.Helper()
	cloneAt(t, c, frozen.Add(5*time.Second))
}

// cloneAt creates the Bound claim volsync-<claim>-src with the given creation
// time, and returns it. The API server sets the creation time, so cloneAt
// sets the server clock to created for the create.
func cloneAt(t *testing.T, c client.Client, created time.Time) *corev1.PersistentVolumeClaim {
	t.Helper()
	defer atServerTime(t, c, created)()
	clone := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "volsync-" + claimN + "-src", Namespace: ns,
			Finalizers: []string{"kubernetes.io/pvc-protection"}},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	if err := c.Create(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
	return clone
}

// A quiesce pass whose status write is lost after the app was stopped is
// run again. The retry keeps the replica count and the Kustomization the
// first pass recorded, so the run still gives the app its 2 replicas back and
// resumes the Kustomization once the clone is cut.
func TestAQuiesceRetriedAfterALostStatusWriteStillRestartsTheApp(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	step(t, r) // plan
	step(t, r) // admit, no queue

	r.Client = loseStatusWriteAt(c, 0)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}}); err == nil {
		t.Fatal("the quiesce pass succeeded, want its lost status write returned")
	}
	step(t, r) // quiesce again

	run := readBackupRun(t, c)
	if len(run.Status.Quiesced) != 1 || run.Status.Quiesced[0].Replicas != 2 {
		t.Errorf("quiesced = %+v, want the Deployment with the 2 replicas it had", run.Status.Quiesced)
	}
	if len(run.Status.SuspendedKustomizations) != 1 {
		t.Errorf("suspended = %v, want the Kustomization the run suspended", run.Status.SuspendedKustomizations)
	}

	step(t, r) // start
	cutClone(t, c)
	step(t, r) // restart

	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d once the clone was cut, want the 2 the app had", got)
	}
	if suspended(t, c) {
		t.Error("the Kustomization the run suspended stayed suspended")
	}
}

// A restart pass whose status write is lost after the app was started again
// is run again a minute later. The retry keeps the restart moment the first
// pass chose, so the snapshot is moved to a time before the app wrote
// anything.
func TestARestartRetriedAfterALostStatusWriteKeepsItsMoment(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start
	cutClone(t, c)

	r.Client = loseStatusWriteAt(c, 2)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}}); err == nil {
		t.Fatal("the restart pass succeeded, want its lost status write returned")
	}
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d after the restart pass, want 2", got)
	}

	r.Now = func() time.Time { return frozen.Add(time.Minute) }
	step(t, r) // restart again
	complete(t, c, "snapshot 6e473100 saved")
	step(t, r) // collect

	run := readBackupRun(t, c)
	if run.Status.RestartedAt == nil || !run.Status.RestartedAt.Time.Equal(frozen) {
		t.Errorf("restartedAt = %v, want %s, the moment of the first restart", run.Status.RestartedAt, frozen)
	}
	calls := r.Retimer.(*retimer).calls
	if len(calls) != 1 || !calls[0].at.Equal(frozen) {
		t.Errorf("retime calls = %+v, want the snapshot moved to %s", calls, frozen)
	}
}

// A clock with a fraction of a second gives the run a restartedAt in whole
// seconds, the precision the status keeps. A snapshot moved in the same pass
// that starts the app carries that same whole-second time, so it matches
// restartedAt as later passes and a synced restore read it back.
func TestARestartMomentIsKeptInWholeSeconds(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	r.Now = func() time.Time { return frozen.Add(500 * time.Millisecond) }
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start

	// The mover finishes and VolSync removes the clone before the run looks
	// again, so the run starts the app and moves the snapshot in one pass.
	complete(t, c, "snapshot 6e473100 saved")
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	calls := r.Retimer.(*retimer).calls
	if len(calls) != 1 || !calls[0].at.Equal(run.Status.RestartedAt.Time) {
		t.Errorf("retime calls = %+v, want the snapshot moved to restartedAt %s", calls, run.Status.RestartedAt)
	}
	if run.Status.QuiescedAt.Time != run.Status.QuiescedAt.Truncate(time.Second) {
		t.Errorf("quiescedAt = %s, want whole seconds", run.Status.QuiescedAt.Format(time.RFC3339Nano))
	}
}

// A clone left from the previous sync does not count as this run's clone.
// VolSync marks a sync done before its cleanup deletes the clone, so the old
// clone can still be there, Bound, when the next run starts. Taking it for
// the new clone would start the app before the new clone is cut, and the
// snapshot would still be tagged quiesced.
func TestALeftoverCloneDoesNotRestartTheApp(t *testing.T) {
	for name, leave := range map[string]func(t *testing.T, c client.Client){
		"terminating": func(t *testing.T, c client.Client) {
			clone := cloneAt(t, c, frozen.Add(-time.Hour))
			if err := c.Delete(context.Background(), clone); err != nil {
				t.Fatal(err)
			}
		},
		"created before the run": func(t *testing.T, c client.Client) {
			cloneAt(t, c, frozen.Add(-time.Minute))
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
			leave(t, c)
			step(t, r) // plan
			step(t, r) // admit, no queue
			step(t, r) // quiesce
			step(t, r) // start

			if run := readBackupRun(t, c); run.Status.RestartedAt != nil {
				t.Errorf("restartedAt = %s; the app was started on the clone of the previous sync", run.Status.RestartedAt)
			}
			if got := replicasOf(t, c); got != 0 {
				t.Errorf("replicas = %d before this run's clone was cut, want 0", got)
			}
		})
	}
}

// failMover stands in for VolSync reporting a failed mover Job on the claim's
// source while it syncs the run's trigger. VolSync writes the logs into
// latestMoverStatus, deletes the Job and tries again, and it leaves
// lastManualSync alone until a Job succeeds. The sync started at the time
// given in started.
func failMover(t *testing.T, c client.Client, started time.Time) {
	t.Helper()
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	at := metav1.NewTime(started)
	source.Status = &volsyncv1alpha1.ReplicationSourceStatus{
		LastSyncStartTime: &at,
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "Fatal: unable to open config file"},
	}
	if err := c.Status().Update(context.Background(), source); err != nil {
		t.Fatalf("fail the mover: %v", err)
	}
}

// A mover that fails during the run's sync fails the item with the mover's
// logs, and the run ends Failed. The run leaves the ReplicationSource alone:
// by the time VolSync reports the failure it has already started a new mover
// Job, and deleting the source would kill that mover mid-backup and leave
// restic's lock in the repository, which stops every later forget.
func TestAFailedMoverFailsTheItem(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // start

	failMover(t, c, frozen.Add(10*time.Second))
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q (%s), want Failed", run.Status.Phase, readyReason(run.Status.Conditions))
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "unable to open config file") {
		t.Errorf("item = %+v, want it Failed with the mover's logs", item)
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: claimN}, source); err != nil {
		t.Fatalf("the source is gone (%v); deleting it kills the mover VolSync has already started again, and restic's lock stays behind", err)
	}
	if got := manualTag(source); got != run.Status.Items[0].Trigger {
		t.Errorf("source trigger = %q, want the run's %q left for VolSync to retry", got, run.Status.Items[0].Trigger)
	}
}

// recordedLockedForget returns what restic 0.18.1's forget printed on a
// repository that a killed mover left locked, as recorded in the restic
// package's killed-mover fixture. VolSync keeps every line of a failed
// mover's logs in status.latestMoverStatus.
func recordedLockedForget(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../restic/testdata/recorded/restic-0.18.1/killed-mover/verdicts.json")
	if err != nil {
		t.Fatal(err)
	}
	var verdicts struct {
		Forget struct {
			Output string `json:"output"`
		} `json:"forget"`
	}
	if err := json.Unmarshal(raw, &verdicts); err != nil {
		t.Fatal(err)
	}
	return verdicts.Forget.Output
}

// A mover that fails because the repository is locked fails the item with
// a message that names the lock and says how it is cleared: `restic unlock`
// removes a stale lock, and the backups the controller triggers run it first,
// so they clear the lock once it is older than 30 minutes.
func TestAMoverStoppedByALockSaysHowToClearIt(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // start

	logs := "snapshot 6e473100 saved\n" + recordedLockedForget(t)
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	at := metav1.NewTime(frozen.Add(10 * time.Second))
	source.Status = &volsyncv1alpha1.ReplicationSourceStatus{
		LastSyncStartTime: &at,
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: logs},
	}
	if err := c.Status().Update(context.Background(), source); err != nil {
		t.Fatal(err)
	}
	step(t, r)

	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed {
		t.Fatalf("item = %+v, want it Failed", item)
	}
	for _, want := range []string{"repository is already locked", "`restic unlock`", "stale", "30 minutes", "next backup"} {
		if !strings.Contains(item.Message, want) {
			t.Errorf("item message %q does not say %q", item.Message, want)
		}
	}
}

// A mover that fails for another reason gets no word about locks.
func TestAMoverFailureWithoutALockSaysNothingOfLocks(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r)
	step(t, r)
	step(t, r)
	failMover(t, c, frozen.Add(10*time.Second))
	step(t, r)
	if item := readBackupRun(t, c).Status.Items[0]; strings.Contains(item.Message, "unlock") {
		t.Errorf("item message %q speaks of unlocking, but nothing was locked", item.Message)
	}
}

// Every trigger the controller writes onto a new or idle source also sets
// spec.restic.unlock to the same value, so VolSync runs `restic unlock`
// before the backup and a lock a killed mover left behind is cleared once it
// is stale.
func TestASourceIsTriggeredWithAnUnlock(t *testing.T) {
	for name, existing := range map[string][]client.Object{"a new source": nil, "an idle source": {idleSource()}} {
		t.Run(name, func(t *testing.T) {
			objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository()}, existing...)
			r, c := backupReconciler(t, objects...)
			step(t, r)
			step(t, r)
			step(t, r)
			source := &volsyncv1alpha1.ReplicationSource{}
			get(t, c, ns, claimN, source)
			if manualTag(source) != TriggerFor(runUID) || source.Spec.Restic == nil || source.Spec.Restic.Unlock != manualTag(source) {
				t.Errorf("trigger = %q, restic = %+v; want unlock equal to the run's trigger", manualTag(source), source.Spec.Restic)
			}
		})
	}
}

// A Failed result from a sync that started before this run is not this
// run's, so the item keeps waiting for its own sync.
func TestAFailedResultFromAnEarlierSyncIsIgnored(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // start

	failMover(t, c, frozen.Add(-time.Hour))
	step(t, r)

	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Errorf("item = %+v, want it still Running", item)
	}
}

// An evicted pod of a stopped workload does not hold the run. The kubelet
// has killed its containers, so it writes nothing, but it stays in the API
// in phase Failed until something deletes it.
func TestAnEvictedPodDoesNotHoldTheRun(t *testing.T) {
	evicted := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-7c4d", Namespace: ns, Labels: map[string]string{"app": appN}},
		Status:     corev1.PodStatus{Phase: corev1.PodFailed, Reason: "Evicted"},
	}
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false), evicted)
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start

	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Errorf("item = %+v, want it Running once every live pod is gone", item)
	}
}

// failOnce returns a reader over c whose first call that fail picks returns
// a ServiceUnavailable error, the way the API server answers while etcd
// elects a leader. Every other call goes through.
func failOnce(c client.Client, fail func(object any) bool) client.Reader {
	failed := false
	unavailable := func(object any) error {
		if !failed && fail(object) {
			failed = true
			return apierrors.NewServiceUnavailable("etcd leader changed")
		}
		return nil
	}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := unavailable(obj); err != nil {
				return err
			}
			return cl.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := unavailable(list); err != nil {
				return err
			}
			return cl.List(ctx, list, opts...)
		},
	})
}

// A failed list while a run plans is returned for a retry, and the next pass
// plans the run. Ending the run Invalid would lose a scheduled tick to a
// moment when the API server was busy.
func TestAFailedReadWhilePlanningIsRetried(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository())
	r.Reader = failOnce(c, func(object any) bool { _, ok := object.(*corev1.PersistentVolumeClaimList); return ok })

	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}}); err == nil {
		t.Error("reconcile succeeded, want the read error returned for a retry")
	}
	if run := readBackupRun(t, c); run.Status.Phase.Finished() {
		t.Fatalf("phase = %q (%s) after a failed read, want the run still to plan", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
	step(t, r)
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Errorf("phase = %q, want Queued", run.Status.Phase)
	}
}

// A failed read while a run starts a volume's backup leaves the item Pending
// with the error in its message, and the next pass starts it.
func TestAFailedReadWhileStartingAnItemIsRetried(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit, no queue
	reader := r.Reader
	r.Reader = failOnce(c, func(object any) bool { _, ok := object.(*corev1.PersistentVolume); return ok })

	if result := step(t, r); result.RequeueAfter == 0 {
		t.Error("the pass with the failed read asked for no requeue, want the item tried again")
	}
	run := readBackupRun(t, c)
	if run.Status.Phase.Finished() || run.Status.Items[0].Phase != backupv1alpha1.ItemPending ||
		!strings.Contains(run.Status.Items[0].Message, "etcd leader changed") {
		t.Fatalf("run = %q, item = %+v after a failed read, want the item still Pending with the error in its message", run.Status.Phase, run.Status.Items[0])
	}
	if reason := readyReason(run.Status.Conditions); reason != backupv1alpha1.ReasonRetrying {
		t.Errorf("Ready reason = %s after a failed read, want Retrying", reason)
	}
	r.Reader = reader
	step(t, r)
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Message != "" {
		t.Errorf("item = %+v, want it Running with the old error cleared", item)
	}
}

// A run that times out while an item still fails to start ends with the
// item's message naming the timeout and the error of its last start attempt,
// so a person reading the failed run sees why the item never started.
func TestATimedOutRunKeepsAnItemsLastStartError(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit, no queue
	r.Reader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*corev1.PersistentVolume); ok {
				return apierrors.NewServiceUnavailable("etcd leader changed")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	step(t, r) // the start fails

	r.Now = func() time.Time { return frozen.Add(2 * time.Hour) }
	step(t, r) // past the one-hour timeout

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed after the timeout", run.Status.Phase)
	}
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "had not finished by") ||
		!strings.Contains(item.Message, "; last error: ") || !strings.Contains(item.Message, "etcd leader changed") {
		t.Errorf("item = %+v, want it Failed naming the timeout and, after \"; last error: \", the start error", item)
	}
}

// When one item fails to start and another waits for a source busy with
// another run's backup, the Ready condition names both, so neither cause is
// hidden behind the other.
func TestReadyNamesARetryAndABusySourceTogether(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), otherRun())
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce: nothing is marked, so nothing stops
	if err := c.Create(context.Background(), busySource(TriggerFor(otherRunUID))); err != nil {
		t.Fatal(err)
	}
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == BackupGVK {
				return apierrors.NewInternalError(errors.New(`failed calling webhook "vbackup.cnpg.io": connect: connection refused`))
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	step(t, r)

	run := readBackupRun(t, c)
	message := readyMessage(run.Status.Conditions)
	if !strings.Contains(message, pgN) || !strings.Contains(message, "connection refused") || !strings.Contains(message, "manual-notes") {
		t.Errorf("Ready = %s: %s, want it to name the Cluster %s with its error and the BackupRun manual-notes the claim waits for",
			readyReason(run.Status.Conditions), message, pgN)
	}
}

// A namespace run in a cluster without the CloudNativePG CRDs backs up the
// volumes. The API server answers a list of Clusters there with a no-match
// error, which means there are no Clusters.
func TestANamespaceRunWithoutCloudNativePGBacksUpTheVolumes(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository())
	r.Reader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if u, ok := list.(*unstructured.UnstructuredList); ok && u.GroupVersionKind().Group == ClusterGVK.Group {
				return &meta.NoKindMatchError{GroupKind: ClusterGVK.GroupKind(), SearchedVersions: []string{"v1"}}
			}
			return cl.List(ctx, list, opts...)
		},
	})
	step(t, r)

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseQueued || len(run.Status.Items) != 1 {
		t.Fatalf("phase = %q (%s), items = %+v; want Queued with the claim", run.Status.Phase, readyMessage(run.Status.Conditions), run.Status.Items)
	}
}

// A workload whose kustomize-controller labels name a Kustomization that does
// not list it in its inventory is scaled down with nothing suspended. The
// labels are only labels, and anyone who can edit the Deployment could point
// them at another team's Kustomization.
func TestAKustomizationThatDoesNotListTheWorkloadIsNotSuspended(t *testing.T) {
	app := deployment()
	app.Labels = map[string]string{fluxNameLabel: "billing", fluxNamespaceLabel: "billing"}
	billing := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"suspend": false},
		"status": map[string]any{"inventory": map[string]any{"entries": []any{
			map[string]any{"id": "billing_api_apps_Deployment", "v": "v1"},
		}}},
	}}
	billing.SetGroupVersionKind(KustomizationGVK)
	billing.SetNamespace("billing")
	billing.SetName("billing")
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), app, billing)
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce

	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d after the quiesce, want 0", got)
	}
	k, _ := getUnstructured(t, c, KustomizationGVK, "billing", "billing")
	if on, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend"); on {
		t.Error("the run suspended a Kustomization that does not apply the app")
	}
	if run := readBackupRun(t, c); len(run.Status.SuspendedKustomizations) != 0 {
		t.Errorf("suspended = %v, want none", run.Status.SuspendedKustomizations)
	}
}

// syncStarted stands in for VolSync starting the sync of the run's trigger
// at the given time: it records status.lastSyncStartTime on the claim's
// source, with no mover result yet.
func syncStarted(t *testing.T, c client.Client, started time.Time) {
	t.Helper()
	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, c, ns, claimN, source)
	at := metav1.NewTime(started)
	source.Status = &volsyncv1alpha1.ReplicationSourceStatus{LastSyncStartTime: &at}
	if err := c.Status().Update(context.Background(), source); err != nil {
		t.Fatalf("start the sync: %v", err)
	}
}

// An item the run fails while VolSync goes on with its sync says what data a
// snapshot of that sync holds. VolSync retries the sync with the clone it
// cut when the sync started, and restic stamps a retry's snapshot with the
// retry's own time, so that snapshot holds data older than its time.
func TestAFailedItemNamesTheDataALaterSnapshotHolds(t *testing.T) {
	started := frozen.Add(10 * time.Second)
	at := started.UTC().Format(time.RFC3339)
	cut := "a snapshot this sync saves later holds the data of " + at
	for name, tc := range map[string]struct {
		run  func(t *testing.T) (*BackupRunReconciler, client.Client)
		fail func(t *testing.T, r *BackupRunReconciler, c client.Client)
		want string
	}{
		"a failed mover": {
			run: sourceRun,
			fail: func(t *testing.T, r *BackupRunReconciler, c client.Client) {
				cloneAt(t, c, started)
				failMover(t, c, started)
				step(t, r)
			},
			want: cut,
		},
		"the run's timeout": {
			run: sourceRun,
			fail: func(t *testing.T, r *BackupRunReconciler, c client.Client) {
				cloneAt(t, c, started)
				syncStarted(t, c, started)
				r.Now = func() time.Time { return frozen.Add(time.Hour) }
				step(t, r)
			},
			want: cut,
		},
		"the quiesce limit with the clone not cut": {
			run: func(t *testing.T) (*BackupRunReconciler, client.Client) {
				return quiescedVolumeRun(t, annotatedNamespace(nil))
			},
			fail: func(t *testing.T, r *BackupRunReconciler, c client.Client) {
				syncStarted(t, c, started)
				replicasAt(t, r, c, 10*time.Minute)
			},
			want: "has not cut its clone yet, so a snapshot this sync saves later holds the data of the moment it cuts the clone",
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := tc.run(t)
			tc.fail(t, r, c)
			item := readBackupRun(t, c).Status.Items[0]
			if item.Phase != backupv1alpha1.ItemFailed {
				t.Fatalf("item = %+v, want it Failed", item)
			}
			if !strings.Contains(item.Message, tc.want) || !strings.Contains(item.Message, "started at "+at) {
				t.Errorf("item message %q does not say %q and when the sync started", item.Message, tc.want)
			}
		})
	}
}

// An item whose sync VolSync has not started says nothing of a later
// snapshot: the sync that starts later cuts a fresh clone.
func TestAFailedItemWithNoSyncSaysNothingOfALaterSnapshot(t *testing.T) {
	r, c := sourceRun(t)
	r.Now = func() time.Time { return frozen.Add(time.Hour) }
	step(t, r)
	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || strings.Contains(item.Message, "later") {
		t.Errorf("item = %+v, want it Failed with no word of a later snapshot", item)
	}
}

// sourceRun creates a BackupRun of the test claim with a one-hour timeout
// and reconciles it until the claim's source carries the run's trigger.
func sourceRun(t *testing.T) (*BackupRunReconciler, client.Client) {
	t.Helper()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // start
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("item = %+v after the start pass, want it Running", item)
	}
	return r, c
}

// A run with two items that both wait for another run names both waits on
// its Ready condition, so the first wait is not hidden by the second.
func TestReadyNamesEveryWait(t *testing.T) {
	other := otherRun()
	other.Spec.Source, other.Spec.All = "", true
	other.Status.Items = append(other.Status.Items, backupv1alpha1.BackupItem{Kind: "ReplicationSource", Name: cacheN,
		Phase: backupv1alpha1.ItemRunning, Trigger: TriggerFor(otherRunUID)})
	objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), other}, cacheClaim()...)
	r, c := backupReconciler(t, objects...)
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce: nothing is marked, and both sources are still idle

	// The other run tags both sources before this run starts its items.
	for _, name := range []string{claimN, cacheN} {
		source := busySource(TriggerFor(otherRunUID))
		source.Name, source.Spec.SourcePVC = name, name
		if err := c.Create(context.Background(), source); err != nil {
			t.Fatal(err)
		}
	}
	step(t, r) // start: both items wait

	run := readBackupRun(t, c)
	for _, item := range run.Status.Items {
		if item.Phase != backupv1alpha1.ItemPending {
			t.Fatalf("item = %+v, want both items Pending while the other run holds them", item)
		}
	}
	reason, message := readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions)
	if reason != backupv1alpha1.ReasonSourceBusy || !strings.Contains(message, claimN) || !strings.Contains(message, cacheN) {
		t.Errorf("Ready = %s: %q, want SourceBusy naming both %s and %s", reason, message, claimN, cacheN)
	}
}
