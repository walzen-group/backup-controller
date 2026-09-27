package runs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// annotatedNamespace returns the test namespace with the given annotations.
func annotatedNamespace(annotations map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Annotations: annotations}}
}

// startedVolumeRun creates a BackupRun of the test claim and reconciles it
// until it has started. The fake client holds the given Namespace object. The
// claim's source never finishes, so the run stays Running until its timeout.
// The timeout argument becomes the run's spec.timeout, and nil leaves it
// unset.
func startedVolumeRun(t *testing.T, timeout *metav1.Duration, namespace *corev1.Namespace) *BackupRunReconciler {
	t.Helper()
	r, _ := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) {
		b.Spec.Source = claimN
		b.Spec.Timeout = timeout
	}), namespace, claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit
	step(t, r) // start
	return r
}

// phaseAt sets the clock to the given duration after frozen, reconciles the
// run once, and returns the run's phase.
func phaseAt(t *testing.T, r *BackupRunReconciler, after time.Duration) backupv1alpha1.RunPhase {
	t.Helper()
	r.Now = func() time.Time { return frozen.Add(after) }
	step(t, r)
	return readBackupRun(t, r.Client).Status.Phase
}

// TestARunWithoutATimeoutGivesUpAfterSixHours checks that a run with no
// timeout of its own, in a namespace without one, fails at six hours.
func TestARunWithoutATimeoutGivesUpAfterSixHours(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(nil))

	if phase := phaseAt(t, r, 6*time.Hour-time.Minute); phase != backupv1alpha1.RunPhaseRunning {
		t.Fatalf("phase = %q a minute before six hours, want Running", phase)
	}
	if phase := phaseAt(t, r, 6*time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at six hours, want Failed", phase)
	}
}

// TestANamespaceTimeoutReplacesTheDefault checks that a namespace's
// backup.wlz.li/timeout of 10h keeps a run going past six hours and fails it
// at ten.
func TestANamespaceTimeoutReplacesTheDefault(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "10h"}))

	if phase := phaseAt(t, r, 6*time.Hour); phase != backupv1alpha1.RunPhaseRunning {
		t.Fatalf("phase = %q at six hours, want Running under the namespace's ten", phase)
	}
	if phase := phaseAt(t, r, 10*time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at ten hours, want Failed", phase)
	}
}

// TestEmptyNamespaceSettingsFallBackToTheDefaults checks that an empty
// timeout or prune-interval annotation counts as no annotation. A Flux
// component can then write the key from a substitution that defaults to "",
// and the controller's default applies.
func TestEmptyNamespaceSettingsFallBackToTheDefaults(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(map[string]string{
		backupv1alpha1.AnnotationTimeout:           "",
		backupv1alpha1.AnnotationPruneIntervalDays: "",
	}))

	source := &volsyncv1alpha1.ReplicationSource{}
	get(t, r.Client, ns, claimN, source)
	if got := source.Spec.Restic.PruneIntervalDays; got == nil || *got != 1 {
		t.Errorf("pruneIntervalDays = %v, want 1", got)
	}
	if phase := phaseAt(t, r, 6*time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q at six hours, want Failed on the default", phase)
	}
}

// TestARunsOwnTimeoutWinsOverTheNamespace checks that a run's spec.timeout of
// one hour takes precedence over the namespace's 10h.
func TestARunsOwnTimeoutWinsOverTheNamespace(t *testing.T) {
	r := startedVolumeRun(t, &metav1.Duration{Duration: time.Hour},
		annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "10h"}))

	if phase := phaseAt(t, r, time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at one hour, want Failed on the run's own timeout", phase)
	}
}

// TestANamespaceTimeoutThatDoesNotParseFailsTheRun checks that a namespace
// timeout that isn't a Go duration fails the run, with a message that names
// the annotation.
func TestANamespaceTimeoutThatDoesNotParseFailsTheRun(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "six hours"}))

	run := readBackupRun(t, r.Client)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", run.Status.Phase)
	}
	message := run.Status.Conditions[0].Message
	if !strings.Contains(message, backupv1alpha1.AnnotationTimeout) {
		t.Errorf("message %q does not name %s", message, backupv1alpha1.AnnotationTimeout)
	}
}

// TestAScheduledRunLeavesTheTimeoutToItsNamespace checks that the scheduler
// creates runs without a spec.timeout. The namespace's timeout then applies,
// so a manual run and a scheduled one in the same namespace give up at the
// same point.
func TestAScheduledRunLeavesTheTimeoutToItsNamespace(t *testing.T) {
	created := time.Date(2026, 9, 24, 4, 0, 0, 0, time.UTC)
	_, c := schedule(t, time.Date(2026, 9, 24, 5, 0, 30, 0, time.UTC), scheduledNamespace("0 5 * * *", created), claim())

	runs := scheduledRuns(t, c)
	if len(runs) != 1 || runs[0].Spec.Timeout != nil {
		t.Fatalf("runs = %v, want one with no timeout of its own", runs)
	}
}

// TestTheNamespacePruneIntervalReachesTheSource checks that the
// ReplicationSource's pruneIntervalDays is 1 without an annotation, and the
// annotation's value when the namespace sets one.
func TestTheNamespacePruneIntervalReachesTheSource(t *testing.T) {
	for name, tc := range map[string]struct {
		annotations map[string]string
		want        int32
	}{
		"default":   {nil, 1},
		"annotated": {map[string]string{backupv1alpha1.AnnotationPruneIntervalDays: "14"}, 14},
	} {
		t.Run(name, func(t *testing.T) {
			r := startedVolumeRun(t, nil, annotatedNamespace(tc.annotations))

			source := &volsyncv1alpha1.ReplicationSource{}
			get(t, r.Client, ns, claimN, source)
			if got := source.Spec.Restic.PruneIntervalDays; got == nil || *got != tc.want {
				t.Fatalf("pruneIntervalDays = %v, want %d", got, tc.want)
			}
		})
	}
}

// TestAPruneIntervalThatDoesNotParseFailsTheItem checks that a prune interval
// of 0 fails the claim's item, with a message that names the annotation.
func TestAPruneIntervalThatDoesNotParseFailsTheItem(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(map[string]string{backupv1alpha1.AnnotationPruneIntervalDays: "0"}))

	run := readBackupRun(t, r.Client)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(run.Status.Items[0].Message, backupv1alpha1.AnnotationPruneIntervalDays) {
		t.Fatalf("run = %+v, want the item failed naming %s", run.Status, backupv1alpha1.AnnotationPruneIntervalDays)
	}
}

// A BackupRun that times out while it waits records its ending in the
// status write that fails its items, and a later pass ends with that
// ending as recorded: the restart that fails in between replaces the
// SourceBusy condition with RestartFailed, and the items' messages are
// edited before the retry, yet the run still ends with the wait it timed
// out on. The timed-out items record reason TimedOut, and a Pending item
// whose start failed keeps its last error in its own message only.
func TestATimedOutRunKeepsItsEndingAcrossAFailedRestart(t *testing.T) {
	wait := "RestoreRun back-to-monday is restoring claim " + claimN + "; this run starts once that restore has finished"
	c := newClient(t, quiescedBackup(func(b *backupv1alpha1.BackupRun) {
		b.Finalizers = []string{Finalizer}
		b.Status.StartedAt = atFrozen(0)
		b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		b.Status.Items = append(b.Status.Items, backupv1alpha1.BackupItem{Kind: backupv1alpha1.ItemKindCluster, Name: pgN,
			Phase: backupv1alpha1.ItemPending, LastStartError: "the webhook refused the Backup",
			Message: "not started yet: the webhook refused the Backup"})
		backupv1alpha1.SetReady(&b.Status.Conditions, b.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonSourceBusy, wait)
	}), claim(), volume(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true))
	refused := true
	refusing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := deploymentScale(sub, obj, opts); ok && refused {
				return apierrors.NewInternalError(errors.New("the API server cannot scale the Deployment"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	br := &BackupRunReconciler{Client: refusing, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{},
		Now: func() time.Time { return frozen.Add(time.Hour + time.Second) }}

	if err := tryStep(br); err == nil {
		t.Fatal("the pass whose restart failed returned no error")
	}
	want := "the run had not finished by " + frozen.Add(time.Hour).Format(time.RFC3339) + "; it was waiting: " + wait
	stored := readBackupRun(t, c)
	if got := stored.Status.Ending; got == nil || *got != (backupv1alpha1.RunEnding{Reason: backupv1alpha1.ReasonFailed, Message: want}) {
		t.Fatalf("ending = %+v after the failed restart, want reason Failed and %q", got, want)
	}
	for _, item := range stored.Status.Items {
		if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut {
			t.Errorf("item %s = %+v, want it Failed with reason TimedOut", item.Name, item)
		}
	}
	if got := stored.Status.Items[1].Message; got != want+"; last error: the webhook refused the Backup" {
		t.Errorf("the Cluster item's message = %q, want the end with its last error", got)
	}

	for i := range stored.Status.Items {
		stored.Status.Items[i].Message = "edited between the passes"
	}
	if err := c.Status().Update(context.Background(), stored); err != nil {
		t.Fatal(err)
	}
	refused = false
	step(t, br)

	run := readBackupRun(t, c)
	if message := readyMessage(run.Status.Conditions); run.Status.Phase != backupv1alpha1.RunPhaseFailed || message != want {
		t.Fatalf("phase = %q, message = %q; want Failed with %q", run.Status.Phase, message, want)
	}
}
