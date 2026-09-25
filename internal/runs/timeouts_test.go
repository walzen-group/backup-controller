package runs

import (
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func annotatedNamespace(annotations map[string]string) *corev1.Namespace {
	return &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns, Annotations: annotations}}
}

// startedVolumeRun reconciles a volume run up to its start, with the source
// left unfinished, and returns the reconciler.
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

// phaseAt reconciles the run once at a moment after its start and returns its
// phase.
func phaseAt(t *testing.T, r *BackupRunReconciler, after time.Duration) backupv1alpha1.RunPhase {
	t.Helper()
	r.Now = func() time.Time { return frozen.Add(after) }
	step(t, r)
	return readBackupRun(t, r.Client).Status.Phase
}

func TestARunWithoutATimeoutGivesUpAfterSixHours(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(nil))

	if phase := phaseAt(t, r, 6*time.Hour-time.Minute); phase != backupv1alpha1.RunPhaseRunning {
		t.Fatalf("phase = %q a minute before six hours, want Running", phase)
	}
	if phase := phaseAt(t, r, 6*time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at six hours, want Failed", phase)
	}
}

func TestANamespaceTimeoutReplacesTheDefault(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "10h"}))

	if phase := phaseAt(t, r, 6*time.Hour); phase != backupv1alpha1.RunPhaseRunning {
		t.Fatalf("phase = %q at six hours, want Running under the namespace's ten", phase)
	}
	if phase := phaseAt(t, r, 10*time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at ten hours, want Failed", phase)
	}
}

// An empty annotation reads as none, so a Flux component can write the key
// from a substitution that defaults to "" and leave the default here.
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

func TestARunsOwnTimeoutWinsOverTheNamespace(t *testing.T) {
	r := startedVolumeRun(t, &metav1.Duration{Duration: time.Hour},
		annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "10h"}))

	if phase := phaseAt(t, r, time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at one hour, want Failed on the run's own timeout", phase)
	}
}

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

// The scheduler leaves the timeout to the namespace, so a manual run and a
// scheduled one in the same namespace give up at the same point.
func TestAScheduledRunLeavesTheTimeoutToItsNamespace(t *testing.T) {
	created := time.Date(2026, 9, 24, 4, 0, 0, 0, time.UTC)
	_, c := schedule(t, time.Date(2026, 9, 24, 5, 0, 30, 0, time.UTC), scheduledNamespace("0 5 * * *", created), claim())

	runs := scheduledRuns(t, c)
	if len(runs) != 1 || runs[0].Spec.Timeout != nil {
		t.Fatalf("runs = %v, want one with no timeout of its own", runs)
	}
}

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

func TestAPruneIntervalThatDoesNotParseFailsTheItem(t *testing.T) {
	r := startedVolumeRun(t, nil, annotatedNamespace(map[string]string{backupv1alpha1.AnnotationPruneIntervalDays: "0"}))

	run := readBackupRun(t, r.Client)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(run.Status.Items[0].Message, backupv1alpha1.AnnotationPruneIntervalDays) {
		t.Fatalf("run = %+v, want the item failed naming %s", run.Status, backupv1alpha1.AnnotationPruneIntervalDays)
	}
}
