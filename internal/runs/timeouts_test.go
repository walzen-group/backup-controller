package runs

import (
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	t.Parallel()
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
	t.Parallel()
	r := startedVolumeRun(t, nil, annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "10h"}))

	if phase := phaseAt(t, r, 6*time.Hour); phase != backupv1alpha1.RunPhaseRunning {
		t.Fatalf("phase = %q at six hours, want Running under the namespace's ten", phase)
	}
	if phase := phaseAt(t, r, 10*time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at ten hours, want Failed", phase)
	}
}

// TestARunsOwnTimeoutWinsOverTheNamespace checks that a run's spec.timeout of
// one hour takes precedence over the namespace's 10h.
func TestARunsOwnTimeoutWinsOverTheNamespace(t *testing.T) {
	t.Parallel()
	r := startedVolumeRun(t, &metav1.Duration{Duration: time.Hour},
		annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "10h"}))

	if phase := phaseAt(t, r, time.Hour); phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q at one hour, want Failed on the run's own timeout", phase)
	}
}

// TestTheNamespacePruneIntervalReachesTheSource checks that the
// ReplicationSource's pruneIntervalDays is 1 without an annotation, and the
// annotation's value when the namespace sets one.
func TestTheNamespacePruneIntervalReachesTheSource(t *testing.T) {
	t.Parallel()
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
