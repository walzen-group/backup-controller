package runs

import (
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// quiescedVolumeRun creates a BackupRun with spec.all set in the given
// Namespace, with the test claim and the quiesce-marked Deployment, and
// reconciles it until the claim's source carries the run's trigger. The
// quiesce pass runs at frozen, so status.quiescedAt is frozen. VolSync never
// cuts a clone unless the test cuts one.
func quiescedVolumeRun(t *testing.T, namespace client.Object) (*BackupRunReconciler, client.Client) {
	t.Helper()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		namespace, claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	step(t, r) // plan
	step(t, r) // admit
	step(t, r) // quiesce
	step(t, r) // start
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("item = %+v after the start pass, want it Running", item)
	}
	return r, c
}

// replicasAt sets the clock to the given duration after frozen, reconciles
// the run once, and returns the Deployment's replica count.
func replicasAt(t *testing.T, r *BackupRunReconciler, c client.Client, after time.Duration) int32 {
	t.Helper()
	r.Now = func() time.Time { return frozen.Add(after) }
	step(t, r)
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	return *d.Spec.Replicas
}

// A clone VolSync never cuts keeps the app down only until the default limit
// of ten minutes after status.quiescedAt. The pass at the limit fails the
// volume item with a message that names the limit and the missing clone,
// records restartedAt and gives the app its replicas back.
func TestTheAppComesBackAtTheQuiesceLimitWhenNoCloneIsCut(t *testing.T) {
	r, c := quiescedVolumeRun(t, annotatedNamespace(nil))

	if replicas := replicasAt(t, r, c, 10*time.Minute-time.Second); replicas != 0 {
		t.Fatalf("replicas = %d a second before the limit, want the app still down", replicas)
	}
	if run := readBackupRun(t, c); run.Status.RestartedAt != nil || run.Status.Items[0].Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("run before the limit: restartedAt = %v, item = %+v, want the item still Running", run.Status.RestartedAt, run.Status.Items[0])
	}

	if replicas := replicasAt(t, r, c, 10*time.Minute); replicas != 2 {
		t.Fatalf("replicas = %d at the limit, want 2 back", replicas)
	}
	run := readBackupRun(t, c)
	if run.Status.RestartedAt == nil || !run.Status.RestartedAt.Time.Equal(frozen.Add(10*time.Minute)) {
		t.Errorf("restartedAt = %v, want the moment of the limit pass", run.Status.RestartedAt)
	}
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed ||
		!strings.Contains(item.Message, backupv1alpha1.AnnotationMaxQuiesce) ||
		!strings.Contains(item.Message, "volsync-"+claimN+"-src") {
		t.Errorf("item = %+v, want it Failed naming %s and the clone volsync-%s-src", item, backupv1alpha1.AnnotationMaxQuiesce, claimN)
	}
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed with its only item failed", run.Status.Phase)
	}
}

// A namespace's backup.wlz.li/max-quiesce of 30m keeps the app down past the
// default ten minutes while the clone is not cut, and gives it back at 30.
func TestANamespaceQuiesceLimitReplacesTheDefault(t *testing.T) {
	r, c := quiescedVolumeRun(t, annotatedNamespace(map[string]string{backupv1alpha1.AnnotationMaxQuiesce: "30m"}))

	if replicas := replicasAt(t, r, c, 10*time.Minute); replicas != 0 {
		t.Fatalf("replicas = %d at ten minutes, want the app still down under the namespace's 30m", replicas)
	}
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("item = %+v at ten minutes, want it still Running", item)
	}
	if replicas := replicasAt(t, r, c, 30*time.Minute); replicas != 2 {
		t.Fatalf("replicas = %d at thirty minutes, want 2 back", replicas)
	}
}

// A backup.wlz.li/max-quiesce that isn't a Go duration fails the run before
// it stops anything, with a message that names the annotation.
func TestAQuiesceLimitThatDoesNotParseFailsTheRun(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		annotatedNamespace(map[string]string{backupv1alpha1.AnnotationMaxQuiesce: "ten minutes"}),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	step(t, r) // plan
	step(t, r) // admit

	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", run.Status.Phase)
	}
	if message := readyMessage(run.Status.Conditions); !strings.Contains(message, backupv1alpha1.AnnotationMaxQuiesce) {
		t.Errorf("message %q does not name %s", message, backupv1alpha1.AnnotationMaxQuiesce)
	}
	if run.Status.QuiescedAt != nil || len(run.Status.Quiesced) != 0 {
		t.Errorf("quiescedAt = %v, quiesced = %v, want nothing stopped", run.Status.QuiescedAt, run.Status.Quiesced)
	}
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 {
		t.Errorf("replicas = %d, want the app left at 2", *d.Spec.Replicas)
	}
}

// A clone cut a second before the limit, and first seen by the pass at the
// limit, counts: the item goes on Running, and the app comes back on the
// normal path.
func TestACloneCutJustBeforeTheQuiesceLimitGoesOn(t *testing.T) {
	r, c := quiescedVolumeRun(t, annotatedNamespace(nil))
	cloneAt(t, c, frozen.Add(10*time.Minute-time.Second))

	if replicas := replicasAt(t, r, c, 10*time.Minute); replicas != 2 {
		t.Fatalf("replicas = %d at the limit with the clone cut, want 2 back", replicas)
	}
	run := readBackupRun(t, c)
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Message != "" {
		t.Errorf("item = %+v, want it still Running with no message", item)
	}
	if run.Status.RestartedAt == nil {
		t.Error("restartedAt is unset after the app came back")
	}
}
