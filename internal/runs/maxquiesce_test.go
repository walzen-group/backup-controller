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
	t.Parallel()
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
