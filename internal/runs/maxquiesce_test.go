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
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

// A claim whose ReplicationSource the API server refuses to write every time,
// as RBAC or a policy webhook does with Forbidden, keeps the app down only
// until the limit. The pass at the limit fails the Pending item with a
// message that names the limit and the Forbidden error, and gives the app its
// replicas back.
func TestASourceThatCannotBeWrittenReleasesTheAppAtTheLimit(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		annotatedNamespace(nil), claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	forbidden := func(obj client.Object) error {
		if _, ok := obj.(*volsyncv1alpha1.ReplicationSource); ok {
			return apierrors.NewForbidden(volsyncv1alpha1.GroupVersion.WithResource("replicationsources").GroupResource(), obj.GetName(),
				errors.New(`admission webhook "policy.example" denied the request`))
		}
		return nil
	}
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if err := forbidden(obj); err != nil {
				return err
			}
			return cl.Create(ctx, obj, opts...)
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err := forbidden(obj); err != nil {
				return err
			}
			return cl.Update(ctx, obj, opts...)
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if err := forbidden(obj); err != nil {
				return err
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // the source write is refused

	if replicas := replicasAt(t, r, c, 10*time.Minute-time.Second); replicas != 0 {
		t.Fatalf("replicas = %d a second before the limit, want the app still down", replicas)
	}
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemPending || !strings.Contains(item.Message, "forbidden") {
		t.Fatalf("item = %+v before the limit, want it Pending with the Forbidden error", item)
	}

	if replicas := replicasAt(t, r, c, 10*time.Minute); replicas != 2 {
		t.Fatalf("replicas = %d at the limit, want 2 back", replicas)
	}
	run := readBackupRun(t, c)
	if run.Status.RestartedAt == nil || !run.Status.RestartedAt.Time.Equal(frozen.Add(10*time.Minute)) {
		t.Errorf("restartedAt = %v, want the moment of the limit pass", run.Status.RestartedAt)
	}
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, backupv1alpha1.AnnotationMaxQuiesce) ||
		!strings.Contains(item.Message, "forbidden") || !strings.Contains(item.Message, "policy.example") {
		t.Errorf("item = %+v, want it Failed naming %s and the Forbidden error", item, backupv1alpha1.AnnotationMaxQuiesce)
	}
}

// A pod of a stopped workload that never goes away, as on a node that stopped
// answering, keeps the app down only until the limit. Before it the run
// waits for the pod; the pass at the limit fails the volume item, which
// never started, and gives the app its replicas back.
func TestAPodThatNeverStopsReleasesTheAppAtTheLimit(t *testing.T) {
	stuck := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "notes-5d9f", Namespace: ns, Labels: map[string]string{"app": appN}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		annotatedNamespace(nil), claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false), stuck)
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce

	if replicas := replicasAt(t, r, c, 10*time.Minute-time.Second); replicas != 0 {
		t.Fatalf("replicas = %d a second before the limit, want the app still down", replicas)
	}
	run := readBackupRun(t, c)
	if message := readyMessage(run.Status.Conditions); !strings.Contains(message, stuck.Name) || run.Status.Items[0].Phase != backupv1alpha1.ItemPending {
		t.Fatalf("Ready = %q, item = %+v before the limit, want the run waiting for pod %s", message, run.Status.Items[0], stuck.Name)
	}

	if replicas := replicasAt(t, r, c, 10*time.Minute); replicas != 2 {
		t.Fatalf("replicas = %d at the limit, want 2 back", replicas)
	}
	run = readBackupRun(t, c)
	if run.Status.RestartedAt == nil {
		t.Error("restartedAt is unset after the app came back")
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "not started before") {
		t.Errorf("item = %+v, want it Failed saying it never started", item)
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := c.Get(context.Background(), client.ObjectKey{Namespace: ns, Name: claimN}, source); !apierrors.IsNotFound(err) {
		t.Errorf("get the source = %v, want no source written while the pod ran", err)
	}
}

// A pass at the limit whose status write is lost leaves the app down, since
// it records status.restartedAt before it starts anything. The next pass is
// past the limit too, restarts the app, and records its own moment, so
// restartedAt is never later than the restart. The clock of that pass moves
// on by a second each time the run reads it, and the test notes the latest
// time the run had read when the Deployment's scale-up reached the API
// server, so a moment taken after the restart would show.
func TestALostWriteAtTheLimitKeepsTheRestartAfterTheMoment(t *testing.T) {
	r, c := quiescedVolumeRun(t, annotatedNamespace(nil))
	healthy := r.Client

	r.Client = loseStatusWriteAt(c, 0)
	r.Now = func() time.Time { return frozen.Add(10 * time.Minute) }
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}}); err == nil {
		t.Fatal("the limit pass succeeded, want its lost status write returned")
	}
	if got := replicasOf(t, c); got != 0 {
		t.Fatalf("replicas = %d after the lost limit pass, want the app still down", got)
	}
	if run := readBackupRun(t, c); run.Status.RestartedAt != nil {
		t.Fatalf("restartedAt = %v after the lost write, want it unset", run.Status.RestartedAt)
	}

	clock := frozen.Add(10*time.Minute + 30*time.Second)
	r.Now = func() time.Time {
		now := clock
		clock = clock.Add(time.Second)
		return now
	}
	var scaledUp time.Time
	r.Client = interceptor.NewClient(healthy.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if replicas, ok := deploymentScale(sub, obj, opts); ok && scaledUp.IsZero() && replicas == 2 {
				// The latest time the run has read.
				scaledUp = clock.Add(-time.Second)
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	step(t, r)
	if replicas := replicasOf(t, c); replicas != 2 || scaledUp.IsZero() {
		t.Fatalf("replicas = %d on the pass after the lost write, want 2 back", replicas)
	}
	run := readBackupRun(t, c)
	if run.Status.RestartedAt == nil || run.Status.RestartedAt.After(scaledUp) {
		t.Errorf("restartedAt = %v, want a moment no later than the scale-up at %s", run.Status.RestartedAt, scaledUp)
	}
	if run.Status.RestartPending {
		t.Error("restartPending is still set after the restart")
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed {
		t.Errorf("item = %+v, want it Failed at the limit", item)
	}
}

// A Lease list the API server keeps failing does not hold the app past the
// limit. The pass lets the Leases of finished items go before it checks the
// limit, and that release is best effort: a Lease left behind is taken over
// once its item is done.
func TestALeaseReleaseThatKeepsFailingDoesNotHoldTheAppPastTheLimit(t *testing.T) {
	r, c := quiescedVolumeRun(t, annotatedNamespace(nil))
	r.Reader = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, ok := list.(*coordinationv1.LeaseList); ok {
				return apierrors.NewServiceUnavailable("etcd leader changed")
			}
			return cl.List(ctx, list, opts...)
		},
	})
	r.Now = func() time.Time { return frozen.Add(10 * time.Minute) }
	_ = tryStep(r)
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d at the limit with the Lease list failing, want 2 back", got)
	}
	if run := readBackupRun(t, c); run.Status.RestartedAt == nil {
		t.Error("restartedAt is unset after the app came back")
	}
}
