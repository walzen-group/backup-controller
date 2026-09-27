package runs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The API groups of Flux's kustomize-controller and of Kueue.
const (
	fluxGroup  = "kustomize.toolkit.fluxcd.io"
	kueueGroup = "kueue.x-k8s.io"
)

// servedBackupReconciler returns a BackupRunReconciler over a strict client
// that holds the given objects and fails every call for a kind the API
// server does not serve, as a real client does (see servingOnly). The groups
// in removed are served by no CRD from the start. It also returns the strict
// client underneath, which the test's own reads and writes go through, and
// its mapper, through which a test removes a group later.
func servedBackupReconciler(t *testing.T, removed []string, objects ...client.Object) (*BackupRunReconciler, client.Client, *servedKinds) {
	t.Helper()
	c := newClient(t, objects...)
	kinds := c.RESTMapper().(*servedKinds)
	kinds.remove(removed...)
	serving := servingOnly(c)
	return &BackupRunReconciler{Client: serving, Reader: serving, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: func() time.Time { return frozen }}, c, kinds
}

// tryStep reconciles the BackupRun before-upgrade once and returns the
// reconcile's error.
func tryStep(r *BackupRunReconciler) error {
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}})
	return err
}

// On a cluster without Flux, a Deployment that still carries
// kustomize-controller's labels is stopped with nothing suspended, and the
// run goes on.
func TestANamespaceIsQuiescedWithoutFlux(t *testing.T) {
	t.Parallel()
	r, c, _ := servedBackupReconciler(t, []string{fluxGroup}, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment())
	step(t, r) // plan
	step(t, r) // admit, no queue
	if err := tryStep(r); err != nil {
		t.Fatalf("quiesce pass: %v", err)
	}
	run := readBackupRun(t, c)
	if run.Status.QuiescedAt == nil || len(run.Status.SuspendedKustomizations) != 0 {
		t.Fatalf("quiescedAt = %v, suspended = %v, want the app stopped with nothing suspended", run.Status.QuiescedAt, run.Status.SuspendedKustomizations)
	}
	if replicas := replicasOf(t, c); replicas != 0 {
		t.Fatalf("replicas = %d after the quiesce pass, want 0", replicas)
	}
}

// On a cluster without Kueue's CRDs a run starts without admission and has
// no Workload to delete, so a volume run finishes.
func TestASourceRunFinishesWithoutKueue(t *testing.T) {
	t.Parallel()
	r, c, _ := servedBackupReconciler(t, []string{kueueGroup}, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit: no Kueue, so it starts at once
	step(t, r) // start
	complete(t, c)
	if err := tryStep(r); err != nil {
		t.Fatalf("collect pass: %v", err)
	}
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}

// A restart the API server keeps refusing keeps the run unfinished. The run
// says why on its Ready condition, with reason RestartFailed, a message that
// names the Deployment and the replicas it is owed, and one Warning event. It
// retries on every pass, and the first pass the API server accepts gives the
// app back and finishes the run.
func TestARestartThatKeepsFailingIsReportedAndRetried(t *testing.T) {
	t.Parallel()
	r, c, _ := servedBackupReconciler(t, nil, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	refuse := false
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if replicas, ok := deploymentScale(sub, obj, opts); ok && refuse && replicas != 0 {
				return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments/scale"}, obj.GetName(), errors.New("a policy refuses the change"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start

	refuse = true
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	r.Now = func() time.Time { return frozen.Add(2 * time.Hour) } // past the one-hour timeout
	for pass := 1; pass <= 2; pass++ {
		if err := tryStep(r); err == nil {
			t.Fatalf("pass %d: the reconcile returned no error, want the refused restart retried", pass)
		}
		run := readBackupRun(t, c)
		if run.Status.Phase.Finished() || run.Status.RestartedAt != nil {
			t.Fatalf("pass %d: phase = %q, restartedAt = %v, want the run unfinished and the restart still owed", pass, run.Status.Phase, run.Status.RestartedAt)
		}
		if reason := readyReason(run.Status.Conditions); reason != backupv1alpha1.ReasonRestartFailed {
			t.Fatalf("pass %d: reason = %q, want %s", pass, reason, backupv1alpha1.ReasonRestartFailed)
		}
		message := readyMessage(run.Status.Conditions)
		for _, want := range []string{"Deployment " + appN, "2 replicas", "a policy refuses the change",
			"scale Deployment " + appN + " to 2", "resume Kustomization flux-system/" + appN, Finalizer} {
			if !strings.Contains(message, want) {
				t.Errorf("pass %d: message %q does not name %q", pass, message, want)
			}
		}
		if strings.Index(message, Finalizer) < strings.Index(message, "scale Deployment "+appN+" to 2") {
			t.Errorf("pass %d: message %q gives the finalizer before the scaling step", pass, message)
		}
	}
	got := recorded(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Warning "+backupv1alpha1.ReasonRestartFailed+" ") {
		t.Fatalf("events = %q, want one Warning %s", got, backupv1alpha1.ReasonRestartFailed)
	}
	if replicas := replicasOf(t, c); replicas != 0 {
		t.Fatalf("replicas = %d while the API server refuses the restart, want 0", replicas)
	}

	refuse = false
	if err := tryStep(r); err != nil {
		t.Fatalf("pass after the refusal ended: %v", err)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Fatalf("replicas = %d once the API server accepts the restart, want 2 back", replicas)
	}
	k, _ := getUnstructured(t, c, quiesce.KustomizationGVK, "flux-system", appN)
	if suspended, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend"); suspended {
		t.Error("the Kustomization the run suspended was not resumed")
	}
	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Fatalf("phase = %q, reason = %q, want the timed-out run Failed", run.Status.Phase, readyReason(run.Status.Conditions))
	}
}
