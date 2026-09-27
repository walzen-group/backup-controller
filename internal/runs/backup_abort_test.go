package runs

import (
	"context"
	"errors"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// An item that abort fails records the reason RunEnded, and the run's
// ending says why the run ended. The item's message stays as before: it
// starts with the run's Ready message. The cases are three of the ways that
// abort ends a BackupRun: a namespace timeout that does not parse at admission
// (the item is Pending), a stop of the app that fails (the item is
// Pending), and a max-quiesce limit that stops to parse while the item
// runs (the item is Running).
func TestAnAbortedBackupItemRecordsRunEnded(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		// run reconciles a new run until abort has ended it, and returns
		// the client that holds it.
		run func(t *testing.T) client.Client
	}{
		{name: "a namespace timeout that does not parse", run: func(t *testing.T) client.Client {
			r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Timeout = nil }),
				annotatedNamespace(map[string]string{backupv1alpha1.AnnotationTimeout: "soon"}),
				claim(), volume(), volumeRestore(), repository())
			step(t, r) // plan
			step(t, r) // admit: the timeout does not parse
			return c
		}},
		{name: "a stop of the app that fails", run: func(t *testing.T) client.Client {
			c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
			failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
					if replicas, ok := deploymentScale(sub, obj, opts); ok && replicas == 0 {
						return apierrors.NewInternalError(errors.New("the scale write failed"))
					}
					return cl.SubResource(sub).Update(ctx, obj, opts...)
				},
			})
			r := &BackupRunReconciler{Client: failing, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
			step(t, r) // plan
			step(t, r) // admit
			step(t, r) // quiesce: the stop fails
			return c
		}},
		{name: "a max-quiesce limit that stops to parse while the item runs", run: func(t *testing.T) client.Client {
			r, c := quiescedVolumeRun(t, annotatedNamespace(nil))
			namespace := &corev1.Namespace{}
			get(t, c, "", ns, namespace)
			namespace.Annotations = map[string]string{backupv1alpha1.AnnotationMaxQuiesce: "ten minutes"}
			if err := c.Update(context.Background(), namespace); err != nil {
				t.Fatal(err)
			}
			step(t, r) // the limit does not parse
			return c
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := readBackupRun(t, tt.run(t))
			if run.Status.Phase != backupv1alpha1.RunPhaseFailed || len(run.Status.Items) == 0 {
				t.Fatalf("phase = %q, items = %+v; want Failed with its items", run.Status.Phase, run.Status.Items)
			}
			message := readyMessage(run.Status.Conditions)
			for _, item := range run.Status.Items {
				if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRunEnded {
					t.Errorf("item %s: phase = %q, reason = %q; want Failed with reason RunEnded", item.Name, item.Phase, item.Reason)
				}
				if !strings.HasPrefix(item.Message, message) {
					t.Errorf("item %s: message = %q; want it to start with the run's message %q", item.Name, item.Message, message)
				}
			}
		})
	}
}
