package runs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// A BackupRun item that stops records why: a database item whose Backup
// CloudNativePG marks failed records BackupFailed, and at the quiesce limit
// a volume item that never started records NotStarted and a Running one
// whose clone is not cut records CloneNotCut. The messages stay as before.
func TestABackupItemRecordsWhyItStopped(t *testing.T) {
	t.Parallel()
	t.Run("a Backup in phase failed", func(t *testing.T) {
		r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), cluster())
		step(t, r) // plan
		step(t, r) // admit
		step(t, r) // start the Backup
		backup, ok := getUnstructured(t, c, cnpg.BackupGVK, ns, cnpg.BackupName(pgN, runUID))
		if !ok {
			t.Fatal("no Backup was created")
		}
		_ = unstructured.SetNestedField(backup.Object, "failed", "status", "phase")
		_ = unstructured.SetNestedField(backup.Object, "the barman upload failed", "status", "error")
		if err := c.Status().Update(context.Background(), backup); err != nil {
			t.Fatal(err)
		}
		step(t, r)

		item := readBackupRun(t, c).Status.Items[0]
		if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonBackupFailed ||
			!strings.Contains(item.Message, "the barman upload failed") {
			t.Errorf("item = %+v, want it Failed with reason BackupFailed and CloudNativePG's error", item)
		}
	})

	t.Run("the quiesce limit with one item Pending and one Running", func(t *testing.T) {
		objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
			annotatedNamespace(nil), claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false)},
			cacheClaim()...)
		r, c := backupReconciler(t, objects...)
		// The source of the claim cacheN can never be written, so its item
		// stays Pending. The other claim's item starts and runs.
		forbidden := func(obj client.Object) error {
			if _, ok := obj.(*volsyncv1alpha1.ReplicationSource); ok && obj.GetName() == cacheN {
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
		step(t, r) // admit
		step(t, r) // quiesce
		step(t, r) // start: one source is written, the other is refused
		replicasAt(t, r, c, 10*time.Minute)

		byName := map[string]backupv1alpha1.BackupItem{}
		for _, item := range readBackupRun(t, c).Status.Items {
			byName[item.Name] = item
		}
		if item := byName[cacheN]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonNotStarted ||
			!strings.HasPrefix(item.Message, "not started before the workloads were given back") || !strings.Contains(item.Message, "policy.example") {
			t.Errorf("Pending item = %+v, want it Failed with reason NotStarted and its start error", item)
		}
		if item := byName[claimN]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonCloneNotCut ||
			!strings.HasPrefix(item.Message, "VolSync had not cut the clone volsync-"+claimN+"-src") {
			t.Errorf("Running item = %+v, want it Failed with reason CloneNotCut naming the clone", item)
		}
	})
}
