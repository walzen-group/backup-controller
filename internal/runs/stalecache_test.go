package runs

import (
	"context"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// laggingCache wraps c so that a Get of the run stale names returns stale, as
// an informer cache that has not yet seen the run's later writes does. Every
// other read, and every write, goes through c.
func laggingCache(c client.Client, stale client.Object) client.Client {
	key := client.ObjectKeyFromObject(stale)
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, k client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if k == key {
				switch want := obj.(type) {
				case *backupv1alpha1.BackupRun:
					if s, ok := stale.(*backupv1alpha1.BackupRun); ok {
						s.DeepCopyInto(want)
						return nil
					}
				case *backupv1alpha1.RestoreRun:
					if s, ok := stale.(*backupv1alpha1.RestoreRun); ok {
						s.DeepCopyInto(want)
						return nil
					}
				}
			}
			return cl.Get(ctx, k, obj, opts...)
		},
	})
}

// A BackupRun pass that reads its run from a cache that lags behind never
// starts the app again when the stored run shows the restart done: another
// run holds the namespace's quiesce Lease and has stopped the app since, and
// a restart then would scale the app up under that run's stop. The run reads
// itself again from the API server before any restart and decides on that
// copy. It holds for the restart after the clones are cut (restartPending in
// the cached copy) and for the one release does when the run ends (the
// cached copy holds the app, or owes its restart).
func TestAStaleCachedBackupRunNeverRestartsTheApp(t *testing.T) {
	t.Parallel()
	running := func(b *backupv1alpha1.BackupRun) {
		b.Finalizers = []string{Finalizer}
		b.Status.StartedAt = atFrozen(-time.Minute)
		b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
		b.Status.Items[0].Phase, b.Status.Items[0].Trigger = backupv1alpha1.ItemRunning, TriggerFor(runUID)
	}
	for _, tc := range []struct {
		name  string
		stale func(*backupv1alpha1.BackupRun)
		now   time.Time
	}{
		{"restart pending while backing up", func(b *backupv1alpha1.BackupRun) {
			b.Status.RestartedAt, b.Status.RestartPending = atFrozen(0), true
		}, frozen},
		{"restart pending at the timeout", func(b *backupv1alpha1.BackupRun) {
			b.Status.RestartedAt, b.Status.RestartPending = atFrozen(0), true
		}, frozen.Add(time.Hour)},
		{"app held at the timeout", func(b *backupv1alpha1.BackupRun) {
			b.Status.RestartedAt, b.Status.RestartPending = nil, false
		}, frozen.Add(time.Hour)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			source := idleSource()
			source.Spec.Trigger.Manual = TriggerFor(runUID)
			other := secondBackupRun(func(b *backupv1alpha1.BackupRun) {
				b.Finalizers = []string{Finalizer}
				b.Spec.All = true
				b.Status.Phase = backupv1alpha1.RunPhaseRunning
				b.Status.StartedAt, b.Status.QuiescedAt = atFrozen(0), atFrozen(0)
				b.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
				b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
			})
			c := newClient(t, quiescedBackup(running, tc.stale), other, claim(), volume(), volumeRestore(), repository(),
				source, stoppedDeployment(), kustomization(true))
			seedQuiesceLease(t, c, "BackupRun", other)

			stale := readBackupRun(t, c)
			stored := stale.DeepCopy()
			stored.Status.RestartedAt, stored.Status.RestartPending = atFrozen(0), false
			if err := c.Status().Update(context.Background(), stored); err != nil {
				t.Fatal(err)
			}

			br := &BackupRunReconciler{Client: laggingCache(c, stale), Reader: c, Snapshots: snapshots{sunday, monday},
				Retimer: &retimer{}, Now: func() time.Time { return tc.now }}
			_ = tryStep(br) // the pass's own status write conflicts with the stored run

			if got := replicasOf(t, c); got != 0 || !suspended(t, c) {
				t.Errorf("replicas = %d, suspended = %t; want the other run's stop left alone", got, suspended(t, c))
			}
			if got := leaseHolderOf(t, c, quiesceLeaseName); got != string(otherRunUID) {
				t.Errorf("quiesce Lease holder = %q, want the other run", got)
			}
		})
	}
}

// A RestoreRun pass that reads its run from a cache that lags behind never
// starts the app again when the stored run shows the restart done, whether
// the restart is the one while the run waits for its Cluster, or the one
// finish or finalize does.
func TestAStaleCachedRestoreRunNeverRestartsTheApp(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		mutate func(*backupv1alpha1.RestoreRun)
		now    time.Time
	}{
		{"waiting for its Cluster", func(*backupv1alpha1.RestoreRun) {}, frozen},
		{"at the timeout", func(r *backupv1alpha1.RestoreRun) {
			r.Spec.Timeout = &metav1.Duration{Duration: time.Hour}
		}, frozen.Add(2 * time.Hour)},
		{"deleted", func(r *backupv1alpha1.RestoreRun) {
			r.DeletionTimestamp = atFrozen(0)
		}, frozen},
	} {
		t.Run(tc.name, func(t *testing.T) {
			run := restoreRun(func(r *backupv1alpha1.RestoreRun) {
				r.Finalizers = []string{Finalizer}
				r.Spec.Database = pgN
				r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
				r.Status.Phase = backupv1alpha1.RunPhaseWaiting
				r.Status.StartedAt = atFrozen(-time.Minute)
				r.Status.QuiescedAt = atFrozen(-time.Minute)
				r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
				r.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
				r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemDeleted, ClusterUID: "old-cluster-uid"}}
			}, tc.mutate)
			other := secondBackupRun(func(b *backupv1alpha1.BackupRun) {
				b.Finalizers = []string{Finalizer}
				b.Spec.All = true
				b.Status.Phase = backupv1alpha1.RunPhaseRunning
				b.Status.StartedAt, b.Status.QuiescedAt = atFrozen(0), atFrozen(0)
				b.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
				b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
			})
			r, c := restoreReconciler(t, nil, run, other, stoppedDeployment(), kustomization(true))
			seedQuiesceLease(t, c, "BackupRun", other)

			stale := readRestoreRun(t, c)
			stored := stale.DeepCopy()
			stored.Status.RestartedAt = atFrozen(0)
			if err := c.Status().Update(context.Background(), stored); err != nil {
				t.Fatal(err)
			}
			r.Client, r.Reader = laggingCache(c, stale), c
			r.Now = func() time.Time { return tc.now }
			_, _ = r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}})

			if got := replicasOf(t, c); got != 0 || !suspended(t, c) {
				t.Errorf("replicas = %d, suspended = %t; want the other run's stop left alone", got, suspended(t, c))
			}
		})
	}
}

// A BackupRun pass that reads its run from a cache that lags behind never
// stops the app again once the stored run shows the stop and the restart
// done. The cached copy still shows the recorded plan without
// status.quiescedAt, and a stop from it would leave the app at 0 with no run
// left to give it back: the run has finished, and the pass's own status write
// conflicts with the stored run. A stored run that is newer and still owes
// the stop, with the plan and no status.quiescedAt, is stopped as before.
func TestAStaleCachedBackupRunNeverStopsTheAppAgain(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		store   func(*backupv1alpha1.BackupRun)
		stopped bool
	}{
		{"restarted and finished", func(b *backupv1alpha1.BackupRun) {
			b.Status.Phase = backupv1alpha1.RunPhaseSucceeded
			b.Status.QuiescedAt, b.Status.RestartedAt = atFrozen(0), atFrozen(0)
			b.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
		}, false},
		{"restarted and still running", func(b *backupv1alpha1.BackupRun) {
			b.Status.QuiescedAt, b.Status.RestartedAt = atFrozen(0), atFrozen(0)
		}, false},
		{"finished with the plan and no stop recorded", func(b *backupv1alpha1.BackupRun) {
			b.Status.Phase = backupv1alpha1.RunPhaseFailed
		}, false},
		{"still owing the stop", func(b *backupv1alpha1.BackupRun) {
			b.Status.Items[0].Message = "noted"
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newClient(t, quiescedBackup(func(b *backupv1alpha1.BackupRun) {
				b.Finalizers = []string{Finalizer}
				b.Status.StartedAt = atFrozen(-time.Minute)
				b.Status.QuiescedAt = nil
				b.Status.SuspendedKustomizations = []string{"flux-system/" + appN}
			}), claim(), volume(), volumeRestore(), repository(), idleSource(), deployment(), kustomization(false))
			seedQuiesceLease(t, c, "BackupRun", readBackupRun(t, c))

			stale := readBackupRun(t, c)
			stored := stale.DeepCopy()
			tc.store(stored)
			if err := c.Status().Update(context.Background(), stored); err != nil {
				t.Fatal(err)
			}

			br := &BackupRunReconciler{Client: laggingCache(c, stale), Reader: c, Snapshots: snapshots{sunday, monday},
				Retimer: &retimer{}, Now: func() time.Time { return frozen }}
			_ = tryStep(br)

			if got := replicasOf(t, c); (got == 0) != tc.stopped || suspended(t, c) != tc.stopped {
				t.Errorf("replicas = %d, suspended = %t; want stopped = %t", got, suspended(t, c), tc.stopped)
			}
		})
	}
}
