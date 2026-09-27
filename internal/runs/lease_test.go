package runs

import (
	"context"
	"slices"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// blindToMovers wraps c so that its lists of ReplicationSources and
// Jobs come back empty. A reconciler reading through it
// stands for a run whose otherMover check ran in the same instant as the
// other run's: neither sees the other's mover object, and only the Leases,
// which go through c, can keep them apart.
func blindToMovers(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *volsyncv1alpha1.ReplicationSourceList, *batchv1.JobList:
				return nil
			}
			return cl.List(ctx, list, opts...)
		},
	})
}

// leaseNames returns the names of the claim's Lease and the repository's
// Lease, from the UIDs the client gave the claim and the Secret.
func leaseNames(t *testing.T, c client.Client) (string, string) {
	t.Helper()
	pvc, secret := &corev1.PersistentVolumeClaim{}, &corev1.Secret{}
	get(t, c, ns, claimN, pvc)
	get(t, c, ns, repoN, secret)
	if pvc.UID == "" || secret.UID == "" {
		t.Fatalf("claim UID = %q, Secret UID = %q; want both set", pvc.UID, secret.UID)
	}
	return claimLeaseName(pvc.UID), repositoryLeaseName(secret.UID)
}

// leaseHolderOf returns the holderIdentity of the named Lease, or "" when the
// Lease does not exist.
func leaseHolderOf(t *testing.T, c client.Client, name string) string {
	t.Helper()
	lease := &coordinationv1.Lease{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: name}, lease); err != nil {
		if apierrors.IsNotFound(err) {
			return ""
		}
		t.Fatal(err)
	}
	return holderUID(lease)
}

// A backup and a restore of the same claim whose otherMover checks both pass
// in the same instant start exactly one mover: the one that creates the
// claim's Lease first creates its mover object, and the other waits with
// reason SourceBusy and names it.
func TestTwoRunsWhoseChecksPassTogetherStartOneMover(t *testing.T) {
	t.Parallel()
	orders := map[string][]string{
		"in turn, backup first":  {"b", "r", "b", "r", "b", "r", "b", "r"},
		"in turn, restore first": {"r", "b", "r", "b", "r", "b", "r", "b"},
	}
	for name, order := range orders {
		t.Run(name, func(t *testing.T) {
			c := newClient(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
				claim(), volume(), volumeRestore(), repository())
			blind := blindToMovers(c)
			br := &BackupRunReconciler{Client: c, Reader: blind, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
			rr := &RestoreRunReconciler{Client: c, Reader: blind, Snapshots: snapshots{sunday, monday}, Now: frozenNow}
			for _, who := range order {
				if who == "b" {
					step(t, br)
				} else {
					restoreStep(t, rr)
				}
			}

			backup, restore := readBackupRun(t, c), readRestoreRun(t, c)
			triggered, restoring := ownSourceTag(t, c) == TriggerFor(runUID), len(movers(t, c)) == 1
			if triggered == restoring {
				t.Fatalf("trigger written = %v, mover created = %v; want exactly one mover object", triggered, restoring)
			}
			claimLease, repoLease := leaseNames(t, c)
			winner, loser, loserConditions, holderName := string(runUID), string(restoreUID), restore.Status.Conditions, "BackupRun before-upgrade"
			if restoring {
				winner, loser, loserConditions, holderName = string(restoreUID), string(runUID), backup.Status.Conditions, "RestoreRun back-to-monday"
			}
			if got := leaseHolderOf(t, c, claimLease); got != winner {
				t.Errorf("claim Lease holder = %q, want the run that started its mover, %q", got, winner)
			}
			if got := leaseHolderOf(t, c, repoLease); got != winner {
				t.Errorf("repository Lease holder = %q, want %q", got, winner)
			}
			if readyReason(loserConditions) != backupv1alpha1.ReasonSourceBusy || !strings.Contains(readyMessage(loserConditions), holderName) {
				t.Errorf("the waiting run %s has reason %q, message %q; want SourceBusy naming %s",
					loser, readyReason(loserConditions), readyMessage(loserConditions), holderName)
			}
		})
	}
}

// A claim Lease left by a run that finished, that no longer exists, or whose
// item for the claim has finished is taken over. One held by a run whose item
// is still Pending is not: the backup waits and names that run.
func TestAStaleLeaseIsTakenOverAndALiveOneIsNot(t *testing.T) {
	t.Parallel()
	finishedRestore := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseFailed
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemFailed}}
	})
	itemDone := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{
			{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemSucceeded},
			{Kind: "PersistentVolumeClaim", Name: "other", Phase: backupv1alpha1.ItemRunning},
		}
	})
	liveRestore := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemPending}}
	})
	for name, tc := range map[string]struct {
		holder []client.Object
		live   bool
	}{
		"finished run":  {holder: []client.Object{finishedRestore}},
		"deleted run":   {},
		"finished item": {holder: []client.Object{itemDone}},
		"live run":      {holder: []client.Object{liveRestore}, live: true},
	} {
		t.Run(name, func(t *testing.T) {
			objects := slices.Concat(tc.holder, []client.Object{
				backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository(),
			})
			r, c := backupReconciler(t, objects...)
			claimLease, _ := leaseNames(t, c)
			held := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: claimLease, Namespace: ns}}
			stamp(held, leaseHolder{kind: "RestoreRun", run: restoreRun(), item: claimN}, []string{claimN})
			if err := c.Create(context.Background(), held); err != nil {
				t.Fatal(err)
			}
			step(t, r)
			step(t, r)
			step(t, r)

			run := readBackupRun(t, c)
			if tc.live {
				if readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
					!strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun back-to-monday") {
					t.Errorf("reason = %q, message = %q; want SourceBusy naming RestoreRun back-to-monday",
						readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
				}
				if got := leaseHolderOf(t, c, claimLease); got != string(restoreUID) {
					t.Errorf("claim Lease holder = %q; a live holder's Lease was taken over", got)
				}
				if ownSourceTag(t, c) == TriggerFor(runUID) {
					t.Error("the backup wrote its trigger while a live run held the claim's Lease")
				}
				return
			}
			if run.Status.Items[0].Phase != backupv1alpha1.ItemRunning || ownSourceTag(t, c) != TriggerFor(runUID) {
				t.Fatalf("item = %+v, reason = %q; want Running with the trigger written", run.Status.Items[0], readyReason(run.Status.Conditions))
			}
			if got := leaseHolderOf(t, c, claimLease); got != string(runUID) {
				t.Errorf("claim Lease holder = %q, want the backup %q", got, runUID)
			}
		})
	}
}
