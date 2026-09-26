package runs

import (
	"context"
	"strings"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// blindToMovers wraps c so that its lists of ReplicationSources and
// ReplicationDestinations come back empty. A reconciler reading through it
// stands for a run whose otherMover check ran in the same instant as the
// other run's: neither sees the other's mover object, and only the Leases,
// which go through c, can keep them apart.
func blindToMovers(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			switch list.(type) {
			case *volsyncv1alpha1.ReplicationSourceList, *volsyncv1alpha1.ReplicationDestinationList:
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
			triggered, restoring := ownSourceTag(t, c) == TriggerFor(runUID), len(destinations(t, c)) == 1
			if triggered == restoring {
				t.Fatalf("trigger written = %v, destination created = %v; want exactly one mover object", triggered, restoring)
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
			objects := append(tc.holder, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository())
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

// A restore releases its Leases when it finishes, so a backup of the claim
// can take them at once.
func TestARestoreReleasesItsLeasesWhenItFinishes(t *testing.T) {
	r, c := restoreReconciler(t, nil,
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	restoreStep(t, r)
	restoreStep(t, r)

	claimLease, repoLease := leaseNames(t, c)
	if leaseHolderOf(t, c, claimLease) != string(restoreUID) || leaseHolderOf(t, c, repoLease) != string(restoreUID) {
		t.Fatalf("claim Lease holder = %q, repository Lease holder = %q; want the restore to hold both before its destination",
			leaseHolderOf(t, c, claimLease), leaseHolderOf(t, c, repoLease))
	}
	run := readRestoreRun(t, c)
	rd := &volsyncv1alpha1.ReplicationDestination{}
	get(t, c, ns, run.Status.Items[0].Destination, rd)
	rd.Status = &volsyncv1alpha1.ReplicationDestinationStatus{LastManualSync: string(restoreUID)}
	if err := c.Status().Update(context.Background(), rd); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q, want Succeeded", run.Status.Phase)
	}
	if h1, h2 := leaseHolderOf(t, c, claimLease), leaseHolderOf(t, c, repoLease); h1 != "" || h2 != "" {
		t.Errorf("claim Lease holder = %q, repository Lease holder = %q; want both released", h1, h2)
	}
}

// Every run takes the claim's Lease before the repository's, so no two runs
// wait for each other. Here a restore of claim notes-data holds both Leases.
// A backup of the same claim fails to create the claim Lease and never asks
// for the repository's, so it holds nothing the restore could wait for; once
// the restore finishes and releases both, the backup takes both in the same
// order. Had the backup taken the repository Lease first, it would hold it
// while the restore held the claim, and each would wait for the other.
func TestLeasesAreTakenClaimFirstSoNoTwoRunsDeadlock(t *testing.T) {
	restore := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN, Phase: backupv1alpha1.ItemRunning}}
	})
	r, c := backupReconciler(t, restore, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	claimLease, repoLease := leaseNames(t, c)
	holder := leaseHolder{kind: "RestoreRun", run: restore, item: claimN}
	for _, name := range []string{claimLease, repoLease} {
		lease := &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns}}
		stamp(lease, holder, []string{claimN})
		if err := c.Create(context.Background(), lease); err != nil {
			t.Fatal(err)
		}
	}
	// The repository Lease disappears; the backup still does not take it,
	// because it has not got the claim's.
	if err := c.Delete(context.Background(), &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: repoLease, Namespace: ns}}); err != nil {
		t.Fatal(err)
	}
	step(t, r)
	step(t, r)
	step(t, r)
	if got := leaseHolderOf(t, c, repoLease); got != "" {
		t.Fatalf("repository Lease holder = %q; the backup took it without the claim's", got)
	}
	if run := readBackupRun(t, c); readyReason(run.Status.Conditions) != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("reason = %q, want SourceBusy while the restore holds the claim's Lease", readyReason(run.Status.Conditions))
	}

	finished := readRestoreRun(t, c)
	finished.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	finished.Status.Items[0].Phase = backupv1alpha1.ItemSucceeded
	if err := c.Status().Update(context.Background(), finished); err != nil {
		t.Fatal(err)
	}
	step(t, r)
	if leaseHolderOf(t, c, claimLease) != string(runUID) || leaseHolderOf(t, c, repoLease) != string(runUID) {
		t.Errorf("claim Lease holder = %q, repository Lease holder = %q; want the backup to hold both",
			leaseHolderOf(t, c, claimLease), leaseHolderOf(t, c, repoLease))
	}
}
