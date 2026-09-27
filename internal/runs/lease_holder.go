package runs

import (
	"context"
	"fmt"
	"slices"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// holderLive reports whether the run that holds a Lease still needs it.
//
// Parameters:
//   - reader reads the holder run, uncached.
//   - lease is the Lease as stored. It lives in its holder's namespace.
//
// It returns true while the holder needs the Lease, and an error when the
// read of the holder fails.
//
// A claim or repository Lease is live while the run exists with the UID the
// Lease names, has not finished, and has an item the Lease names that is
// Pending or Running. A RestoreRun's item also keeps it live once it has
// finished, for as long as it records the UID of its restore Job: the run
// has stopped that Job and not yet seen that no pod of it can write (rule
// X2, see stopJobs). Only an item of the kind that takes the Lease counts: a
// BackupRun's ReplicationSource item or a RestoreRun's PersistentVolumeClaim
// item for a claim or repository Lease, and a BackupRun's Cluster item for a
// Cluster Lease (see clusterLeaseName). A Lease names its items by name
// alone, so a Cluster item with the claim's name does not keep the claim's
// Lease, and a volume item does not keep a Cluster Lease. A quiesce Lease is live
// while the holder's stored status does not show the workloads given back
// (see durablyRestarted): the holder exists with the UID the Lease names, has
// not finished, and has no plan yet or has not recorded its restart as done.
// A run being deleted counts as live until its finalizer has released the
// Lease. A Lease that names no run kind this controller knows is left alone,
// and counts as live.
func holderLive(ctx context.Context, reader client.Reader, lease *coordinationv1.Lease) (bool, error) {
	var run client.Object
	switch lease.Labels[labelLeaseHolderKind] {
	case backupv1alpha1.KindBackupRun:
		run = &backupv1alpha1.BackupRun{}
	case backupv1alpha1.KindRestoreRun:
		run = &backupv1alpha1.RestoreRun{}
	default:
		return true, nil
	}
	key := types.NamespacedName{Namespace: lease.Namespace, Name: lease.Annotations[annotationLeaseHolderName]}
	if err := reader.Get(ctx, key, run); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get %s %s: %w", lease.Labels[labelLeaseHolderKind], key, err)
	}
	return string(run.GetUID()) == holderUID(lease) && needsLease(run, lease), nil
}

// needsLease reports whether a run, read with the UID the Lease names,
// still needs the Lease (see holderLive).
func needsLease(run client.Object, lease *coordinationv1.Lease) bool {
	quiesceLease := lease.Labels[labelLeaseScope] == scopeQuiesce
	items := leaseItems(lease)
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		if r.Status.Phase.Finished() || quiesceLease {
			return !r.Status.Phase.Finished() && !durablyRestarted(r)
		}
		kind := backupv1alpha1.ItemKindSource
		if lease.Labels[labelLeaseScope] == scopeCluster {
			kind = backupv1alpha1.ItemKindCluster
		}
		return slices.ContainsFunc(r.Status.Items, func(item backupv1alpha1.BackupItem) bool {
			return item.Kind == kind && slices.Contains(items, item.Name) && backupItemOpen(item)
		})
	case *backupv1alpha1.RestoreRun:
		if r.Status.Phase.Finished() || quiesceLease {
			return !r.Status.Phase.Finished() && !durablyRestarted(r)
		}
		// A finished item that still records its restore Job's UID has a
		// Job the run has stopped and not yet seen stopped (rule X2), and a
		// pod of that Job may still write.
		return slices.ContainsFunc(r.Status.Items, func(item backupv1alpha1.RestoreItem) bool {
			return item.Kind == backupv1alpha1.ItemKindClaim && slices.Contains(items, item.Name) &&
				(!finished(item) || item.JobUID != "")
		})
	}
	return true
}
