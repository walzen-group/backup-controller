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
// X2, see stopJobs). Only an item of the kind that takes Leases counts, a
// BackupRun's ReplicationSource item or a RestoreRun's PersistentVolumeClaim
// item: a Lease names its items by name alone, and a Cluster item with the
// claim's name does not keep the claim's Lease. A quiesce Lease is live
// while the holder's stored status does not show the workloads given back
// (see durablyRestarted): the holder exists with the UID the Lease names, has
// not finished, and has no plan yet or has not recorded its restart as done.
// A run being deleted counts as live until its finalizer has released the
// Lease. A Lease that names no run kind this controller knows is left alone,
// and counts as live.
func holderLive(ctx context.Context, reader client.Reader, lease *coordinationv1.Lease) (bool, error) {
	key := types.NamespacedName{Namespace: lease.Namespace, Name: lease.Annotations[annotationLeaseHolderName]}
	switch lease.Labels[labelLeaseHolderKind] {
	case backupv1alpha1.KindBackupRun:
		return backupHolderLive(ctx, reader, key, lease)
	case backupv1alpha1.KindRestoreRun:
		return restoreHolderLive(ctx, reader, key, lease)
	}
	return true, nil
}

// backupHolderLive is holderLive for a Lease that a BackupRun holds.
//
// Parameters:
//   - reader reads the BackupRun, uncached.
//   - key is the namespace and the name of the BackupRun that the Lease
//     names.
//   - lease is the Lease as stored.
//
// It returns true while the BackupRun needs the Lease, and an error when
// the read of the BackupRun fails for a reason other than NotFound.
func backupHolderLive(ctx context.Context, reader client.Reader, key types.NamespacedName, lease *coordinationv1.Lease) (bool, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := reader.Get(ctx, key, run); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get BackupRun %s: %w", key, err)
	}
	if string(run.UID) != holderUID(lease) || run.Status.Phase.Finished() {
		return false, nil
	}
	if lease.Labels[labelLeaseScope] == scopeQuiesce {
		return !durablyRestarted(run), nil
	}
	items := leaseItems(lease)
	for _, item := range run.Status.Items {
		if item.Kind == backupv1alpha1.ItemKindSource && slices.Contains(items, item.Name) &&
			(item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning) {
			return true, nil
		}
	}
	return false, nil
}

// restoreHolderLive is holderLive for a Lease that a RestoreRun holds.
//
// Parameters:
//   - reader reads the RestoreRun, uncached.
//   - key is the namespace and the name of the RestoreRun that the Lease
//     names.
//   - lease is the Lease as stored.
//
// It returns true while the RestoreRun needs the Lease, and an error when
// the read of the RestoreRun fails for a reason other than NotFound.
func restoreHolderLive(ctx context.Context, reader client.Reader, key types.NamespacedName, lease *coordinationv1.Lease) (bool, error) {
	run := &backupv1alpha1.RestoreRun{}
	if err := reader.Get(ctx, key, run); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("get RestoreRun %s: %w", key, err)
	}
	if string(run.UID) != holderUID(lease) || run.Status.Phase.Finished() {
		return false, nil
	}
	if lease.Labels[labelLeaseScope] == scopeQuiesce {
		return !durablyRestarted(run), nil
	}
	items := leaseItems(lease)
	for _, item := range run.Status.Items {
		// A finished item that still records its restore Job's UID has
		// a Job the run has stopped and not yet seen stopped (rule X2),
		// and a pod of that Job may still write.
		if item.Kind == backupv1alpha1.ItemKindClaim && slices.Contains(items, item.Name) &&
			(!finished(item) || item.JobUID != "") {
			return true, nil
		}
	}
	return false, nil
}
