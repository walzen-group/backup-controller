package runs

import (
	"context"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
)

// startDatabase starts the backup of one Pending database item and sets the
// item's phase.
//
// Parameters:
//   - run is the admitted BackupRun, for its namespace and its UID.
//   - item is the Pending database item to start. It is changed in place,
//     and the caller writes the status.
//
// It returns an error for any failed read or write a later pass may not
// get, with the item left Pending. The caller, startPending, records that
// error in status.items[].lastStartError and tries the item again on its
// next pass.
//
// It creates a CloudNativePG Backup and moves the item to Running, or skips
// the item when the Cluster is hibernated, with reason ClusterHibernated.
// When the Cluster is gone, it fails the item with reason ClusterMissing.
// A Backup the API server rejects as invalid fails the item with reason
// BackupRefused.
func (r *BackupRunReconciler) startDatabase(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) error {
	cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
	switch {
	case err != nil:
		return err
	case !found:
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonClusterMissing, "the Cluster %s no longer exists", item.Name))
	case cnpg.Hibernated(cluster):
		item.Phase, item.Reason = backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonClusterHibernated
		item.Message = "the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"
	default:
		name, err := cnpg.EnsureBackup(ctx, r.Client, run.Namespace, item.Name, run.UID)
		if apierrors.IsInvalid(err) {
			failBackupItem(item, refuse(backupv1alpha1.ItemReasonBackupRefused, "%v", err))
			return nil
		}
		if err != nil {
			return err
		}
		item.Phase, item.Backup = backupv1alpha1.ItemRunning, name
	}
	return nil
}

// collectDatabase records the result of a Running database item once its
// CloudNativePG Backup has one (see cnpg.BackupResult).
//
// Parameters:
//   - run is the BackupRun, for its namespace.
//   - item is the Running database item, which names its Backup. It is
//     changed in place.
//
// It returns a sentence for the run's Ready message while the Backup waits
// in a phase that needs naming, such as one CloudNativePG 1.30 does not
// have, and "" otherwise.
//
// While the Backup runs, the item's message is that sentence, so it names
// the phase on the item itself; the SourceBusy message leaves it out (see
// work). A completed Backup makes the item Succeeded with no message, and a
// failed one makes it Failed with reason BackupFailed and CloudNativePG's
// error. A failed read
// leaves the item as it was, for the next pass.
func (r *BackupRunReconciler) collectDatabase(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) string {
	outcome, err := cnpg.BackupResult(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Backup)
	if err != nil {
		return ""
	}
	switch outcome.Phase {
	case cnpg.BackupWaiting, cnpg.BackupUnknownPhase:
		item.Message = outcome.Message
		return outcome.Message
	case cnpg.BackupCompleted:
		item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
	case cnpg.BackupFailed:
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonBackupFailed, "%s", outcome.Message))
	}
	return ""
}

// runningNote returns a sentence for the message of a Running item that
// abort or timeOut fails, and "" when there is nothing to add.
//
// Parameters:
//   - run is the BackupRun that ends.
//   - item is the Running item.
//
// For a volume item that recorded its snapshot and has not moved it yet,
// it returns the sentence of unmovedNote. For any other volume item it
// returns what syncGoesOn says of the sync VolSync goes on with. A database
// item can have a Backup in the phase cnpg.BackupUnknownPhase. For that item,
// it returns the message of cnpg.BackupResult. Thus a run that gets to its
// timeout shows the phase that it waited in. A failed read gives "", since the
// sentence only explains the item's failure.
func (r *BackupRunReconciler) runningNote(ctx context.Context, run *backupv1alpha1.BackupRun, item backupv1alpha1.BackupItem) string {
	if item.Kind != backupv1alpha1.ItemKindCluster {
		if item.SnapshotID != "" {
			return unmovedNote(item)
		}
		return r.syncGoesOn(ctx, run, item, nil)
	}
	outcome, err := cnpg.BackupResult(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Backup)
	if err != nil || outcome.Phase != cnpg.BackupUnknownPhase {
		return ""
	}
	return outcome.Message
}
