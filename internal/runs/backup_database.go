package runs

import (
	"context"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
)

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
// failed one makes it Failed with CloudNativePG's error. A failed read
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
		item.Phase, item.Message = backupv1alpha1.ItemFailed, outcome.Message
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
	if item.Kind != "Cluster" {
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
