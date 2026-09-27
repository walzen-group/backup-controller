package runs

import (
	"fmt"
	"slices"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// backupItemOpen reports whether an item is Pending or Running, so it still
// has something to do.
func backupItemOpen(item backupv1alpha1.BackupItem) bool {
	return item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning
}

// anyPending reports whether any item is still Pending, which means the run
// has not started it yet.
func anyPending(items []backupv1alpha1.BackupItem) bool {
	return slices.ContainsFunc(items, func(item backupv1alpha1.BackupItem) bool { return item.Phase == backupv1alpha1.ItemPending })
}

// allDone reports whether every item has left Pending and Running, so none
// has anything more to do.
func allDone(items []backupv1alpha1.BackupItem) bool {
	return !slices.ContainsFunc(items, backupItemOpen)
}

// anyFailed reports whether any item failed. The run decides its end on it:
// Failed when an item failed, and Succeeded otherwise.
func anyFailed(items []backupv1alpha1.BackupItem) bool {
	return slices.ContainsFunc(items, func(item backupv1alpha1.BackupItem) bool { return item.Phase == backupv1alpha1.ItemFailed })
}

// failures returns one line per failed item, naming its kind, its name and
// its message, joined with "; ". It returns an empty string when no item
// failed. It only makes the message. anyFailed decides the end.
func failures(items []backupv1alpha1.BackupItem) string {
	message := ""
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			if message != "" {
				message += "; "
			}
			message += fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message)
		}
	}
	return message
}

// summary returns the Ready message for a run that succeeded. It says for
// each item what it captured: the snapshot a volume saved, the Backup a
// database completed, or why the item was skipped or had nothing to save.
func summary(items []backupv1alpha1.BackupItem) string {
	message := ""
	for _, item := range items {
		if message != "" {
			message += "; "
		}
		switch {
		case item.Phase == backupv1alpha1.ItemSkipped:
			message += fmt.Sprintf("%s %s skipped: %s", item.Kind, item.Name, item.Message)
		case item.Snapshot != "":
			message += fmt.Sprintf("%s %s saved snapshot %s", item.Kind, item.Name, item.Snapshot)
		case item.Empty:
			message += fmt.Sprintf("%s %s was empty", item.Kind, item.Name)
		case item.Backup != "":
			message += fmt.Sprintf("%s %s completed Backup %s", item.Kind, item.Name, item.Backup)
		default:
			message += fmt.Sprintf("%s %s succeeded", item.Kind, item.Name)
		}
	}
	return message
}
