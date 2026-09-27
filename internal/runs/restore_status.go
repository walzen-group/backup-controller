package runs

import (
	"fmt"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// anyRestorePending reports whether any item is still Pending, which means
// the run has not started it yet.
func anyRestorePending(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending {
			return true
		}
	}
	return false
}

// restoreDone reports whether every item is Succeeded, Failed or Skipped, so
// none has anything more to do.
func restoreDone(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if !finished(item) {
			return false
		}
	}
	return true
}

// restoreFailures returns one line per failed item, naming its kind, its name
// and its message, joined with "; ". It returns an empty string when no item
// failed.
func restoreFailures(items []backupv1alpha1.RestoreItem) string {
	var failed []string
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			failed = append(failed, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
		}
	}
	return strings.Join(failed, "; ")
}

// nothingRestored returns the Ready message of a run none of whose items
// succeeded, and "" when at least one did.
//
// Parameters:
//   - items are the run's finished items. The caller has ruled out a failed
//     one, so an item that did not succeed was Skipped.
//
// The message says nothing was restored and gives each item's kind, name and
// message, joined with "; ", so the user sees why each one was left alone.
// A run that restored nothing must not end Succeeded.
func nothingRestored(items []backupv1alpha1.RestoreItem) string {
	var skipped []string
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemSucceeded {
			return ""
		}
		skipped = append(skipped, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
	}
	return "nothing was restored: " + strings.Join(skipped, "; ")
}

// anyItem chooses every item, for stopJobs from finish and finalize.
func anyItem(backupv1alpha1.RestoreItem) bool { return true }

// finished reports whether an item is Succeeded, Failed or Skipped, so its
// restore Job has no more work to do.
func finished(item backupv1alpha1.RestoreItem) bool {
	switch item.Phase {
	case backupv1alpha1.ItemSucceeded, backupv1alpha1.ItemFailed, backupv1alpha1.ItemSkipped:
		return true
	case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
		return false
	}
	return false
}

// restoreItemDone reports whether the run's claim item named name has
// finished (see finished) and names no restore Job, or the run has no such
// item. An item keeps its Job's UID until Stop reports the Job stopped (see
// stopJobs), so until then the Job may still write, and the item keeps its
// Leases. Only a claim item counts, as in holderLive: a Cluster item takes
// no Lease, and one of the same name says nothing about the claim.
func restoreItemDone(run *backupv1alpha1.RestoreRun, name string) bool {
	for _, item := range run.Status.Items {
		if item.Kind == backupv1alpha1.ItemKindClaim && item.Name == name {
			return finished(item) && item.JobUID == ""
		}
	}
	return true
}

// finishedWithMover reports whether any finished item still records the UID
// of its restore Job, so that Job is not stopped yet.
func finishedWithMover(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if finished(item) && item.JobUID != "" {
			return true
		}
	}
	return false
}

// anyRestoreFailed reports whether any item is Failed. The run ends Failed
// when its items are all finished and one of them failed.
func anyRestoreFailed(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			return true
		}
	}
	return false
}

// anyRestoreSucceeded reports whether any item is Succeeded. A run that
// restored nothing must not end Succeeded, so a finished run without a
// Succeeded item ends with reason NoBackupInReach.
func anyRestoreSucceeded(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemSucceeded {
			return true
		}
	}
	return false
}
