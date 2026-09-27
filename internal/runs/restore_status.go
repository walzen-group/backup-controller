package runs

import (
	"fmt"
	"slices"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// anyItemIn reports whether any item is in the given phase.
//
// Parameters:
//   - items are the run's items.
//   - phase is the phase to look for: Pending for an item the run has not
//     started, Failed for a run that ends Failed, and Succeeded for a run
//     that restored something.
func anyItemIn(items []backupv1alpha1.RestoreItem, phase backupv1alpha1.ItemPhase) bool {
	return slices.ContainsFunc(items, func(item backupv1alpha1.RestoreItem) bool { return item.Phase == phase })
}

// restoreDone reports whether every item is Succeeded, Failed or Skipped, so
// none has anything more to do.
func restoreDone(items []backupv1alpha1.RestoreItem) bool {
	return !slices.ContainsFunc(items, func(item backupv1alpha1.RestoreItem) bool { return !finished(item) })
}

// itemLines returns one line per item in the given phase, naming its kind,
// its name and its message, joined with "; ". It returns an empty string
// when no item is in that phase.
func itemLines(items []backupv1alpha1.RestoreItem, phase backupv1alpha1.ItemPhase) string {
	var lines []string
	for _, item := range items {
		if item.Phase == phase {
			lines = append(lines, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
		}
	}
	return strings.Join(lines, "; ")
}

// restoreFailures returns one line per failed item (see itemLines).
func restoreFailures(items []backupv1alpha1.RestoreItem) string {
	return itemLines(items, backupv1alpha1.ItemFailed)
}

// nothingRestored returns the Ready message of a run whose items all
// finished without one that succeeded or failed.
//
// Parameters:
//   - items are the run's finished items, all Skipped.
//
// The message says nothing was restored and gives each item's kind, name and
// message, so the user sees why each one was left alone. A run that restored
// nothing must not end Succeeded.
func nothingRestored(items []backupv1alpha1.RestoreItem) string {
	return "nothing was restored: " + itemLines(items, backupv1alpha1.ItemSkipped)
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
	return slices.ContainsFunc(items, func(item backupv1alpha1.RestoreItem) bool { return finished(item) && item.JobUID != "" })
}

// endReason returns the Ready reason of a restore that ends with a Failed
// item.
//
// Parameters:
//   - items are the run's items, all finished.
//
// It returns ReasonClusterVersionUnsupported when a Failed item records
// reason ClusterVersionUnsupported (see failBlindClusters), and
// ReasonFailed otherwise. It is the one decision that reads an item's
// reason: the reason is a typed field, and the run ends with it in
// whichever pass the run ends, also a pass after the one that failed the
// item.
func endReason(items []backupv1alpha1.RestoreItem) string {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed && item.Reason == backupv1alpha1.ItemReasonClusterVersionUnsupported {
			return backupv1alpha1.ReasonClusterVersionUnsupported
		}
	}
	return backupv1alpha1.ReasonFailed
}
