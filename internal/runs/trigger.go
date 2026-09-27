// Package runs holds the reconcilers for the one-shot operations. A BackupRun
// takes a backup now, and a RestoreRun writes the data from a chosen moment
// back. The Scheduler creates a BackupRun at each tick of a namespace's
// schedule.
//
// A run puts back everything it changed on the way, such as stopped workloads
// and suspended Kustomizations, before it reports a terminal phase. A
// finalizer keeps a deleted run in place until it has done that.
package runs

import (
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TriggerFor returns the manual trigger tag that a BackupRun writes onto a
// ReplicationSource: "backuprun-" followed by the run's UID.
//
// Because the tag comes from the UID, a controller that restarts mid-run
// computes the same tag again, finds its own work in progress and continues.
func TriggerFor(uid types.UID) string {
	return "backuprun-" + string(uid)
}

// manualTag returns the tag in the source's spec.trigger.manual, or an empty
// string when the source has none.
func manualTag(source *volsyncv1alpha1.ReplicationSource) string {
	if source.Spec.Trigger == nil {
		return ""
	}
	return source.Spec.Trigger.Manual
}

// lastManual returns the source's status.lastManualSync, the last manual tag
// VolSync completed on it. It returns an empty string when the source has no
// status yet.
// That is a legitimate state, since VolSync writes the field only when a
// manual sync completes, and it fails closed: a run waits for its own tag in
// lastManualSync, up to its timeout, and busy counts a tag that is not there
// as one VolSync has not completed.
func lastManual(source *volsyncv1alpha1.ReplicationSource) string {
	if source.Status == nil {
		return ""
	}
	return source.Status.LastManualSync
}

// busy reports whether a source has a manual tag that VolSync hasn't
// completed yet.
//
// A completed tag stays on the source. VolSync syncs a source that has no
// trigger at all in a tight loop, and it doesn't sync a source whose manual
// tag equals the last tag it completed. So a spent tag is how a source waits
// for the next run.
func busy(source *volsyncv1alpha1.ReplicationSource) bool {
	return manualTag(source) != "" && manualTag(source) != lastManual(source)
}

// moverFailed reports whether a mover Job failed in the sync of a run's
// trigger, and returns the mover's logs when it did.
//
// Parameters:
//   - trigger is the item's trigger tag.
//   - started is the run's status.startedAt. The run writes its trigger
//     after that moment, so VolSync starts the sync of that trigger after it
//     too.
//
// VolSync writes status.latestMoverStatus only when a mover Job ends, and it
// never clears it. A sync completes only after a Job succeeds, so when the
// previous sync completed, the status held a Successful result. A Failed
// result counts only while the source still carries the trigger, has not
// completed it, and is in a sync that VolSync started at or after the run's
// start, which it records in status.lastSyncStartTime. A Failed result
// written before that sync began belongs to an earlier one.
func moverFailed(source *volsyncv1alpha1.ReplicationSource, trigger string, started *metav1.Time) (string, bool) {
	if manualTag(source) != trigger || lastManual(source) == trigger || source.Status == nil || started == nil {
		return "", false
	}
	status := source.Status
	if status.LatestMoverStatus == nil || status.LatestMoverStatus.Result != volsyncv1alpha1.MoverResultFailed {
		return "", false
	}
	if status.LastSyncStartTime == nil || status.LastSyncStartTime.Before(started) {
		return "", false
	}
	return status.LatestMoverStatus.Logs, true
}
