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
	"fmt"
	"regexp"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FieldOwner is the field manager name, backupv1alpha1.FieldManager, that
// the run controllers send with their patches to workloads and
// Kustomizations.
const FieldOwner = client.FieldOwner(backupv1alpha1.FieldManager)

// Finalizer keeps a deleted run in place until the controller has put back
// whatever the run changed: a stopped workload, a suspended Kustomization, or
// a ReplicationDestination it created.
const Finalizer = "backup.wlz.li/run-cleanup"

// pollInterval is how long a waiting run waits before it checks again. The
// controller doesn't watch the movers' progress, so this interval sets how
// soon a run notices that a mover has finished.
const pollInterval = 10 * time.Second

// after returns a result that requeues the run once the duration d has
// passed. When err is set, it returns only err, with an empty result, because
// controller-runtime ignores a requeue that comes with an error and logs a
// warning about the pair.
func after(d time.Duration, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: d}, nil
}

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

// savedSnapshot matches the line restic prints after a backup, such as
// "snapshot da4d7eb4 saved", and captures the snapshot ID. VolSync keeps that
// line in the mover's status logs.
var savedSnapshot = regexp.MustCompile(`snapshot ([0-9a-f]{8,64}) saved`)

// emptyDirectory is the line VolSync's mover prints when the volume holds no
// files. The mover then runs no backup, and the repository gains no snapshot.
const emptyDirectory = "Directory is empty skipping backup"

// moverOutcome reads the logs of a finished mover from the source's
// status.latestMoverStatus. It returns the ID of the snapshot the mover
// saved, as restic printed it, or true in empty when the mover found the
// volume empty. When the logs show neither, it returns an empty ID and false.
func moverOutcome(source *volsyncv1alpha1.ReplicationSource) (snapshot string, empty bool) {
	if source.Status == nil || source.Status.LatestMoverStatus == nil {
		return "", false
	}
	logs := source.Status.LatestMoverStatus.Logs
	if match := savedSnapshot.FindStringSubmatch(logs); match != nil {
		return match[1], false
	}
	return "", strings.Contains(logs, emptyDirectory)
}

// alreadyLocked is what restic prints when it can't take its lock because
// another lock is in the repository, as in "unable to create lock in
// backend: repository is already locked by PID 33 on
// volsync-src-notes-data-7xk2p" (restic 0.18.1 internal/restic/lock.go:56,
// recorded in internal/restic/testdata/recorded/restic-0.18.1/killed-mover).
// VolSync keeps every line of a failed mover's logs (mover.go:612).
const alreadyLocked = "repository is already locked"

// moverFailure returns the item's message for a mover of source that failed
// with the given logs. When the logs show that restic found the repository
// locked, the message first says what that lock is and how it is cleared,
// and then gives the logs. Otherwise it is the logs as they are.
//
// A mover killed mid-backup leaves its lock, and restic's forget, which the
// mover runs after the backup, refuses any other lock. `restic unlock`
// removes a lock of another host only once it is older than 30 minutes. The
// controller writes spec.restic.unlock with every new trigger, so the next
// backup of the claim runs it first. The retries of this sync run it too
// while spec.restic.unlock differs from status.restic.lastUnlocked, which is
// the case for a sync the controller triggered.
func moverFailure(source *volsyncv1alpha1.ReplicationSource, logs string) string {
	if !strings.Contains(logs, alreadyLocked) {
		return logs
	}
	next := "The next backup does so by itself once the lock is older than 30 minutes."
	if source.Spec.Restic != nil && source.Spec.Restic.Unlock != "" &&
		(source.Status == nil || source.Status.Restic == nil || source.Status.Restic.LastUnlocked != source.Spec.Restic.Unlock) {
		next = "VolSync's retries of this backup and the next backup do so by themselves once the lock is older than 30 minutes."
	}
	return fmt.Sprintf("The restic repository is locked (%s): a lock that another restic process holds or left behind, "+
		"such as one of a mover that was killed, keeps restic from taking the lock it needs. "+
		"`restic unlock` clears a stale lock, and restic counts a lock from another host as stale once it is older than 30 minutes. "+
		"%s Mover logs: %s", alreadyLocked, next, logs)
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
