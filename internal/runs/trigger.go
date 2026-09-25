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
	"regexp"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FieldOwner is the field manager name, backup-controller, that the run
// controllers send with their patches to workloads and Kustomizations.
const FieldOwner = client.FieldOwner("backup-controller")

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
