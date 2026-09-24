// Package runs holds the one-shot operations: BackupRun takes a backup now,
// RestoreRun writes a chosen moment back, and the scheduler creates a
// BackupRun at each tick of a namespace's schedule. Whatever a run changes on
// the way, it puts back before it reports a terminal phase, and a finalizer
// means deleting the run cannot skip that.
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

// FieldOwner names this controller on the writes it makes.
const FieldOwner = client.FieldOwner("backup-controller")

// Finalizer holds a run open until the controller has put back whatever it
// changed: a stopped workload, a suspended Kustomization, a destination.
const Finalizer = "backup.wlz.li/run-cleanup"

// pollInterval is how often a waiting run looks again. The movers' progress is
// not watched, so this is the resolution of every wait in this package.
const pollInterval = 10 * time.Second

// after requeues after d, or returns err alone. controller-runtime ignores a
// requeue that comes with an error, and warns about the pair.
func after(d time.Duration, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{RequeueAfter: d}, nil
}

// TriggerFor returns the manual tag a run writes onto a source.
//
// It is derived from the run's UID, so a controller that restarts mid-run
// recomputes the same tag, finds its own work in progress and continues.
func TriggerFor(uid types.UID) string {
	return "backuprun-" + string(uid)
}

// manualTag reads the tag currently set on a source, empty when there is none.
func manualTag(source *volsyncv1alpha1.ReplicationSource) string {
	if source.Spec.Trigger == nil {
		return ""
	}
	return source.Spec.Trigger.Manual
}

// lastManual reads the tag the source last completed.
func lastManual(source *volsyncv1alpha1.ReplicationSource) string {
	if source.Status == nil {
		return ""
	}
	return source.Status.LastManualSync
}

// busy reports a source whose current tag has not completed yet.
//
// A completed tag stays on the source. VolSync syncs a source that has no
// trigger at all in a tight loop, and one whose manual tag equals the last tag
// it completed not at all, so a spent tag is how a source waits for the next
// run.
func busy(source *volsyncv1alpha1.ReplicationSource) bool {
	return manualTag(source) != "" && manualTag(source) != lastManual(source)
}

// savedSnapshot matches the line restic prints after a backup, which VolSync
// keeps in the mover's status logs.
var savedSnapshot = regexp.MustCompile(`snapshot ([0-9a-f]{8,64}) saved`)

// emptyDirectory is the line VolSync's mover prints when the volume holds no
// files. It then runs no backup, and the repository gains no snapshot.
const emptyDirectory = "Directory is empty skipping backup"

// moverOutcome reads what a finished mover logged: the snapshot it saved, or
// that the volume was empty.
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
