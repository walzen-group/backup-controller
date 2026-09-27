package runs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// inUse reports whether VolSync may still be working on a source: it holds a
// manual tag VolSync hasn't completed (see busy), or VolSync has recorded the
// start of a sync in status.lastSyncStartTime and not yet cleared it. The
// controller writes nothing onto such a source unless the tag is its own, and
// then it doesn't write either.
//
// A missing status or lastSyncStartTime is a legitimate state: no sync has
// started, or the last one finished. The field comes from VolSync's
// v1alpha1 Go type, and the controller reads a source only while the API
// server serves v1alpha1 (see volsyncUnsupported), whose schema VolSync keeps:
// a rename would be a new API version, which the run refuses instead.
func inUse(source *volsyncv1alpha1.ReplicationSource) bool {
	return busy(source) || (source.Status != nil && source.Status.LastSyncStartTime != nil)
}

// errSourceAbandoned is the error that ensureSource returns, wrapped in a
// sourceHeldError, when the ReplicationSource of the claim is busy with a
// tag that no run waits for. The item fails at once. A wait would not end,
// and VolSync would complete a new tag with the older sync.
var errSourceAbandoned = errors.New("the ReplicationSource is still retrying a backup no run waits for")

// sourceHeldError is the error that says why a run must not write the
// ReplicationSource of the claim when no run waits for its tag. It matches
// errSourceAbandoned, and asItemFailure fails the item with reason
// SourceAbandoned. Its message is for the message of the item.
type sourceHeldError struct {
	// message names the source and the run or the tag that holds it, and
	// says what a person can do.
	message string
}

// Error returns the message, which names the source and the run or tag that
// holds it.
func (e *sourceHeldError) Error() string { return e.message }

// Is makes errors.Is match errSourceAbandoned.
func (*sourceHeldError) Is(target error) bool {
	return target == errSourceAbandoned
}

// heldError carries a hold out of the mutate function of
// controllerutil.CreateOrUpdate. An error is the only way to stop the write
// from that function. ensureSource gets the hold back with errors.As, and no
// heldError goes out of ensureSource.
type heldError struct {
	// hold is the reason that the run must not write the source now.
	hold hold
}

// Error returns the text of the hold.
func (e *heldError) Error() string { return e.hold.text }

// holder returns why the run with the trigger tag must not write the
// ReplicationSource source, which is in use (see inUse) with another tag or
// with none.
//
// Parameters:
//   - reader lists the BackupRuns in the source's namespace. The caller
//     passes the uncached Reader: a run is created before it tags a source,
//     and a cache could lag behind that.
//   - source is the source as stored.
//
// It returns a hold of kind holdSourceBusy for a source whose open tag (see
// busy) belongs to a live run, and for a source that VolSync syncs with no
// open tag. The run then waits and tries again later. If the run wrote its
// own tag at that time, then the sync of VolSync would complete the new tag
// with a clone cut for the older tag. Also, a change to the spec of the
// mover would make VolSync replace the mover Job, which stops restic and
// leaves its lock in the repository.
//
// It returns a *sourceHeldError that matches errSourceAbandoned for a source
// whose open tag no live run holds, and the item fails. A tag is live
// when the namespace holds a BackupRun whose UID is the tag without its
// "backuprun-" prefix, which is not being deleted and not finished, and whose
// item for the claim is Pending or Running. Pending counts, because a run
// that wrote its tag and then lost its status write still has the item
// Pending. The tag's format is the same since v0.7.x, so tags written by
// those versions are judged the same way. A failed list comes back as a plain
// error, and the caller retries. No decision uses a failed list.
func holder(ctx context.Context, reader client.Reader, source *volsyncv1alpha1.ReplicationSource) (hold, error) {
	if !busy(source) {
		return sourceBusy("ReplicationSource %s is still syncing; VolSync has not recorded the end of its last sync", source.Name), nil
	}
	open := manualTag(source)
	runs := &backupv1alpha1.BackupRunList{}
	if err := reader.List(ctx, runs, client.InNamespace(source.Namespace)); err != nil {
		return hold{}, fmt.Errorf("list BackupRuns in %s: %w", source.Namespace, err)
	}
	owner := ""
	for i := range runs.Items {
		run := &runs.Items[i]
		if TriggerFor(run.UID) != open {
			continue
		}
		owner = run.Name
		if run.DeletionTimestamp != nil || run.Status.Phase.Finished() {
			break
		}
		for _, item := range run.Status.Items {
			if item.Kind == backupv1alpha1.ItemKindSource && item.Name == source.Name &&
				(item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning) {
				return sourceBusy("ReplicationSource %s is still completing the backup of BackupRun %s", source.Name, run.Name), nil
			}
		}
		break
	}
	return hold{}, &sourceHeldError{message: abandonedMessage(source, owner)}
}

// abandonedMessage returns the item's message for a source busy with a tag
// no run waits for. It says what is going on and what a person can do. owner
// is the name of the BackupRun the tag belongs to, or empty when no such run
// exists; the message then names the tag.
func abandonedMessage(source *volsyncv1alpha1.ReplicationSource, owner string) string {
	of := "the trigger " + manualTag(source)
	if owner != "" {
		of = "BackupRun " + owner
	}
	started, mover := syncState(source)
	return fmt.Sprintf("ReplicationSource %[1]s is still retrying the backup of %[2]s, which no run waits for any more. "+
		"%[3]s; a new trigger would be completed by that older backup, so this run leaves the source alone.%[4]s "+
		"Fix what the mover reports and VolSync finishes on its own. "+
		"To give that backup up, delete the ReplicationSource %[1]s while no pod of Job volsync-src-%[1]s is running; "+
		"the next backup unlocks the repository first.",
		source.Name, of, started, mover)
}

// syncState describes the sync VolSync runs or retries on a source, for a
// message that tells a person why a run waits or fails.
//
// Parameters:
//   - source is the ReplicationSource as stored. Its status says when VolSync
//     started the sync and how the last mover ended.
//
// It returns two parts of a message. The first says when VolSync started
// the sync, or that it has not started it yet, and has no final full stop.
// The second gives the last mover result and the last five lines of its log
// as a sentence that starts with a space, or is empty when VolSync has
// recorded no mover result.
func syncState(source *volsyncv1alpha1.ReplicationSource) (started, mover string) {
	started = "VolSync has not started that sync yet"
	if source.Status != nil && source.Status.LastSyncStartTime != nil {
		started = fmt.Sprintf("VolSync started it at %s and retries it with the clone it cut then until a mover succeeds",
			source.Status.LastSyncStartTime.UTC().Format(time.RFC3339))
	}
	if source.Status != nil && source.Status.LatestMoverStatus != nil {
		mover = fmt.Sprintf(" The last mover result VolSync recorded is %s: %s.",
			source.Status.LatestMoverStatus.Result, lastLines(source.Status.LatestMoverStatus.Logs, 5))
	}
	return started, mover
}

// lastLines returns the last n non-empty lines of logs, joined by " / ".
func lastLines(logs string, n int) string {
	var lines []string
	for _, line := range strings.Split(logs, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, " / ")
}
