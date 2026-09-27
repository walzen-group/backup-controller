package runs

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// syncSkew is how far the clock of the node a mover runs on may differ from
// the clock of VolSync's manager. restic stamps a snapshot with the mover's
// clock, and VolSync records the sync's times with its own, so the window a
// backup looks for its snapshot in is this much wider on each side.
const syncSkew = 5 * time.Second

// syncWindow is the span of time in which restic stamped every snapshot one
// VolSync sync wrote, both ends included.
type syncWindow struct {
	// start is the earliest time such a snapshot can carry.
	start time.Time
	// end is the latest time such a snapshot can carry.
	end time.Time
}

// holds reports whether a snapshot stamped at the given time falls in the
// window, counting both ends.
func (w syncWindow) holds(at time.Time) bool {
	return !at.Before(w.start) && !at.After(w.end)
}

// unidentified returns the refusal of a completed sync that left no record
// of when it ran, so the run can't tell which snapshot in the repository the
// sync wrote. The item fails with reason NoMoverSnapshot; no retry brings
// the missing time back.
//
// Parameters:
//   - source is the ReplicationSource whose status lacks the time.
//   - problem says which status field is missing or unusable.
func unidentified(source *volsyncv1alpha1.ReplicationSource, problem string) error {
	return refuse(backupv1alpha1.ItemReasonNoMoverSnapshot, "ReplicationSource %s completed the run's sync but %s, so VolSync did not record when the sync ran "+
		"and the run can't tell which snapshot in the repository the sync wrote", source.Name, problem)
}

// windowOf returns the window in which restic stamped the snapshots of the
// sync a source completed last.
//
// Parameters:
//   - source is the ReplicationSource whose status records the completed
//     sync. The caller has checked that its lastManualSync is the run's
//     trigger, so the sync is the run's.
//
// It returns the refusal from unidentified when the status lacks lastSyncTime or
// lastSyncDuration, or records a negative duration.
//
// VolSync 0.16.0 sets lastSyncTime when the sync completes, and
// lastSyncDuration from the same clock as the time since lastSyncStartTime,
// which it then clears (statemachine/machine.go 196-219). restic stamps each
// snapshot when its backup starts, inside that span. The status keeps
// lastSyncTime in whole seconds, so the sync ended before one more second
// had passed. The window is therefore [lastSyncTime - lastSyncDuration -
// syncSkew, lastSyncTime + 1s + syncSkew].
func windowOf(source *volsyncv1alpha1.ReplicationSource) (syncWindow, error) {
	status := source.Status
	switch {
	case status == nil || status.LastSyncTime == nil:
		return syncWindow{}, unidentified(source, "has no status.lastSyncTime")
	case status.LastSyncDuration == nil:
		return syncWindow{}, unidentified(source, "has no status.lastSyncDuration")
	case status.LastSyncDuration.Duration < 0:
		return syncWindow{}, unidentified(source, fmt.Sprintf("records status.lastSyncDuration as %s, which no sync can take", status.LastSyncDuration.Duration))
	}
	end := status.LastSyncTime.Time
	start := end.Add(-status.LastSyncDuration.Duration)
	return syncWindow{start: start.Add(-syncSkew), end: end.Add(time.Second + syncSkew)}, nil
}

// identified is what identify found of the snapshots one sync wrote.
type identified struct {
	// snapshot is the snapshot the sync completed with. It is the zero
	// Snapshot when found is false.
	snapshot restic.Snapshot
	// found is false when the sync wrote no snapshot.
	found bool
	// others are the sync's other snapshots, newest first: those of mover
	// pods that saved a snapshot and then failed, before the retry that
	// succeeded.
	others []restic.Snapshot
}

// identify picks the snapshot a sync wrote from a repository's snapshots.
//
// Parameters:
//   - window is the sync's window (see windowOf).
//   - snapshots are the repository's snapshots, in any order.
//   - recorded holds the full IDs that other BackupRuns' items of the same
//     claim recorded. Such a snapshot is theirs, so it is never this sync's.
//
// It returns the newest candidate, by time and then by ID, as the sync's
// snapshot, and the other candidates beside it. It returns found false when
// there is no candidate. A candidate is a snapshot a mover wrote (see
// restic.MoverWritten: host volsync, the one path /data, no original) whose
// time is in the window and whose ID is not in recorded.
//
// VolSync retries a mover pod that fails inside one sync, and a pod that
// saved its snapshot and then failed leaves it, so one sync can write
// several snapshots. The last one is the attempt that succeeded, and restic
// stamps each attempt when it starts, so the newest is that one. The ID
// breaks a tie the same way restic.Repository.Snapshots orders one.
func identify(window syncWindow, snapshots []restic.Snapshot, recorded map[string]bool) identified {
	var candidates []restic.Snapshot
	for _, s := range snapshots {
		if restic.MoverWritten(s) && window.holds(s.Time) && !recorded[s.ID] {
			candidates = append(candidates, s)
		}
	}
	if len(candidates) == 0 {
		return identified{}
	}
	slices.SortFunc(candidates, func(a, b restic.Snapshot) int {
		if c := b.Time.Compare(a.Time); c != 0 {
			return c
		}
		return cmp.Compare(b.ID, a.ID)
	})
	return identified{snapshot: candidates[0], found: true, others: candidates[1:]}
}

// note returns the sentence an item's message carries when the sync left
// other snapshots beside the one it completed with, and "" when it left
// none. It is for a person; no decision reads it.
func (found identified) note() string {
	if len(found.others) == 0 {
		return ""
	}
	ids := make([]string, 0, len(found.others))
	for _, s := range found.others {
		ids = append(ids, s.ShortID())
	}
	noun := "snapshot"
	if len(ids) > 1 {
		noun = "snapshots"
	}
	return fmt.Sprintf("the sync also left %s %s from failed attempts; they stay in the repository until retention forgets them",
		noun, strings.Join(ids, ", "))
}

// relistAfter is how long a volume item waits after a listing that found no
// snapshot of its sync before a listing that finds none makes the claim
// empty. It is pollInterval plus one second, because the status keeps the
// time of the first listing in whole seconds.
const relistAfter = pollInterval + time.Second

// noSnapshotListed records one listing of a claim's repository that found no
// snapshot the item's completed sync wrote, and succeeds the item as an
// empty claim once two such listings lie far enough apart.
//
// Parameters:
//   - item is the Running volume item whose sync completed. It is changed
//     in place.
//   - now is the time of this pass, which the first such listing records
//     in status.items[].noSnapshotListedAt.
//
// VolSync's mover takes no snapshot of a volume that holds nothing but
// lost+found and still reports success, so a sync with no snapshot in its
// window backed up an empty claim. An S3 listing can lag the mover's write,
// though, so one listing that shows no snapshot proves nothing. The first
// one records its time and keeps the item Running. A later pass lists the
// repository again (see identifySnapshot), and when that listing, at least
// relistAfter after the first, still shows no snapshot, the item succeeds
// with Empty set. A pass sooner than that leaves the item as it is.
func noSnapshotListed(item *backupv1alpha1.BackupItem, now time.Time) {
	listed := item.NoSnapshotListedAt
	switch {
	case listed == nil:
		item.NoSnapshotListedAt = newTime(metav1.NewTime(now).Rfc3339Copy())
		item.Message = fmt.Sprintf("the repository listed no snapshot of the sync at %s; the run lists it again after %s "+
			"before it takes the volume for empty", now.UTC().Format(time.RFC3339), relistAfter)
	case now.Before(listed.Add(relistAfter)):
		// The first listing is too recent for a second to count.
	default:
		item.Phase, item.Empty = backupv1alpha1.ItemSucceeded, true
		item.Message = "the volume held no files, so VolSync took no snapshot"
	}
}

// unmovedNote returns the sentence for the message of a volume item that
// fails with its snapshot recorded but not yet moved to the restart moment,
// as when the run times out while the item waits for the restart. It is for
// a person; no decision reads it.
//
// Parameters:
//   - item is the volume item. Its snapshot field holds the short ID the
//     item recorded when it found the snapshot its sync wrote.
//
// The snapshot stays in the repository with the time restic gave it and
// without the quiesced tag, so a restore with syncDatabaseToVolume does not
// pick it.
func unmovedNote(item backupv1alpha1.BackupItem) string {
	return fmt.Sprintf("snapshot %s is saved but was not moved to the restart moment or tagged %s", item.Snapshot, restic.QuiescedTag)
}

// identifySnapshot records the snapshot a completed sync wrote, found in the
// repository, and succeeds the item when the run need not move it.
//
// Parameters:
//   - run is the BackupRun, for its namespace and whether it stopped its
//     workloads.
//   - item is the Running volume item whose sync completed. It is changed
//     in place.
//   - source is the item's ReplicationSource, whose status records when the
//     sync ran.
//
// The window comes from the source's status (see windowOf); a status that
// lacks it fails the item with reason NoMoverSnapshot. The snapshot is the
// newest a mover wrote in that window (see identify), and the item records
// its full ID, its short ID and its time, and names the sync's other
// snapshots in its message. A listing with no such snapshot counts toward an
// empty claim, which takes two listings a poll interval apart (see
// noSnapshotListed). A failed listing leaves the item Running with the error
// in its message, and the next pass tries again until the run's timeout.
//
// The item of a run that stopped its workloads stays Running with the
// snapshot recorded, so the status holds the original's full ID before any
// rewrite. A crash after the rewrite then finds the rewritten copy through
// that ID, where a new search would find no snapshot and take the claim for
// empty.
func (r *BackupRunReconciler) identifySnapshot(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem, source *volsyncv1alpha1.ReplicationSource) {
	window, err := windowOf(source)
	if err != nil {
		failBackupItem(item, err)
		return
	}
	found, err := r.findSnapshot(ctx, run, item.Name, window)
	if err != nil {
		item.Message = fmt.Sprintf("the run could not list the repository for the snapshot its sync wrote: %v", err)
		return
	}
	if !found.found {
		noSnapshotListed(item, r.Now())
		return
	}
	s := found.snapshot
	item.NoSnapshotListedAt = nil
	item.SnapshotID, item.Snapshot, item.SnapshotTime = s.ID, s.ShortID(), newTime(metav1.NewTime(s.Time))
	item.Message = found.note()
	if !stoppedWorkloads(run) {
		item.Phase = backupv1alpha1.ItemSucceeded
	}
}

// findSnapshot lists the repository of a claim and picks the snapshot one
// sync of it wrote.
//
// Parameters:
//   - run is the BackupRun, for its namespace and its UID.
//   - claimName names the claim whose repository is listed.
//   - window is the sync's window (see windowOf).
//
// It returns what identify found. It returns an error when the repository Secret, the repository or
// the namespace's BackupRuns can't be read.
func (r *BackupRunReconciler) findSnapshot(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string, window syncWindow) (identified, error) {
	secret, err := r.repositorySecret(ctx, run.Namespace, claimName)
	if err != nil {
		return identified{}, err
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return identified{}, err
	}
	recorded, err := r.recordedSnapshots(ctx, run, claimName)
	if err != nil {
		return identified{}, err
	}
	return identify(window, snapshots, recorded), nil
}

// recordedSnapshots returns the full snapshot IDs that other BackupRuns in
// the run's namespace recorded for items of the same claim, as a set.
//
// Parameters:
//   - run is the BackupRun that looks for its snapshot; its own items are
//     left out.
//   - claimName names the claim.
//
// It reads the BackupRuns through the uncached Reader, so a run that
// recorded its snapshot a moment ago is seen. It returns an error when the
// list fails.
func (r *BackupRunReconciler) recordedSnapshots(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string) (map[string]bool, error) {
	runs := &backupv1alpha1.BackupRunList{}
	if err := r.Reader.List(ctx, runs, client.InNamespace(run.Namespace)); err != nil {
		return nil, fmt.Errorf("list BackupRuns in %s: %w", run.Namespace, err)
	}
	recorded := map[string]bool{}
	for _, other := range runs.Items {
		if other.UID == run.UID {
			continue
		}
		for _, item := range other.Status.Items {
			if item.Kind == backupv1alpha1.ItemKindSource && item.Name == claimName && item.SnapshotID != "" {
				recorded[item.SnapshotID] = true
			}
		}
	}
	return recorded, nil
}

// retimeRecorded moves the recorded snapshot of a run that stopped its
// workloads to the run's restart moment, tags it quiesced, and succeeds the
// item.
//
// Parameters:
//   - run is the BackupRun, for its namespace, its record of the workloads
//     it stopped, and status.restartedAt.
//   - item is the Running volume item, whose snapshotID an earlier pass
//     recorded. It is changed in place.
//
// Every volume item of such a run cut its clone while the workloads were
// stopped, because the run starts them again only once each clone is cut or
// its item has failed (see clonesCut and giveUpUncut). The item therefore
// waits in Running while status.restartedAt is unset, as it is while another
// item's clone is not cut yet, and is moved once the restart moment is
// recorded. The item then records the rewritten snapshot's IDs and time and
// succeeds. When the rewrite fails, for example because another process
// holds a lock on the repository, the item stays Running with the error in
// its message and the next pass tries again, so the run never reports
// success for a snapshot a synced restore can't use. A run that stopped no
// workload succeeds the item with the snapshot as it was recorded.
func (r *BackupRunReconciler) retimeRecorded(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	if !stoppedWorkloads(run) {
		item.Phase = backupv1alpha1.ItemSucceeded
		return
	}
	if run.Status.RestartedAt == nil {
		item.Message = fmt.Sprintf("snapshot %s is saved and waits for the workloads to start again, to be moved to that moment and tagged %s",
			item.Snapshot, restic.QuiescedTag)
		return
	}
	moved, err := r.retime(ctx, run.Namespace, item.Name, item.SnapshotID, run.Status.RestartedAt.Time)
	if err != nil {
		item.Message = fmt.Sprintf("snapshot %s is saved and waits to be moved to %s and tagged %s: %v",
			item.Snapshot, run.Status.RestartedAt.UTC().Format(time.RFC3339), restic.QuiescedTag, err)
		return
	}
	item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
	item.SnapshotID, item.Snapshot, item.SnapshotTime = moved.ID, moved.ShortID(), newTime(metav1.NewTime(moved.Time))
}

// stoppedWorkloads reports whether the run stopped at least one workload, as
// its status.quiesced records. Only such a run has a moment when nothing
// wrote to the volumes or the databases: its restart moment, which is what
// its snapshots are moved to.
func stoppedWorkloads(run *backupv1alpha1.BackupRun) bool {
	return run.Spec.All && len(run.Status.Quiesced) > 0
}

// retime moves a snapshot a mover saved to a new time and tags it quiesced,
// so a RestoreRun with syncDatabaseToVolume can find it.
//
// Parameters:
//   - namespace and claimName name the claim that was backed up. retime reads
//     the repository Secret through the claim's VolumeRestore.
//   - id is the snapshot's full ID, as the item recorded it when the run
//     found the snapshot in the repository.
//   - at is the time the snapshot should carry. The caller passes the run's
//     status.restartedAt.
//
// It returns the rewritten snapshot, which has a new ID. It returns an error
// when no ID is given, when the Secret can't be read, and when the rewrite
// fails. A *restic.LockedError means another process holds a lock on the
// repository, and the caller tries again on its next pass. A snapshot an
// earlier call already rewrote comes back as that copy (see
// restic.Repository.Retime).
func (r *BackupRunReconciler) retime(ctx context.Context, namespace, claimName, id string, at time.Time) (restic.Snapshot, error) {
	if id == "" {
		return restic.Snapshot{}, errors.New("no snapshot is recorded to move")
	}
	secret, err := r.repositorySecret(ctx, namespace, claimName)
	if err != nil {
		return restic.Snapshot{}, err
	}
	return r.Retimer.Retime(ctx, secret, id, at, restic.QuiescedTag)
}

// repositorySecret reads the Secret holding the restic repository settings
// for the claim named claimName. It finds the Secret's name in
// spec.repository of the claim's VolumeRestore.
func (r *BackupRunReconciler) repositorySecret(ctx context.Context, namespace, claimName string) (*corev1.Secret, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claimName}, claim); err != nil {
		return nil, err
	}
	vr, err := volumeRestoreFor(ctx, r.Reader, claim)
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: vr.Spec.Repository}, secret); err != nil {
		return nil, fmt.Errorf("get Secret %s: %w", vr.Spec.Repository, err)
	}
	return secret, nil
}
