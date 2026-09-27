package runs

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// startItem starts the backup of one Pending item and sets the item's phase.
//
// Parameters:
//   - run is the admitted BackupRun, for its namespace, its UID and the
//     trigger tag built from it.
//   - item is the Pending item to start. It is changed in place, and the
//     caller writes the status.
//
// It returns a message for the run's Ready condition when the item has to
// wait, and an empty string otherwise. It returns an error for any failed
// read or write a later pass may not get, such as a timeout from the API
// server or a 500 from a webhook it cannot reach, with the item left
// Pending. The caller, startPending, records that error in
// status.items[].lastStartError and tries the item again on its next pass.
//
// For a volume, it writes the claim's ReplicationSource with the run's manual
// trigger tag and moves the item to Running. For a database, it creates a
// CloudNativePG Backup and moves the item to Running, or skips the item when
// the Cluster is hibernated, with reason ClusterHibernated. When the claim or
// the Cluster is gone, or the claim's settings are refused, startItem fails
// the item with the refusal (see failBackupItem), which records the item's
// reason and says why in its message. The same goes for a Backup the API
// server rejects as invalid.
//
// An item waits when the volume's ReplicationSource is still completing the
// backup of another run that waits for it, or when a RestoreRun's mover
// works on the claim or its repository (see otherMover). The message names
// that run, and the item stays Pending. When the source is busy with a
// backup no run waits for (see holder), the item fails at once with a
// message that says what a person can do, and the source is left alone.
func (r *BackupRunReconciler) startItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) (string, error) {
	switch item.Kind {
	case "ReplicationSource":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, claim); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", fmt.Errorf("get claim %s: %w", item.Name, err)
			}
			failBackupItem(item, claimGone(item.Name))
			return "", nil
		}
		tag := TriggerFor(run.UID)
		_, busy, err := ensureSource(ctx, r.Client, r.Reader, claim, tag, leaseHolder{kind: "BackupRun", run: run, item: item.Name})
		if busy.held() {
			return busy.text, nil
		}
		// A source no run waits for any more and a refused claim fail the
		// item; any other error leaves it Pending for the next pass.
		if failBackupItem(item, err) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		item.Phase, item.Trigger = backupv1alpha1.ItemRunning, tag

	case "Cluster":
		cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		switch {
		case err != nil:
			return "", err
		case !found:
			failBackupItem(item, refuse(backupv1alpha1.ItemReasonClusterMissing, "the Cluster %s no longer exists", item.Name))
		case cnpg.Hibernated(cluster):
			item.Phase, item.Reason = backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonClusterHibernated
			item.Message = "the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"
		default:
			name, err := cnpg.EnsureBackup(ctx, r.Client, run.Namespace, item.Name, run.UID)
			if apierrors.IsInvalid(err) {
				failBackupItem(item, refuse(backupv1alpha1.ItemReasonBackupRefused, "%v", err))
				return "", nil
			}
			if err != nil {
				return "", err
			}
			item.Phase, item.Backup = backupv1alpha1.ItemRunning, name
		}
	}
	return "", nil
}

// clonesCut reports whether VolSync has cut the clone of every volume the run
// backs up. That is the moment the stopped workloads can start again, because
// VolSync uploads from the clone and no longer reads the app's volume.
//
// A clone counts as cut when the claim volsync-<claim>-src is Bound, is not
// being deleted, and was created after status.quiescedAt. VolSync marks a sync
// done before its cleanup deletes the clone, so the clone of the previous
// sync can still be there when this run starts, and it holds the data from
// before the app stopped. This run writes its trigger at least one pass after
// quiescedAt, so its own clone is always newer.
//
// A ReplicationSource that has already completed the run's trigger tag has
// cut its clone too. That check catches a clone VolSync created and deleted
// between two passes. A Pending volume item means no clone yet, and items
// that failed or were skipped are left out.
func (r *BackupRunReconciler) clonesCut(ctx context.Context, run *backupv1alpha1.BackupRun) bool {
	for _, item := range run.Status.Items {
		if item.Kind != "ReplicationSource" {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			return false
		case backupv1alpha1.ItemRunning:
			if !r.cloneCut(ctx, run, item) {
				return false
			}
		default:
			// An item in any other phase ended or never started a sync, so
			// it has no clone to wait for.
		}
	}
	return true
}

// cloneCut reports whether VolSync has cut the clone of the Running volume
// item, by the rules clonesCut describes. A failed read counts as no clone.
func (r *BackupRunReconciler) cloneCut(ctx context.Context, run *backupv1alpha1.BackupRun, item backupv1alpha1.BackupItem) bool {
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err == nil && lastManual(source) == item.Trigger {
		return true
	}
	clone := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: cloneName(item.Name)}
	if err := r.Reader.Get(ctx, key, clone); err != nil || clone.Status.Phase != corev1.ClaimBound {
		return false
	}
	return clone.DeletionTimestamp.IsZero() && run.Status.QuiescedAt != nil && clone.CreationTimestamp.After(run.Status.QuiescedAt.Time)
}

// cloneName returns the name of the claim VolSync clones the given claim
// into for a backup.
func cloneName(claim string) string { return "volsync-" + claim + "-src" }

// giveUpUncut fails every volume item of the run whose clone VolSync has not
// cut, because the workloads have been stopped for the namespace's
// backup.wlz.li/max-quiesce limit and the pass starts them again.
//
// Parameters:
//   - limit is the limit that ran out, for the message.
//   - now is the time of the pass, which becomes status.restartedAt.
//
// A Pending item fails with a message that says it never started, with the
// error of its last start attempt (status.items[].lastStartError) when it
// has one. A Running item whose clone is not cut fails with a message that
// names the missing clone. A Running item whose clone is cut goes on. A Running item's message also says what
// data a snapshot of the sync VolSync goes on with holds (see syncGoesOn).
// Afterwards clonesCut is true, so the
// caller's restart path records status.restartedAt and starts the workloads.
// The check reads only the clones; a failed read counts as no clone, so an
// API outage never keeps the workloads down past the limit.
func (r *BackupRunReconciler) giveUpUncut(ctx context.Context, run *backupv1alpha1.BackupRun, limit time.Duration, now metav1.Time) {
	at := now.UTC().Format(time.RFC3339)
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindSource {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			message := fmt.Sprintf("not started before the workloads were given back at %s, when the %s limit of %s ran out, so the clone %s was never cut",
				at, backupv1alpha1.AnnotationMaxQuiesce, limit, cloneName(item.Name))
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message+startErrorNote(*item, ": ")
		case backupv1alpha1.ItemRunning:
			if r.cloneCut(ctx, run, *item) {
				continue
			}
			message := fmt.Sprintf("VolSync had not cut the clone %s by %s, when the %s limit of %s ran out and the workloads were given back",
				cloneName(item.Name), at, backupv1alpha1.AnnotationMaxQuiesce, limit)
			if note := r.syncGoesOn(ctx, run, *item, nil); note != "" {
				message += ". " + note
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		default:
			// An item in any other phase has already ended, so the limit
			// changes nothing about it.
		}
	}
}

// syncGoesOn returns a sentence for the message of a Running volume item the
// run fails while VolSync goes on with the item's sync, and "" when VolSync
// has no sync of the item's trigger open.
//
// Parameters:
//   - item is the volume item, with the trigger tag the run wrote.
//   - source is the claim's ReplicationSource as the caller read it, or nil
//     for syncGoesOn to read it.
//
// VolSync keeps a sync open until a mover Job succeeds, and every Job of the
// sync reads the clone it cut when the sync started (the status's
// lastSyncStartTime). restic stamps a snapshot with the time its mover ran,
// so a snapshot a retry saves after the run gave the item up carries a later
// time than the data it holds. The sentence names the data such a snapshot
// holds: that of the sync's start when the clone is cut, and that of the
// moment VolSync cuts the clone otherwise. The clone counts as cut when the
// claim volsync-<claim>-src is Bound, is not being deleted, and was created
// no earlier than the sync started. A failed read gives "", since the
// sentence only explains the item's failure.
func (r *BackupRunReconciler) syncGoesOn(ctx context.Context, run *backupv1alpha1.BackupRun, item backupv1alpha1.BackupItem, source *volsyncv1alpha1.ReplicationSource) string {
	if item.Kind != "ReplicationSource" || item.Trigger == "" {
		return ""
	}
	if source == nil {
		source = &volsyncv1alpha1.ReplicationSource{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
			return ""
		}
	}
	if manualTag(source) != item.Trigger || lastManual(source) == item.Trigger || source.Status == nil || source.Status.LastSyncStartTime == nil {
		return ""
	}
	start := source.Status.LastSyncStartTime
	at := start.UTC().Format(time.RFC3339)
	clone := &corev1.PersistentVolumeClaim{}
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: cloneName(item.Name)}, clone)
	if err != nil && !apierrors.IsNotFound(err) {
		return ""
	}
	if err == nil && clone.Status.Phase == corev1.ClaimBound && clone.DeletionTimestamp.IsZero() && !clone.CreationTimestamp.Before(start) {
		return fmt.Sprintf("VolSync keeps retrying the sync it started at %s with the clone it cut then, so a snapshot this sync saves later "+
			"holds the data of %s, whatever time restic stamps on it.", at, at)
	}
	return fmt.Sprintf("VolSync goes on with the sync it started at %s and has not cut its clone yet, so a snapshot this sync saves later "+
		"holds the data of the moment it cuts the clone, whatever time restic stamps on it.", at)
}

// collectItem records the result of a Running item once it has one, and
// leaves the item Running until then.
//
// Parameters:
//   - run is the BackupRun, for its namespace, its start and its restart
//     moment.
//   - item is the Running item. It is changed in place.
//
// It returns a sentence for the run's Ready message while a database item
// waits in a Backup phase that needs naming, such as one CloudNativePG 1.30
// does not have, and "" otherwise.
//
// A volume item follows its ReplicationSource (see collectVolume), and a
// database item follows the phase of its CloudNativePG Backup (see
// collectDatabase).
func (r *BackupRunReconciler) collectItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) string {
	switch item.Kind {
	case backupv1alpha1.ItemKindSource:
		r.collectVolume(ctx, run, item)
	case backupv1alpha1.ItemKindCluster:
		return r.collectDatabase(ctx, run, item)
	}
	return ""
}

// collectVolume records the result of a Running volume item once its
// ReplicationSource has one.
//
// Parameters:
//   - run is the BackupRun, for its namespace, its start and its restart
//     moment.
//   - item is the Running volume item, which names the claim and the run's
//     trigger tag. It is changed in place.
//
// A volume item is done when its ReplicationSource has completed the run's
// trigger tag. VolSync completes a tag only after a mover Job succeeds.
// While the tag is open, a Failed mover result of this run's sync fails the
// item (see failedSync). A completed tag whose latest mover result is Failed
// fails the item with reason MoverFailed and the mover's logs as they are;
// no decision reads the logs.
//
// A completed sync gets its snapshot from the repository (see
// identifySnapshot). On a run that stopped its workloads the item then stays
// Running until the status holding the snapshot's full ID is written, and a
// later pass, once status.restartedAt is set, moves that snapshot to it and
// tags it quiesced (see retimeRecorded). A failed read of the source leaves
// the item as it was, for the next pass.
func (r *BackupRunReconciler) collectVolume(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
		return
	}
	switch {
	case lastManual(source) != item.Trigger:
		r.failedSync(ctx, run, item, source)
	case source.Status.LatestMoverStatus != nil && source.Status.LatestMoverStatus.Result == volsyncv1alpha1.MoverResultFailed:
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonMoverFailed, "Mover logs: %s", source.Status.LatestMoverStatus.Logs))
	case item.SnapshotID != "":
		r.retimeRecorded(ctx, run, item)
	default:
		r.identifySnapshot(ctx, run, item, source)
	}
}

// failedSync fails a volume item whose sync is still open when a mover Job
// of that sync failed, and leaves the item alone otherwise.
//
// Parameters:
//   - run is the BackupRun, for its start.
//   - item is the Running volume item. It is changed in place.
//   - source is the item's ReplicationSource as this pass read it.
//
// A Failed result that moverFailed places in this run's sync fails the item
// with reason MoverFailed and the message "Mover logs: " followed by the
// logs as VolSync kept them. When VolSync goes on with the sync, the message
// first says what data a snapshot of that sync saves later holds (see
// syncGoesOn).
//
// The source is left alone. VolSync writes the failure after it has already
// started a new mover Job, and deleting the source would kill that mover
// mid-backup and leave restic's lock in the repository. VolSync keeps
// retrying, and a later run fails its item for the claim at once while this
// run's tag is still open (see holder).
func (r *BackupRunReconciler) failedSync(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem, source *volsyncv1alpha1.ReplicationSource) {
	logs, failed := moverFailed(source, item.Trigger, run.Status.StartedAt)
	if !failed {
		return
	}
	message := "Mover logs: " + logs
	if note := r.syncGoesOn(ctx, run, *item, source); note != "" {
		message = note + " " + message
	}
	failBackupItem(item, refuse(backupv1alpha1.ItemReasonMoverFailed, "%s", message))
}
