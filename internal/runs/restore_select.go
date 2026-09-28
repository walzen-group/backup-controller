package runs

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// noBackupError is the reason an item has no backup the run can restore. It
// fails the item, and then the run, before the run changes anything.
type noBackupError struct{ reason string }

// Error returns the reason.
func (e *noBackupError) Error() string { return e.reason }

// noBackup returns a noBackupError with a formatted reason.
func noBackup(format string, args ...any) error {
	return &noBackupError{reason: fmt.Sprintf(format, args...)}
}

// selectBackups picks what each item of an admitted run restores, and records
// it on the item, before the run changes anything in the cluster.
//
// For a volume, it lists the claim's restic repository and selects one
// snapshot (see selectSnapshot). For a database, it lists the Cluster's
// completed base backups and decides where the recovery stops (see
// selectBaseBackup). With spec.syncDatabaseToVolume set, every volume must
// select a snapshot tagged paused from the same moment, which the run
// records in status.syncedTo, and each database recovers to that moment.
//
// It returns an empty string when every item has a backup, and otherwise one
// line per item that has none, joined with "; ". Those items are Failed, and
// every other item is Skipped, so the caller can end the run with reason
// NoBackupInReach. It returns an error when a read fails in a way a later
// pass may not see; the run then tries again.
func (r *RestoreRunReconciler) selectBackups(ctx context.Context, run *backupv1alpha1.RestoreRun) (string, error) {
	at, err := target(run)
	if err != nil {
		return "", err
	}
	sync := run.Spec.SyncDatabaseToVolume
	var synced *restic.Snapshot
	var unreachable []string
	fail := func(item *backupv1alpha1.RestoreItem, reason string) {
		item.Phase, item.Message = backupv1alpha1.ItemFailed, reason
		unreachable = append(unreachable, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, reason))
	}

	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != "PersistentVolumeClaim" || item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		snapshot, err := r.selectSnapshot(ctx, run, item, at, sync)
		var reason *noBackupError
		if errors.As(err, &reason) {
			fail(item, reason.reason)
			continue
		}
		if err != nil {
			return "", err
		}
		if sync {
			if synced == nil {
				synced = &snapshot
			} else if !snapshot.Time.Equal(synced.Time) {
				fail(item, fmt.Sprintf("its paused snapshot %s is from %s and another volume's is from %s; a synced restore needs one moment for every volume",
					snapshot.ShortID(), snapshot.Time.UTC().Format(time.RFC3339), synced.Time.UTC().Format(time.RFC3339)))
				continue
			}
		}
		item.Snapshot, item.SnapshotID, item.SnapshotTime = snapshot.ShortID(), snapshot.ID, newTime(metav1.NewTime(snapshot.Time))
	}

	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != "Cluster" || item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		if sync && synced == nil {
			fail(item, "no volume selected a paused snapshot, so there is no moment to recover the database to")
			continue
		}
		err := r.selectBaseBackup(ctx, run, item, at, synced)
		var reason *noBackupError
		if errors.As(err, &reason) {
			fail(item, reason.reason)
			continue
		}
		if err != nil {
			return "", err
		}
	}

	if len(unreachable) > 0 {
		for i := range run.Status.Items {
			if item := &run.Status.Items[i]; item.Phase == backupv1alpha1.ItemPending {
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, "left alone because another item has no backup the run can restore"
			}
		}
		return strings.Join(unreachable, "; "), nil
	}
	if synced != nil {
		run.Status.SyncedTo = &metav1.Time{Time: synced.Time.UTC()}
	}
	return "", nil
}

// selectSnapshot picks the snapshot a volume item restores.
//
// Parameters:
//   - run is the admitted RestoreRun. Its spec.previous takes part.
//   - item is the volume item, whose repository is listed (see
//     sourceRepository).
//   - at is the run's spec.restoreAsOf, or nil for the newest snapshot.
//   - paused limits the choice to snapshots tagged paused, for a synced
//     restore.
//
// It lists the repository (see sourceRepository) and takes the newest
// snapshot at or before the moment, then steps back spec.previous snapshots.
// It returns a *noBackupError when the repository holds no such snapshot, and
// another error when the repository can't be listed.
func (r *RestoreRunReconciler) selectSnapshot(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, at *time.Time, paused bool) (restic.Snapshot, error) {
	secret, err := r.sourceRepository(ctx, run, item)
	if err != nil {
		return restic.Snapshot{}, err
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return restic.Snapshot{}, fmt.Errorf("list the snapshots in %s: %w", secret.Name, err)
	}
	if paused {
		snapshots = slices.DeleteFunc(slices.Clone(snapshots), func(s restic.Snapshot) bool { return !slices.Contains(s.Tags, restic.PausedTag) })
		if len(snapshots) == 0 {
			return restic.Snapshot{}, noBackup("the repository holds no snapshot tagged %s; only a BackupRun with all: true that paused the app writes one", restic.PausedTag)
		}
	}
	if len(snapshots) == 0 {
		return restic.Snapshot{}, noBackup("the repository holds no snapshot")
	}
	slices.SortStableFunc(snapshots, func(a, b restic.Snapshot) int { return a.Time.Compare(b.Time) })

	index := len(snapshots) - 1
	if at != nil {
		index = -1
		for i, s := range snapshots {
			if !s.Time.After(*at) {
				index = i
			}
		}
		if index < 0 {
			return restic.Snapshot{}, noBackup("no snapshot at or before %s; the oldest, %s, is from %s",
				at.UTC().Format(time.RFC3339), snapshots[0].ShortID(), snapshots[0].Time.UTC().Format(time.RFC3339))
		}
	}
	if run.Spec.Previous != nil {
		index -= int(*run.Spec.Previous)
		if index < 0 {
			return restic.Snapshot{}, noBackup("previous %d reaches past the oldest snapshot", *run.Spec.Previous)
		}
	}
	return snapshots[index], nil
}

// sourceRepository returns the Secret of the restic repository a volume item
// restores from.
//
// Parameters:
//   - run is the RestoreRun.
//   - item is the volume item.
//
// An into restore reads spec.repository when it is set, and otherwise the
// repository of spec.claim's VolumeRestore. A restore in place reads the
// repository of the item's own claim's VolumeRestore, which is how a run with
// all: true restores every claim from its own repository.
//
// It returns a *noBackupError when the claim, its VolumeRestore or the Secret
// is missing, since there is nothing to restore from then, and another error
// when a read fails.
func (r *RestoreRunReconciler) sourceRepository(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) (*corev1.Secret, error) {
	name, claimName := "", item.Name
	if run.Spec.Into != "" {
		name, claimName = run.Spec.Repository, run.Spec.Claim
	}
	if name == "" {
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, noBackup("no claim %s in this namespace", claimName)
			}
			return nil, fmt.Errorf("get claim %s: %w", claimName, err)
		}
		vr, err := volumeRestoreFor(ctx, r.Reader, claim)
		if err != nil {
			var missing *missingVolumeRestoreError
			if errors.As(err, &missing) {
				return nil, noBackup("%v", err)
			}
			return nil, err
		}
		name = vr.Spec.Repository
	}
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: name}, secret); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, noBackup("no repository Secret %s in this namespace", name)
		}
		return nil, fmt.Errorf("get Secret %s: %w", name, err)
	}
	return secret, nil
}

// selectBaseBackup decides where the recovery of a database item stops, and
// records it on the item.
//
// Parameters:
//   - run is the admitted RestoreRun.
//   - item is the Pending Cluster item. It gets BaseBackup and
//     StopAtBaseBackup.
//   - at is the run's spec.restoreAsOf, or nil.
//   - synced is the paused snapshot of a synced restore, or nil.
//
// The recovery never goes ahead of the run's moment, and Postgres always
// reaches its target:
//   - With neither a moment nor a synced snapshot, the recovery replays the
//     whole WAL archive. The item records the newest base backup, which the
//     archive needs as its start.
//   - With spec.restoreAsOf, it stops at the end of the newest base backup
//     that finished at or before that moment.
//   - With a synced snapshot, it stops at the end of the base backup the
//     snapshot's BaseBackupTag names for this Cluster, which completed while
//     the app was paused. A Cluster the snapshot's HibernatedTag names
//     replays the whole archive when no WAL came after the snapshot's time:
//     the hibernated database has not run since. Otherwise, when the tag is
//     missing or the store no longer holds that base backup, it stops at the
//     end of the newest base backup that finished at or before the
//     snapshot's time.
//
// It returns a *noBackupError when the Cluster is missing, archives nowhere,
// or has no base backup in reach, and another error when the store can't be
// read.
func (r *RestoreRunReconciler) selectBaseBackup(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, at *time.Time, synced *restic.Snapshot) error {
	cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
	if err != nil {
		return err
	}
	if !found {
		return noBackup("no Cluster %s in this namespace", item.Name)
	}
	store, serverName, archives := bootstrap.Archiver(cluster)
	if !archives {
		return noBackup("the Cluster archives nowhere, so it has no backup to restore")
	}
	location, err := bootstrap.ResolveLocation(ctx, r.Reader, run.Namespace, store, serverName)
	if err != nil {
		return noBackup("%v", err)
	}
	backups, err := r.Prober.BaseBackups(ctx, location)
	if err != nil {
		return fmt.Errorf("list the base backups of %s: %w", item.Name, err)
	}
	if len(backups) == 0 {
		return noBackup("%s/%s holds no completed base backup; deleting the Cluster would bring it back empty", location.Bucket, location.BasePrefix())
	}

	switch {
	case synced != nil:
		if id, ok := restic.PausedBaseBackup(*synced, item.Name); ok && slices.ContainsFunc(backups, func(b bootstrap.BaseBackup) bool { return b.ID == id }) {
			item.BaseBackup, item.StopAtBaseBackup = id, true
			return nil
		}
		if slices.Contains(synced.Tags, restic.HibernatedTag(item.Name)) {
			woken, err := r.Prober.WALSince(ctx, location, synced.Time)
			if err != nil {
				return fmt.Errorf("check the WAL of %s after %s: %w", item.Name, synced.Time.UTC().Format(time.RFC3339), err)
			}
			if !woken {
				item.BaseBackup = backups[len(backups)-1].ID
				return nil
			}
		}
		at = &synced.Time
	case at == nil:
		item.BaseBackup = backups[len(backups)-1].ID
		return nil
	}
	backup, ok := bootstrap.AtOrBefore(backups, *at)
	if !ok {
		return noBackup("no base backup finished by %s; the oldest, %s, finished at %s",
			at.UTC().Format(time.RFC3339), backups[0].ID, backups[0].End.UTC().Format(time.RFC3339))
	}
	item.BaseBackup, item.StopAtBaseBackup = backup.ID, true
	return nil
}
