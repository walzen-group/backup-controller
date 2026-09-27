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
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	"github.com/walzen-group/backup-controller/internal/restic"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// target parses the run's spec.restoreAsOf. It returns nil when the field is
// unset, which means the newest backup, and an error when the value is not an
// RFC 3339 time.
func target(run *backupv1alpha1.RestoreRun) (*time.Time, error) {
	if run.Spec.RestoreAsOf == nil {
		return nil, nil
	}
	t, err := time.Parse(time.RFC3339, *run.Spec.RestoreAsOf)
	if err != nil {
		return nil, fmt.Errorf("restoreAsOf %q is not an RFC 3339 time", *run.Spec.RestoreAsOf)
	}
	return &t, nil
}

// plan checks, before the run changes anything, that a backup reaches the
// run's moment for every item of an in-place run, and starts the run when
// every item passes.
//
// Parameters:
//   - run is the RestoreRun with an empty phase and no spec.into. plan
//     records its items, the selected snapshots and base backups, and
//     status.syncedTo in its status.
//
// It returns the result of the pass. A run whose checks passed is moved to
// Running with status.startedAt set and requeued after a second; a run that
// ends here returns what finish returns. plan returns an error, and writes
// nothing, when listing the snapshots or the base backups fails, and when a
// read fails in a way a retry may fix, such as a timeout from the API
// server; Reconcile hands such an error to planFailed.
//
// For a volume, the check selects the snapshot the restore would use and
// records its full ID, its short ID and its time on the item (see
// checkVolume and recordSnapshot). For a database, it selects the base
// backup the recovery would start from and records its ID (see
// checkDatabase). plan marks an item with nothing in reach Failed with the
// reason and the message of the refusal (see checkItems). If any item
// fails, plan marks every other Pending item Skipped with reason
// OtherItemFailed (see unreachableItems) and ends the run as Failed with
// reason NoBackupInReach, naming each item it cannot reach. Nothing has been
// deleted or overwritten at that point.
//
// A backup's mover runs restic forget after it saves, and a quiesced
// BackupRun retimes its snapshot after the mover, so a snapshot selected
// during a backup may be gone by the time the restore's mover starts. Before
// it checks a volume, plan therefore looks for a backup of the claim or its
// repository in progress (see volumeBackedUp), and while there is one it
// waits with reason SourceBusy and leaves the run unplanned (see
// waitAtChecks), until spec.timeout from the run's creation ends it TimedOut.
//
// With spec.syncDatabaseToVolume set, each volume may only select a snapshot
// tagged quiesced, and all the selected snapshots must carry the same time.
// plan records that time in status.syncedTo and checks each database against
// it in place of spec.restoreAsOf.
//
// A spec that can't work ends the run as Failed with reason Invalid: a
// restoreAsOf that doesn't parse, a spec.quiesce entry the namespace does not
// hold, a synced run with no claim to take the moment from, or any refusal
// from items, such as a Cluster that another unfinished RestoreRun is
// restoring.
//
// While clusterWebhookBlind reports that the API server no longer serves
// Cluster at the version the bootstrap webhook's rules name, a run with a
// Pending Cluster item ends Failed with reason ClusterVersionUnsupported
// before anything is stopped or deleted: each such item fails with the
// message, and every other Pending item is Skipped.
func (r *RestoreRunReconciler) plan(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	items, err := r.items(ctx, run)
	if asRunRefusal(err) {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	at, err := target(run)
	if err != nil {
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if _, err := quiesce.Named(ctx, r.Reader, run.Namespace, run.Spec.Quiesce); err != nil {
		if !asRunRefusal(err) {
			return ctrl.Result{}, err
		}
		return r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}

	// A Cluster the run would delete comes back empty while the webhook
	// can't see its creation, so the run ends before it changes anything.
	if blind := clusterWebhookBlind(r.RESTMapper()); len(failBlindClusters(items, blind)) > 0 {
		return r.endBlind(ctx, run, items, blind)
	}

	synced, busy, err := r.checkItems(ctx, run, items, at)
	if err != nil {
		return ctrl.Result{}, err
	}
	if busy.held() {
		return r.waitAtChecks(ctx, run, busy)
	}
	run.Status.Items = items
	if unreachable := unreachableItems(items); len(unreachable) > 0 {
		return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, strings.Join(unreachable, "; "))
	}
	if run.Spec.SyncDatabaseToVolume {
		if synced == nil {
			return r.finish(ctx, run, backupv1alpha1.ReasonInvalid,
				fmt.Sprintf("syncDatabaseToVolume needs a claim marked %s: \"true\" in this namespace to take the moment from", backupv1alpha1.AnnotationEnabled))
		}
		run.Status.SyncedTo = &metav1.Time{Time: *synced}
	}

	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = &now
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "restoring")
	return after(time.Second, r.writeStatus(ctx, run))
}

// checkItems runs the checks of plan on each Pending item, in the order of
// the items.
//
// Parameters:
//   - run is the RestoreRun being planned.
//   - items are the run's items as items returns them, claims first. Each
//     check writes its result on its item: the snapshot or the base backup
//     it selected, or Failed with the reason and message of a refusal (see
//     failRestoreItem).
//   - at is the run's moment from target, or nil for the newest backup.
//
// It returns the synced moment of a run with spec.syncDatabaseToVolume (see
// syncedMoment), or nil when no volume selected a snapshot. It returns the
// hold of the first volume whose claim or repository a backup in progress
// holds (see volumeBackedUp), and then it checks no further item. It
// returns a plain error when a check fails in a way a retry may fix.
//
// A synced run takes its databases' moment from the volumes' quiesced
// snapshots. items lists the claims before the Clusters, so checkItems knows
// the moment before it checks the first Cluster. Each check gives its own
// error, so an item never fails with the refusal of the item before it.
func (r *RestoreRunReconciler) checkItems(ctx context.Context, run *backupv1alpha1.RestoreRun, items []backupv1alpha1.RestoreItem, at *time.Time) (synced *time.Time, busy hold, err error) {
	for i := range items {
		item := &items[i]
		if item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		var checkErr error
		switch item.Kind {
		case backupv1alpha1.ItemKindClaim:
			busy, synced, checkErr = r.checkVolumeItem(ctx, run, item, at, synced)
			if busy.held() {
				return synced, busy, nil
			}
		case backupv1alpha1.ItemKindCluster:
			checkErr = r.checkClusterItem(ctx, run, item, at, synced)
		}
		if checkErr != nil && !failRestoreItem(item, checkErr) {
			return synced, hold{}, checkErr
		}
	}
	return synced, hold{}, nil
}

// checkVolumeItem checks one volume item of plan and records the snapshot
// it selected on the item.
//
// Parameters:
//   - run is the RestoreRun being planned.
//   - item is the Pending volume item. It gets the selected snapshot (see
//     recordSnapshot).
//   - at is the run's moment, or nil for the newest snapshot.
//   - synced is the synced moment so far, or nil before the first volume
//     of a run with spec.syncDatabaseToVolume.
//
// It returns the hold of a backup in progress of the claim or its
// repository (see volumeBackedUp), and then it checks nothing. It returns
// the synced moment after this volume (see syncedMoment), or synced as it
// was when the run is not synced. It returns the refusal of checkVolume or
// syncedMoment, which fails the item, and a plain error when a read fails.
//
// The snapshot is selected once no backup of the repository is in
// progress, so after that backup's forget and retime.
func (r *RestoreRunReconciler) checkVolumeItem(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, at, synced *time.Time) (hold, *time.Time, error) {
	busy, err := r.volumeBackedUp(ctx, run, item.Name)
	if err != nil || busy.held() {
		return busy, synced, err
	}
	sync := run.Spec.SyncDatabaseToVolume
	snapshot, err := r.checkVolume(ctx, run, item.Name, at, sync)
	recordSnapshot(item, snapshot)
	if err != nil || !sync {
		return hold{}, synced, err
	}
	moment, err := syncedMoment(snapshot, synced)
	return hold{}, moment, err
}

// syncedMoment gives the moment of a synced restore after one more volume.
//
// Parameters:
//   - snapshot is the quiesced snapshot the volume selected.
//   - synced is the moment of the volumes before it, or nil for the first
//     volume.
//
// It returns the snapshot's time for the first volume, and synced for a
// volume whose snapshot has the same time. For a snapshot of another time,
// it returns synced and a *refusalError with reason NoBackupInReach, since
// a synced restore needs one moment for every volume.
func syncedMoment(snapshot restic.Snapshot, synced *time.Time) (*time.Time, error) {
	if synced == nil {
		moment := snapshot.Time.UTC()
		return &moment, nil
	}
	if !snapshot.Time.Equal(*synced) {
		return synced, refuse(backupv1alpha1.ItemReasonNoBackupInReach, "its quiesced snapshot %s is from %s and another volume's is from %s; a synced restore needs one moment for every volume",
			snapshot.ShortID(), snapshot.Time.UTC().Format(time.RFC3339), synced.Format(time.RFC3339))
	}
	return synced, nil
}

// checkClusterItem checks one database item of plan and records the base
// backup it selected on the item.
//
// Parameters:
//   - run is the RestoreRun being planned.
//   - item is the Pending Cluster item. It gets the ID of the selected
//     base backup, or an empty ID when there is none.
//   - at is the run's moment, or nil for the newest base backup.
//   - synced is the synced moment of the volumes. A run with
//     spec.syncDatabaseToVolume recovers the database to it in place of at.
//
// It returns the refusal of checkDatabase, which fails the item, and a
// plain error when a read fails. For a synced run with no synced moment, it
// returns a *refusalError with reason NoBackupInReach, since no volume
// selected a quiesced snapshot.
func (r *RestoreRunReconciler) checkClusterItem(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, at, synced *time.Time) error {
	moment := at
	if run.Spec.SyncDatabaseToVolume {
		if synced == nil {
			return refuse(backupv1alpha1.ItemReasonNoBackupInReach, "no volume selected a quiesced snapshot, so there is no moment to recover the database to")
		}
		moment = synced
	}
	backup, err := r.checkDatabase(ctx, run.Namespace, item.Name, moment)
	item.BaseBackup = backup
	return err
}

// unreachableItems ends the items of a run that plan finds out of reach.
//
// Parameters:
//   - items are the run's items after checkItems. When another item
//     failed, unreachableItems marks each item still Pending Skipped with
//     reason OtherItemFailed. The run deleted and overwrote nothing yet.
//
// It returns one line for each Failed item, with its kind, name and
// message, for the run's message. It returns none, and changes no item,
// when no item failed.
func unreachableItems(items []backupv1alpha1.RestoreItem) []string {
	var unreachable []string
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			unreachable = append(unreachable, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
		}
	}
	if len(unreachable) == 0 {
		return nil
	}
	for i := range items {
		if items[i].Phase == backupv1alpha1.ItemPending {
			items[i].Phase, items[i].Reason = backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonOtherItemFailed
			items[i].Message = "left alone because another item has no backup the run's moment reaches"
		}
	}
	return unreachable
}

// items returns one Pending item for each claim and Cluster the run's spec
// names.
//
// Parameters:
//   - run is the RestoreRun being planned. Its spec.claim, spec.repository,
//     spec.database and spec.all choose the items, and its namespace is where
//     they are looked up.
//
// It returns the items, claims first. spec.claim names one claim and
// spec.database names one Cluster. A run with neither takes every claim and
// every Cluster in the namespace marked backup.wlz.li/enabled: "true" (see
// enabledItems). A Cluster that archives nowhere has no backup to restore, so
// its item starts out Skipped with reason ClusterArchivesNowhere. A Cluster
// that opts out of the bootstrap webhook, or whose owner declares its own
// bootstrap method, starts out Skipped with reason ClusterLeftAlone (see
// leftAlone).
//
// It returns an *invalidSpecError, which plan turns into reason Invalid: when
// spec.repository is set and spec.claim is not, since a restore in place
// needs a claim to write into, and the refusal sends the user to spec.into
// with a name no claim has, or to spec.claim to overwrite an existing claim
// in place; and when nothing in the namespace is marked. It returns the
// *refusalError of leftAlone, which plan turns into reason Invalid as well,
// when spec.database names a Cluster that opts out of the bootstrap webhook
// or declares its own bootstrap. It returns the *refusalError of
// clustersRestoredElsewhere, which plan turns into reason Invalid as well,
// when another unfinished RestoreRun is restoring a Cluster the run would
// restore.
// A failed read comes back as a plain error, and the caller retries.
func (r *RestoreRunReconciler) items(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	var items []backupv1alpha1.RestoreItem
	var err error
	switch {
	case run.Spec.Claim != "":
		return []backupv1alpha1.RestoreItem{pendingRestore(backupv1alpha1.ItemKindClaim, run.Spec.Claim)}, nil
	case run.Spec.Repository != "":
		return nil, invalidSpec("spec.repository alone restores into a new claim, so spec.into is required, and it must name a claim that does not exist yet. " +
			"To overwrite an existing claim from this repository, set spec.claim to it as well; the run then restores it in place once no pod mounts it.")
	case run.Spec.Database != "":
		items, err = r.databaseItems(ctx, run)
	default:
		items, err = r.enabledItems(ctx, run)
	}
	if err != nil {
		return nil, err
	}
	if err := r.clustersRestoredElsewhere(ctx, run, items); err != nil {
		return nil, err
	}
	return items, nil
}

// pendingRestore returns a Pending restore item of the given kind and name.
func pendingRestore(kind, name string) backupv1alpha1.RestoreItem {
	return backupv1alpha1.RestoreItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
}

// databaseItems returns the one item of a run whose spec.database names a
// Cluster.
//
// Parameters:
//   - run is the RestoreRun being planned, with spec.database set.
//
// It returns one Pending Cluster item. It returns the refusal of leftAlone,
// wrapped with the Cluster's name, when the Cluster exists and the run must
// leave it alone. plan ends the run with reason Invalid (see asRunRefusal),
// and no item records it. It returns a plain error when the read of the
// Cluster fails. A missing Cluster gives an item too, and checkDatabase
// fails it.
func (r *RestoreRunReconciler) databaseItems(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Spec.Database)
	if err != nil {
		return nil, err
	}
	if found {
		if err := leftAlone(cluster); err != nil {
			return nil, fmt.Errorf("%s %s: %w", backupv1alpha1.ItemKindCluster, run.Spec.Database, err)
		}
	}
	return []backupv1alpha1.RestoreItem{pendingRestore(backupv1alpha1.ItemKindCluster, run.Spec.Database)}, nil
}

// enabledItems returns the items of a run that names no claim and no
// Cluster: every claim and every Cluster in the namespace marked
// backup.wlz.li/enabled: "true".
//
// Parameters:
//   - run is the RestoreRun being planned. Its namespace is where the claims
//     and Clusters are listed.
//
// It returns the items, claims first. A Cluster the run must leave alone
// (see leftAlone) starts out Skipped with reason ClusterLeftAlone. A
// Cluster that archives nowhere has no backup to restore, so its item
// starts out Skipped with reason ClusterArchivesNowhere. It returns an
// *invalidSpecError when nothing in the namespace is marked, and a plain
// error when a list fails.
func (r *RestoreRunReconciler) enabledItems(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
	if err != nil {
		return nil, err
	}
	clusters, err := cnpg.EnabledClusters(ctx, r.Reader, r.RESTMapper(), run.Namespace)
	if err != nil {
		return nil, err
	}
	var items []backupv1alpha1.RestoreItem
	for _, claim := range claims {
		items = append(items, pendingRestore(backupv1alpha1.ItemKindClaim, claim.Name))
	}
	for i := range clusters {
		item := pendingRestore(backupv1alpha1.ItemKindCluster, clusters[i].GetName())
		if _, _, archives := bootstrap.Archiver(&clusters[i]); !skipLeftAlone(&item, &clusters[i]) && !archives {
			item.Phase, item.Reason = backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonClusterArchivesNowhere
			item.Message = "the Cluster archives nowhere, so it has no backup to restore"
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, invalidSpec("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
	}
	return items, nil
}

// clustersRestoredElsewhere refuses a run that would restore a Cluster
// another unfinished RestoreRun is restoring.
//
// Parameters:
//   - run is the run being planned. A RestoreRun with its UID is skipped.
//   - items are the run's items as planned. Only a Cluster item that is
//     Pending counts, since the run leaves a Skipped one alone.
//
// It returns a *refusalError with reason ClusterRestoredElsewhere, naming
// the other run and the Cluster, when another RestoreRun in the namespace
// that has not finished holds an item for one of those Clusters in phase
// Pending, Deleted or Recovering, and nil otherwise.
// A failed list of the RestoreRuns comes back as a plain error, and the
// caller retries.
//
// Two runs that both delete one Cluster race for its recovery: the webhook
// recovers it for the first run it lists, and the other run fails. Refused
// at its checks, the second run deletes nothing. Two runs that planned in the
// same instant both pass that check, so restoreDatabase asks again right
// before it marks a Pending item Deleted. The refusal names the other run,
// so the user knows which run to wait for.
func (r *RestoreRunReconciler) clustersRestoredElsewhere(ctx context.Context, run *backupv1alpha1.RestoreRun, items []backupv1alpha1.RestoreItem) error {
	var clusters []string
	for _, item := range items {
		if item.Kind == backupv1alpha1.ItemKindCluster && item.Phase == backupv1alpha1.ItemPending {
			clusters = append(clusters, item.Name)
		}
	}
	if len(clusters) == 0 {
		return nil
	}
	restores := &backupv1alpha1.RestoreRunList{}
	if err := r.Reader.List(ctx, restores, client.InNamespace(run.Namespace)); err != nil {
		return fmt.Errorf("list RestoreRuns in %s: %w", run.Namespace, err)
	}
	for i := range restores.Items {
		other := &restores.Items[i]
		if other.UID == run.UID || other.Status.Phase.Finished() {
			continue
		}
		if cluster, restoring := restoringCluster(other, clusters); restoring {
			return refuse(backupv1alpha1.ItemReasonClusterRestoredElsewhere, "RestoreRun %s is restoring Cluster %s. Create this RestoreRun again once that run has finished", other.Name, cluster)
		}
	}
	return nil
}

// restoringCluster finds a Cluster that another RestoreRun holds a recovery
// of.
//
// Parameters:
//   - other is the other RestoreRun, which has not finished.
//   - clusters names the Clusters the run being planned would restore.
//
// It returns the name of the first of those Clusters that other holds an
// item for in phase Pending, Deleted or Recovering, and true. It returns
// false when other holds none of them.
func restoringCluster(other *backupv1alpha1.RestoreRun, clusters []string) (string, bool) {
	for _, item := range other.Status.Items {
		if item.Kind != backupv1alpha1.ItemKindCluster || !slices.Contains(clusters, item.Name) {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
			return item.Name, true
		default:
			// An item in any other phase has let go of the Cluster or
			// never deleted it, so it holds no recovery.
		}
	}
	return "", false
}

// endBlind ends a run that plan found with a Cluster item the bootstrap
// webhook would not see created again.
//
// Parameters:
//   - run is the RestoreRun being planned.
//   - items are the run's items, in which failBlindClusters failed each
//     Pending Cluster item. endBlind marks every item still Pending Skipped.
//   - blind is the blindness clusterWebhookBlind found. Its message is the
//     run's message.
//
// It returns what finish returns for reason ClusterVersionUnsupported. It
// logs the blindness as an error first.
func (r *RestoreRunReconciler) endBlind(ctx context.Context, run *backupv1alpha1.RestoreRun, items []backupv1alpha1.RestoreItem, blind webhookBlind) (ctrl.Result, error) {
	log.FromContext(ctx).Error(errors.New(blind.message()), "deleting no Cluster: the bootstrap webhook would not see it created again",
		"namespace", run.Namespace, "name", run.Name)
	for i := range items {
		if items[i].Phase == backupv1alpha1.ItemPending {
			items[i].Phase = backupv1alpha1.ItemSkipped
			items[i].Message = "left alone because the run deletes no Cluster the bootstrap webhook would not see created again"
		}
	}
	run.Status.Items = items
	return r.finish(ctx, run, backupv1alpha1.ReasonClusterVersionUnsupported, blind.message())
}
