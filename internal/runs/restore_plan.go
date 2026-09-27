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
// checkDatabase). An item with nothing in reach (see selectSnapshot) is
// marked Failed with the reason. If any item fails, plan
// marks every other Pending item Skipped and ends the run as Failed with
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

	// A synced run takes its databases' moment from the volumes' quiesced
	// snapshots. The items method lists the claims before the Clusters, so
	// the moment is known by the time the first Cluster is checked.
	sync := run.Spec.SyncDatabaseToVolume
	var synced *time.Time
	var unreachable []string
	for i := range items {
		item := &items[i]
		if item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		// Each item's check sets its own reason and error, so an item never
		// fails with the refusal of the item before it.
		var reason string
		var err error
		switch item.Kind {
		case "PersistentVolumeClaim":
			// The snapshot is selected once no backup of the repository is in
			// progress, so after that backup's forget and retime.
			busy, busyErr := r.volumeBackedUp(ctx, run, item.Name)
			if busyErr != nil {
				return ctrl.Result{}, busyErr
			}
			if busy.held() {
				return r.waitAtChecks(ctx, run, busy)
			}
			var snapshot restic.Snapshot
			snapshot, reason, err = r.checkVolume(ctx, run, item.Name, at, sync)
			recordSnapshot(item, snapshot)
			if sync && reason == "" && err == nil {
				switch {
				case synced == nil:
					moment := snapshot.Time.UTC()
					synced = &moment
				case !snapshot.Time.Equal(*synced):
					reason = fmt.Sprintf("its quiesced snapshot %s is from %s and another volume's is from %s; a synced restore needs one moment for every volume",
						snapshot.ShortID(), snapshot.Time.UTC().Format(time.RFC3339), synced.Format(time.RFC3339))
				}
			}
		case "Cluster":
			moment := at
			if sync {
				moment = synced
			}
			if sync && synced == nil {
				reason = "no volume selected a quiesced snapshot, so there is no moment to recover the database to"
				break
			}
			item.BaseBackup, reason, err = r.checkDatabase(ctx, run.Namespace, item.Name, moment)
		}
		// A refusal fails the item with its reason; a reason still given as
		// a string fails it with none.
		if err != nil && !failRestoreItem(item, err) {
			return ctrl.Result{}, err
		}
		if reason != "" {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, reason
		}
		if item.Phase == backupv1alpha1.ItemFailed {
			unreachable = append(unreachable, fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message))
		}
	}

	run.Status.Items = items
	if len(unreachable) > 0 {
		// Nothing has been deleted or overwritten yet. The other items are
		// left as they were, and the run reports every item it cannot reach.
		for i := range run.Status.Items {
			if run.Status.Items[i].Phase == backupv1alpha1.ItemPending {
				run.Status.Items[i].Phase = backupv1alpha1.ItemSkipped
				run.Status.Items[i].Message = "left alone because another item has no backup the run's moment reaches"
			}
		}
		return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, strings.Join(unreachable, "; "))
	}
	if sync {
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
// every Cluster in the namespace marked backup.wlz.li/enabled: "true". A
// Cluster that archives nowhere has no backup to restore, so its item starts
// out Skipped. So does a Cluster that opts out of the bootstrap webhook, or
// whose owner declares its own bootstrap method (see leftAlone).
//
// It returns an *invalidSpecError, which plan turns into reason Invalid: when
// spec.repository is set and spec.claim is not, since a restore in place
// needs a claim to write into, and the refusal sends the user to spec.into
// with a name no claim has, or to spec.claim to overwrite an existing claim
// in place; when nothing in the namespace is marked; and when spec.database
// names a Cluster that opts out of the bootstrap webhook or declares its own
// bootstrap. It returns the *refusalError of clustersRestoredElsewhere, which
// plan turns into reason Invalid as well, when another unfinished RestoreRun
// is restoring a Cluster the run would restore.
// A failed read comes back as a plain error, and the caller retries.
func (r *RestoreRunReconciler) items(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	pending := func(kind, name string) backupv1alpha1.RestoreItem {
		return backupv1alpha1.RestoreItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}
	switch {
	case run.Spec.Claim != "":
		return []backupv1alpha1.RestoreItem{pending("PersistentVolumeClaim", run.Spec.Claim)}, nil
	case run.Spec.Repository != "":
		return nil, invalidSpec("spec.repository alone restores into a new claim, so spec.into is required, and it must name a claim that does not exist yet. " +
			"To overwrite an existing claim from this repository, set spec.claim to it as well; the run then restores it in place once no pod mounts it.")
	case run.Spec.Database != "":
		cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Spec.Database)
		if err != nil {
			return nil, err
		}
		if found {
			if why := leftAlone(cluster); why != "" {
				return nil, invalidSpec("Cluster %s: %s", run.Spec.Database, why)
			}
		}
		items := []backupv1alpha1.RestoreItem{pending("Cluster", run.Spec.Database)}
		if err := r.clustersRestoredElsewhere(ctx, run, items); err != nil {
			return nil, err
		}
		return items, nil
	}

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
		items = append(items, pending("PersistentVolumeClaim", claim.Name))
	}
	for i := range clusters {
		item := pending("Cluster", clusters[i].GetName())
		if why := leftAlone(&clusters[i]); why != "" {
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, why
		} else if _, _, archives := bootstrap.Archiver(&clusters[i]); !archives {
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster archives nowhere, so it has no backup to restore"
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, invalidSpec("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
	}
	if err := r.clustersRestoredElsewhere(ctx, run, items); err != nil {
		return nil, err
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
		if item.Kind == "Cluster" && item.Phase == backupv1alpha1.ItemPending {
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
		for _, item := range other.Status.Items {
			if item.Kind != "Cluster" || !slices.Contains(clusters, item.Name) {
				continue
			}
			switch item.Phase {
			case backupv1alpha1.ItemPending, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
				return refuse(backupv1alpha1.ItemReasonClusterRestoredElsewhere, "RestoreRun %s is restoring Cluster %s. Create this RestoreRun again once that run has finished", other.Name, item.Name)
			default:
				// An item in any other phase has let go of the Cluster or
				// never deleted it, so it holds no recovery.
			}
		}
	}
	return nil
}
