package runs

import (
	"context"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/served"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// restoreDatabase moves one database item a step further: it deletes the
// Cluster of a Pending item, and follows a Deleted or Recovering item until
// the Cluster is recovered.
//
// Parameters:
//   - run is the RestoreRun the item belongs to. Its name is the mark the
//     bootstrap webhook sets on a Cluster it recovers for the run, and its
//     status is written before a delete.
//   - item is the Cluster item, which restoreDatabase updates in place.
//
// It returns an error when a read of the Cluster, the list of the
// RestoreRuns, the status write or the delete fails, and the caller retries.
// It returns nil otherwise, with the item's new phase and message set.
//
// A Pending item whose Cluster opts out of the bootstrap webhook, or whose
// owner declares its own bootstrap method (see leftAlone), moves to Skipped,
// and the run never deletes that Cluster. Right before it marks any other
// Pending item Deleted, restoreDatabase checks again that no other unfinished
// run is restoring the Cluster (see clustersRestoredElsewhere), because two
// runs that planned in the same instant both passed the check at their plan.
// When another run holds the Cluster, the item fails with the refusal,
// naming that run, and the run deletes nothing.
//
// The item is marked Deleted, and the status written, before the Cluster is
// deleted. The bootstrap webhook recovers a Cluster only for a run whose item
// says Deleted, so the mark has to be in place before anything can create the
// Cluster again. The same write records the old Cluster's UID in
// status.items[].clusterUID. A run that found no Cluster to delete leaves its
// item Deleted without a UID.
//
// A Deleted item waits while no Cluster of its name exists, or while one is
// being deleted, and otherwise sorts the live Cluster into one of three kinds:
//
//   - The old Cluster, whose UID is the recorded one: the delete failed, or
//     the controller stopped after the mark was written. The run deletes it
//     again, or Skips the item when the Cluster opted out or declared its own
//     bootstrap since. The webhook's backup.wlz.li/restore-run annotation
//     stays on a recovered Cluster for good, so it says nothing on the old
//     Cluster.
//   - This run's recovery: another UID and the annotation naming this run.
//     The item moves to Recovering.
//   - Any other Cluster: one created again empty by the owner's choice, with
//     its own bootstrap, archiving nowhere, recovered for another run, or not
//     seen by the webhook. The item fails with a message saying so (see
//     notRecovered), and the run leaves the Cluster alone. It did not create
//     that Cluster, and deleting it would only loop as Flux creates it again.
//     An item without a recorded UID has no old Cluster, so any live Cluster
//     that is not the run's recovery lands here.
//
// A Recovering item succeeds once the Cluster reports the healthy phase and
// still carries the run's mark, and fails when the recovered Cluster is
// deleted or replaced by one without the mark.
func (r *RestoreRunReconciler) restoreDatabase(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem) error {
	switch item.Phase {
	case backupv1alpha1.ItemPending:
		cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		if err != nil {
			return err
		}
		if found {
			if why := leftAlone(cluster); why != "" {
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, why
				return nil
			}
		}
		// Two runs that planned in the same instant both passed this check
		// at their plan. Checked again right before the mark, the second
		// run finds the first one's item Pending or Deleted and deletes
		// nothing; of two runs that get here together, each finds the other
		// Pending and neither deletes.
		err = r.clustersRestoredElsewhere(ctx, run, []backupv1alpha1.RestoreItem{*item})
		if failRestoreItem(item, nothingDeleted(err)) {
			return nil
		}
		if err != nil {
			return err
		}
		if found {
			item.ClusterUID = cluster.GetUID()
		}
		item.Phase = backupv1alpha1.ItemDeleted
		if err := r.writeStatus(ctx, run); err != nil {
			return err
		}
		if !found {
			return nil
		}
		return r.deleteCluster(ctx, cluster)

	case backupv1alpha1.ItemDeleted:
		cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		switch {
		case err != nil:
			return err
		case !found || cluster.GetDeletionTimestamp() != nil:
			return nil
		case item.ClusterUID != "" && cluster.GetUID() == item.ClusterUID:
			// The old Cluster, which the earlier delete did not reach. Its
			// annotations say nothing about this run.
			if why := leftAlone(cluster); why != "" {
				// Marked to be left alone before the delete reached it.
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, why
				return nil
			}
			return r.deleteCluster(ctx, cluster)
		case cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun] == run.Name:
			item.Phase = backupv1alpha1.ItemRecovering
		default:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, notRecovered(item, cluster)
		}

	case backupv1alpha1.ItemRecovering:
		cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		switch {
		case err != nil:
			return err
		case !found || cluster.GetDeletionTimestamp() != nil:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, "the recovered Cluster was deleted"
		case cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun] != run.Name:
			// The webhook recovers a Cluster only for a run whose item says
			// Deleted, so one created again now carries no mark of this run.
			item.Phase, item.Message = backupv1alpha1.ItemFailed, "the recovered Cluster was replaced by one this run did not recover"
		case cnpg.Phase(cluster) == cnpg.HealthyPhase:
			item.Phase = backupv1alpha1.ItemSucceeded
		}
	default:
		// An item in any other phase has finished, so there is nothing
		// left to restore.
	}
	return nil
}

// notRecovered explains why a live Cluster that a Deleted item finds is not
// the run's recovery, for the item's message. The caller has already ruled
// out the old Cluster (the recorded UID) and the run's own recovery (the
// backup.wlz.li/restore-run annotation naming the run).
//
// Parameters:
//   - item is the run's Deleted database item. An empty item.ClusterUID
//     means the run can't tell the old Cluster from a new one.
//   - cluster is the live Cluster of the item's name.
//
// With a recorded UID, a Cluster that opted out of the bootstrap webhook or
// declares its own bootstrap came back without a recovery, and the message
// says nothing was restored and how to recover it. Any other Cluster gets
// "came back without this run's recovery" with the reason: it archives
// nowhere, another RestoreRun recovered it, or the webhook did not mark it.
// Without a recorded UID the message gives the same reason and says the run
// can't tell whether this is the Cluster it meant to delete. Every message
// says the run leaves the Cluster alone.
func notRecovered(item *backupv1alpha1.RestoreItem, cluster *unstructured.Unstructured) string {
	name, uid := cluster.GetName(), cluster.GetUID()
	optedOut := cluster.GetAnnotations()[bootstrap.OptOutAnnotation] == bootstrap.OptOutValue
	method := bootstrap.OwnerBootstrap(cluster)

	var why string
	switch other := cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreRun]; {
	case optedOut:
		why = fmt.Sprintf("it carries %s: %s", bootstrap.OptOutAnnotation, bootstrap.OptOutValue)
	case method != "":
		why = fmt.Sprintf("it declares its own spec.bootstrap.%s", method)
	case !archives(cluster):
		why = "it archives nowhere"
	case other != "":
		why = fmt.Sprintf("RestoreRun %s recovered it", other)
	default:
		why = "the bootstrap webhook did not mark it"
	}

	if item.ClusterUID == "" {
		return fmt.Sprintf("Cluster %s (UID %s) is not this run's recovery: %s. The item holds no clusterUID, "+
			"because the run found no Cluster at its start, so it can't tell the old Cluster from a new one, "+
			"and it leaves this one alone. Create a new RestoreRun to restore it", name, uid, why)
	}
	switch {
	case optedOut:
		return fmt.Sprintf("Cluster %s came back carrying %s: %s after this run deleted it, so it started empty and nothing was restored. "+
			"Remove the annotation from its manifest and create a new RestoreRun to recover it. The run leaves the Cluster alone",
			name, bootstrap.OptOutAnnotation, bootstrap.OptOutValue)
	case method != "":
		return fmt.Sprintf("Cluster %s came back declaring spec.bootstrap.%s after this run deleted it, so it started from that bootstrap and nothing was restored. "+
			"Remove spec.bootstrap.%s from its manifest and create a new RestoreRun to recover it. The run leaves the Cluster alone",
			name, method, method)
	}
	return fmt.Sprintf("Cluster %s (UID %s) came back without this run's recovery: %s. The run does not delete a Cluster it did not recover",
		name, uid, why)
}

// archives reports whether a Cluster archives its WAL through the
// barman-cloud plugin (see bootstrap.Archiver). The webhook admits a Cluster
// that archives nowhere unchanged, so such a Cluster is never a recovery.
func archives(cluster *unstructured.Unstructured) bool {
	_, _, found := bootstrap.Archiver(cluster)
	return found
}

// deleteCluster deletes the Cluster the run read. The delete carries the
// Cluster's UID as a precondition, so it never reaches a Cluster of the same
// name created since the read. A Cluster that is already gone is not an
// error. The delete goes out at the version that cnpg.GetCluster read the
// Cluster at. The API server can stop to serve that version after the read.
// If it then refuses the delete, the result is an error (see
// served.VersionGone). It is never a Cluster that is gone.
func (r *RestoreRunReconciler) deleteCluster(ctx context.Context, cluster *unstructured.Unstructured) error {
	uid := cluster.GetUID()
	err := served.VersionGone(r.RESTMapper(), cluster.GroupVersionKind(), r.Delete(ctx, cluster, client.Preconditions{UID: &uid}))
	if err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Cluster %s/%s: %w", cluster.GetNamespace(), cluster.GetName(), err)
	}
	return nil
}

// clusterLeftDeleted returns the note for a Cluster the run deleted and
// ended without: no run waits for the Cluster any more, so the bootstrap
// webhook recovers its next creation to the end of its archive, or to the
// time in the Cluster's own backup.wlz.li/restore-as-of annotation (see
// bootstrap.Decider.Handle, whose waitingRun passes over a finished run and
// a run being deleted).
//
// Parameters:
//   - cluster is the name of the Cluster, which the note names.
func clusterLeftDeleted(cluster string) string {
	return fmt.Sprintf("Cluster %s was deleted, and the run ended before it was created again. No run waits for it now, "+
		"so when Flux or tofu creates it, the bootstrap webhook recovers it to the end of its archive, or to the time in its own %s annotation; "+
		"the moment this run chose no longer applies", cluster, backupv1alpha1.AnnotationRestoreAsOf)
}

// leftAlone says why a run must not restore a Cluster, and returns an empty
// string when it may. The run never deletes a Cluster whose next creation it
// can't turn into its recovery:
//
//   - A Cluster carrying backup.wlz.li/bootstrap: initdb opts out of the
//     bootstrap webhook. The webhook lets it through empty and never marks it
//     as a run's recovery, so a run that deleted it would see it come back
//     empty and delete it again until the timeout.
//   - A Cluster whose owner declares a bootstrap method, such as
//     pg_basebackup or a recovery of their own (see bootstrap.OwnerBootstrap),
//     comes back with that method. The webhook refuses it while the run waits,
//     so the database would stay down until the timeout and nothing would be
//     restored.
//
// The run asks this of a Cluster it has not deleted: at its checks, at a
// Pending item, and of the old Cluster (the recorded UID) that a Deleted item
// finds still there. A Cluster created again after the delete is never
// Skipped, since it came back without the restore; restoreDatabase fails
// that item (see notRecovered). The returned text is the item's message.
func leftAlone(cluster *unstructured.Unstructured) string {
	if cluster.GetAnnotations()[bootstrap.OptOutAnnotation] == bootstrap.OptOutValue {
		return fmt.Sprintf("the Cluster carries %s: %s, which asks for an empty database, so the run leaves it alone",
			bootstrap.OptOutAnnotation, bootstrap.OptOutValue)
	}
	if method := bootstrap.OwnerBootstrap(cluster); method != "" {
		return fmt.Sprintf("the Cluster declares its own spec.bootstrap.%s, so the run leaves it alone", method)
	}
	return ""
}
