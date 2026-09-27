package runs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// work makes one pass over an in-place run that plan has checked, and
// returns when to look again.
//
// Parameters:
//   - run is the RestoreRun, in phase Running or Waiting with its items
//     planned. work updates its status in place and writes it.
//
// It returns a result that requeues the run while it has work left, and an
// empty result once the run has ended (see finish). A failed read, write or
// delete comes back as an error, and controller-runtime retries the pass.
//
// A run past spec.timeout with an item still unfinished ends with reason
// TimedOut (see timeOut), and a message that names the SourceBusy wait it
// was in. A run whose items have all finished only waits for its stopped
// movers to go before it gives the app back, so a deadline that passes
// during that wait leaves it waiting, and it ends as its items say. With
// spec.quiesce set, the first passes call quiesce to stop the listed
// workloads, and later passes restore nothing until every pod of those
// workloads is gone. work then moves each volume item a step further (see
// restoreVolume), and stops the restore Job of each item that has finished
// once its end is in the status (see stopJobs). The databases wait
// until every volume item is done; when a volume restore failed, the
// databases still Pending are skipped and left running, and otherwise each
// is moved a step further (see restoreDatabase).
//
// While a mover the run stopped is not gone yet, the run waits with reason
// WaitingForShutdown and gives nothing back (rule X2). After a Cluster is
// deleted, work also waits with reason WaitingForShutdown until the old
// Cluster's instance pods and PVCs are gone (see cnpg.InstanceLeft). Only then does
// it start the stopped workloads again and resume the Kustomizations it
// suspended, and it waits with reason WaitingForRecreate, whose message asks
// for the Cluster to be created again. The run finishes once every item is
// Succeeded, Failed or Skipped: Failed when an item failed, Failed with
// reason NoBackupInReach when every item was Skipped (see nothingRestored),
// and Succeeded otherwise. At the start of each pass work releases the Leases
// of the items that finished in an earlier pass and whose movers are gone:
// an item that still names a restore Job has one Stop has not reported
// stopped (see restoreItemDone).
//
// While clusterWebhookBlind reports that the bootstrap webhook would not see
// a Cluster created again, work deletes no Cluster: it fails every Pending
// Cluster item (see failBlindClusters), a run that ends in the pass that
// failed such an item ends with reason ClusterVersionUnsupported (see
// failedReason), and a run that waits for a deleted Cluster waits with that
// reason and the message from recreateMessage.
func (r *RestoreRunReconciler) work(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	// A run whose items have all finished only waits to give back what it
	// holds, and that wait is bounded by its movers (rule X2), so it ends as
	// its items say.
	if deadline, over := r.overdue(run); over && !restoreDone(run.Status.Items) {
		return r.timeOut(ctx, run, timedOutMessage(deadline, run.Status.Conditions))
	}
	// The Leases of an item that finished in an earlier pass go now, so a
	// backup of that claim need not wait for the rest of the run. An item
	// whose stopped restore Job may still write keeps its Lease: another
	// run's mover must not start on the claim or the repository meanwhile
	// (rule X2, see restoreItemDone).
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(name string) bool {
		return restoreItemDone(run, name)
	}); err != nil {
		return ctrl.Result{}, err
	}
	// The quiesce Leases go once the stored status shows every workload back,
	// since the run then never touches them again. Best effort: a Lease left
	// behind is stale under holderLive's rule and the next run takes it over.
	if durablyRestarted(run) {
		if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
			log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the run goes on",
				"namespace", run.Namespace, "name", run.Name)
		}
	}

	if len(run.Spec.Quiesce) > 0 && run.Status.QuiescedAt == nil {
		return r.quiesce(ctx, run)
	}
	if stopped(run) && anyRestorePending(run.Status.Items) {
		targets, err := quiesce.Named(ctx, r.Reader, run.Namespace, run.Spec.Quiesce)
		if err != nil {
			if !asRunRefusal(err) {
				return ctrl.Result{}, err
			}
			return r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
		}
		gone, pod, err := quiesce.PodsGone(ctx, r.Reader, run.Namespace, targets)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			return after(2*time.Second, r.waitFor(ctx, run, backupv1alpha1.ReasonRunning,
				fmt.Sprintf("waiting for pod %s to stop before anything is restored", pod)))
		}
	}

	waitReason, waitMessage := "", ""
	volumesDone, volumesFailed := true, false
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != "PersistentVolumeClaim" {
			continue
		}
		reason, message, err := r.restoreVolume(ctx, run, i, item)
		if err != nil {
			return ctrl.Result{}, err
		}
		if reason != "" {
			waitReason, waitMessage = reason, message
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning:
			volumesDone = false
		case backupv1alpha1.ItemFailed:
			volumesFailed = true
		default:
			// A volume item in any other phase has finished without a
			// failure, so it changes neither flag.
		}
	}

	// A finished volume item's phase goes into the status before its restore
	// Job is stopped. The Job's conditions are the only record of how the
	// restore ended. Deleted first, a lost status write or a crash would
	// leave a Running item whose Job is gone, and no later pass could tell
	// how it ended. A later pass stops the Job of any finished item that
	// still names one (see stopJobs). The status is written only when it
	// differs from the stored one: a pass that waits for the same stopped
	// Job as the pass before has its items stored already, and a write
	// would only start another reconcile.
	var stopping jobList
	if finishedWithMover(run.Status.Items) {
		if err := r.writeChangedStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		left, err := r.stopJobs(ctx, run, finished)
		if err != nil {
			return ctrl.Result{}, err
		}
		stopping = left
	}

	// A Cluster the webhook would not see created again comes back empty,
	// so no Cluster is deleted while that holds (see clusterWebhookBlind).
	blind := clusterWebhookBlind(r.RESTMapper())
	blindFailed := failBlindClusters(run.Status.Items, blind)
	if len(blindFailed) > 0 {
		log.FromContext(ctx).Error(errors.New(blind.message()), "deleting no Cluster: the bootstrap webhook would not see it created again",
			"namespace", run.Namespace, "name", run.Name)
	}

	var recreate, shuttingDown []string
	if volumesDone {
		for i := range run.Status.Items {
			item := &run.Status.Items[i]
			if item.Kind != "Cluster" {
				continue
			}
			if volumesFailed && item.Phase == backupv1alpha1.ItemPending {
				item.Phase, item.Message = backupv1alpha1.ItemSkipped, "left running because a volume restore failed"
				continue
			}
			if err := r.restoreDatabase(ctx, run, item); err != nil {
				return ctrl.Result{}, err
			}
			if item.Phase == backupv1alpha1.ItemDeleted {
				left, err := cnpg.InstanceLeft(ctx, r.Reader, run.Namespace, item.Name)
				if err != nil {
					return ctrl.Result{}, err
				}
				if left != "" {
					shuttingDown = append(shuttingDown, left)
				} else {
					recreate = append(recreate, item.Name)
				}
			}
		}
	}

	// A restore Job the run stopped may still write into a claim or the
	// repository, so the run gives nothing back and releases no Lease it
	// still holds until no pod of that Job can write (rule X2, see
	// stopJobs). Only a finished item counts here: the Job of an item that
	// is still Pending or Running is doing the restore, and belongs there.
	if len(stopping) > 0 {
		return r.waitForStopped(ctx, run, stopping.message())
	}

	// A database comes back only when its owner creates it again, and a
	// Kustomization this run suspended creates nothing. So the app is given
	// back once every volume is restored and every database is deleted, down
	// to its last instance pod and PVC. A run whose status shows the restart
	// done starts nothing again, so it never scales up a workload another run
	// has stopped since.
	if volumesDone && !anyRestorePending(run.Status.Items) && len(shuttingDown) == 0 && stopped(run) {
		if err := r.restart(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}

	if restoreDone(run.Status.Items) {
		if failed := restoreFailures(run.Status.Items); failed != "" {
			return r.finish(ctx, run, failedReason(blindFailed), failed)
		}
		if skipped := nothingRestored(run.Status.Items); skipped != "" {
			return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, skipped)
		}
		return r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, "every item holds the restored data")
	}

	switch {
	case len(shuttingDown) > 0:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonShutdown,
			fmt.Sprintf("waiting for %s of the deleted Cluster to be gone before anything creates it again", strings.Join(shuttingDown, ", "))))
	case len(recreate) > 0 && blind.blind():
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonClusterVersionUnsupported,
			blind.recreateMessage(recreate)))
	case len(recreate) > 0:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonRecreate,
			fmt.Sprintf("recreate %s to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it", strings.Join(recreate, ", "))))
	case waitReason != "":
		return after(pollInterval, r.waitFor(ctx, run, waitReason, waitMessage))
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "restoring")
	return after(pollInterval, r.writeStatus(ctx, run))
}
