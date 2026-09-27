package runs

import (
	"context"
	"fmt"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// work makes one pass over a run that plan or planIntoNewClaim has checked,
// and returns when to look again. An into restore has one volume item and
// stops no workload.
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
// was in (intoTimedOut for an into restore). A run whose items have all finished only waits for its stopped
// movers to go before it gives the app back, so a deadline that passes
// during that wait leaves it waiting, and it ends as its items say. The
// first passes call quiesce to stop the workloads spec.quiesce lists and the
// ones marked backup.wlz.li/quiesce (see quiesceFirst), and later passes
// restore nothing until every pod of those workloads is gone. work then
// moves each volume item a step further (see restoreVolume), and stops the
// restore Job of each item that has finished once its end is in the status
// (see stopJobs). The databases wait
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
// While clusterWebhookUnserved reports that the bootstrap webhook would not
// see a Cluster created again, work deletes no Cluster: it fails every
// Pending Cluster item with reason ClusterVersionUnsupported (see
// failBlindClusters). A run with such a Failed item ends with reason
// ClusterVersionUnsupported in whichever pass it ends (see endReason). A run
// that waits for a deleted Cluster waits with that reason and the message
// from recreateMessage.
func (r *RestoreRunReconciler) work(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if done, result, err := r.endIfOverdue(ctx, run); done {
		return result, err
	}
	// An item whose stopped restore Job may still write keeps its Lease:
	// another run's mover must not start on the claim or the repository
	// meanwhile (rule X2, see restoreItemDone).
	if err := r.ops().releaseFinished(ctx, fieldsOf(run), func(name string) bool { return restoreItemDone(run, name) }); err != nil {
		return ctrl.Result{}, err
	}
	if done, result, err := r.quiesceFirst(ctx, run); done {
		return result, err
	}
	if done, result, err := r.waitForStoppedPods(ctx, run); done {
		return result, err
	}
	volumes, err := r.restoreVolumes(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	stopping, err := r.stopFinishedJobs(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	databases, err := r.restoreDatabases(ctx, run, volumes)
	if err != nil {
		return ctrl.Result{}, err
	}
	// A restore Job the run stopped may still write into a claim or the
	// repository, so the run gives nothing back and releases no Lease it
	// still holds until no pod of that Job can write (rule X2, see
	// stopJobs). Only a finished item counts here: the Job of an item that
	// is still Pending or Running is doing the restore, and belongs there.
	if stopping != "" {
		return r.waitForStopped(ctx, run, stopping)
	}
	if err := r.giveBackWhenDone(ctx, run, volumes, databases); err != nil {
		return ctrl.Result{}, err
	}
	return r.finishOrWait(ctx, run, volumes, databases)
}

// volumePass is what restoreVolumes found over the volume items in one pass.
type volumePass struct {
	// done is true when no volume item is Pending or Running.
	done bool
	// failed is true when a volume item is Failed.
	failed bool
	// wait is the hold of the last Pending volume item that waits, or the
	// zero hold when none waits.
	wait hold
}

// databasePass is what restoreDatabases found over the Cluster items in one
// pass.
type databasePass struct {
	// unserved is what clusterWebhookUnserved found in this pass, or nil.
	unserved *UnservedError
	// recreate names each deleted Cluster whose old instance pods and PVCs
	// are gone, so its owner can create it again.
	recreate []string
	// shuttingDown names what is left of each deleted Cluster (see
	// cnpg.InstanceLeft).
	shuttingDown []string
}

// endIfOverdue ends a run past spec.timeout that has an unfinished item.
//
// Parameters:
//   - run is the RestoreRun in its work pass.
//
// It returns done true, with the result and error of timeOut, when the run
// ends here, and done false otherwise. A run whose items have all finished
// only waits to give back what it holds, and that wait is bounded by its
// movers (rule X2), so it ends as its items say.
func (r *RestoreRunReconciler) endIfOverdue(ctx context.Context, run *backupv1alpha1.RestoreRun) (done bool, result ctrl.Result, err error) {
	deadline, over := r.overdue(run)
	if !over || restoreDone(run.Status.Items) {
		return false, ctrl.Result{}, nil
	}
	message := timedOutMessage(deadline, run.Status.Conditions)
	if run.Spec.Into != "" {
		message = intoTimedOut(run.Spec.Into, deadline, run.Status.Conditions)
	}
	result, err = r.timeOut(ctx, run, message)
	return true, result, err
}

// waitForStoppedPods keeps a run that stopped its workloads from restoring
// while a pod of them still runs.
//
// Parameters:
//   - run is the RestoreRun in its work pass.
//
// It returns done false when the run stopped nothing, has no Pending item,
// or every pod of its workloads is gone. It returns done true while a pod
// is left, with a result that looks again in two seconds, and when the
// workloads can not be read: a refusal (see asRunRefusal) aborts the run
// with reason Failed, and any other error comes back for a retry.
func (r *RestoreRunReconciler) waitForStoppedPods(ctx context.Context, run *backupv1alpha1.RestoreRun) (done bool, result ctrl.Result, err error) {
	if !stopped(fieldsOf(run)) || !anyItemIn(run.Status.Items, backupv1alpha1.ItemPending) {
		return false, ctrl.Result{}, nil
	}
	targets, err := quiesce.NamedAndMarked(ctx, r.Reader, run.Namespace, run.Spec.Quiesce)
	if err != nil {
		if !asRunRefusal(err) {
			return true, ctrl.Result{}, err
		}
		result, err = r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
		return true, result, err
	}
	return r.ops().waitForPods(ctx, fieldsOf(run), targets, "anything is restored")
}

// restoreVolumes moves each volume item a step further (see restoreVolume).
//
// Parameters:
//   - run is the RestoreRun in its work pass. Its volume items change in
//     place.
//
// It returns what it found over the volume items, and an error when a step
// fails for a reason a retry may fix.
func (r *RestoreRunReconciler) restoreVolumes(ctx context.Context, run *backupv1alpha1.RestoreRun) (volumePass, error) {
	pass := volumePass{done: true}
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindClaim {
			continue
		}
		wait, err := r.restoreVolume(ctx, run, i, item)
		if err != nil {
			return pass, err
		}
		if wait.held() {
			pass.wait = wait
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning:
			pass.done = false
		case backupv1alpha1.ItemFailed:
			pass.failed = true
		default:
			// A volume item in any other phase has finished without a
			// failure, so it changes neither flag.
		}
	}
	return pass, nil
}

// stopFinishedJobs stops the restore Job of each item that has finished.
//
// Parameters:
//   - run is the RestoreRun in its work pass.
//
// It returns the wait message of stopJobs while a Job is not stopped yet,
// and an error when the status write or a stop fails.
//
// A finished volume item's phase goes into the status before its restore
// Job is stopped. The Job's conditions are the only record of how the
// restore ended. Deleted first, a lost status write or a crash would
// leave a Running item whose Job is gone, and no later pass could tell
// how it ended. A later pass stops the Job of any finished item that
// still names one (see stopJobs). The status is written only when it
// differs from the stored one: a pass that waits for the same stopped
// Job as the pass before has its items stored already, and a write
// would only start another reconcile.
func (r *RestoreRunReconciler) stopFinishedJobs(ctx context.Context, run *backupv1alpha1.RestoreRun) (string, error) {
	if !finishedWithMover(run.Status.Items) {
		return "", nil
	}
	if err := r.writeChangedStatus(ctx, run); err != nil {
		return "", err
	}
	return r.stopJobs(ctx, run, finished)
}

// restoreDatabases moves each Cluster item a step further once every
// volume item is done.
//
// Parameters:
//   - run is the RestoreRun in its work pass. Its Cluster items change in
//     place.
//   - volumes is what restoreVolumes found in this pass.
//
// It returns what it found over the Cluster items, and an error when a
// step fails for a reason a retry may fix.
//
// A Cluster the webhook would not see created again comes back empty, so
// no Cluster is deleted while that holds (see clusterWebhookUnserved): each
// Pending Cluster item fails first (see failBlindClusters). When a volume
// restore failed, each Cluster item still Pending is Skipped with reason
// OtherItemFailed and left running. Otherwise each moves a step further
// (see restoreDatabase), and a deleted one waits until its old instance
// pods and PVCs are gone (see cnpg.InstanceLeft).
func (r *RestoreRunReconciler) restoreDatabases(ctx context.Context, run *backupv1alpha1.RestoreRun, volumes volumePass) (databasePass, error) {
	pass := databasePass{unserved: clusterWebhookUnserved(r.RESTMapper())}
	if failBlindClusters(run.Status.Items, pass.unserved) {
		log.FromContext(ctx).Error(pass.unserved, "deleting no Cluster: the bootstrap webhook would not see it created again",
			"namespace", run.Namespace, "name", run.Name)
	}
	if !volumes.done {
		return pass, nil
	}
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindCluster {
			continue
		}
		if err := r.restoreDatabaseItem(ctx, run, item, volumes.failed, &pass); err != nil {
			return pass, err
		}
	}
	return pass, nil
}

// restoreDatabaseItem moves one Cluster item a step further for
// restoreDatabases.
//
// Parameters:
//   - run is the RestoreRun in its work pass.
//   - item is the Cluster item, which changes in place.
//   - volumesFailed is true when a volume restore of the run failed. A
//     Pending item is then Skipped with reason OtherItemFailed.
//   - pass collects the Clusters that are deleted: in pass.shuttingDown
//     while an old instance pod or PVC is left, and in pass.recreate once
//     none is left.
//
// It returns an error when a step fails for a reason a retry may fix.
func (r *RestoreRunReconciler) restoreDatabaseItem(ctx context.Context, run *backupv1alpha1.RestoreRun, item *backupv1alpha1.RestoreItem, volumesFailed bool, pass *databasePass) error {
	if volumesFailed && item.Phase == backupv1alpha1.ItemPending {
		item.Phase, item.Reason = backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonOtherItemFailed
		item.Message = "left running because a volume restore failed"
		return nil
	}
	if err := r.restoreDatabase(ctx, run, item); err != nil || item.Phase != backupv1alpha1.ItemDeleted {
		return err
	}
	left, err := cnpg.InstanceLeft(ctx, r.Reader, run.Namespace, item.Name)
	if err != nil {
		return err
	}
	if left != "" {
		pass.shuttingDown = append(pass.shuttingDown, left)
	} else {
		pass.recreate = append(pass.recreate, item.Name)
	}
	return nil
}

// giveBackWhenDone starts the stopped workloads again and resumes the
// suspended Kustomizations once nothing is left to restore before them.
//
// Parameters:
//   - run is the RestoreRun in its work pass.
//   - volumes and databases are what this pass found.
//
// It returns the error of restart, or nil.
//
// A database comes back only when its owner creates it again, and a
// Kustomization this run suspended creates nothing. So the app is given
// back once every volume is restored and every database is deleted, down
// to its last instance pod and PVC. A run whose status shows the restart
// done starts nothing again, so it never scales up a workload another run
// has stopped since.
func (r *RestoreRunReconciler) giveBackWhenDone(ctx context.Context, run *backupv1alpha1.RestoreRun, volumes volumePass, databases databasePass) error {
	if !volumes.done || anyItemIn(run.Status.Items, backupv1alpha1.ItemPending) || len(databases.shuttingDown) > 0 || !stopped(fieldsOf(run)) {
		return nil
	}
	return r.ops().restart(ctx, fieldsOf(run))
}

// finishOrWait ends a run whose items have all finished, or records what it
// waits for.
//
// Parameters:
//   - run is the RestoreRun in its work pass.
//   - volumes and databases are what this pass found.
//
// It returns what finish returns for a run that ends: Failed when an item
// failed (see endReason), Failed with reason
// NoBackupInReach when no item succeeded (see nothingRestored), and
// Succeeded otherwise. A run that goes on waits, in this order, with reason
// WaitingForShutdown for a deleted Cluster that is not gone yet, with
// reason ClusterVersionUnsupported or WaitingForRecreate for a Cluster to
// create again, and with the hold of a volume item. With nothing to wait
// for, the run stays Running.
func (r *RestoreRunReconciler) finishOrWait(ctx context.Context, run *backupv1alpha1.RestoreRun, volumes volumePass, databases databasePass) (ctrl.Result, error) {
	items := run.Status.Items
	switch {
	case restoreDone(items) && anyItemIn(items, backupv1alpha1.ItemFailed):
		return r.finish(ctx, run, endReason(items), restoreFailures(items))
	case restoreDone(items) && !anyItemIn(items, backupv1alpha1.ItemSucceeded):
		return r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, nothingRestored(items))
	case restoreDone(items):
		return r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, succeededMessage(run))
	case len(databases.shuttingDown) > 0:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonShutdown,
			fmt.Sprintf("waiting for %s of the deleted Cluster to be gone before anything creates it again", strings.Join(databases.shuttingDown, ", "))))
	case len(databases.recreate) > 0 && databases.unserved != nil:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonClusterVersionUnsupported,
			recreateMessage(databases.unserved, databases.recreate)))
	case len(databases.recreate) > 0:
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonRecreate,
			fmt.Sprintf("recreate %s to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it", strings.Join(databases.recreate, ", "))))
	case volumes.wait.held():
		return after(pollInterval, r.waitFor(ctx, run, volumes.wait.readyReason(), volumes.wait.text))
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, runningMessage(run))
	return after(pollInterval, r.writeStatus(ctx, run))
}
