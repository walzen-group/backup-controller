package runs

import (
	"context"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
)

// quiesce stops the workloads spec.quiesce lists, before the run restores
// anything.
//
// Parameters:
//   - run is the RestoreRun, Running with its items planned and no
//     status.quiescedAt yet. quiesce records its plan and the stop in the
//     run's status.
//
// It returns a result that requeues the run: after pollInterval while it
// waits, after a second or two once the stop is recorded. A run it ends
// returns what finish or abort returns. A failed read of an entry or a
// Kustomization, a failed Lease call and a failed status write come back as
// an error, and the pass is retried with nothing stopped that the status
// does not record.
//
// Before it records a plan, with nothing stopped, quiesce fails a Pending
// volume item that restoreVolume would refuse before its restore Job exists
// (see startRefusal) or whose repository Secret is gone, with the message
// restoreVolume gives. It then waits with reason SourceBusy while a backup
// holds one of the run's claims or repositories (see backupHeldElsewhere),
// while another run is in the way (see waitingOn), and while another run
// holds the namespace's quiesce Lease (see acquireQuiesceLease). A run
// left with no Pending item, by that or because the plan Skipped every item,
// stops nothing: quiesce records status.quiescedAt and status.restartedAt at
// the same moment, and work then finishes the run. A spec.quiesce entry the
// namespace does not hold ends the run as Failed with reason Invalid before
// anything is stopped, and a Kustomization that also applies workloads of
// another namespace aborts it with reason Invalid (see quiesce.Plan).
//
// quiesce then records the plan from quiesce.Plan in status.quiesced and
// status.suspendedKustomizations, with each workload's replica count, and
// writes the status. Only then does quiesce.Apply suspend the Kustomizations and
// scale the workloads to zero. A pass that finds a plan in the status reuses
// it, so a retry after a lost status write still gives back the counts the
// workloads had before the run touched them. Before it stops from such a
// plan, quiesce reads the run again through the uncached Reader (see
// stopOwed), and stops nothing when the stored run has recorded the stop,
// given the app back or ended. quiesce then writes status.quiescedAt. After
// a failed stop, it narrows the plan with quiesce.Applied to what is stopped
// now, and aborts the run with reason Failed, which starts those workloads
// again and resumes the Kustomizations, the same as a BackupRun does.
func (r *RestoreRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if done, result, err := r.readyToStop(ctx, run); done {
		return result, err
	}
	stopErr := quiesce.Apply(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	if stopErr != nil {
		run.Status.Quiesced, run.Status.SuspendedKustomizations = quiesce.Applied(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	}
	now := metav1.NewTime(r.Now())
	run.Status.QuiescedAt = &now
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if stopErr != nil {
		return r.abort(ctx, run, backupv1alpha1.ReasonFailed, stopErr.Error())
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// readyToStop makes sure that the run has a plan to stop the app, and
// that the stored run still owes the stop.
//
// Parameters:
//   - run is the RestoreRun, with no status.quiescedAt yet.
//
// It returns done false when quiesce can apply the plan. It returns done
// true, with the result and the error that the pass returns, when the pass
// ends here.
//
// A run with no plan gets one (see planStop). A run with a plan got it from
// the run as the pass read it, and that copy may lag behind a pass that has
// since stopped the app, given it back and ended the run. The stored run
// decides (see stopOwed). A run that no longer owes the stop changes
// nothing and looks again after a second.
func (r *RestoreRunReconciler) readyToStop(ctx context.Context, run *backupv1alpha1.RestoreRun) (done bool, result ctrl.Result, err error) {
	if len(run.Status.Quiesced) == 0 {
		return r.planStop(ctx, run)
	}
	owed, err := stopOwed(ctx, r.Reader, run)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if !owed {
		return true, ctrl.Result{RequeueAfter: time.Second}, nil
	}
	return false, ctrl.Result{}, nil
}

// planStop records the plan of the stop in the run's status, once the
// pre-check and the waits let the run stop the app.
//
// Parameters:
//   - run is the RestoreRun, with no plan recorded yet. planStop fails its
//     items and records its plan in place.
//
// It returns done false once planStop recorded the plan, and quiesce then
// applies it. It returns done true, with the result and the error that the
// pass returns, when the pass ends here: the run ends, waits, has nothing
// to stop, or a read or a write failed.
//
// The steps are the ones quiesce describes: the spec.quiesce entries (see
// quiesce.Named), the pre-check of the items (see precheckItems), the
// record of a run with no Pending item, the waits (see waitingOn and
// acquireQuiesceLease), and the plan (see quiesce.Plan).
func (r *RestoreRunReconciler) planStop(ctx context.Context, run *backupv1alpha1.RestoreRun) (done bool, result ctrl.Result, err error) {
	targets, err := quiesce.Named(ctx, r.Reader, run.Namespace, run.Spec.Quiesce)
	if asRunRefusal(err) {
		result, err = r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
		return true, result, err
	}
	if err != nil {
		return true, ctrl.Result{}, err
	}
	held, err := r.precheckItems(ctx, run)
	if err != nil || held.held() {
		result, err = r.waitAtQuiesce(ctx, run, held, err)
		return true, result, err
	}
	// With no item left to restore once the checks above failed the
	// rest, or every item Skipped at the plan, there is nothing to stop.
	// The run records the stop and the restart at the same moment, and
	// work then finishes it.
	if !anyRestorePending(run.Status.Items) {
		now := metav1.NewTime(r.Now())
		run.Status.QuiescedAt, run.Status.RestartedAt = &now, &now
		result, err = after(time.Second, r.writeStatus(ctx, run))
		return true, result, err
	}
	// A restore that waits for the Cluster it deleted keeps this run
	// waiting with nothing stopped and no Lease held (see waitingOn).
	held, err = waitingOn(ctx, r.Reader, run)
	if err != nil || held.held() {
		result, err = r.waitAtQuiesce(ctx, run, held, err)
		return true, result, err
	}
	// The namespace's quiesce Lease lets one run at a time stop its
	// workloads. It is taken before the plan and held until the stored
	// status shows the workloads back, so a second run waits here with
	// the app running rather than recording the count the first stopped
	// it at.
	held, err = acquireQuiesceLease(ctx, r.Client, r.Reader, run, backupv1alpha1.KindRestoreRun)
	if err != nil || held.held() {
		result, err = r.waitAtQuiesce(ctx, run, held, err)
		return true, result, err
	}
	return r.recordPlan(ctx, run, targets)
}

// recordPlan plans the stop of the workloads and writes the plan to the
// run's status.
//
// Parameters:
//   - run is the RestoreRun, which holds the namespace's quiesce Lease.
//     recordPlan records the plan in status.quiesced and
//     status.suspendedKustomizations.
//   - targets are the workloads spec.quiesce names (see quiesce.Named).
//
// It returns done false once recordPlan wrote the plan. It returns done
// true when quiesce.Plan refuses a Kustomization that also applies
// workloads of another namespace, with what abort returns (reason
// Invalid), and when a read or the status write fails, with the error.
func (r *RestoreRunReconciler) recordPlan(ctx context.Context, run *backupv1alpha1.RestoreRun, targets []quiesce.Workload) (done bool, result ctrl.Result, err error) {
	stop, suspend, err := quiesce.Plan(ctx, r.Reader, r.RESTMapper(), run.Namespace, targets)
	if asRunRefusal(err) {
		result, err = r.abort(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
		return true, result, err
	}
	if err != nil {
		return true, ctrl.Result{}, err
	}
	run.Status.Quiesced, run.Status.SuspendedKustomizations = stop, suspend
	if err := r.writeStatus(ctx, run); err != nil {
		return true, ctrl.Result{}, err
	}
	return false, ctrl.Result{}, nil
}

// waitAtQuiesce ends a quiesce pass that has to wait or that failed.
//
// Parameters:
//   - run is the RestoreRun, with nothing stopped.
//   - held is why the run waits. waitAtQuiesce reads it only when err is nil.
//   - err is the error of the check that ended the pass, or nil.
//
// It returns err as it is when err is not nil. Otherwise it moves the run
// to Waiting with the reason and the text of held (see waitFor), and
// returns a result that looks again after pollInterval.
func (r *RestoreRunReconciler) waitAtQuiesce(ctx context.Context, run *backupv1alpha1.RestoreRun, held hold, err error) (ctrl.Result, error) {
	if err != nil {
		return ctrl.Result{}, err
	}
	return after(pollInterval, r.waitFor(ctx, run, held.readyReason(), held.text))
}

// precheckItems fails each Pending volume item that restoreVolume would
// refuse, before the run stops anything.
//
// Parameters:
//   - run is the RestoreRun, with nothing stopped. precheckItems fails its
//     items in place.
//
// It returns the hold of the first item that a backup holds (see
// backupHeldElsewhere), and the zero hold when no backup holds an item. A
// failed read comes back as an error, and the pass retries with nothing
// stopped.
//
// A backup of one of the run's claims or repositories that is in progress
// would make the restore wait with the app down, from restoreVolume on.
// The run waits here instead, with the workloads still running.
// restoreVolume keeps its own check: this one is advisory, and the check at
// the mover object is the one that counts. An item restoreVolume would
// refuse before its restore Job exists (see startRefusal), and one whose
// repository Secret is gone, fail now with the message restoreVolume gives,
// so the app is not stopped for a restore that cannot start.
func (r *RestoreRunReconciler) precheckItems(ctx context.Context, run *backupv1alpha1.RestoreRun) (hold, error) {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindClaim || item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		_, err := r.startRefusal(ctx, run, item.Name)
		if failRestoreItem(item, err) {
			continue
		}
		if err != nil {
			return hold{}, err
		}
		held, err := r.backupHeldElsewhere(ctx, run, item.Name)
		if failRestoreItem(item, nothingWrittenTo(item.Name, err)) {
			continue
		}
		if err != nil || held.held() {
			return held, err
		}
	}
	return hold{}, nil
}

// backupHeldElsewhere returns a hold of kind holdSourceBusy that names the
// run that holds the claim or its repository. It returns the zero hold when
// no run holds either. A restore calls it before it stops any workload, so
// that it waits with the app running where restoreVolume would wait with
// the app down.
//
// Parameters:
//   - run is the asking run; its namespace and UID are read. spec.repository
//     and spec.moverSecurityContext are used the way repositoryFor uses them.
//   - claimName names the claim the item restores.
//
// A refusal from repositoryFor, for a claim or a VolumeRestore that is gone,
// gives the zero hold. quiesce already failed such an item with startRefusal
// just before, and restoreVolume fails an item that became one since. A
// repository Secret that does not exist comes back as the refusal that
// leaseNamesFor gives, and quiesce fails the item with it (see
// failRestoreItem) before it stops anything. Any other failed read comes
// back as an error, and the pass retries with nothing stopped.
//
// The check is advisory. A run that starts its mover between this read and
// the stop still goes first under the Leases and otherMover, which run right
// before the mover object is written.
func (r *RestoreRunReconciler) backupHeldElsewhere(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName string) (hold, error) {
	settings, err := repositoryFor(ctx, r.Reader, run.Namespace, claimName, run.Spec.Repository, run.Spec.MoverSecurityContext)
	if err != nil {
		if _, refused := asItemFailure(err); refused {
			return hold{}, nil
		}
		return hold{}, err
	}
	backing, err := otherMover(ctx, r.Reader, run.Namespace, claimName, settings.Secret, backupMover)
	if err != nil {
		return hold{}, err
	}
	if backing.held() {
		return backing, nil
	}
	return leaseHeldElsewhere(ctx, r.Reader, run, run.Namespace, claimName, settings.Secret)
}

// restart gives the stopped workloads their replicas back, resumes the
// Kustomizations the run suspended, and records status.restartedAt. The
// caller writes the status.
//
// Parameters:
//   - run is the RestoreRun whose status shows it holds the app stopped (see
//     stopped).
//
// The run's copy may come from an informer cache that lags behind the run's
// own last writes, so restart first reads the stored run again (see
// readStop). When the stored run shows the restart done, restart starts
// nothing and returns nil: another run may have stopped the app since. A
// failed read, and a failed restart, come back as the error.
func (r *RestoreRunReconciler) restart(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if _, err := readStop(ctx, r.Reader, run); err != nil {
		return err
	}
	if !stopped(run) {
		return nil
	}
	if err := quiesce.Restart(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
		return err
	}
	now := metav1.NewTime(r.Now())
	run.Status.RestartedAt = &now
	return nil
}

// stopped reports whether the run has stopped workloads and not yet started
// them again. A run that recorded its plan to stop them counts as having
// stopped them, even before status.quiescedAt is set, because the pass that
// wrote the plan may have stopped them and then lost its status write.
func stopped(run *backupv1alpha1.RestoreRun) bool {
	return (run.Status.QuiescedAt != nil || len(run.Status.Quiesced) > 0) && run.Status.RestartedAt == nil
}
