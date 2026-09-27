package runs

import (
	"context"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	ctrl "sigs.k8s.io/controller-runtime"
)

// quiesceFirst stops the workloads of the app before the run restores
// anything in place: the ones spec.quiesce lists and the ones marked
// backup.wlz.li/quiesce, which a BackupRun of the namespace stops (see
// quiesce.NamedAndMarked). A volume restored into its own claim and a
// Cluster that the restore deletes both change the data under the running
// app. An into restore does not come here (see restoreIntoEmptyClaim).
//
// Parameters:
//   - run is the RestoreRun in its work pass.
//
// It returns done false when the run has recorded status.quiescedAt, and
// when it has no plan and nothing to stop: that run restores with the app
// as it is, and looks again at the next pass. It returns done true, with
// what quiesce returns, while the run has workloads to stop. A spec.quiesce
// entry the namespace does not hold ends the run as Failed with reason
// Invalid before anything is stopped. Any other failed read comes back as
// an error, and the pass is retried.
func (r *RestoreRunReconciler) quiesceFirst(ctx context.Context, run *backupv1alpha1.RestoreRun) (done bool, result ctrl.Result, err error) {
	if run.Status.QuiescedAt != nil {
		return false, ctrl.Result{}, nil
	}
	var targets []quiesce.Workload
	if len(run.Status.Quiesced) == 0 {
		targets, err = quiesce.NamedAndMarked(ctx, r.Reader, run.Namespace, run.Spec.Quiesce)
		if asRunRefusal(err) {
			result, err = r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
			return true, result, err
		}
		if err != nil {
			return true, ctrl.Result{}, err
		}
		if len(targets) == 0 {
			return false, ctrl.Result{}, nil
		}
	}
	result, err = r.quiesce(ctx, run, targets)
	return true, result, err
}

// quiesce stops the workloads of the app before the run restores anything
// (see runOps.quiesce).
//
// Parameters:
//   - run is the RestoreRun, Running with its items planned and no
//     status.quiescedAt yet.
//   - targets are the workloads to stop (see quiesceFirst).
//
// Before it records a plan, with nothing stopped, quiesce checks the Pending
// volume items (see precheckItems). A failed stop aborts the run with reason
// Failed, which starts the stopped workloads again and resumes the
// Kustomizations, the same as a BackupRun does.
func (r *RestoreRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.RestoreRun, targets []quiesce.Workload) (ctrl.Result, error) {
	return r.ops().quiesce(ctx, fieldsOf(run), targets, stopSteps{
		precheck: func(ctx context.Context) (hold, error) { return r.precheckItems(ctx, run) },
		abort: func(ctx context.Context, reason, message string) (ctrl.Result, error) {
			return r.abort(ctx, run, reason, message)
		},
	})
}

// ops returns the reconciler's client, Reader and clock for the steps both
// kinds of run share.
func (r *RestoreRunReconciler) ops() runOps {
	return runOps{c: r.Client, reader: r.Reader, now: r.Now}
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
		settings, err := r.startRefusal(ctx, run, item.Name)
		if failRestoreItem(item, err) {
			continue
		}
		if err != nil {
			return hold{}, err
		}
		held, err := r.backupHeldElsewhere(ctx, run, item.Name, settings.Secret)
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
//   - run is the asking run; its namespace and UID are read.
//   - claimName names the claim the item restores.
//   - secret names the repository Secret, from the settings startRefusal
//     read for the item.
//
// A repository Secret that does not exist comes back as the refusal that
// leaseNamesFor gives, and quiesce fails the item with it (see
// failRestoreItem) before it stops anything. Any other failed read comes
// back as an error, and the pass retries with nothing stopped.
//
// The check is advisory. A run that starts its mover between this read and
// the stop still goes first under the Leases and otherMover, which run right
// before the mover object is written.
func (r *RestoreRunReconciler) backupHeldElsewhere(ctx context.Context, run *backupv1alpha1.RestoreRun, claimName, secret string) (hold, error) {
	backing, err := otherMover(ctx, r.Reader, run.Namespace, claimName, secret, backupMover)
	if err != nil || backing.held() {
		return backing, err
	}
	return leaseHeldElsewhere(ctx, r.Reader, run, run.Namespace, claimName, secret)
}
