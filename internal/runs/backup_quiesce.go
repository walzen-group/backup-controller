package runs

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// quiesce stops the workloads in the run's namespace that are marked
// backup.wlz.li/quiesce: "true", before the run does anything else (see
// runOps.quiesce). The workloads are read only while the run has no plan.
// Before it records a plan, with nothing stopped, quiesce checks the Pending
// volume items (see precheckItems).
func (r *BackupRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	var targets []quiesce.Workload
	if len(run.Status.Quiesced) == 0 {
		var err error
		if targets, err = quiesce.Targets(ctx, r.Reader, run.Namespace); err != nil {
			return ctrl.Result{}, err
		}
	}
	return r.ops().quiesce(ctx, fieldsOf(run), targets, stopSteps{
		precheck: func(ctx context.Context) (hold, error) { return r.precheckItems(ctx, run) },
		abort: func(ctx context.Context, reason, message string) (ctrl.Result, error) {
			return ctrl.Result{}, r.abort(ctx, run, reason, message)
		},
	})
}

// ops returns the reconciler's client, Reader and clock for the steps both
// kinds of run share.
func (r *BackupRunReconciler) ops() runOps {
	return runOps{c: r.Client, reader: r.Reader, now: r.Now}
}

// precheckItems checks every Pending volume item before quiesce stops the
// app (see precheckItem).
//
// Parameters:
//   - run is the namespace BackupRun. The items that cannot start are
//     failed in place, and the caller writes the status.
//
// It returns the first hold of an item that has to wait, or the zero hold.
// It returns the error of a failed read, and then nothing is stopped.
func (r *BackupRunReconciler) precheckItems(ctx context.Context, run *backupv1alpha1.BackupRun) (hold, error) {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindSource || item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		h, err := r.precheckItem(ctx, run, item)
		if err != nil || h.held() {
			return h, err
		}
	}
	return hold{}, nil
}

// precheckItem checks one Pending volume item before quiesce stops the app.
//
// Parameters:
//   - run is the namespace BackupRun.
//   - item is the Pending volume item. It is failed in place when it cannot
//     start.
//
// It returns a hold when the item has to wait for another run, and the zero
// hold otherwise. It returns the error of a failed read.
//
// A volume still busy with another run's backup or restore would keep the
// stopped workloads down for as long as that run takes. The run waits with
// the workloads still running. A volume busy with a backup no run waits for
// fails its item here, before anything is stopped, and the rest of the
// namespace goes on; the status write records it. An item whose claim is
// gone, whose settings ensureSource refuses (see sourceSettingsFor), whose
// repository Secret is gone, or whose ReplicationSource the controller
// didn't write fails here as well, with the message startVolume gives, so
// the app is not stopped for a backup that cannot start. A read that fails
// is not evidence that the volume is idle, so it comes back as an error and
// nothing is stopped.
//
// The check is advisory. A run that starts its mover between this read and
// the stop still goes first under the Leases and otherMover, which run right
// before the mover object is written.
func (r *BackupRunReconciler) precheckItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) (hold, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			failBackupItem(item, claimGone(item.Name))
			return hold{}, nil
		}
		return hold{}, fmt.Errorf("get claim %s/%s: %w", run.Namespace, item.Name, err)
	}
	settings, err := sourceSettingsFor(ctx, r.Reader, claim)
	if failBackupItem(item, err) {
		return hold{}, nil
	}
	if err != nil {
		return hold{}, err
	}
	// A restore of the claim or its repository holds the item the
	// same way, and startVolume would wait for it with the app down.
	repository := settings.vr.Spec.Repository
	restoring, err := otherMover(ctx, r.Reader, run.Namespace, item.Name, repository, restoreMover)
	if err != nil || restoring.held() {
		return restoring, err
	}
	leased, err := leaseHeldElsewhere(ctx, r.Reader, run, run.Namespace, item.Name, repository)
	if failBackupItem(item, err) {
		// A repository Secret that is gone fails the item now.
		return hold{}, nil
	}
	if err != nil || leased.held() {
		return leased, err
	}
	source := &volsyncv1alpha1.ReplicationSource{}
	err = r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source)
	switch {
	case apierrors.IsNotFound(err):
		// The claim has no source yet, so nothing can be in use.
		return hold{}, nil
	case err != nil:
		return hold{}, fmt.Errorf("get ReplicationSource %s/%s: %w", run.Namespace, item.Name, err)
	}
	if source.Labels[backupv1alpha1.LabelManagedBy] != backupv1alpha1.ManagedByValue {
		failBackupItem(item, foreignSource(item.Name))
		return hold{}, nil
	}
	if !inUse(source) || manualTag(source) == TriggerFor(run.UID) {
		return hold{}, nil
	}
	held, err := holder(ctx, r.Reader, source)
	if failBackupItem(item, err) {
		return hold{}, nil
	}
	return held, err
}

// claimGone returns the refusal of a volume item whose claim no longer
// exists.
//
// Parameters:
//   - claimName is the item's claim, which the message names.
//
// It returns a *refusalError with reason ClaimMissing.
func claimGone(claimName string) error {
	return refuse(backupv1alpha1.ItemReasonClaimMissing, "the claim %s no longer exists", claimName)
}
