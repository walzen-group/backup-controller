package runs

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// quiesce stops the workloads in the run's namespace that are marked
// backup.wlz.li/quiesce: "true", before the run does anything else. It stores
// its now argument, the time of this pass, in status.quiescedAt.
//
// It first records the plan from quiesce.Plan in status.quiesced and
// status.suspendedKustomizations, with each workload's replica count, and
// writes the status. Only then does quiesce.Apply suspend the Kustomizations and
// scale the workloads to zero. A pass that finds a plan in the status reuses
// it, so a retry after a lost status write still gives back the counts the
// workloads had before the run touched them. Before it stops from such a
// plan, quiesce reads the run again through the uncached Reader (see
// stopOwed), and stops nothing when the stored run has recorded the stop,
// given the app back or ended. quiesce then writes status.quiescedAt. After
// a failed stop, it narrows the plan with quiesce.Applied to what is stopped
// now, and aborts the run, which puts that back. When no workload is marked, or no item is left Pending once the
// checks below have failed the others, quiesce stops nothing and sets
// status.restartedAt to the same moment, because there is nothing to start
// again.
//
// Before it records a plan, with nothing stopped, quiesce waits with reason
// SourceBusy while a volume is busy with another run, while another run is in
// the way (see waitingOn), and while another run holds the namespace's
// quiesce Lease.
func (r *BackupRunReconciler) quiesce(ctx context.Context, run *backupv1alpha1.BackupRun, now metav1.Time) (ctrl.Result, error) {
	if len(run.Status.Quiesced) == 0 {
		// A volume still busy with another run's backup would keep the
		// stopped workloads down for as long as that backup takes. The run
		// waits with the workloads still running. A volume busy with a
		// backup no run waits for fails its item here, before anything is
		// stopped, and the rest of the namespace goes on; the status write
		// below records it. An item startItem would refuse (see
		// startRefusal) and one whose ReplicationSource the controller
		// didn't write fail here as well, with the message startItem gives,
		// so the app is not stopped for a backup that cannot start. A read
		// that fails is not evidence that the volume is idle, so it comes
		// back as an error and nothing is stopped.
		for i := range run.Status.Items {
			item := &run.Status.Items[i]
			if item.Kind != "ReplicationSource" || item.Phase != backupv1alpha1.ItemPending {
				continue
			}
			err := r.startRefusal(ctx, run, item.Name)
			if failBackupItem(item, err) {
				continue
			}
			if err != nil {
				return ctrl.Result{}, err
			}
			// A restore of the claim or its repository holds the item the
			// same way, and startItem would wait for it with the app down.
			restoring, err := r.heldElsewhere(ctx, run, item.Name)
			if failBackupItem(item, err) {
				// A repository Secret that is gone fails the item now, so the
				// app is not stopped for a backup that cannot start.
				continue
			}
			if err != nil {
				return ctrl.Result{}, err
			}
			if restoring.held() {
				return after(pollInterval, r.waitFor(ctx, run, restoring.readyReason(), restoring.text))
			}
			source := &volsyncv1alpha1.ReplicationSource{}
			err = r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source)
			switch {
			case apierrors.IsNotFound(err):
				// The claim has no source yet, so nothing can be in use.
				continue
			case err != nil:
				return ctrl.Result{}, fmt.Errorf("get ReplicationSource %s/%s: %w", run.Namespace, item.Name, err)
			}
			if source.Labels[backupv1alpha1.LabelManagedBy] != backupv1alpha1.ManagedByValue {
				failBackupItem(item, foreignSource(item.Name))
				continue
			}
			if !inUse(source) || manualTag(source) == TriggerFor(run.UID) {
				continue
			}
			held, err := holder(ctx, r.Reader, source)
			if failBackupItem(item, err) {
				continue
			}
			if err != nil {
				return ctrl.Result{}, err
			}
			if held.held() {
				return after(pollInterval, r.waitFor(ctx, run, held.readyReason(), held.text))
			}
		}

		targets, err := quiesce.Targets(ctx, r.Reader, run.Namespace)
		if err != nil {
			return ctrl.Result{}, err
		}
		// With no workload marked, or no item left to back up once the checks
		// above failed the rest, there is nothing to stop.
		if len(targets) == 0 || !anyPending(run.Status.Items) {
			run.Status.QuiescedAt, run.Status.RestartedAt = newTime(now), newTime(now)
			return after(time.Second, r.writeStatus(ctx, run))
		}
		// A restore that waits for the Cluster it deleted keeps this run
		// waiting with nothing stopped and no Lease held (see waitingOn).
		waiting, err := waitingOn(ctx, r.Reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if waiting.held() {
			return after(pollInterval, r.waitFor(ctx, run, waiting.readyReason(), waiting.text))
		}
		// The namespace's quiesce Lease lets one run at a time stop its
		// workloads. It is taken before the plan and held until the stored
		// status shows the workloads back, so a second run waits here with
		// the app running rather than recording the count the first stopped
		// it at.
		busy, err := acquireQuiesceLease(ctx, r.Client, r.Reader, run, "BackupRun")
		if err != nil {
			return ctrl.Result{}, err
		}
		if busy.held() {
			return after(pollInterval, r.waitFor(ctx, run, busy.readyReason(), busy.text))
		}
		// A Kustomization that also applies workloads of another namespace
		// is refused before anything is stopped (see quiesce.Plan).
		stop, suspend, err := quiesce.Plan(ctx, r.Reader, r.RESTMapper(), run.Namespace, targets)
		if asRunRefusal(err) {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
		}
		if err != nil {
			return ctrl.Result{}, err
		}
		run.Status.Quiesced, run.Status.SuspendedKustomizations = stop, suspend
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	} else {
		// The plan came from the run as the pass read it, and that copy may
		// lag behind a pass that has since stopped the app, given it back
		// and ended the run. The stored run decides (see stopOwed); a run
		// that no longer owes the stop changes nothing and looks again.
		owed, err := stopOwed(ctx, r.Reader, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !owed {
			return ctrl.Result{RequeueAfter: time.Second}, nil
		}
	}

	stopErr := quiesce.Apply(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	if stopErr != nil {
		run.Status.Quiesced, run.Status.SuspendedKustomizations = quiesce.Applied(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	}
	run.Status.QuiescedAt = newTime(now)
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if stopErr != nil {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, stopErr.Error())
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// startRefusal checks, before anything is written, whether startItem would
// refuse a volume item. A namespace run calls it in its quiesce pre-check,
// so an item that cannot start fails before the app is stopped for it.
//
// Parameters:
//   - run is the asking run; its namespace is read.
//   - claimName names the item's claim.
//
// It returns nil when startItem would go on, and the refusal startItem
// would fail the item with otherwise: the claim is gone (see claimGone), or
// ensureSource refuses the claim's settings (see sourceSettingsFor). The
// caller fails the item with it through failBackupItem. A failed read comes
// back as a plain error, and the pass retries with nothing stopped.
func (r *BackupRunReconciler) startRefusal(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string) error {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return claimGone(claimName)
		}
		return fmt.Errorf("get claim %s/%s: %w", run.Namespace, claimName, err)
	}
	_, err := sourceSettingsFor(ctx, r.Reader, claim)
	return err
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

// heldElsewhere returns a hold of kind holdSourceBusy that names the run
// that holds the claim or its repository. It returns the zero hold when
// no run holds either. A backup calls it before it stops any workload, so
// that it waits with the app running where startItem would wait with the
// app down.
//
// Parameters:
//   - run is the asking run; its namespace and UID are read.
//   - claimName names the claim the run is about to back up.
//
// A claim that does not exist and a VolumeRestore the claim does not have
// give the zero hold. quiesce already failed such an item through startRefusal
// before it asks, and ensureSource checks again right before it writes the
// trigger.
// A repository Secret that does not exist comes back as the refusal
// leaseNamesFor gives, and quiesce fails the item with it (see
// failBackupItem) before anything is stopped. Any other failed read comes
// back as an error, and the pass retries with nothing stopped.
//
// The check is advisory. A run that starts its mover between this read and
// the stop still goes first under the Leases and otherMover, which run right
// before the mover object is written.
func (r *BackupRunReconciler) heldElsewhere(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string) (hold, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return hold{}, nil
		}
		return hold{}, fmt.Errorf("get claim %s/%s: %w", run.Namespace, claimName, err)
	}
	vr, err := volumeRestoreFor(ctx, r.Reader, claim)
	if err != nil {
		if _, refused := asItemFailure(err); refused {
			return hold{}, nil
		}
		return hold{}, err
	}
	restoring, err := otherMover(ctx, r.Reader, run.Namespace, claimName, vr.Spec.Repository, restoreMover)
	if err != nil {
		return hold{}, err
	}
	if restoring.held() {
		return restoring, nil
	}
	return leaseHeldElsewhere(ctx, r.Reader, run, run.Namespace, claimName, vr.Spec.Repository)
}
