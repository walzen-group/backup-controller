package runs

import (
	"context"
	"errors"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// RestoreRunReconciler runs RestoreRuns. A RestoreRun puts volumes and
// databases back to the state they were in at a chosen moment.
//
// A volume restores in place. Once no pod mounts the claim, the run creates
// its own restore Job (internal/restorejob), which mounts the claim and runs
// restic restore of the selected snapshot by its full ID, and the Job's
// terminal conditions say how it ended. A database restores by being created
// again. The run deletes the Cluster, and when Flux or tofu creates it again,
// the bootstrap webhook makes the new Cluster recover to the run's moment.
// With spec.into set, a volume restores into a new claim the run creates, and
// the app's own claims and databases are left alone. The same restore Job,
// mounting that claim, writes the selected snapshot into it by its full ID,
// from the backups of spec.claim or from spec.repository.
//
// The run stops a mover once the item's end is in its status, when the run
// ends and when it is deleted. It suspends a restore Job and waits until no
// pod of it can still write before it gives the app back, releases the
// item's Leases or finishes (rule X2, see stopJobs).
type RestoreRunReconciler struct {
	client.Client

	// Reader reads straight from the API server, without the informer cache.
	// The run reads claims, VolumeRestores, Secrets, ObjectStores, Clusters,
	// ReplicationSources, Deployments, StatefulSets, pods and Jobs through it.
	Reader client.Reader

	// Snapshots lists the snapshots in a restic repository. plan uses it to
	// check that each volume has a snapshot the run's moment reaches.
	Snapshots SnapshotLister

	// Prober lists a database's base backups in its object store. plan uses it
	// to check that each database has a base backup the run's moment reaches.
	Prober BaseBackupLister

	// Recorder writes an event on the run each time its Ready reason changes.
	Recorder events.EventRecorder

	// RestoreImage is the image the run's restore Jobs run restic in, from
	// the controller's required --restore-image flag: the image VolSync runs
	// its restic mover in, so a restore runs the restic that wrote the
	// backup.
	RestoreImage string

	// Now returns the current time. Tests replace it so they can move time
	// forward without sleeping.
	Now func() time.Time

	// schemas caches the check that the installed CRD of the run's kind
	// declares every field the controller writes (see crdOutdated).
	schemas schemaCache
}

// SetupWithManager registers the reconciler with mgr so it runs for every
// RestoreRun. It sets Now to time.Now when the caller left it unset.
func (r *RestoreRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	r.serve()
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.RestoreRun{}).
		Named("restorerun").
		Complete(r)
}

// Reconcile moves the RestoreRun the request names one step further, and
// requeues until the run has finished.
//
// A new run starts in plan, which checks that every item has a backup in reach
// before anything is changed, or in planIntoNewClaim when spec.into is set. An
// error that either of them returns for a retry goes through planFailed,
// which reports it on the Ready condition and ends the run once spec.timeout
// has passed since its creation. A run past its checks continues in work, or in
// restoreIntoEmptyClaim for an into restore (see restore). Before any of these, Reconcile
// adds the run's finalizer. A run being deleted gets its changes put back by
// finalize, and a finished run is deleted once spec.ttlSecondsAfterFinished
// has passed.
//
// A new run is first checked against the installed RestoreRun CRD (see
// schemaCache.crdOutdated), and a run whose CRD lacks a field the controller
// writes ends with reason CRDOutdated before anything is planned.
// Whenever the Ready reason changes during a reconcile, Reconcile records an
// event on the run.
//
// While VolSync serves its kinds only at a version other than v1alpha1, a
// pass that reads a VolSync object, such as the ReplicationSources a volume
// item lists before it starts its restore Job (see otherMover), gets an
// error naming the kind and v1alpha1, which it returns for a retry (see
// serve). The run changes nothing on that error, shows it on its Ready
// condition with reason VolSyncUnsupported (see restore), and an app it has
// already stopped stays stopped while it retries. Ending the run needs no
// VolSync object, so a run that passes spec.timeout ends TimedOut, and a run
// that passes it or is deleted still stops its restore Jobs and gives the
// app back. VolSync is upgraded after the controller, so a supported
// cluster never gets there.
//
// A run that recorded status.ending has decided to end, and every later pass
// only finishes it with that reason and message (see finish), also one that
// waits for a stopped mover or retries a failed restart.
func (r *RestoreRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.RestoreRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Restore") }()
	if !run.DeletionTimestamp.IsZero() {
		return r.finalize(ctx, run)
	}
	if run.Status.Phase.Finished() {
		return expire(ctx, r.Client, run, run.Spec.TTLSecondsAfterFinished, run.Status.CompletedAt, r.Now())
	}
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		controllerutil.AddFinalizer(run, Finalizer)
		if err := r.Update(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer to RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
		}
	}
	if ending := run.Status.Ending; ending != nil {
		return r.finish(ctx, run, ending.Reason, ending.Message)
	}

	if run.Status.Phase == "" {
		return r.start(ctx, run)
	}
	return r.restore(ctx, run)
}

// restore makes one pass over a RestoreRun past its checks: through
// restoreIntoEmptyClaim for an into restore, and through work otherwise.
//
// Parameters:
//   - run is the RestoreRun, with a phase, no ending recorded, and not
//     being deleted.
//
// It returns what the pass returns. When the pass failed on a VolSync
// request at a version the API server no longer serves, the error is also
// shown on the run's Ready condition (see showVolSyncWait), and an error of
// that status write is joined to it.
//
// The run changes nothing on that error and retries it on every pass, with
// an app it has stopped kept stopped, until its spec.timeout: the deadline
// check at the start of each pass needs no VolSync object, so the run then
// ends TimedOut, stops its restore Jobs and gives the app back.
func (r *RestoreRunReconciler) restore(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	pass := r.work
	if run.Spec.Into != "" {
		pass = r.restoreIntoEmptyClaim
	}
	result, err := pass(ctx, run)
	kind, unserved := volsyncUnserved(err)
	if !unserved {
		return result, err
	}
	return result, errors.Join(err, r.showVolSyncWait(ctx, run, kind, err))
}

// start makes the first pass over a new run: it checks the installed
// RestoreRun CRD, then plans the run.
//
// Parameters:
//   - run is the RestoreRun with an empty phase and its finalizer in place.
//
// It returns what plan or planIntoNewClaim returns, or what finish returns
// for a run whose CRD lacks a field the controller writes. An error that
// either check returns for a retry goes through planFailed.
//
// The CRD check comes first for both plans: the old RestoreRun CRD drops the
// items' clusterUID and snapshotTime, which the restore relies on. A run
// with spec.into set is planned by planIntoNewClaim, and any other run by
// plan.
func (r *RestoreRunReconciler) start(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	err := r.schemas.crdOutdated(ctx, r.Reader, restoreRunsCRD, backupv1alpha1.KindRestoreRun, backupv1alpha1.RestoreRun{},
		"the run would lose the Cluster UIDs and snapshot times it records to check its own work")
	var outdated *crdOutdatedError
	if errors.As(err, &outdated) {
		return r.finish(ctx, run, backupv1alpha1.ReasonCRDOutdated, outdated.Error())
	}
	if err != nil {
		return ctrl.Result{}, r.planFailed(ctx, run, err)
	}
	plan := r.plan
	if run.Spec.Into != "" {
		plan = r.planIntoNewClaim
	}
	result, err := plan(ctx, run)
	if err != nil {
		return ctrl.Result{}, r.planFailed(ctx, run, err)
	}
	return result, nil
}

// planFailed handles an error that plan or planIntoNewClaim returned for a
// retry, and returns the error the reconcile hands back.
//
// A run whose checks keep failing never records status.startedAt, so overdue
// never fires for it. Once the deadline from checksOverdue has passed,
// planFailed ends the run as Failed with reason TimedOut and the error in the
// message. Until then it sets Ready to False with reason Retrying and the
// error as the message, so `kubectl get` shows why the run has not started,
// and returns the error for a retry. It writes the status only when it
// differs from the stored one (see writeChangedStatus), because every write
// starts another reconcile.
//
// An error from a pass that had already given the run a phase, such as a lost
// status write, is returned unchanged.
func (r *RestoreRunReconciler) planFailed(ctx context.Context, run *backupv1alpha1.RestoreRun, err error) error {
	if run.Status.Phase != "" {
		return err
	}
	if deadline, over := r.checksOverdue(run); over {
		// Nothing was created before the checks passed, so there is no
		// restore Job to stop and no mover to wait for.
		_, finishErr := r.finish(ctx, run, backupv1alpha1.ReasonTimedOut, checksTimedOut(deadline, err.Error()))
		return finishErr
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRetrying, err.Error())
	if werr := r.writeChangedStatus(ctx, run); werr != nil {
		return errors.Join(err, werr)
	}
	return err
}

// checksOverdue returns the deadline of a run that has not passed its checks,
// its creation plus spec.timeout, and reports whether the run has worked past
// it. Such a run has no status.startedAt, so overdue never fires for it. A run
// with no timeout, or with no creation time yet, is never overdue.
func (r *RestoreRunReconciler) checksOverdue(run *backupv1alpha1.RestoreRun) (time.Time, bool) {
	if run.Spec.Timeout == nil || run.CreationTimestamp.IsZero() {
		return time.Time{}, false
	}
	deadline := run.CreationTimestamp.Add(run.Spec.Timeout.Duration)
	return deadline, !r.Now().Before(deadline)
}

// checksTimedOut returns the Ready message of a run that ends TimedOut
// before it passed its checks: the deadline from checksOverdue, and why the
// checks had not passed, which is the text given in why.
func checksTimedOut(deadline time.Time, why string) string {
	return fmt.Sprintf("the run had not passed its checks by %s: %s", deadline.UTC().Format(time.RFC3339), why)
}

// writeChangedStatus writes the run's status unless the stored run already
// holds that exact status, so a pass that waits and changed nothing starts
// no other reconcile.
//
// Parameters:
//   - run is the RestoreRun as this pass computed it, status included.
//
// It returns nil when the write was skipped or went through, and the error
// of the read or of the write otherwise.
//
// It reads the stored run with the uncached Reader and skips the write only
// when the whole stored status, items and ending included, equals the
// computed one. Any difference writes the status, so whatever the pass
// changed in it before it waits reaches the API server in that pass. The
// resourceVersion does not count: a pass whose copy of the run is older than
// the stored run, and whose status is already stored, has nothing to write,
// and a write with the older resourceVersion would only fail with a
// conflict.
func (r *RestoreRunReconciler) writeChangedStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	stored := &backupv1alpha1.RestoreRun{}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
		return fmt.Errorf("get RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	if equality.Semantic.DeepEqual(stored.Status, run.Status) {
		return nil
	}
	return r.writeStatus(ctx, run)
}

// waitAtChecks holds a run that has not passed its checks while a backup of
// a repository it restores from is in progress, and returns the result the
// reconcile hands back. The run selects its snapshot once that backup has
// finished, so after the backup's restic forget and its retime.
//
// Parameters:
//   - busy is the hold from otherMover that names the backup. Its text
//     becomes the Ready message.
//
// The run keeps its empty phase, because Reconcile plans a run whose phase
// is empty. waitAtChecks sets Ready to False with reason SourceBusy and the
// message, writes the status when it differs from the stored one (see
// writeChangedStatus), and requeues after pollInterval. Once the deadline
// from checksOverdue has passed, it ends the run as Failed with reason
// TimedOut and the message instead. A failed read or write of the status
// comes back as an error.
func (r *RestoreRunReconciler) waitAtChecks(ctx context.Context, run *backupv1alpha1.RestoreRun, busy hold) (ctrl.Result, error) {
	if deadline, over := r.checksOverdue(run); over {
		return r.finish(ctx, run, backupv1alpha1.ReasonTimedOut, checksTimedOut(deadline, busy.text))
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, busy.readyReason(), busy.text)
	return after(pollInterval, r.writeChangedStatus(ctx, run))
}

// waitForStopped reports that a run waits for something it stopped to go
// before it gives the app back, releases its Leases, or finishes (rule X2
// and designs/restorerun.md D1).
//
// Parameters:
//   - run is the RestoreRun that waits, with the status this pass computed.
//     Its status is written when it differs from the stored one.
//   - message is the Ready message that says what the run waits for: a
//     mover it stopped (see jobList.message).
//
// It returns a result that looks again after pollInterval, and the error of
// the status read or write, if any.
//
// It moves an unfinished run to Waiting and sets the Ready condition to
// False with reason WaitingForShutdown and the message. A finished run that
// finalize holds keeps the phase it finished with, which is the record of how
// its restore went. The status is written only when the whole status
// differs from the stored one (see writeChangedStatus), since every write
// starts another reconcile. So an ending that finish recorded, or items
// that work changed, reach the API server in the pass that changed them,
// even when the run waits for the same mover as before. The run keeps its
// finalizer and its Leases meanwhile, so nothing takes the claim or the
// repository over while the mover may still write.
func (r *RestoreRunReconciler) waitForStopped(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) (ctrl.Result, error) {
	if !run.Status.Phase.Finished() {
		run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonShutdown, message)
	return after(pollInterval, r.writeChangedStatus(ctx, run))
}

// overdue returns the run's deadline, status.startedAt plus spec.timeout, and
// reports whether the run has worked past it. A run that has not started, or
// has no timeout, is never overdue.
func (r *RestoreRunReconciler) overdue(run *backupv1alpha1.RestoreRun) (time.Time, bool) {
	if run.Status.StartedAt == nil || run.Spec.Timeout == nil {
		return time.Time{}, false
	}
	deadline := run.Status.StartedAt.Add(run.Spec.Timeout.Duration)
	return deadline, !r.Now().Before(deadline)
}

// waitFor moves the run to Waiting, sets its Ready condition to False with
// reason and message, and writes the status.
func (r *RestoreRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

// writeStatus writes the run's status subresource.
func (r *RestoreRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set RestoreRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}
