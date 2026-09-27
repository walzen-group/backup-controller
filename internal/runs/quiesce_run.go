package runs

import (
	"context"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// stopSteps are the parts of a stop that differ between a BackupRun and a
// RestoreRun.
type stopSteps struct {
	// precheck checks the Pending volume items before anything is stopped.
	// It fails in place an item that could not start, and returns the hold
	// of an item that has to wait for another run.
	precheck func(ctx context.Context) (hold, error)

	// abort ends the run early as Failed, with the reason and the message.
	abort func(ctx context.Context, reason, message string) (ctrl.Result, error)
}

// quiesce stops the workloads of the app before the run changes anything,
// and stores the time of the pass in status.quiescedAt.
//
// Parameters:
//   - f is the run, with no status.quiescedAt yet.
//   - targets are the workloads to stop. quiesce reads them only when the
//     run has no plan recorded yet.
//   - steps are the parts that differ between the two kinds of run.
//
// It returns a result that requeues the run: after pollInterval while it
// waits, and after a second once the stop is recorded. A run it ends
// returns what steps.abort returns. A failed read, a failed Lease call and a
// failed status write come back as an error, and the pass is retried with
// nothing stopped that the status does not record.
//
// It first records the plan (see recordPlan) in status.quiesced and
// status.suspendedKustomizations, with each workload's replica count, and
// writes the status. Only then does quiesce.Apply suspend the
// Kustomizations and scale the workloads to zero. A pass that finds a plan
// in the status reuses it, so a retry after a lost status write still gives
// back the counts the workloads had before the run touched them. Before it
// stops from such a plan, quiesce reads the run again through the uncached
// Reader (see stopOwed), and stops nothing when the stored run has recorded
// the stop, given the app back or ended. After a failed stop, it narrows
// the plan with quiesce.Applied to what is stopped now, and aborts the run
// with reason Failed, which starts those workloads again and resumes the
// Kustomizations.
func (o runOps) quiesce(ctx context.Context, f runFields, targets []quiesce.Workload, steps stopSteps) (ctrl.Result, error) {
	if done, result, err := o.readyToStop(ctx, f, targets, steps); done {
		return result, err
	}
	stopErr := quiesce.Apply(ctx, o.c, f.GetNamespace(), *f.quiesced, *f.suspended)
	if stopErr != nil {
		*f.quiesced, *f.suspended = quiesce.Applied(ctx, o.reader, o.c.RESTMapper(), f.GetNamespace(), *f.quiesced, *f.suspended)
	}
	*f.quiescedAt = o.moment()
	if err := o.writeStatus(ctx, f); err != nil {
		return ctrl.Result{}, err
	}
	if stopErr != nil {
		return steps.abort(ctx, backupv1alpha1.ReasonFailed, stopErr.Error())
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// readyToStop makes sure that the run has a plan of what quiesce stops (see
// recordPlan), and still owes that stop.
//
// It returns done false when quiesce can stop the workloads of the plan. It
// returns done true, with the result and the error the pass returns, when
// recordPlan ends the pass, when the run no longer owes the stop, or when a
// read failed. A plan in the status came from the run as the pass read it,
// and that copy may lag behind a pass that has since stopped the app, given
// it back and ended the run. The stored run decides (see stopOwed); a run
// that no longer owes the stop changes nothing and looks again.
func (o runOps) readyToStop(ctx context.Context, f runFields, targets []quiesce.Workload, steps stopSteps) (done bool, result ctrl.Result, err error) {
	if len(*f.quiesced) == 0 {
		return o.recordPlan(ctx, f, targets, steps)
	}
	owed, err := stopOwed(ctx, o.reader, f.Object)
	if err != nil || !owed {
		result, err = after(time.Second, err)
		return true, result, err
	}
	return false, ctrl.Result{}, nil
}

// recordPlan records the plan of what quiesce stops, with nothing stopped
// yet, once no item and no other run is in the way.
//
// Parameters:
//   - f is the run with no plan recorded. Its status is changed and
//     written.
//   - targets are the workloads to stop.
//   - steps are the parts that differ between the two kinds of run.
//
// It returns done false when it wrote the plan and quiesce can stop the
// workloads. It returns done true, with the result and the error the pass
// returns, when the run waits, when there is nothing to stop, when the run
// ended, or when a call failed.
//
// The steps run in this order, each with nothing stopped:
//
//  1. steps.precheck fails the items that could not start, so the app is
//     not stopped for them, and the run waits with reason SourceBusy while
//     another run holds one of its claims or repositories.
//  2. With no workload to stop, or no item left Pending, the run stops
//     nothing: it records status.quiescedAt and status.restartedAt at the
//     same moment, because there is nothing to start again.
//  3. The run waits while another run is in the way (see waitingOn).
//  4. The run takes the namespace's quiesce Lease (see
//     acquireQuiesceLease), and waits while another run holds it. The
//     Lease is held until the stored status shows the workloads back, so a
//     second run waits here with the app running rather than recording the
//     count the first stopped it at.
//  5. quiesce.Plan plans the stop. A Kustomization that also applies
//     workloads of another namespace aborts the run with reason Invalid.
func (o runOps) recordPlan(ctx context.Context, f runFields, targets []quiesce.Workload, steps stopSteps) (done bool, result ctrl.Result, err error) {
	busy, err := steps.precheck(ctx)
	if done, result, err := o.waitOn(ctx, f, busy, err); done {
		return true, result, err
	}
	if len(targets) == 0 || !anyPendingItem(f.Object) {
		now := o.moment()
		*f.quiescedAt, *f.restartedAt = now, newTime(*now)
		result, err = after(time.Second, o.writeStatus(ctx, f))
		return true, result, err
	}
	busy, err = waitingOn(ctx, o.reader, f.Object)
	if done, result, err := o.waitOn(ctx, f, busy, err); done {
		return true, result, err
	}
	busy, err = acquireQuiesceLease(ctx, o.c, o.reader, f.Object, f.kind)
	if done, result, err := o.waitOn(ctx, f, busy, err); done {
		return true, result, err
	}
	stop, suspend, err := quiesce.Plan(ctx, o.reader, o.c.RESTMapper(), f.GetNamespace(), targets)
	if asRunRefusal(err) {
		result, err = steps.abort(ctx, backupv1alpha1.ReasonInvalid, err.Error())
		return true, result, err
	}
	if err != nil {
		return true, ctrl.Result{}, err
	}
	*f.quiesced, *f.suspended = stop, suspend
	if err := o.writeStatus(ctx, f); err != nil {
		return true, ctrl.Result{}, err
	}
	return false, ctrl.Result{}, nil
}

// anyPendingItem reports whether the run, a BackupRun or a RestoreRun, has
// an item that is still Pending.
func anyPendingItem(run client.Object) bool {
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		return anyPending(r.Status.Items)
	case *backupv1alpha1.RestoreRun:
		return anyItemIn(r.Status.Items, backupv1alpha1.ItemPending)
	}
	return false
}

// restart gives the stopped workloads their replicas back, resumes the
// Kustomizations the run suspended, and records status.restartedAt. The
// caller writes the status.
//
// Parameters:
//   - f is the run. restart does something only when the run holds the app
//     stopped (see stopped), or when a BackupRun has status.restartPending
//     set: that run has chosen its restart moment and may not have started
//     the workloads yet, and restart keeps that moment.
//
// It returns the error of the read or of quiesce.Restart, a
// *quiesce.RestartError that names the workload or Kustomization.
//
// The run's copy may come from an informer cache that lags behind the run's
// own last writes, so restart first reads the stored run again (see
// readStop). When the stored run shows the restart done, restart starts
// nothing: another run may have stopped the app since.
func (o runOps) restart(ctx context.Context, f runFields) error {
	if _, err := readStop(ctx, o.reader, f.Object); err != nil {
		return err
	}
	if !stopped(f) && !*f.restartPending {
		return nil
	}
	if err := quiesce.Restart(ctx, o.c, f.GetNamespace(), *f.quiesced, *f.suspended); err != nil {
		return err
	}
	if *f.restartedAt == nil {
		*f.restartedAt = o.moment()
	}
	*f.restartPending = false
	return nil
}

// stopped reports whether the run has stopped workloads and not yet started
// them again. A run that recorded its plan to stop them counts as having
// stopped them, even before status.quiescedAt is set, because the pass that
// wrote the plan may have stopped them and then lost its status write.
func stopped(f runFields) bool {
	return (*f.quiescedAt != nil || len(*f.quiesced) > 0) && *f.restartedAt == nil
}

// durablyRestarted reports whether the run's stored status shows that it gave
// the workloads back: status.restartedAt is set and, on a BackupRun,
// status.restartPending is cleared.
func durablyRestarted(run client.Object) bool {
	f := fieldsOf(run)
	return *f.restartedAt != nil && !*f.restartPending
}

// readStop reads a run again straight from the API server and puts the
// stored record of its stop and restart on the copy the caller holds. A
// caller that is about to start the workloads again calls it first, so that
// it decides on what the API server holds. stopOwed calls it before a stop
// for the same reason.
//
// Parameters:
//   - reader is the uncached Reader. The reconcilers read their run through
//     the informer cache, which can lag behind the run's own last writes: a
//     pass that reads a copy from before the restart was stored would start
//     the workloads again, under the stop of a run that took the namespace's
//     quiesce Lease since.
//   - run is the BackupRun or RestoreRun as the pass read it. Its
//     status.quiescedAt, status.quiesced, status.suspendedKustomizations,
//     status.restartedAt and, on a BackupRun, status.restartPending are
//     replaced with the stored ones when the stored run is newer.
//
// It returns the stored run's status.phase. It returns an error, and changes
// nothing, when the read fails, when the run is gone, or when the stored run
// with that name is another object (a different UID). The caller then
// changes no workload and the pass runs again.
//
// A copy whose resourceVersion matches the stored one is left as it is. A
// newer stored run changes only the fields above, so the caller's later
// status write still carries the old resourceVersion and fails with a
// conflict, and the next pass works from the stored run.
func readStop(ctx context.Context, reader client.Reader, run client.Object) (backupv1alpha1.RunPhase, error) {
	f := fieldsOf(run)
	stored := emptyLike(run)
	if err := reader.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
		return "", fmt.Errorf("read %s %s/%s again before changing its workloads: %w", f.kind, run.GetNamespace(), run.GetName(), err)
	}
	if stored.GetUID() != run.GetUID() {
		return "", fmt.Errorf("%s %s/%s is now another object (UID %s, was %s); no workload is changed for the old one",
			f.kind, run.GetNamespace(), run.GetName(), stored.GetUID(), run.GetUID())
	}
	s := fieldsOf(stored)
	if stored.GetResourceVersion() != run.GetResourceVersion() {
		*f.quiescedAt, *f.quiesced, *f.suspended = *s.quiescedAt, *s.quiesced, *s.suspended
		*f.restartedAt, *f.restartPending = *s.restartedAt, *s.restartPending
	}
	return *s.phase, nil
}

// stopOwed reports whether a run whose cached copy shows a recorded plan
// without status.quiescedAt still owes the stop, as the API server holds the
// run. quiesce calls it before quiesce.Apply when it took the plan from the
// run's status.
//
// Parameters:
//   - reader is the uncached Reader. A cached copy can lag behind the pass
//     that stopped the workloads, recorded status.quiescedAt, gave the
//     workloads back and ended the run; a stop from that copy would leave the
//     app at 0 with no run left to start it again.
//   - run is the BackupRun or RestoreRun as the pass read it. readStop puts
//     the stored record of its stop and restart on it.
//
// It returns true when the stored run is not finished and still shows the
// plan with neither status.quiescedAt nor status.restartedAt set. A failed
// read comes back as the error from readStop, and the caller stops nothing.
func stopOwed(ctx context.Context, reader client.Reader, run client.Object) (bool, error) {
	phase, err := readStop(ctx, reader, run)
	if err != nil {
		return false, err
	}
	f := fieldsOf(run)
	return !phase.Finished() && len(*f.quiesced) > 0 && *f.quiescedAt == nil && *f.restartedAt == nil, nil
}

// waitingOn returns a hold of kind holdSourceBusy that names another run in
// the namespace of the run. The run must wait for that other run before it
// stops the workloads of the namespace. It returns the zero hold when there
// is no such run. A run calls it before it takes the quiesce Lease of the
// namespace, with nothing stopped.
//
// Parameters:
//   - reader lists the namespace's RestoreRuns, uncached. A failed list comes
//     back as an error, and nothing is decided on it.
//   - run is the asking run; a RestoreRun with its UID is skipped.
//
// The run it names is an unfinished RestoreRun with a Cluster item in phase
// Deleted. That RestoreRun waits for its Cluster to be created again, and a
// Kustomization this run suspends may be the one Flux needs to create it.
func waitingOn(ctx context.Context, reader client.Reader, run metav1.Object) (hold, error) {
	restores := &backupv1alpha1.RestoreRunList{}
	if err := reader.List(ctx, restores, client.InNamespace(run.GetNamespace())); err != nil {
		return hold{}, fmt.Errorf("list RestoreRuns in %s: %w", run.GetNamespace(), err)
	}
	for i := range restores.Items {
		other := &restores.Items[i]
		if other.UID == run.GetUID() || other.Status.Phase.Finished() {
			continue
		}
		if cluster := deletedCluster(other); cluster != "" {
			return sourceBusy("RestoreRun %s has deleted Cluster %s and waits for it to be created again; this run stops the workloads once that Cluster is back",
				other.Name, cluster), nil
		}
	}
	return hold{}, nil
}

// deletedCluster returns the name of a Cluster the run deleted and waits for
// its owner to create again, or "" when it has none.
func deletedCluster(run *backupv1alpha1.RestoreRun) string {
	for _, item := range run.Status.Items {
		if item.Kind == backupv1alpha1.ItemKindCluster && item.Phase == backupv1alpha1.ItemDeleted {
			return item.Name
		}
	}
	return ""
}

// acquireQuiesceLease takes the namespace's quiesce Lease for the run, so
// that one run at a time stops that namespace's workloads. A run calls it
// right before it plans a stop and holds the Lease until its stored status
// shows the workloads back (see releaseQuiesceLeases).
//
// Parameters:
//   - kind is BackupRun or RestoreRun.
//   - run is the run that takes the Lease.
//
// It returns the zero hold once the run holds the Lease. It returns a hold
// of kind holdSourceBusy that names the holder when another live run holds
// the Lease. It returns an error for a failed API call. The caller waits
// with reason SourceBusy and tries again on a later pass, with nothing
// stopped.
//
// Runs of both kinds take the same Lease, so a backup and a restore of
// different claims in one namespace wait for each other even when their
// workloads do not overlap. The one Lease covers every overlap: a namespace
// run stops every marked workload, and a restore's spec.quiesce usually
// lists some of them.
func acquireQuiesceLease(ctx context.Context, c client.Client, reader client.Reader, run metav1.Object, kind string) (hold, error) {
	holder := leaseHolder{kind: kind, run: run, scope: scopeQuiesce}
	return acquireLease(ctx, c, reader, holder, run.GetNamespace(), quiesceLeaseName)
}

// timedOutMessage returns the Ready message of a run that ends because its
// deadline passed.
//
// Parameters:
//   - deadline is the moment the run had to finish by, which the message
//     gives.
//   - conditions are the run's status.conditions before it ends.
//
// The message ends with the wait the run was in, if any (see waitedFor).
func timedOutMessage(deadline time.Time, conditions []metav1.Condition) string {
	return fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339)) + waitedFor(conditions)
}

// waitedFor returns the end of a timed-out run's message that says what the
// run was waiting for.
//
// Parameters:
//   - conditions are the run's status.conditions before it ends.
//
// When the run was waiting, for another run (reason SourceBusy) or for the
// API server to serve VolSync's version again (reason VolSyncUnsupported),
// it returns "; it was waiting: " followed by that wait's own message.
// Otherwise it returns an empty string.
func waitedFor(conditions []metav1.Condition) string {
	ready := meta.FindStatusCondition(conditions, backupv1alpha1.ConditionReady)
	if ready == nil || (ready.Reason != backupv1alpha1.ReasonSourceBusy && ready.Reason != backupv1alpha1.ReasonVolSyncUnsupported) {
		return ""
	}
	return "; it was waiting: " + ready.Message
}

// waitForPods keeps a run that stopped its workloads waiting while a pod of
// them still runs.
//
// Parameters:
//   - f is the run.
//   - targets are the workloads the run stopped.
//   - before says what the run does once the pods are gone, for the Ready
//     message.
//
// It returns done false once every pod of the workloads is gone. It returns
// done true with the error of a failed read, and while a pod is left: the
// run then waits with reason Running and looks again after two seconds.
func (o runOps) waitForPods(ctx context.Context, f runFields, targets []quiesce.Workload, before string) (done bool, result ctrl.Result, err error) {
	gone, pod, err := quiesce.PodsGone(ctx, o.reader, f.GetNamespace(), targets)
	if err != nil || gone {
		return err != nil, ctrl.Result{}, err
	}
	result, err = after(2*time.Second, o.waitFor(ctx, f, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("waiting for pod %s to stop before %s", pod, before)))
	return true, result, err
}
