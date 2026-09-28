package runs

import (
	"context"
	"errors"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/lease"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// restoreRunKind is the group, version and kind that the owner reference on a
// RestoreRun's Kueue Workload names.
var restoreRunKind = backupv1alpha1.GroupVersion.WithKind("RestoreRun")

// RestoreRunReconciler runs RestoreRuns. A RestoreRun puts volumes and
// databases back to the state they were in at a chosen moment.
//
// A volume restores through the controller's own restore Job, which writes
// the exact snapshot the run selected into the claim, in place or into a new
// claim that spec.into names. A database restores by being created again: the
// run deletes the Cluster, its owner creates it again, and the bootstrap
// webhook makes the new Cluster recover to where the run's item says.
type RestoreRunReconciler struct {
	client.Client

	// Reader reads straight from the API server, without the informer cache.
	// Every pass reads the run and everything it decides on through it.
	Reader client.Reader

	// Snapshots lists the snapshots in a restic repository.
	Snapshots restic.Lister

	// Prober lists the completed base backups of a database.
	Prober bootstrap.Prober

	// Leases keeps two runs from acting on the same claim, repository,
	// Cluster or paused workloads at once.
	Leases *lease.Leases

	// RestoreImage is the image of the restore Jobs, from --restore-image:
	// VolSync's mover image, which holds the restic that wrote the
	// repositories.
	RestoreImage string

	// Paused is true when the controller runs with --pause: a run that has
	// not started work waits, and a run that has goes on until it ends.
	Paused bool

	// Recorder writes an event on the run each time its Ready reason changes.
	Recorder events.EventRecorder

	// Now returns the current time. Tests replace it so they can move time
	// forward without sleeping.
	Now func() time.Time
}

// SetupWithManager registers the reconciler with mgr so it runs for every
// RestoreRun. It sets Now to time.Now when the caller left it unset.
func (r *RestoreRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.RestoreRun{}).
		Named("restorerun").
		Complete(r)
}

// Reconcile moves the RestoreRun that the request names one step further,
// and requeues until the run has finished.
//
// A run goes through the same stages as a BackupRun. With no phase, plan
// lists the items and checks the spec, with API reads only. Until
// status.startedAt is set, admit waits for Kueue to admit the run. Once
// admitted, work takes the run's Leases, selects what each item restores
// (see selectBackups), pauses what spec.pauseDuringRestore lists, restores
// the volumes, restores the databases, and resumes the app.
//
// Each pass reads the run from the API server. A run being deleted gets its
// changes put back by finalize, and a finished run is deleted once
// spec.ttlSecondsAfterFinished has passed. While the controller runs with
// --pause, a run that has not started work waits with reason
// ControllerPaused.
func (r *RestoreRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.RestoreRun{}
	if err := r.Reader.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Restore") }()
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, run)
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
	if r.Paused && run.Status.StartedAt == nil {
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonControllerPaused,
			"the controller runs with --pause; the run starts once the pause ends"))
	}

	switch {
	case run.Status.Phase == "":
		return r.plan(ctx, run)
	case run.Status.StartedAt == nil:
		return r.admit(ctx, run)
	default:
		return r.work(ctx, run)
	}
}

// holder returns the identity under which the run holds its Leases.
func (r *RestoreRunReconciler) holder(run *backupv1alpha1.RestoreRun) lease.Holder {
	return lease.Holder{Kind: restoreRunKind.Kind, Name: run.Name, UID: run.UID}
}

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

// plan records one Pending item for each claim and Cluster the run restores
// and moves the run to Queued. It reads only the claims, the Clusters and the
// workloads spec.pauseDuringRestore lists; no repository and no store is
// read before Kueue admits the run.
//
// A spec that can't work ends the run as Failed with reason Invalid: a
// restoreAsOf that does not parse, spec.syncDatabaseToVolume without
// spec.all, a spec.pauseDuringRestore entry the namespace does not hold, and
// the errors of items.
func (r *RestoreRunReconciler) plan(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if _, err := target(run); err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if run.Spec.SyncDatabaseToVolume && !run.Spec.All {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid,
			"syncDatabaseToVolume restores the volumes and the databases of the namespace together, so it needs all: true")
	}
	if _, err := namedTargets(ctx, r.Reader, run.Namespace, run.Spec.PauseDuringRestore); err != nil {
		if !isPauseSpecError(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	items, err := r.items(ctx, run)
	if err != nil {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	run.Status.Items = items
	run.Status.Phase = backupv1alpha1.RunPhaseQueued
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonQueued, queuedMessage)
	return after(time.Second, r.writeStatus(ctx, run))
}

// items returns one Pending item for each thing the run's spec names.
//
// spec.into gives one volume item for the new claim, restored from
// spec.repository or from spec.claim's repository. spec.claim gives one
// volume item restored in place, and spec.database one database item. A run
// with none of them takes every claim and every Cluster in the namespace
// marked backup.wlz.li/enabled: "true". A Cluster that archives nowhere has no
// backup to restore, so its item starts out Skipped.
//
// It returns an error when spec.repository is set without spec.into, since a
// restore in place needs a claim to write into, and when nothing in the
// namespace is marked.
func (r *RestoreRunReconciler) items(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]backupv1alpha1.RestoreItem, error) {
	pending := func(kind, name string) backupv1alpha1.RestoreItem {
		return backupv1alpha1.RestoreItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}
	switch {
	case run.Spec.Into != "":
		if run.Spec.Claim == "" && run.Spec.Repository == "" {
			return nil, fmt.Errorf("spec.into needs spec.claim or spec.repository to name the repository to restore from")
		}
		return []backupv1alpha1.RestoreItem{pending("PersistentVolumeClaim", run.Spec.Into)}, nil
	case run.Spec.Repository != "":
		return nil, fmt.Errorf("spec.into is required when spec.repository names the source, because there is no claim to restore in place")
	case run.Spec.Claim != "":
		return []backupv1alpha1.RestoreItem{pending("PersistentVolumeClaim", run.Spec.Claim)}, nil
	case run.Spec.Database != "":
		return []backupv1alpha1.RestoreItem{pending("Cluster", run.Spec.Database)}, nil
	}

	claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
	if err != nil {
		return nil, err
	}
	clusters, err := enabledClusters(ctx, r.Reader, run.Namespace)
	if err != nil {
		return nil, err
	}
	var items []backupv1alpha1.RestoreItem
	for _, claim := range claims {
		items = append(items, pending("PersistentVolumeClaim", claim.Name))
	}
	for i := range clusters {
		item := pending("Cluster", clusters[i].GetName())
		if _, _, archives := bootstrap.Archiver(&clusters[i]); !archives {
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster archives nowhere, so it has no backup to restore"
		}
		items = append(items, item)
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
	}
	return items, nil
}

// admit waits for the namespace's LocalQueue to admit the run, then moves the
// run to Running and records status.startedAt, from which spec.timeout
// counts. It works as BackupRunReconciler.admit does: a namespace with no
// LocalQueue keeps the run Queued.
func (r *RestoreRunReconciler) admit(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	queue, err := localQueue(ctx, r.Reader, run.Namespace)
	var missing noQueueError
	if errors.As(err, &missing) {
		if setQueued(&run.Status.Conditions, run.Generation, missing.Error()) {
			return ctrl.Result{RequeueAfter: pollInterval}, r.writeStatus(ctx, run)
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	workload, err := ensureWorkload(ctx, r.Client, run, restoreRunKind, queue)
	if err != nil {
		return ctrl.Result{}, err
	}
	queued := setQueued(&run.Status.Conditions, run.Generation, queuedMessage)
	if run.Status.Workload != workload.GetName() || queued {
		run.Status.Workload = workload.GetName()
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if !admitted(workload) {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	if err := markPodsReady(ctx, r.Client, workload, metav1.NewTime(r.Now())); err != nil {
		return ctrl.Result{}, err
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = newTime(metav1.NewTime(r.Now()))
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "selecting the backups to restore")
	return after(time.Second, r.writeStatus(ctx, run))
}

// work makes one pass over a run that the queue has admitted, and returns
// when to look again.
//
// The pass goes through these steps and stops at the first one that has to
// wait:
//  1. A Workload that Kueue evicted, or a run past its timeout, ends the run
//     Failed (see abort).
//  2. The run takes the Leases of its items (see leaseNames). While another
//     run holds one, the run waits with reason Busy.
//  3. Once it holds them, the run selects what each item restores (see
//     selectBackups). An item without a backup ends the run with reason
//     NoBackupInReach, before anything changed.
//  4. It pauses what spec.pauseDuringRestore lists (see pause), and waits
//     until every pod of those workloads is gone.
//  5. It restores every volume (see restoreVolume).
//  6. Once every volume is done, it restores every database (see
//     restoreDatabase). When a volume failed, a database that has not
//     started is Skipped.
//  7. Once every volume is done and every database is deleted and its old
//     pods and claims are gone, it resumes the app, so a suspended Flux
//     Kustomization can create the Cluster again.
//
// The run finishes once every item is done, Succeeded when no item failed
// and Failed otherwise.
func (r *RestoreRunReconciler) work(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if done, err := r.endIfEvicted(ctx, run); done {
		return ctrl.Result{}, err
	}
	if deadline, over := r.overdue(run); over {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonTimedOut, fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339)))
	}

	names, err := r.leaseNames(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	busy, err := holding(ctx, r.Leases, run.Namespace, r.holder(run), names, &run.Status.Leases, func() error { return r.writeStatus(ctx, run) })
	if err != nil {
		return ctrl.Result{}, err
	}
	if busy != "" {
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonBusy, busy))
	}

	// The selection reads the Clusters and the repositories only once no
	// other run works on them: a Cluster that another restore deleted and
	// has not got back yet can't be read.
	if run.Status.SelectedAt == nil {
		unreachable, err := r.selectBackups(ctx, run)
		if err != nil {
			return ctrl.Result{}, err
		}
		if unreachable != "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonNoBackupInReach, unreachable)
		}
		run.Status.SelectedAt = newTime(metav1.NewTime(r.Now()))
		backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "restoring")
		return after(time.Second, r.writeStatus(ctx, run))
	}

	if len(run.Spec.PauseDuringRestore) > 0 && run.Status.PausedAt == nil {
		return r.pause(ctx, run)
	}
	if paused(run) && anyRestorePending(run.Status.Items) {
		if err := applyPause(ctx, r.Client, run.Namespace, run.Status.Paused, run.Status.SuspendedKustomizations); err != nil {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
		}
		gone, pod, err := pausedPodsGone(ctx, r.Reader, run.Namespace, run.Status.Paused)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			return after(2*time.Second, r.waitFor(ctx, run, backupv1alpha1.ReasonRunning,
				fmt.Sprintf("waiting for pod %s to stop before anything is restored", pod)))
		}
	}

	var waiting wait
	volumesDone, volumesFailed := true, false
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != "PersistentVolumeClaim" {
			continue
		}
		w, err := r.restoreVolume(ctx, run, i, item)
		if err != nil {
			return ctrl.Result{}, err
		}
		if w.reason != "" {
			waiting = w
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning:
			volumesDone = false
		case backupv1alpha1.ItemFailed:
			volumesFailed = true
		}
	}

	shuttingDown := false
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
			w, err := r.restoreDatabase(ctx, run, item)
			if err != nil {
				return ctrl.Result{}, err
			}
			if w.reason != "" {
				waiting = w
			}
			if w.reason == backupv1alpha1.ReasonShutdown {
				shuttingDown = true
			}
		}
	}

	if paused(run) && volumesDone && !anyRestorePending(run.Status.Items) && !shuttingDown {
		if err := r.resume(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}

	if restoreDone(run.Status.Items) && (!paused(run) || resumedAll(run.Status.Paused)) {
		if failed := restoreFailures(run.Status.Items); failed != "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, failed)
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, "every item holds the restored data")
	}
	if waiting.reason != "" {
		return after(pollInterval, r.waitFor(ctx, run, waiting.reason, waiting.message))
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "restoring")
	return after(pollInterval, r.writeStatus(ctx, run))
}

// endIfEvicted ends the run Failed with reason Evicted when Kueue evicted its
// Workload after it admitted the run. It returns true when it ended the run,
// with the error of abort. A Workload that can't be read does not end the
// run.
func (r *RestoreRunReconciler) endIfEvicted(ctx context.Context, run *backupv1alpha1.RestoreRun) (bool, error) {
	if run.Status.Workload == "" {
		return false, nil
	}
	workload, err := getWorkload(ctx, r.Reader, run.Namespace, run.Status.Workload)
	if err != nil || workload == nil || !evicted(workload) {
		return false, nil
	}
	return true, r.abort(ctx, run, backupv1alpha1.ReasonEvicted, "Kueue evicted the run's Workload")
}

// leaseNames returns the Leases the run needs before it acts: the claim and
// the restic repository of every volume item, the Cluster of every database
// item, and backup-pause when spec.pauseDuringRestore lists workloads. A
// volume whose repository can't be found gets no repository Lease; its
// restore fails. It returns an error when a read fails.
func (r *RestoreRunReconciler) leaseNames(ctx context.Context, run *backupv1alpha1.RestoreRun) ([]string, error) {
	var names []string
	if len(run.Spec.PauseDuringRestore) > 0 {
		names = append(names, lease.PauseName)
	}
	for _, item := range run.Status.Items {
		switch item.Kind {
		case "PersistentVolumeClaim":
			names = append(names, lease.ClaimPrefix+item.Name)
			secret, err := r.sourceRepository(ctx, run, &item)
			var missing *noBackupError
			switch {
			case err == nil:
				names = append(names, lease.RepositoryPrefix+secret.Name)
			case !errors.As(err, &missing):
				return nil, err
			}
		case "Cluster":
			names = append(names, lease.ClusterPrefix+item.Name)
		}
	}
	return names, nil
}

// pause pauses the workloads spec.pauseDuringRestore lists.
//
// It works out the pause (see pausePlan), records it in status.paused,
// status.suspendedKustomizations and status.pausedAt, writes the status, and
// only then suspends the Kustomizations and scales the workloads to zero (see
// applyPause). A failed patch ends the run with reason Failed, which resumes
// what was paused. A failed read is returned for a retry.
func (r *RestoreRunReconciler) pause(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	targets, err := namedTargets(ctx, r.Reader, run.Namespace, run.Spec.PauseDuringRestore)
	if err != nil {
		if !isPauseSpecError(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	pausedWorkloads, suspend, err := pausePlan(ctx, r.Reader, targets)
	if err != nil {
		return ctrl.Result{}, err
	}
	now := metav1.NewTime(r.Now()).Rfc3339Copy()
	run.Status.Paused, run.Status.SuspendedKustomizations, run.Status.PausedAt = pausedWorkloads, suspend, &now
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if err := applyPause(ctx, r.Client, run.Namespace, pausedWorkloads, suspend); err != nil {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
	}
	return ctrl.Result{RequeueAfter: 2 * time.Second}, nil
}

// resume records status.resumedAt, writes the status, and then gives the
// paused workloads their replicas back and resumes the Kustomizations (see
// resumeWorkloads). A later pass that finds a workload not marked Resumed
// scales it back then. It returns the error of a status write or a patch.
func (r *RestoreRunReconciler) resume(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if run.Status.ResumedAt == nil {
		run.Status.ResumedAt = newTime(metav1.NewTime(r.Now()).Rfc3339Copy())
		if err := r.writeStatus(ctx, run); err != nil {
			return err
		}
	}
	return resumeWorkloads(ctx, r.Client, run.Namespace, run.Status.Paused, run.Status.SuspendedKustomizations)
}

// paused reports whether the run has paused workloads.
func paused(run *backupv1alpha1.RestoreRun) bool {
	return run.Status.PausedAt != nil
}

// resumedAll reports whether every paused workload got its replicas back.
func resumedAll(workloads []backupv1alpha1.PausedWorkload) bool {
	for _, w := range workloads {
		if !w.Resumed {
			return false
		}
	}
	return true
}

// anyRestorePending reports whether any item is still Pending, which means
// the run has not started it yet.
func anyRestorePending(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending {
			return true
		}
	}
	return false
}

// abort ends a run early as Failed.
//
// Parameters:
//   - reason is the Ready reason, such as TimedOut or Evicted.
//   - message says why, and goes onto each item that has not ended.
//
// It fails every item that is Pending or Running, and every database item
// that is Deleted or Recovering, since the run stops following it. Then it
// calls finish, which resumes the paused app and gives the run's Leases and
// quota back.
func (r *RestoreRunReconciler) abort(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		switch item.Phase {
		case backupv1alpha1.ItemPending, backupv1alpha1.ItemRunning, backupv1alpha1.ItemDeleted, backupv1alpha1.ItemRecovering:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
	return r.finish(ctx, run, reason, message)
}

// finish ends the run.
//
// Parameters:
//   - reason is the Ready reason. ReasonSucceeded gives the phase
//     Succeeded, and every other reason the phase Failed.
//   - message is the Ready message.
//
// It releases what the run holds (see release), records the phase, the Ready
// condition and status.completedAt, and removes the finalizer. It returns the
// first error; the next pass then tries the rest again.
func (r *RestoreRunReconciler) finish(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	if err := r.release(ctx, run); err != nil {
		return err
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	if reason != backupv1alpha1.ReasonSucceeded {
		run.Status.Phase = backupv1alpha1.RunPhaseFailed
	}
	run.Status.CompletedAt = &now
	run.Status.Workload = ""
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, reason, message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	return dropFinalizer(ctx, r.Client, run)
}

// release puts back what the run changed and gives back what it holds. It
// deletes the restore Job of every volume item that has one, which stops a
// Job that still writes, such as on a timeout. A Job that failed on its own
// stays for its logs; it carries the run as owner and goes away with the
// run. Until the pods of the deleted Jobs have stopped, release returns an
// error, so the caller tries again and the app never runs while restic
// still writes its claim. Then release resumes the paused app, releases the
// run's Leases, and deletes the run's Workload. It returns the first error.
func (r *RestoreRunReconciler) release(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Job == "" {
			continue
		}
		job := &batchv1.Job{}
		err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Job}, job)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return fmt.Errorf("get restore Job %s: %w", item.Job, err)
		}
		if restorejob.Result(job) == restorejob.Failed {
			continue
		}
		if err := deleteJob(ctx, r.Client, run.Namespace, item.Job); err != nil {
			return err
		}
	}
	for i := range run.Status.Items {
		if item := &run.Status.Items[i]; item.Job != "" {
			pod, err := jobPodLeft(ctx, r.Reader, run.Namespace, item.Job)
			if err != nil {
				return err
			}
			if pod != "" {
				return fmt.Errorf("pod %s of restore Job %s is still stopping; the run ends once it is gone", pod, item.Job)
			}
		}
	}
	if paused(run) && !resumedAll(run.Status.Paused) {
		if err := r.resume(ctx, run); err != nil {
			return err
		}
	}
	if err := releaseAll(ctx, r.Leases, run.Namespace, r.holder(run), run.Status.Leases); err != nil {
		return err
	}
	return deleteWorkload(ctx, r.Client, run.Namespace, restoreRunKind.Kind, run.UID)
}

// finalize puts back what a run changed when the run is deleted before it
// finished, then removes the finalizer so the deletion can complete.
func (r *RestoreRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.release(ctx, run); err != nil {
		return err
	}
	if paused(run) {
		if err := r.writeStatus(ctx, run); err != nil {
			return err
		}
	}
	return dropFinalizer(ctx, r.Client, run)
}

// overdue returns the run's deadline, status.startedAt plus spec.timeout,
// and reports whether the run has worked past it. status.startedAt is the
// moment Kueue admitted the run. A run that has not been admitted, or has no
// timeout, is never overdue.
func (r *RestoreRunReconciler) overdue(run *backupv1alpha1.RestoreRun) (time.Time, bool) {
	if run.Status.StartedAt == nil || run.Spec.Timeout == nil {
		return time.Time{}, false
	}
	deadline := run.Status.StartedAt.Add(run.Spec.Timeout.Duration)
	return deadline, !r.Now().Before(deadline)
}

// waitFor sets the run's Ready condition to False with the reason and the
// message, and writes the status. An admitted run stays Running; a run that
// has not started keeps its phase, so the next pass plans or admits it (see
// Reconcile).
func (r *RestoreRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	if run.Status.StartedAt != nil {
		run.Status.Phase = backupv1alpha1.RunPhaseRunning
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

// writeStatus writes the run's status subresource. The write carries the
// resourceVersion the pass read, so a pass that works from an old copy of the
// run fails here and the next pass starts from the stored run.
func (r *RestoreRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set RestoreRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// restoreDone reports whether every item has ended: Succeeded, Failed or
// Skipped.
func restoreDone(items []backupv1alpha1.RestoreItem) bool {
	for _, item := range items {
		switch item.Phase {
		case backupv1alpha1.ItemSucceeded, backupv1alpha1.ItemFailed, backupv1alpha1.ItemSkipped:
		default:
			return false
		}
	}
	return true
}

// restoreFailures returns one line per failed item, naming its kind, its name
// and its message, joined with "; ". It returns an empty string when no item
// failed.
func restoreFailures(items []backupv1alpha1.RestoreItem) string {
	message := ""
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemFailed {
			if message != "" {
				message += "; "
			}
			message += fmt.Sprintf("%s %s: %s", item.Kind, item.Name, item.Message)
		}
	}
	return message
}
