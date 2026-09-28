package runs

import (
	"context"
	"errors"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/lease"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// backupRunKind is the group, version and kind that the owner reference on a
// BackupRun's Kueue Workload names.
var backupRunKind = backupv1alpha1.GroupVersion.WithKind("BackupRun")

// BackupRunReconciler runs BackupRuns. A BackupRun backs up one volume, one
// database, or every volume and database in its namespace that is marked
// backup.wlz.li/enabled.
type BackupRunReconciler struct {
	client.Client

	// Reader reads straight from the API server, without the informer cache.
	// Every pass reads the run and everything it decides on through it, so
	// no decision rests on a cache that lags behind a write.
	Reader client.Reader

	// Snapshots lists the snapshots in a restic repository. The run lists
	// the repository to find the snapshot a sync wrote.
	Snapshots restic.Lister

	// Retimer rewrites a snapshot with a new time and tags. A run that
	// paused the app moves each volume's snapshot to the moment it resumed
	// the app, and tags it paused.
	Retimer restic.Retimer

	// Leases keeps two runs from acting on the same claim, repository,
	// Cluster or paused workloads at once.
	Leases *lease.Leases

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
// BackupRun. It sets Now to time.Now when the caller left it unset.
func (r *BackupRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.BackupRun{}).
		Named("backuprun").
		Complete(r)
}

// Reconcile moves the BackupRun that the request names one step further, and
// requeues until the run has finished.
//
// A run goes through three stages. With no phase, plan lists the items to
// back up. Until status.startedAt is set, admit waits for Kueue to admit the
// run. From then on, work takes the run's Leases, pauses the app on a run
// with spec.all set, starts every item, resumes the app once every clone is
// cut and every database backup completed, and collects each item's result.
//
// Each pass reads the run from the API server. Before any stage, Reconcile
// adds the run's finalizer. A run being deleted gets its changes put back by
// finalize, and a finished run is deleted once spec.ttlSecondsAfterFinished
// has passed. While the controller runs with --pause, a run that has not
// started work waits with reason ControllerPaused. Whenever the Ready reason
// changes during a pass, Reconcile records an event on the run.
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Reader.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Backup") }()
	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, run)
	}
	if run.Status.Phase.Finished() {
		return expire(ctx, r.Client, run, run.Spec.TTLSecondsAfterFinished, run.Status.CompletedAt, r.Now())
	}
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		controllerutil.AddFinalizer(run, Finalizer)
		if err := r.Update(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer to BackupRun %s/%s: %w", run.Namespace, run.Name, err)
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
func (r *BackupRunReconciler) holder(run *backupv1alpha1.BackupRun) lease.Holder {
	return lease.Holder{Kind: backupRunKind.Kind, Name: run.Name, UID: run.UID}
}

// plan records one Pending item for each thing the run backs up and moves the
// run to Queued. It reads only the claims and Clusters, so a run changes
// nothing and reads no repository before Kueue admits it. When items returns
// an error, plan ends the run as Failed with reason Invalid and the error as
// the message.
func (r *BackupRunReconciler) plan(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
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
// spec.source names one claim, spec.database names one Cluster, and a run
// with neither takes every claim and Cluster in the namespace. A claim's item
// has kind ReplicationSource, after the VolSync object that backs it up.
//
// Everything the run backs up has to be marked backup.wlz.li/enabled: "true",
// so a run and a schedule cover the same set. items returns an error when a
// named claim or Cluster is missing or not marked, and when nothing in the
// namespace is marked.
func (r *BackupRunReconciler) items(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	pending := func(kind, name string) backupv1alpha1.BackupItem {
		return backupv1alpha1.BackupItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}

	switch {
	case run.Spec.Source != "":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}, claim); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, fmt.Errorf("no claim %s in this namespace", run.Spec.Source)
			}
			return nil, fmt.Errorf("get claim %s: %w", run.Spec.Source, err)
		}
		if !backupv1alpha1.Enabled(claim.Annotations) {
			return nil, fmt.Errorf("claim %s is not marked %s: \"true\"", claim.Name, backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("ReplicationSource", claim.Name)}, nil

	case run.Spec.Database != "":
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, run.Spec.Database)
		if err != nil {
			return nil, err
		}
		if !found {
			return nil, fmt.Errorf("no Cluster %s in this namespace", run.Spec.Database)
		}
		if !backupv1alpha1.Enabled(cluster.GetAnnotations()) {
			return nil, fmt.Errorf("the Cluster %s is not marked %s: \"true\"", cluster.GetName(), backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("Cluster", cluster.GetName())}, nil

	default:
		claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
		if err != nil {
			return nil, err
		}
		clusters, err := enabledClusters(ctx, r.Reader, run.Namespace)
		if err != nil {
			return nil, err
		}
		var items []backupv1alpha1.BackupItem
		for _, claim := range claims {
			items = append(items, pending("ReplicationSource", claim.Name))
		}
		for _, cluster := range clusters {
			items = append(items, pending("Cluster", cluster.GetName()))
		}
		if len(items) == 0 {
			return nil, fmt.Errorf("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
		}
		return items, nil
	}
}

// admit waits for the namespace's LocalQueue to admit the run, then moves the
// run to Running and records status.startedAt, from which the run's timeout
// counts.
//
// The run goes through Kueue as one Workload that asks for one RunResource,
// so the ClusterQueue's quota bounds how many runs work at once. Once Kueue
// admits it, admit marks the Workload PodsReady, so that Kueue's
// waitForPodsReady does not evict it. While the Workload waits, or while the
// namespace has no LocalQueue (see localQueue), the run stays Queued and admit
// requeues after pollInterval. A backup.wlz.li/timeout on the namespace that
// does not parse fails the run here, before it pauses or starts anything.
func (r *BackupRunReconciler) admit(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
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
	workload, err := ensureWorkload(ctx, r.Client, run, backupRunKind, queue)
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

	if _, err := timeoutFor(ctx, r.Reader, run); err != nil {
		var bad invalidSetting
		if errors.As(err, &bad) {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonInvalid, bad.Error())
		}
		return ctrl.Result{}, err
	}

	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = newTime(metav1.NewTime(r.Now()))
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "backing up")
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
//     run holds one, the run waits with reason Busy and has changed nothing.
//  3. A run with spec.all set that has not paused yet checks that no
//     ReplicationSource is still busy with an earlier backup, then pauses
//     the workloads marked backup.wlz.li/pause-during-backup (see pause).
//  4. It waits until every pod of the paused workloads is gone.
//  5. It starts every Pending item (see startItem).
//  6. Once every clone is cut and no database Backup runs any more, it
//     resumes the app (see resume).
//  7. It collects the result of every Running item (see collectItem).
//
// The run finishes once every item is done and, on a run with spec.all set,
// the app runs again. It finishes Succeeded when no item failed and Failed
// otherwise. While the app is paused, work looks again every two seconds;
// otherwise it waits pollInterval.
func (r *BackupRunReconciler) work(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	if done, err := r.endIfEvicted(ctx, run); done {
		return ctrl.Result{}, err
	}
	deadline, over, err := r.overdue(ctx, run)
	var bad invalidSetting
	if errors.As(err, &bad) {
		// The namespace's timeout was changed to a value that does not
		// parse. Ending the run resumes the app it may have paused.
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonInvalid, bad.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	if over {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonTimedOut,
			fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339)))
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

	if run.Spec.All && run.Status.PausedAt == nil {
		return r.pause(ctx, run)
	}
	if run.Spec.All && run.Status.ResumedAt == nil && anyPending(run.Status.Items) {
		// A pass that stopped between recording the pause and applying it
		// applies it here; every patch sets a fixed value.
		if err := applyPause(ctx, r.Client, run.Namespace, run.Status.Paused, run.Status.SuspendedKustomizations); err != nil {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
		}
		gone, pod, err := pausedPodsGone(ctx, r.Reader, run.Namespace, run.Status.Paused)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			return after(2*time.Second, r.waitFor(ctx, run, backupv1alpha1.ReasonRunning,
				fmt.Sprintf("waiting for pod %s of the paused app to stop before the backup starts", pod)))
		}
	}

	for i := range run.Status.Items {
		if item := &run.Status.Items[i]; item.Phase == backupv1alpha1.ItemPending {
			r.startItem(ctx, run, item)
		}
	}

	if run.Spec.All && run.Status.PausedAt != nil {
		if err := r.resume(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
	}

	for i := range run.Status.Items {
		if item := &run.Status.Items[i]; item.Phase == backupv1alpha1.ItemRunning {
			r.collectItem(ctx, run, item)
		}
	}

	if allDone(run.Status.Items) && (!run.Spec.All || resumed(run)) {
		if failed := failures(run.Status.Items); failed != "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, failed)
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, summary(run.Status.Items))
	}

	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "backing up")
	interval := pollInterval
	if run.Spec.All && run.Status.ResumedAt == nil {
		// The app stays down until every clone is cut and every database
		// backup completed, so the run looks more often here.
		interval = 2 * time.Second
	}
	return after(interval, r.writeStatus(ctx, run))
}

// endIfEvicted ends the run Failed with reason Evicted when Kueue evicted its
// Workload after it admitted the run. Kueue then expects the work to stop,
// and would admit the Workload again from the start, which would pause the
// app a second time. It returns true when it ended the run, with the error
// of abort. A Workload that can't be read does not end the run; the pass
// goes on, so the timeout still ends a stuck run.
func (r *BackupRunReconciler) endIfEvicted(ctx context.Context, run *backupv1alpha1.BackupRun) (bool, error) {
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
// item, and backup-pause on a run with spec.all set.
//
// It reads each claim's VolumeRestore for the repository's Secret name. A
// claim or VolumeRestore that is gone gets no repository Lease; startItem
// fails that item. It returns an error when a read fails for another
// reason.
func (r *BackupRunReconciler) leaseNames(ctx context.Context, run *backupv1alpha1.BackupRun) ([]string, error) {
	var names []string
	if run.Spec.All {
		names = append(names, lease.PauseName)
	}
	for _, item := range run.Status.Items {
		switch item.Kind {
		case "ReplicationSource":
			names = append(names, lease.ClaimPrefix+item.Name)
			secret, err := r.repositoryName(ctx, run.Namespace, item.Name)
			if err != nil {
				return nil, err
			}
			if secret != "" {
				names = append(names, lease.RepositoryPrefix+secret)
			}
		case "Cluster":
			names = append(names, lease.ClusterPrefix+item.Name)
		}
	}
	return names, nil
}

// repositoryName returns the name of the Secret that names a claim's restic
// repository, from the claim's VolumeRestore, or an empty name when the claim
// or its VolumeRestore is gone. It returns an error when a read fails for
// another reason.
func (r *BackupRunReconciler) repositoryName(ctx context.Context, namespace, claimName string) (string, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claimName}, claim); err != nil {
		return "", client.IgnoreNotFound(err)
	}
	vr, err := volumeRestoreFor(ctx, r.Reader, claim)
	if err != nil {
		var missing *missingVolumeRestoreError
		if errors.As(err, &missing) {
			return "", nil
		}
		return "", err
	}
	return vr.Spec.Repository, nil
}

// pause pauses the workloads in the run's namespace that are marked
// backup.wlz.li/pause-during-backup: "true".
//
// It first checks that no ReplicationSource of the run is still busy with an
// earlier backup, which would keep the paused app down for as long as that
// backup takes; the run then waits with reason SourceBusy and the app keeps
// running. It then works out the pause (see pausePlan), records it in
// status.paused, status.suspendedKustomizations and status.pausedAt, writes
// the status, and only then suspends the Kustomizations and scales the
// workloads to zero (see applyPause). A crash between the write and the
// patches leaves a record that the next pass applies. When no workload is
// marked, pause sets status.resumedAt to the same moment, because there is
// nothing to resume. A patch that fails ends the run Failed, and finish
// resumes what was paused.
func (r *BackupRunReconciler) pause(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	for _, item := range run.Status.Items {
		if item.Kind != "ReplicationSource" {
			continue
		}
		source := &volsyncv1alpha1.ReplicationSource{}
		err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source)
		if err == nil && busy(source) && manualTag(source) != TriggerFor(run.UID) {
			return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, busySourceMessage(source)))
		}
	}

	targets, err := pauseTargets(ctx, r.Reader, run.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	paused, suspend, err := pausePlan(ctx, r.Reader, targets)
	if err != nil {
		return ctrl.Result{}, err
	}
	now := metav1.NewTime(r.Now()).Rfc3339Copy()
	run.Status.Paused, run.Status.SuspendedKustomizations, run.Status.PausedAt = paused, suspend, &now
	if len(paused) == 0 {
		run.Status.ResumedAt = newTime(now)
	}
	if err := r.writeStatus(ctx, run); err != nil {
		return ctrl.Result{}, err
	}
	if err := applyPause(ctx, r.Client, run.Namespace, paused, suspend); err != nil {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, err.Error())
	}
	return ctrl.Result{RequeueAfter: time.Second}, nil
}

// resume gives the paused app back once the backup no longer needs it paused:
// every volume's clone is cut, so VolSync reads the clone and no longer the
// app's volume, and no database Backup runs any more, so each base backup
// that completed holds the database as the paused app left it.
//
// It records status.resumedAt and writes the status before it scales
// anything, so every snapshot of the run is moved to that one moment, even
// when the pass stops halfway. It then scales the workloads back and resumes
// the Kustomizations (see resumeWorkloads). A later pass that finds a
// workload not marked Resumed scales it back then. It returns the error of
// a status write or a patch.
func (r *BackupRunReconciler) resume(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if run.Status.ResumedAt == nil {
		if !r.clonesCut(ctx, run) || databasesRunning(run) {
			return nil
		}
		now := metav1.NewTime(r.Now()).Rfc3339Copy()
		run.Status.ResumedAt = &now
		if err := r.writeStatus(ctx, run); err != nil {
			return err
		}
	}
	if resumed(run) {
		return nil
	}
	return resumeWorkloads(ctx, r.Client, run.Namespace, run.Status.Paused, run.Status.SuspendedKustomizations)
}

// resumed reports whether the run has resumed every workload it paused.
func resumed(run *backupv1alpha1.BackupRun) bool {
	if run.Status.ResumedAt == nil {
		return false
	}
	for _, w := range run.Status.Paused {
		if !w.Resumed {
			return false
		}
	}
	return true
}

// databasesRunning reports whether a database item of the run is Running,
// which means its CloudNativePG Backup has not completed or failed yet.
func databasesRunning(run *backupv1alpha1.BackupRun) bool {
	for _, item := range run.Status.Items {
		if item.Kind == "Cluster" && item.Phase == backupv1alpha1.ItemRunning {
			return true
		}
	}
	return false
}

// startItem starts the backup of one Pending item and sets the item's phase.
//
// For a volume, it writes the claim's ReplicationSource with the run's manual
// trigger tag and moves the item to Running. For a database, it creates a
// CloudNativePG Backup and moves the item to Running, or skips the item when
// the Cluster is hibernated. When something goes wrong, startItem marks the
// item Failed with the reason in its message. A ReplicationSource still busy
// with an earlier backup fails the item, since the run holds the claim's
// Lease and so waited for no other run: the source is retrying a backup that
// no run waits for, and the message says what the admin can do.
func (r *BackupRunReconciler) startItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	switch item.Kind {
	case "ReplicationSource":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, claim); err != nil {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("get claim %s: %v", item.Name, err)
			return
		}
		tag := TriggerFor(run.UID)
		source, err := ensureSource(ctx, r.Client, r.Reader, claim, tag)
		if errors.Is(err, errSourceBusy) {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, busySourceMessage(source)
			return
		}
		if err != nil {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return
		}
		item.Phase, item.Trigger = backupv1alpha1.ItemRunning, tag

	case "Cluster":
		cluster, found, err := getCluster(ctx, r.Reader, run.Namespace, item.Name)
		switch {
		case err != nil:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
		case !found:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the Cluster %s no longer exists", item.Name)
		case hibernated(cluster):
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"
		default:
			name, err := ensureBackup(ctx, r.Client, run.Namespace, item.Name, run.UID)
			if err != nil {
				item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
				return
			}
			item.Phase, item.Backup = backupv1alpha1.ItemRunning, name
		}
	}
}

// busySourceMessage returns the message for a ReplicationSource that is still busy
// with another trigger than the run's: which trigger, whether VolSync's last
// mover failed, and what the admin can do.
func busySourceMessage(source *volsyncv1alpha1.ReplicationSource) string {
	message := fmt.Sprintf("ReplicationSource %s is still completing the backup of trigger %s", source.Name, manualTag(source))
	if source.Status != nil && source.Status.LatestMoverStatus != nil && source.Status.LatestMoverStatus.Result == volsyncv1alpha1.MoverResultFailed {
		message += "; its last mover failed and VolSync tries it again. Fix what the mover reports (kubectl describe replicationsource " + source.Name +
			"), or delete the ReplicationSource while no mover pod of it runs, and the next backup writes it again"
	}
	return message
}

// clonesCut reports whether VolSync has cut the clone of every volume the run
// backs up, for this run. The app's volume is then no longer read, so the
// app may resume.
//
// A clone counts as this run's when the claim volsync-<claim>-src is Bound
// and was created at or after status.pausedAt; a clone left from an earlier
// sync holds data from before the pause. A ReplicationSource that has already
// completed the run's trigger has cut its clone too, which catches a clone
// VolSync created and deleted between two passes. A Pending volume item
// means no clone yet. Items that failed or were skipped are left out.
func (r *BackupRunReconciler) clonesCut(ctx context.Context, run *backupv1alpha1.BackupRun) bool {
	for _, item := range run.Status.Items {
		if item.Kind != "ReplicationSource" {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			return false
		case backupv1alpha1.ItemRunning:
		default:
			continue
		}
		source := &volsyncv1alpha1.ReplicationSource{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err == nil && lastManual(source) == item.Trigger {
			continue
		}
		clone := &corev1.PersistentVolumeClaim{}
		key := types.NamespacedName{Namespace: run.Namespace, Name: "volsync-" + item.Name + "-src"}
		if err := r.Reader.Get(ctx, key, clone); err != nil || clone.Status.Phase != corev1.ClaimBound {
			return false
		}
		if run.Status.PausedAt != nil && clone.CreationTimestamp.Before(run.Status.PausedAt) {
			return false
		}
	}
	return true
}

// collectItem records the result of a Running item once it has one, and
// leaves the item Running until then (see collectVolume and
// collectDatabase).
func (r *BackupRunReconciler) collectItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	switch item.Kind {
	case "ReplicationSource":
		r.collectVolume(ctx, run, item)
	case "Cluster":
		r.collectDatabase(ctx, run, item)
	}
}

// collectVolume records the result of a Running volume item.
//
// The item is done when its ReplicationSource's status.lastManualSync equals
// the run's trigger. VolSync sets it only when a sync completes. A mover that
// fails never completes the sync: VolSync tries it again and records the
// result Failed in status.latestMoverStatus, which fails the item. Once the
// sync completed, the item lists the repository and records the snapshot
// the sync wrote (see restic.SyncedSnapshot). No snapshot in the sync means
// the volume held no files, and the item succeeds with Empty set.
//
// On a run that paused the app, the item waits until the app is resumed,
// since a sync can end while a database backup still keeps the app paused.
// The snapshot is then moved to status.resumedAt and tagged paused, plus one
// BaseBackupTag for each database backup that completed while the app was
// paused (see retime). Before the move, the item records the snapshot's ID
// and the pass writes it, since the moved snapshot lies outside the sync's
// window; a later pass moves it by that ID, and a move that already happened
// returns the moved snapshot. The item stays Running until the move
// succeeds, so the run never reports a snapshot that a synced restore can't
// use.
//
// When a database item of the run failed, the snapshot keeps VolSync's time
// and gets no tag. A paused snapshot therefore always comes with a base
// backup of every database the run backed up, and a restore to the paused
// moment never brings a database back ahead of the volume. A failed read
// leaves the item as it is for the next pass, with the error in its message.
func (r *BackupRunReconciler) collectVolume(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
		item.Message = fmt.Sprintf("read ReplicationSource %s: %v", item.Name, err)
		return
	}
	if lastManual(source) != item.Trigger {
		if s := source.Status; s != nil && s.LatestMoverStatus != nil && s.LatestMoverStatus.Result == volsyncv1alpha1.MoverResultFailed {
			item.Phase = backupv1alpha1.ItemFailed
			item.Message = fmt.Sprintf("VolSync's mover failed to back up the volume, and VolSync tries it again; see kubectl describe replicationsource %s", item.Name)
		}
		return
	}
	if source.Status.LastSyncTime == nil || source.Status.LastSyncDuration == nil {
		item.Message = "VolSync completed the sync but recorded no sync time"
		return
	}
	retimed := run.Spec.All && len(run.Status.Paused) > 0
	if retimed && run.Status.ResumedAt == nil {
		// The sync ended while a database backup still runs and the app
		// stays paused. The snapshot moves to the resume moment, which is
		// not known yet, so the item waits for it.
		item.Message = "the snapshot is saved and waits for the app to be resumed, to be moved to that moment"
		return
	}

	secret, err := r.repositorySecret(ctx, run.Namespace, item.Name)
	if err != nil {
		item.Message = err.Error()
		return
	}
	move := retimed && !databaseFailed(run)
	if item.SnapshotID == "" {
		snapshots, err := r.Snapshots.Snapshots(ctx, secret)
		if err != nil {
			item.Message = fmt.Sprintf("list the snapshots: %v", err)
			return
		}
		snapshot, found := restic.SyncedSnapshot(snapshots, source.Status.LastSyncTime.Time, source.Status.LastSyncDuration.Duration)
		if !found {
			item.Phase, item.Empty = backupv1alpha1.ItemSucceeded, true
			item.Message = "the volume held no files, so VolSync took no snapshot"
			return
		}
		if !move {
			item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
			item.Snapshot, item.SnapshotID, item.SnapshotTime = snapshot.ShortID(), snapshot.ID, newTime(metav1.NewTime(snapshot.Time))
			return
		}
		// The move gives the snapshot a time outside the sync's window, so
		// a later pass finds it only by this ID. The pass writes the ID
		// before the next pass moves the snapshot.
		item.SnapshotID = snapshot.ID
		item.Message = fmt.Sprintf("snapshot %s is saved and waits to be moved to %s", snapshot.ShortID(), run.Status.ResumedAt.UTC().Format(time.RFC3339))
		return
	}
	moved, err := r.Retimer.Retime(ctx, secret, item.SnapshotID, run.Status.ResumedAt.Time, restic.PausedTag, baseBackupTags(run)...)
	if err != nil {
		item.Message = fmt.Sprintf("snapshot %s is saved and waits to be moved to %s and tagged %s: %v",
			restic.Snapshot{ID: item.SnapshotID}.ShortID(), run.Status.ResumedAt.UTC().Format(time.RFC3339), restic.PausedTag, err)
		return
	}
	item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
	item.Snapshot, item.SnapshotID, item.SnapshotTime = moved.ShortID(), moved.ID, newTime(metav1.NewTime(moved.Time))
}

// databaseFailed reports whether a database item of the run failed. The
// resume waits for every database item, so on a resumed run the answer is
// final.
func databaseFailed(run *backupv1alpha1.BackupRun) bool {
	for _, item := range run.Status.Items {
		if item.Kind == "Cluster" && item.Phase == backupv1alpha1.ItemFailed {
			return true
		}
	}
	return false
}

// baseBackupTags returns one restic.BaseBackupTag for each database item of
// the run whose base backup completed while the app was paused, and one
// restic.HibernatedTag for each database item skipped because its Cluster
// was hibernated (see startItem).
func baseBackupTags(run *backupv1alpha1.BackupRun) []string {
	var tags []string
	for _, item := range run.Status.Items {
		switch {
		case item.Kind != "Cluster":
		case item.Phase == backupv1alpha1.ItemSucceeded && item.BaseBackupWhilePaused:
			tags = append(tags, restic.BaseBackupTag(item.Name, item.BaseBackup))
		case item.Phase == backupv1alpha1.ItemSkipped:
			tags = append(tags, restic.HibernatedTag(item.Name))
		}
	}
	return tags
}

// collectDatabase records the result of a Running database item, from the
// phase of its CloudNativePG Backup. A completed Backup records its base
// backup ID. When the run paused the app and has not resumed it yet, the
// base backup completed while nothing wrote, and the item records that too.
// A failed Backup fails the item with CloudNativePG's error. A failed read
// leaves the item Running for the next pass.
func (r *BackupRunReconciler) collectDatabase(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	outcome, err := backupResult(ctx, r.Reader, run.Namespace, item.Backup)
	switch {
	case err != nil || !outcome.Done:
	case outcome.Completed:
		item.Phase, item.BaseBackup = backupv1alpha1.ItemSucceeded, outcome.BackupID
		item.BaseBackupWhilePaused = run.Spec.All && len(run.Status.Paused) > 0 && run.Status.ResumedAt == nil && outcome.BackupID != ""
	default:
		item.Phase, item.Message = backupv1alpha1.ItemFailed, outcome.Message
	}
}

// repositorySecret reads the Secret holding the restic repository settings
// for a claim. It finds the Secret's name in spec.repository of the claim's
// VolumeRestore.
//
// Parameters:
//   - namespace is the claim's namespace.
//   - claimName is the claim's name.
//
// It returns an error naming what is missing when the claim, its
// VolumeRestore or the Secret can't be read.
func (r *BackupRunReconciler) repositorySecret(ctx context.Context, namespace, claimName string) (*corev1.Secret, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claimName}, claim); err != nil {
		return nil, fmt.Errorf("get claim %s: %w", claimName, err)
	}
	vr, err := volumeRestoreFor(ctx, r.Reader, claim)
	if err != nil {
		return nil, err
	}
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: vr.Spec.Repository}, secret); err != nil {
		return nil, fmt.Errorf("get Secret %s: %w", vr.Spec.Repository, err)
	}
	return secret, nil
}

// abort ends a run early as Failed.
//
// Parameters:
//   - reason is the Ready reason, such as TimedOut or Evicted.
//   - message says why, and goes onto each unfinished item and the run.
//
// It marks every Pending or Running item Failed with the message, then calls
// finish, which resumes the paused app and gives the run's Leases and quota
// back.
func (r *BackupRunReconciler) abort(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning {
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
// It resumes the app when the run paused it and has not resumed it, releases
// the run's Leases, deletes the run's Workload, records the phase, the Ready
// condition and status.completedAt, and removes the finalizer. It returns the
// first error; the next pass then tries the rest again.
func (r *BackupRunReconciler) finish(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
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

// release puts back what the run changed in the cluster. When the run paused
// the app and has not resumed it, release records status.resumedAt, writes
// it, and resumes the app. It then releases the run's Leases and deletes the
// run's Workload. It returns the first error.
func (r *BackupRunReconciler) release(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if run.Status.PausedAt != nil && !resumed(run) {
		if run.Status.ResumedAt == nil {
			run.Status.ResumedAt = newTime(metav1.NewTime(r.Now()).Rfc3339Copy())
			if err := r.writeStatus(ctx, run); err != nil {
				return err
			}
		}
		if err := resumeWorkloads(ctx, r.Client, run.Namespace, run.Status.Paused, run.Status.SuspendedKustomizations); err != nil {
			return err
		}
	}
	if err := releaseAll(ctx, r.Leases, run.Namespace, r.holder(run), run.Status.Leases); err != nil {
		return err
	}
	return deleteWorkload(ctx, r.Client, run.Namespace, backupRunKind.Kind, run.UID)
}

// finalize puts back what a run changed when the run is deleted before it
// finished, then removes the finalizer so the deletion can complete.
func (r *BackupRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.release(ctx, run); err != nil {
		return err
	}
	if run.Status.PausedAt != nil {
		if err := r.writeStatus(ctx, run); err != nil {
			return err
		}
	}
	return dropFinalizer(ctx, r.Client, run)
}

// overdue returns the run's deadline and reports whether the run has worked
// past it. The deadline is status.startedAt, the moment Kueue admitted the
// run, plus the timeout, and a run that has not been admitted is never
// overdue. timeoutFor resolves the timeout again on every check, so a change
// to the namespace's backup.wlz.li/timeout during a run moves the run's
// deadline.
func (r *BackupRunReconciler) overdue(ctx context.Context, run *backupv1alpha1.BackupRun) (time.Time, bool, error) {
	if run.Status.StartedAt == nil {
		return time.Time{}, false, nil
	}
	timeout, err := timeoutFor(ctx, r.Reader, run)
	if err != nil {
		return time.Time{}, false, err
	}
	deadline := run.Status.StartedAt.Add(timeout)
	return deadline, !r.Now().Before(deadline), nil
}

// waitFor sets the run's Ready condition to False with the reason and the
// message, and writes the status. An admitted run stays Running; a run that
// has not started keeps its phase, so the next pass plans or admits it (see
// Reconcile).
func (r *BackupRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	if run.Status.StartedAt != nil {
		run.Status.Phase = backupv1alpha1.RunPhaseRunning
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

// writeStatus writes the run's status subresource. The write carries the
// resourceVersion the pass read, so a pass that works from an old copy of the
// run fails here and the next pass starts from the stored run.
func (r *BackupRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set BackupRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// anyPending reports whether any item is still Pending, which means the run
// has not started it yet.
func anyPending(items []backupv1alpha1.BackupItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending {
			return true
		}
	}
	return false
}

// allDone reports whether every item has left Pending and Running, so none
// has anything more to do.
func allDone(items []backupv1alpha1.BackupItem) bool {
	for _, item := range items {
		if item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning {
			return false
		}
	}
	return true
}

// failures returns one line per failed item, naming its kind, its name and
// its message, joined with "; ". It returns an empty string when no item
// failed.
func failures(items []backupv1alpha1.BackupItem) string {
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

// summary returns the Ready message for a run that succeeded. It says for
// each item what it captured: the snapshot a volume saved, the base backup a
// database completed, or why the item was skipped or had nothing to save.
func summary(items []backupv1alpha1.BackupItem) string {
	message := ""
	for _, item := range items {
		if message != "" {
			message += "; "
		}
		switch {
		case item.Phase == backupv1alpha1.ItemSkipped:
			message += fmt.Sprintf("%s %s skipped: %s", item.Kind, item.Name, item.Message)
		case item.Snapshot != "":
			message += fmt.Sprintf("%s %s saved snapshot %s", item.Kind, item.Name, item.Snapshot)
		case item.Empty:
			message += fmt.Sprintf("%s %s was empty", item.Kind, item.Name)
		case item.BaseBackup != "":
			message += fmt.Sprintf("%s %s completed base backup %s", item.Kind, item.Name, item.BaseBackup)
		default:
			message += fmt.Sprintf("%s %s succeeded", item.Kind, item.Name)
		}
	}
	return message
}

// dropFinalizer removes the run's finalizer, if it still has one. Callers
// call it last, once the run has put back everything it changed. Both
// BackupRuns and RestoreRuns use it.
func dropFinalizer(ctx context.Context, c client.Client, run client.Object) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(run, Finalizer)
	if err := c.Update(ctx, run); err != nil {
		return fmt.Errorf("remove the finalizer from %s: %w", run.GetName(), err)
	}
	return nil
}

// expire deletes a finished run once its time to live has passed, and
// requeues the run for that moment until then. Both BackupRuns and
// RestoreRuns use it.
//
// Parameters:
//   - run is the finished BackupRun or RestoreRun.
//   - ttl is the run's spec.ttlSecondsAfterFinished. When it is nil, the run
//     is kept for good.
//   - completed is the run's status.completedAt, which the time to live
//     counts from.
//   - now is the reconciler's current time.
//
// A run that is already gone when expire deletes it is not an error.
func expire(ctx context.Context, c client.Client, run client.Object, ttl *int32, completed *metav1.Time, now time.Time) (ctrl.Result, error) {
	if ttl == nil || completed == nil {
		return ctrl.Result{}, nil
	}
	deadline := completed.Add(time.Duration(*ttl) * time.Second)
	if remaining := deadline.Sub(now); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	if err := c.Delete(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete the expired %s: %w", run.GetName(), err)
	}
	return ctrl.Result{}, nil
}
