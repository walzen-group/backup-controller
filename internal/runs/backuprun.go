package runs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/kueue"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
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
	// The run reads claims, ReplicationSources, Clusters, Secrets, pods,
	// LocalQueues and Kustomizations through it. The controller has no reason
	// to watch any of these kinds.
	Reader client.Reader

	// Snapshots lists the snapshots in a restic repository. The run uses it to
	// find the snapshot a volume's sync wrote, with the time restic stamped
	// on it.
	Snapshots SnapshotLister

	// Retimer rewrites a snapshot with a new time and a tag. A quiesced run
	// uses it to move each volume's snapshot to the moment the run started the
	// workloads again, and to tag it quiesced.
	Retimer SnapshotRetimer

	// Recorder writes an event on the run each time its Ready reason changes.
	Recorder events.EventRecorder

	// Now returns the current time. Tests replace it so they can move time
	// forward without sleeping.
	Now func() time.Time

	// schemas caches the check that the installed CRD of the run's kind
	// declares every field the controller writes (see crdOutdated).
	schemas schemaCache
}

// SetupWithManager registers the reconciler with mgr so it runs for every
// BackupRun. It sets Now to time.Now when the caller left it unset.
func (r *BackupRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	r.serve()
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.BackupRun{}).
		Named("backuprun").
		Complete(r)
}

// Reconcile moves the BackupRun that req names one step further, and requeues
// until the run has finished.
//
// A run goes through three stages, chosen by status.phase. With no phase, plan
// lists the items to back up. In Queued, admit waits for the namespace's
// LocalQueue to admit the run. From then on, work stops the workloads marked
// for quiesce (on a run with spec.all set), starts every item, starts the
// workloads again once the clones are cut, and collects each item's result.
// Each stage acts only on what the last one wrote to the status, so a
// reconcile that runs twice from the same status does the same thing twice.
//
// Before any stage, Reconcile adds the run's finalizer. A run being deleted
// gets its changes put back by finalize, and a finished run is deleted once
// spec.ttlSecondsAfterFinished has passed. Whenever the Ready reason changes
// during a reconcile, Reconcile records an event on the run.
//
// An unfinished run with no spec.database that is not being deleted first
// goes through volsyncUnsupported. While VolSync serves its kinds only at a
// version other than v1alpha1, the run ends through endForVolSync with
// reason VolSyncUnsupported, which gives the app back: that needs no
// VolSync object. A run being deleted goes to finalize as usual.
//
// A run that recorded status.ending has decided to end, and every later pass
// only finishes it with that reason and message (see finish), also one that
// retries a failed restart after the run timed out.
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	before := readyReason(run.Status.Conditions)
	defer func() { announce(r.Recorder, run, run.Status.Conditions, before, "Backup") }()
	// A run that may touch VolSync objects can't go on while VolSync serves
	// its kinds only at a version this controller has no Go types for, and
	// ends. A database-only run needs no VolSync object and goes on.
	if !run.Status.Phase.Finished() && run.DeletionTimestamp.IsZero() && run.Spec.Database == "" {
		if message := volsyncUnsupported(r.RESTMapper()); message != "" {
			return ctrl.Result{}, r.endForVolSync(ctx, run, message)
		}
	}
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
	if ending := run.Status.Ending; ending != nil {
		return ctrl.Result{}, r.finish(ctx, run, ending.Reason, ending.Message)
	}

	switch run.Status.Phase {
	case "":
		return r.plan(ctx, run)
	case backupv1alpha1.RunPhaseQueued:
		return r.admit(ctx, run)
	default:
		return r.work(ctx, run)
	}
}

// plan records one Pending item for each thing the run backs up and moves the
// run to Queued. When items refuses the run (see asRunRefusal), plan ends
// the run as Failed with reason Invalid and the refusal as the message. Any
// other error, such as a timeout from the API server, is returned so the
// reconcile runs again.
//
// Before anything else, plan checks the installed BackupRun CRD (see
// schemaOutdated) and ends a run it refuses with reason CRDOutdated.
func (r *BackupRunReconciler) plan(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	if refused, err := r.schemaOutdated(ctx, run); refused || err != nil {
		return ctrl.Result{}, err
	}
	items, err := r.items(ctx, run)
	if asRunRefusal(err) {
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}
	if err != nil {
		return ctrl.Result{}, err
	}
	run.Status.Items = items
	run.Status.Phase = backupv1alpha1.RunPhaseQueued
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonQueued,
		"waiting for the backup queue to admit the run")
	return after(time.Second, r.writeStatus(ctx, run))
}

// schemaOutdated checks that the installed BackupRun CRD declares every
// field the controller writes on a BackupRun, such as status.restartPending,
// and ends the run as Failed with reason CRDOutdated when it does not. Items
// a planned run has not finished fail with the same message.
//
// It returns true when it ended the run, and an error when the CRD could not
// be read in a way a retry may fix or when ending the run failed. The check
// writes nothing else, so it is safe to repeat.
func (r *BackupRunReconciler) schemaOutdated(ctx context.Context, run *backupv1alpha1.BackupRun) (bool, error) {
	message, err := r.schemas.crdOutdated(ctx, r.Reader, backupRunsCRD, "BackupRun", backupv1alpha1.BackupRun{},
		"the run could leave the workloads it stops at 0 replicas")
	if err != nil || message == "" {
		return false, err
	}
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
	return true, r.finish(ctx, run, backupv1alpha1.ReasonCRDOutdated, message)
}

// items returns one Pending item for each thing the run's spec names.
// spec.source names one claim, spec.database names one Cluster, and a run
// with neither takes every claim and Cluster in the namespace. A claim's item
// has kind ReplicationSource, after the VolSync object that backs it up.
//
// Everything the run backs up has to be marked backup.wlz.li/enabled: "true",
// so a run and a schedule cover the same set. items returns an
// *invalidSpecError when a named claim or Cluster is missing or not marked,
// and when nothing in the namespace is marked. A cluster without the
// CloudNativePG CRDs holds no Cluster. Any other failed read comes back as a
// plain error.
func (r *BackupRunReconciler) items(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	pending := func(kind, name string) backupv1alpha1.BackupItem {
		return backupv1alpha1.BackupItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}

	switch {
	case run.Spec.Source != "":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}, claim); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, invalidSpec("no claim %s in this namespace", run.Spec.Source)
			}
			return nil, fmt.Errorf("get claim %s: %w", run.Spec.Source, err)
		}
		if !backupv1alpha1.Enabled(claim.Annotations) {
			return nil, invalidSpec("claim %s is not marked %s: \"true\"", claim.Name, backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("ReplicationSource", claim.Name)}, nil

	case run.Spec.Database != "":
		cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Spec.Database)
		if err != nil {
			if meta.IsNoMatchError(err) {
				return nil, invalidSpec("no Cluster %s in this namespace; the cluster has no CloudNativePG CRDs", run.Spec.Database)
			}
			return nil, err
		}
		if !found {
			return nil, invalidSpec("no Cluster %s in this namespace", run.Spec.Database)
		}
		if !backupv1alpha1.Enabled(cluster.GetAnnotations()) {
			return nil, invalidSpec("the Cluster %s is not marked %s: \"true\"", cluster.GetName(), backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("Cluster", cluster.GetName())}, nil

	default:
		claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
		if err != nil {
			return nil, err
		}
		clusters, err := cnpg.EnabledClusters(ctx, r.Reader, r.RESTMapper(), run.Namespace)
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
			return nil, invalidSpec("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
		}
		return items, nil
	}
}

// admit waits for the namespace's LocalQueue to admit the run, then moves the
// run to Running and records status.startedAt. A namespace with no LocalQueue
// starts the run at once.
//
// The run goes through Kueue as one Workload, which counts as one pod against
// the queue's quota. Once Kueue admits it, admit marks the Workload PodsReady,
// so that Kueue's waitForPodsReady does not evict it. While the Workload
// waits, admit requeues after pollInterval, and a run Kueue does not admit
// within its timeout from its creation fails (see awaitAdmission).
func (r *BackupRunReconciler) admit(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	queue, err := kueue.LocalQueue(ctx, r.Reader, r.RESTMapper(), run.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if queue != "" {
		workload, err := kueue.EnsureWorkload(ctx, r.Client, run, backupRunKind, queue)
		if err != nil {
			return ctrl.Result{}, err
		}
		if run.Status.Workload != workload.GetName() {
			run.Status.Workload = workload.GetName()
			if err := r.writeStatus(ctx, run); err != nil {
				return ctrl.Result{}, err
			}
		}
		if !kueue.Admitted(workload) {
			return r.awaitAdmission(ctx, run, workload, queue)
		}
		if err := kueue.MarkPodsReady(ctx, r.Client, workload, metav1.NewTime(r.Now())); err != nil {
			return ctrl.Result{}, err
		}
	}

	// A backup.wlz.li/timeout on the namespace that does not parse fails the
	// run here, before it stops or starts anything, because the run would
	// have no deadline to keep.
	if _, err := timeoutFor(ctx, r.Reader, run); err != nil {
		var bad invalidSettingError
		if errors.As(err, &bad) {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
		}
		return ctrl.Result{}, err
	}
	// The same goes for a backup.wlz.li/max-quiesce that does not parse on a
	// run that stops workloads: it would have no limit on how long they stay
	// down.
	if run.Spec.All {
		if _, err := maxQuiesceFor(ctx, r.Reader, run.Namespace); err != nil {
			var bad invalidSettingError
			if errors.As(err, &bad) {
				return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
			}
			return ctrl.Result{}, err
		}
	}

	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.StartedAt = newTime(metav1.NewTime(r.Now()))
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning, "backing up")
	return after(time.Second, r.writeStatus(ctx, run))
}

// awaitAdmission keeps a run whose Workload Kueue has not admitted yet
// Queued, and fails it once it has waited longer than its timeout.
//
// Parameters:
//   - run is the Queued BackupRun.
//   - workload is the run's Workload, as kueue.EnsureWorkload read or created it.
//   - queue is the LocalQueue the Workload waits in.
//
// A run with no status.startedAt is never overdue (see overdue), and a
// namespace's schedule starts no run while one is unfinished, so a run that
// is never admitted would stop the namespace's backups for good without a
// word. That happens when a Kueue release renames the Workload's queueName
// field or its Admitted condition, and when the queue has no quota to give.
// The wait is bounded by the run's timeout (spec.timeout, the namespace's
// backup.wlz.li/timeout, or 6h), counted from the run's creation. Past it,
// the run ends Failed with a message that names Kueue, the Workload and the
// LocalQueue, and finish deletes the Workload. Until then it returns a
// result that looks again after pollInterval. It returns an error when the
// timeout can't be read or ending the run fails.
func (r *BackupRunReconciler) awaitAdmission(ctx context.Context, run *backupv1alpha1.BackupRun, workload *unstructured.Unstructured, queue string) (ctrl.Result, error) {
	timeout, err := timeoutFor(ctx, r.Reader, run)
	if err != nil {
		var bad invalidSettingError
		if errors.As(err, &bad) {
			return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
		}
		return ctrl.Result{}, err
	}
	deadline := run.CreationTimestamp.Add(timeout)
	if run.CreationTimestamp.IsZero() || r.Now().Before(deadline) {
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	message := fmt.Sprintf("Kueue did not admit the run's Workload %s/%s in LocalQueue %s within the run's timeout of %s from its creation: "+
		"the Workload's status.conditions hold no Admitted condition with status True. Check the LocalQueue and its ClusterQueue; "+
		"if the queue has quota to give, a Kueue release may have changed the Workload's fields (see docs/compatibility.md)",
		workload.GetNamespace(), workload.GetName(), queue, timeout)
	return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, message)
}

// work makes one pass over a run that the queue has admitted, and returns when
// to look again.
//
// Parameters:
//   - run is the admitted BackupRun, as the pass read it. Its status is
//     changed and written during the pass.
//
// It returns when to look again: a second after a pass that stopped the app,
// every two seconds while the workloads are stopped, and after pollInterval
// otherwise. A failed read or write comes back as an error, and
// controller-runtime runs the pass again with its backoff; a failed restart
// comes back the same way, after releaseFailed has reported it on the run.
//
// Each pass first releases the Leases of the items that finished in an
// earlier pass. That release is best effort: a failure is logged and the
// pass goes on, so it never keeps the workloads down past the limit below.
//
// On a run with spec.all set, the first pass calls quiesce to stop the
// workloads marked backup.wlz.li/quiesce and does nothing else. Later passes
// start no item until every pod of those workloads is gone.
//
// work then starts every Pending item (see startPending). An item that
// fails to start with an error other than a refusal stays Pending and
// records the error in status.items[].lastStartError, the Ready condition
// takes reason Retrying and names each such item, and the pass goes on; the
// next pass tries the item again. Items that wait for another run are named
// as well: the Retrying message names every such wait, and without a retry
// the run waits with reason SourceBusy and a message that names every wait
// and nothing else. A note on a Running item, such as a Backup phase
// CloudNativePG 1.30 does not have, goes into a Running or Retrying message
// and onto the item's own message.
//
// Once every volume's clone is cut, work records the time of the pass in
// status.restartedAt with status.restartPending set, and writes the status.
// It then scales the workloads back up, resumes the Kustomizations it
// suspended, and clears status.restartPending. When that restart fails, the
// pass reports it at once with reason RestartFailed and a message that says
// the run is still backing up and must not be deleted, and how to give the
// app back by hand (see releaseFailed). A pass that finds
// status.restartPending set repeats the restart and keeps the recorded
// moment, once it has read the run again through the uncached Reader and
// the stored run still has the flag set (see readStop). Once the stored
// status shows the restart done, no pass repeats it. Times in the status
// are whole seconds. Last, work collects the result of every Running item.
//
// The workloads stay stopped for at most the namespace's
// backup.wlz.li/max-quiesce limit, ten minutes by default, counted from
// status.quiescedAt. A pass at or past that moment first fails every volume
// item whose clone is not cut (see giveUpUncut), skips the wait for pods,
// and so reaches the restart above. That also bounds pods that never stop and
// a VolSync that never cuts a clone. A limit that does not parse aborts the
// run, which starts the workloads again.
//
// The run finishes once every item is done and, on a run with spec.all set,
// the workloads are running again. It finishes Succeeded when no item failed
// and Failed otherwise. A run past its timeout ends as Failed through
// timeOut, which fails every unfinished item with reason TimedOut.
func (r *BackupRunReconciler) work(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	// The status keeps times in whole seconds. A snapshot moved in this pass
	// must carry the restartedAt that later passes read back.
	now := metav1.NewTime(r.Now()).Rfc3339Copy()
	deadline, over, err := r.overdue(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if over {
		return ctrl.Result{}, r.timeOut(ctx, run, deadline)
	}
	// The Leases of an item that finished in an earlier pass go now, so a
	// restore of that claim need not wait for the rest of the run. This is
	// best effort: an error here must not keep the workloads down past the
	// limit below. A Lease left behind is taken over once its item is done
	// (see holderLive), and finish releases it again.
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(name string) bool { return backupItemDone(run, name) }); err != nil {
		log.FromContext(ctx).Error(err, "could not release the Leases of the run's finished items; the run goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	// The quiesce Leases go once the stored status shows every workload back,
	// since the run then never touches them again. Best effort as well: a
	// Lease left behind is stale under holderLive's rule and the next run
	// takes it over.
	if durablyRestarted(run) {
		if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
			log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the run goes on",
				"namespace", run.Namespace, "name", run.Name)
		}
	}

	if run.Spec.All && run.Status.QuiescedAt == nil {
		return r.quiesce(ctx, run, now)
	}

	limited := false
	if run.Spec.All && run.Status.RestartedAt == nil && len(run.Status.Quiesced) > 0 {
		limit, err := maxQuiesceFor(ctx, r.Reader, run.Namespace)
		if err != nil {
			// A limit that no longer parses fails the run, which starts the
			// workloads again, so they are never held without a limit.
			var bad invalidSettingError
			if errors.As(err, &bad) {
				return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, bad.Error())
			}
			return ctrl.Result{}, err
		}
		if !now.Time.Before(run.Status.QuiescedAt.Add(limit)) {
			r.giveUpUncut(ctx, run, limit, now)
			limited = true
		}
	}

	if run.Spec.All && run.Status.RestartedAt == nil && !limited && anyPending(run.Status.Items) {
		targets, err := quiesce.Targets(ctx, r.Reader, run.Namespace)
		if err != nil {
			return ctrl.Result{}, err
		}
		gone, pod, err := quiesce.PodsGone(ctx, r.Reader, run.Namespace, targets)
		if err != nil {
			return ctrl.Result{}, err
		}
		if !gone {
			return after(2*time.Second, r.waitFor(ctx, run, backupv1alpha1.ReasonRunning,
				fmt.Sprintf("waiting for pod %s to stop before the clones are cut", pod)))
		}
	}

	// An item that fails to start with an error the API server may stop
	// giving keeps its place and is tried again on the next pass. The pass
	// goes on, because the restart below looks only at volume items, and a
	// database whose Backup cannot be created must not keep the app down.
	waits, retrying := r.startPending(ctx, run)

	restart := false
	if run.Spec.All && run.Status.RestartedAt == nil && r.clonesCut(ctx, run) {
		// The moment is written before the workloads start, so a pass that
		// starts them and then loses its status write is retried with it.
		run.Status.RestartedAt, run.Status.RestartPending = newTime(now), true
		if err := r.writeStatus(ctx, run); err != nil {
			return ctrl.Result{}, err
		}
		// The write decodes the stored object back into run, and a CRD that
		// lacks status.restartPending would have dropped the flag from it.
		// This pass restarts on what it just decided, so it never relies on
		// a field the write may have dropped.
		restart = true
	}
	if !restart && run.Status.RestartPending {
		// The flag came from the informer cache, which may lag behind a
		// pass that has since done the restart; see readStop.
		if _, err := readStop(ctx, r.Reader, run); err != nil {
			return ctrl.Result{}, err
		}
	}
	if restart || run.Status.RestartPending {
		if err := quiesce.Restart(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
			// The run says why the app is still down at once, rather than
			// only at its timeout, and the error makes the pass run again.
			return ctrl.Result{}, r.releaseFailed(ctx, run, err, true)
		}
		run.Status.RestartPending = false
	}

	var notes []string
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase != backupv1alpha1.ItemRunning {
			continue
		}
		if note := r.collectItem(ctx, run, item); note != "" {
			notes = append(notes, note)
		}
	}

	if allDone(run.Status.Items) && (!run.Spec.All || run.Status.RestartedAt != nil) {
		failed := failures(run.Status.Items)
		if failed == "" {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, summary(run.Status.Items))
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, failed)
	}

	reason, message := backupv1alpha1.ReasonRunning, "backing up"
	switch {
	case len(retrying) > 0:
		// An item that waits for another run is named as well, so the retry
		// does not hide it.
		reason, message = backupv1alpha1.ReasonRetrying, "retrying the start of "+strings.Join(retrying, "; ")
		if len(waits) > 0 {
			message += "; " + strings.Join(waits, "; ")
		}
	case len(waits) > 0:
		// Every wait is named, so one item's wait does not hide another's.
		// A note stays on its own item's message, so a timeout that copies
		// this wait does not repeat it.
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, strings.Join(waits, "; ")))
	}
	if len(notes) > 0 {
		// A Backup in a phase that needs naming says why the run goes on.
		message += "; " + strings.Join(notes, "; ")
	}
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	interval := pollInterval
	if run.Spec.All && run.Status.RestartedAt == nil {
		// The app stays down until every clone is cut, so the run looks
		// more often here than in its other waits.
		interval = 2 * time.Second
	}
	return after(interval, r.writeStatus(ctx, run))
}

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
			held := holder(ctx, r.Reader, source)
			switch {
			case errors.Is(held, errSourceAbandoned):
				failBackupItem(item, held)
			case errors.Is(held, errSourceBusy):
				return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, held.Error()))
			default:
				return ctrl.Result{}, held
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

// startPending tries to start every Pending item of the run, and returns
// what keeps the items that stay Pending from starting.
//
// Parameters:
//   - run is the admitted BackupRun. Its items are changed in place, and the
//     caller writes the status.
//
// It returns the waits, one sentence for each item that waits for another
// run (see startItem), and the retries, one line for each item whose start
// failed with an error a later pass may not get, naming the item and the
// error.
//
// An item whose start fails that way records the error in
// status.items[].lastStartError, and its message says it has not started
// yet and why. Before each attempt, startPending clears what an earlier
// failed attempt left on the item, so an item that starts, waits, or is
// refused carries no stale error.
func (r *BackupRunReconciler) startPending(ctx context.Context, run *backupv1alpha1.BackupRun) (waits, retrying []string) {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		// A Pending item carries a message only from a failed start.
		item.LastStartError, item.Message = "", ""
		wait, err := r.startItem(ctx, run, item)
		if err != nil {
			item.LastStartError = err.Error()
			item.Message = "not started yet: " + item.LastStartError
			retrying = append(retrying, fmt.Sprintf("%s %s: %v", item.Kind, item.Name, err))
			continue
		}
		if wait != "" {
			waits = append(waits, wait)
		}
	}
	return waits, retrying
}

// startItem starts the backup of one Pending item and sets the item's phase.
//
// Parameters:
//   - run is the admitted BackupRun, for its namespace, its UID and the
//     trigger tag built from it.
//   - item is the Pending item to start. It is changed in place, and the
//     caller writes the status.
//
// It returns a message for the run's Ready condition when the item has to
// wait, and an empty string otherwise. It returns an error for any failed
// read or write a later pass may not get, such as a timeout from the API
// server or a 500 from a webhook it cannot reach, with the item left
// Pending. The caller, startPending, records that error in
// status.items[].lastStartError and tries the item again on its next pass.
//
// For a volume, it writes the claim's ReplicationSource with the run's manual
// trigger tag and moves the item to Running. For a database, it creates a
// CloudNativePG Backup and moves the item to Running, or skips the item when
// the Cluster is hibernated, with reason ClusterHibernated. When the claim or
// the Cluster is gone, or the claim's settings are refused, startItem fails
// the item with the refusal (see failBackupItem), which records the item's
// reason and says why in its message. The same goes for a Backup the API
// server rejects as invalid.
//
// An item waits when the volume's ReplicationSource is still completing the
// backup of another run that waits for it, or when a RestoreRun's mover
// works on the claim or its repository (see otherMover). The message names
// that run, and the item stays Pending. When the source is busy with a
// backup no run waits for (see holder), the item fails at once with a
// message that says what a person can do, and the source is left alone.
func (r *BackupRunReconciler) startItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) (string, error) {
	switch item.Kind {
	case "ReplicationSource":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, claim); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", fmt.Errorf("get claim %s: %w", item.Name, err)
			}
			failBackupItem(item, claimGone(item.Name))
			return "", nil
		}
		tag := TriggerFor(run.UID)
		_, err := ensureSource(ctx, r.Client, r.Reader, claim, tag, leaseHolder{kind: "BackupRun", run: run, item: item.Name})
		if errors.Is(err, errSourceBusy) {
			return err.Error(), nil
		}
		// A source no run waits for any more and a refused claim fail the
		// item; any other error leaves it Pending for the next pass.
		if failBackupItem(item, err) {
			return "", nil
		}
		if err != nil {
			return "", err
		}
		item.Phase, item.Trigger = backupv1alpha1.ItemRunning, tag

	case "Cluster":
		cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		switch {
		case err != nil:
			return "", err
		case !found:
			failBackupItem(item, refuse(backupv1alpha1.ItemReasonClusterMissing, "the Cluster %s no longer exists", item.Name))
		case cnpg.Hibernated(cluster):
			item.Phase, item.Reason = backupv1alpha1.ItemSkipped, backupv1alpha1.ItemReasonClusterHibernated
			item.Message = "the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"
		default:
			name, err := cnpg.EnsureBackup(ctx, r.Client, run.Namespace, item.Name, run.UID)
			if apierrors.IsInvalid(err) {
				failBackupItem(item, refuse(backupv1alpha1.ItemReasonBackupRefused, "%v", err))
				return "", nil
			}
			if err != nil {
				return "", err
			}
			item.Phase, item.Backup = backupv1alpha1.ItemRunning, name
		}
	}
	return "", nil
}

// clonesCut reports whether VolSync has cut the clone of every volume the run
// backs up. That is the moment the stopped workloads can start again, because
// VolSync uploads from the clone and no longer reads the app's volume.
//
// A clone counts as cut when the claim volsync-<claim>-src is Bound, is not
// being deleted, and was created after status.quiescedAt. VolSync marks a sync
// done before its cleanup deletes the clone, so the clone of the previous
// sync can still be there when this run starts, and it holds the data from
// before the app stopped. This run writes its trigger at least one pass after
// quiescedAt, so its own clone is always newer.
//
// A ReplicationSource that has already completed the run's trigger tag has
// cut its clone too. That check catches a clone VolSync created and deleted
// between two passes. A Pending volume item means no clone yet, and items
// that failed or were skipped are left out.
func (r *BackupRunReconciler) clonesCut(ctx context.Context, run *backupv1alpha1.BackupRun) bool {
	for _, item := range run.Status.Items {
		if item.Kind != "ReplicationSource" {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			return false
		case backupv1alpha1.ItemRunning:
			if !r.cloneCut(ctx, run, item) {
				return false
			}
		default:
			// An item in any other phase ended or never started a sync, so
			// it has no clone to wait for.
		}
	}
	return true
}

// cloneCut reports whether VolSync has cut the clone of the Running volume
// item, by the rules clonesCut describes. A failed read counts as no clone.
func (r *BackupRunReconciler) cloneCut(ctx context.Context, run *backupv1alpha1.BackupRun, item backupv1alpha1.BackupItem) bool {
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err == nil && lastManual(source) == item.Trigger {
		return true
	}
	clone := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: cloneName(item.Name)}
	if err := r.Reader.Get(ctx, key, clone); err != nil || clone.Status.Phase != corev1.ClaimBound {
		return false
	}
	return clone.DeletionTimestamp.IsZero() && run.Status.QuiescedAt != nil && clone.CreationTimestamp.After(run.Status.QuiescedAt.Time)
}

// cloneName returns the name of the claim VolSync clones the given claim
// into for a backup.
func cloneName(claim string) string { return "volsync-" + claim + "-src" }

// giveUpUncut fails every volume item of the run whose clone VolSync has not
// cut, because the workloads have been stopped for the namespace's
// backup.wlz.li/max-quiesce limit and the pass starts them again.
//
// Parameters:
//   - limit is the limit that ran out, for the message.
//   - now is the time of the pass, which becomes status.restartedAt.
//
// A Pending item fails with a message that says it never started, with the
// error of its last start attempt (status.items[].lastStartError) when it
// has one. A Running item whose clone is not cut fails with a message that
// names the missing clone. A Running item whose clone is cut goes on. A Running item's message also says what
// data a snapshot of the sync VolSync goes on with holds (see syncGoesOn).
// Afterwards clonesCut is true, so the
// caller's restart path records status.restartedAt and starts the workloads.
// The check reads only the clones; a failed read counts as no clone, so an
// API outage never keeps the workloads down past the limit.
func (r *BackupRunReconciler) giveUpUncut(ctx context.Context, run *backupv1alpha1.BackupRun, limit time.Duration, now metav1.Time) {
	at := now.UTC().Format(time.RFC3339)
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != backupv1alpha1.ItemKindSource {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			message := fmt.Sprintf("not started before the workloads were given back at %s, when the %s limit of %s ran out, so the clone %s was never cut",
				at, backupv1alpha1.AnnotationMaxQuiesce, limit, cloneName(item.Name))
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message+startErrorNote(*item, ": ")
		case backupv1alpha1.ItemRunning:
			if r.cloneCut(ctx, run, *item) {
				continue
			}
			message := fmt.Sprintf("VolSync had not cut the clone %s by %s, when the %s limit of %s ran out and the workloads were given back",
				cloneName(item.Name), at, backupv1alpha1.AnnotationMaxQuiesce, limit)
			if note := r.syncGoesOn(ctx, run, *item, nil); note != "" {
				message += ". " + note
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		default:
			// An item in any other phase has already ended, so the limit
			// changes nothing about it.
		}
	}
}

// syncGoesOn returns a sentence for the message of a Running volume item the
// run fails while VolSync goes on with the item's sync, and "" when VolSync
// has no sync of the item's trigger open.
//
// Parameters:
//   - item is the volume item, with the trigger tag the run wrote.
//   - source is the claim's ReplicationSource as the caller read it, or nil
//     for syncGoesOn to read it.
//
// VolSync keeps a sync open until a mover Job succeeds, and every Job of the
// sync reads the clone it cut when the sync started (the status's
// lastSyncStartTime). restic stamps a snapshot with the time its mover ran,
// so a snapshot a retry saves after the run gave the item up carries a later
// time than the data it holds. The sentence names the data such a snapshot
// holds: that of the sync's start when the clone is cut, and that of the
// moment VolSync cuts the clone otherwise. The clone counts as cut when the
// claim volsync-<claim>-src is Bound, is not being deleted, and was created
// no earlier than the sync started. A failed read gives "", since the
// sentence only explains the item's failure.
func (r *BackupRunReconciler) syncGoesOn(ctx context.Context, run *backupv1alpha1.BackupRun, item backupv1alpha1.BackupItem, source *volsyncv1alpha1.ReplicationSource) string {
	if item.Kind != "ReplicationSource" || item.Trigger == "" {
		return ""
	}
	if source == nil {
		source = &volsyncv1alpha1.ReplicationSource{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
			return ""
		}
	}
	if manualTag(source) != item.Trigger || lastManual(source) == item.Trigger || source.Status == nil || source.Status.LastSyncStartTime == nil {
		return ""
	}
	start := source.Status.LastSyncStartTime
	at := start.UTC().Format(time.RFC3339)
	clone := &corev1.PersistentVolumeClaim{}
	err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: cloneName(item.Name)}, clone)
	if err != nil && !apierrors.IsNotFound(err) {
		return ""
	}
	if err == nil && clone.Status.Phase == corev1.ClaimBound && clone.DeletionTimestamp.IsZero() && !clone.CreationTimestamp.Before(start) {
		return fmt.Sprintf("VolSync keeps retrying the sync it started at %s with the clone it cut then, so a snapshot this sync saves later "+
			"holds the data of %s, whatever time restic stamps on it.", at, at)
	}
	return fmt.Sprintf("VolSync goes on with the sync it started at %s and has not cut its clone yet, so a snapshot this sync saves later "+
		"holds the data of the moment it cuts the clone, whatever time restic stamps on it.", at)
}

// collectItem records the result of a Running item once it has one, and
// leaves the item Running until then.
//
// Parameters:
//   - run is the BackupRun, for its namespace, its start and its restart
//     moment.
//   - item is the Running item. It is changed in place.
//
// It returns a sentence for the run's Ready message while a database item
// waits in a Backup phase that needs naming, such as one CloudNativePG 1.30
// does not have, and "" otherwise.
//
// A volume item follows its ReplicationSource (see collectVolume), and a
// database item follows the phase of its CloudNativePG Backup (see
// collectDatabase).
func (r *BackupRunReconciler) collectItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) string {
	switch item.Kind {
	case backupv1alpha1.ItemKindSource:
		r.collectVolume(ctx, run, item)
	case backupv1alpha1.ItemKindCluster:
		return r.collectDatabase(ctx, run, item)
	}
	return ""
}

// collectVolume records the result of a Running volume item once its
// ReplicationSource has one.
//
// Parameters:
//   - run is the BackupRun, for its namespace, its start and its restart
//     moment.
//   - item is the Running volume item, which names the claim and the run's
//     trigger tag. It is changed in place.
//
// A volume item is done when its ReplicationSource has completed the run's
// trigger tag. VolSync completes a tag only after a mover Job succeeds.
// While the tag is open, a Failed mover result of this run's sync fails the
// item (see failedSync). A completed tag whose latest mover result is Failed
// fails the item with reason MoverFailed and the mover's logs as they are;
// no decision reads the logs.
//
// A completed sync gets its snapshot from the repository (see
// identifySnapshot). On a run that stopped its workloads the item then stays
// Running until the status holding the snapshot's full ID is written, and a
// later pass, once status.restartedAt is set, moves that snapshot to it and
// tags it quiesced (see retimeRecorded). A failed read of the source leaves
// the item as it was, for the next pass.
func (r *BackupRunReconciler) collectVolume(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	source := &volsyncv1alpha1.ReplicationSource{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
		return
	}
	switch {
	case lastManual(source) != item.Trigger:
		r.failedSync(ctx, run, item, source)
	case source.Status.LatestMoverStatus != nil && source.Status.LatestMoverStatus.Result == volsyncv1alpha1.MoverResultFailed:
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonMoverFailed, "Mover logs: %s", source.Status.LatestMoverStatus.Logs))
	case item.SnapshotID != "":
		r.retimeRecorded(ctx, run, item)
	default:
		r.identifySnapshot(ctx, run, item, source)
	}
}

// failedSync fails a volume item whose sync is still open when a mover Job
// of that sync failed, and leaves the item alone otherwise.
//
// Parameters:
//   - run is the BackupRun, for its start.
//   - item is the Running volume item. It is changed in place.
//   - source is the item's ReplicationSource as this pass read it.
//
// A Failed result that moverFailed places in this run's sync fails the item
// with reason MoverFailed and the message "Mover logs: " followed by the
// logs as VolSync kept them. When VolSync goes on with the sync, the message
// first says what data a snapshot of that sync saves later holds (see
// syncGoesOn).
//
// The source is left alone. VolSync writes the failure after it has already
// started a new mover Job, and deleting the source would kill that mover
// mid-backup and leave restic's lock in the repository. VolSync keeps
// retrying, and a later run fails its item for the claim at once while this
// run's tag is still open (see holder).
func (r *BackupRunReconciler) failedSync(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem, source *volsyncv1alpha1.ReplicationSource) {
	logs, failed := moverFailed(source, item.Trigger, run.Status.StartedAt)
	if !failed {
		return
	}
	message := "Mover logs: " + logs
	if note := r.syncGoesOn(ctx, run, *item, source); note != "" {
		message = note + " " + message
	}
	failBackupItem(item, refuse(backupv1alpha1.ItemReasonMoverFailed, "%s", message))
}

// identifySnapshot records the snapshot a completed sync wrote, found in the
// repository, and succeeds the item when the run need not move it.
//
// Parameters:
//   - run is the BackupRun, for its namespace and whether it stopped its
//     workloads.
//   - item is the Running volume item whose sync completed. It is changed
//     in place.
//   - source is the item's ReplicationSource, whose status records when the
//     sync ran.
//
// The window comes from the source's status (see windowOf); a status that
// lacks it fails the item with reason NoMoverSnapshot. The snapshot is the
// newest a mover wrote in that window (see identify), and the item records
// its full ID, its short ID and its time, and names the sync's other
// snapshots in its message. A listing with no such snapshot counts toward an
// empty claim, which takes two listings a poll interval apart (see
// noSnapshotListed). A failed listing leaves the item Running with the error
// in its message, and the next pass tries again until the run's timeout.
//
// The item of a run that stopped its workloads stays Running with the
// snapshot recorded, so the status holds the original's full ID before any
// rewrite. A crash after the rewrite then finds the rewritten copy through
// that ID, where a new search would find no snapshot and take the claim for
// empty.
func (r *BackupRunReconciler) identifySnapshot(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem, source *volsyncv1alpha1.ReplicationSource) {
	window, err := windowOf(source)
	if err != nil {
		failBackupItem(item, err)
		return
	}
	found, err := r.findSnapshot(ctx, run, item.Name, window)
	if err != nil {
		item.Message = fmt.Sprintf("the run could not list the repository for the snapshot its sync wrote: %v", err)
		return
	}
	if !found.found {
		noSnapshotListed(item, r.Now())
		return
	}
	s := found.snapshot
	item.NoSnapshotListedAt = nil
	item.SnapshotID, item.Snapshot, item.SnapshotTime = s.ID, s.ShortID(), newTime(metav1.NewTime(s.Time))
	item.Message = found.note()
	if !stoppedWorkloads(run) {
		item.Phase = backupv1alpha1.ItemSucceeded
	}
}

// findSnapshot lists the repository of a claim and picks the snapshot one
// sync of it wrote.
//
// Parameters:
//   - run is the BackupRun, for its namespace and its UID.
//   - claimName names the claim whose repository is listed.
//   - window is the sync's window (see windowOf).
//
// It returns what identify found. It returns an error when the reconciler
// has no Snapshots lister, and when the repository Secret, the repository or
// the namespace's BackupRuns can't be read.
func (r *BackupRunReconciler) findSnapshot(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string, window syncWindow) (identified, error) {
	if r.Snapshots == nil {
		return identified{}, errors.New("the controller has no repository lister")
	}
	secret, err := r.repositorySecret(ctx, run.Namespace, claimName)
	if err != nil {
		return identified{}, err
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return identified{}, err
	}
	recorded, err := r.recordedSnapshots(ctx, run, claimName)
	if err != nil {
		return identified{}, err
	}
	return identify(window, snapshots, recorded), nil
}

// recordedSnapshots returns the full snapshot IDs that other BackupRuns in
// the run's namespace recorded for items of the same claim, as a set.
//
// Parameters:
//   - run is the BackupRun that looks for its snapshot; its own items are
//     left out.
//   - claimName names the claim.
//
// It reads the BackupRuns through the uncached Reader, so a run that
// recorded its snapshot a moment ago is seen. It returns an error when the
// list fails.
func (r *BackupRunReconciler) recordedSnapshots(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string) (map[string]bool, error) {
	runs := &backupv1alpha1.BackupRunList{}
	if err := r.Reader.List(ctx, runs, client.InNamespace(run.Namespace)); err != nil {
		return nil, fmt.Errorf("list BackupRuns in %s: %w", run.Namespace, err)
	}
	recorded := map[string]bool{}
	for _, other := range runs.Items {
		if other.UID == run.UID {
			continue
		}
		for _, item := range other.Status.Items {
			if item.Kind == backupv1alpha1.ItemKindSource && item.Name == claimName && item.SnapshotID != "" {
				recorded[item.SnapshotID] = true
			}
		}
	}
	return recorded, nil
}

// retimeRecorded moves the recorded snapshot of a run that stopped its
// workloads to the run's restart moment, tags it quiesced, and succeeds the
// item.
//
// Parameters:
//   - run is the BackupRun, for its namespace, its record of the workloads
//     it stopped, and status.restartedAt.
//   - item is the Running volume item, whose snapshotID an earlier pass
//     recorded. It is changed in place.
//
// Every volume item of such a run cut its clone while the workloads were
// stopped, because the run starts them again only once each clone is cut or
// its item has failed (see clonesCut and giveUpUncut). The item therefore
// waits in Running while status.restartedAt is unset, as it is while another
// item's clone is not cut yet, and is moved once the restart moment is
// recorded. The item then records the rewritten snapshot's IDs and time and
// succeeds. When the rewrite fails, for example because another process
// holds a lock on the repository, the item stays Running with the error in
// its message and the next pass tries again, so the run never reports
// success for a snapshot a synced restore can't use. A run that stopped no
// workload succeeds the item with the snapshot as it was recorded.
func (r *BackupRunReconciler) retimeRecorded(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	if !stoppedWorkloads(run) {
		item.Phase = backupv1alpha1.ItemSucceeded
		return
	}
	if run.Status.RestartedAt == nil {
		item.Message = fmt.Sprintf("snapshot %s is saved and waits for the workloads to start again, to be moved to that moment and tagged %s",
			item.Snapshot, restic.QuiescedTag)
		return
	}
	moved, err := r.retime(ctx, run.Namespace, item.Name, item.SnapshotID, run.Status.RestartedAt.Time)
	if err != nil {
		item.Message = fmt.Sprintf("snapshot %s is saved and waits to be moved to %s and tagged %s: %v",
			item.Snapshot, run.Status.RestartedAt.UTC().Format(time.RFC3339), restic.QuiescedTag, err)
		return
	}
	item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
	item.SnapshotID, item.Snapshot, item.SnapshotTime = moved.ID, moved.ShortID(), newTime(metav1.NewTime(moved.Time))
}

// collectDatabase records the result of a Running database item once its
// CloudNativePG Backup has one (see cnpg.BackupResult).
//
// Parameters:
//   - run is the BackupRun, for its namespace.
//   - item is the Running database item, which names its Backup. It is
//     changed in place.
//
// It returns a sentence for the run's Ready message while the Backup waits
// in a phase that needs naming, such as one CloudNativePG 1.30 does not
// have, and "" otherwise.
//
// While the Backup runs, the item's message is that sentence, so it names
// the phase on the item itself; the SourceBusy message leaves it out (see
// work). A completed Backup makes the item Succeeded with no message, and a
// failed one makes it Failed with CloudNativePG's error. A failed read
// leaves the item as it was, for the next pass.
func (r *BackupRunReconciler) collectDatabase(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) string {
	outcome, err := cnpg.BackupResult(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Backup)
	if err != nil {
		return ""
	}
	switch outcome.Phase {
	case cnpg.BackupWaiting, cnpg.BackupUnknownPhase:
		item.Message = outcome.Message
		return outcome.Message
	case cnpg.BackupCompleted:
		item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
	case cnpg.BackupFailed:
		item.Phase, item.Message = backupv1alpha1.ItemFailed, outcome.Message
	}
	return ""
}

// stoppedWorkloads reports whether the run stopped at least one workload, as
// its status.quiesced records. Only such a run has a moment when nothing
// wrote to the volumes or the databases: its restart moment, which is what
// its snapshots are moved to.
func stoppedWorkloads(run *backupv1alpha1.BackupRun) bool {
	return run.Spec.All && len(run.Status.Quiesced) > 0
}

// retime moves a snapshot a mover saved to a new time and tags it quiesced,
// so a RestoreRun with syncDatabaseToVolume can find it.
//
// Parameters:
//   - namespace and claimName name the claim that was backed up. retime reads
//     the repository Secret through the claim's VolumeRestore.
//   - id is the snapshot's full ID, as the item recorded it when the run
//     found the snapshot in the repository.
//   - at is the time the snapshot should carry. The caller passes the run's
//     status.restartedAt.
//
// It returns the rewritten snapshot, which has a new ID. It returns an error
// when no ID is given, when the reconciler has no Retimer, when the Secret
// can't be read, and when the rewrite fails. A *restic.LockedError means
// another process holds a lock on the repository, and the caller tries again
// on its next pass. A snapshot an earlier call already rewrote comes back as
// that copy (see restic.Repository.Retime).
func (r *BackupRunReconciler) retime(ctx context.Context, namespace, claimName, id string, at time.Time) (restic.Snapshot, error) {
	if id == "" {
		return restic.Snapshot{}, errors.New("no snapshot is recorded to move")
	}
	if r.Retimer == nil {
		return restic.Snapshot{}, errors.New("the controller has no snapshot retimer")
	}
	secret, err := r.repositorySecret(ctx, namespace, claimName)
	if err != nil {
		return restic.Snapshot{}, err
	}
	return r.Retimer.Retime(ctx, secret, id, at, restic.QuiescedTag)
}

// repositorySecret reads the Secret holding the restic repository settings
// for the claim named claimName. It finds the Secret's name in
// spec.repository of the claim's VolumeRestore.
func (r *BackupRunReconciler) repositorySecret(ctx context.Context, namespace, claimName string) (*corev1.Secret, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: claimName}, claim); err != nil {
		return nil, err
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

// runningNote returns a sentence for the message of a Running item that
// abort or timeOut fails, and "" when there is nothing to add.
//
// Parameters:
//   - run is the BackupRun that ends.
//   - item is the Running item.
//
// For a volume item that recorded its snapshot and has not moved it yet,
// it returns the sentence of unmovedNote. For any other volume item it
// returns what syncGoesOn says of the sync VolSync goes on with. A database
// item can have a Backup in the phase cnpg.BackupUnknownPhase. For that item,
// it returns the message of cnpg.BackupResult. Thus a run that gets to its
// timeout shows the phase that it waited in. A failed read gives "", since the
// sentence only explains the item's failure.
func (r *BackupRunReconciler) runningNote(ctx context.Context, run *backupv1alpha1.BackupRun, item backupv1alpha1.BackupItem) string {
	if item.Kind != "Cluster" {
		if item.SnapshotID != "" {
			return unmovedNote(item)
		}
		return r.syncGoesOn(ctx, run, item, nil)
	}
	outcome, err := cnpg.BackupResult(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Backup)
	if err != nil || outcome.Phase != cnpg.BackupUnknownPhase {
		return ""
	}
	return outcome.Message
}

// abort ends a run early as Failed. It fails every Pending or Running item
// with the given message (see failUnfinished), then calls finish, which
// records the ending, starts the stopped workloads again and deletes the
// run's Workload so the queue gets its slot back.
//
// Parameters:
//   - reason is the Ready reason the run ends with: ReasonFailed for a run
//     that hit something it can't get past, ReasonInvalid for a run whose
//     targets it refuses, and ReasonVolSyncUnsupported for a VolSync this
//     controller cannot use.
//   - message is the Ready message, and the start of each unfinished item's
//     message.
//
// The items it fails record no reason; the run's ending says why. A run
// past its deadline ends through timeOut instead.
func (r *BackupRunReconciler) abort(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	r.failUnfinished(ctx, run, message, func(item *backupv1alpha1.BackupItem, text string) {
		item.Phase, item.Message = backupv1alpha1.ItemFailed, text
	})
	return r.finish(ctx, run, reason, message)
}

// timeOut ends a run whose deadline has passed, as Failed.
//
// Parameters:
//   - run is the BackupRun past its deadline, with no ending recorded yet.
//   - deadline is the run's deadline as overdue returns it.
//
// It returns what finish returns: nil once the run has ended, or the error
// of a restart or release that failed, for a retry.
//
// The message is timedOutMessage's, which names the SourceBusy wait the run
// was in. Every Pending or Running item fails with reason TimedOut and that
// message (see failUnfinished), and finish records the message in
// status.ending in the same status write. A later pass, such as one that
// retries a failed restart after releaseFailed replaced the SourceBusy
// condition, ends with the recorded ending and builds no message again.
func (r *BackupRunReconciler) timeOut(ctx context.Context, run *backupv1alpha1.BackupRun, deadline time.Time) error {
	message := timedOutMessage(deadline, run.Status.Conditions)
	r.failUnfinished(ctx, run, message, func(item *backupv1alpha1.BackupItem, text string) {
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonTimedOut, "%s", text))
	})
	return r.finish(ctx, run, backupv1alpha1.ReasonFailed, message)
}

// failUnfinished fails every Pending or Running item of a run that ends
// early, each with a message that starts with the run's.
//
// Parameters:
//   - run is the BackupRun that ends. Its items are changed in place, and
//     the caller writes the status.
//   - message is the run's Ready message.
//   - fail sets an item Failed with its message; the caller decides which
//     reason the item records.
//
// A Pending item whose last start attempt failed adds "; last error: " and
// status.items[].lastStartError to the message. A Running volume item adds
// the snapshot it recorded and did not move, or what data a snapshot of the
// sync VolSync goes on with holds, and a Running database item names a
// Backup phase CloudNativePG 1.30 does not have (see runningNote).
func (r *BackupRunReconciler) failUnfinished(ctx context.Context, run *backupv1alpha1.BackupRun, message string,
	fail func(item *backupv1alpha1.BackupItem, text string)) {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		text := message
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			text += startErrorNote(*item, "; last error: ")
		case backupv1alpha1.ItemRunning:
			if note := r.runningNote(ctx, run, *item); note != "" {
				text += ". " + note
			}
		default:
			// An item that has ended keeps how it ended.
			continue
		}
		fail(item, text)
	}
}

// startErrorNote returns what the message of a Pending item that fails adds
// for the item's last failed start.
//
// Parameters:
//   - item is the Pending item that fails.
//   - separator goes before the error, so the note reads on from the
//     caller's message.
//
// It returns the separator and status.items[].lastStartError, or "" when
// the item's last start attempt did not fail. The text goes into the item's
// message and nothing else; no decision reads it.
func startErrorNote(item backupv1alpha1.BackupItem, separator string) string {
	if item.LastStartError == "" {
		return ""
	}
	return separator + item.LastStartError
}

// finish ends the run. It records the ending, starts any workload the run
// still holds stopped, deletes the run's Workload, and records the terminal
// phase: Succeeded when the ending's reason is ReasonSucceeded and Failed
// for any other reason. It sets the Ready condition to the ending's reason
// and message, records status.completedAt, and removes the finalizer.
//
// Parameters:
//   - reason and message are the Ready reason and message the run ends
//     with. A run that already recorded status.ending ends with that one,
//     and these are not used.
//
// It returns nil once the run has ended, the error of a release that
// failed, or the error of a status write.
//
// finish records reason and message in status.ending before release, so
// every status write from then on carries them, and the items the caller
// failed go in the same write. When release fails, releaseFailed reports
// the failure on the run with reason RestartFailed or ReleaseFailed and
// writes the status, ending included, and the error it returns makes the
// reconcile run again. That pass finds status.ending and calls finish with
// it (see Reconcile), so the run ends as it decided to, whatever it waited
// for when it decided. The run stays unfinished until release succeeds.
func (r *BackupRunReconciler) finish(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	if run.Status.Ending == nil {
		run.Status.Ending = &backupv1alpha1.RunEnding{Reason: reason, Message: message}
	}
	ending := *run.Status.Ending
	if err := r.release(ctx, run); err != nil {
		return r.releaseFailed(ctx, run, err, false)
	}
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	if ending.Reason != backupv1alpha1.ReasonSucceeded {
		run.Status.Phase = backupv1alpha1.RunPhaseFailed
	}
	run.Status.CompletedAt = &now
	run.Status.Workload = ""
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, ending.Reason, ending.Message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	// The stored status now shows the workloads back, so the quiesce Leases
	// may go, and must go before the finalizer: a run whose finalizer is
	// dropped while it still holds them keeps other runs out of the
	// namespace until the Lease's holder reads as gone. Best effort, as in
	// work: a Lease left behind is stale under holderLive's rule.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the run goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return dropFinalizer(ctx, r.Client, run)
}

// release puts back what the run changed in the cluster. When the run stopped
// workloads and has not started them again, release scales them back up,
// resumes the Kustomizations it suspended, and records status.restartedAt. A
// run that recorded its plan to stop them counts as having stopped them, even
// before status.quiescedAt is set, because the pass that wrote the plan may
// have stopped them and then lost its status write. release then releases
// every Lease the run holds (see releaseLeases) and deletes the run's
// Workload.
//
// A run with status.restartPending set has chosen its restart moment and may
// not have started the workloads yet. release starts them and keeps that
// moment. A run whose status shows the restart done starts nothing again, so
// it never scales up a workload another run has stopped since. release
// decides this on the run as stored: it reads the run again through the
// uncached Reader first (see readStop), since the informer cache can lag
// behind the pass that did the restart.
//
// The Leases are released even when the restart fails: the run's items are
// done, and otherMover still keeps another run's mover off a claim whose
// mover runs. The Workload is deleted only once the restart and the Leases
// went through, so the run keeps its place in the queue while it still owes
// the app its replicas.
//
// It returns nil when everything is put back. A failed restart comes back as
// a *quiesce.RestartError from quiesce.Restart, which names the workload or
// Kustomization; a failed Lease release or Workload delete as a
// *releaseError that names what it could not delete. When both the restart
// and the Lease release fail, it returns both, joined with errors.Join. A
// failed read of the stored run comes back as it is, with nothing started.
// Every step is safe to repeat.
func (r *BackupRunReconciler) release(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if _, err := readStop(ctx, r.Reader, run); err != nil {
		return err
	}
	var restartErr error
	holding := (run.Status.QuiescedAt != nil || len(run.Status.Quiesced) > 0) && run.Status.RestartedAt == nil
	if holding || run.Status.RestartPending {
		restartErr = quiesce.Restart(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
		if restartErr == nil {
			if run.Status.RestartedAt == nil {
				run.Status.RestartedAt = newTime(metav1.NewTime(r.Now()).Rfc3339Copy())
			}
			run.Status.RestartPending = false
		}
	}
	if err := releaseLeases(ctx, r.Client, r.Reader, run, func(string) bool { return true }); err != nil {
		return errors.Join(restartErr, leaseReleaseError(run, err))
	}
	if restartErr != nil {
		return restartErr
	}
	if err := kueue.DeleteWorkload(ctx, r.Client, run.Namespace, run.UID); err != nil {
		return &releaseError{
			action: "delete its Kueue Workload " + kueue.WorkloadName(run.UID) + ", which holds the run's place in the queue",
			advice: "Fix the cause, or delete the Workload yourself; either way the run then finishes by itself.",
			err:    err,
		}
	}
	return nil
}

// backupItemDone reports whether the run's volume item for the claim name
// has finished: it is neither Pending nor Running, or the run has no such
// item.
func backupItemDone(run *backupv1alpha1.BackupRun, name string) bool {
	for _, item := range run.Status.Items {
		if item.Kind == "ReplicationSource" && item.Name == name {
			return item.Phase != backupv1alpha1.ItemPending && item.Phase != backupv1alpha1.ItemRunning
		}
	}
	return true
}

// finalize puts back what a run changed when the run is deleted before it
// finished, then removes the finalizer so the deletion can complete. When
// release fails, it reports the failure through releaseFailed and keeps the
// finalizer, so the deletion waits until the run has put everything back.
func (r *BackupRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.BackupRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if err := r.release(ctx, run); err != nil {
		return r.releaseFailed(ctx, run, err, false)
	}
	if err := dropFinalizer(ctx, r.Client, run); err != nil {
		return err
	}
	// After dropFinalizer, and not before: finalize writes no status, so a
	// release that happened while the drop still failed would let another run
	// take the Lease and then watch this run repeat its restart on the retry.
	// Best effort: a Lease left behind is stale under holderLive's rule.
	if err := releaseQuiesceLeases(ctx, r.Client, r.Reader, run); err != nil {
		log.FromContext(ctx).Error(err, "could not release the run's quiesce Leases; the deletion goes on",
			"namespace", run.Namespace, "name", run.Name)
	}
	return nil
}

// releaseFailed reports on the run that it could not put back what it
// changed, and returns err so the reconcile runs again with
// controller-runtime's backoff.
//
// Parameters:
//   - err is the error from release, or the *quiesce.RestartError of the restart
//     after the clones are cut.
//   - working is true when work calls it for that restart, while the run is
//     still backing up, and false when finish or finalize call it because
//     release failed.
//
// It sets the Ready condition to the reason and message from releaseFailure,
// which names what the run could not do and gives advice that fits, and
// writes the status. Announce turns either reason into a Warning event. The
// status write is best effort: a write that fails is made again by the next
// pass that fails.
//
// The run never gives up. A run that finished while it still owed a restart
// would lose the only record of the replicas the app had, and the next
// namespace run would record the stopped workload's 0 as the count to give
// back.
func (r *BackupRunReconciler) releaseFailed(ctx context.Context, run *backupv1alpha1.BackupRun, err error, working bool) error {
	reason, message := releaseFailure(err, releasePlan{
		stopped: run.Status.Quiesced, suspended: run.Status.SuspendedKustomizations,
		kind: "BackupRun", working: working, deleting: !run.DeletionTimestamp.IsZero(), scheduled: run.Spec.All,
	})
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	_ = r.writeStatus(ctx, run)
	return err
}

// overdue returns the run's deadline and reports whether the run has worked
// past it. The deadline is status.startedAt plus the timeout, and a run that
// has not started is never overdue.
//
// timeoutFor resolves the timeout again on every check, so a change to the
// namespace's backup.wlz.li/timeout during a run moves the run's deadline.
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

// waitFor moves the run to Waiting, sets its Ready condition to False with
// reason and message, and writes the status.
func (r *BackupRunReconciler) waitFor(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

// writeStatus writes the run's status subresource.
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
// each item what it captured: the snapshot a volume saved, the Backup a
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
		case item.Backup != "":
			message += fmt.Sprintf("%s %s completed Backup %s", item.Kind, item.Name, item.Backup)
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
