package runs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
	// read the time restic stamped on the snapshot a mover saved.
	Snapshots restic.Lister

	// Retimer rewrites a snapshot with a new time and a tag. A quiesced run
	// uses it to move each volume's snapshot to the moment the run started the
	// workloads again, and to tag it quiesced.
	Retimer restic.Retimer

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
func (r *BackupRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.BackupRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
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
// run to Queued. When items returns a refusal, plan ends the run as Failed
// with reason Invalid and the refusal as the message. Any other error, such as
// a timeout from the API server, is returned so the reconcile runs again.
//
// Before anything else, plan checks the installed BackupRun CRD (see
// schemaOutdated) and ends a run it refuses with reason CRDOutdated.
func (r *BackupRunReconciler) plan(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	if refused, err := r.schemaOutdated(ctx, run); refused || err != nil {
		return ctrl.Result{}, err
	}
	items, err := r.items(ctx, run)
	if err != nil {
		if !isRefusal(err) {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
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
// so a run and a schedule cover the same set. items returns a refusal when a
// named claim or Cluster is missing or not marked, and when nothing in the
// namespace is marked. A cluster without the CloudNativePG CRDs holds no
// Cluster. Any other failed read comes back as a plain error.
func (r *BackupRunReconciler) items(ctx context.Context, run *backupv1alpha1.BackupRun) ([]backupv1alpha1.BackupItem, error) {
	pending := func(kind, name string) backupv1alpha1.BackupItem {
		return backupv1alpha1.BackupItem{Kind: kind, Name: name, Phase: backupv1alpha1.ItemPending}
	}

	switch {
	case run.Spec.Source != "":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Source}, claim); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, refuse("no claim %s in this namespace", run.Spec.Source)
			}
			return nil, fmt.Errorf("get claim %s: %w", run.Spec.Source, err)
		}
		if !backupv1alpha1.Enabled(claim.Annotations) {
			return nil, refuse("claim %s is not marked %s: \"true\"", claim.Name, backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("ReplicationSource", claim.Name)}, nil

	case run.Spec.Database != "":
		cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Spec.Database)
		if err != nil {
			if meta.IsNoMatchError(err) {
				return nil, refuse("no Cluster %s in this namespace; the cluster has no CloudNativePG CRDs", run.Spec.Database)
			}
			return nil, err
		}
		if !found {
			return nil, refuse("no Cluster %s in this namespace", run.Spec.Database)
		}
		if !backupv1alpha1.Enabled(cluster.GetAnnotations()) {
			return nil, refuse("the Cluster %s is not marked %s: \"true\"", cluster.GetName(), backupv1alpha1.AnnotationEnabled)
		}
		return []backupv1alpha1.BackupItem{pending("Cluster", cluster.GetName())}, nil

	default:
		claims, err := enabledClaims(ctx, r.Reader, run.Namespace)
		if err != nil {
			return nil, err
		}
		clusters, err := enabledClusters(ctx, r.Reader, r.RESTMapper(), run.Namespace)
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
			return nil, refuse("nothing in this namespace is marked %s: \"true\"", backupv1alpha1.AnnotationEnabled)
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
// waits, admit requeues after pollInterval.
func (r *BackupRunReconciler) admit(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	queue, err := localQueue(ctx, r.Reader, r.RESTMapper(), run.Namespace)
	if err != nil {
		return ctrl.Result{}, err
	}
	if queue != "" {
		workload, err := ensureWorkload(ctx, r.Client, run, backupRunKind, queue)
		if err != nil {
			return ctrl.Result{}, err
		}
		if run.Status.Workload != workload.GetName() {
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
	}

	// A backup.wlz.li/timeout on the namespace that does not parse fails the
	// run here, before it stops or starts anything, because the run would
	// have no deadline to keep.
	if _, err := timeoutFor(ctx, r.Reader, run); err != nil {
		var bad invalidSetting
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
			var bad invalidSetting
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
// work then starts every Pending item. An item that fails to start with an
// error other than a refusal stays Pending with "not started yet: " and the
// error in its message, the Ready condition takes reason Retrying and names
// each such item, and the pass goes on; the next pass tries the item again.
// Items that wait for another run are named as well: the Retrying message
// names every such wait, and without a retry the run waits with reason
// SourceBusy and a message that names every wait.
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
// and Failed otherwise. A run past its timeout is aborted.
func (r *BackupRunReconciler) work(ctx context.Context, run *backupv1alpha1.BackupRun) (ctrl.Result, error) {
	// The status keeps times in whole seconds. A snapshot moved in this pass
	// must carry the restartedAt that later passes read back.
	now := metav1.NewTime(r.Now()).Rfc3339Copy()
	deadline, over, err := r.overdue(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if over {
		return ctrl.Result{}, r.abort(ctx, run, backupv1alpha1.ReasonFailed, backupTimedOut(run, deadline))
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
			var bad invalidSetting
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
		targets, err := quiesceTargets(ctx, r.Reader, run.Namespace)
		if err != nil {
			return ctrl.Result{}, err
		}
		gone, pod, err := podsGone(ctx, r.Reader, run.Namespace, targets)
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
	var waits []string
	var retrying []string
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase != backupv1alpha1.ItemPending {
			continue
		}
		message, err := r.startItem(ctx, run, item)
		if err != nil {
			item.Message = notStartedYet + err.Error()
			retrying = append(retrying, fmt.Sprintf("%s %s: %v", item.Kind, item.Name, err))
			continue
		}
		if strings.HasPrefix(item.Message, notStartedYet) {
			item.Message = ""
		}
		if message != "" {
			waits = append(waits, message)
		}
	}

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
		if err := restartWorkloads(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
			// The run says why the app is still down at once, rather than
			// only at its timeout, and the error makes the pass run again.
			return ctrl.Result{}, r.releaseFailed(ctx, run, err, true)
		}
		run.Status.RestartPending = false
	}

	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase == backupv1alpha1.ItemRunning {
			r.collectItem(ctx, run, item)
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
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, strings.Join(waits, "; ")))
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
// It first records the plan from planStop in status.quiesced and
// status.suspendedKustomizations, with each workload's replica count, and
// writes the status. Only then does applyStop suspend the Kustomizations and
// scale the workloads to zero. A pass that finds a plan in the status reuses
// it, so a retry after a lost status write still gives back the counts the
// workloads had before the run touched them. Before it stops from such a
// plan, quiesce reads the run again through the uncached Reader (see
// stopOwed), and stops nothing when the stored run has recorded the stop,
// given the app back or ended. quiesce then writes status.quiescedAt. After
// a failed stop, it narrows the plan with appliedPart to what is stopped
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
			refused, err := r.startRefusal(ctx, run, item.Name)
			if err != nil {
				return ctrl.Result{}, err
			}
			if refused != "" {
				item.Phase, item.Message = backupv1alpha1.ItemFailed, refused
				continue
			}
			// A restore of the claim or its repository holds the item the
			// same way, and startItem would wait for it with the app down.
			restoring, err := r.heldElsewhere(ctx, run, item.Name)
			if isRefusal(err) {
				// A repository Secret that is gone fails the item now, so the
				// app is not stopped for a backup that cannot start.
				item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
				continue
			}
			if err != nil {
				return ctrl.Result{}, err
			}
			if restoring != "" {
				return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, restoring))
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
				item.Phase, item.Message = backupv1alpha1.ItemFailed, foreignSource(item.Name).Error()
				continue
			}
			if !inUse(source) || manualTag(source) == TriggerFor(run.UID) {
				continue
			}
			held := holder(ctx, r.Reader, source)
			switch {
			case errors.Is(held, errSourceAbandoned):
				item.Phase, item.Message = backupv1alpha1.ItemFailed, held.Error()
			case errors.Is(held, errSourceBusy):
				return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, held.Error()))
			default:
				return ctrl.Result{}, held
			}
		}

		targets, err := quiesceTargets(ctx, r.Reader, run.Namespace)
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
		if waiting != "" {
			return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, waiting))
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
		if busy != "" {
			return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, busy))
		}
		// A Kustomization that also applies workloads of another namespace
		// is refused before anything is stopped (see planStop).
		stop, suspend, err := planStop(ctx, r.Reader, r.RESTMapper(), run.Namespace, targets)
		if isRefusal(err) {
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

	stopErr := applyStop(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
	if stopErr != nil {
		run.Status.Quiesced, run.Status.SuspendedKustomizations = appliedPart(ctx, r.Reader, r.RESTMapper(), run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
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

// startRefusal returns the message startItem would fail a volume item with
// before it writes anything, or "" when startItem would go on. A namespace
// run calls it in its quiesce pre-check, so an item that cannot start fails
// before the app is stopped for it.
//
// Parameters:
//   - run is the asking run; its namespace is read.
//   - claimName names the item's claim.
//
// The message is the one startItem gives: the claim is gone (see
// claimGone), or ensureSource refuses the claim's settings (see
// sourceSettingsFor). A failed read comes back as an error, and the pass
// retries with nothing stopped.
func (r *BackupRunReconciler) startRefusal(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string) (string, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return claimGone(claimName), nil
		}
		return "", fmt.Errorf("get claim %s/%s: %w", run.Namespace, claimName, err)
	}
	if _, err := sourceSettingsFor(ctx, r.Reader, claim); err != nil {
		if isRefusal(err) {
			return err.Error(), nil
		}
		return "", err
	}
	return "", nil
}

// claimGone returns the message of a volume item whose claim no longer
// exists. claimName is the item's claim.
func claimGone(claimName string) string {
	return fmt.Sprintf("the claim %s no longer exists", claimName)
}

// heldElsewhere returns a message naming the run that holds the claim or its
// repository, or "" when neither is held. A backup calls it before it stops
// any workload, so that it waits with the app running where startItem would
// wait with the app down.
//
// Parameters:
//   - run is the asking run; its namespace and UID are read.
//   - claimName names the claim the run is about to back up.
//
// A claim that does not exist and a VolumeRestore the claim does not have
// give "": quiesce has failed such an item through startRefusal before it
// asks, and ensureSource checks again right before it writes the trigger.
// A repository Secret that does not exist comes back as the refusal
// leaseNamesFor gives (see isRefusal), and quiesce fails the item with it
// before anything is stopped. Any other failed read comes back as an
// error, and the pass retries with nothing stopped.
//
// The check is advisory. A run that starts its mover between this read and
// the stop still goes first under the Leases and otherMover, which run right
// before the mover object is written.
func (r *BackupRunReconciler) heldElsewhere(ctx context.Context, run *backupv1alpha1.BackupRun, claimName string) (string, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: claimName}, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return "", nil
		}
		return "", fmt.Errorf("get claim %s/%s: %w", run.Namespace, claimName, err)
	}
	vr, err := volumeRestoreFor(ctx, r.Reader, claim)
	if err != nil {
		if isRefusal(err) {
			return "", nil
		}
		return "", err
	}
	restoring, err := otherMover(ctx, r.Reader, run.Namespace, claimName, vr.Spec.Repository, restoreMover)
	if err != nil {
		return "", err
	}
	if restoring != "" {
		return restoring, nil
	}
	return leaseHeldElsewhere(ctx, r.Reader, run, run.Namespace, claimName, vr.Spec.Repository)
}

// backupTimedOut returns the Ready message of a run that work aborts because
// its deadline passed.
//
// Parameters:
//   - run is the BackupRun. Its items and its Ready condition are read.
//   - deadline is the run's deadline as overdue returns it.
//
// The first pass past the deadline builds the message with timedOutMessage,
// which adds the SourceBusy wait the run was in, and abort puts it on every
// unfinished item. When that pass cannot give the app back, releaseFailed
// replaces the SourceBusy condition with RestartFailed or ReleaseFailed, and
// a later pass would build the message without the wait. So when an item
// already failed with a message for this deadline, backupTimedOut returns
// that message without the parts abort added for the item alone: the last
// start error of a Pending item ("; last error: ") and the sentence
// syncGoesOn adds for a Running one. Of several such items it returns the
// shortest result, and with none it builds the message afresh.
func backupTimedOut(run *backupv1alpha1.BackupRun, deadline time.Time) string {
	base := timedOutMessage(deadline, nil)
	found := ""
	for _, item := range run.Status.Items {
		if item.Phase != backupv1alpha1.ItemFailed || !strings.HasPrefix(item.Message, base) {
			continue
		}
		message := item.Message
		for _, tail := range []string{"; last error: ", ". VolSync keeps retrying the sync ", ". VolSync goes on with the sync "} {
			if at := strings.Index(message[len(base):], tail); at >= 0 {
				message = message[:len(base)+at]
			}
		}
		if found == "" || len(message) < len(found) {
			found = message
		}
	}
	if found != "" {
		return found
	}
	return timedOutMessage(deadline, run.Status.Conditions)
}

// startItem starts the backup of one Pending item and sets the item's phase.
//
// For a volume, it writes the claim's ReplicationSource with the run's manual
// trigger tag and moves the item to Running. For a database, it creates a
// CloudNativePG Backup and moves the item to Running, or skips the item when
// the Cluster is hibernated. When the claim or the Cluster is gone, or the
// claim's settings are refused, startItem marks the item Failed with the
// reason in its message. The same goes for a Backup the API server rejects as
// invalid.
//
// It returns a message for the run's Ready condition when the item has to
// wait, which happens when the volume's ReplicationSource is still completing
// the backup of another run that waits for it, or when a RestoreRun's mover
// works on the claim or its repository (see otherMover). The message names
// that run, and the item stays Pending. When the source is busy with a
// backup no run waits for (see holder), the item fails at once with a
// message that says what a person can do, and the source is left alone.
// Otherwise it returns an empty string. Any other failed read or write, such
// as a timeout from the API server or a 500 from a webhook it cannot reach,
// comes back as an error with the item left Pending. The caller records the
// error in the item's message and tries the item again on its next pass.
func (r *BackupRunReconciler) startItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) (string, error) {
	switch item.Kind {
	case "ReplicationSource":
		claim := &corev1.PersistentVolumeClaim{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, claim); err != nil {
			if !apierrors.IsNotFound(err) {
				return "", fmt.Errorf("get claim %s: %w", item.Name, err)
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, claimGone(item.Name)
			return "", nil
		}
		tag := TriggerFor(run.UID)
		_, err := ensureSource(ctx, r.Client, r.Reader, claim, tag, leaseHolder{kind: "BackupRun", run: run, item: item.Name})
		if errors.Is(err, errSourceBusy) {
			return err.Error(), nil
		}
		if errors.Is(err, errSourceAbandoned) {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return "", nil
		}
		if err != nil {
			if !isRefusal(err) {
				return "", err
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
			return "", nil
		}
		item.Phase, item.Trigger = backupv1alpha1.ItemRunning, tag

	case "Cluster":
		cluster, found, err := getCluster(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Name)
		switch {
		case err != nil:
			return "", err
		case !found:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, fmt.Sprintf("the Cluster %s no longer exists", item.Name)
		case hibernated(cluster):
			item.Phase, item.Message = backupv1alpha1.ItemSkipped, "the Cluster is hibernated; CloudNativePG fails a Backup of a hibernated Cluster"
		default:
			name, err := ensureBackup(ctx, r.Client, run.Namespace, item.Name, run.UID)
			if err != nil {
				if !apierrors.IsInvalid(err) {
					return "", err
				}
				item.Phase, item.Message = backupv1alpha1.ItemFailed, err.Error()
				return "", nil
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
// error of its last start attempt when it has one. A Running item whose clone
// is not cut fails with a message that names the missing clone. A Running
// item whose clone is cut goes on. A Running item's message also says what
// data a snapshot of the sync VolSync goes on with holds (see syncGoesOn).
// Afterwards clonesCut is true, so the
// caller's restart path records status.restartedAt and starts the workloads.
// The check reads only the clones; a failed read counts as no clone, so an
// API outage never keeps the workloads down past the limit.
func (r *BackupRunReconciler) giveUpUncut(ctx context.Context, run *backupv1alpha1.BackupRun, limit time.Duration, now metav1.Time) {
	at := now.UTC().Format(time.RFC3339)
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Kind != "ReplicationSource" {
			continue
		}
		switch item.Phase {
		case backupv1alpha1.ItemPending:
			message := fmt.Sprintf("not started before the workloads were given back at %s, when the %s limit of %s ran out, so the clone %s was never cut",
				at, backupv1alpha1.AnnotationMaxQuiesce, limit, cloneName(item.Name))
			if last, ok := strings.CutPrefix(item.Message, notStartedYet); ok {
				message += ": " + last
			}
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
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
// A volume item is done when its ReplicationSource has completed the run's
// trigger tag. VolSync completes a tag only after a mover Job succeeds. A Job
// that fails leaves the tag open: VolSync writes the logs into
// status.latestMoverStatus, deletes the Job and starts another, for as long
// as the tag stays. So while the tag is open, a Failed result that
// moverFailed places in this run's sync fails the item with the mover's
// logs, and, when restic found the repository locked, with how that lock is
// cleared (see moverFailure). collectItem leaves the source alone: VolSync
// has already started the next mover Job by then, and deleting the source
// would kill that mover mid-backup and leave restic's lock in the
// repository. VolSync keeps retrying, and a later run fails its item for the
// claim at once while this run's tag is still open (see holder). The
// message also says what data a snapshot of that sync saves later holds
// (see syncGoesOn). A volume with no files succeeds with Empty set,
// since VolSync takes no snapshot of it. Otherwise the item records the snapshot ID
// the mover logged and the time restic stamped on it. On a quiesced run, the
// snapshot is first moved to status.restartedAt and tagged quiesced, and the
// item stays Running until that rewrite succeeds.
//
// A database item follows the phase of its CloudNativePG Backup.
func (r *BackupRunReconciler) collectItem(ctx context.Context, run *backupv1alpha1.BackupRun, item *backupv1alpha1.BackupItem) {
	switch item.Kind {
	case "ReplicationSource":
		source := &volsyncv1alpha1.ReplicationSource{}
		if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: item.Name}, source); err != nil {
			return
		}
		if lastManual(source) != item.Trigger {
			if logs, failed := moverFailed(source, item.Trigger, run.Status.StartedAt); failed {
				// The source is left alone. VolSync writes the failure after
				// it has already started a new mover Job, and deleting the
				// source would kill that mover mid-backup and leave restic's
				// lock in the repository.
				message := moverFailure(source, logs)
				if note := r.syncGoesOn(ctx, run, *item, source); note != "" {
					if !strings.Contains(logs, alreadyLocked) {
						message = "Mover logs: " + message
					}
					message = note + " " + message
				}
				item.Phase, item.Message = backupv1alpha1.ItemFailed, message
			}
			return
		}
		if source.Status.LatestMoverStatus != nil && source.Status.LatestMoverStatus.Result == volsyncv1alpha1.MoverResultFailed {
			item.Phase, item.Message = backupv1alpha1.ItemFailed, moverFailure(source, source.Status.LatestMoverStatus.Logs)
			return
		}
		snapshot, empty := moverOutcome(source)
		if empty {
			item.Phase, item.Empty = backupv1alpha1.ItemSucceeded, true
			item.Message = "the volume held no files, so VolSync took no snapshot"
			return
		}
		if quiesced(run) {
			// The item stays Running until the rewrite goes through. That way
			// the run never reports success for a snapshot that a synced
			// restore cannot use.
			moved, err := r.retime(ctx, run.Namespace, item.Name, snapshot, run.Status.RestartedAt.Time)
			if err != nil {
				item.Message = fmt.Sprintf("snapshot %s is saved and waits to be moved to %s and tagged %s: %v",
					snapshot, run.Status.RestartedAt.UTC().Format(time.RFC3339), restic.QuiescedTag, err)
				return
			}
			item.Phase, item.Message = backupv1alpha1.ItemSucceeded, ""
			item.Snapshot, item.SnapshotTime = moved.ShortID(), newTime(metav1.NewTime(moved.Time))
			return
		}
		item.Phase, item.Snapshot = backupv1alpha1.ItemSucceeded, snapshot
		if at, err := r.snapshotTime(ctx, run.Namespace, item.Name, snapshot); err != nil {
			item.Message = fmt.Sprintf("the snapshot's time could not be read: %v", err)
		} else {
			item.SnapshotTime = at
		}

	case "Cluster":
		done, ok, message, err := backupResult(ctx, r.Reader, r.RESTMapper(), run.Namespace, item.Backup)
		switch {
		case err != nil || !done:
		case ok:
			item.Phase = backupv1alpha1.ItemSucceeded
		default:
			item.Phase, item.Message = backupv1alpha1.ItemFailed, message
		}
	}
}

// quiesced reports whether the run stopped at least one workload and has
// started the workloads again. Only such a run has a moment when nothing wrote
// to the volumes or the databases, which is what its snapshots are moved to.
func quiesced(run *backupv1alpha1.BackupRun) bool {
	return run.Spec.All && len(run.Status.Quiesced) > 0 && run.Status.RestartedAt != nil
}

// retime moves the snapshot a mover saved to a new time and tags it quiesced,
// so a RestoreRun with syncDatabaseToVolume can find it.
//
// Parameters:
//   - namespace and claimName name the claim that was backed up. retime reads
//     the repository Secret through the claim's VolumeRestore.
//   - short is the snapshot ID the mover logged, eight hex characters or
//     more. It is empty when the mover logged no snapshot.
//   - at is the time the snapshot should carry. collectItem passes the run's
//     status.restartedAt.
//
// It returns the rewritten snapshot, which has a new ID. It returns an error
// when the mover logged no snapshot, when the reconciler has no Retimer, when
// the Secret can't be read, and when the rewrite fails. A *restic.LockedError
// means another process holds a lock on the repository, and the caller tries
// again on its next pass.
func (r *BackupRunReconciler) retime(ctx context.Context, namespace, claimName, short string, at time.Time) (restic.Snapshot, error) {
	if short == "" || r.Retimer == nil {
		return restic.Snapshot{}, fmt.Errorf("the mover logged no snapshot")
	}
	secret, err := r.repositorySecret(ctx, namespace, claimName)
	if err != nil {
		return restic.Snapshot{}, err
	}
	return r.Retimer.Retime(ctx, secret, short, at, restic.QuiescedTag)
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

// snapshotTime reads the time restic stamped on a snapshot a mover saved. It
// lists the repository of the claim named claimName and looks for the
// snapshot whose ID starts with the prefix in short. It returns an error when
// that prefix is empty, when the reconciler has no Snapshots lister, and when
// the repository holds no such snapshot.
func (r *BackupRunReconciler) snapshotTime(ctx context.Context, namespace, claimName, short string) (*metav1.Time, error) {
	if short == "" || r.Snapshots == nil {
		return nil, fmt.Errorf("the mover logged no snapshot")
	}
	secret, err := r.repositorySecret(ctx, namespace, claimName)
	if err != nil {
		return nil, err
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return nil, err
	}
	found, ok := restic.ByShortID(snapshots, short)
	if !ok {
		return nil, fmt.Errorf("the repository holds no snapshot %s", short)
	}
	return newTime(metav1.NewTime(found.Time)), nil
}

// abort ends a run early as Failed. It marks every Pending or Running item as
// Failed with the given message, then calls finish, which starts the stopped
// workloads again and deletes the run's Workload so the queue gets its slot
// back.
//
// Parameters:
//   - reason is the Ready reason the run ends with: ReasonFailed for a run
//     that hit something it can't get past, such as its timeout, and
//     ReasonInvalid for a run whose targets it refuses.
//   - message is the Ready message, and each unfinished item's message.
//
// A Pending item whose last start attempt failed keeps that error: its
// message becomes the given message, "; last error: " and the error. A
// Running volume item's message also says what data a snapshot of the sync
// VolSync goes on with holds (see syncGoesOn).
func (r *BackupRunReconciler) abort(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase != backupv1alpha1.ItemPending && item.Phase != backupv1alpha1.ItemRunning {
			continue
		}
		failed := message
		if last, ok := strings.CutPrefix(item.Message, notStartedYet); ok && item.Phase == backupv1alpha1.ItemPending {
			failed += "; last error: " + last
		}
		if item.Phase == backupv1alpha1.ItemRunning {
			if note := r.syncGoesOn(ctx, run, *item, nil); note != "" {
				failed += ". " + note
			}
		}
		item.Phase, item.Message = backupv1alpha1.ItemFailed, failed
	}
	return r.finish(ctx, run, reason, message)
}

// finish ends the run. It starts any workload the run still holds stopped,
// deletes the run's Workload, and records the terminal phase: Succeeded when
// reason is ReasonSucceeded and Failed for any other reason. It sets the
// Ready condition to reason and message, records status.completedAt, and
// removes the finalizer.
//
// When release fails, finish records nothing of the ending: releaseFailed
// reports the failure on the run with reason RestartFailed or ReleaseFailed,
// and the error it returns makes the reconcile run again. The run stays
// unfinished until release succeeds.
func (r *BackupRunReconciler) finish(ctx context.Context, run *backupv1alpha1.BackupRun, reason, message string) error {
	if err := r.release(ctx, run); err != nil {
		return r.releaseFailed(ctx, run, err, false)
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
// a *restartError from restartWorkloads, which names the workload or
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
		restartErr = restartWorkloads(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations)
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
	if err := deleteWorkload(ctx, r.Client, run.Namespace, run.UID); err != nil {
		return &releaseError{
			action: "delete its Kueue Workload " + workloadName(run.UID) + ", which holds the run's place in the queue",
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
//   - err is the error from release, or the *restartError of the restart
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

// notStartedYet prefixes the message of a Pending item whose start failed
// with an error the run tries again. A later pass that starts the item, or
// leaves it waiting for another run, removes the message.
const notStartedYet = "not started yet: "

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
