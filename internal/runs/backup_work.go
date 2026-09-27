package runs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

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
