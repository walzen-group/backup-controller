package runs

import (
	"context"
	"fmt"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
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
	if done, err := r.endIfOverdue(ctx, run); done {
		return ctrl.Result{}, err
	}
	if done, err := r.endIfEvicted(ctx, run); done {
		return ctrl.Result{}, err
	}
	r.releaseFinished(ctx, run)
	if done, result, err := r.quiesceFirst(ctx, run, now); done {
		return result, err
	}
	limited, done, err := r.enforceQuiesceLimit(ctx, run, now)
	if done {
		return ctrl.Result{}, err
	}
	if done, result, err := r.waitForStoppedPods(ctx, run, limited); done {
		return result, err
	}
	// An item that fails to start with an error the API server may stop
	// giving keeps its place and is tried again on the next pass. The pass
	// goes on, because the restart below looks only at volume items, and a
	// database whose Backup cannot be created must not keep the app down.
	waits, retrying := r.startPending(ctx, run)
	if err := r.restartAfterCut(ctx, run, now); err != nil {
		return ctrl.Result{}, err
	}
	notes := r.collectRunning(ctx, run)
	return r.finishOrWait(ctx, run, waits, retrying, notes)
}

// endIfEvicted ends the run when Kueue evicted its Workload after it
// admitted the run.
//
// Parameters:
//   - run is the admitted BackupRun. A run with no status.workload went
//     through no LocalQueue, and endIfEvicted does nothing for it.
//
// It returns done true when the pass must stop here: Kueue evicted the
// Workload and the run ended. The error is the error of finish. A failed
// read of the Workload does not stop the pass. The pass logs it and goes
// on, so that the max-quiesce limit and the timeout still give the app back
// when the controller cannot read Workloads.
//
// Kueue marks an evicted Workload with Evicted True, and expects the owner
// to stop the work (see kueue.Evicted). Kueue would start the work again
// from the start, which on a run with spec.all set would stop the app a
// second time. So the run fails every unfinished item with reason Evicted,
// and finish gives the app back and deletes the Workload. A deleted
// Workload frees its quota in Kueue. The run ends Failed with reason
// Evicted, and the next scheduled run tries again. A Workload that is gone
// holds no quota, and the run goes on.
func (r *BackupRunReconciler) endIfEvicted(ctx context.Context, run *backupv1alpha1.BackupRun) (done bool, err error) {
	if run.Status.Workload == "" {
		return false, nil
	}
	workload, err := kueue.ReadWorkload(ctx, r.Client, run.Namespace, run.UID)
	if err != nil {
		log.FromContext(ctx).Error(err, "cannot read the run's Workload to check for an eviction; the pass goes on")
		return false, nil
	}
	if workload == nil || !kueue.Evicted(workload) {
		return false, nil
	}
	message := fmt.Sprintf("Kueue evicted the run's Workload %s/%s after it admitted the run; "+
		"the run stopped and gave the app back, and the next scheduled run tries again", run.Namespace, workload.GetName())
	r.failUnfinished(ctx, run, message, func(item *backupv1alpha1.BackupItem, text string) {
		failBackupItem(item, refuse(backupv1alpha1.ItemReasonEvicted, "%s", text))
	})
	return true, r.finish(ctx, run, backupv1alpha1.ReasonEvicted, message)
}

// endIfOverdue ends the run when its deadline has passed.
//
// Parameters:
//   - run is the admitted BackupRun.
//
// It returns done true when the pass must stop here: the run is past its
// deadline and timeOut ended it, or the read of the timeout failed. The
// error is the error of that read or of timeOut.
func (r *BackupRunReconciler) endIfOverdue(ctx context.Context, run *backupv1alpha1.BackupRun) (done bool, err error) {
	deadline, over, err := r.overdue(ctx, run)
	if err != nil {
		return true, err
	}
	if !over {
		return false, nil
	}
	return true, r.timeOut(ctx, run, deadline)
}

// releaseFinished releases the Leases that the run no longer needs. It is
// best effort: it logs a failure and the pass goes on.
//
// Parameters:
//   - run is the admitted BackupRun.
//
// The Leases of an item that finished in an earlier pass go first. The
// quiesce Leases go once the stored status shows every workload back.
func (r *BackupRunReconciler) releaseFinished(ctx context.Context, run *backupv1alpha1.BackupRun) {
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
		releaseQuiesceLeases(ctx, r.Client, r.Reader, run)
	}
}

// quiesceFirst stops the workloads of a namespace run that has not stopped
// them yet (see quiesce). That pass does nothing else.
//
// Parameters:
//   - run is the admitted BackupRun.
//   - now is the time of the pass.
//
// It returns done true, with the result and the error of quiesce, when it
// called quiesce. It returns done false when the run has no spec.all or
// has recorded status.quiescedAt.
func (r *BackupRunReconciler) quiesceFirst(ctx context.Context, run *backupv1alpha1.BackupRun, now metav1.Time) (done bool, result ctrl.Result, err error) {
	if !run.Spec.All || run.Status.QuiescedAt != nil {
		return false, ctrl.Result{}, nil
	}
	result, err = r.quiesce(ctx, run, now)
	return true, result, err
}

// enforceQuiesceLimit fails the volume items whose clone is not cut when the
// workloads have been stopped for the namespace's backup.wlz.li/max-quiesce
// limit (see giveUpUncut).
//
// Parameters:
//   - run is the admitted BackupRun, with status.quiescedAt set when it has
//     spec.all.
//   - now is the time of the pass.
//
// It returns limited true when the limit ran out in this pass, so the pass
// does not wait for pods. It returns done true when the pass must stop
// here: the limit does not parse and the run ended, or the read failed.
// The error is the error of that read or of abort.
func (r *BackupRunReconciler) enforceQuiesceLimit(ctx context.Context, run *backupv1alpha1.BackupRun, now metav1.Time) (limited, done bool, err error) {
	if !run.Spec.All || run.Status.RestartedAt != nil || len(run.Status.Quiesced) == 0 {
		return false, false, nil
	}
	limit, err := maxQuiesceFor(ctx, r.Reader, run.Namespace)
	if err != nil {
		// A limit that no longer parses fails the run, which starts the
		// workloads again, so they are never held without a limit.
		return false, true, r.abortOnInvalidSetting(ctx, run, err)
	}
	if now.Time.Before(run.Status.QuiescedAt.Add(limit)) {
		return false, false, nil
	}
	r.giveUpUncut(ctx, run, limit, now)
	return true, false, nil
}

// waitForStoppedPods makes a namespace run wait until every pod of the
// workloads it stopped is gone, before it starts an item.
//
// Parameters:
//   - run is the admitted BackupRun.
//   - limited is true when the quiesce limit ran out in this pass. Then
//     the pass does not wait.
//
// It returns done true, with the result and the error the pass returns,
// when a pod is still there or a read failed. The run then waits with
// reason Running and looks again after two seconds.
func (r *BackupRunReconciler) waitForStoppedPods(ctx context.Context, run *backupv1alpha1.BackupRun, limited bool) (done bool, result ctrl.Result, err error) {
	if !run.Spec.All || run.Status.RestartedAt != nil || limited || !anyPending(run.Status.Items) {
		return false, ctrl.Result{}, nil
	}
	targets, err := quiesce.Targets(ctx, r.Reader, run.Namespace)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	gone, pod, err := quiesce.PodsGone(ctx, r.Reader, run.Namespace, targets)
	if err != nil {
		return true, ctrl.Result{}, err
	}
	if gone {
		return false, ctrl.Result{}, nil
	}
	result, err = after(2*time.Second, r.waitFor(ctx, run, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("waiting for pod %s to stop before the clones are cut", pod)))
	return true, result, err
}

// restartAfterCut starts the workloads of a namespace run again once every
// volume's clone is cut, and repeats a restart that an earlier pass did not
// finish.
//
// Parameters:
//   - run is the admitted BackupRun. Its status is changed and written.
//   - now is the time of the pass, which becomes status.restartedAt.
//
// It returns the error of a status write or a read, or the error of a
// failed restart after releaseFailed has reported it on the run.
func (r *BackupRunReconciler) restartAfterCut(ctx context.Context, run *backupv1alpha1.BackupRun, now metav1.Time) error {
	restart := false
	if run.Spec.All && run.Status.RestartedAt == nil && r.clonesCut(ctx, run) {
		// The moment is written before the workloads start, so a pass that
		// starts them and then loses its status write is retried with it.
		run.Status.RestartedAt, run.Status.RestartPending = newTime(now), true
		if err := r.writeStatus(ctx, run); err != nil {
			return err
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
			return err
		}
	}
	if !restart && !run.Status.RestartPending {
		return nil
	}
	if err := quiesce.Restart(ctx, r.Client, run.Namespace, run.Status.Quiesced, run.Status.SuspendedKustomizations); err != nil {
		// The run says why the app is still down at once, rather than
		// only at its timeout, and the error makes the pass run again.
		return r.releaseFailed(ctx, run, err, true)
	}
	run.Status.RestartPending = false
	return nil
}

// collectRunning collects the result of every Running item. A volume item
// follows its ReplicationSource (see collectVolume), and a database item
// follows the phase of its CloudNativePG Backup (see collectDatabase).
//
// Parameters:
//   - run is the admitted BackupRun. Its items are changed in place.
//
// It returns the notes of the items, one sentence for each item that needs
// one in the Ready message.
func (r *BackupRunReconciler) collectRunning(ctx context.Context, run *backupv1alpha1.BackupRun) []string {
	var notes []string
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		if item.Phase != backupv1alpha1.ItemRunning {
			continue
		}
		switch item.Kind {
		case backupv1alpha1.ItemKindSource:
			r.collectVolume(ctx, run, item)
		case backupv1alpha1.ItemKindCluster:
			if note := r.collectDatabase(ctx, run, item); note != "" {
				notes = append(notes, note)
			}
		}
	}
	return notes
}

// finishOrWait ends a run whose items are all done, and writes the Ready
// condition of a run that goes on.
//
// Parameters:
//   - run is the admitted BackupRun. Its status is written.
//   - waits are the sentences of the items that wait for another run.
//   - retrying are the lines of the items whose start failed.
//   - notes are the notes of the Running items.
//
// It returns when to look again, and the error of finish or of the status
// write.
//
// The run finishes once every item is done and, on a run with spec.all
// set, the workloads are running again: Failed when an item failed, and
// Succeeded otherwise. Otherwise the reason is Retrying when a start
// failed, SourceBusy when an item only waits, and Running else.
func (r *BackupRunReconciler) finishOrWait(ctx context.Context, run *backupv1alpha1.BackupRun, waits, retrying, notes []string) (ctrl.Result, error) {
	if allDone(run.Status.Items) && (!run.Spec.All || run.Status.RestartedAt != nil) {
		if anyFailed(run.Status.Items) {
			return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonFailed, failures(run.Status.Items))
		}
		return ctrl.Result{}, r.finish(ctx, run, backupv1alpha1.ReasonSucceeded, summary(run.Status.Items))
	}
	if len(retrying) == 0 && len(waits) > 0 {
		// Every wait is named, so one item's wait does not hide another's.
		// A note stays on its own item's message, so a timeout that copies
		// this wait does not repeat it.
		return after(pollInterval, r.waitFor(ctx, run, backupv1alpha1.ReasonSourceBusy, strings.Join(waits, "; ")))
	}
	reason, message := backupv1alpha1.ReasonRunning, "backing up"
	if len(retrying) > 0 {
		// An item that waits for another run is named as well, so the retry
		// does not hide it.
		reason, message = backupv1alpha1.ReasonRetrying, "retrying the start of "+strings.Join(retrying, "; ")
		if len(waits) > 0 {
			message += "; " + strings.Join(waits, "; ")
		}
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
// run (see startVolume and startDatabase), and the retries, one line for each
// item whose start failed with an error a later pass may not get, naming the
// item and the error.
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
		var wait hold
		var err error
		switch item.Kind {
		case backupv1alpha1.ItemKindSource:
			wait, err = r.startVolume(ctx, run, item)
		case backupv1alpha1.ItemKindCluster:
			wait, err = r.startDatabase(ctx, run, item)
		}
		if err != nil {
			item.LastStartError = err.Error()
			item.Message = "not started yet: " + item.LastStartError
			retrying = append(retrying, fmt.Sprintf("%s %s: %v", item.Kind, item.Name, err))
			continue
		}
		if wait.held() {
			waits = append(waits, wait.text)
		}
	}
	return waits, retrying
}
