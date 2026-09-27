package runs

import (
	"errors"
	"fmt"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
)

// releaseError is a failure of a step that releases or deletes something a
// run holds: its Leases, its Kueue Workload, or a restore Job it stops (the
// suspend, the delete, and the wait until no pod of the Job can write). The
// step can come before the workloads are back, as a RestoreRun stops its
// movers first, or after them; releasePlan.appDown tells releaseFailure
// which. The error says what the run could not do and what a person can do
// about it, so the run can put both on its Ready condition.
type releaseError struct {
	// action is what the run could not do, such as "release the Leases it
	// holds on its claims and repositories".
	action string

	// advice is one or more sentences that say what a person can do.
	advice string

	// err is the error from the API server or the RESTMapper.
	err error
}

// Error returns "could not ", the action, and the error.
func (e *releaseError) Error() string { return "could not " + e.action + ": " + e.err.Error() }

// Unwrap returns the error from the API server or the RESTMapper.
func (e *releaseError) Unwrap() error { return e.err }

// releasePlan holds what releaseFailure needs from a BackupRun or a
// RestoreRun to pick the Ready reason and write the advice for a failed
// restart or release. The run's releaseFailed fills it from the run's
// status and deletion timestamp.
type releasePlan struct {
	// stopped is the run's status.quiesced, the workloads it may still hold
	// stopped.
	stopped []backupv1alpha1.QuiescedWorkload

	// suspended is the run's status.suspendedKustomizations.
	suspended []string

	// kind is "BackupRun" or "RestoreRun", which the advice names.
	kind string

	// working is true when the run is still working, and false when it is
	// ending or being deleted.
	working bool

	// deleting is true when the run is being deleted.
	deleting bool

	// appDown is true when the run still holds workloads stopped or
	// Kustomizations suspended. A step that fails before the restart, such as
	// a RestoreRun stopping its restore Jobs, then reports RestartFailed with
	// the steps that give the app back by hand, because the app is still down.
	appDown bool

	// scheduled is true when a namespace's schedule starts no new run until
	// this one has finished, which is a BackupRun with spec.all set.
	scheduled bool
}

// releaseFailure returns the Ready reason and message for a run that could
// not put back what it changed or release what it holds.
//
// Parameters:
//   - err is the *quiesce.RestartError from quiesce.Restart, the *releaseError of a
//     step that releases or deletes something the run holds, both joined
//     with errors.Join, or another error from that work, such as a failed
//     read.
//   - plan is the part of the run's status the message is built from, and
//     says whether the app is still down (see releasePlan).
//
// It returns the reason and the message. The reason is RestartFailed while
// the app is still down, which is after a failed restart and after any
// failure while plan.appDown is set, and also when the run could not tell
// what failed. It is ReleaseFailed when only a release step failed and the
// app is back.
//
// The message names every step that failed, says that the run keeps trying,
// and gives advice that fits. A failed restart names each workload with the
// count it is owed and each Kustomization to resume, which a person can do
// while the run tries again, and quiesce.Restart skips what is already
// back. A release step carries its own advice. While the app is down after a
// release step failed, the message adds the same scaling steps, to be taken
// once no restore Job of the run still writes. A run that is still working
// says it must not be deleted; only a run that is ending or being deleted
// says that a person can delete it, or remove its finalizer, once the app
// runs again.
// When err holds both a restart and a release failure, as BackupRun.release
// returns them, the message names both and gives both pieces of advice.
func releaseFailure(err error, plan releasePlan) (string, string) {
	reason := backupv1alpha1.ReasonRestartFailed
	var failed, advice []string
	var restart *quiesce.RestartError
	var step *releaseError
	if errors.As(err, &restart) {
		failed = append(failed, restart.Error())
		switch {
		case plan.working:
			advice = append(advice, fmt.Sprintf("The run is still %s; do not delete it. To give the app back now, %s yourself; "+
				"the run then goes on by itself.", stillDoing(plan.kind), byHand(plan.stopped, plan.suspended)))
		case plan.deleting:
			advice = append(advice, fmt.Sprintf("Fix the cause, or %s yourself; the deletion then completes by itself. "+
				"If it still does not once the app runs again, remove the finalizer %s from this %s.",
				byHand(plan.stopped, plan.suspended), Finalizer, plan.kind))
		default:
			advice = append(advice, fmt.Sprintf("Fix the cause, or %s yourself; the run then finishes by itself. "+
				"If it still does not once the app runs again, delete this %s and remove its finalizer %s.",
				byHand(plan.stopped, plan.suspended), plan.kind, Finalizer))
		}
	}
	if errors.As(err, &step) {
		if restart == nil && !plan.appDown {
			reason = backupv1alpha1.ReasonReleaseFailed
		}
		failed = append(failed, step.Error())
		advice = append(advice, step.advice)
	}
	if len(failed) == 0 {
		failed = []string{"could not put back what the run changed: " + err.Error()}
		advice = []string{"Fixing the cause lets the run finish by itself."}
	}
	if restart == nil && plan.appDown {
		advice = append(advice, fmt.Sprintf("The app stays stopped until the run gets past this. To give it back sooner, "+
			"make sure no restore Job of the run still writes to its claims, then %s yourself.", byHand(plan.stopped, plan.suspended)))
	}
	message := strings.Join(failed, "; it also ") + ". The run retries until it can"
	if plan.scheduled {
		message += ", and this namespace's schedule waits for it"
	}
	message += ". " + strings.Join(advice, " ")
	return reason, message
}

// stillDoing returns what a run of the given kind is still doing while it
// owes the app its workloads, as the advice for a failed restart says it.
func stillDoing(kind string) string {
	if kind == "RestoreRun" {
		return "restoring"
	}
	return "backing up"
}

// byHand returns what a person does to give a run's app back by hand.
//
// Parameters:
//   - stopped is the run's status.quiesced. Each workload is named with the
//     replica count the run recorded for it.
//   - suspended is the run's status.suspendedKustomizations. Each
//     Kustomization is named by its namespace/name key.
//
// It returns a phrase that starts with a verb, such as "scale Deployment
// notes to 2 and resume Kustomization flux-system/notes", and "put the
// workloads back" when the run recorded neither.
func byHand(stopped []backupv1alpha1.QuiescedWorkload, suspended []string) string {
	var steps []string
	var counts []string
	for _, w := range stopped {
		counts = append(counts, fmt.Sprintf("%s %s to %d", w.Kind, w.Name, w.Replicas))
	}
	if len(counts) > 0 {
		steps = append(steps, "scale "+strings.Join(counts, ", "))
	}
	switch len(suspended) {
	case 0:
	case 1:
		steps = append(steps, "resume Kustomization "+suspended[0])
	default:
		steps = append(steps, "resume the Kustomizations "+strings.Join(suspended, ", "))
	}
	if len(steps) == 0 {
		return "put the workloads back"
	}
	return strings.Join(steps, " and ")
}
