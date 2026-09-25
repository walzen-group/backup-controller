package runs

import (
	"unicode/utf8"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// maxNote is the longest note, in bytes, that events.k8s.io/v1 accepts. The
// API server rejects an event with a longer note, and a failed mover's logs
// run past it.
const maxNote = 1024

// readyReason returns the reason on a run's Ready condition, or an empty
// string when the run has no Ready condition yet.
func readyReason(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == backupv1alpha1.ConditionReady {
			return c.Reason
		}
	}
	return ""
}

// announce records an event on a run when its Ready condition moved to a new
// reason during a reconcile. The event's reason is the condition's reason,
// and its note is the condition's message, cut to fit by fitNote.
//
// Parameters:
//   - recorder records the event. When it is nil, announce does nothing.
//   - run is the BackupRun or RestoreRun the event is recorded on.
//   - conditions are the run's status.conditions after the reconcile.
//   - before is the reason the Ready condition had when the reconcile began,
//     from readyReason. When the reason hasn't changed, no event is recorded.
//   - action is the event's action, "Backup" or "Restore".
//
// The event is a Warning when the Ready condition is True with a reason other
// than Succeeded, which is how a run that finished any other way reports. In
// every other case it is Normal.
func announce(recorder events.EventRecorder, run client.Object, conditions []metav1.Condition, before, action string) {
	if recorder == nil {
		return
	}
	for _, ready := range conditions {
		if ready.Type != backupv1alpha1.ConditionReady || ready.Reason == before {
			continue
		}
		eventtype := corev1.EventTypeNormal
		if ready.Status == metav1.ConditionTrue && ready.Reason != backupv1alpha1.ReasonSucceeded {
			eventtype = corev1.EventTypeWarning
		}
		recorder.Eventf(run, nil, eventtype, ready.Reason, action, "%s", fitNote(ready.Message))
	}
}

// fitNote cuts a note to at most maxNote bytes, ending on a whole UTF-8
// character. Only the event's note is cut. The Ready condition keeps the
// whole message.
func fitNote(note string) string {
	if len(note) <= maxNote {
		return note
	}
	cut := note[:maxNote]
	for !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}
