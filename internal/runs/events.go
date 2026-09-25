package runs

import (
	"unicode/utf8"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/events"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// maxNote is the longest note events.k8s.io/v1 accepts. The API server rejects
// a longer one, and a failed mover's logs run past it.
const maxNote = 1024

// readyReason is the reason on a run's Ready condition, empty before the first.
func readyReason(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == backupv1alpha1.ConditionReady {
			return c.Reason
		}
	}
	return ""
}

// announce records an event on a run whose Ready condition moved to another
// reason during a reconcile, with the condition's message as the note. A run
// that finished any way but Succeeded records a Warning.
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

// fitNote cuts a note to maxNote bytes on a character boundary. The Ready
// condition keeps the whole message.
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
