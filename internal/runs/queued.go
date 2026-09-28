package runs

import (
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// queuedMessage is the Ready message of a run that waits for Kueue to admit
// its Workload.
const queuedMessage = "waiting for the backup queue to admit the run"

// setQueued sets a run's Ready condition to False with reason Queued and the
// given message.
//
// Parameters:
//   - conditions is the run's status.conditions.
//   - generation is the run's metadata.generation, for observedGeneration.
//   - message says what the run waits for: queuedMessage, or the message of a
//     NoQueueError from the admission package.
//
// It returns true when the condition changed, so the caller writes the status
// only then.
func setQueued(conditions *[]metav1.Condition, generation int64, message string) bool {
	current := meta.FindStatusCondition(*conditions, backupv1alpha1.ConditionReady)
	if current != nil && current.Reason == backupv1alpha1.ReasonQueued && current.Message == message {
		return false
	}
	backupv1alpha1.SetReady(conditions, generation, metav1.ConditionFalse, backupv1alpha1.ReasonQueued, message)
	return true
}
