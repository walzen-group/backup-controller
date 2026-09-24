// Package-level condition vocabulary for VolumeRestore. The reconcile that
// reports these reasons lives in internal/populator.
package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionReady reports whether the object's claims have been filled: False
// while a claim is being populated from this VolumeRestore, True when none is.
const ConditionReady = "Ready"

// Reasons the Ready condition carries.
const (
	// ReasonRestoring reports a claim that is being populated now.
	ReasonRestoring = "Restoring"

	// ReasonRestoreFailed reports a claim whose mover failed: its volume will
	// not be filled until something changes.
	ReasonRestoreFailed = "RestoreFailed"

	// ReasonRestored reports that nothing is being populated from this object.
	ReasonRestored = "Restored"
)

// Reasons a BackupRun or a RestoreRun carries on its Ready condition. A run is
// Ready False while it works and True once it has finished, either way, so a
// Flux Kustomization with wait: true gates on it the way it gates on a Job.
const (
	// ReasonRunning reports work under way.
	ReasonRunning = "Running"

	// ReasonSucceeded reports work that finished.
	ReasonSucceeded = "Succeeded"

	// ReasonFailed reports a run that gave up. Whatever it created is removed
	// before it says so.
	ReasonFailed = "Failed"

	// ReasonQueued reports a run waiting for Kueue to admit it.
	ReasonQueued = "Queued"

	// ReasonSourceBusy reports a run waiting for another run's backup of the
	// same volume to finish.
	ReasonSourceBusy = "SourceBusy"

	// ReasonNoBackupInReach reports a restore whose moment predates every
	// backup of an item. The run fails before it deletes or overwrites
	// anything.
	ReasonNoBackupInReach = "NoBackupInReach"

	// ReasonRecreate reports a restore waiting for the Clusters it deleted to
	// be created again, by Flux or by a terragrunt apply.
	ReasonRecreate = "WaitingForRecreate"

	// ReasonClaimInUse reports an in-place restore waiting for a pod to let go
	// of the claim it has to write into. Two writers on one filesystem is how
	// the volume being restored is corrupted, so the run waits.
	ReasonClaimInUse = "ClaimInUse"

	// ReasonTimedOut reports a mover still running at the run's timeout. The
	// trigger is cleared and the destination removed before this is reported,
	// so a stuck mover never leaves a source holding a spent tag.
	ReasonTimedOut = "TimedOut"

	// ReasonInvalid reports a spec the controller will not act on, with the
	// field named in the message.
	ReasonInvalid = "Invalid"
)

// SetReady sets or replaces the Ready condition on conditions, stamped with the
// generation the caller observed. It is the only way this package writes the
// condition, so the type and the transition time are never retyped by callers.
func SetReady(conditions *[]metav1.Condition, generation int64, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}
