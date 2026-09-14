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
