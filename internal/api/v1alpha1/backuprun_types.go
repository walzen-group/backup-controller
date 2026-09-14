package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupRunSpec asks for one backup of a volume, now, outside its schedule.
//
// A ReplicationSource takes its backups on a cronspec. Running one on demand
// means giving it a manual trigger and taking that trigger away again when the
// run is over, because a manual tag wins wherever both are set and a source
// left holding a spent tag reports healthy while taking no further backups.
// This object exists so that sequence belongs to a controller rather than to
// whoever remembered to finish it.
type BackupRunSpec struct {
	// Source is the ReplicationSource in this namespace to run.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Source string `json:"source"`

	// Timeout is how long to wait for the mover before giving up. On expiry the
	// run fails and the trigger is cleared, so a mover that never finishes
	// leaves the source back on its schedule rather than silently stopped.
	// +optional
	// +kubebuilder:default="1h"
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// TTLSecondsAfterFinished deletes this object that long after it reaches a
	// terminal phase. Omitted, it is kept, which is usually what you want: the
	// question "did my backup before that restore actually finish" is worth
	// being able to answer later.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// BackupRunStatus reports how far the run got.
type BackupRunStatus struct {
	// Phase is the run's state, and the column `kubectl get` prints.
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// Trigger is the manual tag this run wrote onto the source. It is derived
	// from the run's UID, so a controller that restarts mid-run recomputes the
	// same value and continues rather than starting a second backup.
	// +optional
	Trigger string `json:"trigger,omitempty"`

	// StartedAt is when the trigger was written.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the run reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// SnapshotTime is the source's lastSyncTime for this run, which is the
	// moment the repository holds.
	// +optional
	SnapshotTime *metav1.Time `json:"snapshotTime,omitempty"`

	// Conditions carry the kstatus-compatible state, so a Flux Kustomization
	// with wait: true can gate on this object.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// RunPhase is the state of a one-shot run. Both BackupRun and RestoreRun use
// it, so the column reads the same for either.
type RunPhase string

const (
	// RunPhaseRunning is a run whose work is under way.
	RunPhaseRunning RunPhase = "Running"

	// RunPhaseWaiting is a run held up by something outside its control, with
	// the Ready condition's message naming what.
	RunPhaseWaiting RunPhase = "Waiting"

	// RunPhaseSucceeded is a run whose work finished.
	RunPhaseSucceeded RunPhase = "Succeeded"

	// RunPhaseFailed is a run that gave up. Anything it created is cleaned up
	// before it reports this.
	RunPhaseFailed RunPhase = "Failed"
)

// Finished reports whether a phase is terminal.
func (p RunPhase) Finished() bool {
	return p == RunPhaseSucceeded || p == RunPhaseFailed
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=brun
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.source`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupRun takes one backup of a volume on demand. Submit it and watch it the
// way you would a Job; the controller writes the source's manual trigger, waits
// for the mover, and takes the trigger away again.
type BackupRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   BackupRunSpec   `json:"spec,omitempty"`
	Status BackupRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// BackupRunList is a list of BackupRun.
type BackupRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []BackupRun `json:"items"`
}
