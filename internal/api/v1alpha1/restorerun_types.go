package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestoreRunSpec asks for one restore from a repository, at a point in time of
// your choosing.
//
// A VolumeRestore fills a claim at the moment the claim is created, and always
// from the newest backup. This object is the other operation: it acts on a
// claim that already exists and holds data, and it can reach any snapshot in
// the repository. docs/restores.md has the three shapes and what each discards.
type RestoreRunSpec struct {
	// Claim is the claim in this namespace whose repository to restore from,
	// and, unless Into names another, the claim the restore writes into.
	//
	// Its VolumeRestore supplies the repository Secret, the cache class and the
	// mover's queue label, so none of them is restated here. Give Repository
	// instead to restore from a repository no claim here owns.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Claim string `json:"claim,omitempty"`

	// Repository is the restic Secret in this namespace to restore from, for a
	// restore whose source is not a claim in this namespace. Exactly one of
	// Claim and Repository is required.
	//
	// The Secret has to be in this namespace. A RestoreRun that could name a
	// Secret anywhere would let whoever may create one here read any backup in
	// the cluster, so copying a Secret into this namespace is the deliberate
	// act that grants that.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Repository string `json:"repository,omitempty"`

	// Into is a claim to create and fill, leaving the source claim untouched.
	// Omitted, the restore overwrites Claim in place and the workload holding
	// it has to be stopped first.
	//
	// This is the shape to reach for when the question is whether an older
	// backup is any better, because it answers that without betting the current
	// data on the answer.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Into string `json:"into,omitempty"`

	// IntoSize is the size of the claim named by Into. Omitted, the source
	// claim's request is used, which is what you want unless the repository
	// holds more than the live volume now does.
	// +optional
	IntoSize *resource.Quantity `json:"intoSize,omitempty"`

	// RestoreAsOf selects the newest snapshot taken at or before this time.
	// Omitted, and with Previous unset, the newest snapshot is used.
	// +optional
	// +kubebuilder:validation:Format="date-time"
	RestoreAsOf *string `json:"restoreAsOf,omitempty"`

	// Previous is how many snapshots further back from the selected one to go.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Previous *int32 `json:"previous,omitempty"`

	// Timeout is how long to wait for the mover before giving up. On expiry the
	// run fails and its ReplicationDestination is removed.
	// +optional
	// +kubebuilder:default="4h"
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// MoverSecurityContext is passed through to the ReplicationDestination, for
	// an app whose files belong to a user the mover has to match. Omitted, the
	// source claim's VolumeRestore supplies it.
	// +optional
	MoverSecurityContext *corev1.PodSecurityContext `json:"moverSecurityContext,omitempty"`

	// TTLSecondsAfterFinished deletes this object that long after it reaches a
	// terminal phase. Omitted, it is kept as the record of what was restored.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// RestoreRunStatus reports how far the restore got.
type RestoreRunStatus struct {
	// Phase is the run's state, and the column `kubectl get` prints. A restore
	// in place sits in Waiting until nothing mounts the claim.
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// Destination is the ReplicationDestination doing the work, in this
	// namespace, for as long as the run lasts.
	// +optional
	Destination string `json:"destination,omitempty"`

	// Target is the claim being written, which is Into when it is set and Claim
	// otherwise.
	// +optional
	Target string `json:"target,omitempty"`

	// StartedAt is when the destination was created.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the run reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Conditions carry the kstatus-compatible state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=rrun
// +kubebuilder:printcolumn:name="Target",type=string,JSONPath=`.status.target`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RestoreRun restores a repository into a volume, at a chosen point in time.
// Submit it and watch it the way you would a Job; the controller creates the
// ReplicationDestination, waits for the mover, and removes it again.
type RestoreRun struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   RestoreRunSpec   `json:"spec,omitempty"`
	Status RestoreRunStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// RestoreRunList is a list of RestoreRun.
type RestoreRunList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []RestoreRun `json:"items"`
}
