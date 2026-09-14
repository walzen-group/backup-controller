package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// VolumeRestoreSpec names the restic repository a claim is restored from, and
// how the mover that does the restore should run. Every field is a passthrough
// to a field of the ReplicationDestination the controller creates, and carries
// that field's name.
type VolumeRestoreSpec struct {
	// Repository is the Secret in this namespace holding the restic repository
	// URL, its password and the object store keys. It is the Secret the app's
	// ReplicationSource names, and the controller copies it into its own
	// namespace for the length of the restore.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Repository string `json:"repository"`

	// RestoreAsOf selects the newest snapshot taken at or before this time.
	// Omitted, the newest snapshot in the repository is used.
	// +optional
	// +kubebuilder:validation:Format="date-time"
	RestoreAsOf *string `json:"restoreAsOf,omitempty"`

	// MoverPodLabels are put on the mover pod, so the restore is admitted by
	// the cluster's backup queue the way every other mover is. Keys and values
	// must be label syntax, and the object carries at most 8 of them: the API
	// server installs the label rules only for a map whose size is declared.
	// +optional
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^([a-z0-9]([-a-z0-9_.]*[a-z0-9])?/)?[a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?$'))",message="moverPodLabels keys must be a valid label key"
	// +kubebuilder:validation:XValidation:rule="self.all(k, self[k].matches('^([a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?)?$'))",message="moverPodLabels values must be a valid label value"
	MoverPodLabels map[string]MoverPodLabelValue `json:"moverPodLabels,omitempty"`

	// MoverSecurityContext is passed through to the ReplicationDestination
	// unchanged.
	// +optional
	MoverSecurityContext *corev1.PodSecurityContext `json:"moverSecurityContext,omitempty"`
}

// MoverPodLabelValue is one value of a mover label.
//
// The generated schema bounds a map's values only through their type, and the
// API server refuses to install a CEL rule over a map of unbounded strings:
// the rule's estimated cost runs past its budget by the size such a value may
// reach. 63 is the length Kubernetes gives a label value.
// +kubebuilder:validation:MaxLength=63
type MoverPodLabelValue string

// MoverLabels returns MoverPodLabels as the plain map a ReplicationDestination
// takes, converting the bounded value type back to string.
func (s VolumeRestoreSpec) MoverLabels() map[string]string {
	if s.MoverPodLabels == nil {
		return nil
	}
	labels := make(map[string]string, len(s.MoverPodLabels))
	for key, value := range s.MoverPodLabels {
		labels[key] = string(value)
	}
	return labels
}

// VolumeRestoreStatus reports what is being populated from this object right
// now. A VolumeRestore is a standing declaration rather than a one-shot job,
// so a restore that finished leaves no entry here.
type VolumeRestoreStatus struct {
	// Conditions carry the object's kstatus-compatible state, so a Flux
	// Kustomization with wait: true can gate on this object.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Claims has one entry per claim currently being populated from this
	// object.
	// +optional
	Claims []ClaimRestoreStatus `json:"claims,omitempty"`
}

// ClaimRestoreStatus is one claim being populated from a VolumeRestore.
type ClaimRestoreStatus struct {
	// Name is the claim in the VolumeRestore's namespace that named this
	// object in its dataSourceRef.
	Name string `json:"name"`

	// UID is that claim's UID, which names the objects the restore created.
	UID types.UID `json:"uid"`

	// Phase is how far the claim's restore has got.
	Phase RestorePhase `json:"phase"`

	// StartedAt is when this object first reported the claim.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`
}

// RestorePhase is the state of one claim's restore.
type RestorePhase string

const (
	// RestorePhaseRestoring is a claim whose volume is being filled.
	RestorePhaseRestoring RestorePhase = "Restoring"

	// RestorePhaseFailed is a claim whose mover reported a failure.
	RestorePhaseFailed RestorePhase = "Failed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=vrestore
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`

// VolumeRestore is a namespaced declaration that a claim in this namespace is
// restored from a restic repository. A claim names it in its dataSourceRef.
type VolumeRestore struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   VolumeRestoreSpec   `json:"spec,omitempty"`
	Status VolumeRestoreStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// VolumeRestoreList is a list of VolumeRestore.
type VolumeRestoreList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []VolumeRestore `json:"items"`
}
