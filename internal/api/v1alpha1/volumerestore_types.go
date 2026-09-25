package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// VolumeRestoreSpec names the restic repository a claim is restored from, and
// says how the mover that runs the restore should be set up. Each field is
// passed to the field of the same name on the ReplicationDestination the
// controller creates.
type VolumeRestoreSpec struct {
	// Repository is the name of the Secret in this namespace that holds the
	// restic repository URL, its password and the object store keys. It is
	// the same Secret the app's ReplicationSource names. The controller
	// copies it into its own namespace while a restore runs.
	// +kubebuilder:validation:Required
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Repository string `json:"repository"`

	// RestoreAsOf selects the newest snapshot taken at or before this RFC 3339
	// time. When omitted, the newest snapshot in the repository is used. A
	// claim's own backup.wlz.li/restore-as-of annotation takes precedence over
	// it for that claim.
	// +optional
	// +kubebuilder:validation:Format="date-time"
	RestoreAsOf *string `json:"restoreAsOf,omitempty"`

	// CacheStorageClassName is the storage class for the claim that holds the
	// restic mover's metadata cache. When omitted, the cluster's default class
	// provisions it. If that class has reclaimPolicy Retain, every restore
	// leaves a volume behind, so name a class whose reclaimPolicy is Delete.
	// +optional
	// +kubebuilder:validation:MinLength=1
	// +kubebuilder:validation:MaxLength=253
	CacheStorageClassName *string `json:"cacheStorageClassName,omitempty"`

	// CacheCapacity is the size of the cache claim. When omitted, VolSync
	// picks its own default.
	// +optional
	CacheCapacity *resource.Quantity `json:"cacheCapacity,omitempty"`

	// MoverPodLabels are added to the mover pod, so that the cluster's backup
	// queue admits the restore the same way it admits every other mover. Keys
	// and values must be valid label syntax. The map holds at most 8 entries,
	// because the API server installs the label syntax rules only for a map
	// whose maximum size is declared.
	// +optional
	// +kubebuilder:validation:MaxProperties=8
	// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^([a-z0-9]([-a-z0-9_.]*[a-z0-9])?/)?[a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?$'))",message="moverPodLabels keys must be a valid label key"
	// +kubebuilder:validation:XValidation:rule="self.all(k, self[k].matches('^([a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?)?$'))",message="moverPodLabels values must be a valid label value"
	MoverPodLabels map[string]MoverPodLabelValue `json:"moverPodLabels,omitempty"`

	// MoverSecurityContext is copied onto the ReplicationDestination
	// unchanged.
	// +optional
	MoverSecurityContext *corev1.PodSecurityContext `json:"moverSecurityContext,omitempty"`
}

// MoverPodLabelValue is one value in MoverPodLabels.
//
// It exists as its own type so that the generated schema can bound its length.
// The schema bounds a map's values only through their type, and the API server
// refuses to install a CEL rule over a map of unbounded strings, because the
// rule's estimated cost exceeds its budget by the size such a value could
// reach. 63 is the maximum length Kubernetes allows for a label value.
// +kubebuilder:validation:MaxLength=63
type MoverPodLabelValue string

// MoverLabels returns MoverPodLabels as the plain string map a
// ReplicationDestination takes. It returns nil when no labels are set.
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
// now. A VolumeRestore is a standing declaration that fills claims as they
// appear, so a restore that has finished leaves no entry here.
type VolumeRestoreStatus struct {
	// Conditions holds the object's kstatus-compatible state, so a Flux
	// Kustomization with wait: true can wait for this object.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`

	// Claims has one entry for each claim currently being populated from this
	// object.
	// +optional
	Claims []ClaimRestoreStatus `json:"claims,omitempty"`
}

// ClaimRestoreStatus is one claim being populated from a VolumeRestore.
type ClaimRestoreStatus struct {
	// Name is the name of the claim, in the VolumeRestore's namespace, that
	// names this VolumeRestore in its dataSourceRef.
	Name string `json:"name"`

	// UID is the claim's UID. The objects the restore creates are named
	// after it.
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

	// RestorePhaseFailed is a claim whose restore failed: its mover reported
	// a failure, or no snapshot reaches its restore-as-of time.
	RestorePhaseFailed RestorePhase = "Failed"
)

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=vrestore
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`

// VolumeRestore declares that claims in this namespace are restored from a
// restic repository. A claim names the VolumeRestore in its dataSourceRef,
// and the controller fills the claim's volume from the repository before the
// claim binds.
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
