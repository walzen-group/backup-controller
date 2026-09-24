package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestoreRunSpec asks for one restore: of one volume, of one database, or of
// everything the namespace marks backup.wlz.li/enabled, to the newest backup
// or to a chosen moment.
//
// A volume restores in place, or into a new claim with Into. A database
// restores by being created again: the run deletes its Cluster, and when Flux
// or tofu creates it again the bootstrap webhook recovers it to RestoreAsOf.
// +kubebuilder:validation:XValidation:rule="((has(self.claim) || has(self.repository)) ? 1 : 0) + (has(self.database) ? 1 : 0) + ((has(self.all) && self.all) ? 1 : 0) == 1",message="set exactly one of claim (or repository), database and all"
// +kubebuilder:validation:XValidation:rule="!has(self.previous) || has(self.claim) || has(self.repository)",message="previous applies to one volume only"
// +kubebuilder:validation:XValidation:rule="!has(self.into) || has(self.claim) || has(self.repository)",message="into needs claim or repository"
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
	// restore whose source is not a claim in this namespace. It needs Into.
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
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Into string `json:"into,omitempty"`

	// IntoSize is the size of the claim named by Into. Omitted, the source
	// claim's request is used.
	// +optional
	IntoSize *resource.Quantity `json:"intoSize,omitempty"`

	// Database is the CloudNativePG Cluster in this namespace to restore. The
	// run deletes it, and it is recovered when it is created again.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Database string `json:"database,omitempty"`

	// All restores every claim and every Cluster in this namespace marked
	// backup.wlz.li/enabled: the volumes in place, then the databases.
	// +optional
	All bool `json:"all,omitempty"`

	// RestoreAsOf is the moment to restore to. A volume restores the newest
	// snapshot taken at or before it; a database replays WAL to it exactly.
	// Omitted, and with Previous unset, a volume restores its newest snapshot
	// and a database the end of its WAL archive.
	// +optional
	// +kubebuilder:validation:Format="date-time"
	RestoreAsOf *string `json:"restoreAsOf,omitempty"`

	// Previous is how many snapshots further back from the selected one to go.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Previous *int32 `json:"previous,omitempty"`

	// Timeout is how long to wait for the movers and the recovered databases
	// before giving up.
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
	// Phase is the run's state, and the column `kubectl get` prints.
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// Target is the claim an Into restore creates and fills.
	// +optional
	Target string `json:"target,omitempty"`

	// StartedAt is when the run passed its checks and began restoring.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the run reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Items has one entry per volume restored in place and per database.
	// +optional
	Items []RestoreItem `json:"items,omitempty"`

	// Conditions carry the kstatus-compatible state.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// RestoreItem is one volume or database a RestoreRun restores.
type RestoreItem struct {
	// Kind is PersistentVolumeClaim for a volume and Cluster for a database.
	Kind string `json:"kind"`
	// Name is the claim's or the Cluster's name.
	Name string `json:"name"`
	// Phase is how far this item got.
	Phase ItemPhase `json:"phase"`
	// Message says why an item failed or was skipped.
	// +optional
	Message string `json:"message,omitempty"`
	// Destination is the ReplicationDestination restoring a volume, while it
	// exists.
	// +optional
	Destination string `json:"destination,omitempty"`
	// Snapshot is the restic snapshot a volume restores, as its short ID.
	// +optional
	Snapshot string `json:"snapshot,omitempty"`
	// BaseBackup is the barman base backup a database's recovery starts from.
	// +optional
	BaseBackup string `json:"baseBackup,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=rrun
// +kubebuilder:printcolumn:name="Claim",type=string,JSONPath=`.spec.claim`
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.database`
// +kubebuilder:printcolumn:name="All",type=boolean,JSONPath=`.spec.all`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// RestoreRun restores volumes and databases to a chosen point in time. Submit
// it and watch it the way you would a Job.
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
