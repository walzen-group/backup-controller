package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// RestoreRunSpec asks for one restore. It names one volume, one database, or
// everything in the namespace marked backup.wlz.li/enabled, and restores it to
// the newest backup or to a chosen moment.
//
// A volume is restored in place, or into a new claim when Into is set. A
// database is restored by creating it again. The run deletes its Cluster, and
// when Flux or tofu creates the Cluster again, the bootstrap webhook sets it up
// to recover to RestoreAsOf.
// +kubebuilder:validation:XValidation:rule="((has(self.claim) || has(self.repository)) ? 1 : 0) + (has(self.database) ? 1 : 0) + ((has(self.all) && self.all) ? 1 : 0) == 1",message="set exactly one of claim (or repository), database and all"
// +kubebuilder:validation:XValidation:rule="!has(self.previous) || has(self.claim) || has(self.repository)",message="previous applies to one volume only"
// +kubebuilder:validation:XValidation:rule="!has(self.into) || has(self.claim) || has(self.repository)",message="into needs claim or repository"
// +kubebuilder:validation:XValidation:rule="!has(self.syncDatabaseToVolume) || !self.syncDatabaseToVolume || (has(self.all) && self.all)",message="syncDatabaseToVolume needs all"
// +kubebuilder:validation:XValidation:rule="!has(self.quiesce) || size(self.quiesce) == 0 || !has(self.into)",message="quiesce restores in place; an into restore leaves the app alone"
type RestoreRunSpec struct {
	// Claim is the name of a claim in this namespace. The restore reads from
	// that claim's repository, and writes into the claim itself unless Into
	// names a new one.
	//
	// The claim's VolumeRestore supplies the repository Secret, the cache
	// storage class and the mover's pod labels, so none of them is repeated
	// here. That is the VolumeRestore the claim's dataSourceRef names, or the
	// one with the claim's own name. To restore from a repository that no
	// claim in this namespace uses, set Repository instead.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Claim string `json:"claim,omitempty"`

	// Repository is the name of a restic repository Secret in this namespace
	// to restore from. Use it when the backup belongs to no claim in this
	// namespace. It needs Into.
	//
	// The Secret has to be in this namespace. If a RestoreRun could name a
	// Secret in any namespace, anyone allowed to create one here could read
	// every backup in the cluster. Copying a Secret into this namespace is
	// the deliberate step that grants that access.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Repository string `json:"repository,omitempty"`

	// Into is the name of a new claim to create and fill. The source claim
	// stays untouched. When omitted, the restore overwrites Claim in place,
	// and the workload that mounts it has to stop first.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Into string `json:"into,omitempty"`

	// IntoSize is the storage request of the claim that Into creates. When
	// omitted, the new claim requests the same size as the source claim. A
	// restore from Repository has no source claim to copy a size from, so
	// set it there.
	// +optional
	IntoSize *resource.Quantity `json:"intoSize,omitempty"`

	// Database is the name of the CloudNativePG Cluster in this namespace to
	// restore. The run deletes the Cluster, and the Cluster is recovered when
	// it is created again.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Database string `json:"database,omitempty"`

	// All restores every claim and every Cluster in this namespace marked
	// backup.wlz.li/enabled. The volumes are restored in place first, and the
	// databases after them.
	// +optional
	All bool `json:"all,omitempty"`

	// RestoreAsOf is the moment to restore to, as an RFC 3339 time. A volume
	// restores the newest snapshot taken at or before it, and a database
	// replays WAL up to exactly that moment. When it is omitted and Previous
	// is unset, a volume restores its newest snapshot and a database replays
	// to the end of its WAL archive.
	// +optional
	// +kubebuilder:validation:Format="date-time"
	RestoreAsOf *string `json:"restoreAsOf,omitempty"`

	// Previous is the number of snapshots to step back from the one the run
	// would otherwise select. That is the newest snapshot at or before
	// RestoreAsOf, or the newest snapshot when RestoreAsOf is omitted. It
	// applies to a single volume only.
	// +optional
	// +kubebuilder:validation:Minimum=0
	Previous *int32 `json:"previous,omitempty"`

	// SyncDatabaseToVolume recovers the databases to the moment of the
	// volumes' snapshots, and RestoreAsOf then selects the snapshots only. It
	// needs All. The run considers only snapshots tagged quiesced, which a
	// BackupRun writes when it stopped the workloads. Such a snapshot carries
	// the moment the run gave the workloads back, and neither the volumes nor
	// the databases were written between the stop and that moment. Every
	// volume has to select a snapshot of the same moment.
	// +optional
	SyncDatabaseToVolume bool `json:"syncDatabaseToVolume,omitempty"`

	// Quiesce lists the workloads in this namespace to stop before anything
	// is restored. For each one, the run suspends the Flux Kustomization that
	// applies it and scales it to zero. The run gives the workloads back once
	// every volume is restored and every database deleted, down to the old
	// Cluster's last instance pod and PVC. It gives them back at that point
	// because a database comes back only when its owner creates it again, and
	// a suspended Kustomization creates nothing. When omitted, the run waits
	// for whatever mounts a claim to stop on its own.
	// +optional
	// +kubebuilder:validation:MaxItems=32
	Quiesce []WorkloadRef `json:"quiesce,omitempty"`

	// Timeout is how long the run waits for its movers and recovered databases
	// before it gives up. The clock starts when the run passes its checks.
	// +optional
	// +kubebuilder:default="4h"
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// MoverSecurityContext is copied onto the ReplicationDestination. Set it
	// for an app whose files belong to a user the mover has to run as. When
	// omitted, the one on the claim's VolumeRestore applies.
	// +optional
	MoverSecurityContext *corev1.PodSecurityContext `json:"moverSecurityContext,omitempty"`

	// TTLSecondsAfterFinished is how many seconds after the run finishes the
	// controller deletes it. When omitted, the run is kept as the record of
	// what was restored.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// RestoreRunStatus reports how far the restore got.
type RestoreRunStatus struct {
	// Phase is the run's state. It is the Phase column `kubectl get` prints.
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// Target is the name of the claim an into restore creates and fills.
	// +optional
	Target string `json:"target,omitempty"`

	// SyncedTo is the moment a syncDatabaseToVolume run restores everything
	// to. It is the time on the volumes' quiesced snapshots, and the
	// databases recover to it as well.
	// +optional
	SyncedTo *metav1.Time `json:"syncedTo,omitempty"`

	// QuiescedAt is when the run stopped the workloads that spec.quiesce
	// lists.
	// +optional
	QuiescedAt *metav1.Time `json:"quiescedAt,omitempty"`

	// RestartedAt is when the run gave those workloads their replicas back.
	// +optional
	RestartedAt *metav1.Time `json:"restartedAt,omitempty"`

	// Quiesced lists the workloads this run scaled to zero, each with the
	// replica count the run restores when it starts them again.
	// +optional
	Quiesced []QuiescedWorkload `json:"quiesced,omitempty"`

	// SuspendedKustomizations lists the Flux Kustomizations this run
	// suspended, as namespace/name. The run resumes these and no others.
	// +optional
	SuspendedKustomizations []string `json:"suspendedKustomizations,omitempty"`

	// StartedAt is when the run passed its checks and began restoring.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the run reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// Items has one entry for each claim and each database the run restores.
	// An into restore has a single entry, for the claim it creates.
	// +optional
	Items []RestoreItem `json:"items,omitempty"`

	// Conditions holds the kstatus-compatible state, so a Flux Kustomization
	// with wait: true can wait for this object.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// WorkloadRef names a Deployment or a StatefulSet in the run's namespace.
type WorkloadRef struct {
	// Kind is Deployment or StatefulSet.
	// +kubebuilder:validation:Enum=Deployment;StatefulSet
	Kind string `json:"kind"`
	// Name is the workload's name.
	// +kubebuilder:validation:MaxLength=253
	Name string `json:"name"`
}

// RestoreItem is one volume or database a RestoreRun restores.
type RestoreItem struct {
	// Kind is PersistentVolumeClaim for a volume and Cluster for a database.
	Kind string `json:"kind"`
	// Name is the name of the claim or the Cluster.
	Name string `json:"name"`
	// Phase is how far this item got.
	Phase ItemPhase `json:"phase"`
	// Message says why the item failed or was skipped.
	// +optional
	Message string `json:"message,omitempty"`
	// Destination is the name of the ReplicationDestination restoring a
	// volume. It is cleared once the restore ends and the destination is
	// deleted.
	// +optional
	Destination string `json:"destination,omitempty"`
	// Snapshot is the short ID of the restic snapshot a volume restores.
	// +optional
	Snapshot string `json:"snapshot,omitempty"`
	// BaseBackup is the ID of the barman base backup a database's recovery
	// starts from.
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

// RestoreRun restores volumes and databases to a chosen point in time. Create
// a RestoreRun and watch it the way you would a Job.
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
