package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupRunSpec asks for one backup now. It names one volume, one database, or
// everything in the namespace marked backup.wlz.li/enabled.
//
// For each volume, the controller writes the ReplicationSource itself, names it
// after the claim, and triggers it. For each database, it asks CloudNativePG
// for a base backup. When the namespace has a Kueue LocalQueue, the run waits
// for the queue to admit it as one Workload. A run with all set also scales the
// workloads marked backup.wlz.li/quiesce to zero until every volume's clone is
// cut.
// +kubebuilder:validation:XValidation:rule="(has(self.source) ? 1 : 0) + (has(self.database) ? 1 : 0) + ((has(self.all) && self.all) ? 1 : 0) == 1",message="set exactly one of source, database and all"
type BackupRunSpec struct {
	// Source is the name of the claim to back up. The claim has to be marked
	// backup.wlz.li/enabled, and its ReplicationSource carries the same name.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Source string `json:"source,omitempty"`

	// Database is the name of the CloudNativePG Cluster in this namespace to
	// take a base backup of. The Cluster has to be marked
	// backup.wlz.li/enabled.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Database string `json:"database,omitempty"`

	// All backs up every claim and every Cluster in this namespace marked
	// backup.wlz.li/enabled. It also stops the workloads marked
	// backup.wlz.li/quiesce while the volumes' clones are cut.
	// +optional
	All bool `json:"all,omitempty"`

	// Timeout is how long the run may work on its movers and base backups
	// before it gives up. The clock starts when the queue admits the run, so
	// time spent queued does not count. When omitted, the namespace's
	// backup.wlz.li/timeout annotation applies, and six hours applies when the
	// namespace has none.
	// +optional
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// TTLSecondsAfterFinished is how many seconds after the run finishes the
	// controller deletes it. When omitted, the run is kept as the record of
	// what was backed up.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// BackupRunStatus reports how far the run got.
type BackupRunStatus struct {
	// Phase is the run's state. It is the Phase column `kubectl get` prints.
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// Workload is the name of the Kueue Workload that admits the run. It is
	// cleared when the run finishes and the Workload is deleted.
	// +optional
	Workload string `json:"workload,omitempty"`

	// StartedAt is when the run was admitted and began backing up.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the run reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// QuiescedAt is when the run stopped the workloads marked
	// backup.wlz.li/quiesce, or found that none were marked.
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

	// Items has one entry for each volume and each database the run backs up.
	// +optional
	Items []BackupItem `json:"items,omitempty"`

	// Conditions holds the kstatus-compatible state, so a Flux Kustomization
	// with wait: true can wait for this object.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// QuiescedWorkload is one workload a run scaled to zero.
type QuiescedWorkload struct {
	// Kind is Deployment or StatefulSet.
	Kind string `json:"kind"`
	// Name is the workload's name in the run's namespace.
	Name string `json:"name"`
	// Replicas is the replica count the workload had before the run stopped
	// it.
	Replicas int32 `json:"replicas"`
}

// BackupItem is one volume or database a BackupRun backs up.
type BackupItem struct {
	// Kind is ReplicationSource for a volume and Cluster for a database.
	Kind string `json:"kind"`
	// Name is the object's name. For a volume, it is also the claim's name.
	Name string `json:"name"`
	// Phase is how far this item got.
	Phase ItemPhase `json:"phase"`
	// Message says why the item failed or was skipped, or what it is waiting
	// for.
	// +optional
	Message string `json:"message,omitempty"`
	// Trigger is the manual trigger value the run wrote onto the
	// ReplicationSource.
	// +optional
	Trigger string `json:"trigger,omitempty"`
	// Snapshot is the short ID of the restic snapshot the mover wrote.
	// +optional
	Snapshot string `json:"snapshot,omitempty"`
	// SnapshotTime is the time recorded on that snapshot. restic records the
	// moment the backup of the volume's clone started. When the run stopped
	// workloads, it then rewrites the snapshot to carry the run's restartedAt
	// and tags it quiesced, so that a restore can recover the databases to the
	// same moment.
	// +optional
	SnapshotTime *metav1.Time `json:"snapshotTime,omitempty"`
	// Empty is true when VolSync took no backup of the volume because it held
	// no files. The repository gains no snapshot from such a run.
	// +optional
	Empty bool `json:"empty,omitempty"`
	// Backup is the name of the CloudNativePG Backup the run created for a
	// database.
	// +optional
	Backup string `json:"backup,omitempty"`
}

// ItemPhase is the state of one item in a run.
type ItemPhase string

const (
	// ItemPending is an item the run has not started.
	ItemPending ItemPhase = "Pending"
	// ItemRunning is an item whose backup or restore is under way.
	ItemRunning ItemPhase = "Running"
	// ItemSucceeded is an item that finished.
	ItemSucceeded ItemPhase = "Succeeded"
	// ItemFailed is an item that gave up. Its message gives the reason.
	ItemFailed ItemPhase = "Failed"
	// ItemSkipped is an item the run left alone. Its message gives the
	// reason.
	ItemSkipped ItemPhase = "Skipped"
	// ItemDeleted is a database whose Cluster a RestoreRun deleted, waiting
	// for the Cluster to be created again.
	ItemDeleted ItemPhase = "Deleted"
	// ItemRecovering is a recreated database replaying WAL up to the run's
	// moment.
	ItemRecovering ItemPhase = "Recovering"
)

// RunPhase is the state of a one-shot run. BackupRun and RestoreRun both use
// it, so the Phase column reads the same for either kind.
type RunPhase string

const (
	// RunPhaseQueued is a run waiting for Kueue to admit it.
	RunPhaseQueued RunPhase = "Queued"

	// RunPhaseRunning is a run whose work is under way.
	RunPhaseRunning RunPhase = "Running"

	// RunPhaseWaiting is a run held up by something outside its control. The
	// Ready condition's message names what it is waiting for.
	RunPhaseWaiting RunPhase = "Waiting"

	// RunPhaseSucceeded is a run whose work finished.
	RunPhaseSucceeded RunPhase = "Succeeded"

	// RunPhaseFailed is a run that gave up. The run cleans up what it created
	// before it reports this phase.
	RunPhaseFailed RunPhase = "Failed"
)

// Finished reports whether the phase is terminal, meaning Succeeded or
// Failed.
func (p RunPhase) Finished() bool {
	return p == RunPhaseSucceeded || p == RunPhaseFailed
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Namespaced,shortName=brun
// +kubebuilder:printcolumn:name="Source",type=string,JSONPath=`.spec.source`
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.database`
// +kubebuilder:printcolumn:name="All",type=boolean,JSONPath=`.spec.all`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupRun takes one backup on demand. The scheduler also creates one for
// every scheduled backup, and the object stays as the record of it. Create a
// BackupRun and watch it the way you would a Job.
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
