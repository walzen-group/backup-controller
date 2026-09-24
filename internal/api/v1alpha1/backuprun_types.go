package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// BackupRunSpec asks for one backup now: of one volume, of one database, or of
// everything the namespace marks backup.wlz.li/enabled.
//
// The controller writes and triggers each volume's ReplicationSource itself,
// named after the claim, and asks CloudNativePG for a base backup of each
// database. A run with all set is admitted by the namespace's Kueue queue as
// one unit and stops the workloads marked backup.wlz.li/quiesce until every
// volume's clone is cut.
// +kubebuilder:validation:XValidation:rule="(has(self.source) ? 1 : 0) + (has(self.database) ? 1 : 0) + ((has(self.all) && self.all) ? 1 : 0) == 1",message="set exactly one of source, database and all"
type BackupRunSpec struct {
	// Source is the claim to back up. Its ReplicationSource carries the same
	// name, and the claim has to be marked backup.wlz.li/enabled.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Source string `json:"source,omitempty"`

	// Database is the CloudNativePG Cluster in this namespace to take a base
	// backup of.
	// +optional
	// +kubebuilder:validation:MaxLength=253
	Database string `json:"database,omitempty"`

	// All backs up every claim and every Cluster in this namespace marked
	// backup.wlz.li/enabled, and stops the workloads marked
	// backup.wlz.li/quiesce while the clones are cut.
	// +optional
	All bool `json:"all,omitempty"`

	// Timeout is how long the run waits for its movers and base backups
	// before it gives up. Admission by the queue does not count toward it.
	// +optional
	// +kubebuilder:default="1h"
	Timeout *metav1.Duration `json:"timeout,omitempty"`

	// TTLSecondsAfterFinished deletes this object that long after it reaches a
	// terminal phase. Omitted, it is kept as the record of what was backed up.
	// +optional
	// +kubebuilder:validation:Minimum=0
	TTLSecondsAfterFinished *int32 `json:"ttlSecondsAfterFinished,omitempty"`
}

// BackupRunStatus reports how far the run got.
type BackupRunStatus struct {
	// Phase is the run's state, and the column `kubectl get` prints.
	// +optional
	Phase RunPhase `json:"phase,omitempty"`

	// Workload is the Kueue Workload that admits the run, while it exists.
	// +optional
	Workload string `json:"workload,omitempty"`

	// StartedAt is when the run was admitted and began backing up.
	// +optional
	StartedAt *metav1.Time `json:"startedAt,omitempty"`

	// CompletedAt is when the run reached a terminal phase.
	// +optional
	CompletedAt *metav1.Time `json:"completedAt,omitempty"`

	// QuiescedAt is when the run stopped the workloads marked
	// backup.wlz.li/quiesce, or found none to stop.
	// +optional
	QuiescedAt *metav1.Time `json:"quiescedAt,omitempty"`

	// RestartedAt is when the run gave those workloads their replicas back.
	// +optional
	RestartedAt *metav1.Time `json:"restartedAt,omitempty"`

	// Quiesced lists the workloads this run scaled to zero, with the replicas
	// it gives each back.
	// +optional
	Quiesced []QuiescedWorkload `json:"quiesced,omitempty"`

	// SuspendedKustomizations lists the Flux Kustomizations this run
	// suspended, as namespace/name. The run resumes exactly these.
	// +optional
	SuspendedKustomizations []string `json:"suspendedKustomizations,omitempty"`

	// Items has one entry per volume and database the run backs up.
	// +optional
	Items []BackupItem `json:"items,omitempty"`

	// Conditions carry the kstatus-compatible state, so a Flux Kustomization
	// with wait: true can gate on this object.
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// QuiescedWorkload is one workload a run stopped.
type QuiescedWorkload struct {
	// Kind is Deployment or StatefulSet.
	Kind string `json:"kind"`
	// Name is the workload's name in the run's namespace.
	Name string `json:"name"`
	// Replicas is what the workload ran with before the run stopped it.
	Replicas int32 `json:"replicas"`
}

// BackupItem is one volume or database a BackupRun backs up.
type BackupItem struct {
	// Kind is ReplicationSource for a volume and Cluster for a database.
	Kind string `json:"kind"`
	// Name is the object's name, which for a volume is also the claim's.
	Name string `json:"name"`
	// Phase is how far this item got.
	Phase ItemPhase `json:"phase"`
	// Message says why an item failed or was skipped.
	// +optional
	Message string `json:"message,omitempty"`
	// Trigger is the manual tag the run wrote onto a ReplicationSource.
	// +optional
	Trigger string `json:"trigger,omitempty"`
	// Snapshot is the restic snapshot the mover wrote, as its short ID.
	// +optional
	Snapshot string `json:"snapshot,omitempty"`
	// SnapshotTime is the time restic stamped on that snapshot, which is when
	// the backup of the volume's clone started.
	// +optional
	SnapshotTime *metav1.Time `json:"snapshotTime,omitempty"`
	// Empty reports a volume VolSync did not back up because it held no
	// files. The repository gains no snapshot for such a run.
	// +optional
	Empty bool `json:"empty,omitempty"`
	// Backup is the CloudNativePG Backup the run created for a database.
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
	// ItemFailed is an item that gave up, with the reason in its message.
	ItemFailed ItemPhase = "Failed"
	// ItemSkipped is an item the run left alone, with the reason in its
	// message.
	ItemSkipped ItemPhase = "Skipped"
	// ItemDeleted is a database a RestoreRun deleted, waiting for its Cluster
	// to be created again.
	ItemDeleted ItemPhase = "Deleted"
	// ItemRecovering is a recreated database replaying WAL to the run's time.
	ItemRecovering ItemPhase = "Recovering"
)

// RunPhase is the state of a one-shot run. Both BackupRun and RestoreRun use
// it, so the column reads the same for either.
type RunPhase string

const (
	// RunPhaseQueued is a run waiting for Kueue to admit it.
	RunPhaseQueued RunPhase = "Queued"

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
// +kubebuilder:printcolumn:name="Database",type=string,JSONPath=`.spec.database`
// +kubebuilder:printcolumn:name="All",type=boolean,JSONPath=`.spec.all`
// +kubebuilder:printcolumn:name="Phase",type=string,JSONPath=`.status.phase`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`

// BackupRun takes one backup on demand, and is also the record the scheduler
// leaves for every scheduled backup. Submit it and watch it the way you would
// a Job.
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
