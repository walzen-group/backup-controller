package v1alpha1

// The kinds a run names in its status and in the Leases it takes. Each is the
// Kubernetes kind of the object it stands for. The API types keep these
// fields as plain strings, so the CRDs list no enum for them.
const (
	// ItemKindClaim is the kind of a RestoreRun's volume item, which names
	// the claim it restores.
	ItemKindClaim = "PersistentVolumeClaim"

	// ItemKindSource is the kind of a BackupRun's volume item, which names
	// the ReplicationSource that backs the claim of the same name up.
	ItemKindSource = "ReplicationSource"

	// ItemKindCluster is the kind of a database item in either run, which
	// names a CloudNativePG Cluster.
	ItemKindCluster = "Cluster"

	// WorkloadKindDeployment is a Deployment in spec.quiesce and
	// status.quiesced.
	WorkloadKindDeployment = "Deployment"

	// WorkloadKindStatefulSet is a StatefulSet in spec.quiesce and
	// status.quiesced.
	WorkloadKindStatefulSet = "StatefulSet"

	// KindBackupRun is the kind of a BackupRun, as a Lease names its holder.
	KindBackupRun = "BackupRun"

	// KindRestoreRun is the kind of a RestoreRun, as a Lease names its
	// holder.
	KindRestoreRun = "RestoreRun"
)
