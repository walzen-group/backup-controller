package v1alpha1

// ItemReason is a one-word, machine-readable cause for an item's phase in a
// BackupRun or a RestoreRun. The item's message says the same in a sentence
// for a person; the reason is there for display and alerting, so nothing has
// to read the message to learn why an item ended.
//
// The CRD declares no enum for it: docs/api.md lists the reasons, and a
// release can add one without changing the CRD. An item that ended by a path
// which records no reason leaves it empty.
type ItemReason string

const (
	// ItemReasonTimedOut is an item the run failed because the run's timeout
	// ran out before the item finished. This includes the items of a
	// BackupRun that Kueue did not admit within its timeout.
	ItemReasonTimedOut ItemReason = "TimedOut"

	// ItemReasonMoverFailed is a volume backup whose VolSync mover reported
	// the result Failed.
	ItemReasonMoverFailed ItemReason = "MoverFailed"

	// ItemReasonRestoreJobFailed is a volume restore whose restore Job gave
	// no success: the Job ended with the condition Failed=True, and the
	// item's message carries restic's exit code and what it means; or the
	// run's own Job can't give a result, because it restores another
	// snapshot than the run selected, or the run no longer controls it.
	ItemReasonRestoreJobFailed ItemReason = "RestoreJobFailed"

	// ItemReasonRestoreJobDeleted is a volume restore whose restore Job was
	// deleted before it finished, or whose name now holds another Job. A
	// person may stop a restore that way. The run records no result from
	// the Job and waits until no pod of it can still write.
	ItemReasonRestoreJobDeleted ItemReason = "RestoreJobDeleted"

	// ItemReasonRestoreJobRefused is a volume restore whose restore Job never
	// ran. Either the run created no Job, because the API server refused the
	// create as Forbidden or Invalid, the Job's spec could not be built, or a
	// Job the run did not create holds its name; or the run created the Job
	// suspended and the API server refused to resume it as Forbidden or
	// Invalid. Nothing was written to the claim.
	ItemReasonRestoreJobRefused ItemReason = "RestoreJobRefused"

	// ItemReasonSnapshotChanged is a volume restore whose selected snapshot
	// was no longer in the repository when the run checked it again right
	// before the restore started.
	ItemReasonSnapshotChanged ItemReason = "SnapshotChanged"

	// ItemReasonClaimLost is a volume restore whose claim stopped being the
	// run's while the restore ran: it was deleted, replaced, or taken by
	// something else.
	ItemReasonClaimLost ItemReason = "ClaimLost"

	// ItemReasonNoMoverSnapshot is a volume backup whose sync completed, but
	// whose ReplicationSource status lacks lastSyncTime or lastSyncDuration,
	// or records a negative duration. Without them VolSync did not record
	// when the sync ran, so the run can't tell which snapshot in the
	// repository the sync wrote.
	ItemReasonNoMoverSnapshot ItemReason = "NoMoverSnapshot"

	// ItemReasonClaimMissing is an item whose claim does not exist when the
	// run checks it or starts it.
	ItemReasonClaimMissing ItemReason = "ClaimMissing"

	// ItemReasonClaimDeleting is a volume restore whose claim is being
	// deleted when the run would start it. Nothing was written to the claim.
	ItemReasonClaimDeleting ItemReason = "ClaimDeleting"

	// ItemReasonClaimNotBound is a volume backup whose claim is not bound to
	// a volume yet, so there is no node to place the mover on.
	ItemReasonClaimNotBound ItemReason = "ClaimNotBound"

	// ItemReasonVolumeMissing is a volume backup whose claim is bound to a
	// PersistentVolume that does not exist.
	ItemReasonVolumeMissing ItemReason = "VolumeMissing"

	// ItemReasonNoNodeAffinity is a volume backup whose PersistentVolume
	// declares no node affinity to place the mover by.
	ItemReasonNoNodeAffinity ItemReason = "NoNodeAffinity"

	// ItemReasonVolumeRestoreMissing is an item whose claim has no
	// VolumeRestore to name its restic repository.
	ItemReasonVolumeRestoreMissing ItemReason = "VolumeRestoreMissing"

	// ItemReasonRepositorySecretMissing is an item whose restic repository
	// Secret does not exist, so the run can't take the Lease that keeps
	// other runs' movers off the repository.
	ItemReasonRepositorySecretMissing ItemReason = "RepositorySecretMissing"

	// ItemReasonSettingsInvalid is an item whose settings don't parse: a
	// retention annotation on its claim, or a setting on its namespace.
	ItemReasonSettingsInvalid ItemReason = "SettingsInvalid"

	// ItemReasonSourceNotManaged is a volume backup whose claim has a
	// ReplicationSource the controller did not write, which it never writes
	// over.
	ItemReasonSourceNotManaged ItemReason = "SourceNotManaged"

	// ItemReasonSourceRefused is a volume backup whose ReplicationSource the
	// API server refused as invalid.
	ItemReasonSourceRefused ItemReason = "SourceRefused"

	// ItemReasonSourceAbandoned is a volume backup whose ReplicationSource
	// is still retrying a backup that no run waits for any more.
	ItemReasonSourceAbandoned ItemReason = "SourceAbandoned"

	// ItemReasonClusterMissing is a database backup or restore whose Cluster
	// does not exist when the run starts or checks it.
	ItemReasonClusterMissing ItemReason = "ClusterMissing"

	// ItemReasonClusterHibernated is a database backup the run skipped
	// because its Cluster is hibernated.
	ItemReasonClusterHibernated ItemReason = "ClusterHibernated"

	// ItemReasonBackupRefused is a database backup whose CloudNativePG
	// Backup the API server refused as invalid.
	ItemReasonBackupRefused ItemReason = "BackupRefused"

	// ItemReasonClusterRestoredElsewhere is a database restore whose Cluster
	// another unfinished RestoreRun is restoring. The run deleted nothing.
	ItemReasonClusterRestoredElsewhere ItemReason = "ClusterRestoredElsewhere"

	// ItemReasonIntoClaimTaken is an into restore whose claim name, from
	// spec.into, holds a claim the run did not create when the run would
	// create its own. The run writes only into a claim it created, so
	// nothing was written to that claim.
	ItemReasonIntoClaimTaken ItemReason = "IntoClaimTaken"

	// ItemReasonRunEnded is an item the run failed because the run ended
	// before the item finished, for a cause other than the timeout. The
	// run's status.ending says why the run ended.
	ItemReasonRunEnded ItemReason = "RunEnded"

	// ItemReasonCRDOutdated is an item the run failed because the installed
	// CRD of the run's kind lacks a field the controller writes, or the
	// controller may not read that CRD. The run ended with the Ready reason
	// CRDOutdated before it changed anything.
	ItemReasonCRDOutdated ItemReason = "CRDOutdated"

	// ItemReasonBackupFailed is a database backup whose CloudNativePG
	// Backup ended in the phase failed.
	ItemReasonBackupFailed ItemReason = "BackupFailed"

	// ItemReasonNotStarted is a volume backup the run did not start before
	// the backup.wlz.li/max-quiesce limit ran out and the run gave the
	// workloads back.
	ItemReasonNotStarted ItemReason = "NotStarted"

	// ItemReasonCloneNotCut is a volume backup whose clone VolSync did not
	// cut before the backup.wlz.li/max-quiesce limit ran out and the run
	// gave the workloads back.
	ItemReasonCloneNotCut ItemReason = "CloneNotCut"

	// ItemReasonNoBackupInReach is a restore that has no backup the run's
	// moment reaches: no snapshot or base backup is at or before the moment,
	// spec.previous reaches past the oldest one, or a synced restore has no
	// quiesced moment that every item shares. The run wrote and deleted
	// nothing.
	ItemReasonNoBackupInReach ItemReason = "NoBackupInReach"

	// ItemReasonClusterArchivesNowhere is a database restore whose Cluster
	// archives its WAL nowhere, so it has no backup to restore.
	ItemReasonClusterArchivesNowhere ItemReason = "ClusterArchivesNowhere"

	// ItemReasonOtherItemFailed is an item the run left alone because
	// another item of the run failed.
	ItemReasonOtherItemFailed ItemReason = "OtherItemFailed"

	// ItemReasonClusterLeftAlone is a database restore whose Cluster opts
	// out of the bootstrap webhook or declares its own bootstrap method.
	// The run cannot turn the next creation of that Cluster into its
	// recovery, so it does not delete the Cluster.
	ItemReasonClusterLeftAlone ItemReason = "ClusterLeftAlone"

	// ItemReasonClusterNotRecovered is a database restore whose Cluster
	// came back without the run's recovery, or whose recovered Cluster was
	// deleted or replaced. The run leaves that Cluster alone.
	ItemReasonClusterNotRecovered ItemReason = "ClusterNotRecovered"

	// ItemReasonClusterVersionUnsupported is a database restore that the
	// run did not start, because the API server serves CloudNativePG's
	// Cluster at another version and no longer at postgresql.cnpg.io/v1,
	// the version the bootstrap webhook's rules name. It is also an item
	// that the run skipped beside such a Cluster item at plan. The run
	// deleted nothing.
	ItemReasonClusterVersionUnsupported ItemReason = "ClusterVersionUnsupported"
)
