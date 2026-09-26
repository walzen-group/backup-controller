// This file holds the Ready condition and the reasons it carries, for
// VolumeRestore, BackupRun and RestoreRun. internal/populator reports them on
// a VolumeRestore, and internal/runs reports them on the two run kinds.

package v1alpha1

import (
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ConditionReady is the condition type every kind in this package reports. On
// a VolumeRestore it is False while a claim is being populated from it, and
// True when none is. On a run it is False while the run works, and True once
// the run has finished.
const ConditionReady = "Ready"

// The reasons a VolumeRestore's Ready condition carries.
const (
	// ReasonRestoring reports a claim that is being populated now.
	ReasonRestoring = "Restoring"

	// ReasonRestoreFailed reports a claim whose mover failed. The claim's
	// volume stays unfilled until something about the restore changes.
	ReasonRestoreFailed = "RestoreFailed"

	// ReasonRestored reports that nothing is being populated from this object.
	ReasonRestored = "Restored"
)

// The reasons a BackupRun or a RestoreRun carries on its Ready condition. A
// run is Ready False while it works, and Ready True once it has finished,
// whether it succeeded or failed. A Flux Kustomization with wait: true
// therefore waits for a run the way it waits for a Job.
const (
	// ReasonRunning reports work under way.
	ReasonRunning = "Running"

	// ReasonSucceeded reports work that finished.
	ReasonSucceeded = "Succeeded"

	// ReasonFailed reports a run that gave up. The run removes what it created
	// and gives back any workloads it stopped before it reports this.
	ReasonFailed = "Failed"

	// ReasonQueued reports a run waiting for Kueue to admit it.
	ReasonQueued = "Queued"

	// ReasonSourceBusy reports a run waiting for another run: a backup or
	// restore of the same claim or repository, or a run that has stopped this
	// namespace's workloads and has not given them back yet.
	ReasonSourceBusy = "SourceBusy"

	// ReasonNoBackupInReach reports a restore whose moment is older than every
	// backup of one of its items. A RestoreRun fails with it before it deletes
	// or overwrites anything. A VolumeRestore reports it for a claim whose
	// backup.wlz.li/restore-as-of is older than every snapshot.
	ReasonNoBackupInReach = "NoBackupInReach"

	// ReasonRecreate reports a restore waiting for the Clusters it deleted to
	// be created again, by Flux or by a terragrunt apply.
	ReasonRecreate = "WaitingForRecreate"

	// ReasonShutdown reports a restore waiting for something it deleted to be
	// gone: a deleted Cluster's instance pods and PVCs, the pod of a mover it
	// stopped, or the claim whose VolumeRestore it holds. Until they are gone
	// the app stays stopped, any Kustomization the run suspended stays
	// suspended, and the run holds its Leases and its finalizer.
	ReasonShutdown = "WaitingForShutdown"

	// ReasonClaimInUse reports an in-place restore waiting for a pod to stop
	// mounting the claim the restore writes into. The run waits because a
	// second writer on the same filesystem would corrupt the restored volume.
	ReasonClaimInUse = "ClaimInUse"

	// ReasonTimedOut reports a RestoreRun that had not finished by the end of
	// its spec.timeout, or an into restore whose claim had not bound by then.
	// A run whose checks never passed counts its timeout from its creation.
	// The run removes its ReplicationDestinations and gives back any workloads
	// it stopped before it reports this. A BackupRun that runs out of time
	// reports ReasonFailed.
	ReasonTimedOut = "TimedOut"

	// ReasonInvalid reports a spec the controller will not act on. The
	// condition's message names the field at fault.
	ReasonInvalid = "Invalid"

	// ReasonRetrying reports a RestoreRun whose checks failed with an error it
	// tries again, such as a repository it can't open. The condition's
	// message holds the error. A run still retrying once spec.timeout has
	// passed since it was created ends with reason TimedOut. It also reports
	// a BackupRun with an item it failed to start and tries again, such as a
	// Backup that a CloudNativePG webhook it cannot reach refuses; the message
	// names each such item and its error.
	ReasonRetrying = "Retrying"

	// ReasonCRDOutdated reports a run the controller refused before it
	// changed anything, because the installed CustomResourceDefinition of the
	// run's kind lacks a field the controller writes. The API server drops
	// such a field from every write, so the run could lose track of what it
	// did, such as a quiesced app it still has to start again. It also
	// reports a run the controller could not check, because it may not read
	// the CRD. The message names the missing field or permission. Applying
	// the CRDs of the controller's release fixes it; Helm does not upgrade
	// CRDs on its own.
	ReasonCRDOutdated = "CRDOutdated"

	// ReasonUpgraded reports an unfinished run that an older release of the
	// controller planned (see status.plannedBy). This release does not
	// continue such a run: it ends it as Failed, the way it ends any failed
	// run. It gives back the workloads the run stopped, resumes the
	// Kustomizations it suspended, deletes the ReplicationDestinations it
	// created, and releases its Leases and its Kueue Workload. The message
	// says to create a new run, and names a Cluster the run deleted that has
	// not been recovered. The run records a Warning event.
	ReasonUpgraded = "Upgraded"

	// ReasonRestartFailed reports a BackupRun or RestoreRun whose app is
	// still down, or whose state the run could not read, because the run
	// could not give a stopped workload its replicas back, resume a
	// Kustomization it suspended, delete its ReplicationDestinations or
	// release its Leases. A BackupRun reports it while it is still backing
	// up, as soon as the restart after the clones are cut fails, and a run
	// reports it when it is ending or being deleted. The run stays unfinished
	// and tries again on every reconcile until it can, because finishing
	// would leave the app down with no record of the replicas it is owed.
	// While a namespace run is unfinished, the namespace's schedule starts no
	// new one. The message names what the run could not do and the error, and
	// lists each workload with the replica count it is owed and each
	// Kustomization to resume. Fixing the cause lets the run go on by
	// itself, and so does scaling those workloads and resuming those
	// Kustomizations by hand: the run skips what is already back. A run that
	// is still working must not be deleted. Only for a run that is ending
	// or being deleted does the message also say, after the scaling step,
	// that a person can delete the run and remove its
	// backup.wlz.li/run-cleanup finalizer. The run records a Warning event
	// when it first reports this.
	ReasonRestartFailed = "RestartFailed"

	// ReasonReleaseFailed reports a BackupRun or RestoreRun that has given
	// the app back and is ending, or being deleted, but could not release
	// its Leases on its claims and repositories, delete its Kueue Workload or
	// ReplicationDestinations, or release the VolumeRestore of an into
	// restore. The run stays unfinished and tries again on every reconcile
	// until it can. The message names what the run could not release and the
	// error, and says that a person can delete it by hand, after which the
	// run finishes by itself. The run records a Warning event when it first
	// reports this.
	ReasonReleaseFailed = "ReleaseFailed"
)

// SetReady sets or replaces the Ready condition in a status's condition list.
// Every reconciler in this project writes Ready through it, so no caller
// spells out the condition type.
//
// Parameters:
//   - conditions is the status's condition list, which SetReady changes in
//     place.
//   - generation is the object's metadata.generation as the caller read it.
//     It becomes the condition's observedGeneration.
//   - status, reason and message become the condition's fields of the same
//     names.
//
// meta.SetStatusCondition keeps the old lastTransitionTime unless the status
// changes, so a run that moves from one waiting reason to another keeps the
// time it first went False.
func SetReady(conditions *[]metav1.Condition, generation int64, status metav1.ConditionStatus, reason, message string) {
	meta.SetStatusCondition(conditions, metav1.Condition{
		Type:               ConditionReady,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: generation,
	})
}
