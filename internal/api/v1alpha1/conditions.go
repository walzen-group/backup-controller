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

	// ReasonSourceBusy reports a run waiting for another run's backup of the
	// same volume to finish.
	ReasonSourceBusy = "SourceBusy"

	// ReasonNoBackupInReach reports a restore whose moment is older than every
	// backup of one of its items. A RestoreRun fails with it before it deletes
	// or overwrites anything. A VolumeRestore reports it for a claim whose
	// backup.wlz.li/restore-as-of is older than every snapshot.
	ReasonNoBackupInReach = "NoBackupInReach"

	// ReasonRecreate reports a restore waiting for the Clusters it deleted to
	// be created again, by Flux or by a terragrunt apply.
	ReasonRecreate = "WaitingForRecreate"

	// ReasonShutdown reports a restore waiting for a deleted Cluster's instance
	// pods and PVCs to be gone. Until they are, the app stays stopped and any
	// Kustomization the run suspended stays suspended.
	ReasonShutdown = "WaitingForShutdown"

	// ReasonClaimInUse reports an in-place restore waiting for a pod to stop
	// mounting the claim the restore writes into. The run waits because a
	// second writer on the same filesystem would corrupt the restored volume.
	ReasonClaimInUse = "ClaimInUse"

	// ReasonTimedOut reports a RestoreRun that had not finished by the end of
	// its spec.timeout, or an into restore whose claim had not bound by then.
	// The run removes its ReplicationDestinations and gives back any workloads
	// it stopped before it reports this. A BackupRun that runs out of time
	// reports ReasonFailed.
	ReasonTimedOut = "TimedOut"

	// ReasonInvalid reports a spec the controller will not act on. The
	// condition's message names the field at fault.
	ReasonInvalid = "Invalid"
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
