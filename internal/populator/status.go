package populator

import (
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// find returns the entry of one claim in the VolumeRestore's status.claims.
//
// Parameters:
//   - vr is the VolumeRestore whose status holds the entry.
//   - uid is the UID of the claim.
//
// It returns a pointer into the status, so a change to the entry changes the
// status in memory. It returns nil when the claim has no entry.
func find(vr *backupv1alpha1.VolumeRestore, uid types.UID) *backupv1alpha1.ClaimRestoreStatus {
	for i := range vr.Status.Claims {
		if vr.Status.Claims[i].UID == uid {
			return &vr.Status.Claims[i]
		}
	}
	return nil
}

// entry returns the entry of one claim in the VolumeRestore's status.claims,
// and adds the entry when there is none.
//
// Parameters:
//   - vr is the VolumeRestore whose status holds the entry.
//   - claim is the claim. The entry gets its name and UID.
//
// It returns a pointer into the status. The entry has the claim's current
// name, and a start time of now when it had no start time. The change is in
// memory only, and the caller writes the status.
func entry(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) *backupv1alpha1.ClaimRestoreStatus {
	found := find(vr, claim.UID)
	if found == nil {
		vr.Status.Claims = append(vr.Status.Claims, backupv1alpha1.ClaimRestoreStatus{UID: claim.UID})
		found = &vr.Status.Claims[len(vr.Status.Claims)-1]
	}
	found.Name = claim.Name
	if found.StartedAt == nil {
		now := metav1.Now()
		found.StartedAt = &now
	}
	return found
}

// setSnapshot records which snapshot fills a claim on the claim's entry.
//
// Parameters:
//   - status is the claim's entry, from entry.
//   - snapshot is the chosen snapshot.
//   - found is false when the repository held no snapshot. Then the entry
//     records no snapshot, and the claim binds empty.
func setSnapshot(status *backupv1alpha1.ClaimRestoreStatus, snapshot restic.Snapshot, found bool) {
	if !found {
		status.Snapshot, status.SnapshotID, status.SnapshotTime = "", "", nil
		return
	}
	at := metav1.NewTime(snapshot.Time)
	status.Snapshot, status.SnapshotID, status.SnapshotTime = snapshot.ShortID(), snapshot.ID, &at
}

// decided returns the entry of a claim whose snapshot choice is made and
// whose restore has not failed.
//
// Parameters:
//   - vr is the VolumeRestore whose status holds the entry.
//   - uid is the UID of the claim.
//
// It returns the entry and true when the entry's phase is Restoring. A
// Restoring entry without a snapshot ID is a claim that binds empty. It
// returns false when the claim has no entry or its restore failed, so
// Populate makes the choice again.
func decided(vr *backupv1alpha1.VolumeRestore, uid types.UID) (backupv1alpha1.ClaimRestoreStatus, bool) {
	found := find(vr, uid)
	if found == nil || found.Phase != backupv1alpha1.RestorePhaseRestoring {
		return backupv1alpha1.ClaimRestoreStatus{}, false
	}
	return *found, true
}

// settle sets Ready from the claim entries, after a claim's restore ended.
//
// Parameters:
//   - vr is the VolumeRestore. The change is in memory only.
//
// Ready is True with reason Restored when no entry is Restoring or Failed.
// When an entry is Restoring and none is Failed, Ready is False with reason
// Restoring. When an entry is Failed, Ready stays as the failure set it, so
// its message keeps the cause.
func settle(vr *backupv1alpha1.VolumeRestore) {
	restoring, failed := 0, 0
	for _, status := range vr.Status.Claims {
		switch status.Phase {
		case backupv1alpha1.RestorePhaseRestoring:
			restoring++
		case backupv1alpha1.RestorePhaseFailed:
			failed++
		}
	}
	switch {
	case failed > 0:
		return
	case restoring > 0:
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoring,
			fmt.Sprintf("%d claim(s) still restoring", restoring))
	default:
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionTrue, backupv1alpha1.ReasonRestored,
			"no claim is being restored")
	}
}
