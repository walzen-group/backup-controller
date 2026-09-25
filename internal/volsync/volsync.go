// Package volsync builds the VolSync objects that restore one claim, a
// ReplicationDestination and a copy of the repository Secret, and reads the
// destination's status.
package volsync

import (
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Trigger returns the manual trigger value for a claim's ReplicationDestination,
// which is the claim's UID, or "" for a nil claim. VolSync copies the value to
// status.lastManualSync when the sync finishes, and Complete compares the two.
// The destination is also named after it, as restore-<UID>.
func Trigger(claim *corev1.PersistentVolumeClaim) string {
	if claim == nil {
		return ""
	}
	return string(claim.UID)
}

// RestoreAsOf returns the moment a claim's restore goes back to. The claim's
// own backup.wlz.li/restore-as-of annotation comes first, then the
// VolumeRestore's spec.restoreAsOf. When neither is set, it returns nil, and
// VolSync restores the newest snapshot.
func RestoreAsOf(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) *string {
	if claim != nil {
		if value, ok := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; ok {
			return &value
		}
	}
	return vr.Spec.RestoreAsOf
}

// New builds the ReplicationDestination that restores one claim's volume from
// its restic repository.
//
// Parameters:
//   - vr is the VolumeRestore that the claim names as its data source. The
//     destination takes the restoreAsOf, the cache storage class and
//     capacity, the mover pod labels and the mover security context from its
//     spec.
//   - claim is the claim being restored. Its UID gives the destination its
//     name, restore-<UID>, and its manual trigger. Its restore-as-of
//     annotation can pin the restore to a moment (see RestoreAsOf).
//   - primeClaim is the name of the prime claim that the volume populator
//     library created. VolSync writes the snapshot's files straight into it,
//     with copyMethod Direct.
//   - namespace is the controller namespace, where the destination and the
//     prime claim live.
//
// The destination names the repository Secret by SecretCopyName, so the copy
// from SecretCopy has to exist in the controller namespace for the restore to
// run.
func New(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim, primeClaim, namespace string) *volsyncv1alpha1.ReplicationDestination {
	trigger := Trigger(claim)
	return &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "restore-" + trigger,
			Namespace: namespace,
		},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: trigger},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod:     volsyncv1alpha1.CopyMethodDirect,
					DestinationPVC: new(primeClaim),
				},
				// VolSync looks up the repository Secret in the destination's
				// own namespace, which is the controller's. So this names the
				// copy the controller made there. vr.Spec.Repository names the
				// original, which exists only in the app's namespace.
				Repository:  SecretCopyName(types.UID(trigger)),
				RestoreAsOf: RestoreAsOf(vr, claim),
				// The mover provisions a metadata cache claim for every
				// restore. When the class is unset, the cluster's default
				// class provisions it, and that class's reclaim policy decides
				// whether the cache dataset outlives the restore that made it.
				CacheStorageClassName: vr.Spec.CacheStorageClassName,
				CacheCapacity:         vr.Spec.CacheCapacity,
				MoverConfig: volsyncv1alpha1.MoverConfig{
					MoverPodLabels:       vr.Spec.MoverLabels(),
					MoverSecurityContext: vr.Spec.MoverSecurityContext,
				},
			},
		},
	}
}

// Complete reports whether VolSync has finished the sync started with the
// manual trigger value in trigger. VolSync records that value in the
// destination's status.lastManualSync when the sync finishes. Complete returns
// false for a nil destination or one with no status yet.
func Complete(rd *volsyncv1alpha1.ReplicationDestination, trigger string) bool {
	return rd != nil && rd.Status != nil && rd.Status.LastManualSync == trigger
}

// Failure reads the destination's status.latestMoverStatus. It returns the
// mover logs VolSync recorded there, and true when that mover run's result is
// Failed. It returns "" and false when there's no mover status yet.
func Failure(rd *volsyncv1alpha1.ReplicationDestination) (reason string, failed bool) {
	if rd == nil || rd.Status == nil || rd.Status.LatestMoverStatus == nil {
		return "", false
	}
	mover := rd.Status.LatestMoverStatus
	return mover.Logs, mover.Result == volsyncv1alpha1.MoverResultFailed
}
