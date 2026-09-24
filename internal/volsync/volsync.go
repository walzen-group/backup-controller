package volsync

import (
	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// Trigger returns the manual sync value for a claim.
func Trigger(claim *corev1.PersistentVolumeClaim) string {
	if claim == nil {
		return ""
	}
	return string(claim.UID)
}

// RestoreAsOf is the moment a claim's restore goes back to: the claim's own
// backup.wlz.li/restore-as-of annotation, else the VolumeRestore's
// restoreAsOf, else nil for the newest snapshot.
func RestoreAsOf(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) *string {
	if claim != nil {
		if value, ok := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; ok {
			return &value
		}
	}
	return vr.Spec.RestoreAsOf
}

// New builds a ReplicationDestination that restores into the prime claim.
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
				// VolSync resolves the repository Secret in the
				// destination's own namespace, which is the controller's, so
				// this names the copy the controller put there rather than
				// vr.Spec.Repository, which names the original in the app's
				// namespace.
				Repository:  SecretCopyName(types.UID(trigger)),
				RestoreAsOf: RestoreAsOf(vr, claim),
				// The mover provisions a metadata cache claim for every
				// restore. Left unset the class is the cluster's default, and
				// that class's reclaim policy decides whether the cache
				// dataset outlives the restore that made it.
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

// Complete reports whether VolSync recorded completion for the triggered sync.
func Complete(rd *volsyncv1alpha1.ReplicationDestination, trigger string) bool {
	return rd != nil && rd.Status != nil && rd.Status.LastManualSync == trigger
}

// Failure reports a failed mover and its recorded logs.
func Failure(rd *volsyncv1alpha1.ReplicationDestination) (reason string, failed bool) {
	if rd == nil || rd.Status == nil || rd.Status.LatestMoverStatus == nil {
		return "", false
	}
	mover := rd.Status.LatestMoverStatus
	return mover.Logs, mover.Result == volsyncv1alpha1.MoverResultFailed
}
