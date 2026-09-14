package runs

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// restoreSettings are the values a restore needs that the run itself does not
// state, read off the claim's own VolumeRestore so none of them is retyped.
type restoreSettings struct {
	// Secret is the restic repository Secret, in the run's namespace.
	Secret string

	// CacheStorageClassName is the class the mover's metadata cache comes from.
	// Left empty the cluster's default class provisions it, and a default that
	// reclaims Retain leaves a dataset behind after every restore.
	CacheStorageClassName *string

	// MoverPodLabels put the mover in the cluster's backup queue.
	MoverPodLabels map[string]string

	// MoverSecurityContext matches the mover to the ownership of the files it
	// writes, for an app whose image runs as a user of its own.
	MoverSecurityContext *corev1.PodSecurityContext

	// Capacity and StorageClassName describe the source claim, for sizing the
	// scratch claim an Into restore creates.
	Capacity         *resource.Quantity
	StorageClassName *string
}

// repositoryFor resolves where a run reads from and how its mover should run.
//
// Naming a claim is the ordinary case and states nothing twice: the claim's
// dataSourceRef names its VolumeRestore, and that object already carries the
// repository Secret, the cache class and the queue label. Naming a repository
// directly covers a claim that is not populated, which is every fixed-name
// claim, and a restore from a repository no claim here owns.
func (r *RestoreRunReconciler) repositoryFor(ctx context.Context, run *backupv1alpha1.RestoreRun) (restoreSettings, error) {
	switch {
	case run.Spec.Claim == "" && run.Spec.Repository == "":
		return restoreSettings{}, fmt.Errorf("one of spec.claim and spec.repository is required")
	case run.Spec.Claim == "" && run.Spec.Into == "":
		return restoreSettings{}, fmt.Errorf("spec.into is required when spec.repository names the source, because there is no claim to restore in place")
	}

	settings := restoreSettings{Secret: run.Spec.Repository, MoverSecurityContext: run.Spec.MoverSecurityContext}
	if run.Spec.Claim == "" {
		return settings, nil
	}

	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Claim}
	if err := r.Get(ctx, key, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return settings, fmt.Errorf("no PersistentVolumeClaim %s in this namespace", run.Spec.Claim)
		}
		return settings, fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	}
	settings.StorageClassName = claim.Spec.StorageClassName
	if request, ok := claim.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		settings.Capacity = &request
	}

	// A fixed-name claim names no VolumeRestore, because it binds its volume
	// before anything could fill it. Such a claim restores in place like any
	// other, and its run has to say which repository.
	source := claim.Spec.DataSourceRef
	if source == nil || source.Kind != "VolumeRestore" {
		if settings.Secret == "" {
			return settings, fmt.Errorf("claim %s names no VolumeRestore, so spec.repository has to say which repository to restore from", run.Spec.Claim)
		}
		return settings, nil
	}

	vr := &backupv1alpha1.VolumeRestore{}
	vrKey := types.NamespacedName{Namespace: run.Namespace, Name: source.Name}
	if err := r.Get(ctx, vrKey, vr); err != nil {
		if apierrors.IsNotFound(err) {
			return settings, fmt.Errorf("claim %s names VolumeRestore %s, which does not exist", run.Spec.Claim, source.Name)
		}
		return settings, fmt.Errorf("get VolumeRestore %s: %w", vrKey, err)
	}

	if settings.Secret == "" {
		settings.Secret = vr.Spec.Repository
	}
	settings.CacheStorageClassName = vr.Spec.CacheStorageClassName
	settings.MoverPodLabels = vr.Spec.MoverLabels()
	if settings.MoverSecurityContext == nil {
		settings.MoverSecurityContext = vr.Spec.MoverSecurityContext
	}
	return settings, nil
}

// directDestination builds the in-place restore: the mover mounts the claim the
// app uses and writes the selected snapshot into it.
//
// enableFileDeletion removes files the snapshot lacks, so the volume ends up
// holding the snapshot rather than a merge of the two. cleanupCachePVC drops
// the mover's cache claim when the run ends. The trigger is the run's UID, so a
// controller that restarts mid-restore recognises its own work.
func directDestination(run *backupv1alpha1.RestoreRun, settings restoreSettings, name string) *volsyncv1alpha1.ReplicationDestination {
	return &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: string(run.UID)},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod:     volsyncv1alpha1.CopyMethodDirect,
					DestinationPVC: &run.Spec.Claim,
				},
				Repository:            settings.Secret,
				RestoreAsOf:           run.Spec.RestoreAsOf,
				Previous:              run.Spec.Previous,
				CacheStorageClassName: settings.CacheStorageClassName,
				EnableFileDeletion:    true,
				CleanupCachePVC:       true,
				MoverConfig: volsyncv1alpha1.MoverConfig{
					MoverPodLabels:       settings.MoverPodLabels,
					MoverSecurityContext: settings.MoverSecurityContext,
				},
			},
		},
	}
}

// pointInTimeRestore builds the VolumeRestore an Into restore fills its scratch
// claim from. It is an ordinary VolumeRestore carrying a point in time, so the
// populator path does the work and nothing here duplicates it.
func pointInTimeRestore(run *backupv1alpha1.RestoreRun, settings restoreSettings) *backupv1alpha1.VolumeRestore {
	labels := map[string]backupv1alpha1.MoverPodLabelValue{}
	for key, value := range settings.MoverPodLabels {
		labels[key] = backupv1alpha1.MoverPodLabelValue(value)
	}
	if len(labels) == 0 {
		labels = nil
	}

	return &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{
			Name:      run.Spec.Into,
			Namespace: run.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind("RestoreRun")),
			},
		},
		Spec: backupv1alpha1.VolumeRestoreSpec{
			Repository:            settings.Secret,
			RestoreAsOf:           run.Spec.RestoreAsOf,
			CacheStorageClassName: settings.CacheStorageClassName,
			MoverPodLabels:        labels,
			MoverSecurityContext:  settings.MoverSecurityContext,
		},
	}
}

// scratchClaim builds the claim an Into restore fills.
//
// It takes the source claim's size and class, so the copy is provisioned the
// way the original was. It is an ordinary dynamic claim: deleting it takes its
// dataset with it, which is what makes a scratch copy cheap to throw away.
func scratchClaim(run *backupv1alpha1.RestoreRun, settings restoreSettings, restore string) *corev1.PersistentVolumeClaim {
	size := settings.Capacity
	if run.Spec.IntoSize != nil {
		size = run.Spec.IntoSize
	}

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      run.Spec.Into,
			Namespace: run.Namespace,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(run, backupv1alpha1.GroupVersion.WithKind("RestoreRun")),
			},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: settings.StorageClassName,
			DataSourceRef: &corev1.TypedObjectReference{
				APIGroup: &backupv1alpha1.GroupVersion.Group,
				Kind:     "VolumeRestore",
				Name:     restore,
			},
		},
	}
	if size != nil {
		claim.Spec.Resources = corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: *size},
		}
	}
	return claim
}
