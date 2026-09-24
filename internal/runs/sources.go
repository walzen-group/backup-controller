package runs

import (
	"context"
	"errors"
	"fmt"
	"sort"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// pruneIntervalDays is how often VolSync prunes a repository the controller
// writes a source for.
const pruneIntervalDays = int32(1)

// errSourceBusy is a source still completing another run's tag. The run waits
// for it; writing a second tag would leave the first run waiting for a backup
// that is never taken.
var errSourceBusy = errors.New("the ReplicationSource is still completing another run's backup")

// moverCPU is the mover's CPU request: its share on a busy worker, and what the
// scheduler has to find free on the volume's worker.
var moverCPU = resource.MustParse("500m")

// enabledClaims lists the claims in a namespace marked backup.wlz.li/enabled,
// by name.
func enabledClaims(ctx context.Context, c client.Reader, namespace string) ([]corev1.PersistentVolumeClaim, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, claims, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the claims in %s: %w", namespace, err)
	}
	var enabled []corev1.PersistentVolumeClaim
	for _, claim := range claims.Items {
		if backupv1alpha1.Enabled(claim.Annotations) && claim.DeletionTimestamp.IsZero() {
			enabled = append(enabled, claim)
		}
	}
	sort.Slice(enabled, func(i, j int) bool { return enabled[i].Name < enabled[j].Name })
	return enabled, nil
}

// volumeRestoreFor returns the VolumeRestore that describes a claim's
// repository: the one its dataSourceRef names, or for a fixed-name claim,
// which names none, the one carrying the claim's own name.
func volumeRestoreFor(ctx context.Context, c client.Reader, claim *corev1.PersistentVolumeClaim) (*backupv1alpha1.VolumeRestore, error) {
	name := claim.Name
	if ref := claim.Spec.DataSourceRef; ref != nil && ref.Kind == "VolumeRestore" {
		name = ref.Name
	}
	vr := &backupv1alpha1.VolumeRestore{}
	if err := c.Get(ctx, types.NamespacedName{Namespace: claim.Namespace, Name: name}, vr); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("claim %s has no VolumeRestore %s to name its repository", claim.Name, name)
		}
		return nil, fmt.Errorf("get VolumeRestore %s/%s: %w", claim.Namespace, name, err)
	}
	return vr, nil
}

// volumeAffinity returns node affinity that places a pod where the claim's
// volume is, copied from the PersistentVolume's own. zfs-localpv writes it on
// every volume, and it holds whether or not a pod mounts the claim.
func volumeAffinity(ctx context.Context, c client.Reader, claim *corev1.PersistentVolumeClaim) (*corev1.Affinity, error) {
	if claim.Spec.VolumeName == "" || claim.Status.Phase != corev1.ClaimBound {
		return nil, fmt.Errorf("claim %s is not bound to a volume yet", claim.Name)
	}
	pv := &corev1.PersistentVolume{}
	if err := c.Get(ctx, types.NamespacedName{Name: claim.Spec.VolumeName}, pv); err != nil {
		return nil, fmt.Errorf("get PersistentVolume %s: %w", claim.Spec.VolumeName, err)
	}
	if pv.Spec.NodeAffinity == nil || pv.Spec.NodeAffinity.Required == nil {
		return nil, fmt.Errorf("the PersistentVolume %s declares no node affinity to place the mover by", pv.Name)
	}
	return &corev1.Affinity{
		NodeAffinity: &corev1.NodeAffinity{
			RequiredDuringSchedulingIgnoredDuringExecution: pv.Spec.NodeAffinity.Required.DeepCopy(),
		},
	}, nil
}

// retainLast reads the claim's retention annotation.
func retainLast(claim *corev1.PersistentVolumeClaim) (*string, error) {
	value, ok := claim.Annotations[backupv1alpha1.AnnotationRetainLast]
	if !ok || value == "" {
		return nil, fmt.Errorf("claim %s has no %s annotation", claim.Name, backupv1alpha1.AnnotationRetainLast)
	}
	var n int
	if _, err := fmt.Sscanf(value, "%d", &n); err != nil || n < 1 {
		return nil, fmt.Errorf("claim %s has %s %q, which is not a positive count", claim.Name, backupv1alpha1.AnnotationRetainLast, value)
	}
	return &value, nil
}

// ensureSource writes the claim's ReplicationSource with the run's tag, and
// returns it.
//
// The controller owns every field: the repository, the cache class and the
// mover's security context come from the claim's VolumeRestore, the retention
// from the claim's annotation, and the node from the claim's volume. The
// source is owned by the claim, so a claim deleted for a restore takes its
// source with it and the next run writes a new one.
//
// A source of the same name that this controller did not write is left
// alone: something else declares it, and writing over it would start a fight
// that thing wins on its next reconcile.
func ensureSource(ctx context.Context, c client.Client, reader client.Reader, claim *corev1.PersistentVolumeClaim, tag string) (*volsyncv1alpha1.ReplicationSource, error) {
	vr, err := volumeRestoreFor(ctx, reader, claim)
	if err != nil {
		return nil, err
	}
	affinity, err := volumeAffinity(ctx, reader, claim)
	if err != nil {
		return nil, err
	}
	last, err := retainLast(claim)
	if err != nil {
		return nil, err
	}

	source := &volsyncv1alpha1.ReplicationSource{
		ObjectMeta: metav1.ObjectMeta{Name: claim.Name, Namespace: claim.Namespace},
	}
	existing := &volsyncv1alpha1.ReplicationSource{}
	err = reader.Get(ctx, client.ObjectKeyFromObject(source), existing)
	switch {
	case err == nil && existing.Labels[backupv1alpha1.LabelManagedBy] != backupv1alpha1.ManagedByValue:
		return nil, fmt.Errorf("the ReplicationSource %s exists and was not written by backup-controller; remove it so the controller can write its own", claim.Name)
	case err == nil && busy(existing) && manualTag(existing) != tag:
		return existing, errSourceBusy
	case err != nil && !apierrors.IsNotFound(err):
		return nil, fmt.Errorf("get ReplicationSource %s/%s: %w", claim.Namespace, claim.Name, err)
	}

	_, err = controllerutil.CreateOrUpdate(ctx, c, source, func() error {
		if source.Labels == nil {
			source.Labels = map[string]string{}
		}
		source.Labels[backupv1alpha1.LabelManagedBy] = backupv1alpha1.ManagedByValue
		if err := controllerutil.SetControllerReference(claim, source, c.Scheme()); err != nil {
			return err
		}
		source.Spec = volsyncv1alpha1.ReplicationSourceSpec{
			SourcePVC: claim.Name,
			Trigger:   &volsyncv1alpha1.ReplicationSourceTriggerSpec{Manual: tag},
			Restic: &volsyncv1alpha1.ReplicationSourceResticSpec{
				ReplicationSourceVolumeOptions: volsyncv1alpha1.ReplicationSourceVolumeOptions{
					CopyMethod: volsyncv1alpha1.CopyMethodClone,
					// The clone and the cache are both rebuilt on every run,
					// so both come from the class the VolumeRestore names for
					// its cache, which reclaims Delete.
					StorageClassName: vr.Spec.CacheStorageClassName,
				},
				Repository:            vr.Spec.Repository,
				PruneIntervalDays:     new(pruneIntervalDays),
				Retain:                &volsyncv1alpha1.ResticRetainPolicy{Last: last},
				CacheCapacity:         vr.Spec.CacheCapacity,
				CacheStorageClassName: vr.Spec.CacheStorageClassName,
				MoverConfig: volsyncv1alpha1.MoverConfig{
					MoverSecurityContext: vr.Spec.MoverSecurityContext,
					MoverAffinity:        affinity,
					MoverResources: &corev1.ResourceRequirements{
						Requests: corev1.ResourceList{corev1.ResourceCPU: moverCPU},
					},
				},
			},
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("write ReplicationSource %s/%s: %w", claim.Namespace, claim.Name, err)
	}
	return source, nil
}
