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
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// selectedNodeAnnotation is what a WaitForFirstConsumer class provisions
// against, and what the populator library waits for before it fills a claim.
const selectedNodeAnnotation = "volume.kubernetes.io/selected-node"

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

	// SelectedNode is the worker the source claim's volume is on, carried onto
	// the scratch claim so a WaitForFirstConsumer class provisions it without a
	// pod to schedule.
	SelectedNode string
}

// repositoryFor resolves where a restore reads from and how its mover runs.
//
// Naming a claim is the ordinary case and states nothing twice: the claim's
// dataSourceRef names its VolumeRestore, and that object already carries the
// repository Secret, the cache class and the queue label. Naming a repository
// directly covers a restore from a repository no claim here owns.
func repositoryFor(ctx context.Context, c client.Reader, namespace, claimName, repository string, moverContext *corev1.PodSecurityContext) (restoreSettings, error) {
	settings := restoreSettings{Secret: repository, MoverSecurityContext: moverContext}
	if claimName == "" {
		if repository == "" {
			return settings, fmt.Errorf("one of spec.claim and spec.repository is required")
		}
		return settings, nil
	}

	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: namespace, Name: claimName}
	if err := c.Get(ctx, key, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return settings, fmt.Errorf("no PersistentVolumeClaim %s in this namespace", claimName)
		}
		return settings, fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	}
	settings.StorageClassName = claim.Spec.StorageClassName
	settings.SelectedNode = claim.Annotations[selectedNodeAnnotation]
	if request, ok := claim.Spec.Resources.Requests[corev1.ResourceStorage]; ok {
		settings.Capacity = &request
	}

	// A fixed-name claim names no VolumeRestore in its dataSourceRef, because it
	// binds its volume before anything could fill it; the VolumeRestore of the
	// same name describes its repository. A run that names the repository
	// itself needs neither.
	vr, err := volumeRestoreFor(ctx, c, claim)
	if err != nil {
		if settings.Secret != "" {
			return settings, nil
		}
		return settings, err
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
func directDestination(run *backupv1alpha1.RestoreRun, claim string, settings restoreSettings, name string) *volsyncv1alpha1.ReplicationDestination {
	return &volsyncv1alpha1.ReplicationDestination{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: run.Namespace},
		Spec: volsyncv1alpha1.ReplicationDestinationSpec{
			Trigger: &volsyncv1alpha1.ReplicationDestinationTriggerSpec{Manual: string(run.UID)},
			Restic: &volsyncv1alpha1.ReplicationDestinationResticSpec{
				ReplicationDestinationVolumeOptions: volsyncv1alpha1.ReplicationDestinationVolumeOptions{
					CopyMethod:     volsyncv1alpha1.CopyMethodDirect,
					DestinationPVC: &claim,
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
//
// It also takes the source claim's selected node, and without that it is never
// filled at all. On a WaitForFirstConsumer class the populator library waits
// for volume.kubernetes.io/selected-node before it creates anything, and that
// annotation is written by the scheduler when a pod using the claim is
// scheduled. A scratch claim has no pod, so nothing would ever write it and the
// claim would stay Pending for as long as it existed. Every class on the walzen
// cluster binds WaitForFirstConsumer.
//
// Copying the source's node is also the right answer rather than a way around
// the problem: the copy is made to be compared against the original, and both
// datasets belong on the pool that already holds one of them.
func scratchClaim(run *backupv1alpha1.RestoreRun, settings restoreSettings, restore string) *corev1.PersistentVolumeClaim {
	size := settings.Capacity
	if run.Spec.IntoSize != nil {
		size = run.Spec.IntoSize
	}

	annotations := map[string]string{}
	if settings.SelectedNode != "" {
		annotations[selectedNodeAnnotation] = settings.SelectedNode
	}

	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:        run.Spec.Into,
			Namespace:   run.Namespace,
			Annotations: annotations,
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
