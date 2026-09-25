package runs

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// selectedNodeAnnotation is the claim annotation that names the node a claim's
// volume goes on. A StorageClass that binds WaitForFirstConsumer provisions the
// volume on that node, and the populator library waits for the annotation
// before it fills a claim.
const selectedNodeAnnotation = "volume.kubernetes.io/selected-node"

// restoreSettings holds the values a restore needs that the RestoreRun
// doesn't state itself. repositoryFor reads them from the claim and its
// VolumeRestore, so none of them has to be typed a second time.
type restoreSettings struct {
	// Secret is the name of the restic repository Secret, in the run's
	// namespace.
	Secret string

	// CacheStorageClassName is the StorageClass the mover's metadata cache is
	// provisioned from. When it is empty, the cluster's default class
	// provisions the cache, and a default class that reclaims with Retain
	// leaves a dataset behind after every restore.
	CacheStorageClassName *string

	// MoverPodLabels are the labels put on the mover pod. They place the mover
	// in the cluster's backup queue.
	MoverPodLabels map[string]string

	// MoverSecurityContext sets the user the mover runs as. An app whose image
	// runs as a user of its own needs the mover to write files with that
	// ownership.
	MoverSecurityContext *corev1.PodSecurityContext

	// Capacity and StorageClassName are the source claim's storage request
	// and class. An Into restore sizes and provisions its scratch claim with
	// them.
	Capacity         *resource.Quantity
	StorageClassName *string

	// SelectedNode is the worker that holds the source claim's volume. It is
	// copied onto the scratch claim, so a WaitForFirstConsumer class
	// provisions the scratch claim without a pod to schedule.
	SelectedNode string
}

// repositoryFor works out which restic repository a restore reads from and
// how its mover runs.
//
// Parameters:
//   - c reads the claim and its VolumeRestore.
//   - namespace is the RestoreRun's namespace.
//   - claimName is the claim whose backups the run restores, from the run's
//     spec.claim or from one of its items. It may be empty when repository
//     is set.
//   - repository is the run's spec.repository, the name of a restic
//     repository Secret. When it is set, it takes precedence over the Secret
//     the claim's VolumeRestore names.
//   - moverContext is the run's spec.moverSecurityContext. When it is set, it
//     takes precedence over the VolumeRestore's.
//
// It returns an error when claimName and repository are both empty, or when
// the claim doesn't exist. When the claim has no VolumeRestore, it returns an
// error if repository is empty, and otherwise the settings it could read from
// the claim alone.
//
// Naming a claim is the ordinary case, and it states nothing twice. The
// claim's dataSourceRef names its VolumeRestore, and that object already
// carries the repository Secret, the cache class and the queue label. Naming
// a repository directly covers a restore from a repository that no claim in
// the namespace backs up to.
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

	// A fixed-name claim binds its volume before anything could fill it, so
	// its dataSourceRef names no VolumeRestore. The VolumeRestore with the
	// claim's own name describes its repository. A run that names the
	// repository itself can go on without any VolumeRestore.
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

// directDestination builds the ReplicationDestination for an in-place
// restore. Its mover mounts the claim the app uses and writes the chosen
// snapshot into it.
//
// Parameters:
//   - run is the RestoreRun. Its spec.restoreAsOf and spec.previous choose
//     the snapshot, and its UID becomes the manual trigger.
//   - claim is the name of the claim to write into.
//   - settings are the repository and mover settings from repositoryFor.
//   - name is the name to give the ReplicationDestination.
//
// enableFileDeletion makes the mover remove the files the snapshot lacks, so
// the volume ends up holding exactly the snapshot. cleanupCachePVC drops the
// mover's cache claim when the run ends. The manual trigger is the run's UID,
// so a controller that restarts mid-restore recognises its own work.
//
// For a synced run, one whose status.syncedTo is set, the mover gets that
// time as restoreAsOf and gets no previous. The quiesced snapshot carries
// exactly that time, and the mover picks the newest snapshot at or before
// restoreAsOf, so it picks the quiesced one.
func directDestination(run *backupv1alpha1.RestoreRun, claim string, settings restoreSettings, name string) *volsyncv1alpha1.ReplicationDestination {
	restoreAsOf, previous := run.Spec.RestoreAsOf, run.Spec.Previous
	if run.Status.SyncedTo != nil {
		moment := run.Status.SyncedTo.UTC().Format(time.RFC3339)
		restoreAsOf, previous = &moment, nil
	}
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
				RestoreAsOf:           restoreAsOf,
				Previous:              previous,
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

// pointInTimeRestore builds the VolumeRestore that an Into restore fills its
// scratch claim from. It is named after the run's spec.into and carries the
// run's spec.restoreAsOf. Because it is an ordinary VolumeRestore, the volume
// populator does the restore, and none of that work is repeated here. The run
// is its controller owner.
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

// scratchClaim builds the claim that an Into restore fills.
//
// Parameters:
//   - run is the RestoreRun. The claim takes its name from spec.into, and
//     spec.intoSize, when set, replaces the source claim's size.
//   - settings are the source claim's settings from repositoryFor.
//   - restore is the name of the VolumeRestore that the claim's
//     dataSourceRef names, which is the one pointInTimeRestore builds.
//
// The claim takes the source claim's size and class, so the copy is
// provisioned the way the original was. It is an ordinary dynamic claim.
// Deleting it deletes its dataset too, which makes a scratch copy cheap to
// throw away. The run is its controller owner.
//
// It also takes the source claim's selected node, and without that it is
// never filled at all. On a WaitForFirstConsumer class the populator library
// waits for volume.kubernetes.io/selected-node before it creates anything,
// and the scheduler writes that annotation when it schedules a pod that uses
// the claim. A scratch claim has no pod, so nothing would ever write it, and
// the claim would stay Pending for as long as it existed. Every class on the
// walzen cluster binds WaitForFirstConsumer.
//
// The source's node is also where the copy belongs. The copy is made to be
// compared against the original, and both datasets belong on the pool that
// already holds one of them.
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
