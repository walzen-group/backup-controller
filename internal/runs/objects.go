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
// It returns a refusal when the run names neither a claim nor a repository,
// or when the claim doesn't exist. When the claim has no VolumeRestore, it
// returns a refusal if the run names no repository, and otherwise the
// settings it could read from the claim alone. A read that fails for another
// reason, such as a timeout from the API server, comes back as a plain error,
// which the caller retries.
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
			return settings, refuse("one of spec.claim and spec.repository is required")
		}
		return settings, nil
	}

	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: namespace, Name: claimName}
	if err := c.Get(ctx, key, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return settings, refuse("no PersistentVolumeClaim %s in this namespace", claimName)
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
		if settings.Secret != "" && isRefusal(err) {
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

// selectedMoment returns the restoreAsOf that makes a mover restore the
// snapshot a run's checks selected for an item: the snapshot's time in whole
// seconds, as RFC 3339. The mover takes the newest snapshot at or before
// restoreAsOf, comparing whole seconds, and every newer snapshot is later than
// that, so it takes the selected one. It returns nil for an item with no
// recorded snapshot time, which recheckSnapshot refuses before any
// destination is built.
func selectedMoment(item backupv1alpha1.RestoreItem) *string {
	if item.SnapshotTime == nil {
		return nil
	}
	moment := item.SnapshotTime.UTC().Format(time.RFC3339)
	return &moment
}

// directDestination builds the ReplicationDestination through which every
// volume restore writes straight into a claim: an in-place restore, whose
// claim is the one the app uses, and an into restore, whose claim the run
// created (see scratchClaim). Its mover mounts the claim and writes the
// chosen snapshot into it.
//
// Parameters:
//   - run is the RestoreRun. Its UID becomes the manual trigger.
//   - item is the volume item. The destination writes into the claim it
//     names, and restores the snapshot the run's checks selected for it.
//   - settings are the repository and mover settings from repositoryFor.
//   - name is the name to give the ReplicationDestination.
//
// enableFileDeletion makes the mover remove the files the snapshot lacks, so
// the volume ends up holding exactly the snapshot. cleanupCachePVC drops the
// mover's cache claim when the run ends. The manual trigger is the run's UID,
// so a controller that restarts mid-restore recognises its own work.
//
// The mover gets the selected snapshot's time as restoreAsOf and gets no
// previous (see selectedMoment). Handing it spec.restoreAsOf and
// spec.previous would make it choose again when it starts, and a backup taken
// since the checks would shift what the newest snapshot, or the one before
// it, is. The callers run recheckSnapshot first, which refuses an item with
// no recorded time.
func directDestination(run *backupv1alpha1.RestoreRun, item backupv1alpha1.RestoreItem, settings restoreSettings, name string) *volsyncv1alpha1.ReplicationDestination {
	claim := item.Name
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
				RestoreAsOf:           selectedMoment(item),
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

// ownsDestination reports whether the run created the ReplicationDestination:
// its manual trigger is the run's UID, as directDestination sets it. A run
// created again under the same name has another UID, so it does not own the
// destinations of the run it replaces. A run writes, waits on and deletes
// only destinations it owns.
func ownsDestination(run *backupv1alpha1.RestoreRun, destination *volsyncv1alpha1.ReplicationDestination) bool {
	return destination.Spec.Trigger != nil && destination.Spec.Trigger.Manual == string(run.UID)
}

// notCreatedByRun returns the refusal for an object named spec.into that the
// run does not control, and "" when the run controls it.
//
// Parameters:
//   - run is the RestoreRun. It controls an object whose controller
//     reference carries its UID, as scratchClaim sets it; a run created
//     again under the same name does not.
//   - kind is "claim" or "VolumeRestore", for the message.
//   - object is the object as read from the API server.
//
// When another RestoreRun controls the object, the message names that run:
// the garbage collector deletes the object when that run is deleted, so a
// run that took the object over would lose it under its own success. A
// VolumeRestore describes the backups of the claim of its name, so its name
// is taken by a claim too, even while that claim does not exist.
func notCreatedByRun(run *backupv1alpha1.RestoreRun, kind string, object metav1.Object) string {
	if metav1.IsControlledBy(object, run) {
		return ""
	}
	owner := ""
	if ref := metav1.GetControllerOf(object); ref != nil && ref.Kind == "RestoreRun" &&
		ref.APIVersion == backupv1alpha1.GroupVersion.String() {
		owner = fmt.Sprintf("; it belongs to RestoreRun %s, which deletes it when it is deleted", ref.Name)
	}
	if kind == "claim" {
		return fmt.Sprintf("claim %s already exists and this run did not create it%s. "+
			"spec.into names a new claim for the run to create, and a restore never writes into a claim it did not create. "+
			"Choose a name no claim in this namespace has. To overwrite an existing claim, restore it in place with spec.claim.",
			object.GetName(), owner)
	}
	return fmt.Sprintf("%[1]s %[2]s already exists and this run did not create it%[3]s. "+
		"A %[1]s describes the backups of the claim of its name, so that name belongs to another claim, and "+
		"spec.into names a new claim for the run to create. Choose a name no claim or %[1]s in this namespace has.",
		kind, object.GetName(), owner)
}

// scratchClaim builds the plain claim that an into restore creates and its
// mover fills.
//
// Parameters:
//   - run is the RestoreRun. The claim takes its name from spec.into, and
//     spec.intoSize, when set, replaces the source claim's size.
//   - settings are the source claim's settings from repositoryFor. A
//     restore from spec.repository alone has none.
//
// The claim has no data source: the run's ReplicationDestination writes the
// selected snapshot into it (see directDestination). It takes the source
// claim's size and class, so the copy is provisioned the way the original
// was. It is an ordinary dynamic claim. Deleting it deletes its dataset too,
// which makes a scratch copy cheap to throw away. The run is its controller
// owner.
//
// It also takes the source claim's selected node,
// volume.kubernetes.io/selected-node. On a WaitForFirstConsumer class, which
// is every class on the walzen cluster, the provisioner creates the volume
// on that node at once, and the scheduler places the mover pod, which has no
// other pod to follow, on the node that holds the volume. The copy is made to
// be compared against the original, and both datasets belong on the pool
// that already holds one of them. A restore from spec.repository alone has
// no source node, and the scheduler places the claim with the mover pod,
// which is its first consumer.
func scratchClaim(run *backupv1alpha1.RestoreRun, settings restoreSettings) *corev1.PersistentVolumeClaim {
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
		},
	}
	if size != nil {
		claim.Spec.Resources = corev1.VolumeResourceRequirements{
			Requests: corev1.ResourceList{corev1.ResourceStorage: *size},
		}
	}
	return claim
}
