package runs

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// selectSnapshot returns the snapshot the run would restore, picked from
// restoreAsOf and previous.
//
// Parameters:
//   - secretName names the restic repository Secret in the run's namespace.
//   - at is the moment to restore to. selectSnapshot takes the newest
//     snapshot at or before it, or the newest of all when it is nil.
//   - quiescedOnly passes over every snapshot not tagged quiesced.
//
// Only a snapshot with the layout VolSync's backup mover gives one, host
// volsync and paths exactly /data, is a candidate (see restic.MoverLayout):
// the restore writes a snapshot's files at the root of the claim, and a
// snapshot of another layout would put them in another directory. When
// spec.previous is set, selectSnapshot then steps that many candidates
// further back. It relies on the lister returning the snapshots oldest
// first, and snapshots of one time in the order of their IDs, so a tie
// resolves the same way on every pass and previous reaches each snapshot of
// it.
//
// Every restore restores the selected snapshot by its full ID, so two
// snapshots in one second, or with the same time, are each restored as
// selected.
//
// It returns a *refusalError with reason RepositorySecretMissing when the
// Secret doesn't exist. It returns a *refusalError with reason
// NoBackupInReach when no snapshot is a candidate (see noCandidate), when
// none is at or before the moment, and when spec.previous reaches past the
// oldest candidate. It returns a plain error when the Secret can't be read
// for another reason and when listing the repository fails.
func (r *RestoreRunReconciler) selectSnapshot(ctx context.Context, run *backupv1alpha1.RestoreRun, secretName string, at *time.Time, quiescedOnly bool) (restic.Snapshot, error) {
	all, err := r.repositorySnapshots(ctx, run, secretName)
	if err != nil {
		return restic.Snapshot{}, err
	}
	snapshots := slices.DeleteFunc(slices.Clone(all), func(s restic.Snapshot) bool {
		return !restic.MoverLayout(s) || quiescedOnly && !restic.Quiesced(s)
	})
	if len(snapshots) == 0 {
		return restic.Snapshot{}, noCandidate(all, quiescedOnly)
	}

	index := len(snapshots) - 1
	if at != nil {
		found, ok := restic.AtOrBefore(snapshots, *at)
		if !ok {
			return restic.Snapshot{}, refuse(backupv1alpha1.ItemReasonNoBackupInReach, "no snapshot at or before %s; the oldest, %s, is from %s",
				at.UTC().Format(time.RFC3339), snapshots[0].ShortID(), snapshots[0].Time.UTC().Format(time.RFC3339))
		}
		index = slices.IndexFunc(snapshots, func(s restic.Snapshot) bool { return s.ID == found.ID })
	}
	if run.Spec.Previous != nil {
		index -= int(*run.Spec.Previous)
		if index < 0 {
			return restic.Snapshot{}, refuse(backupv1alpha1.ItemReasonNoBackupInReach, "previous %d reaches past the oldest snapshot", *run.Spec.Previous)
		}
	}
	return snapshots[index], nil
}

// noCandidate refuses a run whose repository holds no snapshot
// selectSnapshot may restore.
//
// Parameters:
//   - all is every snapshot in the repository, oldest first.
//   - quiescedOnly is selectSnapshot's: the run takes only snapshots tagged
//     quiesced.
//
// It returns a *refusalError with reason NoBackupInReach. Its text says one
// of three things. The repository is empty. Or none of its snapshots has the
// mover's layout, and the text names the newest ones with their hosts and
// paths (see passedOver). Or none of those has the tag quiesced.
func noCandidate(all []restic.Snapshot, quiescedOnly bool) error {
	if len(all) == 0 {
		return refuse(backupv1alpha1.ItemReasonNoBackupInReach, "the repository holds no snapshot")
	}
	if slices.ContainsFunc(all, restic.MoverLayout) && quiescedOnly {
		return refuse(backupv1alpha1.ItemReasonNoBackupInReach, "the repository holds no snapshot tagged %s; only a BackupRun that stopped the workloads writes one", restic.QuiescedTag)
	}
	if len(all) == 1 {
		return refuse(backupv1alpha1.ItemReasonNoBackupInReach, "the repository holds 1 snapshot, which no VolSync mover wrote (host volsync, paths [/data]): %s", passedOver(all))
	}
	return refuse(backupv1alpha1.ItemReasonNoBackupInReach, "the repository holds %d snapshots, none written by a VolSync mover (host volsync, paths [/data]): %s",
		len(all), passedOver(all))
}

// passedOverShown is how many snapshots the reason of a repository with no
// snapshot of the mover's layout names at most, so a large repository still
// gives an item message of a few lines.
const passedOverShown = 5

// passedOver names the newest snapshots of a repository that has none of
// the mover's layout, for noCandidate.
//
// Parameters:
//   - all is every snapshot in the repository, oldest first, and not empty.
//
// It returns the newest passedOverShown snapshots, newest first, each with
// its short ID, host and paths, and a count of the older ones it leaves out.
func passedOver(all []restic.Snapshot) string {
	shown := all[max(0, len(all)-passedOverShown):]
	names := make([]string, 0, len(shown)+1)
	for i := len(shown) - 1; i >= 0; i-- {
		s := shown[i]
		names = append(names, fmt.Sprintf("%s (host %s, paths %v)", s.ShortID(), s.Hostname, s.Paths))
	}
	if older := len(all) - len(shown); older > 0 {
		names = append(names, fmt.Sprintf("and %d older", older))
	}
	return strings.Join(names, ", ")
}

// recordSnapshot records on a volume item the snapshot the checks selected:
// its full ID, which the restore Job restores, its short ID for display,
// and its time.
//
// Parameters:
//   - item is the volume item, updated in place.
//   - snapshot is the selected snapshot, or the zero Snapshot when the
//     checks selected none, which records nothing but an empty short ID.
func recordSnapshot(item *backupv1alpha1.RestoreItem, snapshot restic.Snapshot) {
	item.Snapshot, item.SnapshotID = snapshot.ShortID(), snapshot.ID
	if !snapshot.Time.IsZero() {
		item.SnapshotTime = &metav1.Time{Time: snapshot.Time}
	}
}

// repositorySnapshots lists every snapshot in a run's restic repository,
// oldest first.
//
// Parameters:
//   - run is the RestoreRun; the Secret is read in its namespace.
//   - secretName names the repository Secret.
//
// It returns the snapshots. It returns a *refusalError with reason
// RepositorySecretMissing when the Secret doesn't exist, and a plain error
// when the Secret can't be read for another reason or listing the
// repository fails.
func (r *RestoreRunReconciler) repositorySnapshots(ctx context.Context, run *backupv1alpha1.RestoreRun, secretName string) ([]restic.Snapshot, error) {
	secret := &corev1.Secret{}
	if err := r.Reader.Get(ctx, types.NamespacedName{Namespace: run.Namespace, Name: secretName}, secret); err != nil {
		if !apierrors.IsNotFound(err) {
			return nil, fmt.Errorf("read repository Secret %s: %w", secretName, err)
		}
		return nil, refuse(backupv1alpha1.ItemReasonRepositorySecretMissing, "no repository Secret %s in this namespace", secretName)
	}
	snapshots, err := r.Snapshots.Snapshots(ctx, secret)
	if err != nil {
		return nil, fmt.Errorf("list the snapshots in %s: %w", secretName, err)
	}
	return snapshots, nil
}

// checkDatabase finds the base backup that a recovery of one Cluster would
// start from.
//
// Parameters:
//   - namespace and name give the Cluster.
//   - at is the moment to recover to. The base backup is the newest one
//     that finished at or before it, or the newest of all when it is nil.
//
// It returns the base backup's ID. It returns a *refusalError when there is
// none: with reason ClusterMissing when the Cluster is missing, with reason
// ClusterArchivesNowhere when the Cluster archives nowhere, and with reason
// NoBackupInReach when the Cluster names an object store that is missing or
// incomplete, when the store holds no completed base backup, and when no
// base backup finished by the moment. A store with no completed base backup
// is a refusal, because deleting the Cluster would bring it back empty. It
// returns a plain error when the Cluster can't be read, when a read of the
// store or its Secrets fails in a way a retry may fix, and when listing the
// base backups fails.
func (r *RestoreRunReconciler) checkDatabase(ctx context.Context, namespace, name string, at *time.Time) (string, error) {
	cluster, found, err := cnpg.GetCluster(ctx, r.Reader, r.RESTMapper(), namespace, name)
	if err != nil {
		return "", err
	}
	if !found {
		return "", refuse(backupv1alpha1.ItemReasonClusterMissing, "no Cluster %s in this namespace", name)
	}
	store, serverName, archives := bootstrap.Archiver(cluster)
	if !archives {
		return "", refuse(backupv1alpha1.ItemReasonClusterArchivesNowhere, "the Cluster archives nowhere, so it has no backup to restore")
	}
	location, err := bootstrap.ResolveLocation(ctx, r.Reader, r.RESTMapper(), namespace, store, serverName)
	if err != nil {
		if retryable(err) {
			return "", err
		}
		return "", refuse(backupv1alpha1.ItemReasonNoBackupInReach, "%v", err)
	}
	backups, err := r.Prober.BaseBackups(ctx, location)
	if err != nil {
		return "", fmt.Errorf("list the base backups of %s: %w", name, err)
	}
	if len(backups) == 0 {
		return "", refuse(backupv1alpha1.ItemReasonNoBackupInReach, "%s/%s holds no completed base backup; deleting the Cluster would bring it back empty", location.Bucket, location.BasePrefix())
	}
	if at == nil {
		return backups[len(backups)-1].ID, nil
	}
	backup, ok := bootstrap.AtOrBefore(backups, *at)
	if !ok {
		return "", refuse(backupv1alpha1.ItemReasonNoBackupInReach, "no base backup finished by %s; the oldest, %s, finished at %s",
			at.UTC().Format(time.RFC3339), backups[0].ID, backups[0].End.UTC().Format(time.RFC3339))
	}
	return backup.ID, nil
}

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

	// CacheCapacity is the size of the mover's metadata cache claim, from
	// the VolumeRestore. When it is nil, VolSync makes the cache 1Gi (volsync
	// v0.16.0 internal/controller/mover/restic/mover.go:199-200).
	CacheCapacity *resource.Quantity

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
// It returns an *invalidSpecError when the run names neither a claim nor a
// repository, which only planIntoNewClaim can meet, and a *refusalError
// with reason ClaimMissing when the claim doesn't exist. When the claim has
// no VolumeRestore, it returns the *refusalError of volumeRestoreFor if the
// run names no repository, and otherwise the settings it could read from the
// claim alone. A read that fails for another reason, such as a timeout from
// the API server, comes back as a plain error, which the caller retries.
//
// Naming a claim is the ordinary case, and it states nothing twice. The
// claim's dataSourceRef names its VolumeRestore, and that object already
// carries the repository Secret, the cache class and capacity, and the queue
// label. Naming
// a repository directly covers a restore from a repository that no claim in
// the namespace backs up to.
func repositoryFor(ctx context.Context, c client.Reader, namespace, claimName, repository string, moverContext *corev1.PodSecurityContext) (restoreSettings, error) {
	settings := restoreSettings{Secret: repository, MoverSecurityContext: moverContext}
	if claimName == "" {
		if repository == "" {
			return settings, invalidSpec("one of spec.claim and spec.repository is required")
		}
		return settings, nil
	}

	claim := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: namespace, Name: claimName}
	if err := c.Get(ctx, key, claim); err != nil {
		if apierrors.IsNotFound(err) {
			return settings, refuse(backupv1alpha1.ItemReasonClaimMissing, "no PersistentVolumeClaim %s in this namespace", claimName)
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
		if _, refused := asItemFailure(err); refused && settings.Secret != "" {
			return settings, nil
		}
		return settings, err
	}

	if settings.Secret == "" {
		settings.Secret = vr.Spec.Repository
	}
	settings.CacheStorageClassName = vr.Spec.CacheStorageClassName
	settings.CacheCapacity = vr.Spec.CacheCapacity
	settings.MoverPodLabels = vr.Spec.MoverLabels()
	if settings.MoverSecurityContext == nil {
		settings.MoverSecurityContext = vr.Spec.MoverSecurityContext
	}
	return settings, nil
}
