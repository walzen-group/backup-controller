package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// syncedSource names the quiesced snapshots of the namespace's volumes as
// what asks for a recovery, in the messages of the webhook.
const syncedSource = "the moment of the quiesced snapshots of the namespace's volumes"

// syncedTarget finds the moment an automatic restore of a namespace goes
// back to, the same moment the populator fills the claims from.
//
// Parameters:
//   - namespace is the namespace of the Cluster.
//   - pin is the Cluster's backup.wlz.li/restore-as-of, or nil.
//
// It reads the repository Secrets that the namespace's VolumeRestores name,
// lists their snapshots with Snapshots, and returns what
// restic.SyncedMoment returns for them: the moment and true, false when no
// repository holds a quiesced snapshot at or before pin, or a
// *restic.NotSyncedError. A namespace with no VolumeRestore has no moment,
// and neither has a namespace whose volumes are live (see liveVolume): only
// the Cluster comes back there, and a moment older than the live volumes
// would lose database writes.
// It returns another error when a read fails, and when Snapshots is nil and
// the namespace has a VolumeRestore.
func (d *Decider) syncedTarget(ctx context.Context, namespace string, pin *time.Time) (time.Time, bool, error) {
	restores := &backupv1alpha1.VolumeRestoreList{}
	if err := d.Client.List(ctx, restores, client.InNamespace(namespace)); err != nil {
		return time.Time{}, false, fmt.Errorf("list the VolumeRestores: %w", err)
	}
	if len(restores.Items) == 0 {
		return time.Time{}, false, nil
	}
	live, err := d.liveVolume(ctx, namespace)
	if err != nil || live {
		return time.Time{}, false, err
	}
	if d.Snapshots == nil {
		return time.Time{}, false, errors.New("the webhook has no lister to read the restic repositories of the VolumeRestores")
	}
	names := make([]string, 0, len(restores.Items))
	for _, vr := range restores.Items {
		names = append(names, vr.Spec.Repository)
	}
	secret := func(ctx context.Context, name string) (*corev1.Secret, error) {
		s := &corev1.Secret{}
		return s, d.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, s)
	}
	repositories, err := restic.NamespaceSnapshots(ctx, names, secret, d.Snapshots)
	if err != nil {
		return time.Time{}, false, err
	}
	return restic.SyncedMoment(repositories, pin)
}

// automaticTarget gives the target of a recovery that no RestoreRun waits
// for.
//
// Parameters:
//   - c is the create.
//   - pin and source are what restoreTarget returns with no run: the
//     Cluster's backup.wlz.li/restore-as-of and its name, or nil and "the
//     webhook".
//
// It returns the moment of the quiesced snapshots of the namespace (see
// syncedTarget) with syncedSource when there is one, and pin and source
// unchanged when there is none. When the namespace's repositories give two
// moments, it returns a refusal. When a read fails, it returns an HTTP 500
// (see readFailed). It returns no answer otherwise.
func (d *Decider) automaticTarget(ctx context.Context, c creation, pin *time.Time, source string) (*time.Time, string, *admission.Response) {
	moment, found, err := d.syncedTarget(ctx, c.req.Namespace, pin)
	var notSynced *restic.NotSyncedError
	switch {
	case errors.As(err, &notSynced):
		c.logger.Info("refusing the Cluster", "reason", "the namespace's quiesced snapshots have two moments")
		refusal := admission.Denied("The volumes of this namespace have no one quiesced moment to recover the database to: " + notSynced.Error() + ".")
		return nil, "", &refusal
	case err != nil:
		refusal := readFailed(ctx, c.logger, c.budget, "reading the restic repositories of the VolumeRestores in "+c.req.Namespace, err)
		return nil, "", &refusal
	case found:
		return &moment, syncedSource, nil
	default:
		return pin, source, nil
	}
}

// liveVolume reports whether a namespace holds a live volume that a
// VolumeRestore filled.
//
// Parameters:
//   - namespace is the namespace of the Cluster.
//
// It returns true when a claim whose dataSourceRef names a VolumeRestore is
// Bound. The populator library binds such a claim only after the restore Job
// filled it, so a claim that is absent or still Pending comes back with the
// Cluster. It returns an error when the claims can not be listed.
func (d *Decider) liveVolume(ctx context.Context, namespace string) (bool, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := d.Client.List(ctx, claims, client.InNamespace(namespace)); err != nil {
		return false, fmt.Errorf("list the PersistentVolumeClaims: %w", err)
	}
	for _, claim := range claims.Items {
		ref := claim.Spec.DataSourceRef
		restored := ref != nil && ref.APIGroup != nil && *ref.APIGroup == backupv1alpha1.GroupVersion.Group && ref.Kind == "VolumeRestore"
		if restored && claim.Status.Phase == corev1.ClaimBound {
			return true, nil
		}
	}
	return false, nil
}
