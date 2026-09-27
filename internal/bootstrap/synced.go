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
// and neither has a namespace with a live volume (see liveVolume), which
// gets no *restic.NotSyncedError either: only the Cluster comes back there,
// and a recovery to the moment would lose database writes the live volume
// holds.
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
	moment, found, err := restic.SyncedMoment(repositories, pin)
	var notSynced *restic.NotSyncedError
	switch {
	case errors.As(err, &notSynced):
		// Two moments stop only a restore of the whole app. A claim that
		// existed at the later one is live, and the Cluster then recovers
		// as with no quiesced snapshots.
		moment = latest(notSynced.First.Time, notSynced.Other.Time)
	case !found:
		return time.Time{}, false, nil
	}
	live, liveErr := d.liveVolume(ctx, namespace, moment)
	if liveErr != nil || live {
		return time.Time{}, false, liveErr
	}
	return moment, err == nil, err
}

// latest returns the later of two times.
func latest(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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

// liveVolume reports whether a namespace holds a volume that lived on after
// the synced moment.
//
// Parameters:
//   - namespace is the namespace of the Cluster.
//   - moment is the synced moment from restic.SyncedMoment.
//
// It returns true when a claim whose dataSourceRef names a VolumeRestore was
// created at or before the moment. That claim existed when the quiesced
// backup ran, so its data moved on since. A claim created after the moment
// was filled, or is being filled, from its VolumeRestore, so it comes back
// with the Cluster, Bound or not. It returns an error when the claims can
// not be listed.
func (d *Decider) liveVolume(ctx context.Context, namespace string, moment time.Time) (bool, error) {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := d.Client.List(ctx, claims, client.InNamespace(namespace)); err != nil {
		return false, fmt.Errorf("list the PersistentVolumeClaims: %w", err)
	}
	for _, claim := range claims.Items {
		ref := claim.Spec.DataSourceRef
		restored := ref != nil && ref.APIGroup != nil && *ref.APIGroup == backupv1alpha1.GroupVersion.Group && ref.Kind == "VolumeRestore"
		if restored && !claim.CreationTimestamp.After(moment) {
			return true, nil
		}
	}
	return false, nil
}
