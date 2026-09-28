// Package synced finds the paused moment of a namespace: the newest moment
// at which a BackupRun paused the app and backed up every volume of the
// namespace. An automatic restore brings the volumes and the databases back
// to that moment when it can (see docs: Automatic synced restore). The
// populator and the bootstrap webhook both use it, so they pick the same
// moment from the same repositories.
package synced

import (
	"context"
	"fmt"
	"slices"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Repositories lists the snapshots of every restic repository that a
// VolumeRestore in the namespace names.
//
// Parameters:
//   - c reads the VolumeRestores and the repository Secrets.
//   - lister lists a repository's snapshots.
//   - namespace is the namespace.
//
// It returns the snapshots by the name of the repository's Secret. It
// returns an error when a read or a listing fails, since a moment picked
// from part of the repositories could differ from the moment of all of them.
func Repositories(ctx context.Context, c client.Reader, lister restic.Lister, namespace string) (map[string][]restic.Snapshot, error) {
	restores := &backupv1alpha1.VolumeRestoreList{}
	if err := c.List(ctx, restores, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the VolumeRestores in %s: %w", namespace, err)
	}
	repositories := map[string][]restic.Snapshot{}
	for _, vr := range restores.Items {
		name := vr.Spec.Repository
		if _, done := repositories[name]; done {
			continue
		}
		secret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
			return nil, fmt.Errorf("get repository Secret %s/%s: %w", namespace, name, err)
		}
		snapshots, err := lister.Snapshots(ctx, secret)
		if err != nil {
			return nil, fmt.Errorf("list the snapshots in %s/%s: %w", namespace, name, err)
		}
		repositories[name] = snapshots
	}
	return repositories, nil
}

// Moment returns the time of the newest paused snapshot in the namespace,
// and true, when every repository holds a paused snapshot of that time.
//
// Parameters:
//   - repositories are the snapshots of each repository, by Secret name,
//     from Repositories.
//
// One BackupRun gives all its paused snapshots the same time, so the newest
// paused time that every repository holds is the newest paused backup of the
// whole namespace. It returns false when a repository lacks that time, for
// example when a claim held no files at the newest paused backup, or when a
// VolumeRestore names a repository that no BackupRun writes to any more. An
// older time that every repository holds is never used: a restore to it
// would bring every other volume and the databases back further than their
// newest paused backup. It also returns false when there are no
// repositories or no paused snapshots.
func Moment(repositories map[string][]restic.Snapshot) (time.Time, bool) {
	var newest time.Time
	for _, snapshots := range repositories {
		for _, s := range snapshots {
			if t := s.Time.UTC().Truncate(time.Second); slices.Contains(s.Tags, restic.PausedTag) && t.After(newest) {
				newest = t
			}
		}
	}
	if newest.IsZero() {
		return time.Time{}, false
	}
	for _, snapshots := range repositories {
		if _, found := At(snapshots, newest); !found {
			return time.Time{}, false
		}
	}
	return newest, true
}

// At returns the snapshot of one repository that is tagged paused and has
// the moment's time, and true, or false when the repository holds none.
func At(snapshots []restic.Snapshot, moment time.Time) (restic.Snapshot, bool) {
	for _, s := range snapshots {
		if slices.Contains(s.Tags, restic.PausedTag) && s.Time.UTC().Truncate(time.Second).Equal(moment) {
			return s, true
		}
	}
	return restic.Snapshot{}, false
}
