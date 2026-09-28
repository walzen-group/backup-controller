package restic

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// Quiesced reports whether a restore of a synced moment may take a
// snapshot: it has the mover's layout (see MoverLayout) and the tag
// QuiescedTag, which only a BackupRun that stopped the workloads adds.
func Quiesced(s Snapshot) bool {
	return MoverLayout(s) && slices.Contains(s.Tags, QuiescedTag)
}

// NewestQuiesced returns the newest quiesced snapshot of one repository (see
// Quiesced).
//
// Parameters:
//   - all is every snapshot of the repository, oldest first, as Snapshots
//     returns them.
//   - at is the moment to go back to, or nil for the newest. Only a snapshot
//     at or before it by whole second counts (see AtOrBefore).
//
// It returns the snapshot and true, or the zero Snapshot and false when the
// repository holds no quiesced snapshot at or before at.
func NewestQuiesced(all []Snapshot, at *time.Time) (Snapshot, bool) {
	quiesced := slices.DeleteFunc(slices.Clone(all), func(s Snapshot) bool { return !Quiesced(s) })
	if len(quiesced) == 0 {
		return Snapshot{}, false
	}
	if at == nil {
		return quiesced[len(quiesced)-1], true
	}
	return AtOrBefore(quiesced, *at)
}

// NotSyncedError is a namespace whose repositories give two different
// moments for its newest quiesced snapshots, so an automatic restore can
// not bring the volumes and the databases back to one moment.
type NotSyncedError struct {
	// First and Other are the newest quiesced snapshots of two
	// repositories, with different times.
	First, Other Snapshot
	// FirstRepository and OtherRepository name the repository Secrets of
	// First and Other.
	FirstRepository, OtherRepository string
}

// Error names the two snapshots and their times.
func (e *NotSyncedError) Error() string {
	return fmt.Sprintf("the newest quiesced snapshot of repository %s is %s from %s, and of repository %s is %s from %s; set %s on the claims and the Cluster to a moment both reach",
		e.FirstRepository, e.First.ShortID(), e.First.Time.UTC().Format(time.RFC3339),
		e.OtherRepository, e.Other.ShortID(), e.Other.Time.UTC().Format(time.RFC3339),
		"backup.wlz.li/restore-as-of")
}

// SyncedMoment picks the moment an automatic restore of a namespace goes
// back to: the time of the newest quiesced snapshots of its repositories.
// The populator fills each claim from its snapshot of that time, and the
// bootstrap webhook recovers each Cluster to it.
//
// Parameters:
//   - repositories holds every snapshot of each repository that the
//     namespace's VolumeRestores name, by the name of its Secret (see
//     NamespaceSnapshots).
//   - at is the restore-as-of pin, or nil.
//
// It returns the moment in UTC and true. It returns false when no
// repository holds a quiesced snapshot at or before at: the restore then
// works as it did without quiesced snapshots. A repository with no such
// snapshot does not count, so a claim added after the last quiesced backup
// does not stop the others. It returns a *NotSyncedError when two
// repositories give different times. A quiesced BackupRun gives all its
// snapshots one time, so that is a run that did not rewrite every
// snapshot.
//
// Both sides call it on the same data, so they pick the same moment. A
// RestoreRun with spec.syncDatabaseToVolume takes the same snapshots (see
// Quiesced) and refuses two times in the same way.
func SyncedMoment(repositories map[string][]Snapshot, at *time.Time) (time.Time, bool, error) {
	names := make([]string, 0, len(repositories))
	for name := range repositories {
		names = append(names, name)
	}
	slices.Sort(names)

	var first Snapshot
	firstName := ""
	for _, name := range names {
		newest, ok := NewestQuiesced(repositories[name], at)
		if !ok {
			continue
		}
		if firstName == "" {
			first, firstName = newest, name
			continue
		}
		if !newest.Time.Equal(first.Time) {
			return time.Time{}, false, &NotSyncedError{First: first, Other: newest, FirstRepository: firstName, OtherRepository: name}
		}
	}
	if firstName == "" {
		return time.Time{}, false, nil
	}
	return first.Time.UTC(), true, nil
}

// SnapshotLister lists the snapshots of the repository a VolSync repository
// Secret names. S3Lister is the one the controller runs with.
type SnapshotLister interface {
	// Snapshots returns every snapshot, oldest first and by ID among
	// snapshots of one time, and none for a repository that does not exist
	// yet.
	Snapshots(ctx context.Context, secret *corev1.Secret) ([]Snapshot, error)
}

// NamespaceSnapshots lists the snapshots of each repository of a namespace.
//
// Parameters:
//   - c lists the namespace's VolumeRestores and reads their repository
//     Secrets.
//   - namespace is the namespace.
//   - own is a repository Secret that counts even when the list of
//     VolumeRestores does not show its VolumeRestore yet, or "" for none.
//   - lister lists the snapshots of the repository a Secret names.
//
// It returns the snapshots by the name of the Secret, for SyncedMoment, and
// no entry for a namespace with no VolumeRestore and no own. A name that
// comes twice is read once. It returns an error when the list, a Secret or a
// listing can not be read, and when lister is nil and there is a repository
// to read.
func NamespaceSnapshots(ctx context.Context, c client.Reader, namespace, own string, lister SnapshotLister) (map[string][]Snapshot, error) {
	restores := &backupv1alpha1.VolumeRestoreList{}
	if err := c.List(ctx, restores, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the VolumeRestores in %s: %w", namespace, err)
	}
	var names []string
	if own != "" {
		names = append(names, own)
	}
	for _, vr := range restores.Items {
		names = append(names, vr.Spec.Repository)
	}
	if len(names) > 0 && lister == nil {
		return nil, errors.New("no lister reads the restic repositories of the VolumeRestores")
	}
	repositories := make(map[string][]Snapshot, len(names))
	for _, name := range names {
		if _, done := repositories[name]; done {
			continue
		}
		secret := &corev1.Secret{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, secret); err != nil {
			return nil, fmt.Errorf("read repository Secret %s: %w", name, err)
		}
		all, err := lister.Snapshots(ctx, secret)
		if err != nil {
			return nil, fmt.Errorf("list the snapshots in %s: %w", name, err)
		}
		repositories[name] = all
	}
	return repositories, nil
}
