package runs

import (
	"context"
	"time"

	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
)

// SnapshotLister lists the snapshots in the restic repository that a VolSync
// repository Secret names. The controller gives restic.S3Lister. Tests give
// a fake, so that they run without an object store.
type SnapshotLister interface {
	// Snapshots lists the snapshots of the repository that secret names.
	Snapshots(ctx context.Context, secret *corev1.Secret) ([]restic.Snapshot, error)
}

// SnapshotRetimer changes the time of one snapshot and adds a tag to it, in
// the restic repository that a VolSync repository Secret names. The
// controller gives restic.S3Lister. Tests give a fake that records each
// call.
type SnapshotRetimer interface {
	// Retime writes the snapshot id again with the time at and the tag tag,
	// and returns the new snapshot. See restic.Repository.Retime.
	Retime(ctx context.Context, secret *corev1.Secret, id string, at time.Time, tag string) (restic.Snapshot, error)
}

// BaseBackupLister lists the base backups of a database in its object store.
// The controller gives bootstrap.S3Prober. Tests give a fake, so that they
// run without an object store.
type BaseBackupLister interface {
	// BaseBackups lists the completed base backups at the location, oldest
	// first.
	BaseBackups(ctx context.Context, at bootstrap.Location) ([]bootstrap.BaseBackup, error)
}
