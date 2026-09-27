package populator

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
)

// selection is the snapshot a claim is filled from.
type selection struct {
	// snapshot is the snapshot to restore, when empty is false.
	snapshot restic.Snapshot
	// empty is true when there is nothing to restore: no repository or no
	// snapshot at all, and no pin. The claim then binds its empty volume.
	empty bool
}

// pin is a moment a claim's restore goes back to, and where it came from.
type pin struct {
	// source is backup.wlz.li/restore-as-of or spec.restoreAsOf.
	source string
	// value is the moment as it was written.
	value string
	// at is the moment, parsed.
	at time.Time
}

// noBackupProblem says why no snapshot can fill a claim.
type noBackupProblem int

// The reasons no snapshot can fill a claim.
const (
	// badPin is a pin that is not an RFC 3339 time.
	badPin noBackupProblem = iota + 1
	// noSnapshot is a pinned claim whose repository holds no snapshot.
	noSnapshot
	// noCandidate is a repository whose snapshots all have another layout
	// than a VolSync mover's.
	noCandidate
	// outOfReach is a pin older than every candidate.
	outOfReach
	// notSynced is a namespace whose repositories give two moments for
	// their newest quiesced snapshots (see restic.SyncedMoment).
	notSynced
)

// noBackupError is a claim no snapshot in its repository can fill. The claim
// then stays Pending with reason NoBackupInReach, and nothing is created.
type noBackupError struct {
	// problem says which case it is.
	problem noBackupProblem
	// pin is the claim's pin, for badPin, noSnapshot and outOfReach.
	pin pin
	// all is every snapshot in the repository, oldest first, for
	// noCandidate.
	all []restic.Snapshot
	// oldest is the oldest candidate, for outOfReach.
	oldest restic.Snapshot
	// cause is the *restic.NotSyncedError, for notSynced.
	cause error
}

// Error says why no snapshot can fill the claim.
func (e *noBackupError) Error() string {
	switch e.problem {
	case badPin:
		return fmt.Sprintf("%s is %q, which is not an RFC 3339 time", e.pin.source, e.pin.value)
	case noSnapshot:
		return fmt.Sprintf("%s asks for %s and the repository holds no snapshot", e.pin.source, e.pin.value)
	case noCandidate:
		if len(e.all) == 1 {
			return "the repository holds 1 snapshot, which no VolSync mover wrote (host volsync, paths [/data]): " + passedOver(e.all)
		}
		return fmt.Sprintf("the repository holds %d snapshots, none written by a VolSync mover (host volsync, paths [/data]): %s",
			len(e.all), passedOver(e.all))
	case outOfReach:
		return fmt.Sprintf("%s asks for %s; the oldest snapshot, %s, is from %s",
			e.pin.source, e.pin.value, e.oldest.ShortID(), e.oldest.Time.UTC().Format(time.RFC3339))
	case notSynced:
		return e.cause.Error()
	default:
		return "no snapshot can fill the claim"
	}
}

// selectSnapshot picks the snapshot that fills a claim.
//
// Parameters:
//   - ctx bounds the reads.
//   - vr and claim give the pin: the claim's backup.wlz.li/restore-as-of
//     annotation first, then the VolumeRestore's spec.restoreAsOf (see
//     RestoreAsOf). vr also names the claim's repository Secret.
//
// It reads the snapshots of every repository of the claim's namespace (see
// namespaceSnapshots), and gives them to restic.SyncedMoment, as the
// bootstrap webhook does for a Cluster of the namespace. When that gives a
// moment and the claim's repository holds a quiesced snapshot at it, that
// snapshot fills the claim, so the volume and the database come back to one
// moment. Otherwise choose picks from the claim's repository as it does with
// no quiesced snapshots.
//
// It returns the selection, a *noBackupError when no snapshot can fill the
// claim, and a plain error when a read fails. A listing error never counts
// as an empty repository.
func (c *Callbacks) selectSnapshot(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) (selection, error) {
	moment, err := readPin(vr, claim)
	if err != nil {
		return selection{}, err
	}
	repositories, err := c.namespaceSnapshots(ctx, claim.Namespace, vr.Spec.Repository)
	if err != nil {
		return selection{}, err
	}
	own := repositories[vr.Spec.Repository]
	var at *time.Time
	if moment != nil {
		at = &moment.at
	}
	synced, found, err := restic.SyncedMoment(repositories, at)
	if err != nil {
		return selection{}, &noBackupError{problem: notSynced, cause: err}
	}
	if found {
		if snapshot, ok := restic.NewestQuiesced(own, &synced); ok {
			return selection{snapshot: snapshot}, nil
		}
	}
	return choose(own, moment)
}

// namespaceSnapshots lists the snapshots of every repository of a namespace.
//
// Parameters:
//   - namespace is the claim's namespace.
//   - repository is the claim's own repository Secret. It counts even when
//     the list of VolumeRestores does not show its VolumeRestore yet.
//
// It returns the snapshots by the name of the Secret (see
// restic.NamespaceSnapshots), and an error when the VolumeRestores, a Secret
// or a repository can not be read.
func (c *Callbacks) namespaceSnapshots(ctx context.Context, namespace, repository string) (map[string][]restic.Snapshot, error) {
	restores, err := c.operations.ListVolumeRestores(ctx, namespace)
	if err != nil {
		return nil, fmt.Errorf("list the VolumeRestores in %s: %w", namespace, err)
	}
	names := []string{repository}
	for _, vr := range restores {
		names = append(names, vr.Spec.Repository)
	}
	secret := func(ctx context.Context, name string) (*corev1.Secret, error) {
		return c.operations.GetSecret(ctx, namespace, name)
	}
	return restic.NamespaceSnapshots(ctx, names, secret, c.snapshots)
}

// readPin reads the moment a claim's restore goes back to.
//
// Parameters:
//   - vr and claim are passed to RestoreAsOf.
//
// It returns nil when nothing pins the claim, and a *noBackupError with
// problem badPin when the pin is not an RFC 3339 time.
func readPin(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) (*pin, error) {
	value := RestoreAsOf(vr, claim)
	if value == nil {
		return nil, nil
	}
	p := pin{source: "spec.restoreAsOf", value: *value}
	if _, ok := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; ok {
		p.source = backupv1alpha1.AnnotationRestoreAsOf
	}
	at, err := time.Parse(time.RFC3339, p.value)
	if err != nil {
		return nil, &noBackupError{problem: badPin, pin: p}
	}
	p.at = at
	return &p, nil
}

// choose picks the snapshot that fills a claim from a repository's list.
//
// Parameters:
//   - all is every snapshot in the repository, oldest first and by ID among
//     snapshots of one time, as the lister returns them.
//   - moment is the claim's pin, or nil.
//
// Only a snapshot with the layout VolSync's backup mover gives one is a
// candidate (restic.MoverLayout: host volsync, paths exactly /data, retimed
// copies included). The restore writes a snapshot's files at the root of the
// claim, and one of another layout would put them in another directory.
// Without a pin the newest candidate wins; with one, the newest at or before
// it by whole second (restic.AtOrBefore).
//
// It returns an empty selection only for a repository with no snapshot at
// all and no pin, so a first deploy binds its empty volume. Snapshots of
// another layout alone never bind empty: they give a *noBackupError with
// problem noCandidate. A pin that nothing reaches gives one too.
func choose(all []restic.Snapshot, moment *pin) (selection, error) {
	if len(all) == 0 {
		if moment == nil {
			return selection{empty: true}, nil
		}
		return selection{}, &noBackupError{problem: noSnapshot, pin: *moment}
	}
	candidates := slices.DeleteFunc(slices.Clone(all), func(s restic.Snapshot) bool { return !restic.MoverLayout(s) })
	if len(candidates) == 0 {
		return selection{}, &noBackupError{problem: noCandidate, all: all}
	}
	if moment == nil {
		return selection{snapshot: candidates[len(candidates)-1]}, nil
	}
	found, ok := restic.AtOrBefore(candidates, moment.at)
	if !ok {
		return selection{}, &noBackupError{problem: outOfReach, pin: *moment, oldest: candidates[0]}
	}
	return selection{snapshot: found}, nil
}

// passedOverShown is how many snapshots a noCandidate message names at most,
// so a large repository still gives a message of a few lines.
const passedOverShown = 5

// passedOver names the newest snapshots of a repository that has none of the
// mover's layout.
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
		names = append(names, fmt.Sprintf("%s (host %s, paths %v)", shown[i].ShortID(), shown[i].Hostname, shown[i].Paths))
	}
	if older := len(all) - len(shown); older > 0 {
		names = append(names, fmt.Sprintf("and %d older", older))
	}
	return strings.Join(names, ", ")
}
