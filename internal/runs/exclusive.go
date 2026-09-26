package runs

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// moverKind names the kind of mover otherMover looks for: the backup movers
// of ReplicationSources, or the restore movers of ReplicationDestinations.
type moverKind int

const (
	// backupMover makes otherMover look for a backup in progress, which a
	// restore waits for.
	backupMover moverKind = iota
	// restoreMover makes otherMover look for a restore in progress, which a
	// backup waits for.
	restoreMover
)

// otherMover returns a message for the Ready condition naming the run whose
// mover works on a claim or its repository, or "" when there is none. A
// backup and a restore of the same claim or repository never run at once:
// a backup would cut its clone from a volume the restore is half way through
// writing, and its restic forget needs the exclusive lock, which fails while
// the restore holds its read lock. A restore that starts during a backup
// would select its snapshot before the forget and the retime.
//
// Parameters:
//   - reader lists the objects. Callers pass the uncached Reader: a run
//     creates its mover object only after it exists, and a cache could lag
//     behind either.
//   - namespace is the namespace of the run that asks.
//   - claim is the claim the asking run backs up or writes; empty matches no
//     claim.
//   - secret is the restic repository Secret the asking run reads or writes;
//     empty matches no repository.
//   - kind is the kind of mover to look for.
//
// Whichever run already has its mover object goes first, and the other one
// waits. No run waits without an object to wait for, so the two never wait
// on each other. A run calls otherMover right before it writes its own mover
// object (for a backup, inside the write), so of two runs that both saw no
// object, the one that writes second sees the first one's.
//
// For backupMover it looks at every ReplicationSource in the namespace whose
// sourcePVC is claim or whose repository is secret. A source whose manual
// tag belongs to a BackupRun that is not finished and not being deleted, and
// whose item for the source is Pending or Running, holds the claim. That
// covers a quiesced run after its mover and before its retime, when VolSync
// has already completed the tag. A source VolSync is still syncing (see
// inUse) holds it too, whatever wrote its tag, because a mover may be
// running.
//
// For restoreMover it looks at every ReplicationDestination in the namespace
// whose destinationPVC is claim or whose repository is secret. One whose
// manual tag is the UID of a RestoreRun that is not finished and not being
// deleted holds the claim, unless the run's item that names the destination
// has finished. A destination of a run that finished or no longer exists
// does not; the finished run deletes it.
//
// A live RestoreRun that restores a claim's backups into a new claim through
// the populator holds that claim and the repository its VolumeRestore names
// once the VolumeRestore, which the run controls, exists: the populator's
// mover runs in the controller's namespace, which this list does not reach.
//
// A failed list comes back as an error, and the caller retries; nothing is
// decided on it.
func otherMover(ctx context.Context, reader client.Reader, namespace, claim, secret string, kind moverKind) (string, error) {
	if kind == backupMover {
		return backupInProgress(ctx, reader, namespace, claim, secret)
	}
	return restoreInProgress(ctx, reader, namespace, claim, secret)
}

// backupInProgress is otherMover for backupMover.
func backupInProgress(ctx context.Context, reader client.Reader, namespace, claim, secret string) (string, error) {
	sources := &volsyncv1alpha1.ReplicationSourceList{}
	if err := reader.List(ctx, sources, client.InNamespace(namespace)); err != nil {
		return "", fmt.Errorf("list ReplicationSources in %s: %w", namespace, err)
	}
	var runs *backupv1alpha1.BackupRunList
	for i := range sources.Items {
		source := &sources.Items[i]
		repository := ""
		if source.Spec.Restic != nil {
			repository = source.Spec.Restic.Repository
		}
		if !matches(source.Spec.SourcePVC, claim) && !matches(repository, secret) {
			continue
		}
		if runs == nil {
			runs = &backupv1alpha1.BackupRunList{}
			if err := reader.List(ctx, runs, client.InNamespace(namespace)); err != nil {
				return "", fmt.Errorf("list BackupRuns in %s: %w", namespace, err)
			}
		}
		if run := liveBackup(runs, source); run != "" {
			return fmt.Sprintf("BackupRun %s is backing up claim %s to repository %s with ReplicationSource %s; this run starts once that backup has finished",
				run, source.Spec.SourcePVC, repository, source.Name), nil
		}
		if inUse(source) {
			return fmt.Sprintf("ReplicationSource %s is still syncing claim %s to repository %s; this run starts once VolSync has finished that sync",
				source.Name, source.Spec.SourcePVC, repository), nil
		}
	}
	return "", nil
}

// liveBackup returns the name of the BackupRun that still waits for the
// source's manual tag, or "" when no run does. The run must not be finished
// or being deleted, and its item for the source must be Pending or Running.
func liveBackup(runs *backupv1alpha1.BackupRunList, source *volsyncv1alpha1.ReplicationSource) string {
	tag := manualTag(source)
	if tag == "" {
		return ""
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if TriggerFor(run.UID) != tag || run.DeletionTimestamp != nil || run.Status.Phase.Finished() {
			continue
		}
		for _, item := range run.Status.Items {
			if item.Kind == "ReplicationSource" && item.Name == source.Name &&
				(item.Phase == backupv1alpha1.ItemPending || item.Phase == backupv1alpha1.ItemRunning) {
				return run.Name
			}
		}
	}
	return ""
}

// restoreInProgress is otherMover for restoreMover.
func restoreInProgress(ctx context.Context, reader client.Reader, namespace, claim, secret string) (string, error) {
	runs := &backupv1alpha1.RestoreRunList{}
	if err := reader.List(ctx, runs, client.InNamespace(namespace)); err != nil {
		return "", fmt.Errorf("list RestoreRuns in %s: %w", namespace, err)
	}
	var live []*backupv1alpha1.RestoreRun
	for i := range runs.Items {
		run := &runs.Items[i]
		if run.DeletionTimestamp == nil && !run.Status.Phase.Finished() {
			live = append(live, run)
		}
	}
	if len(live) == 0 {
		return "", nil
	}

	list := &volsyncv1alpha1.ReplicationDestinationList{}
	if err := reader.List(ctx, list, client.InNamespace(namespace)); err != nil {
		return "", fmt.Errorf("list ReplicationDestinations in %s: %w", namespace, err)
	}
	for i := range list.Items {
		destination := &list.Items[i]
		if destination.Spec.Restic == nil || destination.Spec.Trigger == nil || destination.Spec.Trigger.Manual == "" {
			continue
		}
		target := ""
		if destination.Spec.Restic.DestinationPVC != nil {
			target = *destination.Spec.Restic.DestinationPVC
		}
		if !matches(target, claim) && !matches(destination.Spec.Restic.Repository, secret) {
			continue
		}
		for _, run := range live {
			if string(run.UID) != destination.Spec.Trigger.Manual || itemFinished(run, destination.Name) {
				continue
			}
			return fmt.Sprintf("RestoreRun %s is restoring claim %s from repository %s with ReplicationDestination %s; this run starts once that restore has finished",
				run.Name, target, destination.Spec.Restic.Repository, destination.Name), nil
		}
	}

	// A run that restores a claim's backups into a new claim through the
	// populator has its mover in the controller's namespace, where this
	// list does not reach. Its VolumeRestore, which the run owns, stands for
	// it here.
	for _, run := range live {
		if run.Spec.Claim == "" || run.Spec.Into == "" {
			continue
		}
		vr := &backupv1alpha1.VolumeRestore{}
		if err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: run.Spec.Into}, vr); err != nil {
			if apierrors.IsNotFound(err) {
				continue
			}
			return "", fmt.Errorf("get VolumeRestore %s/%s: %w", namespace, run.Spec.Into, err)
		}
		if !metav1.IsControlledBy(vr, run) || (!matches(run.Spec.Claim, claim) && !matches(vr.Spec.Repository, secret)) {
			continue
		}
		return fmt.Sprintf("RestoreRun %s is restoring the backups of claim %s from repository %s into claim %s through VolumeRestore %s; this run starts once that restore has finished",
			run.Name, run.Spec.Claim, vr.Spec.Repository, run.Spec.Into, vr.Name), nil
	}
	return "", nil
}

// itemFinished reports whether the run's item that names the destination
// has finished. An item that names no destination yet, as after a lost
// status write, has not.
func itemFinished(run *backupv1alpha1.RestoreRun, destination string) bool {
	for _, item := range run.Status.Items {
		if item.Destination == destination {
			return finished(item)
		}
	}
	return false
}

// matches reports whether value is want, where an empty want matches
// nothing.
func matches(value, want string) bool {
	return want != "" && value == want
}
