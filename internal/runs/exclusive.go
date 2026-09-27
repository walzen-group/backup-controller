package runs

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// moverKind names the kind of mover otherMover looks for: the backup movers
// of ReplicationSources, or the controller's restore Jobs.
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
// running; the message then says how that sync ends (see syncingMessage).
//
// For restoreMover it looks at every restore Job of the controller in the
// namespace, selected by restorejob.ManagedLabels, whose
// restorejob.AnnotationClaim annotation is claim or whose
// restorejob.AnnotationRepository annotation is secret. Such a Job holds the
// claim while its RestoreRun still restores with it, and after that for as
// long as it or its pods may still write (see restoreJobHolds).
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
			return syncingMessage(source, repository), nil
		}
	}
	return "", nil
}

// syncingMessage returns the Ready message of a restore that waits for a
// sync no live BackupRun waits for, and says how that wait ends.
//
// Parameters:
//   - source is the ReplicationSource VolSync syncs (see inUse), as stored.
//     Its labels say whether the controller wrote it, and its status says
//     when the sync started and how the last mover ended.
//   - repository is the name of the source's restic repository Secret, which
//     the message names next to the claim.
//
// A sync of a finished or deleted BackupRun keeps going while VolSync
// retries a failing mover, and the restore would wait for as long as that
// lasts. The message gives the last mover result VolSync recorded. For a
// source the controller wrote, it says how to give the sync up: delete the
// source while no pod of its mover Job runs, since deleting it under a
// running mover kills restic and leaves a lock; the next backup of the claim
// writes the source again and unlocks the repository first. A source the
// controller did not write belongs to someone else, and the message leaves
// that decision to them.
func syncingMessage(source *volsyncv1alpha1.ReplicationSource, repository string) string {
	started, mover := syncState(source)
	way := fmt.Sprintf("ReplicationSource %s was not written by backup-controller, so whoever manages it decides how that sync ends.",
		source.Name)
	if source.Labels[backupv1alpha1.LabelManagedBy] == backupv1alpha1.ManagedByValue {
		way = fmt.Sprintf("If the mover keeps failing, fix what it reports and VolSync finishes on its own. "+
			"To give that sync up, delete the ReplicationSource %[1]s while no pod of Job volsync-src-%[1]s is running; "+
			"the next backup of the claim writes it again and unlocks the repository first.", source.Name)
	}
	return fmt.Sprintf("ReplicationSource %s is still syncing claim %s to repository %s; this run starts once VolSync has finished that sync. "+
		"%s.%s %s",
		source.Name, source.Spec.SourcePVC, repository, started, mover, way)
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
	jobs := &batchv1.JobList{}
	if err := reader.List(ctx, jobs, client.InNamespace(namespace), client.MatchingLabels(restorejob.ManagedLabels())); err != nil {
		return "", fmt.Errorf("list restore Jobs in %s: %w", namespace, err)
	}
	var runs *backupv1alpha1.RestoreRunList
	for i := range jobs.Items {
		job := &jobs.Items[i]
		if !matches(job.Annotations[restorejob.AnnotationClaim], claim) && !matches(job.Annotations[restorejob.AnnotationRepository], secret) {
			continue
		}
		if runs == nil {
			runs = &backupv1alpha1.RestoreRunList{}
			if err := reader.List(ctx, runs, client.InNamespace(namespace)); err != nil {
				return "", fmt.Errorf("list RestoreRuns in %s: %w", namespace, err)
			}
		}
		held, err := restoreJobHolds(ctx, reader, runs, job)
		if err != nil || held != "" {
			return held, err
		}
	}
	return "", nil
}

// restoreJobHolds returns a message for the Ready condition when a restore
// Job holds the claim and the repository it names, or "" when it does not.
//
// Parameters:
//   - ctx bounds the pod list.
//   - reader lists the Job's pods. Callers pass the uncached Reader, which
//     restoreInProgress read the Job through.
//   - runs are the RestoreRuns of the Job's namespace, which say whose Job it
//     is and whether that run still restores with it.
//   - job is a restore Job that writes the asking run's claim or reads its
//     repository.
//
// It returns an error from a failed pod list, and the caller retries.
//
// The Job holds them while the run named by its LabelRestoreRun label is not
// finished and not being deleted and the run's item that names the Job has
// not finished (see itemFinished). Past that, and for a Job of the volume
// populator or of a run that is gone, it holds them for as long as the Job
// controller may start a pod for it or one of its pods may still run restic
// (see restorejob.MayStillWrite): a run records its item's end before it
// stops the Job, and a stop takes until every pod has ended.
func restoreJobHolds(ctx context.Context, reader client.Reader, runs *backupv1alpha1.RestoreRunList, job *batchv1.Job) (string, error) {
	claim, repository := job.Annotations[restorejob.AnnotationClaim], job.Annotations[restorejob.AnnotationRepository]
	run := jobRun(runs, job)
	if run != nil && run.DeletionTimestamp == nil && !run.Status.Phase.Finished() && !itemFinished(run, job.Name) {
		return fmt.Sprintf("RestoreRun %s is restoring claim %s from repository %s with restore Job %s; this run starts once that restore has finished",
			run.Name, claim, repository, job.Name), nil
	}
	pods := &corev1.PodList{}
	if err := reader.List(ctx, pods, client.InNamespace(job.Namespace), client.MatchingLabels{batchv1.ControllerUidLabel: string(job.UID)}); err != nil {
		return "", fmt.Errorf("list the pods of restore Job %s: %w", job.Name, err)
	}
	if !restorejob.MayStillWrite(job, pods.Items) {
		return "", nil
	}
	owner := "a RestoreRun that no longer exists"
	switch {
	case run != nil:
		owner = "RestoreRun " + run.Name
	case job.Labels[restorejob.LabelRestoreClaim] != "":
		owner = "the VolumeRestore populator"
	}
	return fmt.Sprintf("restore Job %s of %s may still write claim %s from repository %s; this run starts once the Job is stopped and its pods have ended",
		job.Name, owner, claim, repository), nil
}

// jobRun returns the RestoreRun whose UID the restore Job's LabelRestoreRun
// label holds, or nil when no run in the list has it.
func jobRun(runs *backupv1alpha1.RestoreRunList, job *batchv1.Job) *backupv1alpha1.RestoreRun {
	uid := job.Labels[restorejob.LabelRestoreRun]
	for i := range runs.Items {
		if uid != "" && string(runs.Items[i].UID) == uid {
			return &runs.Items[i]
		}
	}
	return nil
}

// itemFinished reports whether the run's item that names the restore Job
// has finished. An item that names no Job yet, as after a lost status
// write, has not.
//
// Parameters:
//   - run is the RestoreRun the Job restores for.
//   - job is the Job's name, which the item records in its job field.
func itemFinished(run *backupv1alpha1.RestoreRun, job string) bool {
	for _, item := range run.Status.Items {
		if item.Job == job {
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
