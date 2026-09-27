package populator

import (
	"context"
	"fmt"
	"slices"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// markRestoring marks a claim's entry Restoring, sets Ready from every entry
// with summarize, and writes the status when that changed something.
//
// Parameters:
//   - vr is the VolumeRestore, changed in place and written.
//   - claim is the claim being filled.
//
// It returns an error from summarize's reads or from the status write.
func (c *Callbacks) markRestoring(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) error {
	before := vr.Status.DeepCopy()
	setClaimStatus(vr, claim, backupv1alpha1.RestorePhaseRestoring)
	if err := c.summarize(ctx, vr); err != nil {
		return err
	}
	return c.writeChanged(ctx, vr, before)
}

// markFailed marks a claim's entry Failed, sets Ready False with a reason
// and the cause's message, and writes the status when that changed
// something.
//
// Parameters:
//   - vr is the VolumeRestore, changed in place and written.
//   - claim is the claim whose restore failed. The message starts with
//     "claim <name>: ", so a VolumeRestore that fills several claims says
//     which one failed.
//   - reason is the Ready reason: RestoreFailed, NoBackupInReach or
//     RestoreJobRefused.
//   - cause is the typed failure, rendered into the message here and read
//     by nothing else.
//
// It returns the error of the status write.
func (c *Callbacks) markFailed(ctx context.Context, vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim, reason string, cause error) error {
	before := vr.Status.DeepCopy()
	setClaimStatus(vr, claim, backupv1alpha1.RestorePhaseFailed)
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, reason, claimMessage(claim, cause.Error()))
	return c.writeChanged(ctx, vr, before)
}

// writeChanged writes the VolumeRestore's status when it differs from the
// status before the change, and returns the error of the write.
func (c *Callbacks) writeChanged(ctx context.Context, vr *backupv1alpha1.VolumeRestore, before *backupv1alpha1.VolumeRestoreStatus) error {
	if equality.Semantic.DeepEqual(before, &vr.Status) {
		return nil
	}
	if err := c.operations.SetStatus(ctx, vr); err != nil {
		return fmt.Errorf("set VolumeRestore status: %w", err)
	}
	return nil
}

// summarize sets the VolumeRestore's Ready condition from every entry in its
// status.claims. Populate and Cleanup call it, so every claim that shares the
// VolumeRestore computes the same condition. It changes the status in memory
// only, and the caller writes it.
//
// Parameters:
//   - vr is the VolumeRestore, changed in place.
//
// It returns an error when a claim's Job or its pods can't be read.
//
// With no entry, Ready is True with reason Restored. While any entry is
// Failed, it leaves Ready as it is: the failed claim's own pass writes its
// failure, and another claim's pass must not overwrite it. Otherwise Ready
// is False with reason Restoring, and the message names the restore Job of
// every entry, in status.claims order, with the waiting reason of its pod
// while there is one, and the controller namespace they run in, since a
// reader who looks in the app's namespace won't find them.
func (c *Callbacks) summarize(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error {
	claims := vr.Status.Claims
	if len(claims) == 0 {
		backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionTrue, backupv1alpha1.ReasonRestored, "no claim is being restored")
		return nil
	}
	if slices.ContainsFunc(claims, func(s backupv1alpha1.ClaimRestoreStatus) bool { return s.Phase == backupv1alpha1.RestorePhaseFailed }) {
		return nil
	}
	jobs := make([]string, 0, len(claims))
	for _, status := range claims {
		note, err := c.jobNote(ctx, status.UID)
		if err != nil {
			return err
		}
		jobs = append(jobs, note)
	}
	noun := "restore Job"
	if len(jobs) > 1 {
		noun = "restore Jobs"
	}
	waiting := fmt.Sprintf("waiting for %s %s in %s", noun, strings.Join(jobs, ", "), c.namespace)
	backupv1alpha1.SetReady(&vr.Status.Conditions, vr.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRestoring, waiting)
	return nil
}

// jobNote names a claim's restore Job for the Ready message.
//
// Parameters:
//   - claimUID is the UID of the claim.
//
// It returns the Job's name and the short ID of the snapshot it restores,
// with the waiting reason of its newest pod while restorejob.Read finds one,
// and an error when the Job or its pods can't be read. A claim with no Job
// gets the name alone. The note is for a person and decides nothing.
func (c *Callbacks) jobNote(ctx context.Context, claimUID types.UID) (string, error) {
	key := c.jobKey(claimUID)
	job, err := c.operations.GetJob(ctx, key)
	switch {
	case apierrors.IsNotFound(err):
		return key.Name, nil
	case err != nil:
		return "", fmt.Errorf("read restore Job %s: %w", key, err)
	}
	pods, err := c.operations.ListJobPods(ctx, job.Namespace, job.UID)
	if err != nil {
		return "", fmt.Errorf("list the pods of restore Job %s: %w", key, err)
	}
	snapshot := restic.Snapshot{ID: job.Annotations[restorejob.AnnotationSnapshotID]}.ShortID()
	if waiting := restorejob.Read(job, pods).Waiting; waiting != nil {
		return fmt.Sprintf("%s (snapshot %s; %s)", key.Name, snapshot, waiting), nil
	}
	return fmt.Sprintf("%s (snapshot %s)", key.Name, snapshot), nil
}

// claimMessage prefixes a failure message with "claim <name>: ", so a
// VolumeRestore that fills several claims says which claim failed.
func claimMessage(claim *corev1.PersistentVolumeClaim, message string) string {
	return fmt.Sprintf("claim %s: %s", claim.Name, message)
}

// setClaimStatus sets the phase of the claim's entry in the VolumeRestore's
// status.claims, and adds the entry when there isn't one. It also updates the
// entry's name, and sets startedAt to now when the entry has no start time
// yet. It changes the status in memory only, and the caller writes it.
func setClaimStatus(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim, phase backupv1alpha1.RestorePhase) {
	for i := range vr.Status.Claims {
		status := &vr.Status.Claims[i]
		if status.UID != claim.UID {
			continue
		}
		status.Name = claim.Name
		status.Phase = phase
		if status.StartedAt == nil {
			now := metav1.Now()
			status.StartedAt = &now
		}
		return
	}
	now := metav1.Now()
	vr.Status.Claims = append(vr.Status.Claims, backupv1alpha1.ClaimRestoreStatus{
		Name:      claim.Name,
		UID:       claim.UID,
		Phase:     phase,
		StartedAt: &now,
	})
}

// retireClaimStatus removes the claim's entry from the VolumeRestore's
// status.claims, which ends the VolumeRestore's report of that restore. When
// no entry remains, it sets claims to nil. It changes the status in memory
// only, and the caller writes it.
func retireClaimStatus(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) {
	remaining := vr.Status.Claims[:0]
	for _, status := range vr.Status.Claims {
		if status.UID != claim.UID {
			remaining = append(remaining, status)
		}
	}
	if len(remaining) == 0 {
		vr.Status.Claims = nil
		return
	}
	vr.Status.Claims = remaining
}
