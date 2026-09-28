package populator

import (
	"fmt"
	"slices"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/synced"
	corev1 "k8s.io/api/core/v1"
)

// pin is a moment that a claim's restore goes back to.
type pin struct {
	// at is the moment.
	at time.Time
	// value is the moment as the setting holds it, for messages.
	value string
	// source is the setting that holds the moment, for messages.
	source string
}

// outOfReachError reports a pinned restore that no snapshot can serve: the
// pin is not an RFC 3339 time, or every snapshot is later than the pin.
// Populate reports it with reason NoBackupInReach.
type outOfReachError struct {
	// reason says why no snapshot serves the pin, in words for the Ready
	// message.
	reason string
}

// Error returns the reason in words.
func (e *outOfReachError) Error() string {
	return e.reason
}

// pinOf reads the moment that a claim's restore goes back to.
//
// Parameters:
//   - vr is the VolumeRestore of the claim. Its spec.restoreAsOf
//     sets the pin for every claim that has no pin of its own.
//   - claim is the claim being restored. Its backup.wlz.li/restore-as-of
//     annotation comes before the VolumeRestore's setting.
//
// It returns the pin and true, or false when neither setting is present. It
// returns an *outOfReachError when the setting is not an RFC 3339 time.
func pinOf(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim) (pin, bool, error) {
	var source, value string
	if annotated, ok := claim.Annotations[backupv1alpha1.AnnotationRestoreAsOf]; ok {
		source, value = "the claim's "+backupv1alpha1.AnnotationRestoreAsOf, annotated
	} else if vr.Spec.RestoreAsOf != nil {
		source, value = "spec.restoreAsOf", *vr.Spec.RestoreAsOf
	} else {
		return pin{}, false, nil
	}
	at, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return pin{}, false, &outOfReachError{reason: fmt.Sprintf("%s is %q, which is not an RFC 3339 time", source, value)}
	}
	return pin{at: at, value: value, source: source}, true, nil
}

// choose selects the snapshot that fills a claim.
//
// Parameters:
//   - snapshots are the snapshots of the claim's repository, in any order.
//   - pinned is the moment the restore goes back to, from pinOf, or nil
//     when the claim has no pin.
//   - moment is the paused moment of the claim's namespace, from
//     synced.Moment, or nil when the namespace has none. The choice ignores
//     it when there is a pin.
//
// It returns the snapshot and true. It returns false and no error when the
// claim has no pin and the repository holds no snapshot, so the claim binds
// empty. It returns an *outOfReachError when there is a pin and no snapshot
// is at or before it.
//
// With a pin, the choice is the newest snapshot at or before the pin.
// Without a pin, it is the paused snapshot at the namespace's paused moment,
// so all volumes of the namespace come back from one paused backup. When the
// repository holds no snapshot at that moment, or there is no moment, it is
// the newest snapshot.
func choose(snapshots []restic.Snapshot, pinned *pin, moment *time.Time) (restic.Snapshot, bool, error) {
	if pinned != nil {
		return atPin(snapshots, *pinned)
	}
	if moment != nil {
		if snapshot, ok := synced.At(snapshots, *moment); ok {
			return snapshot, true, nil
		}
	}
	if len(snapshots) == 0 {
		return restic.Snapshot{}, false, nil
	}
	return slices.MaxFunc(snapshots, func(a, b restic.Snapshot) int { return a.Time.Compare(b.Time) }), true, nil
}

// atPin returns the newest snapshot at or before a pin.
//
// Parameters:
//   - snapshots are the snapshots of the claim's repository.
//   - pinned is the moment the restore goes back to.
//
// It returns the snapshot and true. When every snapshot is later than the
// pin, it returns an *outOfReachError with the ID and time of the oldest
// snapshot. When the repository holds no snapshot, the error says so.
func atPin(snapshots []restic.Snapshot, pinned pin) (restic.Snapshot, bool, error) {
	if snapshot, ok := restic.AtOrBefore(snapshots, pinned.at); ok {
		return snapshot, true, nil
	}
	if len(snapshots) == 0 {
		return restic.Snapshot{}, false, &outOfReachError{
			reason: fmt.Sprintf("%s asks for %s and the repository holds no snapshot", pinned.source, pinned.value),
		}
	}
	oldest := slices.MinFunc(snapshots, func(a, b restic.Snapshot) int { return a.Time.Compare(b.Time) })
	return restic.Snapshot{}, false, &outOfReachError{
		reason: fmt.Sprintf("%s asks for %s; the oldest snapshot, %s, is from %s",
			pinned.source, pinned.value, oldest.ShortID(), oldest.Time.UTC().Format(time.RFC3339)),
	}
}
