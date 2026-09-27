package populator

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// snapshotsBySecret is a SnapshotLister that returns the snapshots of each
// repository by the name of its Secret.
type snapshotsBySecret map[string][]restic.Snapshot

func (s snapshotsBySecret) Snapshots(_ context.Context, secret *corev1.Secret) ([]restic.Snapshot, error) {
	return s[secret.Name], nil
}

// quiesced returns the snapshot with the tag a quiesced BackupRun adds.
func quiesced(s restic.Snapshot) restic.Snapshot {
	s.Tags = []string{restic.QuiescedTag}
	return s
}

// addMediaVolume adds a second VolumeRestore in apps, media-data, whose
// repository Secret is media-secret.
func addMediaVolume(t *testing.T, ops *fakeOperations) {
	t.Helper()
	vr := &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "media-data", Namespace: appNS}, Spec: backupv1alpha1.VolumeRestoreSpec{Repository: "media-secret"}}
	if err := ops.cluster.Create(context.Background(), vr); err != nil {
		t.Fatal(err)
	}
	ops.secrets[namespacedName(appNS, "media-secret")] = &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "media-secret", Namespace: appNS}}
}

// TestAQuiescedNamespaceRestoresTheQuiescedSnapshot checks that a claim of a
// namespace with quiesced backups is filled from the newest quiesced
// snapshot, not from a newer live one, so the volume matches the moment the
// bootstrap webhook recovers the database to.
func TestAQuiescedNamespaceRestoresTheQuiescedSnapshot(t *testing.T) {
	ops := populatedOperations(t)
	if err := newCallbacks(ops, quiesced(monday), tuesday).Populate(context.Background(), params()); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	job := ops.job(t, "claim-123")
	if job == nil || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID {
		t.Fatalf("Job = %+v, want one for the quiesced snapshot %s", job, monday.ID)
	}
}

// TestAClaimOfAnotherMomentWaitsForAPin checks that a claim stays Pending
// with reason NoBackupInReach when another repository of the namespace has
// its newest quiesced snapshot at another time, and that a pin both reach
// fills it from the quiesced snapshot of that moment.
func TestAClaimOfAnotherMomentWaitsForAPin(t *testing.T) {
	ops := populatedOperations(t)
	addMediaVolume(t, ops)
	lister := snapshotsBySecret{
		"repo-secret":  {quiesced(monday), quiesced(tuesday)},
		"media-secret": {quiesced(monday)},
	}
	if err := New(ops, controllerNS, testImage, lister).Populate(context.Background(), params()); err == nil {
		t.Fatal("Populate() filled a claim whose namespace has two quiesced moments")
	}
	last := ops.statuses[len(ops.statuses)-1]
	if cond := last.Status.Conditions[0]; cond.Reason != backupv1alpha1.ReasonNoBackupInReach || !strings.Contains(cond.Message, "media-secret") {
		t.Fatalf("condition = %+v, want NoBackupInReach naming media-secret", cond)
	}

	ops = populatedOperations(t)
	addMediaVolume(t, ops)
	pin := monday.Time.Add(time.Hour).Format(time.RFC3339)
	if err := New(ops, controllerNS, testImage, lister).Populate(context.Background(), pinned(pin)); err != nil {
		t.Fatalf("Populate() error = %v", err)
	}
	if job := ops.job(t, "claim-123"); job == nil || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID {
		t.Fatalf("Job = %+v, want one for %s", job, monday.ID)
	}
}
