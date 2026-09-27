package bootstrap

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// snapshotsBySecret is a restic.SnapshotLister that returns the snapshots of
// each repository by the name of its Secret.
type snapshotsBySecret map[string][]restic.Snapshot

func (s snapshotsBySecret) Snapshots(_ context.Context, secret *corev1.Secret) ([]restic.Snapshot, error) {
	return s[secret.Name], nil
}

// moverAt returns a snapshot with the layout of VolSync's backup mover, the
// ID given and the time given, with the tags given.
func moverAt(id string, at time.Time, tags ...string) restic.Snapshot {
	return restic.Snapshot{ID: id, Time: at, Hostname: "volsync", Paths: []string{"/data"}, Tags: tags}
}

// volumeRestore returns the VolumeRestore name in app and the repository
// Secret it names, name-repo.
func volumeRestore(name string) []runtime.Object {
	return []runtime.Object{
		&backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "app"}, Spec: backupv1alpha1.VolumeRestoreSpec{Repository: name + "-repo"}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name + "-repo", Namespace: "app"}},
	}
}

// decideSynced sends the create of the Cluster c to a Decider whose S3Prober
// reads the recorded store done-base, shifted so its completed backup
// starts at sunday, and whose lister gives the snapshots of lister. The
// fake client holds the objects in existing.
func decideSynced(t *testing.T, c *unstructured.Unstructured, lister restic.SnapshotLister, existing ...runtime.Object) admission.Response {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	server := recordedS3(t, recordedAt(t, "done-base", sunday))
	objects := append([]runtime.Object{storeAt(server.URL)}, existing...)
	decider := &Decider{Client: newBuilder(t).WithObjects(secret()).WithRuntimeObjects(objects...).Build(), Prober: S3Prober{}, Snapshots: lister}
	return decider.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create, Namespace: "app", Name: "app-pg", Object: runtime.RawExtension{Raw: raw},
	}})
}

// TestAQuiescedNamespaceRecoversToTheQuiescedMoment checks that a Cluster
// created with no RestoreRun, in a namespace whose volumes have quiesced
// snapshots, recovers to the time of those snapshots, the moment the
// populator fills the claims from. A newer live snapshot does not count.
func TestAQuiescedNamespaceRecoversToTheQuiescedMoment(t *testing.T) {
	moment := sunday.Add(24 * time.Hour)
	lister := snapshotsBySecret{"data-repo": {moverAt("aa", moment, restic.QuiescedTag), moverAt("bb", moment.Add(time.Hour))}}
	original := cluster(t, nil)

	patched := applied(t, original, decideSynced(t, original, lister, volumeRestore("data")...))

	target, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime")
	if target != moment.Format(time.RFC3339) {
		t.Errorf("targetTime = %q, want the quiesced moment %s", target, moment.Format(time.RFC3339))
	}
}

// TestTwoQuiescedMomentsRefuseTheCluster checks that the create is refused
// when two repositories of the namespace have their newest quiesced
// snapshots at different times, and that the refusal names both
// repositories.
func TestTwoQuiescedMomentsRefuseTheCluster(t *testing.T) {
	moment := sunday.Add(24 * time.Hour)
	lister := snapshotsBySecret{
		"data-repo":  {moverAt("aa", moment, restic.QuiescedTag)},
		"media-repo": {moverAt("bb", moment.Add(time.Hour), restic.QuiescedTag)},
	}
	existing := append(volumeRestore("data"), volumeRestore("media")...)

	response := decideSynced(t, cluster(t, nil), lister, existing...)

	if response.Allowed {
		t.Fatal("a Cluster was admitted while its namespace has two quiesced moments")
	}
	if !strings.Contains(response.Result.Message, "data-repo") || !strings.Contains(response.Result.Message, "media-repo") {
		t.Errorf("the refusal does not name both repositories: %q", response.Result.Message)
	}
}

// restoredClaim returns the Bound claim data in app, created at the time
// given, whose dataSourceRef names the VolumeRestore data.
func restoredClaim(created time.Time) *corev1.PersistentVolumeClaim {
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: "app", CreationTimestamp: metav1.NewTime(created)},
		Spec: corev1.PersistentVolumeClaimSpec{DataSourceRef: &corev1.TypedObjectReference{
			APIGroup: &backupv1alpha1.GroupVersion.Group, Kind: "VolumeRestore", Name: "data",
		}},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

// TestLiveVolumesKeepTheNewestRecovery checks that a Cluster created beside a
// claim that existed at the quiesced moment recovers to the end of its
// archive, as with no quiesced snapshots. Only the Cluster comes back, so a
// recovery to that moment would lose database writes the live volume holds.
func TestLiveVolumesKeepTheNewestRecovery(t *testing.T) {
	moment := sunday.Add(24 * time.Hour)
	lister := snapshotsBySecret{"data-repo": {moverAt("aa", moment, restic.QuiescedTag)}}
	original := cluster(t, nil)

	patched := applied(t, original, decideSynced(t, original, lister, append(volumeRestore("data"), restoredClaim(moment.Add(-time.Hour)))...))

	if _, found, _ := unstructured.NestedMap(patched, "spec", "bootstrap", "recovery", "recoveryTarget"); found {
		t.Error("a Cluster beside a live volume got a recovery target")
	}
}

// TestAVolumeRestoredBeforeTheClusterKeepsTheMoment checks that a claim
// created after the quiesced moment, which its VolumeRestore filled and
// the library bound before the Cluster was created, does not count as live:
// the Cluster still recovers to the moment.
func TestAVolumeRestoredBeforeTheClusterKeepsTheMoment(t *testing.T) {
	moment := sunday.Add(24 * time.Hour)
	lister := snapshotsBySecret{"data-repo": {moverAt("aa", moment, restic.QuiescedTag)}}
	original := cluster(t, nil)

	patched := applied(t, original, decideSynced(t, original, lister, append(volumeRestore("data"), restoredClaim(moment.Add(time.Hour)))...))

	target, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime")
	if target != moment.Format(time.RFC3339) {
		t.Errorf("targetTime = %q, want the quiesced moment %s", target, moment.Format(time.RFC3339))
	}
}

// TestTwoMomentsBesideALiveVolumeRecoverTheNewest checks that two quiesced
// moments do not refuse a Cluster that comes back beside a live volume: it
// recovers to the end of its archive, as with no quiesced snapshots.
func TestTwoMomentsBesideALiveVolumeRecoverTheNewest(t *testing.T) {
	moment := sunday.Add(24 * time.Hour)
	lister := snapshotsBySecret{
		"data-repo":  {moverAt("aa", moment, restic.QuiescedTag)},
		"media-repo": {moverAt("bb", moment.Add(time.Hour), restic.QuiescedTag)},
	}
	existing := append(append(volumeRestore("data"), volumeRestore("media")...), restoredClaim(moment.Add(-time.Hour)))
	original := cluster(t, nil)

	response := decideSynced(t, original, lister, existing...)

	if !response.Allowed {
		t.Fatalf("the Cluster was refused: %q", response.Result.Message)
	}
	if _, found, _ := unstructured.NestedMap(applied(t, original, response), "spec", "bootstrap", "recovery", "recoveryTarget"); found {
		t.Error("a Cluster beside a live volume got a recovery target")
	}
}
