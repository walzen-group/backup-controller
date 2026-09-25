package volsync

import (
	"reflect"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// TestNewPassesThroughSpecFields checks that New names the destination after
// the claim UID, restores Direct into the prime claim, names the Secret copy,
// and copies every optional field from the VolumeRestore's spec. When those
// fields are unset, the destination leaves them unset too.
func TestNewPassesThroughSpecFields(t *testing.T) {
	restoreAsOf := "2026-09-14T12:00:00Z"
	cacheClass := "zfs-ephemeral"
	cacheCapacity := resource.MustParse("2Gi")
	securityContext := &corev1.PodSecurityContext{RunAsNonRoot: new(true)}
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Generation: 7},
		Spec: backupv1alpha1.VolumeRestoreSpec{
			Repository:            "repo-secret",
			RestoreAsOf:           &restoreAsOf,
			CacheStorageClassName: &cacheClass,
			CacheCapacity:         &cacheCapacity,
			MoverPodLabels:        map[string]backupv1alpha1.MoverPodLabelValue{"queue": "backup"},
			MoverSecurityContext:  securityContext,
		},
	}
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{UID: types.UID("claim-123"), Name: "notes"}}

	rd := New(vr, claim, "prime-claim-123", "backup-system")

	if rd.Name != "restore-claim-123" || rd.Namespace != "backup-system" {
		t.Fatalf("destination identity = %s/%s, want backup-system/restore-claim-123", rd.Namespace, rd.Name)
	}
	if rd.Spec.Trigger == nil || rd.Spec.Trigger.Manual != "claim-123" {
		t.Fatalf("manual trigger = %#v, want claim UID", rd.Spec.Trigger)
	}
	if rd.Spec.Restic == nil {
		t.Fatal("restic spec is nil")
	}
	if rd.Spec.Restic.CopyMethod != volsyncv1alpha1.CopyMethodDirect {
		t.Errorf("copy method = %q, want Direct", rd.Spec.Restic.CopyMethod)
	}
	if rd.Spec.Restic.DestinationPVC == nil || *rd.Spec.Restic.DestinationPVC != "prime-claim-123" {
		t.Errorf("destination PVC = %#v, want prime-claim-123", rd.Spec.Restic.DestinationPVC)
	}
	// VolSync reads the repository Secret in the destination's own namespace,
	// so the destination has to name the copy the controller made there.
	// vr.Spec.Repository exists only in the app's namespace. Naming it here
	// would send every real restore looking for a Secret that doesn't exist.
	if rd.Spec.Restic.Repository != "claim-123" {
		t.Errorf("repository = %q, want the copied Secret claim-123", rd.Spec.Restic.Repository)
	}
	if rd.Spec.Restic.RestoreAsOf == nil || *rd.Spec.Restic.RestoreAsOf != restoreAsOf {
		t.Errorf("restore as of = %#v, want %q", rd.Spec.Restic.RestoreAsOf, restoreAsOf)
	}
	// Without a class, the mover's cache claim comes from the cluster's
	// default class, and a default class that reclaims Retain leaves its
	// dataset on the pool after every restore.
	if rd.Spec.Restic.CacheStorageClassName == nil || *rd.Spec.Restic.CacheStorageClassName != cacheClass {
		t.Errorf("cache storage class = %#v, want %q", rd.Spec.Restic.CacheStorageClassName, cacheClass)
	}
	if rd.Spec.Restic.CacheCapacity == nil || rd.Spec.Restic.CacheCapacity.Cmp(cacheCapacity) != 0 {
		t.Errorf("cache capacity = %#v, want %s", rd.Spec.Restic.CacheCapacity, cacheCapacity.String())
	}
	if !reflect.DeepEqual(rd.Spec.Restic.MoverPodLabels, map[string]string{"queue": "backup"}) {
		t.Errorf("mover labels = %#v, want queue label", rd.Spec.Restic.MoverPodLabels)
	}
	if !reflect.DeepEqual(rd.Spec.Restic.MoverSecurityContext, securityContext) {
		t.Errorf("mover security context = %#v, want %#v", rd.Spec.Restic.MoverSecurityContext, securityContext)
	}

	empty := New(&backupv1alpha1.VolumeRestore{Spec: backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"}}, claim, "prime-claim-123", "backup-system")
	if empty.Spec.Restic.RestoreAsOf != nil || empty.Spec.Restic.MoverPodLabels != nil || empty.Spec.Restic.MoverSecurityContext != nil ||
		empty.Spec.Restic.CacheStorageClassName != nil || empty.Spec.Restic.CacheCapacity != nil {
		t.Fatalf("empty optional fields = %#v, want nil", empty.Spec.Restic)
	}
}

// TestTriggerUsesClaimUID checks that the manual trigger is the claim's UID.
func TestTriggerUsesClaimUID(t *testing.T) {
	claim := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{UID: types.UID("claim-123")}}
	if got := Trigger(claim); got != "claim-123" {
		t.Fatalf("Trigger = %q, want claim UID", got)
	}
}

// TestCompleteMatchesManualTrigger checks that Complete is true only when
// status.lastManualSync equals the trigger.
func TestCompleteMatchesManualTrigger(t *testing.T) {
	for name, test := range map[string]struct {
		last, trigger string
		want          bool
	}{
		"equal":     {last: "claim-123", trigger: "claim-123", want: true},
		"different": {last: "other", trigger: "claim-123", want: false},
		"empty":     {last: "", trigger: "claim-123", want: false},
	} {
		t.Run(name, func(t *testing.T) {
			rd := &volsyncv1alpha1.ReplicationDestination{Status: &volsyncv1alpha1.ReplicationDestinationStatus{LastManualSync: test.last}}
			if got := Complete(rd, test.trigger); got != test.want {
				t.Fatalf("Complete = %t, want %t", got, test.want)
			}
		})
	}
}

// TestFailureReportsMoverLogs checks that Failure reports a failed mover run
// and returns its logs.
func TestFailureReportsMoverLogs(t *testing.T) {
	rd := &volsyncv1alpha1.ReplicationDestination{Status: &volsyncv1alpha1.ReplicationDestinationStatus{
		LatestMoverStatus: &volsyncv1alpha1.MoverStatus{Result: volsyncv1alpha1.MoverResultFailed, Logs: "restic failed"},
	}}
	reason, failed := Failure(rd)
	if !failed || reason != "restic failed" {
		t.Fatalf("Failure = %q, %t, want restic failed, true", reason, failed)
	}
}

// TestSecretCopyPreservesDataAndType checks that SecretCopy names the copy
// after the claim UID and keeps the type and data. It also checks that the
// data bytes are copied, so a change to the copy leaves the original alone.
func TestSecretCopyPreservesDataAndType(t *testing.T) {
	repo := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "repo-secret", Namespace: "apps"},
		Type:       corev1.SecretTypeOpaque,
		Data:       map[string][]byte{"repository": []byte("s3://bucket"), "password": []byte("secret")},
	}
	copy := SecretCopy(repo, types.UID("claim-123"), "backup-system")
	if copy.Name != "claim-123" || copy.Namespace != "backup-system" {
		t.Fatalf("secret identity = %s/%s, want backup-system/claim-123", copy.Namespace, copy.Name)
	}
	if copy.Type != repo.Type || !reflect.DeepEqual(copy.Data, repo.Data) {
		t.Fatalf("secret copy = %#v, want type and data from repository", copy)
	}
	copy.Data["password"][0] = 'X'
	if string(repo.Data["password"]) != "secret" {
		t.Fatal("SecretCopy aliases repository data")
	}
}
