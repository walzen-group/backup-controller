//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// volumeManifests returns the objects Flux applies for a test app without a
// database. They are:
//   - the VolumeRestore data, which names the restic repository;
//   - the claim data, marked for backup, whose dataSourceRef names the
//     VolumeRestore data, as every app claim on prod does, so a new claim is
//     filled from the newest snapshot;
//   - the Deployment app, which mounts the claim and which a BackupRun with
//     all: true pauses.
func volumeManifests() string {
	return fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: data
spec:
  repository: restic
  cacheStorageClassName: %[1]s
  cacheCapacity: 100Mi
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  annotations:
    backup.wlz.li/enabled: "true"
    backup.wlz.li/retain-last: "10"
spec:
  storageClassName: %[1]s
  accessModes: [ReadWriteOnce]
  resources:
    requests: {storage: 100Mi}
  dataSourceRef:
    apiGroup: backup.wlz.li
    kind: VolumeRestore
    name: data
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  annotations:
    %[2]s: "true"
spec:
  replicas: 1
  selector:
    matchLabels: {app: app}
  template:
    metadata:
      labels: {app: app}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: app
          image: %[3]s
          command: [sleep, "86400"]
          volumeMounts: [{name: data, mountPath: /data}]
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: data}
`, storageClass, pauseAnnotation, appImage)
}

// write writes a line into the file /data/note of the Deployment app,
// replacing what the file held, and flushes it to the volume.
//
// Parameters:
//   - content is the line to write.
//
// It waits up to five minutes for a running pod of the Deployment, since the
// claim may still be filling or the app may still be starting.
func (a *app) write(content string) {
	a.t.Helper()
	waitFor(a.t, "the app to write "+content, 5*time.Minute, func() (bool, string) {
		out, err := run(a.t.Context(), "", "-n", a.ns, "exec", "deploy/app", "--", "sh", "-c",
			fmt.Sprintf("echo %s > /data/note && sync && cat /data/note", content))
		if err != nil {
			return false, firstLine(err.Error())
		}
		return strings.TrimSpace(out) == content, strings.TrimSpace(out)
	}, a.describe)
}

// waitNote waits up to five minutes until the file /data/note of the
// Deployment app holds a line.
//
// Parameters:
//   - what names the wait in the log and in the failure message.
//   - want is the line the file must hold.
func (a *app) waitNote(what, want string) {
	a.t.Helper()
	waitFor(a.t, what, 5*time.Minute, func() (bool, string) {
		out, err := run(a.t.Context(), "", "-n", a.ns, "exec", "deploy/app", "--", "cat", "/data/note")
		if err != nil {
			return false, firstLine(err.Error())
		}
		return strings.TrimSpace(out) == want, strings.TrimSpace(out)
	}, a.describe)
}

// firstLine returns the first line of a text, for a short state in a wait.
func firstLine(text string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(text), "\n")
	return line
}

// mustSucceed fails the test when a run did not end Succeeded.
//
// Parameters:
//   - kind and name identify the run in the failure message.
//   - phase and conditions are the run's status.phase and
//     status.conditions.
//   - describe returns the state of the app for the failure message.
func mustSucceed(t *testing.T, kind, name string, phase backupv1alpha1.RunPhase, conditions []metav1.Condition, describe func() string) {
	t.Helper()
	if phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("%s %s ended %s: %s\n%s", kind, name, phase, ready(conditions), describe())
	}
}

// TestAClaimRestoresInPlace backs up the claim of a running app twice, with
// a different file each time, and restores it in place three ways: to the
// snapshot before the newest (spec.previous), to a moment between the two
// backups (spec.restoreAsOf), and to the newest. The RestoreRun pauses the
// app while it writes the claim, and after each restore the app runs again
// and reads the file of the chosen backup.
func TestAClaimRestoresInPlace(t *testing.T) {
	t.Parallel()
	a := newApp(t, "claim")
	a.publish(volumeManifests())

	a.write("first")
	first := a.backup("first", "source: data")
	mustSucceed(t, "BackupRun", "first", first.Status.Phase, first.Status.Conditions, a.describe)
	between := time.Now().UTC().Add(time.Second).Truncate(time.Second)
	time.Sleep(2 * time.Second)
	a.write("second")
	second := a.backup("second", "source: data")
	mustSucceed(t, "BackupRun", "second", second.Status.Phase, second.Status.Conditions, a.describe)
	a.write("not backed up")

	cases := []struct {
		name, spec, want string
	}{
		{"previous", "claim: data\nprevious: 1", "first"},
		{"as-of", "claim: data\nrestoreAsOf: " + between.Format(time.RFC3339), "first"},
		{"newest", "claim: data", "second"},
	}
	for _, c := range cases {
		run := a.restore(c.name, c.spec+"\npauseDuringRestore:\n  - {kind: Deployment, name: app}")
		mustSucceed(t, "RestoreRun", c.name, run.Status.Phase, run.Status.Conditions, a.describe)
		a.waitNote("the app to read the file of RestoreRun "+c.name, c.want)
	}
}

// TestASnapshotRestoresIntoANewClaim restores the newest snapshot of the
// app's repository into a new claim named copy. The new claim holds the
// file, the app's own claim keeps what it holds now, and the new claim stays
// after the RestoreRun is deleted: it belongs to the user.
func TestASnapshotRestoresIntoANewClaim(t *testing.T) {
	t.Parallel()
	a := newApp(t, "into")
	a.publish(volumeManifests())

	a.write("backed up")
	backedUp := a.backup("base", "source: data")
	mustSucceed(t, "BackupRun", "base", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)
	a.write("live")

	restored := a.restore("copy", "repository: restic\ninto: copy\nintoSize: 100Mi")
	mustSucceed(t, "RestoreRun", "copy", restored.Status.Phase, restored.Status.Conditions, a.describe)
	kubectl(t, "", "-n", a.ns, "delete", "restorerun", "copy", "--wait=true", "--timeout=2m")

	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: read-copy
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: read
      image: %s
      command: [cat, /copy/note]
      volumeMounts: [{name: copy, mountPath: /copy}]
  volumes:
    - name: copy
      persistentVolumeClaim: {claimName: copy}
`, a.ns, appImage))
	waitFor(t, "the pod read-copy to read the restored file", 5*time.Minute, func() (bool, string) {
		var pod struct {
			Status struct{ Phase string } `json:"status"`
		}
		if err := getJSON(a.ns, "pod", "read-copy", &pod); err != nil {
			return false, err.Error()
		}
		return pod.Status.Phase == "Succeeded", pod.Status.Phase
	}, a.describe)
	if got := strings.TrimSpace(a.logs(a.ns, "pod/read-copy")); got != "backed up" {
		t.Errorf("claim copy holds %q, want %q", got, "backed up")
	}
	a.waitNote("the app's own claim to keep its file", "live")
}

// TestThePopulatorFillsARecreatedClaim backs up the app's claim, then
// deletes the claim and the Deployment, as a rebuild of the namespace does.
// Flux creates both again, and the populator fills the new claim from the
// newest snapshot before the app can mount it.
func TestThePopulatorFillsARecreatedClaim(t *testing.T) {
	t.Parallel()
	a := newApp(t, "populator")
	a.publish(volumeManifests())

	a.write("before the rebuild")
	backedUp := a.backup("base", "source: data")
	mustSucceed(t, "BackupRun", "base", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)

	kubectl(t, "", "-n", a.ns, "delete", "deployment", "app", "--wait=true", "--timeout=2m")
	kubectl(t, "", "-n", a.ns, "delete", "pvc", "data", "--wait=true", "--timeout=3m")
	a.reconcile()
	a.waitNote("the app to read the file from the filled claim", "before the rebuild")
}

// TestAFailedPopulatorRestoreRunsAgainOnceItsJobIsDeleted fills a
// recreated claim while the VolumeRestore's moverSecurityContext asks for a
// sysctl the kubelet forbids, so every pod of the restore Job is rejected and
// the Job fails. The claim stays Pending and the VolumeRestore reports
// RestoreFailed. After the setting is fixed in the app's manifests, the
// procedure in docs/operations.md, deleting the failed Job, makes the
// populator select the snapshot again and fill the claim.
func TestAFailedPopulatorRestoreRunsAgainOnceItsJobIsDeleted(t *testing.T) {
	t.Parallel()
	a := newApp(t, "populator-retry")
	a.publish(volumeManifests())
	a.write("kept")
	backedUp := a.backup("base", "source: data")
	mustSucceed(t, "BackupRun", "base", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)

	forbidden := strings.Replace(volumeManifests(), "  cacheCapacity: 100Mi\n",
		"  cacheCapacity: 100Mi\n  moverSecurityContext:\n    sysctls: [{name: kernel.msgmax, value: \"1024\"}]\n", 1)
	a.publish(forbidden)
	kubectl(t, "", "-n", a.ns, "delete", "deployment", "app", "--wait=true", "--timeout=2m")
	kubectl(t, "", "-n", a.ns, "delete", "pvc", "data", "--wait=true", "--timeout=3m")
	a.reconcile()
	waitFor(t, "the VolumeRestore to report the failed restore", 8*time.Minute, func() (bool, string) {
		var vr backupv1alpha1.VolumeRestore
		if err := getJSON(a.ns, "volumerestore", "data", &vr); err != nil {
			return false, err.Error()
		}
		reason := readyReason(vr.Status.Conditions)
		return reason == backupv1alpha1.ReasonRestoreFailed, reason
	}, a.describe)
	var claim struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
		Status   struct{ Phase string } `json:"status"`
	}
	if err := getJSON(a.ns, "pvc", "data", &claim); err != nil {
		t.Fatal(err)
	}
	if claim.Status.Phase == "Bound" {
		t.Fatalf("claim data is Bound although its restore failed\n%s", a.describe())
	}

	a.publish(volumeManifests())
	waitFor(t, "Flux to remove the forbidden sysctl", 3*time.Minute, func() (bool, string) {
		var vr backupv1alpha1.VolumeRestore
		if err := getJSON(a.ns, "volumerestore", "data", &vr); err != nil {
			return false, err.Error()
		}
		return vr.Spec.MoverSecurityContext == nil, fmt.Sprintf("moverSecurityContext %+v", vr.Spec.MoverSecurityContext)
	}, a.describe)
	kubectl(t, "", "-n", controllerNamespace, "delete", "job", "restore-"+string(claim.Metadata.UID), "--wait=true", "--timeout=2m")
	a.waitNote("the app to read the file from the claim filled on the second try", "kept")
}
