//go:build e2e && demo

package e2e

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// TestDemo shows the controller working end to end on the e2e cluster, for a
// person watching `make demo`. It runs only with the demo build tag, so `make
// e2e` leaves it out.
//
// It sets up a namespace the way prod does, with an app that writes a known
// file and the moment it wrote it into its claim, and gives the namespace a
// schedule whose one tick falls a minute or two ahead. It waits for the
// scheduler's BackupRun, lists the snapshot from RustFS with the mover's
// restic release, changes the file, restores the claim in place with a
// RestoreRun that quiesces the app, and checks the file is back and the app
// runs again. The namespace and the repository prefix are deleted at the end.
//
// A step that fails stops the demo with the run's status, the namespace's
// events and the controller's log.
func TestDemo(t *testing.T) {
	start := time.Now()
	step := func(n int, format string, args ...any) {
		t.Logf("")
		t.Logf("==== step %d (%s in): %s", n, time.Since(start).Round(time.Second), fmt.Sprintf(format, args...))
	}

	// Cleanups run last in, first out, so this one reports after the
	// namespace and the repository prefix are gone.
	t.Cleanup(func() {
		t.Logf("==== cleaned up; the demo took %s", time.Since(start).Round(time.Second))
	})

	step(1, "create a demo namespace, its restic repository, a claim and an app writing into it")
	ns := newTestNamespace(t, "demo")
	repo := newResticRepo(t, "demo/"+ns.Name+"/data")
	evidence := func() string { return runEvidence(ns.Name) }
	t.Logf("namespace %s, repository s3://%s/%s", ns.Name, volsyncBucket, repo.Prefix)

	const (
		claim = "data"
		file  = "demo.txt"
	)
	known := "demo data in " + ns.Name
	apply(t, repo.secretManifest(ns.Name, "restic-data")+fmt.Sprintf(`---
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  repository: restic-data
  cacheStorageClassName: e2e-hostpath
  cacheCapacity: 100Mi
  moverPodLabels:
    kueue.x-k8s.io/queue-name: %[3]s
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[2]s
  namespace: %[1]s
  annotations:
    backup.wlz.li/enabled: "true"
    backup.wlz.li/retain-last: "3"
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: e2e-hostpath
  resources:
    requests:
      storage: 100Mi
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: app
  namespace: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels: {app: demo}
  template:
    metadata:
      labels: {app: demo}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: app
          image: quay.io/backube/volsync:0.16.0
          imagePullPolicy: IfNotPresent
          command: [/bin/sh, -ec]
          # The app writes its file once, on an empty claim, so a restart
          # after the restore leaves the restored file as it is.
          args:
            - |
              [ -f /data/%[4]s ] || printf '%%s, written at %%s\n' "$KNOWN" "$(date -u +%%Y-%%m-%%dT%%H:%%M:%%SZ)" > /data/%[4]s
              sync
              exec sleep infinity
          env:
            - {name: KNOWN, value: %[5]q}
          volumeMounts:
            - {name: data, mountPath: /data}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: %[2]s}
`, ns.Name, claim, ns.Queue, file, known))

	var original string
	waitFor(t, "the app to write "+file, 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/app", "--", "cat", "/data/"+file)
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		if !strings.HasPrefix(out, known+", written at ") {
			return false, fmt.Sprintf("file holds %q", out), nil
		}
		original = out
		return true, "written", nil
	}, evidence)
	t.Logf("the app wrote /data/%s: %s", file, strings.TrimSpace(original))

	// The scheduler counts ticks from the namespace's creation, so one fixed
	// minute ahead fires once. It lies at least 45 seconds ahead, so the
	// schedule is in place well before the tick.
	tick := time.Now().UTC().Add(45 * time.Second).Truncate(time.Minute).Add(time.Minute)
	schedule := fmt.Sprintf("%d %d %d %d *", tick.Minute(), tick.Hour(), tick.Day(), int(tick.Month()))
	mustKubectl(t, "", "annotate", "namespace", ns.Name, "--overwrite", backupv1alpha1.AnnotationSchedule+"="+schedule)
	t.Logf("namespace %s: %s=%q, one tick at %s (in %s)", ns.Name, backupv1alpha1.AnnotationSchedule, schedule,
		tick.Format(time.RFC3339), time.Until(tick).Round(time.Second))

	runName := "scheduled-" + tick.Format("20060102-1504")
	step(2, "wait for the scheduler to create BackupRun %s at the tick, and for it to succeed", runName)
	var run backupv1alpha1.BackupRun
	waitFor(t, "BackupRun "+runName, time.Until(tick)+8*time.Minute, 3*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "backuprun", runName, "-o", "json", "--ignore-not-found")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if strings.TrimSpace(out) == "" {
			return false, "not created yet", nil
		}
		run = backupv1alpha1.BackupRun{}
		if err := json.Unmarshal([]byte(out), &run); err != nil {
			return false, "", fmt.Errorf("decode BackupRun: %w", err)
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions)), nil
	}, evidence)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("scheduled BackupRun %s ended %s: %s\n%s", runName, run.Status.Phase, readyMessage(run.Status.Conditions), evidence())
	}
	if len(run.Status.Items) != 1 || run.Status.Items[0].Snapshot == "" || run.Status.Items[0].SnapshotTime == nil {
		t.Fatalf("scheduled BackupRun %s items = %+v, want one item with a snapshot and its time\n%s", runName, run.Status.Items, evidence())
	}
	item := run.Status.Items[0]
	t.Logf("BackupRun %s succeeded: %s %s saved snapshot %s at %s", runName, item.Kind, item.Name, item.Snapshot,
		item.SnapshotTime.UTC().Format(time.RFC3339))

	snaps := repo.snapshots(t)
	var snap *resticSnapshot
	t.Logf("restic %s snapshots in s3://%s/%s:", resticMoverVersion, volsyncBucket, repo.Prefix)
	for i := range snaps {
		t.Logf("  %s  %s  paths=%v tags=%v", snaps[i].ShortID, snaps[i].Time.UTC().Format(time.RFC3339), snaps[i].Paths, snaps[i].Tags)
		if snaps[i].ShortID == item.Snapshot || strings.HasPrefix(snaps[i].ID, item.Snapshot) {
			snap = &snaps[i]
		}
	}
	if snap == nil {
		t.Fatalf("the repository holds no snapshot %s\n%s", item.Snapshot, evidence())
	}
	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	saved, err := repo.restic(t, endpoint, "dump", "--no-lock", snap.ID, "/"+file)
	if err != nil {
		t.Fatalf("restic dump %s /%s: %v", snap.ID, file, err)
	}
	t.Logf("snapshot %s holds /%s: %s", snap.ShortID, file, strings.TrimSpace(saved))
	if saved != original {
		t.Fatalf("snapshot %s holds %q, the app wrote %q", snap.ShortID, saved, original)
	}

	step(3, "change the data in the app, so the restore has something to undo")
	changed := "changed after the backup, at " + time.Now().UTC().Format(time.RFC3339)
	mustKubectl(t, "", "-n", ns.Name, "exec", "deploy/app", "--", "/bin/sh", "-ec",
		fmt.Sprintf(`printf '%%s\n' "$1" > /data/%s && sync`, file), "sh", changed)
	now := mustKubectl(t, "", "-n", ns.Name, "exec", "deploy/app", "--", "cat", "/data/"+file)
	t.Logf("/data/%s now holds: %s", file, strings.TrimSpace(now))
	if now == original {
		t.Fatalf("the file did not change")
	}

	// restic stamps the snapshot to the nanosecond and the run records whole
	// seconds, so one second past the recorded time still selects it, and
	// no later snapshot exists.
	asOf := item.SnapshotTime.UTC().Add(time.Second).Format(time.RFC3339)
	step(4, "restore claim %s in place to snapshot %s (restoreAsOf %s), quiescing Deployment app", claim, snap.ShortID, asOf)
	const restoreName = "demo-restore"
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: %s
  namespace: %s
spec:
  claim: %s
  restoreAsOf: %q
  quiesce:
    - {kind: Deployment, name: app}
  timeout: 8m
`, restoreName, ns.Name, claim, asOf))
	var restore backupv1alpha1.RestoreRun
	waitFor(t, "RestoreRun "+restoreName, 10*time.Minute, 3*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "restorerun", restoreName, "-o", "json")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		restore = backupv1alpha1.RestoreRun{}
		if err := json.Unmarshal([]byte(out), &restore); err != nil {
			return false, "", fmt.Errorf("decode RestoreRun: %w", err)
		}
		state := fmt.Sprintf("phase=%s %s", restore.Status.Phase, readyMessage(restore.Status.Conditions))
		if restore.Status.QuiescedAt != nil {
			state += " (app quiesced)"
		}
		if restore.Status.RestartedAt != nil {
			state += " (app restarted)"
		}
		return restore.Status.Phase.Finished(), state, nil
	}, evidence)
	if restore.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("RestoreRun %s ended %s: %s\n%s", restoreName, restore.Status.Phase, readyMessage(restore.Status.Conditions), evidence())
	}
	for _, it := range restore.Status.Items {
		t.Logf("restored %s %s: phase=%s snapshot=%s %s", it.Kind, it.Name, it.Phase, it.Snapshot, it.Message)
		if it.Snapshot != "" && it.Snapshot != item.Snapshot && !strings.HasPrefix(snap.ID, it.Snapshot) {
			t.Errorf("RestoreRun restored snapshot %s, want %s", it.Snapshot, snap.ShortID)
		}
	}

	step(5, "check the restored file equals the original and the app runs again")
	waitFor(t, "Deployment app to run again", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "deploy", "app", "-o", "jsonpath={.spec.replicas}/{.status.readyReplicas}")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		return out == "1/1", "replicas/ready = " + out, nil
	}, evidence)
	var restored string
	waitFor(t, "the restored "+file, time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/app", "--", "cat", "/data/"+file)
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		restored = out
		return true, "read", nil
	}, evidence)
	t.Logf("/data/%s after the restore: %s", file, strings.TrimSpace(restored))
	if restored != original {
		t.Fatalf("restored file holds %q, want the original %q\n%s", restored, original, evidence())
	}
	t.Logf("the restored file equals the original, and the app runs again")

	step(6, "clean up: delete namespace %s and s3://%s/%s/ (the test's cleanups do both)", ns.Name, volsyncBucket, repo.Prefix)
}
