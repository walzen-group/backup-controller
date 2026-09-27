//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// TestVolumeBackup backs up one claim with a BackupRun on spec.source and
// checks the snapshot the run records is in the restic repository in RustFS,
// holding the file the app wrote.
//
// The namespace is set up the way the infrastructure repository sets one up:
// kueue-managed, a schedule, a LocalQueue, the restic repository Secret, a
// VolumeRestore bound to the claim by name, and a claim marked
// backup.wlz.li/enabled. A small Deployment writes a file of known content
// into the claim before the run starts.
func TestVolumeBackup(t *testing.T) {
	ns := newTestNamespace(t, "e2e-volume-backup")
	repo := newResticRepo(t, "e2e/"+ns.Name+"/data")
	evidence := func() string { return runEvidence(ns.Name) }

	const (
		claim   = "data"
		file    = "e2e-known.txt"
		content = "written by TestVolumeBackup"
	)
	known := fmt.Sprintf("%s in %s", content, ns.Name)

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
  name: writer
  namespace: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels: {app: writer}
  template:
    metadata:
      labels: {app: writer}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: writer
          image: quay.io/backube/volsync:0.16.0
          imagePullPolicy: IfNotPresent
          command: [/bin/sh, -ec]
          args:
            - printf '%%s' "$KNOWN" > /data/%[4]s && sync && exec sleep infinity
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

	waitFor(t, "the writer to write "+file, 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/writer", "--", "cat", "/data/"+file)
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		if out != known {
			return false, fmt.Sprintf("file holds %q", out), nil
		}
		return true, "written", nil
	}, evidence)

	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: e2e-data
  namespace: %s
spec:
  source: %s
  timeout: 8m
`, ns.Name, claim))

	var run backupv1alpha1.BackupRun
	waitFor(t, "BackupRun e2e-data to end", 10*time.Minute, 3*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "backuprun", "e2e-data", "-o", "json")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		run = backupv1alpha1.BackupRun{}
		if err := json.Unmarshal([]byte(out), &run); err != nil {
			return false, "", fmt.Errorf("decode BackupRun: %w", err)
		}
		state := fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions))
		ended := run.Status.Phase == backupv1alpha1.RunPhaseSucceeded || run.Status.Phase == backupv1alpha1.RunPhaseFailed
		return ended, state, nil
	}, evidence)

	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun ended %s: %s\n%s", run.Status.Phase, readyMessage(run.Status.Conditions), evidence())
	}
	if len(run.Status.Items) != 1 {
		t.Fatalf("BackupRun has %d items, want 1: %+v\n%s", len(run.Status.Items), run.Status.Items, evidence())
	}
	item := run.Status.Items[0]
	if item.Kind != "ReplicationSource" || item.Name != claim || item.Phase != backupv1alpha1.ItemSucceeded {
		t.Fatalf("item = %+v, want ReplicationSource %s Succeeded\n%s", item, claim, evidence())
	}
	if item.Snapshot == "" {
		t.Fatalf("item records no snapshot: %+v\n%s", item, evidence())
	}
	if item.SnapshotTime == nil {
		t.Errorf("item records no snapshotTime: %+v", item)
	}
	t.Logf("run recorded snapshot %s at %v", item.Snapshot, item.SnapshotTime)

	snaps := repo.snapshots(t)
	var found *resticSnapshot
	for i := range snaps {
		if snaps[i].ShortID == item.Snapshot || strings.HasPrefix(snaps[i].ID, item.Snapshot) {
			found = &snaps[i]
		}
	}
	if found == nil {
		t.Fatalf("restic repository s3:%s/%s holds no snapshot %s; it holds %+v\n%s",
			volsyncBucket, repo.Prefix, item.Snapshot, snaps, evidence())
	}
	// snapshotTime is restic's stamp in whole seconds.
	if item.SnapshotTime != nil && item.SnapshotTime.Time.Sub(found.Time).Abs() >= time.Second {
		t.Errorf("item snapshotTime %v, restic stamped %v", item.SnapshotTime.Time, found.Time)
	}

	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	// VolSync's mover runs restic backup . inside /data, so the snapshot
	// records the path /data and holds the claim's files at its root.
	path := "/" + file
	got, err := repo.restic(t, endpoint, "dump", "--no-lock", found.ID, path)
	if err != nil {
		ls, _ := repo.restic(t, endpoint, "ls", "--no-lock", found.ID)
		t.Fatalf("restic dump %s %s: %v\nsnapshot holds:\n%s", found.ID, path, err, ls)
	}
	if got != known {
		t.Errorf("snapshot's %s holds %q, want %q", path, got, known)
	}
}

// kubectlQuick runs one kubectl command in namespace with a 30 second limit.
func kubectlQuick(namespace string, args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return kubectl(ctx, "", append([]string{"-n", namespace}, args...)...)
}

// readyMessage returns the Ready condition's reason and message.
func readyMessage(conds []metav1.Condition) string {
	for _, c := range conds {
		if c.Type == "Ready" {
			return fmt.Sprintf("%s: %s", c.Reason, c.Message)
		}
	}
	return "(no Ready condition)"
}

// firstLine returns s up to its first newline.
func firstLine(s string) string {
	line, _, _ := strings.Cut(s, "\n")
	return line
}

// lastLine returns the last line of s that holds anything but spaces,
// trimmed, or "" when there is none.
func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}
