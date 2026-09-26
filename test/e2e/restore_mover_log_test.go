//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
)

// TestRestoreMoverLogNamesTheSnapshot records what VolSync 0.16.0 writes to a
// ReplicationDestination's status.latestMoverStatus after a restore, which a
// RestoreRun reads to confirm the snapshot its mover restored. The
// destinations are built the way the controller's directDestination builds
// them: Direct into a claim, pinned by restoreAsOf to a whole second, with
// file deletion and the queue label.
//
// It checks two restores of one backup. Pinned to the snapshot's second, the
// log carries restic's "restoring snapshot <short ID> of [/data] at ..."
// line. Pinned an hour before it, the mover exits 0 with "No eligible
// snapshots found", and VolSync still completes the trigger. The logs are
// printed with -v; internal/runs/restorerun_moverlog_test.go holds a copy.
func TestRestoreMoverLogNamesTheSnapshot(t *testing.T) {
	ns := newTestNamespace(t, "e2e-restore-log")
	repo := newResticRepo(t, "e2e/"+ns.Name+"/data")
	evidence := func() string { return runEvidence(ns.Name) }

	apply(t, repo.secretManifest(ns.Name, "restic-data")+fmt.Sprintf(`---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
  namespace: %[1]s
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
            - echo restored > /data/known.txt && sync && exec sleep infinity
          volumeMounts:
            - {name: data, mountPath: /data}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: data}
`, ns.Name))

	waitFor(t, "the writer to write known.txt", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/writer", "--", "cat", "/data/known.txt")
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		return strings.TrimSpace(out) == "restored", "written", nil
	}, evidence)

	apply(t, fmt.Sprintf(`apiVersion: volsync.backube/v1alpha1
kind: ReplicationSource
metadata:
  name: data
  namespace: %[1]s
spec:
  sourcePVC: data
  trigger:
    manual: backup-1
  restic:
    repository: restic-data
    copyMethod: Direct
    cacheStorageClassName: e2e-hostpath
    cacheCapacity: 100Mi
    retain:
      last: "3"
    moverPodLabels:
      kueue.x-k8s.io/queue-name: %[2]s
`, ns.Name, ns.Queue))
	waitFor(t, "the backup", 6*time.Minute, 3*time.Second, func() (bool, string, error) {
		var source volsyncv1alpha1.ReplicationSource
		if err := getJSON(ns.Name, "replicationsource", "data", &source); err != nil {
			return false, firstLine(err.Error()), nil
		}
		if source.Status == nil {
			return false, "no status", nil
		}
		return source.Status.LastManualSync == "backup-1", "lastManualSync=" + source.Status.LastManualSync, nil
	}, evidence)

	snaps := repo.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("repository holds %d snapshots, want 1: %+v", len(snaps), snaps)
	}
	snap := snaps[0]
	t.Logf("snapshot %s at %s, paths %v", snap.ShortID, snap.Time.Format(time.RFC3339Nano), snap.Paths)

	restore := func(name, restoreAsOf string) *volsyncv1alpha1.MoverStatus {
		t.Helper()
		apply(t, fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: e2e-hostpath
  resources:
    requests:
      storage: 100Mi
---
apiVersion: volsync.backube/v1alpha1
kind: ReplicationDestination
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  trigger:
    manual: restore-1
  restic:
    repository: restic-data
    copyMethod: Direct
    destinationPVC: %[2]s
    restoreAsOf: %[3]q
    cacheStorageClassName: e2e-hostpath
    cacheCapacity: 100Mi
    enableFileDeletion: true
    cleanupCachePVC: true
    moverPodLabels:
      kueue.x-k8s.io/queue-name: %[4]s
`, ns.Name, name, restoreAsOf, ns.Queue))
		var destination volsyncv1alpha1.ReplicationDestination
		waitFor(t, "restore "+name, 6*time.Minute, 3*time.Second, func() (bool, string, error) {
			destination = volsyncv1alpha1.ReplicationDestination{}
			if err := getJSON(ns.Name, "replicationdestination", name, &destination); err != nil {
				return false, firstLine(err.Error()), nil
			}
			if destination.Status == nil {
				return false, "no status", nil
			}
			return destination.Status.LastManualSync == "restore-1", "lastManualSync=" + destination.Status.LastManualSync, nil
		}, evidence)
		mover := destination.Status.LatestMoverStatus
		if mover == nil {
			t.Fatalf("destination %s completed its trigger with no latestMoverStatus\n%s", name, evidence())
		}
		t.Logf("destination %s latestMoverStatus.result: %s", name, mover.Result)
		t.Logf("destination %s latestMoverStatus.logs:\n%s", name, mover.Logs)
		return mover
	}

	pin := snap.Time.UTC().Format(time.RFC3339)
	found := restore("found", pin)
	want := regexp.MustCompile(`(?m)^restoring snapshot ` + regexp.QuoteMeta(snap.ShortID) + ` of \[/data\] at `)
	if found.Result != volsyncv1alpha1.MoverResultSuccessful || !want.MatchString(found.Logs) {
		t.Errorf("restore pinned to %s: result %s, logs without %q:\n%s", pin, found.Result, want, found.Logs)
	}

	before := snap.Time.Add(-time.Hour).UTC().Format(time.RFC3339)
	none := restore("none", before)
	if none.Result != volsyncv1alpha1.MoverResultSuccessful || !strings.Contains(none.Logs, "No eligible snapshots found") {
		t.Errorf("restore pinned to %s: result %s, logs without \"No eligible snapshots found\":\n%s", before, none.Result, none.Logs)
	}
	if strings.Contains(none.Logs, "restoring") {
		t.Errorf("restore pinned to %s names a snapshot:\n%s", before, none.Logs)
	}
}

// getJSON reads one object of kind named name in namespace into into.
func getJSON(namespace, kind, name string, into any) error {
	out, err := kubectlQuick(namespace, "get", kind, name, "-o", "json")
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(out), into)
}
