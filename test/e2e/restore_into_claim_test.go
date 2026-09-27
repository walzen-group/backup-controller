//go:build e2e

package e2e

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// TestAnIntoRestoreFromAClaimFillsAPlainClaimOnTheSourcesNode checks what an
// into restore from a claim relies on, with the real CSI driver, scheduler,
// Kueue and VolSync 0.16.0: a plain claim with the source claim's size, class
// and volume.kubernetes.io/selected-node and no data source, and a
// ReplicationDestination in the app's namespace with copyMethod Direct that
// writes into it. The class binds WaitForFirstConsumer and no pod but the
// mover ever uses the new claim.
//
// No controller runs here. The test builds every object by hand, the backup
// included: a ReplicationSource with a manual trigger stands in for a
// BackupRun, and there is no RestoreRun and no VolumeRestore. The claim
// carries the spec fields scratchClaim writes, and has no ownerReference to
// a run. The destination, under the fixed name restore-copy-0 and trigger
// restore-1, stands in for the controller's restore Job: that Job mounts the
// claim by name as its first consumer in the same way. So the test shows
// what the scheduler and the CSI driver do with such a claim. The run's own
// steps for an into restore (its checks, Leases, restore Job and cleanup)
// are covered by the unit tests only: the e2e tests that create a
// RestoreRun restore in place.
//
// The test backs up a claim a running writer mounts, then restores the
// snapshot into the new claim while the writer keeps running. The mover must
// complete the trigger with a log naming the snapshot, the new claim must be
// Bound on the source claim's node, and a pod reading it must find the file
// the writer wrote.
func TestAnIntoRestoreFromAClaimFillsAPlainClaimOnTheSourcesNode(t *testing.T) {
	ns := newTestNamespace(t, "e2e-restore-into")
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

	var source corev1.PersistentVolumeClaim
	if err := getJSON(ns.Name, "pvc", "data", &source); err != nil {
		t.Fatal(err)
	}
	node := source.Annotations["volume.kubernetes.io/selected-node"]
	if node == "" {
		t.Fatalf("claim data carries no selected-node annotation: %v", source.Annotations)
	}
	t.Logf("claim data is on node %s", node)

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
	pin := snap.Time.UTC().Format(time.RFC3339)
	t.Logf("snapshot %s at %s", snap.ShortID, snap.Time.Format(time.RFC3339Nano))

	// The claim with the spec fields scratchClaim writes for spec.claim:
	// data, spec.into: copy, and the destination that fills it in place of
	// the restore Job, built by hand under a fixed name and trigger (see the
	// test's comment).
	apply(t, fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: copy
  namespace: %[1]s
  annotations:
    volume.kubernetes.io/selected-node: %[2]s
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
  name: restore-copy-0
  namespace: %[1]s
spec:
  trigger:
    manual: restore-1
  restic:
    repository: restic-data
    copyMethod: Direct
    destinationPVC: copy
    restoreAsOf: %[3]q
    cacheStorageClassName: e2e-hostpath
    cacheCapacity: 100Mi
    enableFileDeletion: true
    cleanupCachePVC: true
    moverPodLabels:
      kueue.x-k8s.io/queue-name: %[4]s
`, ns.Name, node, pin, ns.Queue))

	var destination volsyncv1alpha1.ReplicationDestination
	waitFor(t, "the restore into claim copy", 6*time.Minute, 3*time.Second, func() (bool, string, error) {
		destination = volsyncv1alpha1.ReplicationDestination{}
		if err := getJSON(ns.Name, "replicationdestination", "restore-copy-0", &destination); err != nil {
			return false, firstLine(err.Error()), nil
		}
		if destination.Status == nil {
			return false, "no status", nil
		}
		return destination.Status.LastManualSync == "restore-1", "lastManualSync=" + destination.Status.LastManualSync, nil
	}, evidence)
	mover := destination.Status.LatestMoverStatus
	want := regexp.MustCompile(`(?m)^restoring snapshot ` + regexp.QuoteMeta(snap.ShortID) + ` of \[/data\] at `)
	if mover == nil || mover.Result != volsyncv1alpha1.MoverResultSuccessful || !want.MatchString(mover.Logs) {
		t.Fatalf("mover status %+v, want Successful with a log naming %s\n%s", mover, snap.ShortID, evidence())
	}
	t.Logf("mover logs:\n%s", mover.Logs)

	var copyClaim corev1.PersistentVolumeClaim
	if err := getJSON(ns.Name, "pvc", "copy", &copyClaim); err != nil {
		t.Fatal(err)
	}
	if copyClaim.Status.Phase != corev1.ClaimBound || copyClaim.Annotations["volume.kubernetes.io/selected-node"] != node {
		t.Fatalf("claim copy phase %s, annotations %v; want Bound on %s\n%s", copyClaim.Status.Phase, copyClaim.Annotations, node, evidence())
	}
	var volume corev1.PersistentVolume
	if err := getJSON("", "pv", copyClaim.Spec.VolumeName, &volume); err != nil {
		t.Fatal(err)
	}
	if affinity := fmt.Sprint(volume.Spec.NodeAffinity); !strings.Contains(affinity, node) {
		t.Errorf("volume %s node affinity %s, want it on %s", volume.Name, affinity, node)
	}

	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: reader
  namespace: %[1]s
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  containers:
    - name: reader
      image: quay.io/backube/volsync:0.16.0
      imagePullPolicy: IfNotPresent
      command: [/bin/sh, -ec, "cat /data/known.txt"]
      volumeMounts:
        - {name: copy, mountPath: /data}
      resources:
        requests: {cpu: 10m, memory: 16Mi}
  volumes:
    - name: copy
      persistentVolumeClaim: {claimName: copy}
`, ns.Name))
	waitFor(t, "the reader to read claim copy", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		var pod corev1.Pod
		if err := getJSON(ns.Name, "pod", "reader", &pod); err != nil {
			return false, firstLine(err.Error()), nil
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return true, "succeeded", nil
		case corev1.PodFailed:
			return false, "failed", fmt.Errorf("the reader failed")
		default:
			return false, string(pod.Status.Phase), nil
		}
	}, evidence)
	out, err := kubectlQuick(ns.Name, "logs", "reader")
	if err != nil || strings.TrimSpace(out) != "restored" {
		t.Fatalf("claim copy holds %q (%v), want the writer's file", out, err)
	}
	if out, err := kubectlQuick(ns.Name, "exec", "deploy/writer", "--", "cat", "/data/known.txt"); err != nil || strings.TrimSpace(out) != "restored" {
		t.Errorf("the writer's claim reads %q (%v) after the restore, want it untouched", out, err)
	}
}
