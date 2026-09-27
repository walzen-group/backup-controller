//go:build e2e

package e2e

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// TestAnIntoRestoreFromAClaimFillsAPlainClaimOnTheSourcesNode checks a
// RestoreRun with spec.into against the real CSI driver, scheduler, Kueue and
// restic. The run creates a plain claim with the source claim's size, class
// and volume.kubernetes.io/selected-node and no data source, and fills it
// with its own restore Job, which mounts the claim as its first consumer.
// The class binds WaitForFirstConsumer.
//
// The test backs up a claim a running writer mounts with a BackupRun, then
// restores that snapshot into a new claim while the writer keeps running.
// The run must succeed with the snapshot's full ID; its Job must restore that
// ID into the new claim; the namespace must hold no ReplicationDestination;
// the new claim must be Bound on the source claim's node; and a pod reading
// it must find the file the writer wrote, while the writer's claim is left
// as it was.
func TestAnIntoRestoreFromAClaimFillsAPlainClaimOnTheSourcesNode(t *testing.T) {
	ns := newTestNamespace(t, "e2e-restore-into")
	repo := newResticRepo(t, "e2e/"+ns.Name+"/data")
	evidence := func() string { return runEvidence(ns.Name) }
	image := pinnedResticImage(t)

	apply(t, repo.secretManifest(ns.Name, "restic-data")+fmt.Sprintf(`---
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: data
  namespace: %[1]s
spec:
  repository: restic-data
  cacheStorageClassName: e2e-hostpath
  cacheCapacity: 100Mi
  moverPodLabels:
    kueue.x-k8s.io/queue-name: %[2]s
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data
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
          image: %[3]s
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
`, ns.Name, ns.Queue, image))

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

	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: backup
  namespace: %s
spec:
  source: data
  timeout: 8m
`, ns.Name))
	waitFor(t, "BackupRun backup to end", 10*time.Minute, 3*time.Second, func() (bool, string, error) {
		run, err := readBackupRun(t, ns.Name, "backup")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run == nil {
			return false, "not created yet", nil
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions)), nil
	}, evidence)
	if run, _ := readBackupRun(t, ns.Name, "backup"); run == nil || run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun backup did not succeed\n%s", evidence())
	}

	snaps := repo.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("repository holds %d snapshots, want 1: %+v", len(snaps), snaps)
	}
	snap := snaps[0]
	t.Logf("snapshot %s at %s", snap.ID, snap.Time.Format(time.RFC3339Nano))

	jobs := watchObjects[batchv1.Job](t, ns.Name, "jobs", "app.kubernetes.io/component=restore")
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: into
  namespace: %s
spec:
  claim: data
  into: copy
  timeout: 8m
`, ns.Name))
	var run *backupv1alpha1.RestoreRun
	waitFor(t, "RestoreRun into to finish", 10*time.Minute, 2*time.Second, func() (bool, string, error) {
		var err error
		run, err = readRestoreRun(t, ns.Name, "into")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run == nil {
			return false, "not created yet", nil
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s items=%s", run.Status.Phase, readyMessage(run.Status.Conditions), itemStates(run.Status.Items)), nil
	}, evidence)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("RestoreRun into ended %s: %s; items %s\n%s", run.Status.Phase, readyMessage(run.Status.Conditions), itemStates(run.Status.Items), evidence())
	}
	item := oneVolumeItem(t, run, evidence)
	if item.SnapshotID != snap.ID {
		t.Errorf("item snapshotID = %s, want %s", item.SnapshotID, snap.ID)
	}
	wantJob := fmt.Sprintf("restore-%s-0", run.UID)
	if item.Job != wantJob {
		t.Errorf("item job = %q, want %q", item.Job, wantJob)
	}
	job, seen := jobs.latest()[wantJob]
	if !seen {
		t.Fatalf("no restore Job %s was seen in the namespace; seen %v\n%s", wantJob, jobs.latest(), evidence())
	}
	args := job.Spec.Template.Spec.Containers[0].Args
	if job.Annotations["backup.wlz.li/snapshot-id"] != snap.ID || job.Annotations["backup.wlz.li/claim"] != "copy" ||
		!slices.Contains(args, snap.ID) || !slices.Contains(args, "--delete") {
		t.Errorf("Job %s annotations %v, args %v; want snapshot %s restored into claim copy with --delete", wantJob, job.Annotations, args, snap.ID)
	}
	if out, err := kubectlQuick(ns.Name, "get", "replicationdestinations", "-o", "name"); err != nil || strings.TrimSpace(out) != "" {
		t.Errorf("the namespace holds ReplicationDestinations %q (%v), want none", out, err)
	}

	var copyClaim corev1.PersistentVolumeClaim
	if err := getJSON(ns.Name, "pvc", "copy", &copyClaim); err != nil {
		t.Fatal(err)
	}
	if copyClaim.Status.Phase != corev1.ClaimBound || copyClaim.Annotations["volume.kubernetes.io/selected-node"] != node || copyClaim.Spec.DataSourceRef != nil {
		t.Fatalf("claim copy phase %s, annotations %v, dataSourceRef %v; want Bound on %s with no data source\n%s",
			copyClaim.Status.Phase, copyClaim.Annotations, copyClaim.Spec.DataSourceRef, node, evidence())
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
      image: %[2]s
      imagePullPolicy: IfNotPresent
      command: [/bin/sh, -ec, "cat /data/known.txt"]
      volumeMounts:
        - {name: copy, mountPath: /data}
      resources:
        requests: {cpu: 10m, memory: 16Mi}
  volumes:
    - name: copy
      persistentVolumeClaim: {claimName: copy}
`, ns.Name, image))
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
