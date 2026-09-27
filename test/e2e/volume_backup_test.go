//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// TestVolumeBackup backs up one claim with a BackupRun on spec.source and
// checks the run records the one snapshot its sync wrote to the restic
// repository in RustFS, holding the file the app wrote.
//
// The namespace is set up the way the infrastructure repository sets one up:
// kueue-managed, a schedule, a LocalQueue, the restic repository Secret, a
// VolumeRestore bound to the claim by name, and a claim marked
// backup.wlz.li/enabled. A small Deployment writes a file of known content
// into the claim before the run starts.
//
// hack/e2e/volsync runs VolSync with MOVER_LOG_MAX_BYTES=0, so the mover's
// log never reaches the ReplicationSource. The test checks the log is empty
// and that the run still found the snapshot by listing the repository: the
// repository is new, so the snapshot the sync wrote is the only one in it,
// and the item's snapshotID must be that snapshot's full ID.
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

	apply(t, repo.secretManifest(ns.Name, "restic-data")+backupClaimManifest(ns, claim)+fmt.Sprintf(`---
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
            - printf '%%s' "$KNOWN" > /data/%[3]s && sync && exec sleep infinity
          env:
            - {name: KNOWN, value: %[4]q}
          volumeMounts:
            - {name: data, mountPath: /data}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: %[2]s}
`, ns.Name, claim, file, known))

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

	run := runSourceBackup(t, ns.Name, "e2e-data", claim, nil, evidence)

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
	if item.Snapshot == "" || item.SnapshotID == "" {
		t.Fatalf("item records no snapshot: %+v\n%s", item, evidence())
	}
	if item.SnapshotTime == nil {
		t.Errorf("item records no snapshotTime: %+v", item)
	}
	if item.Empty {
		t.Errorf("item of a claim holding %s says empty: %+v", file, item)
	}
	t.Logf("run recorded snapshot %s (%s) at %v", item.Snapshot, item.SnapshotID, item.SnapshotTime)

	// The run found the snapshot without the mover's log: VolSync stored
	// none on the ReplicationSource.
	if logs := moverLogs(t, ns.Name); logs != "" {
		t.Errorf("the ReplicationSources carry mover logs %q; hack/e2e/volsync sets MOVER_LOG_MAX_BYTES=0 so they carry none", logs)
	}

	snaps := repo.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("the new repository s3:%s/%s holds %d snapshots after one sync, want 1: %+v\n%s",
			volsyncBucket, repo.Prefix, len(snaps), snaps, evidence())
	}
	found := snaps[0]
	if item.SnapshotID != found.ID {
		t.Fatalf("item snapshotID %s, the one snapshot in the repository is %s\n%s", item.SnapshotID, found.ID, evidence())
	}
	if item.Snapshot != found.ShortID {
		t.Errorf("item snapshot %s, the snapshot's short ID is %s", item.Snapshot, found.ShortID)
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

// TestAnEmptyClaimSucceedsEmpty backs up a claim that holds no file and
// checks the item succeeds with empty set, only after two listings of the
// repository that found no snapshot of its sync.
//
// VolSync's mover takes no snapshot of a directory that holds nothing but
// lost+found and still reports success. The run then lists the repository,
// records the first listing that shows no snapshot in noSnapshotListedAt
// with the item still Running, and succeeds the item as empty on a pass at
// least a poll interval (10s) plus one second later. The test polls the run
// every second, so it sees the Running item with noSnapshotListedAt set, and
// checks the run completed no sooner than eleven seconds after that listing.
// The repository gains no snapshot. An idle Deployment mounts the claim and
// writes nothing, since e2e-hostpath binds a claim only once a pod uses it.
func TestAnEmptyClaimSucceedsEmpty(t *testing.T) {
	ns := newTestNamespace(t, "e2e-empty-claim")
	repo := newResticRepo(t, "e2e/"+ns.Name+"/data")
	evidence := func() string { return runEvidence(ns.Name) }

	const claim = "data"
	// e2e-hostpath binds a claim once a pod uses it, so an app that mounts
	// the claim and writes nothing holds it bound.
	apply(t, repo.secretManifest(ns.Name, "restic-data")+backupClaimManifest(ns, claim)+fmt.Sprintf(`---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: idle
  namespace: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels: {app: idle}
  template:
    metadata:
      labels: {app: idle}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: idle
          image: quay.io/backube/volsync:0.16.0
          imagePullPolicy: IfNotPresent
          command: [/bin/sh, -ec, "exec sleep infinity"]
          volumeMounts:
            - {name: data, mountPath: /data}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: %[2]s}
`, ns.Name, claim))
	waitFor(t, "claim "+claim+" to bind with nothing written to it", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/idle", "--", "ls", "-A", "/data")
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		return true, fmt.Sprintf("/data holds %q", strings.TrimSpace(out)), nil
	}, evidence)

	var firstListing *metav1.Time
	run := runSourceBackup(t, ns.Name, "e2e-empty", claim, func(run *backupv1alpha1.BackupRun) {
		for _, item := range run.Status.Items {
			if item.Phase == backupv1alpha1.ItemRunning && item.NoSnapshotListedAt != nil && firstListing == nil {
				listed := *item.NoSnapshotListedAt
				firstListing = &listed
				t.Logf("item Running with noSnapshotListedAt %s: %s", listed.UTC().Format(time.RFC3339), item.Message)
			}
		}
	}, evidence)

	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun ended %s: %s\n%s", run.Status.Phase, readyMessage(run.Status.Conditions), evidence())
	}
	if len(run.Status.Items) != 1 {
		t.Fatalf("BackupRun has %d items, want 1: %+v\n%s", len(run.Status.Items), run.Status.Items, evidence())
	}
	item := run.Status.Items[0]
	t.Logf("item: %+v", item)
	if item.Kind != "ReplicationSource" || item.Name != claim || item.Phase != backupv1alpha1.ItemSucceeded || !item.Empty {
		t.Fatalf("item = %+v, want ReplicationSource %s Succeeded with empty set\n%s", item, claim, evidence())
	}
	if item.Snapshot != "" || item.SnapshotID != "" || item.SnapshotTime != nil {
		t.Errorf("the empty item records snapshot %q (%q) at %v, want none", item.Snapshot, item.SnapshotID, item.SnapshotTime)
	}
	if firstListing == nil {
		t.Fatalf("no poll saw the item Running with noSnapshotListedAt set before it succeeded\n%s", evidence())
	}
	if item.NoSnapshotListedAt == nil || !item.NoSnapshotListedAt.Equal(firstListing) {
		t.Errorf("the item's noSnapshotListedAt is %v, the first listing recorded %v", item.NoSnapshotListedAt, firstListing)
	}
	const relist = 11 * time.Second
	if run.Status.CompletedAt == nil || run.Status.CompletedAt.Time.Before(firstListing.Add(relist)) {
		t.Errorf("the run completed at %v, sooner than %s after the first listing at %v", run.Status.CompletedAt, relist, firstListing)
	}
	if n := snapshotObjects(t, repo); n != 0 {
		t.Errorf("s3://%s/%s/snapshots/ holds %d objects after the backup of an empty claim, want 0", volsyncBucket, repo.Prefix, n)
	}
}

// backupClaimManifest returns the objects a claim needs to be backed up, the
// way the infrastructure repository declares them: a VolumeRestore naming
// the repository Secret restic-data, and a 100Mi claim on e2e-hostpath
// marked backup.wlz.li/enabled. The YAML starts with a document separator,
// so it can follow another manifest.
//
// Parameters:
//   - ns is the test namespace; the VolumeRestore's mover pods go to its
//     LocalQueue.
//   - claim names the claim and its VolumeRestore.
func backupClaimManifest(ns *testNamespace, claim string) string {
	return fmt.Sprintf(`---
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
`, ns.Name, claim, ns.Queue)
}

// runSourceBackup creates a BackupRun on one claim with an 8 minute timeout
// and waits for it to end, polling every second.
//
// Parameters:
//   - namespace, name: where the BackupRun goes and its name.
//   - claim is the claim spec.source names.
//   - observe, when not nil, is called with every state a poll reads, so a
//     test can check what the run passes through.
//   - evidence collects what explains a failure.
//
// Returns the BackupRun as it ended, Succeeded or Failed.
func runSourceBackup(t *testing.T, namespace, name, claim string, observe func(*backupv1alpha1.BackupRun), evidence func() string) backupv1alpha1.BackupRun {
	t.Helper()
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: %s
  namespace: %s
spec:
  source: %s
  timeout: 8m
`, name, namespace, claim))

	var run backupv1alpha1.BackupRun
	waitFor(t, "BackupRun "+name+" to end", 10*time.Minute, time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(namespace, "get", "backuprun", name, "-o", "json")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		run = backupv1alpha1.BackupRun{}
		if err := json.Unmarshal([]byte(out), &run); err != nil {
			return false, "", fmt.Errorf("decode BackupRun: %w", err)
		}
		if observe != nil {
			observe(&run)
		}
		state := fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions))
		ended := run.Status.Phase == backupv1alpha1.RunPhaseSucceeded || run.Status.Phase == backupv1alpha1.RunPhaseFailed
		return ended, state, nil
	}, evidence)
	return run
}

// moverLogs returns the mover logs every ReplicationSource in namespace
// carries in status.latestMoverStatus.logs, joined, or "" when none carries
// any. It fails the test when the namespace holds no ReplicationSource with
// a mover status, since then there is nothing to check.
func moverLogs(t *testing.T, namespace string) string {
	t.Helper()
	var sources struct {
		Items []struct {
			Metadata struct {
				Name string `json:"name"`
			} `json:"metadata"`
			Status struct {
				LatestMoverStatus *struct {
					Result string `json:"result"`
					Logs   string `json:"logs"`
				} `json:"latestMoverStatus"`
			} `json:"status"`
		} `json:"items"`
	}
	if err := getJSON(namespace, "replicationsources.volsync.backube", "", &sources); err != nil {
		t.Fatalf("read the ReplicationSources: %v", err)
	}
	var logs []string
	statuses := 0
	for _, s := range sources.Items {
		if s.Status.LatestMoverStatus == nil {
			continue
		}
		statuses++
		if s.Status.LatestMoverStatus.Logs != "" {
			logs = append(logs, s.Metadata.Name+": "+s.Status.LatestMoverStatus.Logs)
		}
	}
	if statuses == 0 {
		t.Fatalf("no ReplicationSource in %s records a mover status", namespace)
	}
	return strings.Join(logs, "\n")
}

// snapshotObjects counts the objects under the repository's snapshots/
// prefix in RustFS, through a port-forward. It works for a repository no
// mover has initialised, where restic snapshots fails.
func snapshotObjects(t *testing.T, repo *resticRepo) int {
	t.Helper()
	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(repo.AccessKey, repo.SecretKey, ""),
		BucketLookup: minio.BucketLookupPath,
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatalf("S3 client for RustFS: %v", err)
	}
	n := 0
	for obj := range client.ListObjects(ctx, volsyncBucket, minio.ListObjectsOptions{Prefix: repo.Prefix + "/snapshots/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("list s3://%s/%s/snapshots/: %v", volsyncBucket, repo.Prefix, obj.Err)
		}
		n++
	}
	return n
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
