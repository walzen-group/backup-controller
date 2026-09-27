//go:build e2e

package e2e

import (
	"bufio"
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	//nolint:staticcheck // restic's format is Poly1305-AES, and the lock file the test writes has to carry that MAC.
	"golang.org/x/crypto/poly1305"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// restoreFixture is a namespace set up for RestoreRuns on one claim: the
// repository Secret restic-data, a VolumeRestore data that names it, and the
// claim data the VolumeRestore belongs to.
type restoreFixture struct {
	// ns is the test's namespace.
	ns *testNamespace
	// repo is the restic repository the Secret points at.
	repo *resticRepo
	// image is the restic image hack/e2e/volsync pins, the one the
	// controller's restore Jobs run.
	image string
	// pods counts the helper pods the fixture has started, for their names.
	pods int
}

// The names the fixture gives its objects.
const (
	fixtureClaim  = "data"
	fixtureSecret = "restic-data"
	// fixtureFile is the file every seed writes into the claim and into the
	// snapshots, so a restore can be read back from it.
	fixtureFile = "content.txt"
)

// newRestoreFixture creates the namespace and its objects.
//
// Parameters:
//   - prefix: the start of the namespace's name
//   - claimSize: the storage request of the claim data, such as 100Mi
func newRestoreFixture(t *testing.T, prefix, claimSize string) *restoreFixture {
	t.Helper()
	ns := newTestNamespace(t, prefix)
	f := &restoreFixture{ns: ns, repo: newResticRepo(t, "e2e/"+ns.Name+"/data"), image: pinnedResticImage(t)}
	apply(t, f.repo.secretManifest(ns.Name, fixtureSecret)+fmt.Sprintf(`---
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  repository: %[3]s
  cacheStorageClassName: e2e-hostpath
  cacheCapacity: 100Mi
  moverPodLabels:
    kueue.x-k8s.io/queue-name: %[4]s
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %[2]s
  namespace: %[1]s
  annotations:
    backup.wlz.li/enabled: "true"
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: e2e-hostpath
  resources:
    requests:
      storage: %[5]s
`, ns.Name, fixtureClaim, fixtureSecret, ns.Queue, claimSize))
	return f
}

// evidence gathers what explains a failure in the fixture's namespace.
func (f *restoreFixture) evidence() string { return runEvidence(f.ns.Name) }

// pinnedResticImage returns the image hack/e2e/volsync/pins.json pins for
// VolSync's restic mover, by tag and digest. The controller runs its restore
// Jobs in it, so the test writes its snapshots with the same restic.
func pinnedResticImage(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile("../../hack/e2e/volsync/pins.json")
	if err != nil {
		t.Fatal(err)
	}
	var pins struct {
		VolSync struct {
			Image struct {
				Repository string `json:"repository"`
				Tag        string `json:"tag"`
				Digest     string `json:"digest"`
			} `json:"image"`
		} `json:"volsync"`
	}
	if err := json.Unmarshal(raw, &pins); err != nil {
		t.Fatalf("read hack/e2e/volsync/pins.json: %v", err)
	}
	img := pins.VolSync.Image
	return fmt.Sprintf("%s:%s@%s", img.Repository, img.Tag, img.Digest)
}

// runPod runs a pod in the restic image with the claim data at /claim and
// waits for it to end, then deletes it. The pod gets the repository Secret as
// its environment, an empty /data and a restic cache, and runs script with
// bash -ec. The pod carries no queue label, so Kueue leaves it alone. It
// fails the test when the pod fails or does not end within timeout.
//
// Returns the pod's log.
func (f *restoreFixture) runPod(t *testing.T, what, script string, timeout time.Duration) string {
	t.Helper()
	f.pods++
	name := fmt.Sprintf("helper-%d", f.pods)
	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  restartPolicy: Never
  terminationGracePeriodSeconds: 1
  containers:
    - name: helper
      image: %[3]s
      imagePullPolicy: IfNotPresent
      command: [/bin/bash, -ec, %[4]q]
      envFrom:
        - secretRef: {name: %[5]s}
      env:
        - {name: RESTIC_CACHE_DIR, value: /cache}
      volumeMounts:
        - {name: claim, mountPath: /claim}
        - {name: data, mountPath: /data}
        - {name: cache, mountPath: /cache}
      resources:
        requests: {cpu: 10m, memory: 32Mi}
        limits: {cpu: "1", memory: 256Mi}
  volumes:
    - name: claim
      persistentVolumeClaim: {claimName: %[6]s}
    - name: data
      emptyDir: {sizeLimit: 256Mi}
    - name: cache
      emptyDir: {sizeLimit: 128Mi}
`, f.ns.Name, name, f.image, script, fixtureSecret, fixtureClaim))
	waitFor(t, what, timeout, 2*time.Second, func() (bool, string, error) {
		var pod corev1.Pod
		if err := getJSON(f.ns.Name, "pod", name, &pod); err != nil {
			return false, firstLine(err.Error()), nil
		}
		switch pod.Status.Phase {
		case corev1.PodSucceeded:
			return true, "succeeded", nil
		case corev1.PodFailed:
			logs, _ := kubectlQuick(f.ns.Name, "logs", name)
			return false, "failed", fmt.Errorf("pod %s failed:\n%s", name, logs)
		default:
			return false, string(pod.Status.Phase), nil
		}
	}, f.evidence)
	logs, err := kubectlQuick(f.ns.Name, "logs", name)
	if err != nil {
		t.Fatal(err)
	}
	mustKubectl(t, "", "-n", f.ns.Name, "delete", "pod", name, "--wait=false")
	return logs
}

// seed writes content into the claim's fixtureFile and makes sure the
// repository exists, then runs backups in /data the way VolSync's mover
// does. Each entry of snapshots is the file's content for one
// `restic backup --host volsync .` in /data, taken with --time at when, in
// order. extra is bash that runs in /data before the first backup, to add
// files.
func (f *restoreFixture) seed(t *testing.T, claimContent string, when time.Time, extra string, snapshots ...string) {
	t.Helper()
	var script strings.Builder
	fmt.Fprintf(&script, "printf '%%s' %q > /claim/%s\n", claimContent, fixtureFile)
	script.WriteString("restic cat config >/dev/null 2>&1 || restic init\n")
	script.WriteString("cd /data\n")
	script.WriteString(extra + "\n")
	for _, content := range snapshots {
		fmt.Fprintf(&script, "printf '%%s' %q > %s\n", content, fixtureFile)
		fmt.Fprintf(&script, "restic backup --host volsync --time %q .\n", when.UTC().Format("2006-01-02 15:04:05"))
	}
	logs := f.runPod(t, "the seed pod", script.String(), 5*time.Minute)
	t.Logf("seed pod log:\n%s", logs)
}

// claimContent reads the claim's fixtureFile through a pod.
func (f *restoreFixture) claimContent(t *testing.T) string {
	t.Helper()
	return f.runPod(t, "the reader pod", "cat /claim/"+fixtureFile, 3*time.Minute)
}

// restore applies a RestoreRun of the claim data with the given extra spec
// lines (indented by two spaces, each ending in a newline) and waits for it
// to finish.
//
// Returns the finished run.
func (f *restoreFixture) restore(t *testing.T, name, spec string) *backupv1alpha1.RestoreRun {
	t.Helper()
	f.applyRestore(t, name, spec)
	return f.awaitRestore(t, name, 10*time.Minute)
}

// applyRestore applies a RestoreRun of the claim data with a timeout of 8m
// and the given extra spec lines.
func (f *restoreFixture) applyRestore(t *testing.T, name, spec string) {
	t.Helper()
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: %s
  namespace: %s
spec:
  claim: %s
  timeout: 8m
%s`, name, f.ns.Name, fixtureClaim, spec))
}

// awaitRestore waits for the RestoreRun name to finish and returns it.
func (f *restoreFixture) awaitRestore(t *testing.T, name string, timeout time.Duration) *backupv1alpha1.RestoreRun {
	t.Helper()
	var run *backupv1alpha1.RestoreRun
	waitFor(t, "RestoreRun "+name+" to finish", timeout, 2*time.Second, func() (bool, string, error) {
		var err error
		run, err = readRestoreRun(t, f.ns.Name, name)
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run == nil {
			return false, "not created yet", nil
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s items=%s", run.Status.Phase, readyMessage(run.Status.Conditions), itemStates(run.Status.Items)), nil
	}, f.evidence)
	return run
}

// itemStates renders the items' phases, reasons and messages on one line.
func itemStates(items []backupv1alpha1.RestoreItem) string {
	var parts []string
	for _, item := range items {
		parts = append(parts, fmt.Sprintf("[%s %s %s %s job=%s: %s]", item.Kind, item.Name, item.Phase, item.Reason, item.Job, item.Message))
	}
	return strings.Join(parts, " ")
}

// oneVolumeItem returns the run's only item and fails the test unless there
// is exactly one, of kind PersistentVolumeClaim.
func oneVolumeItem(t *testing.T, run *backupv1alpha1.RestoreRun, evidence func() string) backupv1alpha1.RestoreItem {
	t.Helper()
	if len(run.Status.Items) != 1 || run.Status.Items[0].Kind != "PersistentVolumeClaim" {
		t.Fatalf("RestoreRun %s items = %s, want one claim\n%s", run.Name, itemStates(run.Status.Items), evidence())
	}
	return run.Status.Items[0]
}

// wantRestored fails the test unless the run succeeded, its one item
// restored the snapshot with the full ID want through the Job named after
// the run and the item, and the claim holds content.
func (f *restoreFixture) wantRestored(t *testing.T, run *backupv1alpha1.RestoreRun, want resticSnapshot, content string) {
	t.Helper()
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("RestoreRun %s ended %s: %s; items %s\n%s", run.Name, run.Status.Phase, readyMessage(run.Status.Conditions), itemStates(run.Status.Items), f.evidence())
	}
	item := oneVolumeItem(t, run, f.evidence)
	if item.SnapshotID != want.ID {
		t.Errorf("RestoreRun %s restored snapshotID %s, want %s", run.Name, item.SnapshotID, want.ID)
	}
	if job := fmt.Sprintf("restore-%s-0", run.UID); item.Job != job {
		t.Errorf("RestoreRun %s item job = %q, want %q", run.Name, item.Job, job)
	}
	if got := f.claimContent(t); got != content {
		t.Errorf("after RestoreRun %s the claim holds %q, want %q (snapshot %s)", run.Name, got, content, want.ShortID)
	}
}

// TestRestoreByExactIDInOneSecond checks that two snapshots stamped with the
// same second both restore, each by its own full ID. A pod in the restore
// image writes them into the repository with restic backup --time and
// different content, the way VolSync's mover writes a snapshot. The run
// orders snapshots by time and then by ID, so previous 0 restores the one
// with the higher ID and previous 1 the other. Each run must record that
// snapshot's full ID and leave its content in the claim.
func TestRestoreByExactIDInOneSecond(t *testing.T) {
	f := newRestoreFixture(t, "e2e-restore-tie", "100Mi")
	when := time.Now().Add(-time.Minute).Truncate(time.Second)
	f.seed(t, "before any restore", when, "", "first snapshot", "second snapshot")

	snaps := f.repo.snapshots(t)
	if len(snaps) != 2 || !snaps[0].Time.Equal(snaps[1].Time) {
		t.Fatalf("repository holds %+v, want two snapshots of one time", snaps)
	}
	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	content := map[string]string{}
	for _, s := range snaps {
		if s.Hostname != "volsync" || !slices.Equal(s.Paths, []string{"/data"}) {
			t.Fatalf("snapshot %s has host %q and paths %v, want volsync and [/data]", s.ShortID, s.Hostname, s.Paths)
		}
		got, err := f.repo.restic(t, endpoint, "dump", "--no-lock", s.ID, "/"+fixtureFile)
		if err != nil {
			t.Fatal(err)
		}
		content[s.ID] = got
		t.Logf("snapshot %s at %s holds %q", s.ID, s.Time.Format(time.RFC3339Nano), got)
	}
	slices.SortFunc(snaps, func(a, b resticSnapshot) int { return strings.Compare(a.ID, b.ID) })
	newest, older := snaps[1], snaps[0]

	run := f.restore(t, "previous-0", "  previous: 0\n")
	f.wantRestored(t, run, newest, content[newest.ID])
	run = f.restore(t, "previous-1", "  previous: 1\n")
	f.wantRestored(t, run, older, content[older.ID])
}

// setQueueStopPolicy sets the stopPolicy of the namespace's LocalQueue: Hold
// admits no new workload, and None admits again.
func (f *restoreFixture) setQueueStopPolicy(t *testing.T, policy string) {
	t.Helper()
	mustKubectl(t, "", "-n", f.ns.Name, "patch", "localqueue", f.ns.Queue, "--type=merge",
		"-p", fmt.Sprintf(`{"spec":{"stopPolicy":%q}}`, policy))
}

// TestARestoreOfAMissingSnapshotFails checks that a restore Job whose
// snapshot is forgotten after the run started it fails the item with restic's
// exit code, and leaves the claim as it was. The LocalQueue holds the Job's
// pod before it starts, so the snapshot can be forgotten between the run's
// last check and restic. Once the queue admits the pod, restic exits 1 on
// each of the Job's attempts, and the Job fails.
func TestARestoreOfAMissingSnapshotFails(t *testing.T) {
	f := newRestoreFixture(t, "e2e-restore-missing", "100Mi")
	f.seed(t, "the claim's own file", time.Now().Add(-time.Minute), "", "the snapshot's file")
	snaps := f.repo.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("repository holds %+v, want one snapshot", snaps)
	}

	f.setQueueStopPolicy(t, "Hold")
	f.applyRestore(t, "missing", "")
	waitFor(t, "the restore Job's pod to wait for the queue", 5*time.Minute, 2*time.Second, func() (bool, string, error) {
		run, err := readRestoreRun(t, f.ns.Name, "missing")
		if err != nil || run == nil || len(run.Status.Items) != 1 {
			return false, fmt.Sprintf("run %v, err %v", run != nil, err), nil
		}
		item := run.Status.Items[0]
		if item.Phase != backupv1alpha1.ItemRunning || item.Job == "" {
			return false, itemStates(run.Status.Items), nil
		}
		var pods corev1.PodList
		if err := getJSON(f.ns.Name, "pods", "", &pods); err != nil {
			return false, firstLine(err.Error()), nil
		}
		for _, pod := range pods.Items {
			if pod.Labels["batch.kubernetes.io/job-name"] == item.Job && len(pod.Spec.SchedulingGates) > 0 {
				return true, fmt.Sprintf("pod %s gated by %v", pod.Name, pod.Spec.SchedulingGates), nil
			}
		}
		return false, "item Running on " + item.Job + ", no gated pod yet", nil
	}, f.evidence)

	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	if out, err := f.repo.restic(t, endpoint, "forget", snaps[0].ID); err != nil {
		t.Fatalf("forget %s: %v\n%s", snaps[0].ID, err, out)
	}
	t.Logf("forgot snapshot %s; the queue admits the pod now", snaps[0].ID)
	f.setQueueStopPolicy(t, "None")

	run := f.awaitRestore(t, "missing", 8*time.Minute)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("RestoreRun ended %s: %s; items %s, want Failed\n%s", run.Status.Phase, readyMessage(run.Status.Conditions), itemStates(run.Status.Items), f.evidence())
	}
	item := oneVolumeItem(t, run, f.evidence)
	t.Logf("item: %s", itemStates(run.Status.Items))
	if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobFailed {
		t.Errorf("item phase %s reason %s, want Failed RestoreJobFailed", item.Phase, item.Reason)
	}
	for _, want := range []string{"restic exited 1 (failure) in container restore", "failed to find snapshot"} {
		if !strings.Contains(item.Message, want) {
			t.Errorf("item message %q does not say %q", item.Message, want)
		}
	}
	if got := f.claimContent(t); got != "the claim's own file" {
		t.Errorf("after the failed restore the claim holds %q, want its own file", got)
	}
}

// staleLockHost is the hostname in the lock file TestARestoreWaitsOutAStaleLock
// writes. No pod has it, so restic judges the lock by its age alone.
const staleLockHost = "e2e-stale-lock-host"

// TestARestoreWaitsOutAStaleLock checks that an exclusive lock left in the
// repository 31 minutes ago does not stop a restore. restic's restore does
// not skip a stale lock on its own; the Job's unlock init container removes
// it. The test first shows that the lock blocks a plain restic restore.
func TestARestoreWaitsOutAStaleLock(t *testing.T) {
	f := newRestoreFixture(t, "e2e-restore-lock", "100Mi")
	f.seed(t, "the claim's own file", time.Now().Add(-time.Minute), "", "the snapshot's file")
	snaps := f.repo.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("repository holds %+v, want one snapshot", snaps)
	}

	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	lockAge := 31 * time.Minute
	lock := f.repo.writeLock(t, endpoint, time.Now().Add(-lockAge))
	t.Logf("wrote exclusive lock %s dated %s back", lock, lockAge)
	out, err := f.repo.restic(t, endpoint, "restore", snaps[0].ID, "--target", t.TempDir())
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 11 {
		t.Fatalf("restic restore with the lock in place: %v, want exit 11 (locked)\n%s", err, out)
	}
	t.Logf("the lock blocks a plain restic restore: %v", err)

	run := f.restore(t, "stale-lock", "")
	f.wantRestored(t, run, snaps[0], "the snapshot's file")
	locks, err := f.repo.restic(t, endpoint, "list", "locks", "--no-lock")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(locks, lock) {
		t.Errorf("lock %s is still in the repository after the restore", lock)
	}
}

// writeLock writes an exclusive restic lock file dated at into the
// repository, the file a restic process that died at that moment leaves. It
// encrypts the lock with the repository's master key, which restic cat
// masterkey prints, as restic's design document lays encrypted files out:
// IV || AES-256-CTR ciphertext || Poly1305-AES MAC.
//
// Parameters:
//   - endpoint: host:port RustFS answers at on this machine
//   - at: the lock's time
//
// Returns the lock file's ID.
func (r *resticRepo) writeLock(t *testing.T, endpoint string, at time.Time) string {
	t.Helper()
	out, err := r.restic(t, endpoint, "cat", "masterkey", "--no-lock")
	if err != nil {
		t.Fatal(err)
	}
	var master struct {
		MAC struct {
			K []byte `json:"k"`
			R []byte `json:"r"`
		} `json:"mac"`
		Encrypt []byte `json:"encrypt"`
	}
	if err := json.Unmarshal([]byte(out), &master); err != nil || len(master.Encrypt) != 32 || len(master.MAC.K) != 16 || len(master.MAC.R) != 16 {
		t.Fatalf("read restic cat masterkey: %v", err)
	}
	plain, err := json.Marshal(map[string]any{
		"time": at, "exclusive": true, "hostname": staleLockHost, "username": "e2e", "pid": 4242, "uid": 0, "gid": 0,
	})
	if err != nil {
		t.Fatal(err)
	}

	iv := make([]byte, 16)
	if _, err := rand.Read(iv); err != nil {
		t.Fatal(err)
	}
	block, err := aes.NewCipher(master.Encrypt)
	if err != nil {
		t.Fatal(err)
	}
	ciphertext := make([]byte, len(plain))
	cipher.NewCTR(block, iv).XORKeyStream(ciphertext, plain)
	macBlock, err := aes.NewCipher(master.MAC.K)
	if err != nil {
		t.Fatal(err)
	}
	var oneTime [32]byte
	copy(oneTime[:16], master.MAC.R)
	macBlock.Encrypt(oneTime[16:], iv)
	var mac [16]byte
	poly1305.Sum(&mac, ciphertext, &oneTime)
	sealed := slices.Concat(iv, ciphertext, mac[:])

	sum := sha256.Sum256(sealed)
	id := hex.EncodeToString(sum[:])
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(r.AccessKey, r.SecretKey, ""),
		BucketLookup: minio.BucketLookupPath,
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.PutObject(ctx, volsyncBucket, r.Prefix+"/locks/"+id, bytes.NewReader(sealed), int64(len(sealed)), minio.PutObjectOptions{}); err != nil {
		t.Fatalf("write lock %s: %v", id, err)
	}
	return id
}

// stopTestData is how much random data TestStoppingARestoreEndsResticAtOnce
// backs up, and stopTestCPU the CPU limit its restore Job's containers get,
// so that restic is still writing when the run is deleted. RustFS keeps its
// data in an emptyDir of 1Gi, which the data has to share with the other
// tests.
const (
	stopTestData = "160M"
	stopTestCPU  = "100m"
)

// TestStoppingARestoreEndsResticAtOnce checks that deleting a RestoreRun
// while its Job's restic writes ends restic within seconds. restic runs as
// the container's only process, so the SIGTERM the kubelet sends on the
// pod's deletion reaches it; restic removes its lock and exits 130. The pod
// ends Failed well inside its grace period. The test watches the pod, since
// a terminated pod that is being deleted disappears within seconds.
func TestStoppingARestoreEndsResticAtOnce(t *testing.T) {
	f := newRestoreFixture(t, "e2e-restore-stop", "512Mi")
	f.seed(t, "the claim's own file", time.Now().Add(-time.Minute),
		"head -c "+stopTestData+" /dev/urandom > big.bin", "the snapshot's file")

	// restic restores the data in about a second at full speed. A default
	// CPU limit for containers that set none, which the restore Job's
	// containers do not, slows it to many seconds, so the run can be deleted
	// while restic writes.
	apply(t, fmt.Sprintf(`apiVersion: v1
kind: LimitRange
metadata:
  name: slow-restic
  namespace: %s
spec:
  limits:
    - type: Container
      default: {cpu: %s}
      defaultRequest: {cpu: 10m}
`, f.ns.Name, stopTestCPU))

	pods := watchObjects[corev1.Pod](t, f.ns.Name, "pods", "-l", "app.kubernetes.io/component=restore")
	f.applyRestore(t, "stopped", "")

	var restorePod string
	// restic prints "restoring snapshot <ID> of ..." once it has opened the
	// repository and loaded the index, right before it writes files. A stop
	// before that line ends restic with exit 1 ("config cannot be loaded:
	// context canceled"), so the test waits for it.
	waitFor(t, "restic to start writing", 5*time.Minute, 300*time.Millisecond, func() (bool, string, error) {
		for name, pod := range pods.latest() {
			for _, status := range pod.Status.ContainerStatuses {
				if status.Name == "restore" && status.State.Terminated != nil {
					return false, "", fmt.Errorf("restic ended before the stop: exit %d; back up more than %s", status.State.Terminated.ExitCode, stopTestData)
				}
				if status.Name != "restore" || status.State.Running == nil {
					continue
				}
				logs, err := kubectlQuick(f.ns.Name, "logs", name, "-c", "restore")
				if err != nil {
					return false, "pod " + name + " runs restore: " + firstLine(err.Error()), nil
				}
				if !strings.Contains(logs, "restoring snapshot") {
					return false, "pod " + name + " runs restore, which has not started writing", nil
				}
				restorePod = name
				return true, "pod " + name + ": " + lastLine(logs), nil
			}
		}
		return false, fmt.Sprintf("%d pods", len(pods.latest())), nil
	}, f.evidence)
	mustKubectl(t, "", "-n", f.ns.Name, "delete", "restorerun", "stopped", "--wait=false")
	t.Logf("deleted RestoreRun stopped while pod %s restores", restorePod)

	var ended corev1.Pod
	waitFor(t, "the restore pod to end", 2*time.Minute, 200*time.Millisecond, func() (bool, string, error) {
		pod, ok := pods.latest()[restorePod]
		if !ok {
			return false, "", fmt.Errorf("pod %s is no longer seen", restorePod)
		}
		ended = pod
		return pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed,
			fmt.Sprintf("phase %s, deletionTimestamp %v", pod.Status.Phase, pod.DeletionTimestamp), nil
	}, f.evidence)

	var terminated *corev1.ContainerStateTerminated
	for _, status := range ended.Status.ContainerStatuses {
		if status.Name == "restore" {
			terminated = status.State.Terminated
		}
	}
	if ended.Status.Phase != corev1.PodFailed || terminated == nil || terminated.ExitCode != 130 {
		t.Fatalf("pod %s ended in phase %s with restore %+v, want Failed with exit 130\n%s", restorePod, ended.Status.Phase, terminated, f.evidence())
	}
	if ended.DeletionTimestamp == nil || ended.DeletionGracePeriodSeconds == nil {
		t.Fatalf("pod %s ended without being deleted: %+v", restorePod, ended.ObjectMeta)
	}
	grace := time.Duration(*ended.DeletionGracePeriodSeconds) * time.Second
	stopAsked := ended.DeletionTimestamp.Add(-grace)
	took := terminated.FinishedAt.Sub(stopAsked)
	t.Logf("pod %s was asked to stop at %s with %s grace; restic exited %d at %s, %s later: %q",
		restorePod, stopAsked.Format(time.RFC3339), grace, terminated.ExitCode, terminated.FinishedAt.Format(time.RFC3339), took, terminated.Message)
	// FinishedAt and the deletion timestamp are whole seconds.
	if took > 10*time.Second || grace < 20*time.Second {
		t.Errorf("restic ended %s after the stop within a grace period of %s, want well inside it", took, grace)
	}

	waitFor(t, "RestoreRun stopped to be gone", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		run, err := readRestoreRun(t, f.ns.Name, "stopped")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run != nil {
			return false, fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions)), nil
		}
		return true, "gone", nil
	}, f.evidence)
	var jobs batchv1.JobList
	if err := getJSON(f.ns.Name, "jobs", "", &jobs); err != nil {
		t.Fatal(err)
	}
	for _, job := range jobs.Items {
		if job.DeletionTimestamp == nil {
			t.Errorf("Job %s is left after the run is gone", job.Name)
		}
	}
}

// objectWatch keeps every state of each object a kubectl watch reports, in
// the order the watch reported them, including an object's last state before
// it was deleted.
type objectWatch[T any] struct {
	mu    sync.Mutex
	state map[string]T
	seen  map[string][]T
}

// latest returns a copy of the latest state of every object seen, by name.
func (w *objectWatch[T]) latest() map[string]T {
	w.mu.Lock()
	defer w.mu.Unlock()
	out := make(map[string]T, len(w.state))
	for k, v := range w.state {
		out[k] = v
	}
	return out
}

// history returns every state of the object name seen so far, oldest first.
func (w *objectWatch[T]) history(name string) []T {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.seen[name])
}

// watchObjects starts kubectl get --watch for objects of a kind in namespace
// that the selector arguments pick, and records every state it reports until
// the test ends.
//
// Parameters:
//   - kind: the resource, such as pods or jobs
//   - selectArgs: kubectl's selector flags, such as -l with a label selector
//     or --field-selector with metadata.name=<name>
func watchObjects[T any](t *testing.T, namespace, kind string, selectArgs ...string) *objectWatch[T] {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	args := append([]string{"--context", kubeContext, "-n", namespace, "get", kind}, selectArgs...)
	cmd := exec.CommandContext(ctx, "kubectl", append(args, "--watch", "--output-watch-events", "-o", "json")...)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start the watch of %s: %v", kind, err)
	}
	w := &objectWatch[T]{state: map[string]T{}, seen: map[string][]T{}}
	done := make(chan struct{})
	go func() {
		defer close(done)
		dec := json.NewDecoder(bufio.NewReader(stdout))
		for {
			var event struct {
				Object json.RawMessage `json:"object"`
			}
			if err := dec.Decode(&event); err != nil {
				return
			}
			var meta struct {
				Metadata struct {
					Name string `json:"name"`
				} `json:"metadata"`
			}
			var obj T
			if json.Unmarshal(event.Object, &meta) != nil || json.Unmarshal(event.Object, &obj) != nil {
				continue
			}
			w.mu.Lock()
			w.state[meta.Metadata.Name] = obj
			w.seen[meta.Metadata.Name] = append(w.seen[meta.Metadata.Name], obj)
			w.mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		<-done
	})
	return w
}
