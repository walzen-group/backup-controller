//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// The names the populator gives what it creates for a claim, as
// internal/populator/names.go and secretcopy.go name them.
const (
	primeJobUIDAnnotation = "backup.wlz.li/restore-job-uid"
	snapshotIDAnnotation  = "backup.wlz.li/snapshot-id"
	restoreClaimLabel     = "backup.wlz.li/restore-claim"
)

// populatedVolume is one app volume the way the infrastructure repository's
// flux/templates/backups/pvc component declares it: the restic repository
// Secret <claim>-restic, the VolumeRestore <claim> that names it, and the
// claim <claim> whose dataSourceRef names the VolumeRestore.
type populatedVolume struct {
	// ns is the test's namespace.
	ns *testNamespace
	// claim is the claim's name, which the VolumeRestore shares.
	claim string
	// repo is the repository the Secret points at.
	repo *resticRepo
}

// newPopulatedVolume names a volume and its repository, and applies the
// repository Secret and the VolumeRestore. The claim comes later, from
// claimManifest.
//
// The VolumeRestore has the template's shape: the repository Secret, a cache
// class, and the queue label for the restore's pod, which runs in the
// controller namespace and so names the LocalQueue backup there. The template
// leaves cacheCapacity at its default of 1Gi; the test sets 100Mi to keep
// the e2e cluster's disk small.
func newPopulatedVolume(t *testing.T, ns *testNamespace, claim string) *populatedVolume {
	t.Helper()
	v := &populatedVolume{ns: ns, claim: claim, repo: newResticRepo(t, "e2e/"+ns.Name+"/"+claim)}
	apply(t, v.repo.secretManifest(ns.Name, claim+"-restic")+fmt.Sprintf(`---
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  repository: %[2]s-restic
  cacheStorageClassName: e2e-hostpath
  cacheCapacity: 100Mi
  moverPodLabels:
    kueue.x-k8s.io/queue-name: backup
`, ns.Name, claim))
	return v
}

// claimManifest returns the volume's claim, marked for backup. With
// populated it names the VolumeRestore in its dataSourceRef, as every app
// claim in the infrastructure repository does; without, it is a plain claim.
func (v *populatedVolume) claimManifest(populated bool) string {
	source := ""
	if populated {
		source = fmt.Sprintf(`  dataSourceRef:
    apiGroup: backup.wlz.li
    kind: VolumeRestore
    name: %s
`, v.claim)
	}
	return fmt.Sprintf(`apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: %s
  namespace: %s
  annotations:
    backup.wlz.li/enabled: "true"
    backup.wlz.li/retain-last: "3"
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: e2e-hostpath
  resources:
    requests:
      storage: 100Mi
%s`, v.claim, v.ns.Name, source)
}

// readClaim reads the volume's claim, or returns nil when it does not exist.
func (v *populatedVolume) readClaim(t *testing.T) *corev1.PersistentVolumeClaim {
	t.Helper()
	out, err := kubectlQuick(v.ns.Name, "get", "pvc", v.claim, "-o", "name", "--ignore-not-found")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) == "" {
		return nil
	}
	var claim corev1.PersistentVolumeClaim
	if err := getJSON(v.ns.Name, "pvc", v.claim, &claim); err != nil {
		t.Fatal(err)
	}
	return &claim
}

// readVolumeRestore reads the volume's VolumeRestore.
func (v *populatedVolume) readVolumeRestore(t *testing.T) *backupv1alpha1.VolumeRestore {
	t.Helper()
	var vr backupv1alpha1.VolumeRestore
	if err := getJSON(v.ns.Name, "volumerestore", v.claim, &vr); err != nil {
		t.Fatal(err)
	}
	return &vr
}

// appManifest returns a Deployment named name whose pod mounts each of
// claims at /<claim> and runs script with sh -ec in the restic image. The
// claims bind WaitForFirstConsumer, so this pod is what places their volumes.
func appManifest(namespace, name, image, script string, claims ...string) string {
	var mounts, volumes strings.Builder
	for _, claim := range claims {
		fmt.Fprintf(&mounts, "            - {name: %[1]s, mountPath: /%[1]s}\n", claim)
		fmt.Fprintf(&volumes, "        - name: %[1]s\n          persistentVolumeClaim: {claimName: %[1]s}\n", claim)
	}
	return fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  replicas: 1
  selector:
    matchLabels: {app: %[2]s}
  template:
    metadata:
      labels: {app: %[2]s}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: app
          image: %[3]s
          imagePullPolicy: IfNotPresent
          command: [/bin/sh, -ec, %[4]q]
          volumeMounts:
%[5]s          resources:
            requests: {cpu: 10m, memory: 16Mi}
      volumes:
%[6]s`, namespace, name, image, script, mounts.String(), volumes.String())
}

// populatorEvidence gathers what explains a populator failure: the
// namespace's runs, claims, pods and events, its VolumeRestores, and the
// controller namespace's claims, Jobs, pods and events, where the prime
// claims and the restore Jobs live.
func populatorEvidence(namespace string) string {
	return runEvidence(namespace) + collect(
		[]string{"-n", namespace, "get", "volumerestores", "-o", "yaml"},
		[]string{"-n", controllerNamespace, "get", "pvc,jobs,pods,secrets", "-o", "wide"},
		[]string{"-n", controllerNamespace, "get", "jobs", "-l", "app.kubernetes.io/component=restore", "-o", "yaml"},
		[]string{"-n", controllerNamespace, "get", "events", "--sort-by=.lastTimestamp"},
	)
}

// waitBound waits until the volume's claim is Bound and returns it.
func (v *populatedVolume) waitBound(t *testing.T, timeout time.Duration, evidence func() string) *corev1.PersistentVolumeClaim {
	t.Helper()
	var claim *corev1.PersistentVolumeClaim
	waitFor(t, "claim "+v.claim+" to bind", timeout, 3*time.Second, func() (bool, string, error) {
		claim = v.readClaim(t)
		if claim == nil {
			return false, "no claim", nil
		}
		vr := v.readVolumeRestore(t)
		return claim.Status.Phase == corev1.ClaimBound,
			fmt.Sprintf("claim %s; VolumeRestore %s claims %+v", claim.Status.Phase, readyMessage(vr.Status.Conditions), vr.Status.Claims), nil
	}, evidence)
	return claim
}

// waitRestored waits until the volume's VolumeRestore reports Ready with
// reason Restored and lists no claim. The populator's cleanup writes that
// once the claim is bound, on the library's next pass over the claim.
func (v *populatedVolume) waitRestored(t *testing.T, evidence func() string) {
	t.Helper()
	start := time.Now()
	waitFor(t, "VolumeRestore "+v.claim+" to report Restored", 2*time.Minute, 2*time.Second, func() (bool, string, error) {
		vr := v.readVolumeRestore(t)
		ready := readyMessage(vr.Status.Conditions)
		return len(vr.Status.Claims) == 0 && strings.HasPrefix(ready, backupv1alpha1.ReasonRestored+":"),
			fmt.Sprintf("%s; claims %+v", ready, vr.Status.Claims), nil
	}, evidence)
	t.Logf("VolumeRestore %s reported Restored %s after the check started", v.claim, time.Since(start).Round(time.Second))
}

// backUp runs a BackupRun named name on the volume's claim and fails the
// test unless it succeeds.
func (v *populatedVolume) backUp(t *testing.T, name string, evidence func() string) {
	t.Helper()
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: %s
  namespace: %s
spec:
  source: %s
  timeout: 8m
`, name, v.ns.Name, v.claim))
	var run *backupv1alpha1.BackupRun
	waitFor(t, "BackupRun "+name+" to end", 10*time.Minute, 3*time.Second, func() (bool, string, error) {
		var err error
		run, err = readBackupRun(t, v.ns.Name, name)
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run == nil {
			return false, "not created yet", nil
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions)), nil
	}, evidence)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun %s ended %s: %s\n%s", name, run.Status.Phase, readyMessage(run.Status.Conditions), evidence())
	}
}

// deleteAndWait deletes one object and waits until it is gone.
func deleteAndWait(t *testing.T, namespace, kind, name string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	if _, err := kubectl(ctx, "", "-n", namespace, "delete", kind, name, "--wait=true", "--timeout=170s"); err != nil {
		t.Fatalf("delete %s %s/%s: %v\n%s", kind, namespace, name, err, populatorEvidence(namespace))
	}
}

// resourceVersion parses an object's resourceVersion. The e2e cluster's API
// server stores objects in one etcd, whose revision every write raises, so
// two resourceVersions it hands out order the writes that made them.
func resourceVersion(t *testing.T, rv string) uint64 {
	t.Helper()
	n, err := strconv.ParseUint(rv, 10, 64)
	if err != nil {
		t.Fatalf("resourceVersion %q is not a number: %v", rv, err)
	}
	return n
}

// jobCondition reports whether a Job has the condition typ with status True.
func jobCondition(job batchv1.Job, typ batchv1.JobConditionType) bool {
	return slices.ContainsFunc(job.Status.Conditions, func(c batchv1.JobCondition) bool {
		return c.Type == typ && c.Status == corev1.ConditionTrue
	})
}

// restoreArgs returns the arguments of a Job's restore container.
func restoreArgs(job batchv1.Job) []string {
	for _, c := range job.Spec.Template.Spec.Containers {
		if c.Name == "restore" {
			return c.Args
		}
	}
	return nil
}

// TestThePopulatorFillsAClaimFromTheRepository checks the populator end to
// end against the real CSI driver, scheduler, Kueue, VolSync and restic: an
// app's claim, deleted with its app and declared again in the shape the
// infrastructure repository gives every app claim, comes back holding the
// file the app wrote before its last backup.
//
// The app writes a file into a plain claim, and a BackupRun backs the claim
// up through VolSync. The test deletes the app and the claim, and declares
// the claim again with a dataSourceRef to the VolumeRestore, then the app.
// It watches the restore Job and the prime claim in the controller
// namespace. The Job must be created suspended, restore the backup's
// snapshot by its full ID with --delete, stay suspended until the prime claim
// records the Job's UID, then be resumed and complete. The claim must bind
// holding the file, and the Job and the repository Secret copy must be gone
// afterwards.
func TestThePopulatorFillsAClaimFromTheRepository(t *testing.T) {
	ns := newTestNamespace(t, "e2e-populator")
	evidence := func() string { return populatorEvidence(ns.Name) }
	image := pinnedResticImage(t)
	v := newPopulatedVolume(t, ns, "app-data")
	const file = "known.txt"
	known := "written before the backup in " + ns.Name

	apply(t, v.claimManifest(false)+"---\n"+appManifest(ns.Name, "app", image,
		fmt.Sprintf("printf '%%s' %q > /%s/%s && sync && exec sleep infinity", known, v.claim, file), v.claim))
	waitFor(t, "the app to write "+file, 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/app", "--", "cat", "/"+v.claim+"/"+file)
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		return out == known, fmt.Sprintf("file holds %q", out), nil
	}, evidence)

	v.backUp(t, "backup", evidence)
	snaps := v.repo.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("repository holds %d snapshots, want 1: %+v", len(snaps), snaps)
	}
	backup := snaps[0]
	t.Logf("the backup wrote snapshot %s", backup.ID)

	deleteAndWait(t, ns.Name, "deployment", "app")
	waitFor(t, "the app's pod to go", 2*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "pods", "-l", "app=app", "-o", "name")
		return strings.TrimSpace(out) == "", "pods: " + strings.TrimSpace(out), err
	}, evidence)
	deleteAndWait(t, ns.Name, "pvc", v.claim)
	t.Logf("deleted the app and claim %s", v.claim)

	apply(t, v.claimManifest(true))
	claim := v.readClaim(t)
	if claim == nil {
		t.Fatalf("claim %s is gone right after its apply", v.claim)
	}
	uid := claim.UID
	jobName, primeName, copyName := "restore-"+string(uid), "prime-"+string(uid), string(uid)
	jobs := watchObjects[batchv1.Job](t, controllerNamespace, "jobs", "-l", restoreClaimLabel+"="+string(uid))
	primes := watchObjects[corev1.PersistentVolumeClaim](t, controllerNamespace, "pvc", "--field-selector", "metadata.name="+primeName)
	// The watches list what exists when they start; give them a moment
	// before the app places the claim and the populator starts.
	time.Sleep(2 * time.Second)
	apply(t, appManifest(ns.Name, "app", image, "exec sleep infinity", v.claim))

	bound := v.waitBound(t, 8*time.Minute, evidence)
	t.Logf("claim %s (UID %s) bound to %s", v.claim, uid, bound.Spec.VolumeName)
	waitFor(t, "the app to start on the restored claim", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/app", "--", "cat", "/"+v.claim+"/"+file)
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		if out != known {
			return false, "", fmt.Errorf("the restored claim's %s holds %q, want %q", file, out, known)
		}
		return true, "the file is back", nil
	}, evidence)

	waitFor(t, "the restore Job and the Secret copy to go", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		job, err := kubectlQuick(controllerNamespace, "get", "job", jobName, "-o", "name", "--ignore-not-found")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		secret, err := kubectlQuick(controllerNamespace, "get", "secret", copyName, "-o", "name", "--ignore-not-found")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		return job == "" && secret == "", fmt.Sprintf("job %q, secret %q", strings.TrimSpace(job), strings.TrimSpace(secret)), nil
	}, evidence)

	history := jobs.history(jobName)
	if len(history) == 0 {
		t.Fatalf("the watch saw no Job %s\n%s", jobName, evidence())
	}
	for i, job := range history {
		t.Logf("Job state %d: rv %s suspend %v conditions %v", i, job.ResourceVersion, job.Spec.Suspend != nil && *job.Spec.Suspend, job.Status.Conditions)
	}
	created := history[0]
	if created.Spec.Suspend == nil || !*created.Spec.Suspend {
		t.Errorf("Job %s was first seen with suspend %v, want created suspended", jobName, created.Spec.Suspend)
	}
	if got := created.Annotations[snapshotIDAnnotation]; got != backup.ID {
		t.Errorf("Job %s restores snapshot %q, want the backup's %s", jobName, got, backup.ID)
	}
	args := restoreArgs(created)
	if len(args) < 2 || args[0] != "restore" || args[1] != backup.ID || !slices.Contains(args, "--delete") {
		t.Errorf("Job %s runs restic %v, want restore %s ... --delete", jobName, args, backup.ID)
	}
	if !slices.ContainsFunc(created.OwnerReferences, func(o metav1.OwnerReference) bool {
		return o.Kind == "PersistentVolumeClaim" && o.Name == primeName
	}) {
		t.Errorf("Job %s has owners %+v, want the prime claim %s", jobName, created.OwnerReferences, primeName)
	}
	resumed := slices.IndexFunc(history, func(j batchv1.Job) bool { return j.Spec.Suspend != nil && !*j.Spec.Suspend })
	if resumed < 0 {
		t.Fatalf("the watch never saw Job %s resumed\n%s", jobName, evidence())
	}
	if last := history[len(history)-1]; !jobCondition(last, batchv1.JobComplete) {
		t.Errorf("Job %s was last seen with conditions %+v, want Complete", jobName, last.Status.Conditions)
	}

	primeHistory := primes.history(primeName)
	recorded := slices.IndexFunc(primeHistory, func(p corev1.PersistentVolumeClaim) bool {
		return types.UID(p.Annotations[primeJobUIDAnnotation]) == created.UID
	})
	if recorded < 0 {
		t.Fatalf("the watch never saw prime claim %s record Job UID %s in %d states\n%s", primeName, created.UID, len(primeHistory), evidence())
	}
	recordedRV := resourceVersion(t, primeHistory[recorded].ResourceVersion)
	resumedRV := resourceVersion(t, history[resumed].ResourceVersion)
	t.Logf("prime claim %s recorded the Job's UID at rv %d; the Job was resumed at rv %d", primeName, recordedRV, resumedRV)
	if resumedRV <= recordedRV {
		t.Errorf("Job %s was resumed at rv %d, before prime claim %s recorded its UID at rv %d", jobName, resumedRV, primeName, recordedRV)
	}

	v.waitRestored(t, evidence)
}

// countObjects counts the objects under prefix in bucket.
func countObjects(t *testing.T, endpoint string, repo *resticRepo) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(repo.AccessKey, repo.SecretKey, ""),
		BucketLookup: minio.BucketLookupPath,
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for obj := range client.ListObjects(ctx, volsyncBucket, minio.ListObjectsOptions{Prefix: repo.Prefix + "/", Recursive: true}) {
		if obj.Err != nil {
			t.Fatalf("list s3://%s/%s/: %v", volsyncBucket, repo.Prefix, obj.Err)
		}
		n++
	}
	return n
}

// TestAFirstDeployBindsEmpty checks that an app's first deploy starts on
// empty volumes: a claim whose repository does not exist yet, and a claim
// whose repository exists and holds no snapshot, both bind empty, and no
// restore Job is created for either. The controller writes nothing into the
// repository that did not exist.
func TestAFirstDeployBindsEmpty(t *testing.T) {
	ns := newTestNamespace(t, "e2e-first-deploy")
	evidence := func() string { return populatorEvidence(ns.Name) }
	image := pinnedResticImage(t)
	missing := newPopulatedVolume(t, ns, "new-data")
	empty := newPopulatedVolume(t, ns, "empty-data")

	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	if out, err := empty.repo.restic(t, endpoint, "init"); err != nil {
		t.Fatalf("restic init: %v\n%s", err, out)
	}
	if n := countObjects(t, endpoint, missing.repo); n != 0 {
		t.Fatalf("repository %s already holds %d objects", missing.repo.Prefix, n)
	}

	apply(t, missing.claimManifest(true)+"---\n"+empty.claimManifest(true))
	claims := map[string]types.UID{}
	for _, v := range []*populatedVolume{missing, empty} {
		claim := v.readClaim(t)
		if claim == nil {
			t.Fatalf("claim %s is gone right after its apply", v.claim)
		}
		claims[v.claim] = claim.UID
	}
	jobs := watchObjects[batchv1.Job](t, controllerNamespace, "jobs", "-l", "app.kubernetes.io/component=restore")
	time.Sleep(2 * time.Second)
	apply(t, appManifest(ns.Name, "app", image, "exec sleep infinity", missing.claim, empty.claim))

	for _, v := range []*populatedVolume{missing, empty} {
		v.waitBound(t, 6*time.Minute, evidence)
	}
	var listing string
	waitFor(t, "the app to start", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/app", "--", "ls", "-A", "/"+missing.claim, "/"+empty.claim)
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		listing = out
		return true, "started", nil
	}, evidence)
	t.Logf("the app sees:\n%s", listing)
	for _, line := range strings.Split(strings.TrimSpace(listing), "\n") {
		if line != "" && !strings.HasSuffix(line, ":") {
			t.Errorf("a first deploy's claim holds %q, want nothing", line)
		}
	}

	for name, uid := range claims {
		if h := jobs.history("restore-" + string(uid)); len(h) != 0 {
			t.Errorf("claim %s got restore Job %s, want none", name, h[0].Name)
		}
	}
	if n := countObjects(t, endpoint, missing.repo); n != 0 {
		t.Errorf("repository %s holds %d objects after the first deploy, want none", missing.repo.Prefix, n)
	}
	for _, v := range []*populatedVolume{missing, empty} {
		v.waitRestored(t, evidence)
	}
}

// TestSnapshotsOfAnotherLayoutKeepAClaimPending checks that a claim whose
// repository holds snapshots, none of the layout a VolSync mover writes,
// never binds empty. The test backs up a directory of this machine with
// restic, which records its own host and path, and declares the claim and an
// app. The claim must stay Pending with reason NoBackupInReach on the
// VolumeRestore, naming the snapshot, and no restore Job must be created.
func TestSnapshotsOfAnotherLayoutKeepAClaimPending(t *testing.T) {
	ns := newTestNamespace(t, "e2e-other-layout")
	evidence := func() string { return populatorEvidence(ns.Name) }
	image := pinnedResticImage(t)
	v := newPopulatedVolume(t, ns, "app-data")

	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	if out, err := v.repo.restic(t, endpoint, "init"); err != nil {
		t.Fatalf("restic init: %v\n%s", err, out)
	}
	dir := t.TempDir()
	if out, err := v.repo.restic(t, endpoint, "backup", "--host", "laptop", dir); err != nil {
		t.Fatalf("restic backup: %v\n%s", err, out)
	}
	snaps := v.repo.snapshots(t)
	if len(snaps) != 1 {
		t.Fatalf("repository holds %+v, want one snapshot", snaps)
	}
	other := snaps[0]
	t.Logf("snapshot %s has host %s and paths %v", other.ShortID, other.Hostname, other.Paths)

	apply(t, v.claimManifest(true))
	claim := v.readClaim(t)
	if claim == nil {
		t.Fatalf("claim %s is gone right after its apply", v.claim)
	}
	uid := claim.UID
	jobs := watchObjects[batchv1.Job](t, controllerNamespace, "jobs", "-l", restoreClaimLabel+"="+string(uid))
	time.Sleep(2 * time.Second)
	apply(t, appManifest(ns.Name, "app", image, "exec sleep infinity", v.claim))

	waitFor(t, "the VolumeRestore to report NoBackupInReach", 5*time.Minute, 3*time.Second, func() (bool, string, error) {
		vr := v.readVolumeRestore(t)
		ready := readyMessage(vr.Status.Conditions)
		failed := slices.ContainsFunc(vr.Status.Claims, func(c backupv1alpha1.ClaimRestoreStatus) bool {
			return c.UID == uid && c.Phase == backupv1alpha1.RestorePhaseFailed
		})
		return failed && strings.HasPrefix(ready, backupv1alpha1.ReasonNoBackupInReach+":"), fmt.Sprintf("%s; claims %+v", ready, vr.Status.Claims), nil
	}, evidence)
	vr := v.readVolumeRestore(t)
	if ready := readyMessage(vr.Status.Conditions); !strings.Contains(ready, other.ShortID) {
		t.Errorf("Ready %q does not name snapshot %s", ready, other.ShortID)
	}

	// The populator retries a claim at least every 30 seconds; a claim that
	// stays Pending over two of those did not bind empty on a retry either.
	time.Sleep(65 * time.Second)
	if claim := v.readClaim(t); claim == nil || claim.Status.Phase != corev1.ClaimPending {
		t.Errorf("claim %s is %v after a minute, want Pending\n%s", v.claim, claim, evidence())
	}
	if h := jobs.history("restore-" + string(uid)); len(h) != 0 {
		t.Errorf("claim %s got restore Job %s, want none", v.claim, h[0].Name)
	}
}
