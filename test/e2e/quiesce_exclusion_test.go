//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// fluxImages holds the image references hack/e2e/flux installs, keyed by the
// name pins.json gives each one. The test needs the Flux CLI, which pushes the
// Kustomization's artifact from inside the cluster.
var fluxImages = map[string]string{}

// fluxImageRef returns the reference of the pinned image named name as
// image:tag@digest, reading hack/e2e/flux/pins.json once.
func fluxImageRef(t *testing.T, name string) string {
	t.Helper()
	if len(fluxImages) == 0 {
		data, err := os.ReadFile(filepath.Join("..", "..", "hack", "e2e", "flux", "pins.json"))
		if err != nil {
			t.Fatalf("read the Flux pins: %v", err)
		}
		var pins struct {
			Flux struct {
				Images map[string]struct {
					Image  string `json:"image"`
					Tag    string `json:"tag"`
					Digest string `json:"digest"`
				} `json:"images"`
			} `json:"flux"`
		}
		if err := json.Unmarshal(data, &pins); err != nil {
			t.Fatalf("decode the Flux pins: %v", err)
		}
		for key, image := range pins.Flux.Images {
			fluxImages[key] = fmt.Sprintf("%s:%s@%s", image.Image, image.Tag, image.Digest)
		}
	}
	ref, ok := fluxImages[name]
	if !ok {
		t.Fatalf("the Flux pins hold no image %s", name)
	}
	return ref
}

// applyFluxKustomization sets up the OCIRepository the in-cluster registry
// serves and the Kustomization that applies the app, the way
// hack/e2e/flux/flux.sh's check does: a ConfigMap holds the manifests, a Job
// running the Flux CLI pushes them as an artifact, and the Kustomization
// applies them into the app's namespace.
//
// Parameters:
//   - appNamespace is the namespace the Kustomization applies into.
//   - deployName is the Deployment's name, and the image is the pinned
//     busybox the Deployment's pod runs. The Deployment carries
//     backup.wlz.li/quiesce: "true" in the manifest, as prod declares it:
//     Flux removes an annotation that kubectl annotate added once it applies
//     the Deployment again.
//
// It returns the namespace and name of the Kustomization it created, which it
// deletes when the test ends.
func applyFluxKustomization(t *testing.T, appNamespace, deployName string) (string, string) {
	t.Helper()
	const fluxNamespace = "flux-system"
	name := fmt.Sprintf("quiesce-%d", time.Now().Unix()%1000000)
	registry := "e2e-registry." + fluxNamespace + ".svc.cluster.local:5000"
	busybox := fluxImageRef(t, "busybox")

	manifest := fmt.Sprintf(`apiVersion: apps/v1
kind: Deployment
metadata:
  name: %[1]s
  namespace: %[2]s
  annotations:
    backup.wlz.li/quiesce: "true"
spec:
  replicas: 2
  selector: {matchLabels: {app: %[1]s}}
  template:
    metadata:
      labels: {app: %[1]s}
    spec:
      terminationGracePeriodSeconds: 1
      containers:
        - name: sleep
          image: %[3]s
          imagePullPolicy: IfNotPresent
          command: [sleep, "86400"]
`, deployName, appNamespace, busybox)
	mustKubectl(t, "", "-n", fluxNamespace, "create", "configmap", name, "--from-literal=deploy.yaml="+manifest)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		deletions := [][]string{
			{"-n", fluxNamespace, "delete", "kustomization", name, "--ignore-not-found", "--wait=true", "--timeout=60s"},
			{"-n", fluxNamespace, "delete", "ocirepository", name, "--ignore-not-found", "--timeout=60s"},
			{"-n", fluxNamespace, "delete", "job", name + "-push", "--ignore-not-found", "--cascade=foreground", "--timeout=60s"},
			{"-n", fluxNamespace, "delete", "configmap", name, "--ignore-not-found"},
		}
		for _, args := range deletions {
			_, _ = kubectl(ctx, "", args...)
		}
	})

	push := `apiVersion: batch/v1
kind: Job
metadata:
  name: %[1]s-push
  namespace: %[2]s
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: push
          image: %[3]s
          imagePullPolicy: IfNotPresent
          env: [{name: HOME, value: /tmp}]
          command: [sh, -c]
          args:
            - >-
              mkdir /tmp/m && cp /manifests/deploy.yaml /tmp/m/ &&
              flux push artifact "oci://%[4]s/%[1]s:%[1]s"
              --path=/tmp/m --source=e2e
              --revision=quiesce@sha1:0000000000000000000000000000000000000000
              --insecure-registry
          volumeMounts: [{name: m, mountPath: /manifests}, {name: tmp, mountPath: /tmp}]
      volumes: [{name: m, configMap: {name: %[1]s}}, {name: tmp, emptyDir: {}}]
`
	apply(t, fmt.Sprintf(push, name, fluxNamespace, fluxImageRef(t, "flux-cli"), registry))
	mustKubectl(t, "", "-n", fluxNamespace, "wait", "--for=condition=Complete", "--timeout=180s", "job/"+name+"-push")

	flux := `apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  interval: 1m
  url: oci://%[3]s/%[1]s
  ref: {tag: %[1]s}
  insecure: true
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  interval: 1h
  prune: true
  wait: true
  timeout: 2m
  targetNamespace: %[4]s
  sourceRef: {kind: OCIRepository, name: %[1]s}
`
	apply(t, fmt.Sprintf(flux, name, fluxNamespace, registry, appNamespace))
	mustKubectl(t, "", "-n", fluxNamespace, "wait", "--for=condition=Ready", "--timeout=300s", "kustomization/"+name)
	return fluxNamespace, name
}

// appReplicas returns the app Deployment's spec.replicas as the API server
// holds it, with an absent count counting as one.
func appReplicas(t *testing.T, namespace, name string) int32 {
	t.Helper()
	out := mustKubectl(t, "", "-n", namespace, "get", "deployment", name, "-o", "json")
	var deployment struct {
		Spec struct {
			Replicas *int32 `json:"replicas"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(out), &deployment); err != nil {
		t.Fatalf("decode Deployment %s/%s: %v", namespace, name, err)
	}
	if deployment.Spec.Replicas == nil {
		return 1
	}
	return *deployment.Spec.Replicas
}

// kustomizationSuspended reports whether the named Kustomization has
// spec.suspend set.
func kustomizationSuspended(t *testing.T, namespace, name string) bool {
	t.Helper()
	out := mustKubectl(t, "", "-n", namespace, "get", "kustomization", name, "-o", "json")
	var kustomization struct {
		Spec struct {
			Suspend bool `json:"suspend"`
		} `json:"spec"`
	}
	if err := json.Unmarshal([]byte(out), &kustomization); err != nil {
		t.Fatalf("decode Kustomization %s/%s: %v", namespace, name, err)
	}
	return kustomization.Spec.Suspend
}

// quiesceLeaseHolder returns the run that holds the Lease
// backup-controller-quiesce in namespace, the app's namespace, as its kind
// and name ("RestoreRun restore"), or "" when the namespace holds no such
// Lease.
//
// Parameters:
//   - namespace is the app's namespace, where the runs take the Lease.
//
// It reads the Lease by its name, and the holder from the
// backup.wlz.li/lease-holder-kind label and the
// backup.wlz.li/lease-holder-name annotation the controller writes on it.
func quiesceLeaseHolder(t *testing.T, namespace string) string {
	t.Helper()
	out := mustKubectl(t, "", "-n", namespace, "get", "lease", "backup-controller-quiesce", "-o", "json", "--ignore-not-found")
	if strings.TrimSpace(out) == "" {
		return ""
	}
	var lease struct {
		Metadata struct {
			Labels      map[string]string `json:"labels"`
			Annotations map[string]string `json:"annotations"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(out), &lease); err != nil {
		t.Fatalf("decode the quiesce Lease: %v", err)
	}
	return lease.Metadata.Labels["backup.wlz.li/lease-holder-kind"] + " " + lease.Metadata.Annotations["backup.wlz.li/lease-holder-name"]
}

// readBackupRun reads a BackupRun as the API server holds it, or nil when it
// does not exist yet.
func readBackupRun(t *testing.T, namespace, name string) (*backupv1alpha1.BackupRun, error) {
	t.Helper()
	out, err := kubectlQuick(namespace, "get", "backuprun", name, "-o", "json", "--ignore-not-found")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out) == "" {
		return nil, nil
	}
	run := &backupv1alpha1.BackupRun{}
	if err := json.Unmarshal([]byte(out), run); err != nil {
		return nil, fmt.Errorf("decode BackupRun %s/%s: %w", namespace, name, err)
	}
	return run, nil
}

// readRestoreRun reads a RestoreRun as the API server holds it, or nil when it
// does not exist yet.
func readRestoreRun(t *testing.T, namespace, name string) (*backupv1alpha1.RestoreRun, error) {
	t.Helper()
	out, err := kubectlQuick(namespace, "get", "restorerun", name, "-o", "json", "--ignore-not-found")
	if err != nil {
		return nil, err
	}
	if strings.TrimSpace(out) == "" {
		return nil, nil
	}
	run := &backupv1alpha1.RestoreRun{}
	if err := json.Unmarshal([]byte(out), run); err != nil {
		return nil, fmt.Errorf("decode RestoreRun %s/%s: %w", namespace, name, err)
	}
	return run, nil
}

// TestQuiesceExclusion checks on the e2e cluster that a RestoreRun which has
// stopped the app and a namespace BackupRun never stop it together: the backup
// waits with reason SourceBusy until the restore has given the app back,
// records the replicas the app has then, and both runs end Succeeded with the
// app running again behind its Flux Kustomization.
//
// The strict client cannot show this: it has no kustomize-controller to notice
// a scale-down behind a suspended Kustomization, and no second controller
// process to race the first on a real API server. The namespace, the app and
// the Kustomization that applies it belong to the test, and the check that at
// most one quiesce Lease exists is sampled while the runs work; the run
// statuses and the final replica count carry the real assertion.
//
// The runs stop and start the Deployments through the scale subresource
// alone: a Warn admission policy on the namespace makes the API server
// return a warning for every write the controller sends for a Deployment,
// the controller logs each one, and the test checks each was an update of
// /scale (see watchDeploymentWrites).
func TestQuiesceExclusion(t *testing.T) {
	ns := newTestNamespace(t, "e2e-quiesce")
	repo := newResticRepo(t, "e2e/"+ns.Name+"/data")
	evidence := func() string { return runEvidence(ns.Name) }

	const (
		claim = "data"
		app   = "app"
	)
	kustomizationNamespace, kustomizationName := applyFluxKustomization(t, ns.Name, app)
	t.Logf("Kustomization %s/%s applies Deployment %s/%s", kustomizationNamespace, kustomizationName, ns.Name, app)

	// appObjects follows the repository Secret in one stream, so it starts
	// with its own document separator.
	appObjects := `---
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
          image: %[4]s
          imagePullPolicy: IfNotPresent
          command: [/bin/sh, -ec]
          args:
            - printf '%%s' "$KNOWN" > /data/known.txt && sync && exec sleep infinity
          env:
            - {name: KNOWN, value: quiesce-exclusion}
          volumeMounts:
            - {name: data, mountPath: /data}
          resources:
            requests: {cpu: 10m, memory: 16Mi}
      volumes:
        - name: data
          persistentVolumeClaim: {claimName: %[2]s}
`
	apply(t, repo.secretManifest(ns.Name, "restic-data")+fmt.Sprintf(appObjects, ns.Name, claim, ns.Queue, fluxImageRef(t, "busybox")))
	waitFor(t, "the app's Deployment to be ready", 5*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "deployment", app, "-o", "jsonpath={.spec.replicas}/{.status.readyReplicas}")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		return out == "2/2", "replicas/ready = " + out, nil
	}, evidence)
	// A backup of a claim that holds no file takes no snapshot, so the
	// writer's file has to be there first.
	waitFor(t, "the writer to write known.txt", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", "deploy/writer", "--", "cat", "/data/known.txt")
		if err != nil {
			return false, "not yet: " + firstLine(err.Error()), nil
		}
		return out == "quiesce-exclusion", fmt.Sprintf("file holds %q", out), nil
	}, evidence)

	// One backup first, so the restore has a snapshot to reach.
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: first
  namespace: %s
spec:
  source: %s
  timeout: 8m
`, ns.Name, claim))
	waitFor(t, "the first BackupRun to succeed", 10*time.Minute, 3*time.Second, func() (bool, string, error) {
		run, err := readBackupRun(t, ns.Name, "first")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run == nil {
			return false, "not created yet", nil
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions)), nil
	}, evidence)
	if run, _ := readBackupRun(t, ns.Name, "first"); run == nil || run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("the first BackupRun did not succeed\n%s", evidence())
	}

	// From here on every write the controller sends for a Deployment in the
	// namespace is seen, so the test can check the runs stop and start the
	// Deployments through /scale alone.
	writes := watchDeploymentWrites(t, ns.Name, "writer")

	// The restore stops the app, and the second backup starts right after it
	// has: the two must not stop the app together. The restore also stops the
	// writer, which mounts the claim, since an in-place restore starts only
	// once no pod mounts it.
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: restore
  namespace: %s
spec:
  claim: %s
  quiesce:
    - {kind: Deployment, name: %s}
    - {kind: Deployment, name: writer}
  timeout: 10m
`, ns.Name, claim, app))

	var quiescedAt metav1.Time
	waitFor(t, "the RestoreRun to stop the app", 10*time.Minute, 2*time.Second, func() (bool, string, error) {
		restore, err := readRestoreRun(t, ns.Name, "restore")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if restore == nil {
			return false, "not created yet", nil
		}
		if restore.Status.QuiescedAt != nil {
			quiescedAt = *restore.Status.QuiescedAt
		}
		state := fmt.Sprintf("phase=%s quiescedAt=%v replicas=%d",
			restore.Status.Phase, restore.Status.QuiescedAt, appReplicas(t, ns.Name, app))
		if restore.Status.QuiescedAt == nil && restore.Status.Phase.Finished() {
			return false, state, fmt.Errorf("the RestoreRun ended without stopping the app: %s", readyMessage(restore.Status.Conditions))
		}
		return restore.Status.QuiescedAt != nil, state, nil
	}, evidence)
	t.Logf("the restore stopped the app at %s", quiescedAt.UTC().Format(time.RFC3339))

	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: second
  namespace: %s
spec:
  all: true
  timeout: 10m
`, ns.Name))

	// The backup waits with the app down. Until the restore has given the app
	// back, the second BackupRun never holds the quiesce Lease: a poll that
	// finds it the holder fails the test (the design asks for this check to
	// be sampled). The restore releases the Lease once its stored status
	// records status.restartedAt, which can come before the restore
	// finishes, so a backup that holds the Lease after that is in order.
	// The Lease is read before the restore, so a poll fails only when the
	// restore had not recorded the restart after the backup held the Lease.
	backupHoldsEarly := func() error {
		if quiesceLeaseHolder(t, ns.Name) != "BackupRun second" {
			return nil
		}
		// A failed read of the restore proves nothing, so only a read that
		// shows the restart not yet recorded fails the poll.
		restore, err := readRestoreRun(t, ns.Name, "restore")
		if err == nil && restore != nil && restore.Status.RestartedAt == nil {
			return fmt.Errorf("the second BackupRun holds the quiesce Lease while the restore has not given the app back (phase %s)", restore.Status.Phase)
		}
		return nil
	}
	waited := false
	waitFor(t, "the second BackupRun to report the restore's wait", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		if err := backupHoldsEarly(); err != nil {
			return false, "", err
		}
		run, err := readBackupRun(t, ns.Name, "second")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run == nil {
			return false, "not created yet", nil
		}
		if len(run.Status.Quiesced) > 0 {
			return false, "", fmt.Errorf("the second BackupRun planned %+v while the restore held the app", run.Status.Quiesced)
		}
		state := fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions))
		waited = readyReason(run.Status.Conditions) == backupv1alpha1.ReasonSourceBusy &&
			strings.Contains(readyMessage(run.Status.Conditions), "RestoreRun restore")
		return waited, state, nil
	}, evidence)
	if !waited {
		t.Fatalf("the second BackupRun did not wait with reason SourceBusy naming the restore\n%s", evidence())
	}
	if replicas := appReplicas(t, ns.Name, app); replicas != 0 {
		t.Fatalf("the app stands at %d replicas while the restore holds it, want 0", replicas)
	}
	// The restore stopped both Deployments through /scale.
	checkScaledThroughScale(t, writes, app)
	checkScaledThroughScale(t, writes, "writer")

	// The restore gives the app back and ends; only then does the backup plan
	// the replicas the app has.
	waitFor(t, "the restore to succeed", 12*time.Minute, 3*time.Second, func() (bool, string, error) {
		if err := backupHoldsEarly(); err != nil {
			return false, "", err
		}
		restore, err := readRestoreRun(t, ns.Name, "restore")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if restore == nil {
			return false, "not created yet", nil
		}
		return restore.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s", restore.Status.Phase, readyMessage(restore.Status.Conditions)), nil
	}, evidence)
	if restore, _ := readRestoreRun(t, ns.Name, "restore"); restore == nil || restore.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("the restore did not succeed\n%s", evidence())
	}
	if replicas := appReplicas(t, ns.Name, app); replicas != 2 {
		t.Fatalf("the restore gave the app %d replicas back, want 2", replicas)
	}

	var second backupv1alpha1.BackupRun
	waitFor(t, "the second BackupRun to succeed", 12*time.Minute, 3*time.Second, func() (bool, string, error) {
		run, err := readBackupRun(t, ns.Name, "second")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		if run == nil {
			return false, "not created yet", nil
		}
		second = *run
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s quiesced=%+v",
			run.Status.Phase, readyMessage(run.Status.Conditions), run.Status.Quiesced), nil
	}, evidence)
	if second.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("the second BackupRun ended %s: %s\n%s", second.Status.Phase, readyMessage(second.Status.Conditions), evidence())
	}
	if len(second.Status.Quiesced) != 1 || second.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the second BackupRun recorded %+v, want Deployment %s at 2\n%s", second.Status.Quiesced, app, evidence())
	}
	if replicas := appReplicas(t, ns.Name, app); replicas != 2 {
		t.Fatalf("the app stands at %d replicas after both runs, want 2", replicas)
	}

	// The Kustomization the runs suspended is resumed, and Flux keeps the app
	// at its manifest's replicas.
	waitFor(t, "the Kustomization to be unsuspended again", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		return !kustomizationSuspended(t, kustomizationNamespace, kustomizationName),
			fmt.Sprintf("suspend=%t", kustomizationSuspended(t, kustomizationNamespace, kustomizationName)), nil
	}, evidence)
	mustKubectl(t, "", "-n", kustomizationNamespace, "annotate", "--overwrite", "kustomization", kustomizationName,
		fmt.Sprintf("reconcile.fluxcd.io/requestedAt=%d", time.Now().UnixNano()))
	waitFor(t, "Flux to keep the app at 2 replicas", 3*time.Minute, 3*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "deployment", app, "-o", "jsonpath={.status.readyReplicas}")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		return out == "2", "ready replicas = " + out, nil
	}, evidence)
	// Both runs stopped and started the app, and the restore the writer,
	// through /scale alone.
	checkScaledThroughScale(t, writes, app)
	checkScaledThroughScale(t, writes, "writer")
}

// controllerServiceAccount is the user the controller calls the API server as.
const controllerServiceAccount = "system:serviceaccount:" + controllerNamespace + ":backup-controller"

// deploymentWrites sees every write the controller makes to a Deployment in
// namespace, and to its scale subresource, while the test runs.
type deploymentWrites struct {
	// namespace is the test namespace whose Deployments are watched.
	namespace string
	// since is when the watch started; the controller's log is read from
	// then on.
	since time.Time
}

// watchDeploymentWrites installs a ValidatingAdmissionPolicy with a Warn
// binding that matches every CREATE, UPDATE and DELETE the controller's
// ServiceAccount sends for a Deployment in namespace or for its scale
// subresource, and removes both when the test ends.
//
// Parameters:
//   - namespace is the test namespace; the policy matches it by its
//     kubernetes.io/metadata.name label, so other tests are never matched.
//   - probeDeployment names a Deployment in namespace whose Scale the watch
//     updates in a dry run until the policy warns on it.
//
// A merge patch is an UPDATE to admission, so a patch of the Deployment
// itself matches as well. Each matched request is admitted, with a warning
// that names the Deployment, the operation and the subresource;
// controller-runtime's client logs every warning the API server returns, so
// the controller's log holds one line per write (see writes). The kind
// cluster keeps no audit log and the test may not reconfigure it, and the
// Deployment's managedFields cannot show these writes: a Scale with
// replicas 0 carries no spec.replicas, so the scale to 0 leaves no owner of
// spec.replicas, and the API server records no manager for an update of a
// Scale whose managedFields are empty, which the restart then is.
//
// The watch waits until the policy warns on a server-side dry run of a
// scale, impersonating the controller, so no write of the runs is missed.
func watchDeploymentWrites(t *testing.T, namespace, probeDeployment string) *deploymentWrites {
	t.Helper()
	name := "e2e-deployment-writes-" + namespace
	apply(t, fmt.Sprintf(`apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicy
metadata:
  name: %[1]s
  labels:
    e2e.backup.wlz.li/test: "true"
spec:
  failurePolicy: Ignore
  matchConstraints:
    namespaceSelector:
      matchLabels: {kubernetes.io/metadata.name: %[2]s}
    resourceRules:
      - apiGroups: [apps]
        apiVersions: ["*"]
        operations: [CREATE, UPDATE, DELETE]
        resources: [deployments, deployments/scale]
  matchConditions:
    - name: controller
      expression: request.userInfo.username == '%[3]s'
  validations:
    - expression: "false"
      messageExpression: "'e2e-write ' + request.namespace + '/' + request.name + ' ' + request.operation + ' subresource=' + request.subResource + ' dryRun=' + string(request.dryRun)"
---
apiVersion: admissionregistration.k8s.io/v1
kind: ValidatingAdmissionPolicyBinding
metadata:
  name: %[1]s
  labels:
    e2e.backup.wlz.li/test: "true"
spec:
  policyName: %[1]s
  validationActions: [Warn]
`, name, namespace, controllerServiceAccount))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
		defer cancel()
		for _, kind := range []string{"validatingadmissionpolicybinding", "validatingadmissionpolicy"} {
			if _, err := kubectl(ctx, "", "delete", kind, name, "--ignore-not-found"); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	})

	path := fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s/scale", namespace, probeDeployment)
	// The probe's Scale carries no resourceVersion, so a status write to the
	// Deployment between the read and the dry run is no Conflict.
	var scaleObject map[string]any
	if err := json.Unmarshal([]byte(mustKubectl(t, "", "get", "--raw", path)), &scaleObject); err != nil {
		t.Fatalf("decode the Scale of %s/%s: %v", namespace, probeDeployment, err)
	}
	if meta, ok := scaleObject["metadata"].(map[string]any); ok {
		delete(meta, "resourceVersion")
	}
	scaleBody, err := json.Marshal(scaleObject)
	if err != nil {
		t.Fatal(err)
	}
	scale := string(scaleBody)
	waitFor(t, "the write policy to warn on a scale", 2*time.Minute, 2*time.Second, func() (bool, string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "--as", controllerServiceAccount,
			"replace", "--raw", path+"?dryRun=All", "-f", "-")
		cmd.Stdin = strings.NewReader(scale)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return false, "", fmt.Errorf("dry-run update of %s's Scale as the controller: %w: %s", probeDeployment, err, out)
		}
		warned := strings.Contains(string(out), "e2e-write "+namespace+"/"+probeDeployment)
		return warned, fmt.Sprintf("warned=%t", warned), nil
	}, nil)
	return &deploymentWrites{namespace: namespace, since: time.Now()}
}

// writes returns the controller's writes to the Deployment named name since
// the watch started, one "OPERATION subresource=<name>" per request, with
// "subresource=" empty for a write to the Deployment itself. Dry runs, the
// watch's own probes, are left out.
//
// It reads them from the controller's log, which holds the warning the
// policy returned for each (see watchDeploymentWrites). A controller
// container that started after the watch began lost the earlier lines,
// so the test fails on it rather than miss writes.
func (w *deploymentWrites) writes(t *testing.T, name string) []string {
	t.Helper()
	started := mustKubectl(t, "", "-n", controllerNamespace, "get", "pods", "-l", "app.kubernetes.io/name=backup-controller",
		"-o", `jsonpath={range .items[*]}{.metadata.name} {.status.containerStatuses[0].state.running.startedAt}{"\n"}{end}`)
	for _, line := range strings.Split(strings.TrimSpace(started), "\n") {
		pod, at, _ := strings.Cut(line, " ")
		if since, err := time.Parse(time.RFC3339, at); err != nil || since.After(w.since) {
			t.Fatalf("controller pod %s runs since %q, after the write watch began at %s, so its log misses writes",
				pod, at, w.since.UTC().Format(time.RFC3339))
		}
	}
	log := mustKubectl(t, "", "-n", controllerNamespace, "logs", "deploy/backup-controller",
		"--since-time="+w.since.UTC().Format(time.RFC3339))
	marker := "e2e-write " + w.namespace + "/" + name + " "
	var writes []string
	for _, line := range strings.Split(log, "\n") {
		_, rest, ok := strings.Cut(line, marker)
		if !ok {
			continue
		}
		rest, _, _ = strings.Cut(rest, "\"")
		write, dryRun, _ := strings.Cut(rest, " dryRun=")
		if dryRun == "true" {
			continue
		}
		writes = append(writes, write)
	}
	return writes
}

// checkScaledThroughScale fails the test unless the controller wrote a
// Deployment only through its scale subresource, and did write it.
//
// Parameters:
//   - writes watches the test namespace (see watchDeploymentWrites).
//   - name is a Deployment a run stopped and started again.
//
// Two sources back it: the policy's warnings in the controller's log list
// every write the controller made, each of which must be an UPDATE of
// subresource scale; and the Deployment's managedFields must hold no entry
// of the controller's field manager for the object itself, which any
// update or patch of the Deployment would leave.
func checkScaledThroughScale(t *testing.T, writes *deploymentWrites, name string) {
	t.Helper()
	seen := writes.writes(t, name)
	t.Logf("the controller's writes to Deployment %s/%s: %q", writes.namespace, name, seen)
	if len(seen) == 0 {
		t.Errorf("the controller's log shows no write to Deployment %s/%s, which a run stopped and started", writes.namespace, name)
	}
	for _, write := range seen {
		if write != "UPDATE subresource=scale" {
			t.Errorf("the controller sent %s for Deployment %s/%s; it may only update /scale", write, writes.namespace, name)
		}
	}

	out := mustKubectl(t, "", "-n", writes.namespace, "get", "deployment", name, "-o", "json", "--show-managed-fields")
	var deployment struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
	}
	if err := json.Unmarshal([]byte(out), &deployment); err != nil {
		t.Fatalf("decode Deployment %s/%s: %v", writes.namespace, name, err)
	}
	for _, entry := range deployment.Metadata.ManagedFields {
		if entry.Manager == backupv1alpha1.FieldManager && entry.Subresource != "scale" {
			t.Errorf("Deployment %s/%s has a managedFields entry of %s (operation %s, subresource %q, fields %s); the controller may only write /scale",
				writes.namespace, name, entry.Manager, entry.Operation, entry.Subresource, entry.FieldsV1.GetRawString())
		}
	}
}
