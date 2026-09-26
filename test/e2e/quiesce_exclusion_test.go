//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
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
//     busybox the Deployment's pod runs.
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

	appObjects := `apiVersion: backup.wlz.li/v1alpha1
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
	mustKubectl(t, "", "-n", ns.Name, "annotate", "deployment", app, backupv1alpha1.AnnotationQuiesce+"=true")
	waitFor(t, "the app's Deployment to be ready", 5*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "deployment", app, "-o", "jsonpath={.spec.replicas}/{.status.readyReplicas}")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		return out == "2/2", "replicas/ready = " + out, nil
	}, evidence)

	// One backup first, so the restore has a snapshot to reach.
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: first
  namespace: %s
spec:
  claim: %s
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

	// The restore stops the app, and the second backup starts right after it
	// has: the two must not stop the app together.
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: restore
  namespace: %s
spec:
  claim: %s
  quiesce:
    - {kind: Deployment, name: %s}
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
		return restore.Status.QuiescedAt != nil, fmt.Sprintf("phase=%s quiescedAt=%v replicas=%d",
			restore.Status.Phase, restore.Status.QuiescedAt, appReplicas(t, ns.Name, app)), nil
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

	// The backup waits with the app down. Until the restore has finished,
	// the second BackupRun never holds the quiesce Lease: a poll that finds
	// it the holder fails the test (the design asks for this check to be
	// sampled).
	// The Lease is read before the restore, so a poll fails only when the
	// restore was still unfinished after the backup held the Lease.
	backupHoldsEarly := func() error {
		if quiesceLeaseHolder(t, ns.Name) != "BackupRun second" {
			return nil
		}
		restore, err := readRestoreRun(t, ns.Name, "restore")
		if err != nil || restore == nil || restore.Status.Phase.Finished() {
			return nil
		}
		return fmt.Errorf("the second BackupRun holds the quiesce Lease while the restore is unfinished (phase %s)", restore.Status.Phase)
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
}
