//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// fluxCLIImage pushes the manifests of a test app to the registry, as a CI
// job pushes an app's manifests on prod.
const fluxCLIImage = "ghcr.io/fluxcd/flux-cli:v2.9.5"

// registry is the in-cluster OCI registry Flux reads the test apps from
// (hack/kind/manifests/cluster.yaml).
const registry = "registry.registry.svc:5000"

// app is one test namespace, set up the way the infra repository sets up an
// app namespace: a LocalQueue for the runs, the restic repository Secret and
// the ObjectStore of the database. Flux applies the app's own objects from
// the registry, so a deleted Cluster or claim comes back the way it comes
// back on prod.
type app struct {
	t  *testing.T
	ns string
}

// newApp sets up the namespace of one test app.
//
// Parameters:
//   - t is the test that owns the app. Its cleanup deletes the app.
//   - name goes into the namespace name, e2e-<name>-<random>, so a leftover
//     namespace shows which test made it.
//
// It returns the app. The namespace holds the LocalQueue backups for the
// ClusterQueue backup, the Secret s3 with the S3 credentials, the Secret
// restic that names the repository s3:<endpoint>/volsync/<namespace>/data,
// and the ObjectStore store that archives into s3://postgres/<namespace>/.
// The app's own objects come later, from publish.
func newApp(t *testing.T, name string) *app {
	t.Helper()
	a := &app{t: t, ns: "e2e-" + name + "-" + suffix()}
	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %[1]s
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: backups
  namespace: %[1]s
spec:
  clusterQueue: backup
---
apiVersion: v1
kind: Secret
metadata:
  name: s3
  namespace: %[1]s
stringData:
  AWS_ACCESS_KEY_ID: %[2]s
  AWS_SECRET_ACCESS_KEY: %[3]s
---
apiVersion: v1
kind: Secret
metadata:
  name: restic
  namespace: %[1]s
stringData:
  RESTIC_REPOSITORY: s3:%[4]s/volsync/%[1]s/data
  RESTIC_PASSWORD: e2e-restic-password
  AWS_ACCESS_KEY_ID: %[2]s
  AWS_SECRET_ACCESS_KEY: %[3]s
---
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: store
  namespace: %[1]s
spec:
  configuration:
    destinationPath: s3://postgres/%[1]s/
    endpointURL: %[4]s
    s3Credentials:
      accessKeyId: {name: s3, key: AWS_ACCESS_KEY_ID}
      secretAccessKey: {name: s3, key: AWS_SECRET_ACCESS_KEY}
`, a.ns, accessKey, secretKey, s3Endpoint))
	t.Cleanup(a.delete)
	return a
}

// delete removes the app from the test cluster. It deletes the Flux
// Kustomization first and waits for it, so Flux stops applying into the
// namespace, then the OCIRepository, the push Jobs and their ConfigMaps, and
// last the namespace. It logs a failed delete and goes on with the next one.
//
// It runs as a cleanup of the test, when the test's context is already
// cancelled, so it gives the deletes a context of their own.
func (a *app) delete() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	for _, args := range [][]string{
		{"-n", "flux-system", "delete", "kustomization", a.ns, "--ignore-not-found", "--wait=true", "--timeout=2m"},
		{"-n", "flux-system", "delete", "ocirepository", a.ns, "--ignore-not-found"},
		{"-n", "flux-system", "delete", "job,configmap", "-l", "e2e.backup.wlz.li/app=" + a.ns, "--ignore-not-found"},
		{"delete", "namespace", a.ns, "--ignore-not-found", "--wait=false"},
	} {
		if _, err := run(ctx, "", args...); err != nil {
			a.t.Logf("cleanup: %v", err)
		}
	}
}

// publish makes Flux apply the app's objects, the way Flux applies an app on
// prod.
//
// Parameters:
//   - manifests are the YAML documents of the app, without namespaces.
//
// A Job in flux-system pushes the manifests to the in-cluster registry as an
// OCI artifact. publish waits for the push, then creates an OCIRepository
// and a Kustomization that applies the artifact into the app's namespace
// every 30 seconds, so Flux creates a deleted object again as it does on
// prod. A second call pushes a new revision and asks Flux to apply it at
// once. The test fails when the push fails or takes more than three minutes.
func (a *app) publish(manifests string) {
	a.t.Helper()
	revision := suffix()
	job := "push-" + revision
	apply(a.t, fmt.Sprintf(`apiVersion: v1
kind: ConfigMap
metadata:
  name: %[2]s
  namespace: flux-system
  labels: {e2e.backup.wlz.li/app: %[1]s}
data:
  app.yaml: |
%[5]s
---
apiVersion: batch/v1
kind: Job
metadata:
  name: %[2]s
  namespace: flux-system
  labels: {e2e.backup.wlz.li/app: %[1]s}
spec:
  backoffLimit: 3
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: push
          image: %[3]s
          env: [{name: HOME, value: /tmp}]
          command: [sh, -ec]
          args:
            - >-
              mkdir /tmp/m && cp /manifests/app.yaml /tmp/m/ &&
              flux push artifact oci://%[4]s/%[1]s:latest --path=/tmp/m
              --source=e2e --revision=%[6]s@sha1:0000000000000000000000000000000000000000
              --insecure-registry
          volumeMounts: [{name: m, mountPath: /manifests}]
      volumes: [{name: m, configMap: {name: %[2]s}}]
`, a.ns, job, fluxCLIImage, registry, indent(manifests, 4), revision))
	waitFor(a.t, "the push of "+a.ns, 3*time.Minute, func() (bool, string) {
		var j struct {
			Status struct{ Succeeded, Failed int } `json:"status"`
		}
		if err := getJSON("flux-system", "job", job, &j); err != nil {
			return false, err.Error()
		}
		if j.Status.Failed > 0 {
			a.t.Fatalf("the push Job %s failed:\n%s", job, a.logs("flux-system", "job/"+job))
		}
		return j.Status.Succeeded > 0, fmt.Sprintf("succeeded=%d", j.Status.Succeeded)
	}, func() string { return a.logs("flux-system", "job/"+job) })

	apply(a.t, fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: %[1]s
  namespace: flux-system
spec:
  interval: 1m
  url: oci://%[2]s/%[1]s
  ref: {tag: latest}
  insecure: true
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: %[1]s
  namespace: flux-system
spec:
  interval: 30s
  prune: true
  wait: false
  targetNamespace: %[1]s
  sourceRef: {kind: OCIRepository, name: %[1]s}
`, a.ns, registry))
	a.reconcile()
}

// unpublish deletes the app's Kustomization and waits up to five minutes
// until Flux has pruned every object it applied, as when an app is removed
// on prod.
func (a *app) unpublish() {
	a.t.Helper()
	kubectl(a.t, "", "-n", "flux-system", "delete", "kustomization", a.ns, "--wait=true", "--timeout=5m")
}

// reconcile asks Flux to fetch the app's artifact and apply it now, where
// Flux on its own waits for its next interval.
func (a *app) reconcile() {
	a.t.Helper()
	now := fmt.Sprint(time.Now().UnixNano())
	kubectl(a.t, "", "-n", "flux-system", "annotate", "--overwrite", "ocirepository", a.ns, "reconcile.fluxcd.io/requestedAt="+now)
	kubectl(a.t, "", "-n", "flux-system", "annotate", "--overwrite", "kustomization", a.ns, "reconcile.fluxcd.io/requestedAt="+now)
}

// logs reads the last 50 log lines of a workload for a failure message.
//
// Parameters:
//   - namespace is where the workload runs.
//   - target names it as kubectl logs takes it, for example job/push-1a2b3c.
//
// It returns the lines of all containers, or the error that kept kubectl
// from reading them.
func (a *app) logs(namespace, target string) string {
	out, err := run(a.t.Context(), "", "-n", namespace, "logs", target, "--all-containers", "--tail=50")
	if err != nil {
		return err.Error()
	}
	return out
}

// describe returns the state of the app for a failure message: its runs,
// Clusters, CNPG Backups, claims, pods and VolSync objects, the runs in
// full, the namespace's events, and the last 80 lines of the controller's
// log.
func (a *app) describe() string {
	var b strings.Builder
	for _, args := range [][]string{
		{"-n", a.ns, "get", "backupruns,restoreruns,volumerestores,clusters.postgresql.cnpg.io,backups.postgresql.cnpg.io,pvc,pods,replicationsources,replicationdestinations", "-o", "wide"},
		{"-n", a.ns, "get", "backupruns,restoreruns", "-o", "yaml"},
		{"-n", a.ns, "get", "events", "--sort-by=.lastTimestamp"},
		{"-n", "backup-system", "logs", "deployment/backup-controller", "--tail=80"},
	} {
		out, err := run(a.t.Context(), "", args...)
		if err != nil {
			out = err.Error()
		}
		fmt.Fprintf(&b, "$ kubectl %s\n%s\n", strings.Join(args, " "), out)
	}
	return b.String()
}
