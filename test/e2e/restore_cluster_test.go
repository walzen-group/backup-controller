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

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// The names of what hack/e2e/flux and hack/e2e/cnpg install, which the
// Cluster test below drives.
const (
	// fluxSystem is the namespace the flux component installs
	// kustomize-controller, source-controller and the OCI registry into.
	fluxSystem = "flux-system"

	// fluxRegistry is the in-cluster OCI registry the flux component
	// installs, and where the test pushes the artifact its Kustomization
	// applies.
	fluxRegistry = "e2e-registry.flux-system.svc.cluster.local:5000"

	// cnpgBucket is the RustFS bucket the database archives live in, beside
	// the volsync bucket the volume repositories live in (hack/e2e/rustfs).
	cnpgBucket = "postgres"

	// clusterName is the Cluster the test applies, backs up and restores.
	clusterName = "pg"

	// storeName is the ObjectStore the Cluster archives through.
	storeName = "store"

	// cnpgHealthy is the status.phase CloudNativePG reports for a Cluster
	// whose instances are up and consistent.
	cnpgHealthy = "Cluster in healthy state"

	// skipWalCheckAnnotation is what the bootstrap webhook sets on a Cluster
	// it recovered, so the barman plugin archives into a prefix that already
	// holds WAL (internal/bootstrap/webhook.go, SkipCheckAnnotation).
	skipWalCheckAnnotation = "cnpg.io/skipEmptyWalArchiveCheck"
)

// cnpgCluster is the part of a CloudNativePG Cluster the tests read.
type cnpgCluster struct {
	Metadata struct {
		UID         string            `json:"uid"`
		Labels      map[string]string `json:"labels"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		Bootstrap map[string]struct {
			Source string `json:"source"`
		} `json:"bootstrap"`
		ExternalClusters []struct {
			Name   string `json:"name"`
			Plugin struct {
				Name       string            `json:"name"`
				Parameters map[string]string `json:"parameters"`
			} `json:"plugin"`
		} `json:"externalClusters"`
	} `json:"spec"`
	Status struct {
		Phase      string `json:"phase"`
		Conditions []struct {
			Type   string `json:"type"`
			Status string `json:"status"`
		} `json:"conditions"`
	} `json:"status"`
}

// condition returns the status of the named condition, or an empty string
// when the Cluster has none.
func (c cnpgCluster) condition(typ string) string {
	for _, cond := range c.Status.Conditions {
		if cond.Type == typ {
			return cond.Status
		}
	}
	return ""
}

// recoveredFor says why the Cluster is not the recovery of the named
// RestoreRun, and returns an empty string when it is: the run's annotation,
// the recovery and externalClusters entry the webhook wrote (see
// bootstrap.setRecovery), and no initdb left beside them.
func (c cnpgCluster) recoveredFor(runName string) string {
	if got := c.Metadata.Annotations[backupv1alpha1.AnnotationRestoreRun]; got != runName {
		return fmt.Sprintf("%s is %q, want %q", backupv1alpha1.AnnotationRestoreRun, got, runName)
	}
	if got := c.Metadata.Annotations[skipWalCheckAnnotation]; got != "enabled" {
		return fmt.Sprintf("%s is %q, want \"enabled\"", skipWalCheckAnnotation, got)
	}
	if _, ok := c.Spec.Bootstrap["initdb"]; ok {
		return "spec.bootstrap still holds initdb beside the recovery"
	}
	recovery, ok := c.Spec.Bootstrap["recovery"]
	if !ok {
		return "spec.bootstrap holds no recovery"
	}
	if recovery.Source != "backup-controller" {
		return fmt.Sprintf("the recovery's source is %q, want \"backup-controller\"", recovery.Source)
	}
	for _, external := range c.Spec.ExternalClusters {
		if external.Name != "backup-controller" {
			continue
		}
		if external.Plugin.Name != "barman-cloud.cloudnative-pg.io" {
			return fmt.Sprintf("the externalClusters entry's plugin is %q, want barman-cloud.cloudnative-pg.io", external.Plugin.Name)
		}
		if got := external.Plugin.Parameters["barmanObjectName"]; got != storeName {
			return fmt.Sprintf("the externalClusters entry's barmanObjectName is %q, want %q", got, storeName)
		}
		if got := external.Plugin.Parameters["serverName"]; got != clusterName {
			return fmt.Sprintf("the externalClusters entry's serverName is %q, want %q", got, clusterName)
		}
		return ""
	}
	return "spec.externalClusters names no backup-controller entry"
}

// readCNPGCluster reads Cluster name in namespace, and fails the test when it
// can't be read.
func readCNPGCluster(t *testing.T, namespace, name string) cnpgCluster {
	t.Helper()
	var cluster cnpgCluster
	if err := getJSON(namespace, "clusters.postgresql.cnpg.io", name, &cluster); err != nil {
		t.Fatal(err)
	}
	return cluster
}

// findRestoreItem returns the run's item for name, and whether the run has
// one.
func findRestoreItem(run backupv1alpha1.RestoreRun, name string) (backupv1alpha1.RestoreItem, bool) {
	for _, item := range run.Status.Items {
		if item.Name == name {
			return item, true
		}
	}
	return backupv1alpha1.RestoreItem{}, false
}

// findBackupItem returns the run's item for name, and whether the run has one.
func findBackupItem(run backupv1alpha1.BackupRun, name string) (backupv1alpha1.BackupItem, bool) {
	for _, item := range run.Status.Items {
		if item.Name == name {
			return item, true
		}
	}
	return backupv1alpha1.BackupItem{}, false
}

// readyReason returns the Ready condition's reason, or an empty string.
func readyReason(conds []metav1.Condition) string {
	for _, c := range conds {
		if c.Type == backupv1alpha1.ConditionReady {
			return c.Reason
		}
	}
	return ""
}

// pinnedFluxCLI returns the flux-cli image reference (tag@digest) the flux
// component pins for pushing artifacts, the image its own check pushes with.
func pinnedFluxCLI(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "hack", "e2e", "flux", "pins.json"))
	if err != nil {
		t.Fatalf("read the flux component's pins.json: %v", err)
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
	if err := json.Unmarshal(raw, &pins); err != nil {
		t.Fatalf("decode the flux component's pins.json: %v", err)
	}
	cli, ok := pins.Flux.Images["flux-cli"]
	if !ok || cli.Image == "" || cli.Tag == "" || cli.Digest == "" {
		t.Fatalf("the flux component's pins.json names no complete flux-cli image: %+v", pins.Flux.Images)
	}
	return fmt.Sprintf("%s:%s@%s", cli.Image, cli.Tag, cli.Digest)
}

// prodPostgresImage returns spec.imageName for the test's Cluster: the
// PostgreSQL operand image prod's Clusters run, as versions.json records it
// and hack/e2e/cnpg pins it.
func prodPostgresImage(t *testing.T) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "versions.json"))
	if err != nil {
		t.Fatalf("read versions.json: %v", err)
	}
	var versions struct {
		Components struct {
			Operand struct {
				Image string `json:"image"`
			} `json:"postgresql-operand"`
		} `json:"components"`
	}
	if err := json.Unmarshal(raw, &versions); err != nil {
		t.Fatalf("decode versions.json: %v", err)
	}
	if versions.Components.Operand.Image == "" {
		t.Fatalf("versions.json records no postgresql-operand image")
	}
	return versions.Components.Operand.Image
}

// TestARecoveredClusterSurvivesAFluxReconcile proves on the cluster what a
// RestoreRun's Recovering check relies on: a Cluster the bootstrap webhook
// recovered for a run, marked with backup.wlz.li/restore-run, keeps that mark
// and the recovery itself when its owner applies it from its source again.
//
// The Cluster's owner here is Flux, as it is in prod: the test pushes the
// Cluster manifest into the flux component's in-cluster OCI registry the way
// hack/e2e/flux/flux.sh check does, and points an OCIRepository and a
// Kustomization at it. It gives the Cluster an ObjectStore and marks it
// backup.wlz.li/enabled, writes a row into it, and takes a base backup with a
// BackupRun on spec.database.
//
// A RestoreRun on spec.database then deletes the Cluster and waits for its
// owner to create it again. Flux creates it, the bootstrap webhook recovers it
// for the run and marks it, and the run's item moves to Recovering. While the
// item is Recovering, the test changes the manifest Flux applies and reconciles
// the Kustomization again: the label it adds is the proof that the
// server-side apply reached the Cluster, and the test then checks the mark,
// the recovery the webhook wrote and the run's item all survived it. The run
// has to reach Succeeded with the mark still on the Cluster, and the recovered
// database has to hold the row written before the backup.
func TestARecoveredClusterSurvivesAFluxReconcile(t *testing.T) {
	ns := newTestNamespace(t, "e2e-cluster-restore")
	name := ns.Name
	accessKey, secretKey := rustfsCredentials(t)

	const (
		// restoreRunName and backupRunName are the runs the test creates.
		restoreRunName = "restore-database"
		backupRunName  = "backup-database"

		// s3Secret is the Secret the ObjectStore reads its keys from.
		s3Secret = "s3"

		// roundLabel is the label the test adds to the manifest Flux applies
		// for its second reconcile, and round its value.
		roundLabel = "e2e.backup.wlz.li/flux-round"
		round      = "2"

		// row is the row the test writes before the base backup and reads
		// back from the recovered database.
		row = "written before the restore"
	)
	prefix := cnpgBucket + "/" + ns.Name
	evidence := func() string {
		return runEvidence(ns.Name) + collect(
			[]string{"-n", ns.Name, "get", "clusters.postgresql.cnpg.io,backups.postgresql.cnpg.io,objectstores.barmancloud.cnpg.io", "-o", "yaml"},
			[]string{"-n", ns.Name, "logs", "-l", "cnpg.io/cluster=" + clusterName, "--all-containers", "--tail=100", "--prefix"},
			[]string{"-n", fluxSystem, "get", "kustomizations.kustomize.toolkit.fluxcd.io,ocirepositories.source.toolkit.fluxcd.io", name, "-o", "yaml"},
			[]string{"-n", fluxSystem, "logs", "deploy/kustomize-controller", "--tail=100"},
		)
	}

	// The Cluster and the Kustomization that applies it are the namespace's
	// and flux-system's to delete, in that order: the Kustomization goes
	// first, so nothing creates the Cluster again while the namespace goes.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		for _, args := range [][]string{
			{"-n", fluxSystem, "delete", "kustomization", name, "--ignore-not-found", "--wait=true", "--timeout=60s"},
			{"-n", fluxSystem, "delete", "ocirepository", name, "--ignore-not-found", "--timeout=60s"},
			{"-n", fluxSystem, "delete", "job", name + "-push", "--ignore-not-found", "--cascade=foreground", "--timeout=60s"},
			{"-n", fluxSystem, "delete", "configmap", name, "--ignore-not-found"},
		} {
			if _, err := kubectl(ctx, "", args...); err != nil {
				t.Errorf("cleanup: %v", err)
			}
		}
	})
	t.Cleanup(func() {
		endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
		if err := deletePrefix(endpoint, accessKey, secretKey, cnpgBucket, ns.Name+"/"); err != nil {
			t.Errorf("cleanup: delete s3://%s/%s/: %v", cnpgBucket, ns.Name, err)
		}
	})

	// The ObjectStore and its Secret the Cluster archives through. Prod's
	// terragrunt unit declares them beside the Cluster in one Kustomization;
	// the test creates them itself, so the first apply of the Cluster cannot
	// race the ObjectStore it has to be read through.
	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %[2]s
  namespace: %[1]s
stringData:
  AWS_ACCESS_KEY_ID: %[3]q
  AWS_SECRET_ACCESS_KEY: %[4]q
---
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: %[5]s
  namespace: %[1]s
spec:
  configuration:
    destinationPath: s3://%[6]s/%[1]s/
    endpointURL: %[7]s
    s3Credentials:
      accessKeyId: {name: %[2]s, key: AWS_ACCESS_KEY_ID}
      secretAccessKey: {name: %[2]s, key: AWS_SECRET_ACCESS_KEY}
`, ns.Name, s3Secret, accessKey, secretKey, storeName, cnpgBucket, rustfsInCluster))

	// The Cluster Flux applies. spec.bootstrap.initdb is there on purpose:
	// prod's source holds initdb, so every reconcile of a recovered Cluster
	// offers the webhook the two bootstrap methods it has to fix up
	// (keepRecovery).
	manifest := fmt.Sprintf(`apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: %[1]s
  labels:
    app.kubernetes.io/name: %[1]s
  annotations:
    backup.wlz.li/enabled: "true"
spec:
  instances: 1
  imageName: %[2]s
  storage:
    storageClass: standard
    size: 512Mi
  postgresql:
    parameters:
      min_wal_size: 32MB
      max_wal_size: 64MB
      shared_buffers: 32MB
  bootstrap:
    initdb: {}
  plugins:
    - name: barman-cloud.cloudnative-pg.io
      isWALArchiver: true
      parameters:
        barmanObjectName: %[3]s
`, clusterName, prodPostgresImage(t), storeName)

	// Push the manifest as an OCI artifact from inside the cluster, the way
	// the flux component's own check does, and give a Kustomization the
	// OCIRepository to apply it from. wait is off: a reconcile then ends with
	// the apply, where a health check would sit on the recovered Cluster and
	// hold the next reconcile behind it.
	mustKubectl(t, manifest, "-n", fluxSystem, "create", "configmap", name, "--from-file=cluster.yaml=/dev/stdin")
	apply(t, fmt.Sprintf(`apiVersion: batch/v1
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
              mkdir /tmp/m && cp /manifests/cluster.yaml /tmp/m/ &&
              flux push artifact "oci://%[4]s/%[1]s:%[1]s" --path=/tmp/m --source=e2e
              --revision=e2e@sha1:0000000000000000000000000000000000000000 --insecure-registry
          volumeMounts: [{name: m, mountPath: /manifests}, {name: tmp, mountPath: /tmp}]
      volumes: [{name: m, configMap: {name: %[1]s}}, {name: tmp, emptyDir: {}}]
`, name, fluxSystem, pinnedFluxCLI(t), fluxRegistry))
	waitFor(t, "the artifact push Job", 5*time.Minute, 3*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(fluxSystem, "get", "job", name+"-push", "-o", "json")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		var job struct {
			Status struct {
				Succeeded int32 `json:"succeeded"`
				Failed    int32 `json:"failed"`
			} `json:"status"`
		}
		if err := json.Unmarshal([]byte(out), &job); err != nil {
			return false, "", fmt.Errorf("decode Job %s-push: %w", name, err)
		}
		state := fmt.Sprintf("succeeded=%d failed=%d", job.Status.Succeeded, job.Status.Failed)
		if job.Status.Failed > 0 {
			return false, state, fmt.Errorf("the push Job failed; its pod's log says why")
		}
		return job.Status.Succeeded > 0, state, nil
	}, func() string {
		return collect([]string{"-n", fluxSystem, "logs", "job/" + name + "-push"})
	})

	apply(t, fmt.Sprintf(`apiVersion: source.toolkit.fluxcd.io/v1
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
  wait: false
  targetNamespace: %[4]s
  sourceRef: {kind: OCIRepository, name: %[1]s}
`, name, fluxSystem, fluxRegistry, ns.Name))

	// Flux applies the Cluster, which comes up on the empty archive the
	// webhook admitted, and starts archiving.
	waitFor(t, "the Cluster Flux applied to become healthy", 10*time.Minute, 3*time.Second, func() (bool, string, error) {
		var applied cnpgCluster
		if err := getJSON(ns.Name, "clusters.postgresql.cnpg.io", clusterName, &applied); err != nil {
			return false, firstLine(err.Error()), nil
		}
		state := fmt.Sprintf("phase=%q archiving=%s", applied.Status.Phase, applied.condition("ContinuousArchiving"))
		return applied.Status.Phase == cnpgHealthy && applied.condition("ContinuousArchiving") == "True", state, nil
	}, evidence)

	applied := readCNPGCluster(t, ns.Name, clusterName)
	if got, ok := applied.Metadata.Annotations[backupv1alpha1.AnnotationRestoreRun]; ok {
		t.Fatalf("before the restore, Cluster %s already carries %s=%q\n%s", clusterName, backupv1alpha1.AnnotationRestoreRun, got, evidence())
	}
	if _, ok := applied.Spec.Bootstrap["initdb"]; !ok || len(applied.Spec.Bootstrap) != 1 {
		t.Fatalf("before the restore, Cluster %s spec.bootstrap = %v, want initdb alone\n%s", clusterName, applied.Spec.Bootstrap, evidence())
	}
	t.Logf("Cluster %s (UID %s) runs on its own initdb, Flux applied it; the webhook has not recovered it", clusterName, applied.Metadata.UID)

	// A row written before the base backup, so the recovered database can be
	// read back for it.
	waitFor(t, "the Cluster's database to take the row", 3*time.Minute, 3*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", clusterName+"-1", "-c", "postgres", "--", "psql", "-v", "ON_ERROR_STOP=1", "-tAc",
			fmt.Sprintf("create table if not exists restore_demo (id int primary key, note text); "+
				"insert into restore_demo values (1, '%s') on conflict (id) do nothing; "+
				"select note from restore_demo where id = 1", row))
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		// psql prints a result for each statement, so the select's row is
		// the last line.
		got := lastLine(out)
		return got == row, fmt.Sprintf("note=%q", got), nil
	}, evidence)

	// The base backup the recovery will start from, taken through the
	// controller the way prod takes it.
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  database: %[3]s
  timeout: 8m
`, backupRunName, ns.Name, clusterName))
	var backup backupv1alpha1.BackupRun
	waitFor(t, "BackupRun "+backupRunName, 10*time.Minute, 3*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "get", "backuprun", backupRunName, "-o", "json")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		backup = backupv1alpha1.BackupRun{}
		if err := json.Unmarshal([]byte(out), &backup); err != nil {
			return false, "", fmt.Errorf("decode BackupRun: %w", err)
		}
		return backup.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s", backup.Status.Phase, readyMessage(backup.Status.Conditions)), nil
	}, evidence)
	if backup.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("BackupRun %s ended %s: %s\n%s", backupRunName, backup.Status.Phase, readyMessage(backup.Status.Conditions), evidence())
	}
	backupItem, ok := findBackupItem(backup, clusterName)
	if !ok || backupItem.Phase != backupv1alpha1.ItemSucceeded || backupItem.Backup == "" {
		t.Fatalf("BackupRun %s items = %+v, want one Cluster item Succeeded naming its base backup\n%s", backupRunName, backup.Status.Items, evidence())
	}
	t.Logf("BackupRun %s took base backup %s of the Cluster", backupRunName, backupItem.Backup)

	// The restore: the run deletes the Cluster and waits for its owner to
	// create it again, which is when the webhook recovers it.
	apply(t, fmt.Sprintf(`apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: %[1]s
  namespace: %[2]s
spec:
  database: %[3]s
  timeout: 15m
`, restoreRunName, ns.Name, clusterName))
	var run backupv1alpha1.RestoreRun
	waitFor(t, "RestoreRun "+restoreRunName+" to ask for the Cluster to be created again", 8*time.Minute, 2*time.Second, func() (bool, string, error) {
		if err := getJSON(ns.Name, "restorerun", restoreRunName, &run); err != nil {
			return false, firstLine(err.Error()), nil
		}
		state := fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions))
		if run.Status.Phase.Finished() {
			return false, state, fmt.Errorf("RestoreRun ended %s before the Cluster came back", run.Status.Phase)
		}
		return readyReason(run.Status.Conditions) == backupv1alpha1.ReasonRecreate, state, nil
	}, evidence)
	item, ok := findRestoreItem(run, clusterName)
	if !ok || item.Phase != backupv1alpha1.ItemDeleted || item.ClusterUID == "" {
		t.Fatalf("RestoreRun %s item when the Cluster was deleted: %+v, want Deleted with the old Cluster's UID\n%s", restoreRunName, run.Status.Items, evidence())
	}
	t.Logf("RestoreRun %s deleted Cluster %s (UID %s) and waits for it to come back", restoreRunName, clusterName, item.ClusterUID)

	// Flux creates the Cluster again. The webhook sees the run's item is
	// Deleted and recovers the new Cluster for the run, marking it.
	mustKubectl(t, "", "-n", fluxSystem, "annotate", "--overwrite", "kustomization", name,
		"reconcile.fluxcd.io/requestedAt="+fmt.Sprint(time.Now().UnixNano()))
	waitFor(t, "RestoreRun "+restoreRunName+" to move the item to Recovering", 6*time.Minute, 2*time.Second, func() (bool, string, error) {
		if err := getJSON(ns.Name, "restorerun", restoreRunName, &run); err != nil {
			return false, firstLine(err.Error()), nil
		}
		item, ok := findRestoreItem(run, clusterName)
		if !ok {
			return false, "no item for the Cluster", nil
		}
		state := fmt.Sprintf("phase=%s item=%s %s", run.Status.Phase, item.Phase, item.Message)
		if item.Phase == backupv1alpha1.ItemFailed {
			return false, state, fmt.Errorf("the item failed: %s", item.Message)
		}
		if run.Status.Phase.Finished() {
			return false, state, fmt.Errorf("RestoreRun ended %s", run.Status.Phase)
		}
		return item.Phase == backupv1alpha1.ItemRecovering, state, nil
	}, evidence)

	recovered := readCNPGCluster(t, ns.Name, clusterName)
	if why := recovered.recoveredFor(restoreRunName); why != "" {
		t.Fatalf("the Cluster Flux created again is not the run's recovery: %s\n%s", why, evidence())
	}
	t.Logf("the webhook recovered Cluster %s (UID %s) for RestoreRun %s; the item is Recovering",
		clusterName, recovered.Metadata.UID, restoreRunName)

	// While the item is Recovering, make Flux apply the Cluster again, with
	// the manifest changed so the apply is a real update of the stored
	// Cluster. A server-side apply that dropped the annotation here would
	// fail the item on the run's next pass (restoreDatabase's Recovering
	// case), so the item's phase afterwards says whether the recovery
	// survived the reconcile.
	patch := fmt.Sprintf(`{"spec":{"patches":[{"patch":"[{\"op\":\"add\",\"path\":\"/metadata/labels/%s\",\"value\":\"%s\"}]","target":{"kind":"Cluster","name":"%s"}}]}}`,
		strings.ReplaceAll(roundLabel, "/", "~1"), round, clusterName)
	mustKubectl(t, "", "-n", fluxSystem, "patch", "kustomization", name, "--type=merge", "-p", patch)
	mustKubectl(t, "", "-n", fluxSystem, "annotate", "--overwrite", "kustomization", name,
		"reconcile.fluxcd.io/requestedAt="+fmt.Sprint(time.Now().UnixNano()))
	waitFor(t, "Flux to apply the changed manifest", 4*time.Minute, 3*time.Second, func() (bool, string, error) {
		var applied cnpgCluster
		if err := getJSON(ns.Name, "clusters.postgresql.cnpg.io", clusterName, &applied); err != nil {
			return false, firstLine(err.Error()), nil
		}
		return applied.Metadata.Labels[roundLabel] == round, fmt.Sprintf("label %s=%q", roundLabel, applied.Metadata.Labels[roundLabel]), nil
	}, evidence)

	applied = readCNPGCluster(t, ns.Name, clusterName)
	if why := applied.recoveredFor(restoreRunName); why != "" {
		t.Fatalf("after Flux applied the changed manifest, Cluster %s: %s\n%s", clusterName, why, evidence())
	}
	if err := getJSON(ns.Name, "restorerun", restoreRunName, &run); err != nil {
		t.Fatalf("read RestoreRun %s after the reconcile: %v", restoreRunName, err)
	}
	if item, ok := findRestoreItem(run, clusterName); ok {
		t.Logf("after Flux applied the changed manifest: RestoreRun %s item is %s; Cluster %s still holds %s and its recovery",
			restoreRunName, item.Phase, clusterName, backupv1alpha1.AnnotationRestoreRun)
	}
	if run.Status.Phase == backupv1alpha1.RunPhaseFailed {
		t.Fatalf("the run failed over the reconcile: %s\n%s", readyMessage(run.Status.Conditions), evidence())
	}

	// The recovery then has to finish, with the mark still on the Cluster.
	waitFor(t, "RestoreRun "+restoreRunName+" to finish", 12*time.Minute, 3*time.Second, func() (bool, string, error) {
		if err := getJSON(ns.Name, "restorerun", restoreRunName, &run); err != nil {
			return false, firstLine(err.Error()), nil
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("phase=%s %s", run.Status.Phase, readyMessage(run.Status.Conditions)), nil
	}, evidence)
	if run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("RestoreRun %s ended %s: %s\n%s", restoreRunName, run.Status.Phase, readyMessage(run.Status.Conditions), evidence())
	}
	item, ok = findRestoreItem(run, clusterName)
	if !ok || item.Phase != backupv1alpha1.ItemSucceeded {
		t.Fatalf("RestoreRun %s item = %+v, want Succeeded\n%s", restoreRunName, run.Status.Items, evidence())
	}
	if item.BaseBackup == "" {
		t.Errorf("RestoreRun %s item records no base backup", restoreRunName)
	}
	applied = readCNPGCluster(t, ns.Name, clusterName)
	if why := applied.recoveredFor(restoreRunName); why != "" {
		t.Fatalf("at the end of the run, Cluster %s: %s\n%s", clusterName, why, evidence())
	}
	if applied.Status.Phase != cnpgHealthy {
		t.Errorf("Cluster %s phase is %q after the run succeeded, want %q", clusterName, applied.Status.Phase, cnpgHealthy)
	}
	t.Logf("RestoreRun %s Succeeded from base backup %s; Cluster %s is healthy, recovered for the run, and still carries %s=%s",
		restoreRunName, item.BaseBackup, clusterName, backupv1alpha1.AnnotationRestoreRun, restoreRunName)

	// The recovery is real: the recovered database holds the row.
	waitFor(t, "the recovered database to hold the row", 3*time.Minute, 2*time.Second, func() (bool, string, error) {
		out, err := kubectlQuick(ns.Name, "exec", clusterName+"-1", "-c", "postgres", "--", "psql", "-tAc",
			"select note from restore_demo where id = 1")
		if err != nil {
			return false, firstLine(err.Error()), nil
		}
		got := strings.TrimSpace(out)
		return got == row, fmt.Sprintf("note=%q", got), nil
	}, evidence)
	t.Logf("the recovered database holds %q, and s3://%s/%s/ holds the archive", row, cnpgBucket, prefix)
}
