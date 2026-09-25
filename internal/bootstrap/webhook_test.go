package bootstrap

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"errors"
	"math/big"
	"strings"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// stubProber answers the base backup question without an object store.
type stubProber struct {
	has     bool
	err     error
	backups []BaseBackup
}

func (s stubProber) HasBaseBackup(context.Context, Location) (bool, error) {
	return s.has, s.err
}

func (s stubProber) BaseBackups(context.Context, Location) ([]BaseBackup, error) {
	return s.backups, s.err
}

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("register the core types: %v", err)
	}
	if err := backupv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("register the backup types: %v", err)
	}
	// The handler reads both kinds as unstructured, so the fake client needs
	// them by GVK rather than by Go type.
	s.AddKnownTypeWithName(ObjectStoreGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(ClusterListGVK, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(ClusterListGVK.GroupVersion().WithKind("Cluster"), &unstructured.Unstructured{})
	return s
}

// cluster builds a Cluster that archives through the plugin, which is the
// shape every case here starts from.
func cluster(t *testing.T, mutate func(map[string]any)) *unstructured.Unstructured {
	t.Helper()
	object := map[string]any{
		"apiVersion": "postgresql.cnpg.io/v1",
		"kind":       "Cluster",
		"metadata": map[string]any{
			"name":      "app-pg",
			"namespace": "app",
		},
		"spec": map[string]any{
			"instances": int64(1),
			"bootstrap": map[string]any{
				"initdb": map[string]any{"database": "app", "owner": "app"},
			},
			"plugins": []any{
				map[string]any{
					"name":          PluginName,
					"isWALArchiver": true,
					"parameters":    map[string]any{"barmanObjectName": "app-pg-store"},
				},
			},
		},
	}
	if mutate != nil {
		mutate(object)
	}
	return &unstructured.Unstructured{Object: object}
}

// store and secret are the two objects ResolveLocation reads.
func store() *unstructured.Unstructured {
	s := &unstructured.Unstructured{}
	s.SetGroupVersionKind(ObjectStoreGVK)
	s.SetName("app-pg-store")
	s.SetNamespace("app")
	_ = unstructured.SetNestedMap(s.Object, map[string]any{
		"destinationPath": "s3://backups/app/",
		"endpointURL":     "https://store.example",
		"s3Credentials": map[string]any{
			"accessKeyId":     map[string]any{"name": "app-pg-backup", "key": "ACCESS_KEY_ID"},
			"secretAccessKey": map[string]any{"name": "app-pg-backup", "key": "ACCESS_SECRET_KEY"},
		},
	}, "spec", "configuration")
	return s
}

func secret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pg-backup", Namespace: "app"},
		Data: map[string][]byte{
			"ACCESS_KEY_ID":     []byte("key"),
			"ACCESS_SECRET_KEY": []byte("secret"),
		},
	}
}

func decide(t *testing.T, c *unstructured.Unstructured, prober Prober, existing ...runtime.Object) admission.Response {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}

	builder := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(secret())
	objects := append([]runtime.Object{store()}, existing...)
	builder = builder.WithRuntimeObjects(objects...)

	decider := &Decider{Client: builder.Build(), Prober: prober}
	return decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
}

// applied replays the response's patches so a test can read the Cluster the
// API server would have stored.
func applied(t *testing.T, original *unstructured.Unstructured, response admission.Response) map[string]any {
	t.Helper()
	raw, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}
	patch, err := json.Marshal(response.Patches)
	if err != nil {
		t.Fatalf("marshal the patches: %v", err)
	}
	decoded, err := jsonpatchApply(raw, patch)
	if err != nil {
		t.Fatalf("apply the patches: %v", err)
	}
	var out map[string]any
	if err := json.Unmarshal(decoded, &out); err != nil {
		t.Fatalf("decode the patched cluster: %v", err)
	}
	return out
}

// jsonpatchApply replays the RFC 6902 patch the webhook returned, which is
// what the API server does with it.
func jsonpatchApply(original, patch []byte) ([]byte, error) {
	decoded, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return nil, err
	}
	return decoded.Apply(original)
}

// Two databases archiving to one prefix interleave their WAL and leave the
// archive unrestorable, silently and permanently. Neither Cluster is invalid
// on its own, so only something that can see both can catch it. Measured on
// the test cluster on 2026-09-15, when a new canary and an existing Flux
// canary both wanted canary-postgres-pg.
func TestAClusterIsRefusedWhenAnotherArchivesThere(t *testing.T) {
	// Same object store and the same server name, in another namespace.
	other := cluster(t, func(object map[string]any) {
		metadata, _ := object["metadata"].(map[string]any)
		metadata["name"] = "app-pg"
		metadata["namespace"] = "other"
	})
	other.SetAPIVersion("postgresql.cnpg.io/v1")
	other.SetKind("Cluster")

	// The other namespace needs its own store and secret for the holder's
	// destination to resolve.
	otherStore := store()
	otherStore.SetNamespace("other")
	otherSecret := secret()
	otherSecret.Namespace = "other"

	raw, err := json.Marshal(cluster(t, nil))
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}
	builder := fake.NewClientBuilder().WithScheme(scheme(t)).
		WithObjects(secret(), otherSecret).
		WithRuntimeObjects(store(), otherStore, other)

	decider := &Decider{Client: builder.Build(), Prober: stubProber{has: false}}
	response := decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: raw},
		},
	})

	if response.Allowed {
		t.Fatal("a Cluster was admitted while another database archives to its prefix")
	}
	if !strings.Contains(response.Result.Message, "other/app-pg") {
		t.Errorf("the refusal does not name the holder: %q", response.Result.Message)
	}
}

// Recreating the same database is not a collision with the record of itself.
func TestARecreateOfTheSameClusterIsNotACollision(t *testing.T) {
	existing := cluster(t, nil)
	existing.SetAPIVersion("postgresql.cnpg.io/v1")
	existing.SetKind("Cluster")

	response := decide(t, cluster(t, nil), stubProber{has: true}, existing)

	if !response.Allowed {
		t.Fatalf("recreating a database was refused: %v", response.Result)
	}
	if len(response.Patches) == 0 {
		t.Error("the recreate was not restored from its own archive")
	}
}

func TestAnEmptyStoreLeavesTheClusterOnInitdb(t *testing.T) {
	response := decide(t, cluster(t, nil), stubProber{has: false})

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("the cluster was rewritten: %v", response.Patches)
	}
}

func TestAStoreWithABackupRecoversTheCluster(t *testing.T) {
	original := cluster(t, nil)
	response := decide(t, original, stubProber{has: true})

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}

	patched := applied(t, original, response)

	if _, found, _ := unstructured.NestedMap(patched, "spec", "bootstrap", "initdb"); found {
		t.Error("initdb survived the rewrite")
	}
	source, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "source")
	if source != RecoverySource {
		t.Errorf("recovery source = %q, want %q", source, RecoverySource)
	}

	external, _, _ := unstructured.NestedSlice(patched, "spec", "externalClusters")
	if len(external) != 1 {
		t.Fatalf("external clusters = %d, want 1", len(external))
	}
	entry, _ := external[0].(map[string]any)
	name, _, _ := unstructured.NestedString(entry, "name")
	if name != RecoverySource {
		t.Errorf("external cluster name = %q, want %q", name, RecoverySource)
	}
	server, _, _ := unstructured.NestedString(entry, "plugin", "parameters", "serverName")
	if server != "app-pg" {
		t.Errorf("serverName = %q, want the cluster's own name", server)
	}

	annotations, _, _ := unstructured.NestedStringMap(patched, "metadata", "annotations")
	if annotations[SkipCheckAnnotation] != "enabled" {
		t.Errorf("%s = %q, want enabled", SkipCheckAnnotation, annotations[SkipCheckAnnotation])
	}
}

// CloudNativePG defaults a recovery's database and owner to "app". A Cluster
// created with another database name would then come back with its data in
// that database and an empty "app" beside it, and the <cluster>-app Secret the
// workload reads would point at the empty one. Measured on the test cluster
// with v0.3.2, which dropped these fields.
func TestTheDatabaseAndOwnerSurviveTheRewrite(t *testing.T) {
	original := cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		spec["bootstrap"] = map[string]any{
			"initdb": map[string]any{
				"database": "canary",
				"owner":    "canary",
				"secret":   map[string]any{"name": "canary-credentials"},
			},
		}
	})

	response := decide(t, original, stubProber{has: true})
	patched := applied(t, original, response)

	database, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "database")
	if database != "canary" {
		t.Errorf("recovery database = %q, want canary", database)
	}
	owner, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "owner")
	if owner != "canary" {
		t.Errorf("recovery owner = %q, want canary", owner)
	}
	secretName, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "secret", "name")
	if secretName != "canary-credentials" {
		t.Errorf("recovery secret = %q, want canary-credentials", secretName)
	}
}

func TestTheOptOutAnnotationKeepsTheDatabaseEmpty(t *testing.T) {
	c := cluster(t, func(object map[string]any) {
		metadata, _ := object["metadata"].(map[string]any)
		metadata["annotations"] = map[string]any{OptOutAnnotation: OptOutValue}
	})

	response := decide(t, c, stubProber{has: true})

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("an opted-out cluster was rewritten: %v", response.Patches)
	}
}

// declaredRecovery is a Cluster written the way the terragrunt restore input
// and the Flux postgres-recovery component write one.
func declaredRecovery(t *testing.T) *unstructured.Unstructured {
	return cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		spec["bootstrap"] = map[string]any{
			"recovery": map[string]any{
				"source":         "objectstore",
				"recoveryTarget": map[string]any{"targetTime": "2026-09-15T07:33:00Z"},
			},
		}
	})
}

// waiting is a RestoreRun that deleted app-pg and waits for it to come back.
func waiting(asOf *string) *backupv1alpha1.RestoreRun {
	return &backupv1alpha1.RestoreRun{
		ObjectMeta: metav1.ObjectMeta{Name: "back-to-monday", Namespace: "app"},
		Spec:       backupv1alpha1.RestoreRunSpec{Database: "app-pg", RestoreAsOf: asOf},
		Status: backupv1alpha1.RestoreRunStatus{
			Phase: backupv1alpha1.RunPhaseWaiting,
			Items: []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: "app-pg", Phase: backupv1alpha1.ItemDeleted}},
		},
	}
}

var monday = time.Date(2026, 9, 21, 0, 0, 0, 0, time.UTC)

// A declared recovery archives like any other Cluster, so it collides like
// one: two databases writing one prefix interleave their WAL whatever either
// bootstrapped from.
func TestADeclaredRecoveryIsRefusedWhenAnotherArchivesThere(t *testing.T) {
	other := cluster(t, func(object map[string]any) {
		metadata, _ := object["metadata"].(map[string]any)
		metadata["name"] = "second-pg"
	})
	other.SetAPIVersion("postgresql.cnpg.io/v1")
	other.SetKind("Cluster")
	// second-pg archives to app-pg's prefix by naming it as its server.
	plugins, _, _ := unstructured.NestedSlice(other.Object, "spec", "plugins")
	plugin, _ := plugins[0].(map[string]any)
	plugin["parameters"] = map[string]any{"barmanObjectName": "app-pg-store", "serverName": "app-pg"}
	_ = unstructured.SetNestedSlice(other.Object, plugins, "spec", "plugins")

	response := decide(t, declaredRecovery(t), stubProber{has: true}, other)

	if response.Allowed {
		t.Fatal("a declared recovery was admitted onto a prefix another database archives to")
	}
	if !strings.Contains(response.Result.Message, "app/second-pg") {
		t.Errorf("the refusal does not name the holder: %q", response.Result.Message)
	}
}

func TestARunWaitingForTheClusterSetsItsTarget(t *testing.T) {
	asOf := "2026-09-22T00:00:00Z"
	original := cluster(t, nil)
	prober := stubProber{has: true, backups: []BaseBackup{{ID: "20260920T030000", End: monday.Add(-24 * time.Hour)}}}

	response := decide(t, original, prober, waiting(&asOf))
	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	patched := applied(t, original, response)

	target, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime")
	if target != asOf {
		t.Errorf("targetTime = %q, want the run's restoreAsOf %q", target, asOf)
	}
	annotations, _, _ := unstructured.NestedStringMap(patched, "metadata", "annotations")
	if annotations[backupv1alpha1.AnnotationRestoreRun] != "back-to-monday" {
		t.Errorf("%s = %q, want the run's name", backupv1alpha1.AnnotationRestoreRun, annotations[backupv1alpha1.AnnotationRestoreRun])
	}
}

// A run syncing the databases to the volumes recovers to the moment it found
// on the volumes' quiesced snapshots, which it records in status.syncedTo.
func TestASyncedRunRecoversToTheVolumesMoment(t *testing.T) {
	original := cluster(t, nil)
	prober := stubProber{has: true, backups: []BaseBackup{{ID: "20260920T030000", End: monday.Add(-24 * time.Hour)}}}
	run := waiting(nil)
	run.Spec.SyncDatabaseToVolume = true
	run.Status.SyncedTo = &metav1.Time{Time: time.Date(2026, 9, 21, 3, 0, 5, 0, time.UTC)}

	response := decide(t, original, prober, run)
	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	patched := applied(t, original, response)

	target, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime")
	if target != "2026-09-21T03:00:05Z" {
		t.Errorf("targetTime = %q, want the run's syncedTo 2026-09-21T03:00:05Z", target)
	}
}

func TestARunWithoutAMomentRecoversToTheEndOfTheArchive(t *testing.T) {
	original := cluster(t, nil)
	response := decide(t, original, stubProber{has: true}, waiting(nil))
	patched := applied(t, original, response)

	if _, found, _ := unstructured.NestedMap(patched, "spec", "bootstrap", "recovery", "recoveryTarget"); found {
		t.Error("a run without restoreAsOf set a recovery target")
	}
	annotations, _, _ := unstructured.NestedStringMap(patched, "metadata", "annotations")
	if annotations[backupv1alpha1.AnnotationRestoreRun] != "back-to-monday" {
		t.Error("the recovered Cluster does not name the run")
	}
}

func TestADeclaredRecoveryIsRefusedWhileARunWaits(t *testing.T) {
	response := decide(t, declaredRecovery(t), stubProber{has: true}, waiting(nil))

	if response.Allowed {
		t.Fatal("a declared recovery was admitted while a RestoreRun waits for the same Cluster")
	}
	if !strings.Contains(response.Result.Message, "back-to-monday") {
		t.Errorf("the refusal does not name the run: %q", response.Result.Message)
	}
}

func TestTheRestoreAsOfAnnotationSetsTheTarget(t *testing.T) {
	original := cluster(t, func(object map[string]any) {
		metadata, _ := object["metadata"].(map[string]any)
		metadata["annotations"] = map[string]any{backupv1alpha1.AnnotationRestoreAsOf: "2026-09-22T00:00:00Z"}
	})
	prober := stubProber{has: true, backups: []BaseBackup{{ID: "a", End: monday}}}

	response := decide(t, original, prober)
	patched := applied(t, original, response)

	target, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime")
	if target != "2026-09-22T00:00:00Z" {
		t.Errorf("targetTime = %q, want the annotation's time", target)
	}
}

// A target before the oldest base backup finished is one Postgres can never
// reach, so the Cluster is refused with the oldest backup named.
func TestATargetBeforeEveryBaseBackupIsRefused(t *testing.T) {
	asOf := "2026-09-01T00:00:00Z"
	prober := stubProber{has: true, backups: []BaseBackup{{ID: "20260920T030000", End: monday}}}

	response := decide(t, cluster(t, nil), prober, waiting(&asOf))

	if response.Allowed {
		t.Fatal("a target before every base backup was admitted")
	}
	if !strings.Contains(response.Result.Message, "20260920T030000") {
		t.Errorf("the refusal does not name the oldest backup: %q", response.Result.Message)
	}
}

// A run that asks for a recovery must not come back as an empty database.
func TestARunIsRefusedWhenTheStoreHoldsNoBaseBackup(t *testing.T) {
	response := decide(t, cluster(t, nil), stubProber{has: false}, waiting(nil))

	if response.Allowed {
		t.Fatal("a Cluster a RestoreRun waits for was admitted to initdb")
	}
}

func TestADeclaredRecoveryIsLeftAlone(t *testing.T) {
	c := cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		spec["bootstrap"] = map[string]any{
			"recovery": map[string]any{
				"source":         "objectstore",
				"recoveryTarget": map[string]any{"targetTime": "2026-09-15T07:33:00Z"},
			},
		}
	})

	response := decide(t, c, stubProber{has: true})

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("a point-in-time restore was rewritten: %v", response.Patches)
	}
}

func TestAClusterThatArchivesNowhereIsLeftAlone(t *testing.T) {
	c := cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		delete(spec, "plugins")
	})

	response := decide(t, c, stubProber{has: true})

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("a cluster with no archive was rewritten: %v", response.Patches)
	}
}

// An unreadable store is refused rather than allowed. Allowing would create an
// empty database beside a full archive and report success, which is the
// failure this webhook exists to remove.
func TestAnUnreadableStoreRefusesTheCluster(t *testing.T) {
	response := decide(t, cluster(t, nil), stubProber{err: errors.New("the endpoint refused the connection")})

	if response.Allowed {
		t.Fatal("a cluster was admitted while its store could not be read")
	}
}

// A dry-run request is allowed without reading anything, even where a real
// request on the same Cluster would be refused.
//
// Flux dry-runs every object in a Kustomization before it applies any of them,
// so on an app's first deploy this handler sees a Cluster whose ObjectStore is
// in the same set and does not exist yet. Refusing there fails the dry-run,
// Flux applies nothing, the ObjectStore is never created, and every later
// reconcile repeats it. Measured on 2026-09-15 as:
//
//	dry-run failed: admission webhook "bootstrap.backup.wlz.li" denied the
//	request: read ObjectStore app/app-pg-store:
//	objectstores.barmancloud.cnpg.io "app-pg-store" not found
func TestADryRunIsAllowedWithoutReadingTheStore(t *testing.T) {
	raw, err := json.Marshal(cluster(t, nil))
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}

	dryRun := true
	decider := &Decider{
		// No objects, so a read fails the way a missing ObjectStore does.
		Client: fake.NewClientBuilder().WithScheme(scheme(t)).Build(),
		Prober: stubProber{err: errors.New("the prober must not run on a dry run")},
	}

	response := decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: raw},
			DryRun:    &dryRun,
		},
	})

	if !response.Allowed {
		t.Fatalf("a dry run was refused: %s", response.Result.Message)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("a dry run returned %d patches, want none", len(response.Patches))
	}
}

// update sends an update of old to new, as a dry run when dryRun is set, with
// a client that holds nothing, so a handler that reads anything fails.
func update(t *testing.T, old, new *unstructured.Unstructured, dryRun bool) admission.Response {
	t.Helper()
	oldRaw, err := json.Marshal(old)
	if err != nil {
		t.Fatalf("marshal the old cluster: %v", err)
	}
	newRaw, err := json.Marshal(new)
	if err != nil {
		t.Fatalf("marshal the new cluster: %v", err)
	}
	decider := &Decider{
		Client: fake.NewClientBuilder().WithScheme(scheme(t)).Build(),
		Prober: stubProber{err: errors.New("the prober must not run on an update")},
	}
	return decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Update,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: newRaw},
			OldObject: runtime.RawExtension{Raw: oldRaw},
			DryRun:    &dryRun,
		},
	})
}

// recovered is a Cluster as this webhook left it after a restore.
func recovered(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	return cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		spec["bootstrap"] = map[string]any{
			"recovery": map[string]any{"source": RecoverySource, "database": "app", "owner": "app"},
		}
	})
}

// Flux applies the Cluster from git on every reconcile, and git still holds
// initdb. Server-side apply keeps the recovery the webhook wrote and adds initdb
// back, and CloudNativePG refuses the result. Measured on 2026-09-17 as:
//
//	dry-run failed (Invalid): admission webhook "vcluster.cnpg.io" denied the
//	request: Cluster.cluster.cnpg.io "canary-backup-aio-flux-pg" is invalid:
//	spec.bootstrap: Forbidden: Only one bootstrap method can be specified at a time
func TestAnUpdateDropsInitdbFromARecoveredCluster(t *testing.T) {
	for _, dryRun := range []bool{true, false} {
		merged := recovered(t)
		_ = unstructured.SetNestedMap(merged.Object, map[string]any{"database": "app", "owner": "app"}, "spec", "bootstrap", "initdb")

		response := update(t, recovered(t), merged, dryRun)

		if !response.Allowed {
			t.Fatalf("dry run %v: the update was refused: %v", dryRun, response.Result)
		}
		patched := applied(t, merged, response)
		if _, found, _ := unstructured.NestedMap(patched, "spec", "bootstrap", "initdb"); found {
			t.Errorf("dry run %v: initdb survived the update", dryRun)
		}
		source, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "source")
		if source != RecoverySource {
			t.Errorf("dry run %v: recovery source = %q, want %q", dryRun, source, RecoverySource)
		}
	}
}

// A Cluster this webhook did not recover keeps whatever its update says.
func TestAnUpdateOfAnInitdbClusterIsLeftAlone(t *testing.T) {
	response := update(t, cluster(t, nil), cluster(t, nil), true)

	if !response.Allowed {
		t.Fatalf("the update was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("an initdb cluster was rewritten: %v", response.Patches)
	}
}

// A point-in-time restore someone wrote by hand is theirs to get right.
func TestAnUpdateOfADeclaredRecoveryIsLeftAlone(t *testing.T) {
	declared := func() *unstructured.Unstructured {
		return cluster(t, func(object map[string]any) {
			spec, _ := object["spec"].(map[string]any)
			spec["bootstrap"] = map[string]any{
				"initdb":   map[string]any{"database": "app"},
				"recovery": map[string]any{"source": "objectstore"},
			}
		})
	}

	response := update(t, declared(), declared(), true)

	if len(response.Patches) != 0 {
		t.Fatalf("a declared recovery was rewritten: %v", response.Patches)
	}
}

func TestTheServerNameParameterWinsOverTheClusterName(t *testing.T) {
	original := cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		plugins, _ := spec["plugins"].([]any)
		plugin, _ := plugins[0].(map[string]any)
		parameters, _ := plugin["parameters"].(map[string]any)
		parameters["serverName"] = "app-pg-original"
	})

	response := decide(t, original, stubProber{has: true})
	patched := applied(t, original, response)

	external, _, _ := unstructured.NestedSlice(patched, "spec", "externalClusters")
	entry, _ := external[0].(map[string]any)
	server, _, _ := unstructured.NestedString(entry, "plugin", "parameters", "serverName")
	if server != "app-pg-original" {
		t.Errorf("serverName = %q, want the plugin's own parameter", server)
	}
}

func TestBasePrefixIsTheServersBackupDirectory(t *testing.T) {
	at := Location{Bucket: "backups", Prefix: "app/app-pg"}
	if got, want := at.BasePrefix(), "app/app-pg/base/"; got != want {
		t.Errorf("BasePrefix() = %q, want %q", got, want)
	}
}

func TestSplitDestinationSeparatesBucketFromPrefix(t *testing.T) {
	for _, tc := range []struct {
		in     string
		bucket string
		prefix string
	}{
		{"s3://backups/", "backups", ""},
		{"s3://backups/app/", "backups", "app"},
		{"s3://backups/one/two/", "backups", "one/two"},
	} {
		bucket, prefix, err := splitDestination(tc.in)
		if err != nil {
			t.Fatalf("splitDestination(%q): %v", tc.in, err)
		}
		if bucket != tc.bucket || prefix != tc.prefix {
			t.Errorf("splitDestination(%q) = %q, %q; want %q, %q", tc.in, bucket, prefix, tc.bucket, tc.prefix)
		}
	}
}

// A store fronted by a public authority declares no endpointCA, and the client
// verifies against the roots the image ships.
func TestNoEndpointCAMeansThePublicRoots(t *testing.T) {
	transport, err := tlsTransport(nil)
	if err != nil {
		t.Fatalf("tlsTransport(nil): %v", err)
	}
	if transport.TLSClientConfig.RootCAs == nil {
		t.Error("no root pool was built")
	}
	if transport.TLSClientConfig.MinVersion != tls.VersionTLS12 {
		t.Error("the transport accepts TLS below 1.2")
	}
}

// A store fronted by a private authority carries the bundle that signs it, and
// nothing about that authority is configured on this controller.
func TestAnEndpointCAIsAddedToTheRoots(t *testing.T) {
	// A throwaway self-signed certificate, generated in this test so the
	// repository carries no certificate material of its own.
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate a key: %v", err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "store.example"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create a certificate: %v", err)
	}
	bundle := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})

	transport, err := tlsTransport(bundle)
	if err != nil {
		t.Fatalf("tlsTransport(bundle): %v", err)
	}

	subjects := transport.TLSClientConfig.RootCAs.Subjects() //nolint:staticcheck // reading the pool is the assertion
	if len(subjects) == 0 {
		t.Fatal("the bundle was not added to the pool")
	}
}

func TestAnUnparsableEndpointCAIsRejected(t *testing.T) {
	if _, err := tlsTransport([]byte("this is not a certificate")); err == nil {
		t.Fatal("a bundle holding no PEM certificate was accepted")
	}
}

func TestSplitDestinationRejectsAnotherProvider(t *testing.T) {
	if _, _, err := splitDestination("azure://container/"); err == nil {
		t.Fatal("an azure:// destination was accepted")
	}
}
