package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/barmanstore"
	"github.com/walzen-group/backup-controller/internal/testinfra/s3fault"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// stubProber is an ArchiveProber that returns fixed answers, so the handler can be
// tested without an object store, for the cases whose outcome doesn't depend
// on what the store holds. BaseBackups lists no backup. Every method also
// returns the err field.
type stubProber struct {
	has bool
	err error
}

func (s stubProber) BaseBackups(context.Context, Location) ([]BaseBackup, error) {
	return nil, s.err
}

// Survey answers from the has field: a stub store with a completed base
// backup holds one base backup directory, and one without is an empty prefix.
// Like BaseBackups, it lists no backup finished by any target.
func (s stubProber) Survey(_ context.Context, _ Location, target *time.Time) (Archive, error) {
	if !s.has {
		return Archive{Empty: true}, s.err
	}
	archive := Archive{Backups: 1, Read: 1}
	if target == nil {
		archive.Found = &BaseBackup{ID: "stub"}
	}
	return archive, s.err
}

// scheme registers the core and backup types, plus ObjectStore and Cluster as
// unstructured kinds, for the fake client.
func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("register the core types: %v", err)
	}
	if err := backupv1alpha1.AddToScheme(s); err != nil {
		t.Fatalf("register the backup types: %v", err)
	}
	// The handler reads both kinds as unstructured, so the fake client knows
	// them by GVK. There are no Go types for them.
	s.AddKnownTypeWithName(ObjectStoreGVK, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(ObjectStoreListGVK, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(ClusterListGVK, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(ClusterListGVK.GroupVersion().WithKind("Cluster"), &unstructured.Unstructured{})
	return s
}

// newBuilder returns a fake client builder with scheme(t) and a RESTMapper
// that serves every kind of that scheme at the version registered there, as
// the API server's discovery would. The handler looks the versions of
// Cluster and ObjectStore up in it (see served.Kind).
func newBuilder(t *testing.T) *fake.ClientBuilder {
	t.Helper()
	s := scheme(t)
	mapper := meta.NewDefaultRESTMapper(s.PrioritizedVersionsAllGroups())
	for gvk := range s.AllKnownTypes() {
		if !strings.HasSuffix(gvk.Kind, "List") {
			mapper.Add(gvk, meta.RESTScopeNamespace)
		}
	}
	return fake.NewClientBuilder().WithScheme(s).WithRESTMapper(mapper)
}

// cluster builds the Cluster app/app-pg, bootstrapped with initdb and archiving
// through the plugin to the ObjectStore app-pg-store. Every case here starts
// from it. The mutate function, when given, edits the object before it is
// returned.
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

// store builds the ObjectStore app/app-pg-store, which archives to
// s3://backups/app/ and takes its credentials from the Secret that secret()
// builds. ResolveLocation reads both.
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

// secret builds the Secret app/app-pg-backup, which holds the credentials that
// the ObjectStore from store() names.
func secret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: "app-pg-backup", Namespace: "app"},
		Data: map[string][]byte{
			"ACCESS_KEY_ID":     []byte("key"),
			"ACCESS_SECRET_KEY": []byte("secret"),
		},
	}
}

// decide sends a create of the Cluster c in namespace app to a Decider with
// the given Prober. Its fake client holds store(), secret() and any extra
// objects passed in existing.
func decide(t *testing.T, c *unstructured.Unstructured, prober ArchiveProber, existing ...runtime.Object) admission.Response {
	t.Helper()
	return decideWith(t, c, prober, store(), existing...)
}

// decideWith is decide with the Cluster's ObjectStore passed in, for the cases
// that point the store at another endpoint, such as a fake S3 server.
func decideWith(t *testing.T, c *unstructured.Unstructured, prober ArchiveProber, objectStore *unstructured.Unstructured, existing ...runtime.Object) admission.Response {
	t.Helper()

	builder := newBuilder(t).WithObjects(secret())
	objects := append([]runtime.Object{objectStore}, existing...)
	builder = builder.WithRuntimeObjects(objects...)

	decider := &Decider{Client: builder.Build(), Prober: prober}
	return create(t, decider, c)
}

// create sends decider a create of the Cluster c as app/app-pg, and returns
// the answer.
func create(t *testing.T, decider *Decider, c *unstructured.Unstructured) admission.Response {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}
	return decider.Handle(context.Background(), admission.Request{
		AdmissionRequest: admissionv1.AdmissionRequest{
			Operation: admissionv1.Create,
			Namespace: "app",
			Name:      "app-pg",
			Object:    runtime.RawExtension{Raw: raw},
		},
	})
}

// applied applies the response's patches to the original Cluster and returns
// the result, which is the Cluster the API server would have stored.
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

// jsonpatchApply applies an RFC 6902 JSON patch to a JSON document, the way
// the API server applies the patch a webhook returns.
func jsonpatchApply(original, patch []byte) ([]byte, error) {
	decoded, err := jsonpatch.DecodePatch(patch)
	if err != nil {
		return nil, err
	}
	return decoded.Apply(original)
}

// elsewhere builds the Cluster other/app-pg, which archives through the
// ObjectStore other/app-pg-store to app-pg's own prefix, and that store. The
// mutate function, when given, edits the store's spec.configuration.
func elsewhere(t *testing.T, mutate func(map[string]any)) (*unstructured.Unstructured, *unstructured.Unstructured) {
	t.Helper()
	other := cluster(t, func(object map[string]any) {
		metadata, _ := object["metadata"].(map[string]any)
		metadata["namespace"] = "other"
	})
	other.SetAPIVersion("postgresql.cnpg.io/v1")
	other.SetKind("Cluster")
	otherStore := store()
	otherStore.SetNamespace("other")
	if mutate != nil {
		configuration, _, _ := unstructured.NestedMap(otherStore.Object, "spec", "configuration")
		mutate(configuration)
		_ = unstructured.SetNestedMap(otherStore.Object, configuration, "spec", "configuration")
	}
	return other, otherStore
}

// TestAHolderWithoutItsSecretStillCollides checks that a Cluster archiving to
// the same prefix is found even when the Secret its ObjectStore names is
// missing. Where a Cluster archives is written in its ObjectStore, and a
// missing credential does not move it.
func TestAHolderWithoutItsSecretStillCollides(t *testing.T) {
	other, otherStore := elsewhere(t, nil)

	response := decide(t, cluster(t, nil), stubProber{has: false}, other, otherStore)

	if response.Allowed {
		t.Fatal("a Cluster was admitted while another database archives to its prefix")
	}
	if !strings.Contains(response.Result.Message, "other/app-pg") {
		t.Errorf("the refusal does not name the holder: %q", response.Result.Message)
	}
}

// TestTheSamePrefixOnAnotherEndpointIsACollision checks that a Cluster is
// refused when another Cluster uses the same bucket and prefix behind a
// different endpointURL, and that the refusal tells the owner to give one of
// the two its own prefix.
//
// No comparison of two endpoint names can prove they are different services:
// an Ingress host and a Service name, an IP and a DNS name, or a CNAME can all
// front one store. When they do, barman overwrites WAL segments by name and
// the two databases' WAL interleave in one prefix, silently leaving every base
// backup behind them unrecoverable. The check refuses instead, and an owner
// with two genuinely separate services moves one of them to another prefix.
func TestTheSamePrefixOnAnotherEndpointIsACollision(t *testing.T) {
	other, otherStore := elsewhere(t, func(configuration map[string]any) {
		configuration["endpointURL"] = "https://another-store.example"
	})

	otherSecret := secret()
	otherSecret.Namespace = "other"

	response := decide(t, cluster(t, nil), stubProber{has: false}, other, otherStore, otherSecret)

	if response.Allowed {
		t.Fatal("a Cluster was admitted while another database archives to its bucket and prefix behind another endpointURL")
	}
	for _, want := range []string{"other/app-pg", "destinationPath", "whatever the endpointURL says"} {
		if !strings.Contains(response.Result.Message, want) {
			t.Errorf("the refusal does not say %q: %q", want, response.Result.Message)
		}
	}
}

// TestABucketNameInAnotherCaseIsACollision checks that bucket names are
// compared without letter case. S3 and MinIO bucket names are lower case, so
// Backups and backups can only name one bucket.
func TestABucketNameInAnotherCaseIsACollision(t *testing.T) {
	other, otherStore := elsewhere(t, func(configuration map[string]any) {
		configuration["destinationPath"] = "s3://Backups/app/"
	})

	response := decide(t, cluster(t, nil), stubProber{has: false}, other, otherStore)

	if response.Allowed {
		t.Fatal("a Cluster was admitted while another database archives to its prefix in a bucket spelled Backups")
	}
}

// TestAnotherPrefixOnTheSameEndpointIsNoCollision checks that a Cluster is
// admitted when another Cluster archives to a different prefix of the same
// bucket on the same endpoint.
func TestAnotherPrefixOnTheSameEndpointIsNoCollision(t *testing.T) {
	other, otherStore := elsewhere(t, func(configuration map[string]any) {
		configuration["destinationPath"] = "s3://backups/other/"
	})

	response := decide(t, cluster(t, nil), stubProber{has: false}, other, otherStore)

	if !response.Allowed {
		t.Fatalf("a Cluster was refused over an archive at another prefix: %v", response.Result)
	}
}

// TestAnUnreadableHolderStoreRefusesTheCluster checks that a Cluster is
// refused with HTTP 500 when the other Clusters' ObjectStores can't be listed,
// here because of a server timeout. That Cluster may archive to the same
// prefix, and admitting on a transient API error would let the collision
// through.
func TestAnUnreadableHolderStoreRefusesTheCluster(t *testing.T) {
	other, otherStore := elsewhere(t, nil)

	c := newBuilder(t).
		WithObjects(secret()).
		WithRuntimeObjects(store(), otherStore, other).
		WithInterceptorFuncs(interceptor.Funcs{
			List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
				if list.GetObjectKind().GroupVersionKind().Kind == ObjectStoreListGVK.Kind {
					return apierrors.NewServerTimeout(schema.GroupResource{Group: ObjectStoreGVK.Group, Resource: "objectstores"}, "list", 1)
				}
				return c.List(ctx, list, opts...)
			},
		}).Build()

	decider := &Decider{Client: c, Prober: stubProber{has: false}}
	response := create(t, decider, cluster(t, nil))

	if response.Allowed {
		t.Fatal("a Cluster was admitted while another Cluster's ObjectStore could not be read")
	}
	if response.Result.Code != http.StatusInternalServerError {
		t.Errorf("code = %d, want %d", response.Result.Code, http.StatusInternalServerError)
	}
}

// TestARecreateOfTheSameClusterIsNotACollision checks that a Cluster whose own
// namespace and name are still in the Cluster list is admitted and recovered
// from its own archive. The collision check skips the Cluster's own record.
func TestARecreateOfTheSameClusterIsNotACollision(t *testing.T) {
	existing := cluster(t, nil)
	existing.SetAPIVersion("postgresql.cnpg.io/v1")
	existing.SetKind("Cluster")

	response := decideOn(t, cluster(t, nil), recordedS3(t, barmanstore.MustLoad(t, "done-base")), existing)

	if !response.Allowed {
		t.Fatalf("recreating a database was refused: %v", response.Result)
	}
	if len(response.Patches) == 0 {
		t.Error("the recreate was not restored from its own archive")
	}
}

// TestAnEmptyStoreLeavesTheClusterOnInitdb checks that a Cluster whose store
// holds no base backup is admitted without a patch, through the real S3Prober
// against the empty store that barman-cloud 3.20.0 recorded.
func TestAnEmptyStoreLeavesTheClusterOnInitdb(t *testing.T) {
	response := decideOn(t, cluster(t, nil), recordedS3(t, barmanstore.MustLoad(t, "empty")))

	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("the cluster was rewritten: %v", response.Patches)
	}
}

// TestAStoreWithABackupRecoversTheCluster checks the rewrite of a Cluster
// whose store holds a base backup: initdb is gone, the recovery reads through
// one externalClusters entry named RecoverySource with the Cluster's own name
// as serverName, and SkipCheckAnnotation is "enabled". The store is the
// recorded done-base store, read through the real S3Prober.
func TestAStoreWithABackupRecoversTheCluster(t *testing.T) {
	original := cluster(t, nil)
	response := decideOn(t, original, recordedS3(t, barmanstore.MustLoad(t, "done-base")))

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

// TestTheDatabaseAndOwnerSurviveTheRewrite checks that initdb's database,
// owner and secret are copied into the recovery.
//
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

	response := decideOn(t, original, recordedS3(t, barmanstore.MustLoad(t, "done-base")))
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

// optedOut builds the Cluster from cluster() carrying OptOutAnnotation set to
// OptOutValue.
func optedOut(t *testing.T) *unstructured.Unstructured {
	return cluster(t, func(object map[string]any) {
		metadata, _ := object["metadata"].(map[string]any)
		metadata["annotations"] = map[string]any{OptOutAnnotation: OptOutValue}
	})
}

// TestTheOptOutAnnotationIsRefusedOverAnOldArchive checks that an opted-out
// Cluster is refused, with the way out, when its prefix still holds the
// archive of an earlier database, here the recorded done-base store. Finding
// W1, designs/webhook.md A: CloudNativePG would never archive the new
// database into a prefix holding WAL.
func TestTheOptOutAnnotationIsRefusedOverAnOldArchive(t *testing.T) {
	response := decideOn(t, optedOut(t), recordedS3(t, barmanstore.MustLoad(t, "done-base")))

	if response.Allowed {
		t.Fatalf("an opted-out cluster was admitted over an old archive (patches %v)", response.Patches)
	}
	message := response.Result.Message
	for _, want := range []string{OptOutAnnotation + ": " + OptOutValue, "s3://backups/app/app-pg/", "delete everything under s3://backups/app/app-pg/", `serverName that is not "app-pg"`} {
		if !strings.Contains(message, want) {
			t.Errorf("the refusal %q does not say %q", message, want)
		}
	}
}

// declaredRecovery builds a Cluster that declares its own point-in-time
// recovery, the way the terragrunt restore input and the Flux
// postgres-recovery component write one.
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

// waiting builds the RestoreRun app/back-to-monday, which has deleted app-pg
// and waits for it to be created again. The asOf argument becomes the run's
// spec.restoreAsOf, and nil leaves it unset.
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

// TestARunWaitingForTheClusterSetsItsTarget checks that a RestoreRun waiting
// for the Cluster sets the recovery target to its restoreAsOf, and that the
// Cluster is annotated with the run's name. The store is done-base, shifted
// so its completed backup starts at sunday.
func TestARunWaitingForTheClusterSetsItsTarget(t *testing.T) {
	asOf := "2026-09-22T00:00:00Z"
	original := cluster(t, nil)
	server := recordedS3(t, recordedAt(t, "done-base", sunday))

	response := decideOn(t, original, server, waiting(&asOf))
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

// TestASyncedRunRecoversToTheVolumesMoment checks that a RestoreRun with
// syncDatabaseToVolume recovers the database to its status.syncedTo. That is
// the moment the run found on the volumes' quiesced snapshots. The store is
// done-base, shifted so its completed backup starts at sunday.
func TestASyncedRunRecoversToTheVolumesMoment(t *testing.T) {
	original := cluster(t, nil)
	server := recordedS3(t, recordedAt(t, "done-base", sunday))
	run := waiting(nil)
	run.Spec.SyncDatabaseToVolume = true
	run.Status.SyncedTo = &metav1.Time{Time: time.Date(2026, 9, 21, 3, 0, 5, 0, time.UTC)}

	response := decideOn(t, original, server, run)
	if !response.Allowed {
		t.Fatalf("the cluster was refused: %v", response.Result)
	}
	patched := applied(t, original, response)

	target, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime")
	if target != "2026-09-21T03:00:05Z" {
		t.Errorf("targetTime = %q, want the run's syncedTo 2026-09-21T03:00:05Z", target)
	}
}

// TestARunWithoutAMomentRecoversToTheEndOfTheArchive checks that a RestoreRun
// with no restoreAsOf gets a recovery with no recoveryTarget, and that the
// Cluster is still annotated with the run's name.
func TestARunWithoutAMomentRecoversToTheEndOfTheArchive(t *testing.T) {
	original := cluster(t, nil)
	response := decideOn(t, original, recordedS3(t, barmanstore.MustLoad(t, "done-base")), waiting(nil))
	patched := applied(t, original, response)

	if _, found, _ := unstructured.NestedMap(patched, "spec", "bootstrap", "recovery", "recoveryTarget"); found {
		t.Error("a run without restoreAsOf set a recovery target")
	}
	annotations, _, _ := unstructured.NestedStringMap(patched, "metadata", "annotations")
	if annotations[backupv1alpha1.AnnotationRestoreRun] != "back-to-monday" {
		t.Error("the recovered Cluster does not name the run")
	}
}

// TestADeclaredRecoveryIsRefusedWhileARunWaits checks that a Cluster declaring
// its own recovery is refused while a RestoreRun waits for it, and that the
// refusal names the run.
func TestADeclaredRecoveryIsRefusedWhileARunWaits(t *testing.T) {
	response := decide(t, declaredRecovery(t), stubProber{has: true}, waiting(nil))

	if response.Allowed {
		t.Fatal("a declared recovery was admitted while a RestoreRun waits for the same Cluster")
	}
	if !strings.Contains(response.Result.Message, "back-to-monday") {
		t.Errorf("the refusal does not name the run: %q", response.Result.Message)
	}
}

// TestTheRestoreAsOfAnnotationSetsTheTarget checks that, with no RestoreRun
// waiting, the Cluster's backup.wlz.li/restore-as-of annotation sets the
// recovery target. The store is done-base, shifted so its completed backup
// starts at sunday.
func TestTheRestoreAsOfAnnotationSetsTheTarget(t *testing.T) {
	original := cluster(t, func(object map[string]any) {
		metadata, _ := object["metadata"].(map[string]any)
		metadata["annotations"] = map[string]any{backupv1alpha1.AnnotationRestoreAsOf: "2026-09-22T00:00:00Z"}
	})
	server := recordedS3(t, recordedAt(t, "done-base", sunday))

	response := decideOn(t, original, server)
	patched := applied(t, original, response)

	target, _, _ := unstructured.NestedString(patched, "spec", "bootstrap", "recovery", "recoveryTarget", "targetTime")
	if target != "2026-09-22T00:00:00Z" {
		t.Errorf("targetTime = %q, want the annotation's time", target)
	}
}

// TestATargetBeforeEveryBaseBackupIsRefused checks that a target before the
// oldest base backup finished is refused, and that the refusal names that
// backup. Postgres can never reach such a target.
//
// The store is many-failed, as barman-cloud 3.20.0 recorded it: 160 failed
// base backups and then one completed one, shifted so the completed one
// starts at sunday. The refusal must name the completed backup, since a
// failed one is no base backup to recover from.
func TestATargetBeforeEveryBaseBackupIsRefused(t *testing.T) {
	asOf := "2026-09-01T00:00:00Z"
	recorded := recordedAt(t, "many-failed", sunday)
	oldestFailed := recorded.Backups()["app-pg"][0]

	response := decideOn(t, cluster(t, nil), recordedS3(t, recorded), waiting(&asOf))

	if response.Allowed {
		t.Fatal("a target before every base backup was admitted")
	}
	if !strings.Contains(response.Result.Message, sundayID) {
		t.Errorf("the refusal does not name the oldest completed backup %s: %q", sundayID, response.Result.Message)
	}
	if strings.Contains(response.Result.Message, oldestFailed) {
		t.Errorf("the refusal names the failed backup %s: %q", oldestFailed, response.Result.Message)
	}
}

// TestARunIsRefusedWhenTheStoreHoldsNoBaseBackup checks that a Cluster a
// RestoreRun waits for is refused when its store holds no base backup. A run
// that asks for a recovery must never get an empty database back. The store
// is the recorded empty store.
func TestARunIsRefusedWhenTheStoreHoldsNoBaseBackup(t *testing.T) {
	response := decideOn(t, cluster(t, nil), recordedS3(t, barmanstore.MustLoad(t, "empty")), waiting(nil))

	if response.Allowed {
		t.Fatal("a Cluster a RestoreRun waits for was admitted to initdb")
	}
}

// TestADeclaredRecoveryIsLeftAlone checks that a Cluster declaring its own
// point-in-time recovery is admitted without a patch when no RestoreRun waits
// for it.
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

// TestAClusterThatArchivesNowhereIsLeftAlone checks that a Cluster with no
// plugins is admitted without a patch.
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

// TestAnUnreadableStoreRefusesTheCluster checks that a Cluster is refused when
// the object store can't be listed. Allowing it would create an empty database
// beside a full archive and report success, which is the failure this webhook
// exists to remove.
//
// The store is the recorded done-base store behind an s3fault proxy that
// answers every request with 403 AccessDenied, which minio-go does not retry.
// The refusal has to name the 403 and AccessDenied, and the same store
// without the rule has to recover the Cluster, so the refusal is known to
// come from the 403.
func TestAnUnreadableStoreRefusesTheCluster(t *testing.T) {
	endpoint, proxy := faultyS3(t, recordedS3(t, barmanstore.MustLoad(t, "done-base")))
	unavailable := proxy.Add(s3fault.Rule{Status: http.StatusForbidden, Code: "AccessDenied"})

	response := decideWith(t, cluster(t, nil), S3Prober{}, storeAt(endpoint))

	if response.Allowed {
		t.Fatal("a cluster was admitted while its store could not be read")
	}
	if unavailable.Hits() == 0 {
		t.Error("the store was refused without a request reaching the 403 rule")
	}
	for _, want := range []string{"403", "AccessDenied"} {
		if !strings.Contains(response.Result.Message, want) {
			t.Errorf("the refusal %q does not name the store's answer %q", response.Result.Message, want)
		}
	}

	// The control: the same store behind the same proxy, without the rule,
	// recovers the Cluster, so the refusal above came from the 403.
	unavailable.Remove()
	response = decideWith(t, cluster(t, nil), S3Prober{}, storeAt(endpoint))
	if !response.Allowed || len(response.Patches) == 0 {
		t.Fatalf("the cluster was not recovered once the store answered: %v", response.Result)
	}
}

// TestADryRunIsAllowedWithoutReadingTheStore checks that a dry-run create is
// allowed without a patch and without reading anything, even where a real
// request for the same Cluster would be refused.
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
		Client: newBuilder(t).Build(),
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

// update sends a Decider an update of the Cluster app/app-pg from the old
// object to the updated one, as a dry run when dryRun is true. The Decider's
// client holds nothing and its Prober always fails, so the request fails if
// the handler reads anything.
func update(t *testing.T, old, updated *unstructured.Unstructured, dryRun bool) admission.Response {
	t.Helper()
	oldRaw, err := json.Marshal(old)
	if err != nil {
		t.Fatalf("marshal the old cluster: %v", err)
	}
	newRaw, err := json.Marshal(updated)
	if err != nil {
		t.Fatalf("marshal the new cluster: %v", err)
	}
	decider := &Decider{
		Client: newBuilder(t).Build(),
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

// recovered builds a Cluster the way this webhook leaves it after a restore,
// with a recovery whose source is RecoverySource.
func recovered(t *testing.T) *unstructured.Unstructured {
	t.Helper()
	return cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		spec["bootstrap"] = map[string]any{
			"recovery": map[string]any{"source": RecoverySource, "database": "app", "owner": "app"},
		}
	})
}

// TestAnUpdateDropsInitdbFromARecoveredCluster checks that an update adding
// initdb back to a Cluster this webhook recovered gets a patch that removes
// initdb and keeps the recovery, on a dry run and on a real request.
//
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

// TestAnUpdateOfAnInitdbClusterIsLeftAlone checks that an update of a Cluster
// this webhook didn't recover is allowed without a patch.
func TestAnUpdateOfAnInitdbClusterIsLeftAlone(t *testing.T) {
	response := update(t, cluster(t, nil), cluster(t, nil), true)

	if !response.Allowed {
		t.Fatalf("the update was refused: %v", response.Result)
	}
	if len(response.Patches) != 0 {
		t.Fatalf("an initdb cluster was rewritten: %v", response.Patches)
	}
}

// TestSplitDestinationSeparatesBucketFromPrefix checks that splitDestination
// returns the bucket and a prefix with its slashes trimmed, for an empty,
// one-level and two-level prefix.
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
