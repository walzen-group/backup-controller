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
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// stubProber is a Prober that returns fixed answers, so the handler can be
// tested without an object store. Every method also returns the err field.
type stubProber struct {
	has     bool
	err     error
	backups []BaseBackup
}

func (s stubProber) ArchiveEmpty(context.Context, Location) (bool, error) {
	return s.has, s.err
}

func (s stubProber) BaseBackups(context.Context, Location) ([]BaseBackup, error) {
	return s.backups, s.err
}

func (s stubProber) WALSince(context.Context, Location, time.Time) (bool, error) {
	return false, s.err
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
	s.AddKnownTypeWithName(ClusterListGVK, &unstructured.UnstructuredList{})
	s.AddKnownTypeWithName(ClusterListGVK.GroupVersion().WithKind("Cluster"), &unstructured.Unstructured{})
	return s
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

// update sends a Decider an update of the Cluster app/app-pg from the old
// object to the new one, as a dry run when dryRun is true. The Decider's
// client holds nothing and its Prober always fails, so the request fails if
// the handler reads anything.
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

// TestAnUpdateOfADeclaredRecoveryIsLeftAlone checks that an update of a
// Cluster whose recovery someone wrote by hand gets no patch, even when it
// carries initdb too. A hand-written point-in-time restore is theirs to get
// right.
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

// TestTheServerNameParameterWinsOverTheClusterName checks that the recovery
// reads from the plugin's serverName parameter when the Cluster sets one.
func TestTheServerNameParameterWinsOverTheClusterName(t *testing.T) {
	original := cluster(t, func(object map[string]any) {
		spec, _ := object["spec"].(map[string]any)
		plugins, _ := spec["plugins"].([]any)
		plugin, _ := plugins[0].(map[string]any)
		parameters, _ := plugin["parameters"].(map[string]any)
		parameters["serverName"] = "app-pg-original"
	})

	store, serverName, found := Archiver(original)
	if !found {
		t.Fatal("Archiver found no archiving plugin")
	}
	if err := setRecovery(original, store, serverName, recoveryTarget{}); err != nil {
		t.Fatal(err)
	}

	external, _, _ := unstructured.NestedSlice(original.Object, "spec", "externalClusters")
	entry, _ := external[0].(map[string]any)
	server, _, _ := unstructured.NestedString(entry, "plugin", "parameters", "serverName")
	if server != "app-pg-original" {
		t.Errorf("serverName = %q, want the plugin's own parameter", server)
	}
}

// TestBasePrefixIsTheServersBackupDirectory checks that BasePrefix appends
// "/base/" to the location's prefix.
func TestBasePrefixIsTheServersBackupDirectory(t *testing.T) {
	at := Location{Bucket: "backups", Prefix: "app/app-pg"}
	if got, want := at.BasePrefix(), "app/app-pg/base/"; got != want {
		t.Errorf("BasePrefix() = %q, want %q", got, want)
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

// TestNoEndpointCAMeansThePublicRoots checks that tlsTransport with no bundle
// still builds a root pool and requires TLS 1.2. A store fronted by a public
// authority declares no endpointCA, and the client verifies against the roots
// the image ships.
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

// TestAnEndpointCAIsAddedToTheRoots checks that a PEM bundle passed to
// tlsTransport ends up in the root pool. A store fronted by a private
// authority carries the bundle that signs it, and this controller has no CA
// settings of its own.
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

// TestAnUnparsableEndpointCAIsRejected checks that tlsTransport returns an
// error for a bundle holding no PEM certificate.
func TestAnUnparsableEndpointCAIsRejected(t *testing.T) {
	if _, err := tlsTransport([]byte("this is not a certificate")); err == nil {
		t.Fatal("a bundle holding no PEM certificate was accepted")
	}
}

// TestSplitDestinationRejectsAnotherProvider checks that splitDestination
// returns an error for a destination with a scheme other than s3://.
func TestSplitDestinationRejectsAnotherProvider(t *testing.T) {
	if _, _, err := splitDestination("azure://container/"); err == nil {
		t.Fatal("an azure:// destination was accepted")
	}
}
