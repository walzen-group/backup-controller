package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	jsonpatch "github.com/evanphx/json-patch/v5"
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
	has bool
	err error
}

func (s stubProber) HasBaseBackup(context.Context, Location) (bool, error) {
	return s.has, s.err
}

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("register the core types: %v", err)
	}
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

func decide(t *testing.T, c *unstructured.Unstructured, prober Prober) admission.Response {
	t.Helper()
	raw, err := json.Marshal(c)
	if err != nil {
		t.Fatalf("marshal the cluster: %v", err)
	}

	builder := fake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(secret())
	builder = builder.WithRuntimeObjects(store())

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

func TestSplitDestinationRejectsAnotherProvider(t *testing.T) {
	if _, _, err := splitDestination("azure://container/"); err == nil {
		t.Fatal("an azure:// destination was accepted")
	}
}
