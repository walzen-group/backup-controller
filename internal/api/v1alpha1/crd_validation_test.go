//go:build envtest

// The validation rules listed in docs/api.md live in the generated CRD: the
// required repository, the RFC 3339 restoreAsOf and the label syntax rules.
// The Go types enforce none of them, and only a real API server shows whether
// they reject what they should. This suite starts an envtest control plane,
// installs config/crd, creates objects and reads the server's answer.
//
// Run it with make envtest. The flake's envtest shell sets KUBEBUILDER_ASSETS
// to a kube-apiserver and an etcd at production's Kubernetes version, taken
// from the store, so nothing is downloaded:
//
//	nix develop .#envtest -c go test -tags envtest ./internal/api/...
package v1alpha1

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// crdDirectory is config/crd, relative to this package's directory.
var crdDirectory = filepath.Join("..", "..", "..", "config", "crd")

// k8sClient talks to the envtest API server that TestMain starts.
var k8sClient client.Client

// TestMain starts an envtest control plane with the CRDs from config/crd
// installed, builds k8sClient against it, runs the tests and stops the control
// plane.
func TestMain(m *testing.M) {
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{crdDirectory},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start envtest control plane: %v\n", err)
		os.Exit(1)
	}

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{clientgoscheme.AddToScheme, AddToScheme} {
		if err := add(scheme); err != nil {
			fmt.Fprintf(os.Stderr, "register types with the scheme: %v\n", err)
			os.Exit(1)
		}
	}
	if k8sClient, err = client.New(cfg, client.Options{Scheme: scheme}); err != nil {
		fmt.Fprintf(os.Stderr, "build the client: %v\n", err)
		os.Exit(1)
	}

	code := m.Run()
	if err := testEnv.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "stop envtest control plane: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

// TestCRDValidation creates one VolumeRestore for each row of the validation
// table in docs/api.md. It checks that the API server accepts the valid object
// and rejects each malformed one with an Invalid error.
func TestCRDValidation(t *testing.T) {
	pointer := func(s string) *string { return &s }

	cases := []struct {
		name    string
		object  func() *VolumeRestore
		wantErr bool
	}{
		{
			name: "valid",
			object: func() *VolumeRestore {
				return &VolumeRestore{
					ObjectMeta: metav1.ObjectMeta{Name: "notes-data", Namespace: "default"},
					Spec: VolumeRestoreSpec{
						Repository:  "notes-restic",
						RestoreAsOf: pointer("2026-09-13T00:00:00Z"),
						MoverPodLabels: map[string]MoverPodLabelValue{
							"kueue.x-k8s.io/queue-name": "backup",
						},
					},
				}
			},
		},
		{
			name: "repository not a DNS-1123 subdomain",
			object: func() *VolumeRestore {
				return &VolumeRestore{
					ObjectMeta: metav1.ObjectMeta{Name: "upper-case", Namespace: "default"},
					Spec:       VolumeRestoreSpec{Repository: "Not_A_Name"},
				}
			},
			wantErr: true,
		},
		{
			name: "repository missing",
			object: func() *VolumeRestore {
				return &VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: "no-repository", Namespace: "default"}}
			},
			wantErr: true,
		},
		{
			name: "restoreAsOf not RFC3339",
			object: func() *VolumeRestore {
				return &VolumeRestore{
					ObjectMeta: metav1.ObjectMeta{Name: "bad-timestamp", Namespace: "default"},
					Spec: VolumeRestoreSpec{
						Repository:  "notes-restic",
						RestoreAsOf: pointer("yesterday"),
					},
				}
			},
			wantErr: true,
		},
		{
			name: "moverPodLabels key invalid",
			object: func() *VolumeRestore {
				return &VolumeRestore{
					ObjectMeta: metav1.ObjectMeta{Name: "bad-label-key", Namespace: "default"},
					Spec: VolumeRestoreSpec{
						Repository:     "notes-restic",
						MoverPodLabels: map[string]MoverPodLabelValue{"UPPER KEY": "v"},
					},
				}
			},
			wantErr: true,
		},
		{
			name: "moverPodLabels value invalid",
			object: func() *VolumeRestore {
				return &VolumeRestore{
					ObjectMeta: metav1.ObjectMeta{Name: "bad-label-value", Namespace: "default"},
					Spec: VolumeRestoreSpec{
						Repository:     "notes-restic",
						MoverPodLabels: map[string]MoverPodLabelValue{"app": "bad value!"},
					},
				}
			},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			object := tc.object()
			err := k8sClient.Create(context.Background(), object)
			switch {
			case tc.wantErr && err == nil:
				t.Fatalf("the API server stored %s, want an Invalid rejection", tc.name)
			case tc.wantErr && !apierrors.IsInvalid(err):
				t.Fatalf("the API server rejected %s with %v, want apierrors.IsInvalid", tc.name, err)
			case !tc.wantErr && err != nil:
				t.Fatalf("the API server rejected the valid object: %v", err)
			}
		})
	}
}

// TestRunsNameExactlyOneScope checks the CEL rules on BackupRun and RestoreRun
// specs. A run names one volume, one database, or the whole namespace, and
// never two of them. The RestoreRun cases also cover the rules for previous,
// into, syncDatabaseToVolume and quiesce.
func TestRunsNameExactlyOneScope(t *testing.T) {
	previous := int32(1)
	backups := []struct {
		name    string
		spec    BackupRunSpec
		wantErr bool
	}{
		{"source", BackupRunSpec{Source: "notes-data"}, false},
		{"database", BackupRunSpec{Database: "notes-pg"}, false},
		{"all", BackupRunSpec{All: true}, false},
		{"nothing", BackupRunSpec{}, true},
		{"source and all", BackupRunSpec{Source: "notes-data", All: true}, true},
		{"source and database", BackupRunSpec{Source: "notes-data", Database: "notes-pg"}, true},
	}
	for i, tc := range backups {
		t.Run("BackupRun "+tc.name, func(t *testing.T) {
			run := &BackupRun{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("brun-%d", i), Namespace: "default"}, Spec: tc.spec}
			check(t, k8sClient.Create(context.Background(), run), tc.wantErr)
		})
	}

	size := resource.MustParse("2Gi")
	restores := []struct {
		name    string
		spec    RestoreRunSpec
		wantErr bool
	}{
		{"claim", RestoreRunSpec{Claim: "notes-data"}, false},
		{"claim into", RestoreRunSpec{Claim: "notes-data", Into: "notes-data-monday"}, false},
		{"repository into", RestoreRunSpec{Repository: "notes-restic", Into: "copy", IntoSize: &size}, false},
		{"repository into without a size", RestoreRunSpec{Repository: "notes-restic", Into: "copy"}, true},
		{"database", RestoreRunSpec{Database: "notes-pg"}, false},
		{"all", RestoreRunSpec{All: true}, false},
		{"nothing", RestoreRunSpec{}, true},
		{"claim and database", RestoreRunSpec{Claim: "notes-data", Database: "notes-pg"}, true},
		{"database and all", RestoreRunSpec{Database: "notes-pg", All: true}, true},
		{"previous with all", RestoreRunSpec{All: true, Previous: &previous}, true},
		{"into with database", RestoreRunSpec{Database: "notes-pg", Into: "copy"}, true},
		{"sync with all", RestoreRunSpec{All: true, SyncDatabaseToVolume: true}, false},
		{"sync with a database", RestoreRunSpec{Database: "notes-pg", SyncDatabaseToVolume: true}, true},
		{"quiesce with all", RestoreRunSpec{All: true, Quiesce: []WorkloadRef{{Kind: "Deployment", Name: "notes"}}}, false},
		{"quiesce with a claim", RestoreRunSpec{Claim: "notes-data", Quiesce: []WorkloadRef{{Kind: "StatefulSet", Name: "notes"}}}, false},
		{"quiesce with into", RestoreRunSpec{Claim: "notes-data", Into: "copy", Quiesce: []WorkloadRef{{Kind: "Deployment", Name: "notes"}}}, true},
		{"quiesce of a CronJob", RestoreRunSpec{All: true, Quiesce: []WorkloadRef{{Kind: "CronJob", Name: "notes"}}}, true},
	}
	for i, tc := range restores {
		t.Run("RestoreRun "+tc.name, func(t *testing.T) {
			run := &RestoreRun{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("rrun-%d", i), Namespace: "default"}, Spec: tc.spec}
			check(t, k8sClient.Create(context.Background(), run), tc.wantErr)
		})
	}
}

// check fails the test when the API server's answer to creating a run does not
// match wantErr. A wanted error has to be an Invalid rejection.
func check(t *testing.T, err error, wantErr bool) {
	t.Helper()
	switch {
	case wantErr && err == nil:
		t.Fatal("the API server stored the run, want an Invalid rejection")
	case wantErr && !apierrors.IsInvalid(err):
		t.Fatalf("the API server rejected the run with %v, want apierrors.IsInvalid", err)
	case !wantErr && err != nil:
		t.Fatalf("the API server rejected a valid run: %v", err)
	}
}

// TestCRDRoundTrip creates a VolumeRestore and reads it back, and checks that
// each field it set survives. A schema that accepted the object but dropped a
// field would leave a claim with nothing to restore from.
func TestCRDRoundTrip(t *testing.T) {
	ctx := context.Background()
	created := &VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: "round-trip", Namespace: "default"},
		Spec: VolumeRestoreSpec{
			Repository:     "notes-restic",
			RestoreAsOf:    func() *string { s := "2026-09-13T00:00:00Z"; return &s }(),
			MoverPodLabels: map[string]MoverPodLabelValue{"kueue.x-k8s.io/queue-name": "backup"},
		},
	}
	if err := k8sClient.Create(ctx, created); err != nil {
		t.Fatalf("create the valid object: %v", err)
	}

	readBack := &VolumeRestore{}
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(created), readBack); err != nil {
		t.Fatalf("read the object back: %v", err)
	}
	if readBack.Spec.Repository != "notes-restic" {
		t.Errorf("repository read back as %q, want %q", readBack.Spec.Repository, "notes-restic")
	}
	if readBack.Spec.RestoreAsOf == nil || *readBack.Spec.RestoreAsOf != "2026-09-13T00:00:00Z" {
		t.Errorf("restoreAsOf read back as %v, want 2026-09-13T00:00:00Z", readBack.Spec.RestoreAsOf)
	}
	if got := readBack.Spec.MoverPodLabels["kueue.x-k8s.io/queue-name"]; got != "backup" {
		t.Errorf("moverPodLabels read back as %v, want the queue-name label intact", readBack.Spec.MoverPodLabels)
	}
}

// TestRunStatusFieldsRoundTrip writes the typed status fields of v0.9.0 onto a
// BackupRun and a RestoreRun through the status subresource, reads each run
// back, and checks that every field survives.
//
// The status is written as unstructured JSON, so the test sees what the
// installed CRD keeps and what the API server prunes. A field the schema lacks
// is dropped silently, and a run would then lose the state its later passes
// decide on: the ending a failed restart must repeat, the last start error, a
// Cluster left deleted, the full snapshot ID, the restore Job, its UID and the
// item's reason. The stored run is then decoded into the Go type and encoded
// again, so a JSON name in the Go type that differs from the CRD's fails as
// well.
func TestRunStatusFieldsRoundTrip(t *testing.T) {
	ending := map[string]any{"reason": "TimedOut", "message": "the run timed out after 6h0m0s"}
	runs := []struct {
		kind  string
		typed any
		spec  map[string]any
		item  map[string]any
	}{
		{
			kind:  "BackupRun",
			typed: &BackupRun{},
			spec:  map[string]any{"source": "notes-data"},
			item: map[string]any{
				"kind": "ReplicationSource", "name": "notes-data", "phase": "Pending",
				"snapshotID":     "4f3c2b1a0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a",
				"reason":         "TimedOut",
				"lastStartError": "the claim notes-data is not bound yet",
			},
		},
		{
			kind:  "RestoreRun",
			typed: &RestoreRun{},
			spec:  map[string]any{"claim": "notes-data"},
			item: map[string]any{
				"kind": "Cluster", "name": "notes-pg", "phase": "Failed",
				"snapshotID":         "4f3c2b1a0d9e8f7a6b5c4d3e2f1a0b9c8d7e6f5a4b3c2d1e0f9a8b7c6d5e4f3a",
				"job":                "notes-data-restore",
				"jobUID":             "0b6c1a52-8f4e-4d3a-9c7b-2e5f6a7b8c9d",
				"reason":             "RestoreJobFailed",
				"clusterLeftDeleted": true,
			},
		},
	}
	for _, run := range runs {
		t.Run(run.kind, func(t *testing.T) {
			status := map[string]any{"phase": "Failed", "ending": ending, "items": []any{run.item}}
			stored := writeRunStatus(t, run.kind, run.spec, status)
			for _, got := range []*unstructured.Unstructured{stored, throughGoType(t, stored, run.typed)} {
				assertField(t, got, ending, "status", "ending")
				for field, want := range run.item {
					assertField(t, got, want, "status", "items", "0", field)
				}
			}
		})
	}
}

// writeRunStatus creates a run and writes a status onto it through the status
// subresource, and returns the run as the API server stores it.
//
// Parameters:
//   - t is the calling test, failed when the API server refuses a step.
//   - kind is BackupRun or RestoreRun, the kind of run to create.
//   - spec is a spec the CRD accepts, since the run has to exist first.
//   - status is the status to write, as the JSON the controller would send.
//
// It returns the stored object read back from the API server, so the caller
// sees the status after the CRD's structural schema has pruned it.
func writeRunStatus(t *testing.T, kind string, spec, status map[string]any) *unstructured.Unstructured {
	t.Helper()
	ctx := context.Background()
	run := &unstructured.Unstructured{Object: map[string]any{"spec": spec}}
	run.SetGroupVersionKind(GroupVersion.WithKind(kind))
	run.SetNamespace("default")
	run.SetGenerateName("status-round-trip-")
	if err := k8sClient.Create(ctx, run); err != nil {
		t.Fatalf("create the %s: %v", kind, err)
	}
	run.Object["status"] = status
	if err := k8sClient.Status().Update(ctx, run); err != nil {
		t.Fatalf("write the %s status: %v", kind, err)
	}
	stored := &unstructured.Unstructured{}
	stored.SetGroupVersionKind(run.GroupVersionKind())
	if err := k8sClient.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
		t.Fatalf("read the %s back: %v", kind, err)
	}
	return stored
}

// assertField fails the test when the stored object lacks a field or holds
// another value in it.
//
// Parameters:
//   - t is the calling test.
//   - stored is the object read back from the API server.
//   - want is the value the test wrote into the field.
//   - path names the field from the object's root. A numeric step indexes a
//     list, so "items", "0" is the first item.
func assertField(t *testing.T, stored *unstructured.Unstructured, want any, path ...string) {
	t.Helper()
	var node any = stored.Object
	for _, step := range path {
		switch value := node.(type) {
		case map[string]any:
			node = value[step]
		case []any:
			index, err := strconv.Atoi(step)
			if err != nil || index >= len(value) {
				t.Fatalf("%s: no list entry %s in %v", strings.Join(path, "."), step, value)
			}
			node = value[index]
		}
	}
	if !reflect.DeepEqual(node, want) {
		t.Errorf("%s read back as %v, want %v: the CRD pruned or changed it", strings.Join(path, "."), node, want)
	}
}

// throughGoType decodes a stored object into its Go type and encodes it again,
// so that a field survives only when the Go type carries it under the same
// JSON name as the CRD.
//
// Parameters:
//   - t is the calling test, failed when either conversion fails.
//   - stored is the object read back from the API server.
//   - typed is a pointer to an empty value of the object's Go type, such as
//     &BackupRun{}, which the function fills.
//
// It returns the object as the Go type encodes it.
func throughGoType(t *testing.T, stored *unstructured.Unstructured, typed any) *unstructured.Unstructured {
	t.Helper()
	converter := runtime.DefaultUnstructuredConverter
	if err := converter.FromUnstructured(stored.Object, typed); err != nil {
		t.Fatalf("decode the stored %s into its Go type: %v", stored.GetKind(), err)
	}
	encoded, err := converter.ToUnstructured(typed)
	if err != nil {
		t.Fatalf("encode the %s from its Go type: %v", stored.GetKind(), err)
	}
	return &unstructured.Unstructured{Object: encoded}
}
