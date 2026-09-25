//go:build envtest

// The validation rules listed in docs/api.md live in the generated CRD: the
// required repository, the RFC 3339 restoreAsOf and the label syntax rules.
// The Go types enforce none of them, and only a real API server shows whether
// they reject what they should. This suite starts an envtest control plane,
// installs config/crd, creates objects and reads the server's answer.
//
// Run it with the envtest assets on the path:
//
//	KUBEBUILDER_ASSETS=$(setup-envtest use 1.37.0 -p path) go test -tags envtest ./internal/api/...
package v1alpha1

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
