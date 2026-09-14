//go:build envtest

// The validation in docs/api.md is the API server's, not the Go types': the
// required repository, the RFC3339 restoreAsOf and the label syntax rules live
// in the generated CRD. Only a real API server shows whether they reject what
// they claim to, so this suite starts an envtest control plane, installs
// config/crd and reads the server's answer.
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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// crdDirectory is config/crd relative to this package directory.
var crdDirectory = filepath.Join("..", "..", "..", "config", "crd")

var k8sClient client.Client

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

// TestCRDValidation is the API server's answer for one object per row of the
// validation table in docs/api.md: the valid object is accepted, and each
// malformed one comes back as an Invalid rejection rather than being stored.
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

// TestCRDRoundTrip reads a stored object back. A schema that accepted the
// object while dropping a field would leave a claim with nothing to restore
// from, so the field the API server hands back is part of the contract.
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
