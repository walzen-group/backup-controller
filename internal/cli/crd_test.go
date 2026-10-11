//go:build envtest

// The plugin leaves the rules about which fields go together to the API
// server. This suite installs config/crd into an envtest control plane and
// creates the objects the plugin builds, so a flag that maps to the wrong
// field, or a rule message the plugin would mangle, shows up here.
//
// Run it with the envtest assets on the path:
//
//	KUBEBUILDER_ASSETS=$(setup-envtest use 1.37.0 -p path) go test -tags envtest ./internal/cli/...
package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/spf13/pflag"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// k8sClient talks to the envtest API server that TestMain starts.
var k8sClient client.Client

// TestMain starts an envtest control plane with config/crd installed, runs the
// tests and stops the control plane.
func TestMain(m *testing.M) {
	testEnv := &envtest.Environment{
		CRDDirectoryPaths:     []string{filepath.Join("..", "..", "config", "crd")},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := testEnv.Start()
	if err != nil {
		fmt.Fprintf(os.Stderr, "start envtest control plane: %v\n", err)
		os.Exit(1)
	}
	scheme := runtime.NewScheme()
	if err := backupv1alpha1.AddToScheme(scheme); err != nil {
		fmt.Fprintf(os.Stderr, "register types with the scheme: %v\n", err)
		os.Exit(1)
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

// TestServerAnswers creates the object each command line builds and checks
// that the API server accepts it, or rejects it with the rule's own message.
func TestServerAnswers(t *testing.T) {
	cases := []struct {
		command string
		args    []string
		reject  string
	}{
		{"run", []string{"all", "--all"}, ""},
		{"run", []string{"one-claim", "--source", "data", "--timeout", "2h", "--ttl", "3600"}, ""},
		{"run", []string{"one-database", "--database", "app-pg"}, ""},
		{"run", []string{"two-targets", "--all", "--source", "data"}, "set exactly one of source, database and all"},
		{"run", []string{"no-target"}, "set exactly one of source, database and all"},
		{"restore", []string{"in-place", "--claim", "data", "--as-of", "2026-10-11T04:00:00Z", "--previous", "1"}, ""},
		{"restore", []string{"into", "--claim", "data", "--into", "data-old", "--into-size", "10Gi"}, ""},
		{"restore", []string{"everything", "--all", "--sync-database-to-volume", "--pause", "deploy/app"}, ""},
		{"restore", []string{"database", "--database", "app-pg"}, ""},
		{"restore", []string{"database-into", "--database", "app-pg", "--into", "x"}, "into needs claim or repository"},
		{"restore", []string{"sync-one", "--claim", "data", "--sync-database-to-volume"}, "syncDatabaseToVolume needs all"},
		{"restore", []string{"bad-time", "--claim", "data", "--as-of", "yesterday"}, "restoreAsOf"},
	}
	for _, tc := range cases {
		t.Run(tc.command+"/"+tc.args[0], func(t *testing.T) {
			flags := func(fs *pflag.FlagSet) builder { return backupFlags(fs) }
			if tc.command == "restore" {
				flags = func(fs *pflag.FlagSet) builder { return restoreFlags(fs) }
			}
			obj, _, err := parse(append(tc.args, "-n", "default"), tc.command, io.Discard, flags)
			if err != nil {
				t.Fatalf("parse: %v", err)
			}
			err = k8sClient.Create(context.Background(), obj)
			switch {
			case tc.reject == "" && err != nil:
				t.Fatalf("server rejected the object: %v", err)
			case tc.reject != "" && err == nil:
				t.Fatalf("server accepted the object; want a rejection containing %q", tc.reject)
			case tc.reject != "" && (!apierrors.IsInvalid(err) || !strings.Contains(err.Error(), tc.reject)):
				t.Fatalf("server answered %v; want an Invalid error containing %q", err, tc.reject)
			}
		})
	}
}
