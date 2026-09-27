//go:build envtest

package runs

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// TestEnvtestTheVolSyncCheckNamesReplicationSourceOnAFreshMapper starts a
// kube-apiserver that serves VolSync's two kinds at v1beta1 alone and asks
// volsyncSourceUnserved through a fresh lazy RESTMapper, as the controller's
// startup check does before any run has looked a kind up. The first answer
// names ReplicationSource, the one VolSync kind the controller uses, and
// never ReplicationDestination, which it no longer uses. The second answer
// is the same.
//
//	nix develop .#envtest -c go test -tags envtest ./internal/runs/ -run Envtest
func TestEnvtestTheVolSyncCheckNamesReplicationSourceOnAFreshMapper(t *testing.T) {
	dir := t.TempDir()
	for _, f := range crdsWithVolSyncAt(t, "v1beta1") {
		base := filepath.Base(f)
		if base != "volsync.backube_replicationsources.yaml" && base != "volsync.backube_replicationdestinations.yaml" {
			continue
		}
		data, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, base), data, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	env := &envtest.Environment{CRDDirectoryPaths: []string{dir}, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	httpClient, err := rest.HTTPClientFor(cfg)
	if err != nil {
		t.Fatal(err)
	}
	mapper, err := apiutil.NewDynamicRESTMapper(cfg, httpClient)
	if err != nil {
		t.Fatal(err)
	}

	for _, pass := range []string{"first", "second"} {
		message := fmt.Sprint(volsyncSourceUnserved(mapper))
		if !strings.Contains(message, "ReplicationSource") {
			t.Errorf("%s check: message %q does not name ReplicationSource", pass, message)
		}
		if strings.Contains(message, "ReplicationDestination") {
			t.Errorf("%s check: message %q names ReplicationDestination, which the controller no longer uses", pass, message)
		}
	}
}
