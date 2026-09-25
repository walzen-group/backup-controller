package main

import (
	"os"
	"path/filepath"
	"testing"
)

// testKubeconfig writes a kubeconfig naming the given API server URL into the
// test's temporary directory and returns its path.
func testKubeconfig(t *testing.T, server string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "kubeconfig")
	kubeconfig := `apiVersion: v1
kind: Config
clusters:
- name: test
  cluster:
    server: ` + server + `
contexts:
- name: test
  context:
    cluster: test
    user: test
current-context: test
users:
- name: test
  user:
    token: test
`
	if err := os.WriteFile(path, []byte(kubeconfig), 0o600); err != nil {
		t.Fatalf("write the kubeconfig: %v", err)
	}
	return path
}

// TestTheClientIsNotRateLimitedOnItsOwnSide checks that the client
// configuration turns off client-go's own rate limiter, as ctrl.GetConfig
// does, and leaves the API server's priority and fairness to share out
// requests.
//
// A QPS of 0 means client-go's default of 5 requests a second with a burst of
// 10. The bootstrap webhook reads one ObjectStore for every archiving Cluster
// on the cluster within a 15 second timeout, and fails closed, so a limit that
// low turns a busy cluster into one where no Cluster can be created.
func TestTheClientIsNotRateLimitedOnItsOwnSide(t *testing.T) {
	config, err := restConfig(testKubeconfig(t, "https://127.0.0.1:6443"))
	if err != nil {
		t.Fatalf("restConfig: %v", err)
	}
	if config.QPS >= 0 {
		t.Errorf("QPS = %v, want a negative value, which turns the client-side limiter off", config.QPS)
	}
}
