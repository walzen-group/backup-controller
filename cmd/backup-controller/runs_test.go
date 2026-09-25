package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestAManagerThatStopsOnItsOwnIsReported checks that startRunControllers
// calls its failure function when the running manager stops with an error
// while its context is still live. The manager here fails at once, because
// the address its metrics listener wants is already taken.
//
// A manager that stopped used to be logged and forgotten. The populator kept
// the process alive with no webhook server and no reconcilers, the webhook's
// failurePolicy Fail refused every Cluster create on the cluster, and nothing
// restarted the pod.
func TestAManagerThatStopsOnItsOwnIsReported(t *testing.T) {
	// An API server that answers nothing. The manager fails before it needs
	// one.
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)

	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = taken.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	failed := make(chan error, 1)
	fail := func(err error) { failed <- err }
	if err := startRunControllers(ctx, testKubeconfig(t, server.URL), taken.Addr().String(), "0", BootstrapWebhook{}, fail); err != nil {
		t.Fatalf("startRunControllers: %v", err)
	}

	select {
	case err := <-failed:
		if err == nil {
			t.Error("the failure function was called with a nil error")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the manager stopped and nothing was told")
	}
}

// TestTheManagerServesHealthAndReadiness checks that the manager answers
// /healthz and /readyz with 200 on the address it is given, which the
// Deployment's liveness and readiness probes call. The manager answers them
// while its caches are still syncing against an API server that knows
// nothing.
func TestTheManagerServesHealthAndReadiness(t *testing.T) {
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)

	// Reserve a free port and let it go, so the manager can bind it.
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := probe.Addr().String()
	_ = probe.Close()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	fail := func(err error) { t.Errorf("the manager stopped: %v", err) }
	if err := startRunControllers(ctx, testKubeconfig(t, server.URL), "0", addr, BootstrapWebhook{}, fail); err != nil {
		t.Fatalf("startRunControllers: %v", err)
	}

	for _, path := range []string{"/healthz", "/readyz"} {
		deadline := time.Now().Add(10 * time.Second)
		for {
			response, err := http.Get("http://" + addr + path)
			if err == nil {
				_ = response.Body.Close()
				if response.StatusCode == http.StatusOK {
					break
				}
			}
			if time.Now().After(deadline) {
				t.Fatalf("%s never answered 200: last error %v", path, err)
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}
