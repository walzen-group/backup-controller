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
	if err := startRunControllers(ctx, testKubeconfig(t, server.URL), taken.Addr().String(), BootstrapWebhook{}, fail); err != nil {
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
