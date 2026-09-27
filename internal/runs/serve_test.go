package runs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// testManager returns a manager over an API server that answers nothing,
// for SetupWithManager; the test never starts it.
func testManager(t *testing.T) ctrl.Manager {
	t.Helper()
	server := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(server.Close)
	skip := true
	manager, err := ctrl.NewManager(&rest.Config{Host: server.URL}, ctrl.Options{
		Scheme:                 scheme(t),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		Controller:             ctrlconfig.Controller{SkipNameValidation: &skip},
	})
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

// checkVersionGoneRetried fails the test unless a read of a
// ReplicationDestination through c, whose API server no longer serves the
// version, comes back as an error that is not NotFound.
func checkVersionGoneRetried(t *testing.T, what string, c client.Reader) {
	t.Helper()
	err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "restore-9b7d4e21-0"}, &volsyncv1alpha1.ReplicationDestination{})
	if err == nil || apierrors.IsNotFound(err) {
		t.Errorf("%s: get = %v, want an error that is not NotFound", what, err)
	}
}

// SetupWithManager wraps a BackupRunReconciler's client and reader with
// served.Client, so a read at a version the API server no longer serves is
// retried, never taken for a missing object.
func TestBackupRunSetupWrapsTheClientForGoneVersions(t *testing.T) {
	gone := destinationGone(newClient(t))
	r := &BackupRunReconciler{Client: gone, Reader: gone}
	if err := r.SetupWithManager(testManager(t)); err != nil {
		t.Fatal(err)
	}
	checkVersionGoneRetried(t, "Client", r.Client)
	checkVersionGoneRetried(t, "Reader", r.Reader)
}
