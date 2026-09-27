package main

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/bootstrap"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	if err := startRunControllers(ctx, testRunOptions(t, server.URL, taken.Addr().String(), "0"), fail); err != nil {
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
	if err := startRunControllers(ctx, testRunOptions(t, server.URL, "0", addr), fail); err != nil {
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

// TestTheRunSchemeKnowsTheMoverJob checks that the scheme of the run manager
// holds batch/v1 Job. A RestoreRun reads the Job of a mover it stopped before
// it gives the app back (rule X2). Without the kind in the scheme every such
// read fails, and every restore would wait until someone deletes it.
func TestTheRunSchemeKnowsTheMoverJob(t *testing.T) {
	scheme, err := runScheme()
	if err != nil {
		t.Fatal(err)
	}
	if job := batchv1.SchemeGroupVersion.WithKind("Job"); !scheme.Recognizes(job) {
		t.Errorf("the run manager's scheme does not hold %s", job)
	}
}

// testRunOptions returns the options startRunControllers gets in these
// tests.
//
// Parameters:
//   - t writes the kubeconfig file, which testKubeconfig removes when the
//     test ends.
//   - server is the URL of the test's API server.
//   - metricsAddr and healthAddr are the listen addresses the test wants
//     for the metrics and probe servers.
//
// The run controllers start without a webhook, in namespace backup-system,
// with an image that is only carried, since no test here starts a restore.
func testRunOptions(t *testing.T, server, metricsAddr, healthAddr string) RunOptions {
	t.Helper()
	return RunOptions{
		Kubeconfig:   testKubeconfig(t, server),
		Namespace:    "backup-system",
		MetricsAddr:  metricsAddr,
		HealthAddr:   healthAddr,
		RestoreImage: "restic.example/restic:test",
	}
}

// TestConfigureLoggingMakesControllerRuntimeLog checks that after
// configureLogging runs, controller-runtime's logger is enabled.
//
// v0.2.2 ran on the cluster with no logger set. The BackupRun and RestoreRun
// reconcilers discarded every line they wrote, and controller-runtime reported
// it once, after thirty seconds, with a stack trace:
//
//	[controller-runtime] log.SetLogger(...) was never called; logs will not be
//	displayed.
//
// Until SetLogger is called, controller-runtime's delegating sink drops every
// line, so Enabled reports false. Every defect found in this controller so far
// was found by reading a log, which is why a silent reconciler gets a test.
func TestConfigureLoggingMakesControllerRuntimeLog(t *testing.T) {
	configureLogging()

	if !ctrl.Log.Enabled() {
		t.Error("controller-runtime's logger is disabled, so the reconcilers write nothing anywhere")
	}
}

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

// TestTheBinaryCarriesTheZoneDatabase checks that time/tzdata is among the
// binary's dependencies. The image is FROM scratch and has no
// /usr/share/zoneinfo, so a schedule with a CRON_TZ prefix can load its zone
// only when the binary carries the zone database. Every developer machine has
// the zone files, so parsing a schedule here would pass either way. The test
// reads the build's package list for that reason.
func TestTheBinaryCarriesTheZoneDatabase(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for _, pkg := range strings.Fields(string(out)) {
		if pkg == "time/tzdata" {
			return
		}
	}
	t.Fatal("time/tzdata is not among the binary's packages; a CRON_TZ schedule fails in the image")
}

// lookupRecorder is a RESTMapper that records each group and kind looked up
// with no version, the lookup that makes controller-runtime's lazy mapper
// read every version of a group.
type lookupRecorder struct {
	meta.RESTMapper
	looked map[schema.GroupKind]bool
}

// RESTMapping records gk when no version is given.
func (m *lookupRecorder) RESTMapping(gk schema.GroupKind, versions ...string) (*meta.RESTMapping, error) {
	if len(versions) == 0 {
		m.looked[gk] = true
	}
	return m.RESTMapper.RESTMapping(gk, versions...)
}

// The startup check warms the manager's mapper for the bootstrap webhook:
// it looks up Cluster and ObjectStore, so the webhook's first admission
// request finds both groups cached and runs no discovery call.
func TestTheStartupCheckWarmsTheWebhooksGroups(t *testing.T) {
	cluster := schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}
	defaults := meta.NewDefaultRESTMapper(nil)
	defaults.Add(cluster, meta.RESTScopeNamespace)
	defaults.Add(bootstrap.ObjectStoreGVK, meta.RESTScopeNamespace)
	mapper := &lookupRecorder{RESTMapper: defaults, looked: map[schema.GroupKind]bool{}}

	checkServedVersions(mapper)

	for _, gk := range []schema.GroupKind{cluster.GroupKind(), bootstrap.ObjectStoreGVK.GroupKind()} {
		if !mapper.looked[gk] {
			t.Errorf("the startup check did not look up %s", gk)
		}
	}
}

// versionGoneClient returns a fake client that answers every read of a Job
// with the plain-text 404 kube-apiserver gives for a version it no longer
// serves, as after an upgrade that drops batch/v1 while the client's mapper
// still caches it. client-go turns that answer into a NotFound, the same as
// for a Job that is gone.
func versionGoneClient(t *testing.T) client.WithWatch {
	t.Helper()
	s := runtime.NewScheme()
	if err := batchv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	gone := func(key client.ObjectKey) error {
		return apierrors.NewGenericServerResponse(http.StatusNotFound, "get",
			schema.GroupResource{Group: batchv1.GroupName, Resource: "jobs"}, key.Name, "404 page not found", 0, true)
	}
	return fake.NewClientBuilder().WithScheme(s).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				return gone(key)
			}
			return c.Get(ctx, key, obj, opts...)
		},
	}).Build()
}

// checkNotMissing fails the test unless err is an error that does not read
// as a missing object.
func checkNotMissing(t *testing.T, what string, err error) {
	t.Helper()
	if err == nil || apierrors.IsNotFound(err) {
		t.Errorf("%s = %v, want an error that is not NotFound", what, err)
	}
}

// The populator reads a restore Job at a version the API server no longer
// serves as an error it retries, never as a Job that is gone, which would
// let it create a second Job or bind the claim.
func TestThePopulatorNeverTakesAVersionGoneForAMissingJob(t *testing.T) {
	_, err := operationsFor(versionGoneClient(t)).GetJob(context.Background(), types.NamespacedName{Namespace: "backup-system", Name: "restore-9b7d4e21"})
	checkNotMissing(t, "GetJob", err)
}
