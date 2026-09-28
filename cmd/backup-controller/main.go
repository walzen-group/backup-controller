// Command backup-controller runs the backup.wlz.li controllers in one process.
//
// It fills PersistentVolumeClaims from restic repositories. It hands the
// callbacks in internal/populator to lib-volume-populator's provider-function
// mode, so the controller's own restic restore Job fills a claim whose
// dataSourceRef names a VolumeRestore, and this binary builds no populator pod
// of its own. It also runs a controller-runtime manager that reconciles
// BackupRun and RestoreRun, runs the namespace backup scheduler, and serves
// the bootstrap webhook for CloudNativePG Clusters when a certificate
// directory is given.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/populator"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/served"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	// The image is FROM scratch and has no zone files, so a schedule with a
	// CRON_TZ prefix loads its zone from the copy of the zone database this
	// import compiles into the binary.
	_ "time/tzdata"
)

// version is set at build time with -ldflags "-X main.version=...". The
// Dockerfile passes its VERSION build argument through to it.
var version = "dev"

// main parses the flags, runs the controller, and ends the process with the
// status that run returns.
//
// The function that main gives to run ends the process with status 1 after it
// logs why. run hands it to startRunControllers, which calls it when the
// manager stops on its own. Without the manager the process serves no webhook
// and reconciles no run, while the populator keeps it alive. The webhook's
// failurePolicy is Fail, so the API server refuses every Cluster create until
// something restarts the pod. The exit makes the kubelet restart the pod.
func main() {
	var (
		runOptions  = runFlags(flag.CommandLine)
		metricsAddr = flag.String("metrics-addr", ":8080", "address the populator library's metrics listener binds")
		metricsPath = flag.String("metrics-path", "/metrics", "path the metrics listener serves")
		printVer    = flag.Bool("version", false, "print the version and exit")
	)
	klog.InitFlags(nil)
	flag.Parse()

	if *printVer {
		fmt.Println(version)
		return
	}

	// The restore Jobs need an image and the controller carries none of its
	// own, so a command line without one ends the process here, before it
	// builds a client or starts a controller.
	options, err := runOptions()
	if err != nil {
		klog.Errorf("refusing to start: %v", err)
		os.Exit(1)
	}

	os.Exit(run(options, metrics{addr: *metricsAddr, path: *metricsPath}, func(err error) {
		klog.Errorf("the run controllers stopped, exiting so the pod restarts: %v", err)
		klog.Flush()
		os.Exit(1)
	}))
}

// metrics holds the flags of the populator library's metrics listener.
type metrics struct {
	// addr is the address that the listener binds, from --metrics-addr.
	addr string
	// path is the path that the listener serves, from --metrics-path.
	path string
}

// run starts the run controllers, and then runs the populator library until
// it stops.
//
// Parameters:
//   - options holds the settings from the command line (see RunOptions).
//   - listener holds the flags of the metrics listener.
//   - exitOnFailure ends the process. main gives it, and run hands it to
//     startRunControllers for a manager that stops on its own.
//
// It returns the exit status of the process: 0 when the populator library
// stops, and 1 when the controller cannot start.
func run(options RunOptions, listener metrics, exitOnFailure func(error)) int {
	operations, err := newClientOperations(options.Kubeconfig)
	if err != nil {
		klog.Errorf("failed to build the Kubernetes client: %v", err)
		return 1
	}
	callbacks := populator.New(operations, options.Namespace, options.RestoreImage, restic.S3Lister{})
	if options.Paused {
		callbacks.Pause()
		klog.Info("paused: new runs wait, runs in progress finish")
	}

	// The populator library drives only the one kind it is given, so
	// BackupRun and RestoreRun are reconciled by a controller-runtime manager
	// that this binary starts itself. The deferred cancel stops that manager
	// once the library returns, which is the only shutdown signal this process
	// gets. A manager that stops on its own calls exitOnFailure, so the
	// process never runs on with the populator alone.
	runs, stopRuns := context.WithCancel(context.Background())
	defer stopRuns()
	if err := startRunControllers(runs, options, exitOnFailure); err != nil {
		klog.Errorf("failed to start the run controllers: %v", err)
		return 1
	}

	// The library registers its own handler for SIGTERM and interrupt, and it
	// closes the stop channel itself. This binary installs no second handler,
	// because two handlers closing one channel race and the loser panics.
	// RunControllerWithConfig returns once the controller has stopped, and
	// returning from run after it is the clean exit. When the library fails
	// instead, to build its clients or to sync its caches, it calls
	// klog.Fatalf, which exits the process with a non-zero status, so a
	// failed populator restarts the pod without help from this binary.
	populatormachinery.RunControllerWithConfig(populatormachinery.VolumePopulatorConfig{
		Kubeconfig:   options.Kubeconfig,
		HttpEndpoint: listener.addr,
		MetricsPath:  listener.path,
		Namespace:    options.Namespace,
		Prefix:       populator.Prefix,
		Gk:           schema.GroupKind{Group: backupv1alpha1.GroupVersion.Group, Kind: "VolumeRestore"},
		Gvr:          backupv1alpha1.GroupVersion.WithResource("volumerestores"),
		ProviderFunctionConfig: &populatormachinery.ProviderFunctionConfig{
			PopulateFn:         callbacks.Populate,
			PopulateCompleteFn: callbacks.Complete,
			PopulateCleanupFn:  callbacks.Cleanup,
		},
	})

	klog.Info("stopping backup-controller")
	return 0
}

// newClientOperations builds the cluster operations the populator callbacks
// run through. They use a client with the run manager's scheme (see
// runScheme). The kubeconfig argument is the path from the --kubeconfig
// flag, and an empty path means the in-cluster configuration.
//
// It returns an error when the client configuration can't be built or a
// scheme fails to register.
func newClientOperations(kubeconfig string) (populator.Operations, error) {
	config, err := restConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build client configuration: %w", err)
	}

	scheme, err := runScheme()
	if err != nil {
		return nil, err
	}

	kubeClient, err := client.New(config, clientOptions(scheme))
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	klog.Infof("starting backup-controller: watching %s VolumeRestore", backupv1alpha1.GroupVersion.String())
	return operationsFor(kubeClient), nil
}

// operationsFor returns the populator's operations over kubeClient.
//
// Parameters:
//   - kubeClient is the populator's client, built with clientOptions.
//
// served.Client wraps it, so a 404 for a version the API server no longer
// serves is an error the populator retries, never a restore Job that is
// gone.
func operationsFor(kubeClient client.Client) populator.Operations {
	return populator.NewOperations(served.Client(kubeClient))
}

// clientOptions returns the options of every client the controller writes
// through: the manager's client and the populator's.
//
// The scheme argument is the client's scheme.
//
// Every create, update and patch asks for fieldValidation=Strict, so a field
// the target's schema does not declare fails the write with an error that
// names it, which the run reports. Without it kube-apiserver defaults to
// Warn (k8s.io/apiserver v0.36.3 pkg/endpoints/handlers/rest.go:409-414):
// it prunes a custom resource's unknown fields and answers with a warning
// only, so a field a CloudNativePG, Flux, Kueue or VolSync release renamed or
// removed would vanish from the controller's write without a trace.
// controller-runtime v0.24.1 passes the option on every Create, Update and
// Patch of the client and of its Status and SubResource writers
// (pkg/client/client.go:124-126, pkg/client/fieldvalidation.go).
// TestEnvtestTheControllersWritesRefuseUnknownFields checks both against the
// pinned kube-apiserver.
func clientOptions(scheme *runtime.Scheme) client.Options {
	return client.Options{Scheme: scheme, FieldValidation: metav1.FieldValidationStrict}
}
