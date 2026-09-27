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
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
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

// main parses the flags, starts the run controllers, and then runs the
// populator library until it stops.
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

	operations, err := newClientOperations(options.Kubeconfig)
	if err != nil {
		klog.Errorf("failed to build the Kubernetes client: %v", err)
		os.Exit(1)
	}
	callbacks := populator.New(operations, options.Namespace, options.RestoreImage, restic.S3Lister{})

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
		os.Exit(1) //nolint:gocritic // exitAfterDefer: the exit ends all stopRuns would stop; T3 moves the exit
	}

	// The library registers its own handler for SIGTERM and interrupt, and it
	// closes the stop channel itself. This binary installs no second handler,
	// because two handlers closing one channel race and the loser panics.
	// RunControllerWithConfig returns once the controller has stopped, and
	// returning from main after it is the clean exit. When the library fails
	// instead, to build its clients or to sync its caches, it calls
	// klog.Fatalf, which exits the process with a non-zero status, so a
	// failed populator restarts the pod without help from this binary.
	populatormachinery.RunControllerWithConfig(populatormachinery.VolumePopulatorConfig{
		Kubeconfig:            options.Kubeconfig,
		HttpEndpoint:          *metricsAddr,
		MetricsPath:           *metricsPath,
		Namespace:             options.Namespace,
		Prefix:                populator.Prefix,
		SharedInformerOptions: populatorInformerOptions(),
		Gk:                    schema.GroupKind{Group: backupv1alpha1.GroupVersion.Group, Kind: "VolumeRestore"},
		Gvr:                   backupv1alpha1.GroupVersion.WithResource("volumerestores"),
		ProviderFunctionConfig: &populatormachinery.ProviderFunctionConfig{
			PopulateFn:         callbacks.Populate,
			PopulateCompleteFn: callbacks.Complete,
			PopulateCleanupFn:  callbacks.Cleanup,
		},
	})

	klog.Info("stopping backup-controller")
}

// exitOnFailure ends the process with status 1 after logging why. main hands
// it to startRunControllers, which calls it when the manager stops on its own.
//
// Without the manager the process serves no webhook and reconciles no run,
// while the populator keeps it alive. The webhook's failurePolicy is Fail, so
// every Cluster create on the cluster is refused until something restarts the
// pod, and exiting is what gets the kubelet to restart it.
func exitOnFailure(err error) {
	klog.Errorf("the run controllers stopped, exiting so the pod restarts: %v", err)
	klog.Flush()
	os.Exit(1) //nolint:revive // deep-exit: the pod restarts only when the process ends; T3 moves the exit into main
}

// newClientOperations builds the cluster operations the populator callbacks
// run through. They use a client whose scheme knows the core, batch and
// backup.wlz.li types. The kubeconfig argument is the path from the
// --kubeconfig flag, and an empty path means the in-cluster configuration.
//
// It returns an error when the client configuration can't be built or a
// scheme fails to register.
func newClientOperations(kubeconfig string) (populator.Operations, error) {
	config, err := restConfig(kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build client configuration: %w", err)
	}

	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":          corev1.AddToScheme,
		"batch":         batchv1.AddToScheme,
		"backup.wlz.li": backupv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, fmt.Errorf("register the %s types: %w", name, err)
		}
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
func operationsFor(kubeClient client.Client) *clientOperations {
	return newOperations(served.Client(kubeClient))
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
