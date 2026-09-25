// Command backup-controller runs the backup.wlz.li controllers in one process.
//
// It fills PersistentVolumeClaims from restic repositories. It hands the
// callbacks in internal/populator to lib-volume-populator's provider-function
// mode, so a VolSync mover fills a claim whose dataSourceRef names a
// VolumeRestore, and this binary builds no populator pod of its own. It also
// runs a controller-runtime manager that reconciles BackupRun and RestoreRun,
// runs the namespace backup scheduler, and serves the bootstrap webhook for
// CloudNativePG Clusters when a certificate directory is given.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/populator"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
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

// prefix is the domain of the annotations and finalizers the populator library
// writes onto the claims it fills. It is the API group, so every mark this
// controller leaves on an object can be traced to this project.
const prefix = "backup.wlz.li"

// main parses the flags, starts the run controllers, and then runs the
// populator library until it stops.
func main() {
	var (
		kubeconfig  = kubeconfigFlag(flag.CommandLine)
		namespace   = flag.String("namespace", "backup-system", "namespace the prime claim and the ReplicationDestination live in")
		metricsAddr = flag.String("metrics-addr", ":8080", "address the populator library's metrics listener binds")
		metricsPath = flag.String("metrics-path", "/metrics", "path the metrics listener serves")
		runsMetrics = flag.String("runs-metrics-addr", ":8081", "address the scheduler's metrics listener binds, at /metrics")
		webhookCert = flag.String("webhook-cert-dir", "", "directory holding tls.crt and tls.key; empty serves no webhook")
		webhookPort = flag.Int("webhook-port", 9443, "port the admission webhook listens on")
		printVer    = flag.Bool("version", false, "print the version and exit")
	)
	klog.InitFlags(nil)
	flag.Parse()

	if *printVer {
		fmt.Println(version)
		return
	}

	operations, err := newClientOperations(kubeconfig())
	if err != nil {
		klog.Errorf("failed to build the Kubernetes client: %v", err)
		os.Exit(1)
	}
	callbacks := populator.New(operations, *namespace, restic.S3Lister{})

	// The populator library drives only the one kind it is given, so
	// BackupRun and RestoreRun are reconciled by a controller-runtime manager
	// that this binary starts itself. The deferred cancel stops that manager
	// once the library returns, which is the only shutdown signal this process
	// gets.
	runs, stopRuns := context.WithCancel(context.Background())
	defer stopRuns()
	hook := BootstrapWebhook{CertDir: *webhookCert, Port: *webhookPort}
	if err := startRunControllers(runs, kubeconfig(), *runsMetrics, hook); err != nil {
		klog.Errorf("failed to start the run controllers: %v", err)
		os.Exit(1)
	}

	// The library registers its own handler for SIGTERM and interrupt, and it
	// closes the stop channel itself. This binary installs no second handler,
	// because two handlers closing one channel race and the loser panics.
	// RunControllerWithConfig returns once the controller has stopped, and
	// returning from main after it is the clean exit.
	populatormachinery.RunControllerWithConfig(populatormachinery.VolumePopulatorConfig{
		Kubeconfig:   kubeconfig(),
		HttpEndpoint: *metricsAddr,
		MetricsPath:  *metricsPath,
		Namespace:    *namespace,
		Prefix:       prefix,
		Gk:           schema.GroupKind{Group: backupv1alpha1.GroupVersion.Group, Kind: "VolumeRestore"},
		Gvr:          backupv1alpha1.GroupVersion.WithResource("volumerestores"),
		ProviderFunctionConfig: &populatormachinery.ProviderFunctionConfig{
			PopulateFn:         callbacks.Populate,
			PopulateCompleteFn: callbacks.Complete,
			PopulateCleanupFn:  callbacks.Cleanup,
		},
	})

	klog.Info("stopping backup-controller")
}

// newClientOperations builds the cluster operations the populator callbacks
// run through. They use a client whose scheme knows the core, VolSync and
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
		"volsync":       volsyncv1alpha1.AddToScheme,
		"backup.wlz.li": backupv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, fmt.Errorf("register the %s types: %w", name, err)
		}
	}

	kubeClient, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	klog.Infof("starting backup-controller: watching %s VolumeRestore", backupv1alpha1.GroupVersion.String())
	return &clientOperations{client: kubeClient}, nil
}
