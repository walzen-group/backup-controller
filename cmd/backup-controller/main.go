// Command backup-controller runs the backup.wlz.li controllers in one process.
//
// It fills PersistentVolumeClaims from restic repositories. It hands the
// callbacks in internal/populator to lib-volume-populator's provider-function
// mode, so the controller's own restic Job fills a claim whose dataSourceRef
// refers to a VolumeRestore, and this binary builds no populator pod of its
// own. It also runs a controller-runtime manager that reconciles BackupRun
// and RestoreRun, runs the namespace backup scheduler, and serves the
// bootstrap webhook for CloudNativePG Clusters when a certificate directory
// is given.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	"github.com/walzen-group/backup-controller/internal/admission"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/populator"
	"github.com/walzen-group/backup-controller/internal/restic"
	"golang.org/x/time/rate"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
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
		namespace   = flag.String("namespace", "backup-system", "namespace the prime claims and the restore Jobs live in")
		metricsAddr = flag.String("metrics-addr", ":8080", "address the populator library's metrics listener binds")
		metricsPath = flag.String("metrics-path", "/metrics", "path the metrics listener serves")
		runsMetrics = flag.String("runs-metrics-addr", ":8081", "address the scheduler's metrics listener binds, at /metrics")
		webhookCert = flag.String("webhook-cert-dir", "", "directory holding tls.crt and tls.key; empty serves no webhook")
		webhookPort = flag.Int("webhook-port", 9443, "port the admission webhook listens on")
		printVer    = flag.Bool("version", false, "print the version and exit")
		pause       = flag.Bool("pause", false, "start paused, for an upgrade: new runs wait, runs that started work finish, the scheduler creates no run")
		restoreImg  = flag.String("restore-image", "", "image of the restore Jobs: VolSync's mover image, which holds the restic that wrote the repositories")
		leaderElect = flag.Bool("leader-elect", false, "take the Lease backup-controller in --namespace before running any controller, so several replicas can run")
	)
	klog.InitFlags(nil)
	flag.Parse()

	if *printVer {
		fmt.Println(version)
		return
	}
	// Every volume restore runs a Job with this image. Without it, each
	// restore would fail only once it starts, so the process stops here.
	if *restoreImg == "" {
		klog.Errorf("--restore-image is required: pass VolSync's mover image, which holds the restic that wrote the repositories")
		os.Exit(2)
	}

	operations, err := newClientOperations(kubeconfig())
	if err != nil {
		klog.Errorf("failed to build the Kubernetes client: %v", err)
		os.Exit(1)
	}
	callbacks := populator.New(populator.Config{
		Operations: operations,
		Client:     operations.client,
		Reader:     operations.client,
		Namespace:  *namespace,
		Image:      *restoreImg,
		Snapshots:  restic.S3Lister{},
		Paused:     *pause,
	})

	// The populator library drives only the one kind it is given, so
	// BackupRun and RestoreRun are reconciled by a controller-runtime manager
	// that this binary starts itself. Cancelling runs stops that manager once
	// the library returns, which is the only shutdown signal this process
	// gets.
	runs, stopRuns := context.WithCancel(context.Background())
	defer stopRuns()
	hook := BootstrapWebhook{CertDir: *webhookCert, Port: *webhookPort}
	election := Election{Enabled: *leaderElect, Namespace: *namespace}
	elected, stopped, err := startRunControllers(runs, kubeconfig(), *runsMetrics, hook, election, *pause, *restoreImg)
	if err != nil {
		klog.Errorf("failed to start the run controllers: %v", err)
		os.Exit(1)
	}

	// The populator library has no leader election of its own, so it starts
	// only once the manager holds the Lease. A replica waiting here serves the
	// webhook and nothing else. SIGTERM ends it with Go's default handling,
	// since the library's handler is installed only once it starts.
	<-elected

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
		Workqueue: populatorQueue(),
	})

	// Waiting for the manager lets it release the Lease, so the other replica
	// takes over now and not when the Lease expires.
	klog.Info("stopping backup-controller")
	stopRuns()
	<-stopped
}

// newClientOperations builds the cluster operations that the populator
// callbacks run through.
//
// Parameters:
//   - kubeconfig is the path from the --kubeconfig flag. An empty path means
//     the in-cluster configuration.
//
// It returns the operations, or an error when the client configuration
// cannot be built or a scheme fails to register.
//
// The client reads straight from the API server, with no cache. Its scheme
// knows the core, batch and backup.wlz.li types.
func newClientOperations(kubeconfig string) (*clientOperations, error) {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build client configuration: %w", err)
	}

	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":          corev1.AddToScheme,
		"batch":         batchv1.AddToScheme,
		"backup.wlz.li": backupv1alpha1.AddToScheme,
		"kueue":         admission.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, fmt.Errorf("register the %s types: %w", name, err)
		}
	}

	kubeClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	klog.Infof("starting backup-controller: watching %s VolumeRestore", backupv1alpha1.GroupVersion.String())
	return &clientOperations{client: kubeClient}, nil
}

// populatorQueue returns the work queue of the populator library.
//
// The library calls Populate again after an error and Complete again while a
// restore Job runs, each after the queue's backoff. The library's default
// backoff doubles up to 1000 seconds, so a claim waiting for Kueue or for its
// Job would be looked at only every 16 minutes. This queue doubles from one
// second up to 30 seconds, and allows 10 retries a second overall.
func populatorQueue() workqueue.TypedRateLimitingInterface[any] {
	return workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedMaxOfRateLimiter(
		workqueue.NewTypedItemExponentialFailureRateLimiter[any](time.Second, 30*time.Second),
		&workqueue.TypedBucketRateLimiter[any]{Limiter: rate.NewLimiter(rate.Limit(10), 100)},
	))
}
