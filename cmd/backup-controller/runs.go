package main

import (
	"context"
	"errors"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/populator"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/runs"
	"github.com/walzen-group/backup-controller/internal/served"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	ctrlmanager "sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// configureLogging sends controller-runtime's log output to klog.
//
// Until a logger is set, controller-runtime discards every line its manager
// and reconcilers write, and it says so only once, after thirty seconds, with
// a stack trace. A reconciler whose errors go nowhere can't be diagnosed, and
// every defect found in this controller so far was found by reading a log.
func configureLogging() {
	ctrl.SetLogger(klog.Background())
}

// restConfig builds the client configuration for the API server, with
// client-go's own rate limiter turned off. The kubeconfig argument is the path
// from the --kubeconfig flag, and an empty path means the in-cluster
// configuration. It returns an error when the configuration can't be built.
//
// clientcmd leaves QPS at 0, which client-go reads as 5 requests a second
// with a burst of 10. The bootstrap webhook reads an ObjectStore for every
// archiving Cluster within its 15 second timeout and fails closed, so that
// limit made every Cluster create time out on a cluster with a few dozen
// databases. A negative QPS turns the limiter off and leaves the API server's
// priority and fairness to share out requests, which is what ctrl.GetConfig
// does too.
func restConfig(kubeconfig string) (*rest.Config, error) {
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return nil, err
	}
	if config.QPS == 0 {
		config.QPS = -1
	}
	return config, nil
}

// BootstrapWebhook says where the admission webhook for CloudNativePG
// Clusters listens. CertDir is the directory holding its serving certificate
// as tls.crt and tls.key, and Port is the port it listens on. An empty CertDir
// leaves the webhook unregistered, which is how the binary runs in a cluster
// that has not installed the webhook.
type BootstrapWebhook struct {
	CertDir string
	Port    int
}

// runScheme returns the scheme of the run manager: every typed kind the
// runs, the scheduler and the populator's orphan reconciler read or write.
// It returns an error when a group fails to register.
//
// A kind missing here makes every read of it fail at run time, so each kind
// a reconciler reads through its typed client must be listed. batch/v1 is
// here for the Job of a restore mover the run has stopped, which a RestoreRun
// waits for before it gives the app back. autoscaling/v1 is here for the
// Scale quiesce reads and writes through a workload's scale subresource.
func runScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":                corev1.AddToScheme,
		"apps":                appsv1.AddToScheme,
		"autoscaling":         autoscalingv1.AddToScheme,
		"batch":               batchv1.AddToScheme,
		"coordination.k8s.io": coordinationv1.AddToScheme,
		"volsync":             volsyncv1alpha1.AddToScheme,
		"backup.wlz.li":       backupv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, fmt.Errorf("register the %s types: %w", name, err)
		}
	}
	return scheme, nil
}

// RunOptions carries the command line settings the run controllers start
// with. main fills it from its flags and hands it to startRunControllers.
type RunOptions struct {
	// Kubeconfig is the path from the --kubeconfig flag. Empty means the
	// in-cluster configuration.
	Kubeconfig string
	// Namespace is the controller namespace from the --namespace flag,
	// where the populator keeps its prime claims, Secret copies and restore
	// Jobs. populator.OrphanReconciler cleans up there
	// after a claim whose VolumeRestore is gone.
	Namespace string
	// MetricsAddr is the address where the manager serves the scheduler's
	// metrics. The populator library serves its own registry on another
	// port, and nothing else can register metrics into that one.
	MetricsAddr string
	// HealthAddr is the address where the manager serves /healthz and
	// /readyz for the Deployment's probes. "0" serves neither.
	HealthAddr string
	// Hook says where the bootstrap webhook listens. An empty Hook.CertDir
	// serves no webhook.
	Hook BootstrapWebhook
	// RestoreImage is the image from the required --restore-image flag,
	// which the RestoreRun reconciler's restore Jobs run restic in.
	RestoreImage string
	// Paused is true when the command line sets --pause. New BackupRuns,
	// RestoreRuns and restores of VolumeRestore claims then wait, and work
	// in progress goes on to its end. The value changes only with a restart
	// of the pod.
	Paused bool
}

// startRunControllers starts the controller-runtime manager that reconciles
// BackupRun and RestoreRun, finishes the cleanup of claims whose VolumeRestore
// is gone, runs the namespace scheduler, and serves the
// bootstrap webhook when one is configured. It returns as soon as the manager
// is starting, and the manager keeps running in a goroutine.
//
// Parameters:
//   - ctx stops the manager when it is cancelled. main passes a context it
//     cancels once the populator library returns. The manager doesn't use
//     ctrl.SetupSignalHandler, because the populator library installs the
//     process's only signal handler and a second one on the same channel
//     panics.
//   - options holds the settings from the command line (see RunOptions).
//   - fail is called, from the manager's goroutine, when the manager stops
//     while the context is still live, with the error it stopped on. main
//     passes exitOnFailure, so the kubelet restarts the pod. A manager that
//     fails to sync its caches, for one, would otherwise leave the process
//     running with no webhook server and no reconcilers.
//
// It returns an error when the client configuration can't be built, a scheme
// fails to register, or the manager or one of its controllers can't be set
// up. An error from a manager stopped by the context is only logged.
func startRunControllers(ctx context.Context, options RunOptions, fail func(error)) error {
	manager, err := newRunManager(options)
	if err != nil {
		return err
	}
	if options.Hook.CertDir != "" {
		registerBootstrapWebhook(manager, options.Hook.Port)
	}
	if err := addProbes(manager, options.Hook); err != nil {
		return err
	}
	if err := addRunControllers(manager, options); err != nil {
		return err
	}
	go runManager(ctx, manager, fail)
	klog.Infof("starting backup-controller: reconciling %s BackupRun and RestoreRun, scheduling namespaces", backupv1alpha1.GroupVersion.String())
	return nil
}

// newRunManager builds the controller-runtime manager of the run controllers.
//
// Parameters:
//   - options holds the settings from the command line. newRunManager uses
//     the kubeconfig, the metrics and health addresses and the webhook.
//
// It returns the manager. It returns an error when the client configuration
// cannot be built, a scheme fails to register, or the manager cannot be made.
//
// It sends the log of controller-runtime to klog before it makes the manager.
func newRunManager(options RunOptions) (ctrl.Manager, error) {
	config, err := restConfig(options.Kubeconfig)
	if err != nil {
		return nil, fmt.Errorf("build client configuration: %w", err)
	}

	configureLogging()

	scheme, err := runScheme()
	if err != nil {
		return nil, err
	}

	manager, err := ctrl.NewManager(config, managerOptions(scheme, options.MetricsAddr, options.HealthAddr, options.Hook))
	if err != nil {
		return nil, fmt.Errorf("create the manager: %w", err)
	}
	return manager, nil
}

// registerBootstrapWebhook registers the bootstrap webhook on the webhook
// server of the manager.
//
// Parameters:
//   - manager is the run manager. Its options must have a webhook server.
//   - port is the port of the webhook server, for the log line.
//
// The webhook reads through the uncached API reader, so its reads of
// Secrets and ObjectStores need only the get verb. The manager's
// cached client would need list and watch on every Secret in the
// cluster, and would hold all of them in memory for the life of the
// process.
func registerBootstrapWebhook(manager ctrl.Manager, port int) {
	decider := &bootstrap.Decider{
		Client:    manager.GetAPIReader(),
		Mapper:    manager.GetRESTMapper(),
		Prober:    bootstrap.S3Prober{},
		Snapshots: restic.S3Lister{},
	}
	manager.GetWebhookServer().Register(
		bootstrap.WebhookPath,
		&admission.Webhook{Handler: decider},
	)
	klog.Infof("serving the bootstrap webhook on :%d%s", port, bootstrap.WebhookPath)
}

// addProbes adds the /healthz and /readyz checks to the manager.
//
// Parameters:
//   - manager is the run manager.
//   - hook says where the bootstrap webhook listens. An empty hook.CertDir
//     means that the manager has no webhook server.
//
// It returns an error when the manager does not accept a check.
//
// /healthz answers while the process runs. /readyz waits for the webhook
// server to accept TLS connections, so the webhook's Service sends the
// API server to a pod only once it can answer. Without a webhook there is
// nothing to wait for.
func addProbes(manager ctrl.Manager, hook BootstrapWebhook) error {
	if err := manager.AddHealthzCheck("ping", healthz.Ping); err != nil {
		return fmt.Errorf("add the health check: %w", err)
	}
	ready := healthz.Ping
	if hook.CertDir != "" {
		ready = manager.GetWebhookServer().StartedChecker()
	}
	if err := manager.AddReadyzCheck("webhook", ready); err != nil {
		return fmt.Errorf("add the readiness check: %w", err)
	}
	return nil
}

// addRunControllers adds the served version checks, the BackupRun and
// RestoreRun reconcilers, the scheduler and the populator's orphan
// reconciler to the manager.
//
// Parameters:
//   - manager is the run manager.
//   - options holds the settings from the command line. addRunControllers
//     uses the namespace and the restore image.
//
// It returns an error when the manager does not accept a runnable or a
// controller.
//
// An incompatible VolSync ends every BackupRun that touches VolSync
// objects with reason VolSyncUnsupported. The VolSync requests of a RestoreRun
// then fail, and the run retries them. A Cluster the bootstrap
// webhook would not see created again ends or holds every restore of a
// database with reason ClusterVersionUnsupported. Both checks also run
// once when the manager starts, so the log says it before any run does,
// and the startup warms the mapper for the bootstrap webhook (see
// bootstrap.Warm).
func addRunControllers(manager ctrl.Manager, options RunOptions) error {
	if err := manager.Add(ctrlmanager.RunnableFunc(func(context.Context) error {
		checkServedVersions(manager.GetRESTMapper())
		return nil
	})); err != nil {
		return fmt.Errorf("add the served version checks: %w", err)
	}

	reader := manager.GetAPIReader()
	recorder := manager.GetEventRecorder("backup-controller")
	backups := &runs.BackupRunReconciler{Client: manager.GetClient(), Reader: reader, Snapshots: restic.S3Lister{}, Retimer: restic.S3Lister{}, Recorder: recorder, Paused: options.Paused}
	if err := backups.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the BackupRun controller: %w", err)
	}
	restores := &runs.RestoreRunReconciler{Client: manager.GetClient(), Reader: reader, Snapshots: restic.S3Lister{}, Prober: bootstrap.S3Prober{}, Recorder: recorder, RestoreImage: options.RestoreImage, Paused: options.Paused}
	if err := restores.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the RestoreRun controller: %w", err)
	}
	if err := (&runs.Scheduler{Client: manager.GetClient(), Reader: reader, Recorder: recorder, Paused: options.Paused}).SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the scheduler: %w", err)
	}

	orphans := orphanReconciler(manager.GetClient(), reader, recorder, options.Namespace)
	if err := orphans.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the populator orphan controller: %w", err)
	}
	return nil
}

// runManager runs the manager until main cancels ctx or the manager stops.
//
// Parameters:
//   - ctx stops the manager when main cancels it.
//   - manager is the run manager.
//   - fail gets the error when the manager stops while ctx is still live.
//
// If main cancelled ctx, then the process shuts down and runManager only
// logs an error from the manager. A manager that returns no error while ctx
// is live still gets an error for fail.
func runManager(ctx context.Context, manager ctrl.Manager, fail func(error)) {
	err := manager.Start(ctx)
	if ctx.Err() != nil {
		if err != nil {
			klog.Errorf("the run controllers stopped: %v", err)
		}
		return
	}
	if err == nil {
		err = errors.New("the manager returned while its context was live")
	}
	fail(err)
}

// managerOptions returns the options of the controller-runtime manager
// startRunControllers builds.
//
// Parameters:
//   - scheme is the run scheme (see runScheme).
//   - metricsAddr and healthAddr are the addresses of the metrics and probe
//     servers, as startRunControllers takes them.
//   - hook says where the bootstrap webhook listens; an empty hook.CertDir
//     adds no webhook server.
//
// The manager's client is built with clientOptions, so every write the
// reconcilers send asks for strict field validation. The manager runs no
// leader election: the Deployment runs one replica.
func managerOptions(scheme *runtime.Scheme, metricsAddr, healthAddr string, hook BootstrapWebhook) ctrl.Options {
	skipNameValidation := true
	options := ctrl.Options{
		Scheme:                 scheme,
		Client:                 clientOptions(scheme),
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: healthAddr,
		LeaderElection:         false,
		// controller-runtime refuses a controller name it has seen before in
		// the process. The binary builds one manager, so its names are unique
		// already, and the tests build several managers in one process.
		Controller: ctrlconfig.Controller{SkipNameValidation: &skipNameValidation},
	}
	if hook.CertDir != "" {
		options.WebhookServer = webhook.NewServer(webhook.Options{
			CertDir: hook.CertDir,
			Port:    hook.Port,
		})
	}
	return options
}

// checkServedVersions logs, once at startup, each served version the
// controller can't work with, and warms mapper for the bootstrap webhook.
//
// Parameters:
//   - mapper is the manager's RESTMapper, which the reconcilers and the
//     webhook share.
//
// It logs the error of runs.CheckServedVersions when a kind is not served
// at the version the controller uses, so the log says it before any run
// does. It then calls bootstrap.Warm, so the webhook's first request
// finds postgresql.cnpg.io and barmancloud.cnpg.io cached, and logs a lookup
// that failed; the webhook then looks the group up on its first request,
// within its budget.
func checkServedVersions(mapper meta.RESTMapper) {
	if err := runs.CheckServedVersions(mapper); err != nil {
		klog.Errorf("a kind this controller uses is not served at the version it uses: %v", err)
	}
	if err := bootstrap.Warm(mapper); err != nil {
		klog.Errorf("could not look up the served versions the bootstrap webhook reads; its first request looks them up again: %v", err)
	}
}

// orphanReconciler returns the populator's orphan reconciler over the
// manager's client and uncached reader.
//
// Parameters:
//   - c is the manager's client, which the reconciler writes through.
//   - reader is the manager's uncached API reader.
//   - recorder writes the reconciler's events.
//   - namespace is the controller namespace, where the reconciler cleans up.
//
// The reconciler reads restore Jobs and takes a NotFound for one that is
// gone. served.Client and served.Reader wrap both, so a 404 for a version the
// API server no longer serves is an error it retries, never a Job that is
// gone.
func orphanReconciler(c client.Client, reader client.Reader, recorder events.EventRecorder, namespace string) *populator.OrphanReconciler {
	wrapped := served.Client(c)
	return &populator.OrphanReconciler{Client: wrapped, Reader: served.Reader(reader, wrapped), Recorder: recorder, Namespace: namespace}
}
