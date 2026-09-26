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
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
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
// waits for before it gives the app back.
func runScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":                corev1.AddToScheme,
		"apps":                appsv1.AddToScheme,
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
//   - kubeconfig is the path from the --kubeconfig flag. Empty means the
//     in-cluster configuration.
//   - namespace is the controller namespace from the --namespace flag, where
//     the populator keeps its prime claims, Secret copies and
//     ReplicationDestinations. populator.OrphanReconciler cleans up there
//     after a claim whose VolumeRestore is gone.
//   - metricsAddr is the address where the manager serves the scheduler's
//     metrics. The populator library serves its own registry on another port,
//     and nothing else can register metrics into that one.
//   - healthAddr is the address where the manager serves /healthz and
//     /readyz for the Deployment's probes. "0" serves neither.
//   - hook says where the bootstrap webhook listens. An empty hook.CertDir
//     serves no webhook.
//   - fail is called, from the manager's goroutine, when the manager stops
//     while the context is still live, with the error it stopped on. main
//     passes exitOnFailure, so the kubelet restarts the pod. A manager that
//     fails to sync its caches, for one, would otherwise leave the process
//     running with no webhook server and no reconcilers.
//
// It returns an error when the client configuration can't be built, a scheme
// fails to register, or the manager or one of its controllers can't be set
// up. An error from a manager stopped by the context is only logged.
func startRunControllers(ctx context.Context, kubeconfig, namespace, metricsAddr, healthAddr string, hook BootstrapWebhook, fail func(error)) error {
	config, err := restConfig(kubeconfig)
	if err != nil {
		return fmt.Errorf("build client configuration: %w", err)
	}

	configureLogging()

	scheme, err := runScheme()
	if err != nil {
		return err
	}

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

	manager, err := ctrl.NewManager(config, options)
	if err != nil {
		return fmt.Errorf("create the manager: %w", err)
	}

	if hook.CertDir != "" {
		// The webhook reads through the uncached API reader, so its reads of
		// Secrets and ObjectStores need only the get verb. The manager's
		// cached client would need list and watch on every Secret in the
		// cluster, and would hold all of them in memory for the life of the
		// process.
		decider := &bootstrap.Decider{
			Client: manager.GetAPIReader(),
			Mapper: manager.GetRESTMapper(),
			Prober: bootstrap.S3Prober{},
		}
		manager.GetWebhookServer().Register(
			bootstrap.WebhookPath,
			&admission.Webhook{Handler: decider},
		)
		klog.Infof("serving the bootstrap webhook on :%d%s", hook.Port, bootstrap.WebhookPath)
	}

	// /healthz answers while the process runs. /readyz waits for the webhook
	// server to accept TLS connections, so the webhook's Service sends the
	// API server to a pod only once it can answer. Without a webhook there is
	// nothing to wait for.
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

	// An incompatible VolSync ends or holds every run that touches VolSync
	// objects with reason VolSyncUnsupported, and a Cluster the bootstrap
	// webhook would not see created again ends or holds every restore of a
	// database with reason ClusterVersionUnsupported. Both checks also run
	// once when the manager starts, so the log says it before any run does, and
	// the startup warms the mapper for the bootstrap webhook (see
	// bootstrap.Warm).
	if err := manager.Add(ctrlmanager.RunnableFunc(func(context.Context) error {
		checkServedVersions(manager.GetRESTMapper())
		return nil
	})); err != nil {
		return fmt.Errorf("add the served version checks: %w", err)
	}

	reader := manager.GetAPIReader()
	recorder := manager.GetEventRecorder("backup-controller")
	backups := &runs.BackupRunReconciler{Client: manager.GetClient(), Reader: reader, Snapshots: restic.S3Lister{}, Retimer: restic.S3Lister{}, Recorder: recorder}
	if err := backups.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the BackupRun controller: %w", err)
	}
	restores := &runs.RestoreRunReconciler{Client: manager.GetClient(), Reader: reader, Snapshots: restic.S3Lister{}, Prober: bootstrap.S3Prober{}, Recorder: recorder}
	if err := restores.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the RestoreRun controller: %w", err)
	}
	if err := (&runs.Scheduler{Client: manager.GetClient(), Reader: reader, Recorder: recorder}).SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the scheduler: %w", err)
	}

	// The orphan reconciler reads ReplicationDestinations and takes a
	// NotFound for one that is gone; served.Client makes a 404 for a VolSync
	// version no longer served an error it retries instead.
	orphanClient := served.Client(manager.GetClient())
	orphans := &populator.OrphanReconciler{Client: orphanClient, Reader: served.Reader(reader, orphanClient), Recorder: recorder, Namespace: namespace}
	if err := orphans.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the populator orphan controller: %w", err)
	}

	go func() {
		err := manager.Start(ctx)
		if ctx.Err() != nil {
			// main cancelled the context, so the process is shutting down
			// and the manager stopping is expected.
			if err != nil {
				klog.Errorf("the run controllers stopped: %v", err)
			}
			return
		}
		if err == nil {
			err = errors.New("the manager returned while its context was live")
		}
		fail(err)
	}()

	klog.Infof("starting backup-controller: reconciling %s BackupRun and RestoreRun, scheduling namespaces", backupv1alpha1.GroupVersion.String())
	return nil
}

// checkServedVersions logs, once at startup, each served version the
// controller can't work with, and warms mapper for the bootstrap webhook.
//
// Parameters:
//   - mapper is the manager's RESTMapper, which the reconcilers and the
//     webhook share.
//
// It logs the message of runs.VolSyncUnsupported and of
// runs.ClusterWebhookBlind when either reports, so the log says it before
// any run does. It then calls bootstrap.Warm, so the webhook's first request
// finds postgresql.cnpg.io and barmancloud.cnpg.io cached, and logs a lookup
// that failed; the webhook then looks the group up on its first request,
// within its budget.
func checkServedVersions(mapper meta.RESTMapper) {
	if message := runs.VolSyncUnsupported(mapper); message != "" {
		klog.Errorf("VolSync is not served at the version this controller uses: %s", message)
	}
	if message := runs.ClusterWebhookBlind(mapper); message != "" {
		klog.Errorf("the bootstrap webhook would not see a Cluster created: %s", message)
	}
	if err := bootstrap.Warm(mapper); err != nil {
		klog.Errorf("could not look up the served versions the bootstrap webhook reads; its first request looks them up again: %v", err)
	}
}
