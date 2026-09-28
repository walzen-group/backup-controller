package main

import (
	"context"
	"fmt"

	batchv1 "k8s.io/api/batch/v1"

	"github.com/walzen-group/backup-controller/internal/lease"
	coordinationv1 "k8s.io/api/coordination/v1"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/admission"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/runs"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	webhookadmission "sigs.k8s.io/controller-runtime/pkg/webhook/admission"
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

// BootstrapWebhook says where the admission webhook for CloudNativePG
// Clusters listens. CertDir is the directory holding its serving certificate
// as tls.crt and tls.key, and Port is the port it listens on. An empty CertDir
// leaves the webhook unregistered, which is how the binary runs in a cluster
// that has not installed the webhook.
type BootstrapWebhook struct {
	CertDir string
	Port    int
}

// startRunControllers starts the controller-runtime manager that reconciles
// BackupRun and RestoreRun, runs the namespace scheduler, and serves the
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
//   - metricsAddr is the address where the manager serves the scheduler's
//     metrics. The populator library serves its own registry on another port,
//     and nothing else can register metrics into that one.
//   - hook says where the bootstrap webhook listens. An empty hook.CertDir
//     serves no webhook.
//   - paused is the --pause flag: the run controllers start no new run while
//     it is set, and finish the runs that already started work.
//   - restoreImage is the --restore-image flag, the image of the restore
//     Jobs.
//
// It returns an error when the client configuration can't be built, a scheme
// fails to register, or the manager or one of its controllers can't be set
// up. An error from the running manager is only logged.
func startRunControllers(ctx context.Context, kubeconfig, metricsAddr string, hook BootstrapWebhook, paused bool, restoreImage string) error {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("build client configuration: %w", err)
	}

	configureLogging()

	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":          corev1.AddToScheme,
		"coordination":  coordinationv1.AddToScheme,
		"batch":         batchv1.AddToScheme,
		"apps":          appsv1.AddToScheme,
		"volsync":       volsyncv1alpha1.AddToScheme,
		"backup.wlz.li": backupv1alpha1.AddToScheme,
		"kueue":         admission.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return fmt.Errorf("register the %s types: %w", name, err)
		}
	}

	options := ctrl.Options{
		Scheme:         scheme,
		Metrics:        metricsserver.Options{BindAddress: metricsAddr},
		LeaderElection: false,
	}
	if hook.CertDir != "" {
		options.WebhookServer = webhook.NewServer(webhook.Options{
			CertDir: hook.CertDir,
			Port:    hook.Port,
		})
	}

	manager, err := ctrl.NewManager(restConfig, options)
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
			Client:    manager.GetAPIReader(),
			Prober:    bootstrap.S3Prober{},
			Snapshots: restic.S3Lister{},
		}
		manager.GetWebhookServer().Register(
			bootstrap.WebhookPath,
			&webhookadmission.Webhook{Handler: decider},
		)
		klog.Infof("serving the bootstrap webhook on :%d%s", hook.Port, bootstrap.WebhookPath)
	}

	reader := manager.GetAPIReader()
	recorder := manager.GetEventRecorder("backup-controller")
	leases := &lease.Leases{Client: manager.GetClient(), Reader: reader, Alive: runs.RunAlive(reader)}
	if paused {
		klog.Info("paused: new runs wait, runs in progress finish")
	}
	backups := &runs.BackupRunReconciler{Client: manager.GetClient(), Reader: reader, Snapshots: restic.S3Lister{}, Retimer: restic.S3Lister{}, Leases: leases, Paused: paused, Recorder: recorder}
	if err := backups.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the BackupRun controller: %w", err)
	}
	restores := &runs.RestoreRunReconciler{Client: manager.GetClient(), Reader: reader, Snapshots: restic.S3Lister{}, Prober: bootstrap.S3Prober{},
		Leases: leases, RestoreImage: restoreImage, Paused: paused, Recorder: recorder}
	if err := restores.SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the RestoreRun controller: %w", err)
	}
	if err := (&runs.Scheduler{Client: manager.GetClient(), Reader: reader, Paused: paused, Recorder: recorder}).SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the scheduler: %w", err)
	}

	go func() {
		if err := manager.Start(ctx); err != nil {
			klog.Errorf("the run controllers stopped: %v", err)
		}
	}()

	klog.Infof("starting backup-controller: reconciling %s BackupRun and RestoreRun, scheduling namespaces", backupv1alpha1.GroupVersion.String())
	return nil
}
