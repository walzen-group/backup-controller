package main

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
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
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// configureLogging points controller-runtime at klog.
//
// Until a logger is set, controller-runtime discards every line its manager and
// reconcilers produce and says so once, after thirty seconds, with a stack
// trace. Reconcilers whose errors reach nothing cannot be diagnosed, and every
// defect found in this controller so far was found by reading a log.
func configureLogging() {
	ctrl.SetLogger(klog.Background())
}

// BootstrapWebhook is where the Cluster admission webhook listens and finds
// its serving certificate. An empty CertDir leaves the webhook unregistered,
// which is how the binary runs in a cluster that has not installed it.
type BootstrapWebhook struct {
	CertDir string
	Port    int
}

// startRunControllers brings up the manager that reconciles BackupRun and
// RestoreRun, runs the scheduler, serves the bootstrap webhook when one is
// configured, and returns as soon as it is running.
//
// The manager is given a context of the caller's rather than
// ctrl.SetupSignalHandler, because the populator library installs the process's
// only signal handler and a second one on the same channel panics. Cancelling
// that context after the library returns is what stops this manager.
//
// metricsAddr is where the manager serves the scheduler's series. The library
// serves its own registry on another port, and nothing can register into it.
func startRunControllers(ctx context.Context, kubeconfig, metricsAddr string, hook BootstrapWebhook) error {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("build client configuration: %w", err)
	}

	configureLogging()

	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":          corev1.AddToScheme,
		"apps":          appsv1.AddToScheme,
		"volsync":       volsyncv1alpha1.AddToScheme,
		"backup.wlz.li": backupv1alpha1.AddToScheme,
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
		// The uncached reader, so the webhook's Secret and ObjectStore reads
		// need get alone. The manager's cached client would require list and
		// watch on every Secret in the cluster and would hold them in memory
		// for the life of the process.
		decider := &bootstrap.Decider{
			Client: manager.GetAPIReader(),
			Prober: bootstrap.S3Prober{},
		}
		manager.GetWebhookServer().Register(
			bootstrap.WebhookPath,
			&admission.Webhook{Handler: decider},
		)
		klog.Infof("serving the bootstrap webhook on :%d%s", hook.Port, bootstrap.WebhookPath)
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

	go func() {
		if err := manager.Start(ctx); err != nil {
			klog.Errorf("the run controllers stopped: %v", err)
		}
	}()

	klog.Infof("starting backup-controller: reconciling %s BackupRun and RestoreRun, scheduling namespaces", backupv1alpha1.GroupVersion.String())
	return nil
}
