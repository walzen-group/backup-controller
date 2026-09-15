package main

import (
	"context"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/runs"
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
// RestoreRun, serves the bootstrap webhook when one is configured, and returns
// as soon as it is running.
//
// The manager is given a context of the caller's rather than
// ctrl.SetupSignalHandler, because the populator library installs the process's
// only signal handler and a second one on the same channel panics. Cancelling
// that context after the library returns is what stops this manager.
func startRunControllers(ctx context.Context, kubeconfig string, hook BootstrapWebhook) error {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("build client configuration: %w", err)
	}

	configureLogging()

	scheme := runtime.NewScheme()
	for name, add := range map[string]func(*runtime.Scheme) error{
		"core":          corev1.AddToScheme,
		"volsync":       volsyncv1alpha1.AddToScheme,
		"backup.wlz.li": backupv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return fmt.Errorf("register the %s types: %w", name, err)
		}
	}

	options := ctrl.Options{
		Scheme: scheme,
		// The library already serves this process's metrics endpoint, so the
		// manager serves none: two listeners on one address and the second
		// fails.
		Metrics:        metricsserver.Options{BindAddress: "0"},
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

	if err := (&runs.BackupRunReconciler{Client: manager.GetClient()}).SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the BackupRun controller: %w", err)
	}
	if err := (&runs.RestoreRunReconciler{Client: manager.GetClient()}).SetupWithManager(manager); err != nil {
		return fmt.Errorf("register the RestoreRun controller: %w", err)
	}

	go func() {
		if err := manager.Start(ctx); err != nil {
			klog.Errorf("the run controllers stopped: %v", err)
		}
	}()

	klog.Infof("starting backup-controller: reconciling %s BackupRun and RestoreRun", backupv1alpha1.GroupVersion.String())
	return nil
}
