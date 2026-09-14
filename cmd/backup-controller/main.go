// Command backup-controller populates PersistentVolumeClaims from a restic
// repository. It binds the callbacks in internal/populator to
// lib-volume-populator's provider-function mode, so a claim naming a
// VolumeRestore in its dataSourceRef is filled by a VolSync mover rather than
// by a populator pod this binary would have to build.
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
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// version is stamped at build time with -ldflags "-X main.version=...". The
// Dockerfile passes its VERSION build argument through to it.
var version = "dev"

// prefix names the annotation and finalizer domain the library writes onto the
// claims it manages. It is the API group, so every mark this controller leaves
// on an object is traceable to this project.
const prefix = "backup.wlz.li"

func main() {
	var (
		kubeconfig  = kubeconfigFlag(flag.CommandLine)
		namespace   = flag.String("namespace", "backup-system", "namespace the prime claim and the ReplicationDestination live in")
		metricsAddr = flag.String("metrics-addr", ":8080", "address the metrics listener binds")
		metricsPath = flag.String("metrics-path", "/metrics", "path the metrics listener serves")
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
	callbacks := populator.New(operations, *namespace)

	// BackupRun and RestoreRun are reconciled by a controller-runtime manager
	// of this binary's own, because the populator library drives only the one
	// kind it is given. It is started here and cancelled after the library
	// returns, which is the only shutdown signal this process gets.
	runs, stopRuns := context.WithCancel(context.Background())
	defer stopRuns()
	if err := startRunControllers(runs, kubeconfig()); err != nil {
		klog.Errorf("failed to start the run controllers: %v", err)
		os.Exit(1)
	}

	// The library registers its own SIGTERM and interrupt handler and closes
	// the stop channel itself, so this binary installs no second handler: two
	// closers race on one channel and the loser panics. RunControllerWithConfig
	// returns once the controller has stopped, and returning from main here is
	// the clean exit.
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

// newClientOperations builds the cluster operations the callbacks run through,
// backed by a client that knows this project's types and VolSync's.
func newClientOperations(kubeconfig string) (populator.Operations, error) {
	restConfig, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
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

	kubeClient, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("create client: %w", err)
	}
	klog.Infof("starting backup-controller: watching %s VolumeRestore", backupv1alpha1.GroupVersion.String())
	return &clientOperations{client: kubeClient}, nil
}
