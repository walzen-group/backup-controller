package main

import (
	"errors"
	"flag"
	"strings"
)

// kubeconfigFlag returns a function that reads the --kubeconfig flag from fs.
// It registers the flag only when fs doesn't already have one, and otherwise
// reads the existing flag. main passes flag.CommandLine.
//
// controller-runtime's pkg/client/config registers a --kubeconfig flag on the
// default FlagSet in its init function, so any binary that imports
// controller-runtime has one before main runs. Registering a second one panics
// at startup with "flag redefined: kubeconfig". No unit test catches that,
// because none of them calls main, and v0.2.0 shipped that way and crashlooped
// on the cluster.
//
// Reading whichever flag exists keeps the command line the same no matter who
// registered it, and keeps working if the dependency stops registering one.
func kubeconfigFlag(fs *flag.FlagSet) func() string {
	const name = "kubeconfig"
	if existing := fs.Lookup(name); existing != nil {
		return existing.Value.String
	}
	value := fs.String(name, "", "path to a kubeconfig; empty means the ambient configuration")
	return func() string { return *value }
}

// errNoRestoreImage is the error restoreImageFlag's reader returns when the
// command line sets no --restore-image. main logs it and exits, so the pod
// never runs without the image its restore Jobs need.
var errNoRestoreImage = errors.New("--restore-image is required: pass the image VolSync runs its restic mover in, pinned by digest, so restores run the restic that wrote the backups")

// restoreImageFlag registers the --restore-image flag on a FlagSet and
// returns a function that reads it once the command line is parsed.
//
// Parameters:
//   - fs is the FlagSet to register on. main passes flag.CommandLine; tests
//     pass a FlagSet of their own.
//
// The returned function gives the image, or errNoRestoreImage when the flag
// is missing, empty or only white space.
//
// The flag has no default on purpose. The restore Job runs restic from this
// image, and it has to be the restic that VolSync backs up with; only the
// installer knows which that is, so the controller refuses to start rather
// than restore with an image nobody chose.
func restoreImageFlag(fs *flag.FlagSet) func() (string, error) {
	value := fs.String("restore-image", "", "image the restore Jobs run restic in, the one VolSync runs its restic mover in (required)")
	return func() (string, error) {
		if strings.TrimSpace(*value) == "" {
			return "", errNoRestoreImage
		}
		return *value, nil
	}
}

// runFlags registers the flags the run controllers read, and returns a
// function that builds their RunOptions once the command line is parsed.
//
// Parameters:
//   - fs is the FlagSet to register on. main passes flag.CommandLine; tests
//     pass a FlagSet of their own.
//
// The returned function gives the options, or errNoRestoreImage when the
// command line sets no --restore-image. main passes the kubeconfig and the
// namespace on to the populator library as well, so both parts of the
// process read the same values.
func runFlags(fs *flag.FlagSet) func() (RunOptions, error) {
	kubeconfig := kubeconfigFlag(fs)
	restoreImage := restoreImageFlag(fs)
	namespace := fs.String("namespace", "backup-system", "namespace the populator's prime claims, Secret copies and restore Jobs live in")
	metricsAddr := fs.String("runs-metrics-addr", ":8081", "address the scheduler's metrics listener binds, at /metrics")
	healthAddr := fs.String("health-probe-addr", ":8082", "address serving /healthz and /readyz for the run controllers; 0 serves none")
	webhookCert := fs.String("webhook-cert-dir", "", "directory holding tls.crt and tls.key; empty serves no webhook")
	webhookPort := fs.Int("webhook-port", 9443, "port the admission webhook listens on")
	return func() (RunOptions, error) {
		image, err := restoreImage()
		if err != nil {
			return RunOptions{}, err
		}
		return RunOptions{
			Kubeconfig:   kubeconfig(),
			Namespace:    *namespace,
			MetricsAddr:  *metricsAddr,
			HealthAddr:   *healthAddr,
			Hook:         BootstrapWebhook{CertDir: *webhookCert, Port: *webhookPort},
			RestoreImage: image,
		}, nil
	}
}
