// Package cli is the kubectl-backup plugin: it turns a command line such as
//
//	kubectl backup run before-the-move -n tracearr --all
//
// into a BackupRun or a RestoreRun and creates it. The plugin checks only what
// it needs to build the object; the API server's validation rules decide
// whether the combination of fields is allowed, so their messages reach the
// user unchanged and the rules exist in one place.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/spf13/pflag"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

const usage = `kubectl backup creates BackupRuns and RestoreRuns.

Usage:
  kubectl backup run NAME [flags]       create a BackupRun
  kubectl backup restore NAME [flags]   create a RestoreRun
  kubectl backup version                print the plugin's version

Run "kubectl backup run --help" or "kubectl backup restore --help" for the
flags of each.
`

// Main runs the plugin with args, the command line without the program name,
// and returns the process exit code. version is printed by the version
// command.
func Main(args []string, version string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		_, _ = fmt.Fprint(stderr, usage)
		return 2
	}
	var err error
	switch args[0] {
	case "run":
		err = create(args[1:], "run", stdout, stderr, func(fs *pflag.FlagSet) builder { return backupFlags(fs) })
	case "restore":
		err = create(args[1:], "restore", stdout, stderr, func(fs *pflag.FlagSet) builder { return restoreFlags(fs) })
	case "version":
		_, _ = fmt.Fprintln(stdout, version)
		return 0
	case "-h", "--help", "help":
		_, _ = fmt.Fprint(stdout, usage)
		return 0
	default:
		_, _ = fmt.Fprintf(stderr, "unknown command %q\n\n%s", args[0], usage)
		return 2
	}
	switch {
	case errors.Is(err, pflag.ErrHelp):
		return 0
	case errors.Is(err, errUsage):
		return 2
	case err != nil:
		_, _ = fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	return 0
}

// errUsage marks a command line the plugin could not parse. The parser has
// already printed what was wrong and the flags, so Main prints nothing more.
var errUsage = errors.New("usage")

// builder turns the parsed flags into the object to create. The caller sets
// the namespace afterwards, once it knows it.
type builder func(name string, fs *pflag.FlagSet) (client.Object, error)

// connection holds the flags every command shares.
type connection struct {
	namespace  string
	kubeconfig string
	context    string
	dryRun     bool
}

func (c *connection) register(fs *pflag.FlagSet) {
	fs.StringVarP(&c.namespace, "namespace", "n", "", "namespace to create the run in; the kubeconfig context's namespace when omitted")
	fs.StringVar(&c.kubeconfig, "kubeconfig", "", "path to a kubeconfig; KUBECONFIG or ~/.kube/config when omitted")
	fs.StringVar(&c.context, "context", "", "kubeconfig context to use; the current context when omitted")
	fs.BoolVar(&c.dryRun, "dry-run", false, "send the run to the API server as a dry run and print it as YAML; nothing is created")
}

// parse reads the command line of one command and builds its object. The
// object's namespace is the --namespace flag, empty when the flag is omitted.
func parse(args []string, command string, stderr io.Writer, flags func(*pflag.FlagSet) builder) (client.Object, connection, error) {
	fs := pflag.NewFlagSet("kubectl backup "+command, pflag.ContinueOnError)
	fs.SetOutput(stderr)
	fs.Usage = func() {
		_, _ = fmt.Fprintf(stderr, "Usage: kubectl backup %s NAME [flags]\n\nFlags:\n%s", command, fs.FlagUsages())
	}
	var conn connection
	conn.register(fs)
	build := flags(fs)
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, pflag.ErrHelp) {
			return nil, conn, err
		}
		// With ContinueOnError, pflag returns the error without printing it.
		_, _ = fmt.Fprintf(stderr, "%v\n", err)
		fs.Usage()
		return nil, conn, errUsage
	}
	if fs.NArg() != 1 {
		_, _ = fmt.Fprintf(stderr, "kubectl backup %s needs exactly one NAME, got %d\n", command, fs.NArg())
		fs.Usage()
		return nil, conn, errUsage
	}
	obj, err := build(fs.Arg(0), fs)
	if err != nil {
		return nil, conn, err
	}
	obj.SetNamespace(conn.namespace)
	return obj, conn, nil
}

// create parses the command line of one command, builds its object and
// creates it, or prints it when --dry-run is set.
func create(args []string, command string, stdout, stderr io.Writer, flags func(*pflag.FlagSet) builder) error {
	obj, conn, err := parse(args, command, stderr, flags)
	if err != nil {
		return err
	}

	loader := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: conn.kubeconfig, Precedence: clientcmd.NewDefaultClientConfigLoadingRules().Precedence},
		&clientcmd.ConfigOverrides{CurrentContext: conn.context},
	)
	if obj.GetNamespace() == "" {
		ns, _, err := loader.Namespace()
		if err != nil {
			return fmt.Errorf("read the namespace from the kubeconfig: %w", err)
		}
		obj.SetNamespace(ns)
	}

	cfg, err := loader.ClientConfig()
	if err != nil {
		return fmt.Errorf("load the kubeconfig: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := backupv1alpha1.AddToScheme(scheme); err != nil {
		return fmt.Errorf("register the backup.wlz.li types: %w", err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		return fmt.Errorf("build the client: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var opts []client.CreateOption
	if conn.dryRun {
		opts = append(opts, client.DryRunAll)
	}
	if err := c.Create(ctx, obj, opts...); err != nil {
		return err
	}

	if conn.dryRun {
		obj.SetManagedFields(nil)
		out, err := yaml.Marshal(obj)
		if err != nil {
			return fmt.Errorf("print the run: %w", err)
		}
		_, err = stdout.Write(out)
		return err
	}
	kind := strings.ToLower(obj.GetObjectKind().GroupVersionKind().Kind)
	_, err = fmt.Fprintf(stdout, "%s.%s/%s created\n", kind, backupv1alpha1.GroupVersion.Group, obj.GetName())
	return err
}

// common holds the flags both kinds of run share beyond the connection.
type common struct {
	timeout time.Duration
	ttl     int32
}

func (c *common) register(fs *pflag.FlagSet) {
	fs.DurationVar(&c.timeout, "timeout", 0, "how long the run may work once admitted, such as 2h; the run's default when omitted")
	fs.Int32Var(&c.ttl, "ttl", 0, "seconds after the run finishes until the controller deletes it; kept when omitted")
}

func (c *common) apply(fs *pflag.FlagSet, timeout **metav1.Duration, ttl **int32) {
	if fs.Changed("timeout") {
		*timeout = &metav1.Duration{Duration: c.timeout}
	}
	if fs.Changed("ttl") {
		v := c.ttl
		*ttl = &v
	}
}

// backupFlags registers the BackupRun flags on fs and returns the builder that
// reads them.
func backupFlags(fs *pflag.FlagSet) builder {
	var (
		all      bool
		source   string
		database string
		shared   common
	)
	fs.BoolVar(&all, "all", false, "back up every claim and Cluster in the namespace marked backup.wlz.li/enabled")
	fs.StringVar(&source, "source", "", "name of the one claim to back up")
	fs.StringVar(&database, "database", "", "name of the one CloudNativePG Cluster to take a base backup of")
	shared.register(fs)

	return func(name string, fs *pflag.FlagSet) (client.Object, error) {
		run := &backupv1alpha1.BackupRun{
			TypeMeta:   metav1.TypeMeta{APIVersion: backupv1alpha1.GroupVersion.String(), Kind: "BackupRun"},
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec:       backupv1alpha1.BackupRunSpec{All: all, Source: source, Database: database},
		}
		shared.apply(fs, &run.Spec.Timeout, &run.Spec.TTLSecondsAfterFinished)
		return run, nil
	}
}

// restoreFlags registers the RestoreRun flags on fs and returns the builder
// that reads them.
func restoreFlags(fs *pflag.FlagSet) builder {
	var (
		all        bool
		claim      string
		repository string
		database   string
		into       string
		intoSize   string
		asOf       string
		previous   int32
		syncDB     bool
		pause      []string
		shared     common
	)
	fs.BoolVar(&all, "all", false, "restore every claim and Cluster in the namespace marked backup.wlz.li/enabled")
	fs.StringVar(&claim, "claim", "", "name of the one claim to restore, in place unless --into is set")
	fs.StringVar(&repository, "repository", "", "name of a restic repository Secret to restore from; needs --into")
	fs.StringVar(&database, "database", "", "name of the one CloudNativePG Cluster to restore")
	fs.StringVar(&into, "into", "", "name of a new claim to create and fill; the source claim stays untouched")
	fs.StringVar(&intoSize, "into-size", "", "storage request of the claim --into creates, such as 10Gi; the source claim's size when omitted")
	fs.StringVar(&asOf, "as-of", "", "moment to restore to, as an RFC 3339 time such as 2026-10-11T04:00:00Z; the newest backup when omitted")
	fs.Int32Var(&previous, "previous", 0, "number of snapshots to step back from the one otherwise selected; one claim only")
	fs.BoolVar(&syncDB, "sync-database-to-volume", false, "restore volumes and databases to one paused moment; needs --all")
	fs.StringArrayVar(&pause, "pause", nil, "workload to stop before the restore, as deployment/NAME or statefulset/NAME; repeat for more")
	shared.register(fs)

	return func(name string, fs *pflag.FlagSet) (client.Object, error) {
		run := &backupv1alpha1.RestoreRun{
			TypeMeta:   metav1.TypeMeta{APIVersion: backupv1alpha1.GroupVersion.String(), Kind: "RestoreRun"},
			ObjectMeta: metav1.ObjectMeta{Name: name},
			Spec: backupv1alpha1.RestoreRunSpec{
				All:                  all,
				Claim:                claim,
				Repository:           repository,
				Database:             database,
				Into:                 into,
				SyncDatabaseToVolume: syncDB,
			},
		}
		if fs.Changed("into-size") {
			q, err := resource.ParseQuantity(intoSize)
			if err != nil {
				return nil, fmt.Errorf("--into-size %q is not a quantity such as 10Gi: %w", intoSize, err)
			}
			run.Spec.IntoSize = &q
		}
		if fs.Changed("as-of") {
			v := asOf
			run.Spec.RestoreAsOf = &v
		}
		if fs.Changed("previous") {
			v := previous
			run.Spec.Previous = &v
		}
		for _, p := range pause {
			ref, err := workloadRef(p)
			if err != nil {
				return nil, err
			}
			run.Spec.PauseDuringRestore = append(run.Spec.PauseDuringRestore, ref)
		}
		shared.apply(fs, &run.Spec.Timeout, &run.Spec.TTLSecondsAfterFinished)
		return run, nil
	}
}

// workloadRef parses KIND/NAME with the kind spelled the way kubectl accepts
// it: deployment, deployments or deploy, and statefulset, statefulsets or sts.
func workloadRef(s string) (backupv1alpha1.WorkloadRef, error) {
	kind, name, ok := strings.Cut(s, "/")
	if !ok || name == "" {
		return backupv1alpha1.WorkloadRef{}, fmt.Errorf("--pause %q is not KIND/NAME, such as deployment/tracearr", s)
	}
	switch strings.ToLower(kind) {
	case "deployment", "deployments", "deploy":
		return backupv1alpha1.WorkloadRef{Kind: "Deployment", Name: name}, nil
	case "statefulset", "statefulsets", "sts":
		return backupv1alpha1.WorkloadRef{Kind: "StatefulSet", Name: name}, nil
	}
	return backupv1alpha1.WorkloadRef{}, fmt.Errorf("--pause %q names kind %q; use deployment/NAME or statefulset/NAME", s, kind)
}
