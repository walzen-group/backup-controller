package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// WebhookPath is the URL path the admission server serves Decider on. The
	// MutatingWebhookConfiguration in deploy/ points at the same path.
	WebhookPath = "/mutate-postgresql-cnpg-io-v1-cluster"

	// PluginName is the name of the Barman Cloud plugin. Archiver looks for it
	// in a Cluster's spec.plugins, and setRecovery writes it into the
	// externalClusters entry it adds.
	PluginName = "barman-cloud.cloudnative-pg.io"

	// SkipCheckAnnotation lets a recovered database archive into the prefix it
	// restored from. CloudNativePG refuses to archive into a non-empty prefix
	// from a freshly bootstrapped database, and every restore is one.
	// setRecovery sets it to "enabled".
	SkipCheckAnnotation = "cnpg.io/skipEmptyWalArchiveCheck"

	// OptOutAnnotation asks for an empty database when a Cluster carries it
	// set to OptOutValue. Without it there is no way to discard a database,
	// because deleting the Cluster would restore it again. Handle admits it
	// only over an empty prefix: discarding a database also means deleting
	// its archive or choosing a new serverName, since CloudNativePG never
	// archives a new database into a prefix that holds WAL.
	OptOutAnnotation = "backup.wlz.li/bootstrap"

	// OptOutValue is the value OptOutAnnotation must have to opt out. The
	// webhook ignores any other value, so a typo can't silently leave a
	// database empty.
	OptOutValue = "initdb"

	// RecoverySource is the name of the externalClusters entry this webhook
	// adds, and the value it writes to spec.bootstrap.recovery.source. The two
	// have to match, and no other component uses the name, so one constant
	// serves both. keepRecovery reads the source back to recognise a
	// Cluster this webhook recovered.
	RecoverySource = "backup-controller"
)

// Decider is the mutating admission webhook for CloudNativePG Clusters. It
// chooses a new Cluster's bootstrap at admission time, and it keeps a
// recovered Cluster valid when Flux applies it again.
type Decider struct {
	// Client reads ObjectStores, Secrets, Clusters and RestoreRuns. The
	// handler only ever reads, so a client.Reader is enough.
	// cmd/backup-controller passes the manager's uncached API reader, which
	// keeps the controller's Secret grant at get. A cached client would need
	// list and watch on every Secret in the cluster and would hold them all
	// in memory.
	Client client.Reader
	// Mapper looks up the versions at which the API server serves Cluster and
	// ObjectStore (see served.Kind). cmd/backup-controller passes the
	// manager's RESTMapper, which caches each lookup, so a request costs a
	// discovery call only when the mapper has not seen the group yet or
	// has just forgotten it (see served.VersionGone). Nil means the
	// RESTMapper of Client, when Client has one; with neither, Handle refuses
	// every create.
	// Handle bounds every lookup by its budget (see boundedMapper), and
	// cmd/backup-controller warms the mapper at startup (see Warm).
	Mapper meta.RESTMapper
	// Prober asks the object store what the database's prefix holds and
	// which base backups exist. cmd/backup-controller passes S3Prober, and
	// the tests pass a stub.
	Prober ArchiveProber
	// Budget is how long Handle may spend on one create. Zero means
	// DefaultBudget. Tests lower it.
	Budget time.Duration
}

// DefaultBudget is how long Handle spends on one create before it refuses
// with what it read so far. The budget starts after the dry-run check and
// covers every Kubernetes read and S3 request Handle makes. deploy/webhook.yaml
// gives the API server's call 15 s; the other 5 s cover TLS, the API server's
// own work and the answer's way back.
const DefaultBudget = 10 * time.Second

// Handle answers one admission request for a Cluster. For a new Cluster whose
// object store already holds a completed base backup, it rewrites the Cluster
// to recover from that backup.
//
// The req argument is the admission request. Its Operation, DryRun, Object and
// OldObject decide what happens:
//   - An update goes to keepRecovery, on a dry run too.
//   - Any operation other than create or update is allowed unchanged.
//   - A dry-run create is allowed unchanged without reading anything.
//   - A create is allowed unchanged when the Cluster has no archiving plugin
//     (see Archiver).
//   - A create is refused when the ObjectStore can't be resolved, when
//     another Cluster anywhere already archives to the same bucket and prefix
//     on any endpoint, or when the Cluster list or the ObjectStore list that
//     the collision check reads fails (see archiveHolder).
//   - A Cluster that carries OptOutAnnotation set to OptOutValue and declares
//     no bootstrap other than initdb is allowed unchanged when nothing exists
//     under its prefix, and refused when anything does, since CloudNativePG
//     would never archive the new database there.
//   - A Cluster that declares a bootstrap other than initdb, such as
//     recovery or pg_basebackup (see declaredBootstrap), is allowed unchanged,
//     unless a RestoreRun is waiting for it. Then it's refused.
//   - A create is refused when a RestoreRun's restoreAsOf or the Cluster's
//     restore-as-of annotation isn't an RFC 3339 time.
//   - A create is refused when a RestoreRun or the restore-as-of annotation
//     asks for a recovery and the store holds no completed base backup, or
//     none finished by the requested moment.
//   - Otherwise, when the store holds a completed base backup, the response
//     patches the Cluster to recover from it (see setRecovery). When a
//     RestoreRun waits for the Cluster, the patch also sets the
//     backup.wlz.li/restore-run annotation to the run's name. With no
//     completed base backup and nothing asking for a recovery, the Cluster is
//     allowed unchanged and starts empty when nothing at all exists under
//     its prefix, and refused when anything does (WAL, or base backups that
//     failed or never finished): CloudNativePG would never archive a new
//     database there, and a recovery has nothing to start from.
//
// Failures to read the store, list Clusters or RestoreRuns, or talk to the
// object store return an HTTP 500 error response, which the API server treats
// as a refusal. A body that isn't a valid Cluster returns an HTTP 400. A
// create gets Budget (DefaultBudget when zero) to decide. When a Kubernetes
// read runs out of it, the HTTP 500 says "the webhook ran out of its <budget>
// budget while <step>" (see readFailed). When the object store survey runs
// out of it, the create is refused with the counts read so far, the last
// backup.info that failed to read, if any, and how to clear the failed
// backups (see surveyFailed).
func (d *Decider) Handle(ctx context.Context, req admission.Request) admission.Response {
	logger := log.FromContext(ctx).WithValues(
		"cluster", fmt.Sprintf("%s/%s", req.Namespace, req.Name),
	)

	if req.Operation == admissionv1.Update {
		return keepRecovery(req)
	}
	if req.Operation != admissionv1.Create {
		return admission.Allowed("not a creation or an update")
	}

	// A dry-run decision is thrown away: the API server never persists a
	// mutating patch from a dry-run request, so choosing a bootstrap here
	// changes nothing.
	//
	// Enforcing here does do something, and it is a deadlock. Flux dry-runs
	// every object in a Kustomization before it applies any of them, so on an
	// app's first deploy this handler is asked about a Cluster whose
	// ObjectStore sits in the same set and does not exist yet. Refusing fails
	// the dry-run, Flux applies nothing, the ObjectStore is never created, and
	// the next reconcile asks the same question and gets the same answer.
	//
	// The real request arrives with DryRun unset and is decided in full.
	if req.DryRun != nil && *req.DryRun {
		return admission.Allowed("dry run")
	}

	// The API server refuses the create after its own timeout with a bare
	// deadline error. The handler stops first, so its refusal says why.
	budget := d.Budget
	if budget == 0 {
		budget = DefaultBudget
	}
	ctx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()
	return d.create(ctx, creation{req: req, logger: logger, budget: budget})
}

// creation holds what Handle knows about one Cluster create while it decides
// the create.
type creation struct {
	// req is the admission request of the create.
	req admission.Request
	// logger logs the decision. It carries the namespace and the name of the
	// Cluster.
	logger logr.Logger
	// budget is the time that Handle gives to the create. readFailed names it.
	budget time.Duration
	// cluster is the Cluster from the request.
	cluster *unstructured.Unstructured
	// method is the bootstrap method that the Cluster declares (see
	// declaredBootstrap).
	method string
	// store and serverName tell where the Cluster archives (see Archiver).
	store, serverName string
	// at is the Location of the archive, from ResolveLocation.
	at Location
}

// create decides a Cluster create that is not a dry run.
//
// Parameters:
//   - ctx carries the deadline of the budget of the create.
//   - c holds the request, the logger and the budget from Handle. create
//     fills the other fields.
//
// It returns the answer that Handle gives (see Handle).
func (d *Decider) create(ctx context.Context, c creation) admission.Response {
	c.cluster = &unstructured.Unstructured{}
	if err := json.Unmarshal(c.req.Object.Raw, c.cluster); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	c.method = declaredBootstrap(c.cluster)

	var found bool
	c.store, c.serverName, found = Archiver(c.cluster)
	if !found {
		c.logger.Info("leaving the Cluster alone", "reason", "it archives nowhere")
		return admission.Allowed("no archiving plugin")
	}

	mapper := d.mapper()
	if mapper == nil {
		return admission.Errored(http.StatusInternalServerError, errors.New("the webhook has no RESTMapper to look up the served versions of Cluster and ObjectStore"))
	}
	// A lookup that runs discovery ends with the budget too.
	mapper = boundedMapper{RESTMapper: mapper, ctx: ctx}

	at, err := ResolveLocation(ctx, d.Client, mapper, c.req.Namespace, c.store, c.serverName)
	if err != nil {
		// The store is named and unreadable. Refusing is the failure that gets
		// noticed; allowing would create an empty database beside a full
		// archive and report success.
		return readFailed(ctx, c.logger, c.budget, fmt.Sprintf("reading the ObjectStore %s/%s and its Secrets", c.req.Namespace, c.store), err)
	}
	c.at = at

	if refusal, refused := d.sharedArchive(ctx, mapper, c); refused {
		return refusal
	}

	// The opt-out asks for an empty database, which only initdb gives, so it
	// changes nothing for a Cluster that declares another bootstrap.
	if c.cluster.GetAnnotations()[OptOutAnnotation] == OptOutValue && c.method == "" {
		return d.optOut(ctx, c)
	}

	run, err := waitingRun(ctx, d.Client, c.req.Namespace, c.req.Name)
	if err != nil {
		return readFailed(ctx, c.logger, c.budget, "listing the RestoreRuns in "+c.req.Namespace, err)
	}

	// A Cluster that already names a bootstrap other than initdb was written
	// that way on purpose: a recovery by the Flux component or a unit's
	// restore input, a pg_basebackup by a replica or a migration. It keeps its
	// own source, unless a RestoreRun is waiting to recover it: then two
	// sources name one database, and neither may win by accident.
	if c.method != "" {
		return keepDeclared(c, run)
	}
	return d.recover(ctx, c, run)
}

// optOut decides the create of a Cluster that carries OptOutAnnotation set to
// OptOutValue and declares no bootstrap other than initdb.
//
// An empty database can archive only into an empty prefix. CloudNativePG
// refuses a prefix that holds WAL, and the skip annotation would let the new
// database overwrite the segments of the old one. So optOut allows the create
// when the prefix is empty and refuses it when the prefix holds anything.
func (d *Decider) optOut(ctx context.Context, c creation) admission.Response {
	archive, err := d.Prober.Survey(ctx, c.at, nil)
	if err != nil {
		return surveyFailed(c.logger, c.at, err)
	}
	if !archive.Empty {
		c.logger.Info("refusing the Cluster", "reason", "opted out over an old archive", "prefix", c.at.ServerPrefix())
		return admission.Denied(fmt.Sprintf(
			"The Cluster asks for an empty database (%s: %s), and s3://%s/%s still holds the archive of an earlier one. CloudNativePG will not archive a new database into a prefix that holds WAL, so this one would never be backed up. To discard the old archive, delete everything under s3://%s/%s and create the Cluster again. To keep it, give this Cluster a serverName that is not %q.",
			OptOutAnnotation, OptOutValue, c.at.Bucket, c.at.ServerPrefix(), c.at.Bucket, c.at.ServerPrefix(), c.serverName,
		))
	}
	c.logger.Info("leaving the Cluster to initdb", "reason", "opted out by annotation, and the prefix is empty")
	return admission.Allowed("opted out")
}

// keepDeclared decides the create of a Cluster that declares a bootstrap
// other than initdb.
//
// Parameters:
//   - c is the create. Its method field names the declared bootstrap.
//   - run is the RestoreRun that waits for the Cluster, or nil.
//
// It refuses the create when a RestoreRun waits for the Cluster, and allows
// the Cluster unchanged when no RestoreRun waits.
func keepDeclared(c creation, run *backupv1alpha1.RestoreRun) admission.Response {
	if run != nil {
		return admission.Denied(fmt.Sprintf(
			"RestoreRun %s is waiting to recover %s/%s, and the Cluster declares its own spec.bootstrap.%s. Remove the declared bootstrap (for a recovery, the terragrunt restore input or the postgres-recovery component), or delete the RestoreRun.",
			run.Name, c.req.Namespace, c.req.Name, c.method,
		))
	}
	c.logger.Info("leaving the Cluster alone", "reason", "it declares its own bootstrap", "method", c.method)
	return admission.Allowed("declares its own bootstrap")
}

// recover decides the create of a Cluster that declares no bootstrap other
// than initdb and did not opt out.
//
// Parameters:
//   - c is the create.
//   - run is the RestoreRun that waits for the Cluster, or nil.
//
// It refuses the create when the recovery target is not an RFC 3339 time, or
// when the target is before the end of the oldest base backup. With no
// completed base backup it returns the answer of withoutBaseBackup. Otherwise
// it returns the answer of recoverFrom, a patch that recovers the Cluster.
func (d *Decider) recover(ctx context.Context, c creation, run *backupv1alpha1.RestoreRun) admission.Response {
	target, source, err := restoreTarget(c.cluster, run)
	if err != nil {
		return admission.Denied(err.Error())
	}

	archive, err := d.Prober.Survey(ctx, c.at, target)
	if err != nil {
		return surveyFailed(c.logger, c.at, err)
	}
	// A target before the oldest base backup's end is one Postgres can never
	// reach. CloudNativePG would keep the Cluster in recovery reporting that
	// no backup matched, so the webhook refuses it here with the reason.
	if archive.Found == nil && archive.Oldest != nil && target != nil {
		return admission.Denied(fmt.Sprintf(
			"%s asks for %s, and no base backup in %s/%s finished by then%s.",
			source, target.Format(time.RFC3339), c.at.Bucket, c.at.BasePrefix(), oldest([]BaseBackup{*archive.Oldest}),
		))
	}
	if archive.Found == nil {
		return withoutBaseBackup(c, archive, run != nil || target != nil, source)
	}
	return recoverFrom(c, run, target, source)
}

// withoutBaseBackup decides the create of a Cluster whose store holds no
// completed base backup.
//
// Parameters:
//   - c is the create.
//   - archive is what Survey found under the prefix of the Cluster.
//   - asked is true when a RestoreRun or the restore-as-of annotation asks for
//     a recovery.
//   - source names what asks for the recovery, for the message.
//
// It refuses the create when something asks for a recovery, and when the
// prefix holds anything. It allows the Cluster unchanged when nothing asks
// for a recovery and the prefix is empty.
func withoutBaseBackup(c creation, archive Archive, asked bool, source string) admission.Response {
	if asked {
		return admission.Denied(fmt.Sprintf(
			"%s asks for a recovery, and %s/%s holds no completed base backup to recover from.",
			source, c.at.Bucket, c.at.BasePrefix(),
		))
	}
	// CloudNativePG refuses to archive a new database into a prefix that
	// holds WAL, so a database started empty over anything at all could
	// never be backed up, and a recovery has no base backup to start
	// from. An empty prefix is the only place initdb is safe.
	if !archive.Empty {
		c.logger.Info("refusing the Cluster", "reason", "an archive with no completed base backup", "prefix", c.at.ServerPrefix())
		return admission.Denied(noDoneBackup(c.at, c.serverName, archive.Backups))
	}
	c.logger.Info("leaving the Cluster to initdb", "reason", "no completed base backup in the store", "prefix", c.at.BasePrefix())
	return admission.Allowed("no base backup")
}

// recoverFrom returns the patch that makes the Cluster of a create recover
// from its store (see setRecovery).
//
// Parameters:
//   - c is the create.
//   - run is the RestoreRun that waits for the Cluster, or nil. When it is
//     set, the patch also sets the backup.wlz.li/restore-run annotation to
//     the name of the run.
//   - target is the moment to recover to, or nil for the end of the archive.
//   - source names what asks for the recovery, for the log.
//
// It returns an HTTP 500 when the Cluster cannot be changed or encoded.
func recoverFrom(c creation, run *backupv1alpha1.RestoreRun, target *time.Time, source string) admission.Response {
	if err := setRecovery(c.cluster, c.store, c.serverName, target); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if run != nil {
		annotations := c.cluster.GetAnnotations()
		annotations[backupv1alpha1.AnnotationRestoreRun] = run.Name
		c.cluster.SetAnnotations(annotations)
	}

	patched, err := json.Marshal(c.cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	c.logger.Info("recovering the Cluster from its object store", "prefix", c.at.BasePrefix(), "target", target, "for", source)
	return admission.PatchResponseFromRaw(c.req.Object.Raw, patched)
}

// mapper returns the RESTMapper the handler looks versions up with: Mapper,
// or else the RESTMapper of Client when Client has one. It returns nil when
// there is neither, and Handle then refuses the create.
func (d *Decider) mapper() meta.RESTMapper {
	if d.Mapper != nil {
		return d.Mapper
	}
	if withMapper, ok := d.Client.(interface{ RESTMapper() meta.RESTMapper }); ok {
		return withMapper.RESTMapper()
	}
	return nil
}
