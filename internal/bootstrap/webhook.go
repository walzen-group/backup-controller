package bootstrap

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/go-logr/logr"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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

	// OptOutAnnotation, set to OptOutValue on a Cluster, asks for an empty
	// database. Without it there is no way to discard a database, because
	// deleting the Cluster would restore it again. Handle admits it only over
	// an empty prefix: discarding a database also means deleting its archive
	// or choosing a new serverName, since CloudNativePG never archives a new
	// database into a prefix that holds WAL.
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
	// Prober asks the object store what the database's prefix holds and
	// which base backups exist. cmd/backup-controller passes S3Prober, and
	// the tests pass a stub.
	Prober ArchiveProber
	// Budget is how long Handle may spend on one create. Zero means
	// DefaultBudget. Tests lower it.
	Budget time.Duration
}

// DefaultBudget is how long Handle spends on one create before it refuses
// with what it read so far. deploy/webhook.yaml gives the API server's call
// 15 s; the other 5 s cover TLS, the API server and the Kubernetes reads.
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
//     on any endpoint, or when another Cluster's ObjectStore can't be read
//     for a reason other than NotFound (see archiveHolder).
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
// create gets Budget (DefaultBudget when zero) to decide; when the object
// store survey runs out of it, the create is refused with the counts read so
// far and how to clear the failed backups (see surveyFailed).
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

	cluster := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.Object.Raw, cluster); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	method := declaredBootstrap(cluster)

	store, serverName, found := Archiver(cluster)
	if !found {
		logger.Info("leaving the Cluster alone", "reason", "it archives nowhere")
		return admission.Allowed("no archiving plugin")
	}

	at, err := ResolveLocation(ctx, d.Client, req.Namespace, store, serverName)
	if err != nil {
		// The store is named and unreadable. Refusing is the failure that gets
		// noticed; allowing would create an empty database beside a full
		// archive and report success.
		logger.Error(err, "cannot read the object store")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// Two databases archiving to one prefix interleave their WAL and leave the
	// archive unrestorable, which is silent and permanent. Nothing else on the
	// cluster can see this coming: each Cluster is valid on its own, and the
	// pair is the problem. Where a Cluster archives does not depend on how it
	// bootstraps, so a Cluster declaring its own bootstrap is checked too.
	holder, err := archiveHolder(ctx, d.Client, req.Namespace, req.Name, at)
	if err != nil {
		logger.Error(err, "cannot check which databases archive here")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if holder != "" {
		logger.Info("refusing the Cluster", "reason", "another database archives here", "holder", holder, "prefix", at.Prefix)
		return admission.Denied(fmt.Sprintf(
			"%s already archives to %s/%s. Two databases writing one archive interleave their WAL and leave it unrestorable. Give this Cluster an archive of its own, or a serverName that is not %q. The check compares bucket and prefix whatever the endpointURL says, because two endpoints can name one service; if %s really archives to a different S3 service, give one of the two its own prefix in destinationPath.",
			holder, at.Bucket, at.Prefix, serverName, holder,
		))
	}

	// The opt-out asks for an empty database, which only initdb gives, so it
	// changes nothing for a Cluster that declares another bootstrap. An empty
	// database can archive only into an empty prefix: CloudNativePG refuses a
	// prefix that holds WAL, and the skip annotation would let the new
	// database overwrite the old one's segments.
	if cluster.GetAnnotations()[OptOutAnnotation] == OptOutValue && method == "" {
		archive, err := d.Prober.Survey(ctx, at, nil)
		if err != nil {
			return surveyFailed(logger, at, err)
		}
		if !archive.Empty {
			logger.Info("refusing the Cluster", "reason", "opted out over an old archive", "prefix", at.ServerPrefix())
			return admission.Denied(fmt.Sprintf(
				"The Cluster asks for an empty database (%s: %s), and s3://%s/%s still holds the archive of an earlier one. CloudNativePG will not archive a new database into a prefix that holds WAL, so this one would never be backed up. To discard the old archive, delete everything under s3://%s/%s and create the Cluster again. To keep it, give this Cluster a serverName that is not %q.",
				OptOutAnnotation, OptOutValue, at.Bucket, at.ServerPrefix(), at.Bucket, at.ServerPrefix(), serverName,
			))
		}
		logger.Info("leaving the Cluster to initdb", "reason", "opted out by annotation, and the prefix is empty")
		return admission.Allowed("opted out")
	}

	run, err := waitingRun(ctx, d.Client, req.Namespace, req.Name)
	if err != nil {
		logger.Error(err, "cannot read the namespace's RestoreRuns")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// A Cluster that already names a bootstrap other than initdb was written
	// that way on purpose: a recovery by the Flux component or a unit's
	// restore input, a pg_basebackup by a replica or a migration. It keeps its
	// own source, unless a RestoreRun is waiting to recover it: then two
	// sources name one database, and neither may win by accident.
	if method != "" {
		if run != nil {
			return admission.Denied(fmt.Sprintf(
				"RestoreRun %s is waiting to recover %s/%s, and the Cluster declares its own spec.bootstrap.%s. Remove the declared bootstrap (for a recovery, the terragrunt restore input or the postgres-recovery component), or delete the RestoreRun.",
				run.Name, req.Namespace, req.Name, method,
			))
		}
		logger.Info("leaving the Cluster alone", "reason", "it declares its own bootstrap", "method", method)
		return admission.Allowed("declares its own bootstrap")
	}

	target, source, err := restoreTarget(cluster, run)
	if err != nil {
		return admission.Denied(err.Error())
	}

	archive, err := d.Prober.Survey(ctx, at, target)
	if err != nil {
		return surveyFailed(logger, at, err)
	}
	// A target before the oldest base backup's end is one Postgres can never
	// reach. CloudNativePG would keep the Cluster in recovery reporting that
	// no backup matched, so the webhook refuses it here with the reason.
	if archive.Found == nil && archive.Oldest != nil && target != nil {
		return admission.Denied(fmt.Sprintf(
			"%s asks for %s, and no base backup in %s/%s finished by then%s.",
			source, target.Format(time.RFC3339), at.Bucket, at.BasePrefix(), oldest([]BaseBackup{*archive.Oldest}),
		))
	}
	if archive.Found == nil {
		if run != nil || target != nil {
			return admission.Denied(fmt.Sprintf(
				"%s asks for a recovery, and %s/%s holds no completed base backup to recover from.",
				source, at.Bucket, at.BasePrefix(),
			))
		}
		// CloudNativePG refuses to archive a new database into a prefix that
		// holds WAL, so a database started empty over anything at all could
		// never be backed up, and a recovery has no base backup to start
		// from. An empty prefix is the only place initdb is safe.
		if !archive.Empty {
			logger.Info("refusing the Cluster", "reason", "an archive with no completed base backup", "prefix", at.ServerPrefix())
			return admission.Denied(noDoneBackup(at, serverName, Contents{Any: true, BaseDirs: archive.Backups}))
		}
		logger.Info("leaving the Cluster to initdb", "reason", "no completed base backup in the store", "prefix", at.BasePrefix())
		return admission.Allowed("no base backup")
	}

	if err := setRecovery(cluster, store, serverName, target); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if run != nil {
		annotations := cluster.GetAnnotations()
		annotations[backupv1alpha1.AnnotationRestoreRun] = run.Name
		cluster.SetAnnotations(annotations)
	}

	patched, err := json.Marshal(cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	logger.Info("recovering the Cluster from its object store", "prefix", at.BasePrefix(), "target", target, "for", source)
	return admission.PatchResponseFromRaw(req.Object.Raw, patched)
}

// declaredBootstrap names the bootstrap method a Cluster declares for itself,
// one the webhook must leave alone.
//
// It returns the first key under spec.bootstrap, in sorted order, other than
// initdb, such as "recovery" or "pg_basebackup". It returns an empty string
// when spec.bootstrap is missing or holds initdb alone. Those are the Clusters
// the webhook may rewrite: CloudNativePG runs initdb when no method is named,
// and setRecovery replaces initdb and nothing else. Adding a recovery beside
// any other method gives the Cluster two, and CloudNativePG refuses that.
func declaredBootstrap(cluster *unstructured.Unstructured) string {
	bootstrap, _, _ := unstructured.NestedMap(cluster.Object, "spec", "bootstrap")
	methods := make([]string, 0, len(bootstrap))
	for method := range bootstrap {
		if method != "initdb" {
			methods = append(methods, method)
		}
	}
	if len(methods) == 0 {
		return ""
	}
	sort.Strings(methods)
	return methods[0]
}

// OwnerBootstrap names the bootstrap method a Cluster's owner declares, such
// as "pg_basebackup", and returns an empty string when there is none. A
// RestoreRun leaves such a Cluster alone: Flux would create it again with that
// method, and the webhook refuses it while a run waits for it.
//
// It is declaredBootstrap without the recovery this webhook wrote itself,
// whose source is RecoverySource. A Cluster the webhook recovered keeps that
// recovery in its spec, and a later restore of it is the controller's own.
func OwnerBootstrap(cluster *unstructured.Unstructured) string {
	method := declaredBootstrap(cluster)
	if method == "recovery" {
		if source, _, _ := unstructured.NestedString(cluster.Object, "spec", "bootstrap", "recovery", "source"); source == RecoverySource {
			return ""
		}
	}
	return method
}

// waitingRun finds the RestoreRun that deleted a Cluster and is waiting for it
// to be created again, so the webhook can recover the new Cluster for that
// run.
//
// It lists the RestoreRuns in the given namespace and returns the first one
// that hasn't finished, isn't being deleted, and has an entry in status.items
// for a Cluster with the given name in phase Deleted. It returns nil when no
// run matches, and an error when the list fails.
func waitingRun(ctx context.Context, c client.Reader, namespace, name string) (*backupv1alpha1.RestoreRun, error) {
	runs := &backupv1alpha1.RestoreRunList{}
	if err := c.List(ctx, runs, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list RestoreRuns in %s: %w", namespace, err)
	}
	for i := range runs.Items {
		run := &runs.Items[i]
		if run.Status.Phase.Finished() || !run.DeletionTimestamp.IsZero() {
			continue
		}
		for _, item := range run.Status.Items {
			if item.Kind == "Cluster" && item.Name == name && item.Phase == backupv1alpha1.ItemDeleted {
				return run, nil
			}
		}
	}
	return nil, nil
}

// restoreTarget works out the moment a new Cluster should recover to, and
// names what asked for it so refusal messages can point there.
//
// Parameters:
//   - cluster is the Cluster being created. Its backup.wlz.li/restore-as-of
//     annotation sets the target when no RestoreRun waits for it.
//   - run is the RestoreRun waiting for the Cluster (see waitingRun), or nil.
//
// With a waiting RestoreRun, the target is the run's status.syncedTo, or else
// its spec.restoreAsOf, or else nil, and the source is "RestoreRun <name>".
// Without one, the target is the annotation's time and the source is
// "annotation backup.wlz.li/restore-as-of". With neither, the target is nil and
// the source is "the webhook". A nil target means the recovery replays to the
// end of the archive. It returns an error when restoreAsOf or the annotation
// isn't an RFC 3339 time.
func restoreTarget(cluster *unstructured.Unstructured, run *backupv1alpha1.RestoreRun) (*time.Time, string, error) {
	if run != nil {
		source := "RestoreRun " + run.Name
		if run.Status.SyncedTo != nil {
			t := run.Status.SyncedTo.UTC()
			return &t, source, nil
		}
		if run.Spec.RestoreAsOf == nil {
			return nil, source, nil
		}
		t, err := time.Parse(time.RFC3339, *run.Spec.RestoreAsOf)
		if err != nil {
			return nil, source, fmt.Errorf("%s has an unparsable restoreAsOf %q: %w", source, *run.Spec.RestoreAsOf, err)
		}
		return &t, source, nil
	}

	value, ok := cluster.GetAnnotations()[backupv1alpha1.AnnotationRestoreAsOf]
	if !ok {
		return nil, "the webhook", nil
	}
	source := "annotation " + backupv1alpha1.AnnotationRestoreAsOf
	t, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return nil, source, fmt.Errorf("%s is %q, which is not an RFC 3339 time", source, value)
	}
	return &t, source, nil
}

// oldest returns the end of a refusal message that names the oldest base
// backup and when it finished, or says there is none. It expects the list
// oldest first, as BaseBackups returns it.
func oldest(backups []BaseBackup) string {
	if len(backups) == 0 {
		return "; it holds no completed base backup"
	}
	return fmt.Sprintf("; the oldest, %s, finished at %s", backups[0].ID, backups[0].End.UTC().Format(time.RFC3339))
}

// keepRecovery answers an update of a Cluster. When the old Cluster was
// recovered by this webhook (its spec.bootstrap.recovery.source is
// RecoverySource) and the new one carries spec.bootstrap.initdb, it returns a
// patch that removes initdb. Every other update is allowed unchanged.
//
// Flux applies the Cluster from git on every reconcile, and git holds initdb.
// Server-side apply keeps the recovery written at creation and adds initdb
// back, and CloudNativePG refuses a Cluster with two bootstrap methods. That
// refusal fails the dry run Flux runs first, so keepRecovery runs on dry runs
// too. It reads nothing from the cluster, and CloudNativePG ignores
// spec.bootstrap once a Cluster exists, so dropping initdb changes nothing
// about the database.
func keepRecovery(req admission.Request) admission.Response {
	old := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.OldObject.Raw, old); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	source, _, _ := unstructured.NestedString(old.Object, "spec", "bootstrap", "recovery", "source")
	if source != RecoverySource {
		return admission.Allowed("not recovered by this webhook")
	}

	cluster := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.Object.Raw, cluster); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}
	if _, found, _ := unstructured.NestedMap(cluster.Object, "spec", "bootstrap", "initdb"); !found {
		return admission.Allowed("no initdb to drop")
	}
	unstructured.RemoveNestedField(cluster.Object, "spec", "bootstrap", "initdb")

	patched, err := json.Marshal(cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	return admission.PatchResponseFromRaw(req.Object.Raw, patched)
}

// archiveHolder finds an existing Cluster that already archives to the same
// bucket and prefix as the Cluster being admitted, whatever endpointURL either
// names. Two databases archiving to one prefix interleave their WAL and leave
// the archive unrestorable.
//
// Parameters:
//   - namespace and name identify the Cluster being admitted. A Cluster with
//     the same namespace and name is skipped, so a recreate of the same
//     database doesn't collide with its own record.
//   - at is where the admitted Cluster would archive, from ResolveLocation.
//
// It returns the holder as "namespace/name", or an empty string when no
// Cluster archives there. It returns an error when the Cluster list fails, or
// when another Cluster's ObjectStore can't be read for any reason other than
// NotFound, such as a server timeout. That Cluster may archive to the same
// prefix, so the caller refuses the create instead of missing a collision.
//
// It lists every Cluster in every namespace, reads each archiving one's
// ObjectStore with archiveAt, and compares the two with sameArchive: bucket
// and prefix, never the endpoint, since two endpoint names can reach one
// service. Two Clusters can reach one prefix through differently named
// ObjectStores, so comparing store names would miss them. The check reads no
// Secret. Where a Cluster archives is written in its ObjectStore, so a holder
// whose credentials are missing is still found, and each other Cluster costs
// one read, which keeps a create on a cluster with many databases inside the
// webhook's timeout. A Cluster whose ObjectStore does not exist or names no
// s3:// destination archives nowhere, so it is skipped.
func archiveHolder(
	ctx context.Context,
	c client.Reader,
	namespace, name string,
	at Location,
) (string, error) {
	clusters := &unstructured.UnstructuredList{}
	clusters.SetGroupVersionKind(ClusterListGVK)
	if err := c.List(ctx, clusters); err != nil {
		return "", fmt.Errorf("list the Clusters: %w", err)
	}

	for i := range clusters.Items {
		other := &clusters.Items[i]
		if other.GetNamespace() == namespace && other.GetName() == name {
			continue
		}
		store, serverName, found := Archiver(other)
		if !found {
			continue
		}
		theirs, _, err := archiveAt(ctx, c, other.GetNamespace(), store, serverName)
		var noDestination *destinationError
		if apierrors.IsNotFound(err) || errors.As(err, &noDestination) {
			continue
		}
		if err != nil {
			return "", fmt.Errorf("find where %s/%s archives: %w", other.GetNamespace(), other.GetName(), err)
		}
		if theirs.sameArchive(at) {
			return fmt.Sprintf("%s/%s", other.GetNamespace(), other.GetName()), nil
		}
	}
	return "", nil
}

// Archiver finds where a Cluster archives its WAL. It looks in spec.plugins for
// the first entry named PluginName with isWALArchiver set to true and a
// non-empty barmanObjectName parameter.
//
// It returns that entry's barmanObjectName as the store and its serverName
// parameter as the server name, with found set to true. When serverName is
// unset, the server name is the Cluster's own name. It returns found as false
// when no entry matches. Such a Cluster backs nothing up and has nothing to
// recover from.
func Archiver(cluster *unstructured.Unstructured) (store, serverName string, found bool) {
	plugins, ok, err := unstructured.NestedSlice(cluster.Object, "spec", "plugins")
	if err != nil || !ok {
		return "", "", false
	}

	for _, entry := range plugins {
		plugin, ok := entry.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(plugin, "name"); name != PluginName {
			continue
		}
		if isArchiver, _, _ := unstructured.NestedBool(plugin, "isWALArchiver"); !isArchiver {
			continue
		}
		store, _, _ = unstructured.NestedString(plugin, "parameters", "barmanObjectName")
		if store == "" {
			continue
		}
		serverName, _, _ = unstructured.NestedString(plugin, "parameters", "serverName")
		if serverName == "" {
			// Barman defaults the server name to the Cluster's name, so an
			// unset parameter means the archive sits under that.
			serverName = cluster.GetName()
		}
		return store, serverName, true
	}
	return "", "", false
}

// setRecovery rewrites a Cluster, in place, to recover from its own archive.
//
// Parameters:
//   - cluster is the Cluster being created. It's modified directly.
//   - store and serverName say where the Cluster archives, as Archiver
//     returns them. The recovery reads from the same place.
//   - target is the moment to recover to, written as
//     recoveryTarget.targetTime. A nil target replays to the end of the
//     archive.
//
// It replaces spec.bootstrap.initdb with spec.bootstrap.recovery, carrying
// over initdb's database, owner and secret. It adds an externalClusters entry
// named RecoverySource that reads through the Barman Cloud plugin, or replaces
// an entry of that name if one is there. It sets SkipCheckAnnotation to
// "enabled" so the recovered database can archive into the prefix it restored
// from. It returns an error only when the unstructured object can't be read or
// written at those paths.
func setRecovery(cluster *unstructured.Unstructured, store, serverName string, target *time.Time) error {
	recovery := map[string]any{"source": RecoverySource}
	if target != nil {
		recovery["recoveryTarget"] = map[string]any{"targetTime": target.UTC().Format(time.RFC3339)}
	}

	// The application database, its owning role and the Secret holding that
	// role's password carry over from initdb.
	//
	// Dropping them breaks the app. CloudNativePG defaults a recovery's
	// database and owner to "app", so a Cluster that was created with
	// database "canary" comes back with its data in "canary" and an empty
	// "app" beside it, and the <cluster>-app Secret the workload reads points
	// at the empty one. The restore then looks like data loss, though the
	// data is all there.
	for _, field := range []string{"database", "owner"} {
		value, found, err := unstructured.NestedString(cluster.Object, "spec", "bootstrap", "initdb", field)
		if err == nil && found && value != "" {
			recovery[field] = value
		}
	}
	if secret, found, err := unstructured.NestedMap(cluster.Object, "spec", "bootstrap", "initdb", "secret"); err == nil && found {
		recovery["secret"] = secret
	}

	unstructured.RemoveNestedField(cluster.Object, "spec", "bootstrap", "initdb")

	if err := unstructured.SetNestedMap(cluster.Object, recovery, "spec", "bootstrap", "recovery"); err != nil {
		return fmt.Errorf("set the recovery bootstrap: %w", err)
	}

	entry := map[string]any{
		"name": RecoverySource,
		"plugin": map[string]any{
			"name": PluginName,
			"parameters": map[string]any{
				"barmanObjectName": store,
				"serverName":       serverName,
			},
		},
	}

	external, _, err := unstructured.NestedSlice(cluster.Object, "spec", "externalClusters")
	if err != nil {
		return fmt.Errorf("read the external clusters: %w", err)
	}
	replaced := false
	for i, existing := range external {
		item, ok := existing.(map[string]any)
		if !ok {
			continue
		}
		if name, _, _ := unstructured.NestedString(item, "name"); name == RecoverySource {
			external[i] = entry
			replaced = true
			break
		}
	}
	if !replaced {
		external = append(external, entry)
	}
	if err := unstructured.SetNestedSlice(cluster.Object, external, "spec", "externalClusters"); err != nil {
		return fmt.Errorf("set the external clusters: %w", err)
	}

	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[SkipCheckAnnotation] = "enabled"
	cluster.SetAnnotations(annotations)
	return nil
}

// surveyFailed answers a create whose Survey failed. An *OutOfTimeError is
// refused with a message a person can act on: the counts read so far, and
// how to clear the failed backups barman never deletes. Any other error is
// an HTTP 500, which the API server treats as a refusal.
func surveyFailed(logger logr.Logger, at Location, err error) admission.Response {
	var late *OutOfTimeError
	if !errors.As(err, &late) {
		logger.Error(err, "cannot read the object store")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	logger.Info("refusing the Cluster", "reason", "the survey ran out of time", "backups", late.Backups, "read", late.Read)
	base := fmt.Sprintf("s3://%s/%s", at.Bucket, at.BasePrefix())
	if late.Backups == 0 {
		return admission.Denied(fmt.Sprintf(
			"Checking %s ran out of time before the object store listed its base backups. Check that the object store answers, then create the Cluster again.", base,
		))
	}
	return admission.Denied(fmt.Sprintf(
		"Checking %s ran out of time: %d base backups are there, and the newest %d read were none of them completed. barman never deletes failed or unfinished backups. Delete the base/<id>/ directories whose backup.info does not say status=DONE (barman-cloud-backup-delete --backup-id <id> deletes one), then create the Cluster again.",
		base, late.Backups, late.Read,
	))
}

// noDoneBackup words the refusal of a Cluster whose prefix holds objects but
// no completed base backup, and nothing asks for a recovery.
//
// Parameters:
//   - at is the database's Location.
//   - serverName is the directory the Cluster archives under, which the
//     message suggests changing.
//   - contents is what the prefix holds. BaseDirs picks the wording of the
//     parenthesis.
func noDoneBackup(at Location, serverName string, contents Contents) string {
	what := "(no base backup under base/, but other objects, such as WAL, under the prefix)"
	switch contents.BaseDirs {
	case 0:
	case 1:
		what = "(1 base backup under base/, not DONE)"
	default:
		what = fmt.Sprintf("(%d base backups under base/, none DONE)", contents.BaseDirs)
	}
	prefix := fmt.Sprintf("s3://%s/%s", at.Bucket, at.ServerPrefix())
	return fmt.Sprintf(
		"%s holds an archive with no completed base backup %s. A database started empty here could never archive its WAL, because CloudNativePG refuses a prefix that already holds WAL, and a recovery has nothing to start from. If that archive is worth nothing, delete everything under %s and create the Cluster again. Otherwise give this Cluster a serverName that is not %q.",
		prefix, what, prefix, serverName,
	)
}
