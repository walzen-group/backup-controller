package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	admissionv1 "k8s.io/api/admission/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

const (
	// WebhookPath is where the admission server serves this handler, and the
	// path the MutatingWebhookConfiguration in deploy/ points at.
	WebhookPath = "/mutate-postgresql-cnpg-io-v1-cluster"

	// PluginName is the Barman Cloud plugin, named in a Cluster's plugins list
	// and again in the externalClusters entry this webhook writes.
	PluginName = "barman-cloud.cloudnative-pg.io"

	// SkipCheckAnnotation lets a recovered database archive into the prefix it
	// restored from. CloudNativePG refuses a non-empty archive on a freshly
	// bootstrapped database, which is every restore.
	SkipCheckAnnotation = "cnpg.io/skipEmptyWalArchiveCheck"

	// OptOutAnnotation asks for an empty database whatever the object store
	// holds. Without it there is no way to discard a database, because
	// deleting the Cluster would restore it again.
	OptOutAnnotation = "backup.wlz.li/bootstrap"

	// OptOutValue is what OptOutAnnotation is set to. Any other value is
	// ignored, so a typo does not silently wipe a database.
	OptOutValue = "initdb"

	// RecoverySource names the externalClusters entry this webhook adds. The
	// entry's name and bootstrap.recovery.source have to match, and nothing
	// else reads either, so one constant serves both.
	RecoverySource = "backup-controller"
)

// Decider chooses a Cluster's bootstrap at admission time.
//
// Client is a reader rather than a full client because this handler only ever
// reads, and an uncached reader keeps the controller's Secret grant at get.
// A cached client would need list and watch on every Secret in the cluster and
// would hold them all in memory.
type Decider struct {
	Client client.Reader
	Prober Prober
}

// Handle reads a Cluster being created and, when its object store already
// holds a base backup, rewrites it to recover from that backup.
//
// Every path that is not a clear "recover this" allows the Cluster through
// unchanged. A webhook that refuses Clusters it does not understand would put
// this controller in the way of every database on the cluster.
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

	cluster := &unstructured.Unstructured{}
	if err := json.Unmarshal(req.Object.Raw, cluster); err != nil {
		return admission.Errored(http.StatusBadRequest, err)
	}

	if cluster.GetAnnotations()[OptOutAnnotation] == OptOutValue {
		logger.Info("leaving the Cluster to initdb", "reason", "opted out by annotation")
		return admission.Allowed("opted out")
	}

	_, declared, _ := unstructured.NestedMap(cluster.Object, "spec", "bootstrap", "recovery")

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
	// bootstraps, so a Cluster declaring its own recovery is checked too.
	holder, err := archiveHolder(ctx, d.Client, req.Namespace, req.Name, at)
	if err != nil {
		logger.Error(err, "cannot check which databases archive here")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if holder != "" {
		logger.Info("refusing the Cluster", "reason", "another database archives here", "holder", holder, "prefix", at.Prefix)
		return admission.Denied(fmt.Sprintf(
			"%s already archives to %s/%s. Two databases writing one archive interleave their WAL and leave it unrestorable. Give this Cluster an archive of its own, or a serverName that is not %q.",
			holder, at.Bucket, at.Prefix, serverName,
		))
	}

	run, err := waitingRun(ctx, d.Client, req.Namespace, req.Name)
	if err != nil {
		logger.Error(err, "cannot read the namespace's RestoreRuns")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	// A Cluster that already asks for a recovery was written that way on
	// purpose, by the Flux component or a unit's restore input. It keeps its
	// own target, unless a RestoreRun is waiting to recover it: then two
	// targets name one database, and neither may win by accident.
	if declared {
		if run != nil {
			return admission.Denied(fmt.Sprintf(
				"RestoreRun %s is waiting to recover %s/%s, and the Cluster declares its own spec.bootstrap.recovery. Remove the declared recovery (the terragrunt restore input or the postgres-recovery component), or delete the RestoreRun.",
				run.Name, req.Namespace, req.Name,
			))
		}
		logger.Info("leaving the Cluster alone", "reason", "it already declares a recovery")
		return admission.Allowed("already recovering")
	}

	target, source, err := restoreTarget(cluster, run)
	if err != nil {
		return admission.Denied(err.Error())
	}

	has, err := d.Prober.HasBaseBackup(ctx, at)
	if err != nil {
		logger.Error(err, "cannot list the object store")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if !has {
		if run != nil || target != nil {
			return admission.Denied(fmt.Sprintf(
				"%s asks for a recovery, and %s/%s holds no base backup to recover from.",
				source, at.Bucket, at.BasePrefix(),
			))
		}
		logger.Info("leaving the Cluster to initdb", "reason", "no base backup in the store", "prefix", at.BasePrefix())
		return admission.Allowed("no base backup")
	}

	// A target before the oldest base backup's end is one Postgres can never
	// reach. CloudNativePG would keep the Cluster in recovery reporting that
	// no backup matched, so the webhook refuses it here with the reason.
	if target != nil {
		backups, err := d.Prober.BaseBackups(ctx, at)
		if err != nil {
			logger.Error(err, "cannot list the base backups")
			return admission.Errored(http.StatusInternalServerError, err)
		}
		if _, ok := AtOrBefore(backups, *target); !ok {
			return admission.Denied(fmt.Sprintf(
				"%s asks for %s, and no base backup in %s/%s finished by then%s.",
				source, target.Format(time.RFC3339), at.Bucket, at.BasePrefix(), oldest(backups),
			))
		}
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

// waitingRun returns the RestoreRun that deleted this Cluster and waits for it
// to come back, or nil when none does.
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

// restoreTarget returns the moment to recover to and what asked for it: the
// waiting RestoreRun's restoreAsOf, else the Cluster's restore-as-of
// annotation, else no target, which replays to the end of the archive.
func restoreTarget(cluster *unstructured.Unstructured, run *backupv1alpha1.RestoreRun) (*time.Time, string, error) {
	if run != nil {
		source := "RestoreRun " + run.Name
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

// oldest names the oldest base backup for a refusal message.
func oldest(backups []BaseBackup) string {
	if len(backups) == 0 {
		return "; it holds no completed base backup"
	}
	return fmt.Sprintf("; the oldest, %s, finished at %s", backups[0].ID, backups[0].End.UTC().Format(time.RFC3339))
}

// keepRecovery drops initdb from an update of a Cluster this webhook recovered.
//
// Flux applies the Cluster from git on every reconcile, and git holds initdb.
// Server-side apply keeps the recovery written at creation and adds initdb back,
// and CloudNativePG refuses a Cluster with two bootstrap methods. That refusal
// fails the dry-run Flux runs first, so this runs on dry-runs too. It reads
// nothing, and CloudNativePG ignores spec.bootstrap once a Cluster exists, so
// dropping initdb changes nothing about the database.
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

// archiveHolder returns the name of an existing Cluster that already archives
// to the same bucket and prefix, or an empty string when none does.
//
// It compares resolved destinations rather than names, because two Clusters
// can reach one prefix through differently named ObjectStores. The Cluster
// being admitted is skipped by namespace and name, so a recreate of the same
// database does not collide with the record of itself.
//
// A Cluster whose own store cannot be read is skipped rather than treated as a
// collision. Refusing a new database because an unrelated one is misconfigured
// would put this check in the way of work it has no business blocking.
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
		theirs, err := ResolveLocation(ctx, c, other.GetNamespace(), store, serverName)
		if err != nil {
			continue
		}
		if theirs.Bucket == at.Bucket && theirs.Prefix == at.Prefix {
			return fmt.Sprintf("%s/%s", other.GetNamespace(), other.GetName()), nil
		}
	}
	return "", nil
}

// Archiver finds the Cluster's WAL archiving plugin and the server name it
// writes under. A Cluster with no such plugin backs nothing up and has nothing
// to recover from.
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

// setRecovery rewrites the Cluster to restore from its own archive: the
// bootstrap, the external cluster it reads through, and the annotation that
// lets it archive into the prefix it restored from. A nil target replays to
// the end of the archive.
func setRecovery(cluster *unstructured.Unstructured, store, serverName string, target *time.Time) error {
	recovery := map[string]any{"source": RecoverySource}
	if target != nil {
		recovery["recoveryTarget"] = map[string]any{"targetTime": target.UTC().Format(time.RFC3339)}
	}

	// The application database, its owning role and the Secret holding that
	// role's password carry over from initdb.
	//
	// Dropping them is not harmless. CloudNativePG defaults a recovery's
	// database and owner to "app", so a Cluster that was created with
	// database "canary" comes back with its data in "canary" and an empty
	// "app" beside it, and the <cluster>-app Secret the workload reads points
	// at the empty one. The restore looks like data loss and is not.
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
