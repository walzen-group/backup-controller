package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

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

	if req.Operation != admissionv1.Create {
		return admission.Allowed("not a creation")
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

	// A Cluster that already asks for a recovery was written that way on
	// purpose, by the Flux component or a unit's restore input, and it carries
	// a point in time this webhook has no opinion about.
	if _, found, _ := unstructured.NestedMap(cluster.Object, "spec", "bootstrap", "recovery"); found {
		logger.Info("leaving the Cluster alone", "reason", "it already declares a recovery")
		return admission.Allowed("already recovering")
	}

	store, serverName, found := archiver(cluster)
	if !found {
		logger.Info("leaving the Cluster to initdb", "reason", "it archives nowhere")
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
	// pair is the problem.
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

	has, err := d.Prober.HasBaseBackup(ctx, at)
	if err != nil {
		logger.Error(err, "cannot list the object store")
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if !has {
		logger.Info("leaving the Cluster to initdb", "reason", "no base backup in the store", "prefix", at.BasePrefix())
		return admission.Allowed("no base backup")
	}

	if err := setRecovery(cluster, store, serverName); err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}

	patched, err := json.Marshal(cluster)
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, err)
	}
	logger.Info("recovering the Cluster from its object store", "prefix", at.BasePrefix())
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
		store, serverName, found := archiver(other)
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

// archiver finds the Cluster's WAL archiving plugin and the server name it
// writes under. A Cluster with no such plugin backs nothing up and has nothing
// to recover from.
func archiver(cluster *unstructured.Unstructured) (store, serverName string, found bool) {
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
// lets it archive into the prefix it restored from.
func setRecovery(cluster *unstructured.Unstructured, store, serverName string) error {
	recovery := map[string]any{"source": RecoverySource}

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
