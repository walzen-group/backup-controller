package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

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
	recovery := recoveryBootstrap(cluster, target)

	unstructured.RemoveNestedField(cluster.Object, "spec", "bootstrap", "initdb")

	if err := unstructured.SetNestedMap(cluster.Object, recovery, "spec", "bootstrap", "recovery"); err != nil {
		return fmt.Errorf("set the recovery bootstrap: %w", err)
	}
	if err := setExternalCluster(cluster, store, serverName); err != nil {
		return err
	}

	annotations := cluster.GetAnnotations()
	if annotations == nil {
		annotations = map[string]string{}
	}
	annotations[SkipCheckAnnotation] = "enabled"
	cluster.SetAnnotations(annotations)
	return nil
}

// recoveryBootstrap makes the spec.bootstrap.recovery of a Cluster that
// setRecovery rewrites.
//
// Parameters:
//   - cluster is the Cluster before setRecovery changes it. Its initdb gives
//     the database, the owner and the secret.
//   - target is the moment to recover to, or nil for the end of the archive.
//
// It returns the recovery, with RecoverySource as its source.
func recoveryBootstrap(cluster *unstructured.Unstructured, target *time.Time) map[string]any {
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
	return recovery
}

// setExternalCluster adds the externalClusters entry named RecoverySource to
// a Cluster, or replaces the entry with that name.
//
// Parameters:
//   - cluster is the Cluster that setRecovery rewrites. setExternalCluster
//     changes it in place.
//   - store and serverName tell where the entry reads from, through the
//     Barman Cloud plugin.
//
// It returns an error when spec.externalClusters cannot be read or written.
func setExternalCluster(cluster *unstructured.Unstructured, store, serverName string) error {
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
	return nil
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
