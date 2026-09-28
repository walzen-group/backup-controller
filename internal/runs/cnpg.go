package runs

import (
	"context"
	"fmt"
	"sort"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterGVK, BackupGVK and clusterListGV are the CloudNativePG kinds a run
// reads and writes. The controller reads them as unstructured objects, so
// it doesn't depend on CloudNativePG's Go module.
var (
	ClusterGVK    = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}
	BackupGVK     = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Backup"}
	clusterListGV = bootstrap.ClusterListGVK
)

// hibernationAnnotation is the annotation that tells CloudNativePG to stop a
// Cluster's database. A Backup of a hibernated Cluster fails, and it stays
// failed after the Cluster wakes.
const hibernationAnnotation = "cnpg.io/hibernation"

// healthyPhase is the status.phase CloudNativePG reports for a Cluster that
// is up.
const healthyPhase = "Cluster in healthy state"

// getCluster reads one Cluster. It returns false, with no error, when the
// Cluster doesn't exist.
func getCluster(ctx context.Context, c client.Reader, namespace, name string) (*unstructured.Unstructured, bool, error) {
	cluster := &unstructured.Unstructured{}
	cluster.SetGroupVersionKind(ClusterGVK)
	err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, cluster)
	if apierrors.IsNotFound(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get Cluster %s/%s: %w", namespace, name, err)
	}
	return cluster, true, nil
}

// enabledClusters lists the Clusters in a namespace that carry the annotation
// backup.wlz.li/enabled: "true", sorted by name. A Cluster that is being
// deleted is left out.
func enabledClusters(ctx context.Context, c client.Reader, namespace string) ([]unstructured.Unstructured, error) {
	clusters := &unstructured.UnstructuredList{}
	clusters.SetGroupVersionKind(clusterListGV)
	if err := c.List(ctx, clusters, client.InNamespace(namespace)); err != nil {
		return nil, fmt.Errorf("list the Clusters in %s: %w", namespace, err)
	}
	var enabled []unstructured.Unstructured
	for _, cluster := range clusters.Items {
		if backupv1alpha1.Enabled(cluster.GetAnnotations()) && cluster.GetDeletionTimestamp() == nil {
			enabled = append(enabled, cluster)
		}
	}
	sort.Slice(enabled, func(i, j int) bool { return enabled[i].GetName() < enabled[j].GetName() })
	return enabled, nil
}

// hibernated reports whether a Cluster is hibernated, which means it carries
// the annotation cnpg.io/hibernation: "on" and CloudNativePG has stopped its
// database.
func hibernated(cluster *unstructured.Unstructured) bool {
	return cluster.GetAnnotations()[hibernationAnnotation] == "on"
}

// clusterPhase returns the Cluster's status.phase as CloudNativePG reports
// it, or an empty string when it has none.
func clusterPhase(cluster *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(cluster.Object, "status", "phase")
	return phase
}

// backupName returns the name of the Backup a run creates for a Cluster: the
// Cluster's name, a dash, and the first eight characters of the run's UID.
// Because the name comes from the UID, a restarted controller finds the Backup
// it made.
func backupName(cluster string, uid types.UID) string {
	short := string(uid)
	if len(short) > 8 {
		short = short[:8]
	}
	return cluster + "-" + short
}

// ensureBackup creates a CloudNativePG Backup that asks for a base backup of a
// Cluster through the barman-cloud plugin. It is the same object the plugin's
// own on-demand path creates.
//
// Parameters:
//   - namespace is the run's namespace, which holds the Cluster.
//   - cluster is the Cluster's name.
//   - uid is the run's UID. backupName builds the Backup's name from it.
//
// It returns the Backup's name. A Backup of that name that already exists
// counts as created, so a second call for the same run is safe. The Backup
// carries the label app.kubernetes.io/managed-by: backup-controller.
func ensureBackup(ctx context.Context, c client.Client, namespace, cluster string, uid types.UID) (string, error) {
	backup := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"method":              "plugin",
			"pluginConfiguration": map[string]any{"name": bootstrap.PluginName},
			"cluster":             map[string]any{"name": cluster},
		},
	}}
	backup.SetGroupVersionKind(BackupGVK)
	backup.SetNamespace(namespace)
	backup.SetName(backupName(cluster, uid))
	backup.SetLabels(map[string]string{backupv1alpha1.LabelManagedBy: backupv1alpha1.ManagedByValue})
	if err := c.Create(ctx, backup); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create Backup %s/%s: %w", namespace, backup.GetName(), err)
	}
	return backup.GetName(), nil
}

// backupOutcome is what a CloudNativePG Backup reports about itself.
type backupOutcome struct {
	// Done is true once the Backup's phase is completed or failed.
	// CloudNativePG changes neither phase afterwards.
	Done bool
	// Completed is true when the phase is completed.
	Completed bool
	// Message is CloudNativePG's error from status.error for a failed Backup.
	Message string
	// BackupID is barman's ID of the base backup, from status.backupId, such
	// as 20260928T093012. CloudNativePG sets it for a completed Backup.
	BackupID string
}

// backupResult reads what a CloudNativePG Backup reports.
//
// Parameters:
//   - c reads the Backup from the API server.
//   - namespace and name identify the Backup.
//
// It returns the outcome of the Backup, and an error when the Backup can't be
// read. A phase other than completed or failed gives an outcome that is not
// done.
func backupResult(ctx context.Context, c client.Reader, namespace, name string) (backupOutcome, error) {
	backup := &unstructured.Unstructured{}
	backup.SetGroupVersionKind(BackupGVK)
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, backup); err != nil {
		return backupOutcome{}, fmt.Errorf("get Backup %s/%s: %w", namespace, name, err)
	}
	phase, _, _ := unstructured.NestedString(backup.Object, "status", "phase")
	switch phase {
	case "completed":
		id, _, _ := unstructured.NestedString(backup.Object, "status", "backupId")
		return backupOutcome{Done: true, Completed: true, BackupID: id}, nil
	case "failed":
		message, _, _ := unstructured.NestedString(backup.Object, "status", "error")
		return backupOutcome{Done: true, Message: message}, nil
	}
	return backupOutcome{}, nil
}

// newTime returns a pointer to a copy of t, so a caller can set a *metav1.Time
// field from a function's result in one expression.
func newTime(t metav1.Time) *metav1.Time { return &t }
