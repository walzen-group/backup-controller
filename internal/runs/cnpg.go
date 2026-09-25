package runs

import (
	"context"
	"fmt"
	"sort"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	corev1 "k8s.io/api/core/v1"
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

// clusterLabel is the label CloudNativePG puts on each instance pod and PVC it
// creates for a Cluster. Its value is the Cluster's name.
const clusterLabel = "cnpg.io/cluster"

// instanceLeft looks for an instance pod or PVC of a Cluster that is still in
// the namespace. A RestoreRun calls it after it deletes a Cluster. Until
// nothing is left, the run waits with reason WaitingForShutdown, and it
// restarts the workloads it stopped only after that.
//
// Parameters:
//   - c lists the pods and PVCs.
//   - namespace is the Cluster's namespace.
//   - name is the Cluster's name, matched against the cnpg.io/cluster label.
//
// It returns a description of the first one it finds, such as
// "pod notes-pg-1" or "PVC notes-pg-1", or an empty string once none is left.
// A pod in phase Succeeded or Failed doesn't count.
//
// An instance pod outlives its deleted Cluster until Postgres has shut down,
// which takes up to the Cluster's smartShutdownTimeout. Until then the
// Cluster's Services still reach the pod, and a new Cluster can't take its
// names.
func instanceLeft(ctx context.Context, c client.Reader, namespace, name string) (string, error) {
	selector := client.MatchingLabels{clusterLabel: name}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace), selector); err != nil {
		return "", fmt.Errorf("list the pods of Cluster %s/%s: %w", namespace, name, err)
	}
	for _, pod := range pods.Items {
		if pod.Status.Phase != corev1.PodSucceeded && pod.Status.Phase != corev1.PodFailed {
			return "pod " + pod.Name, nil
		}
	}
	claims := &corev1.PersistentVolumeClaimList{}
	if err := c.List(ctx, claims, client.InNamespace(namespace), selector); err != nil {
		return "", fmt.Errorf("list the PVCs of Cluster %s/%s: %w", namespace, name, err)
	}
	if len(claims.Items) > 0 {
		return "PVC " + claims.Items[0].Name, nil
	}
	return "", nil
}

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

// backupResult reads the status.phase of the Backup with the given name.
//
// The result done is true once the phase is completed or failed, and ok is
// true when it completed. For a failed Backup, message holds CloudNativePG's
// error from status.error. It returns an error when the Backup can't be read.
func backupResult(ctx context.Context, c client.Reader, namespace, name string) (done, ok bool, message string, err error) {
	backup := &unstructured.Unstructured{}
	backup.SetGroupVersionKind(BackupGVK)
	if err := c.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, backup); err != nil {
		return false, false, "", fmt.Errorf("get Backup %s/%s: %w", namespace, name, err)
	}
	phase, _, _ := unstructured.NestedString(backup.Object, "status", "phase")
	switch phase {
	case "completed":
		return true, true, "", nil
	case "failed":
		message, _, _ := unstructured.NestedString(backup.Object, "status", "error")
		return true, false, message, nil
	}
	return false, false, "", nil
}

// newTime returns a pointer to a copy of t, so a caller can set a *metav1.Time
// field from a function's result in one expression.
func newTime(t metav1.Time) *metav1.Time { return &t }
