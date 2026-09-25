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

// The CloudNativePG kinds a run reads and writes, as unstructured, so the
// controller carries no dependency on CloudNativePG's Go module.
var (
	ClusterGVK    = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}
	BackupGVK     = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Backup"}
	clusterListGV = bootstrap.ClusterListGVK
)

// hibernationAnnotation is CloudNativePG's switch for a stopped database. A
// Backup of a hibernated Cluster fails and stays failed after it wakes.
const hibernationAnnotation = "cnpg.io/hibernation"

// healthyPhase is the phase CloudNativePG reports for a Cluster that is up.
const healthyPhase = "Cluster in healthy state"

// clusterLabel names the Cluster on each instance pod and PVC CloudNativePG
// creates for it.
const clusterLabel = "cnpg.io/cluster"

// instanceLeft names a pod or PVC of Cluster name still in the namespace, and
// returns empty once none is. An instance pod outlives its deleted Cluster
// until Postgres has shut down, which takes up to the Cluster's
// smartShutdownTimeout, and until then the Cluster's Services still reach it
// and a new Cluster cannot take its names.
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

// getCluster reads one Cluster, and reports false when it does not exist.
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

// enabledClusters lists the Clusters in a namespace marked
// backup.wlz.li/enabled, by name.
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

// hibernated reports a Cluster CloudNativePG has stopped.
func hibernated(cluster *unstructured.Unstructured) bool {
	return cluster.GetAnnotations()[hibernationAnnotation] == "on"
}

// clusterPhase reads the phase CloudNativePG reports.
func clusterPhase(cluster *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(cluster.Object, "status", "phase")
	return phase
}

// backupName is the Backup a run creates for a Cluster, derived from the run's
// UID so a restarted controller finds the one it made.
func backupName(cluster string, uid types.UID) string {
	short := string(uid)
	if len(short) > 8 {
		short = short[:8]
	}
	return cluster + "-" + short
}

// ensureBackup asks CloudNativePG for a base backup through the barman-cloud
// plugin, the same object the plugin's own on-demand path creates.
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

// backupResult reads a Backup's phase: done reports a terminal one, and
// message carries CloudNativePG's error for a failed one.
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

// newTime returns a pointer to a metav1.Time.
func newTime(t metav1.Time) *metav1.Time { return &t }
