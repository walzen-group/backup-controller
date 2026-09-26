package runs

import (
	"context"
	"fmt"
	"sort"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/served"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterGVK and BackupGVK are the CloudNativePG kinds a run reads and
// writes. The controller reads them as unstructured objects, so it doesn't
// depend on CloudNativePG's Go module. v1 is the version this code was
// written against; every request goes out at the version the API server
// serves, which served.Kind looks up by group and kind, so a CloudNativePG
// release that moves the kinds to a new version with the same fields keeps
// working.
var (
	ClusterGVK = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Cluster"}
	BackupGVK  = schema.GroupVersionKind{Group: "postgresql.cnpg.io", Version: "v1", Kind: "Backup"}
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
//   - c lists the pods and PVCs. A RestoreRun passes its uncached Reader,
//     because an informer cache that has not caught up would report the
//     instance gone and let the app back onto the old Postgres.
//   - namespace is the Cluster's namespace.
//   - name is the Cluster's name, matched against the cnpg.io/cluster label.
//
// It returns a description of the first one it finds, pods before PVCs, such
// as "pod notes-pg-1" or "PVC notes-pg-1", and an empty string once none is
// left. A pod in phase Succeeded or Failed doesn't count, and a PVC counts
// while it exists, being deleted or not. It returns an error naming the
// Cluster when the list of the pods or of the PVCs fails, and the caller
// retries with nothing given back.
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

// getCluster reads one Cluster at the version the API server serves (see
// served.Kind).
//
// Parameters:
//   - c reads the Cluster. The reconcilers pass their uncached Reader.
//   - mapper looks up the served version, from the client's RESTMapper.
//   - namespace and name name the Cluster.
//
// It returns false, with no error, when the Cluster doesn't exist. When the
// API server serves no version of Cluster, it returns an error for which
// meta.IsNoMatchError is true, so a caller can say the cluster has no
// CloudNativePG CRDs. A read at a version the API server has stopped serving
// returns a *served.VersionGoneError, which never reads as a missing
// Cluster. Any other failed lookup or read is returned wrapped.
func getCluster(ctx context.Context, c client.Reader, mapper meta.RESTMapper, namespace, name string) (*unstructured.Unstructured, bool, error) {
	cluster, err := served.Get(ctx, c, mapper, ClusterGVK.GroupKind(), types.NamespacedName{Namespace: namespace, Name: name})
	if apierrors.IsNotFound(err) && !served.IsNotServed(err) {
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
//
// Parameters:
//   - c lists the Clusters.
//   - mapper looks up the version at which the API server serves Cluster.
//   - namespace is the namespace to list.
//
// A cluster that serves no version of Cluster has no CloudNativePG CRDs and
// so no Clusters, which gives an empty list. A list at a version the API
// server has stopped serving is an error (see served.VersionGone): taking it
// for an empty namespace would plan a run without its databases. Every other
// failed lookup or list is an error too.
func enabledClusters(ctx context.Context, c client.Reader, mapper meta.RESTMapper, namespace string) ([]unstructured.Unstructured, error) {
	clusters, err := served.List(ctx, c, mapper, ClusterGVK.GroupKind(), client.InNamespace(namespace))
	if served.IsNotServed(err) {
		return nil, nil
	}
	if err != nil {
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
// An empty or unknown phase is a legitimate state, since CloudNativePG
// reports many phases while a Cluster comes up, and it fails closed: a
// RestoreRun waits for healthyPhase up to its timeout and never takes
// another phase for a healthy Cluster.
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
// carries the label app.kubernetes.io/managed-by: backup-controller. It is
// created at the version the API server serves Backup at (see served.Kind),
// from c's RESTMapper. A failed lookup, and a create at a version the API
// server has stopped serving (see served.VersionGone), return an error and
// the caller tries again.
func ensureBackup(ctx context.Context, c client.Client, namespace, cluster string, uid types.UID) (string, error) {
	gvk, err := served.Kind(c.RESTMapper(), BackupGVK.GroupKind())
	if err != nil {
		return "", fmt.Errorf("create the Backup of Cluster %s/%s: %w", namespace, cluster, err)
	}
	backup := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"method":              "plugin",
			"pluginConfiguration": map[string]any{"name": bootstrap.PluginName},
			"cluster":             map[string]any{"name": cluster},
		},
	}}
	backup.SetGroupVersionKind(gvk)
	backup.SetNamespace(namespace)
	backup.SetName(backupName(cluster, uid))
	backup.SetLabels(map[string]string{backupv1alpha1.LabelManagedBy: backupv1alpha1.ManagedByValue})
	if err := served.VersionGone(c.RESTMapper(), gvk, c.Create(ctx, backup)); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create Backup %s/%s: %w", namespace, backup.GetName(), err)
	}
	return backup.GetName(), nil
}

// backupResult reads the status.phase of the Backup with the given name.
//
// Parameters:
//   - c reads the Backup, and mapper looks up the version at which the API
//     server serves it (see served.Get).
//   - namespace and name name the Backup.
//
// The result done is true once the Backup can go no further, and ok is true
// when it completed. The phases are those of CloudNativePG 1.30
// (api/v1/backup_types.go:32-58):
//   - "completed" is done and ok.
//   - "failed" and "invalid backup definition" are done and not ok, and
//     message holds CloudNativePG's error from status.error. CloudNativePG
//     never goes on with an invalid definition.
//   - No phase yet, "pending", "started", "running", "finalizing" and
//     "walArchivingFailing" are not done: CloudNativePG has not reconciled
//     the Backup yet, is taking it, or retries it, and the run waits up to
//     its timeout.
//   - Any other value is not done, and message names status.phase and the
//     value (see unknownBackupPhase). A CloudNativePG release may add a
//     phase that changes nothing the run relies on, so the run waits for
//     completed or failed. It names the phase in its Ready message while it
//     waits, and in the item's message when it reaches its timeout.
//
// For a Backup that is not done, message is empty in every known phase.
//
// It returns an error when the Backup can't be read, a read at a version the
// API server has stopped serving included.
func backupResult(ctx context.Context, c client.Reader, mapper meta.RESTMapper, namespace, name string) (done, ok bool, message string, err error) {
	backup, err := served.Get(ctx, c, mapper, BackupGVK.GroupKind(), types.NamespacedName{Namespace: namespace, Name: name})
	if err != nil {
		return false, false, "", fmt.Errorf("get Backup %s/%s: %w", namespace, name, err)
	}
	phase, _, _ := unstructured.NestedString(backup.Object, "status", "phase")
	switch phase {
	case "completed":
		return true, true, "", nil
	case "failed", "invalid backup definition":
		message, _, _ := unstructured.NestedString(backup.Object, "status", "error")
		if message == "" {
			message = fmt.Sprintf("the Backup %s/%s reports status.phase %q", namespace, name, phase)
		}
		return true, false, message, nil
	case "", "pending", "started", "running", "finalizing", "walArchivingFailing":
		return false, false, "", nil
	}
	return false, false, unknownBackupPhase(namespace, name, phase), nil
}

// unknownBackupPhase returns the sentence for a Backup whose status.phase is
// none of the phases of CloudNativePG 1.30.
//
// Parameters:
//   - namespace and name name the Backup.
//   - phase is the value of its status.phase, which the sentence quotes.
//
// The sentence says that the run waits for completed or failed, so a person
// who reads it in the Ready message knows why the run goes on.
func unknownBackupPhase(namespace, name, phase string) string {
	return fmt.Sprintf("CloudNativePG reports status.phase %q on Backup %s/%s, which is not a phase of CloudNativePG 1.30; "+
		"the run waits for completed or failed up to its timeout (see docs/compatibility.md)", phase, namespace, name)
}

// newTime returns a pointer to a copy of t, so a caller can set a *metav1.Time
// field from a function's result in one expression.
func newTime(t metav1.Time) *metav1.Time { return &t }
