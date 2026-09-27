// Package cnpg reads CloudNativePG Clusters and creates and reads their
// Backups for the BackupRun and RestoreRun reconcilers. It holds no
// decision about a run.
package cnpg

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

// HibernationAnnotation is the annotation that tells CloudNativePG to stop a
// Cluster's database. A Backup of a hibernated Cluster fails, and it stays
// failed after the Cluster wakes.
const HibernationAnnotation = "cnpg.io/hibernation"

// HealthyPhase is the status.phase CloudNativePG reports for a Cluster that
// is up.
const HealthyPhase = "Cluster in healthy state"

// ClusterLabel is the label CloudNativePG puts on each instance pod and PVC it
// creates for a Cluster. Its value is the Cluster's name.
const ClusterLabel = "cnpg.io/cluster"

// PodRoleLabel and PodRoleInstance are the label and value that CloudNativePG
// puts on each instance pod (CloudNativePG v1.30.0 pkg/specs/pods.go:548). A
// Pooler pod carries ClusterLabel too, with PodRoleLabel set to pooler
// (pkg/specs/pgbouncer/deployments.go:55-58). CloudNativePG itself selects the
// instance pods of a Cluster by both labels
// (internal/controller/cluster_restore.go:367-374), and it adds PodRoleLabel
// to an older instance pod that has no such label
// (pkg/reconciler/instance/metadata.go:241-244).
const (
	PodRoleLabel    = "cnpg.io/podRole"
	PodRoleInstance = "instance"
)

// InstanceLeft looks for an instance pod or PVC of a Cluster that is still in
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
//     Pods must also carry cnpg.io/podRole=instance, so a Pooler pod does not
//     count (see PodRoleLabel).
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
func InstanceLeft(ctx context.Context, c client.Reader, namespace, name string) (string, error) {
	selector := client.MatchingLabels{ClusterLabel: name}
	instances := client.MatchingLabels{ClusterLabel: name, PodRoleLabel: PodRoleInstance}
	pods := &corev1.PodList{}
	if err := c.List(ctx, pods, client.InNamespace(namespace), instances); err != nil {
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

// GetCluster reads one Cluster at the version the API server serves (see
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
func GetCluster(ctx context.Context, c client.Reader, mapper meta.RESTMapper, namespace, name string) (*unstructured.Unstructured, bool, error) {
	cluster, err := served.Get(ctx, c, mapper, ClusterGVK.GroupKind(), types.NamespacedName{Namespace: namespace, Name: name})
	if apierrors.IsNotFound(err) && !served.IsNotServed(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("get Cluster %s/%s: %w", namespace, name, err)
	}
	return cluster, true, nil
}

// EnabledClusters lists the Clusters in a namespace that carry the annotation
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
func EnabledClusters(ctx context.Context, c client.Reader, mapper meta.RESTMapper, namespace string) ([]unstructured.Unstructured, error) {
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

// Hibernated reports whether a Cluster is hibernated, which means it carries
// the annotation cnpg.io/hibernation: "on" and CloudNativePG has stopped its
// database.
func Hibernated(cluster *unstructured.Unstructured) bool {
	return cluster.GetAnnotations()[HibernationAnnotation] == "on"
}

// Phase returns the Cluster's status.phase as CloudNativePG reports
// it, or an empty string when it has none.
// An empty or unknown phase is a legitimate state, since CloudNativePG
// reports many phases while a Cluster comes up, and it fails closed: a
// RestoreRun waits for HealthyPhase up to its timeout and never takes
// another phase for a healthy Cluster.
func Phase(cluster *unstructured.Unstructured) string {
	phase, _, _ := unstructured.NestedString(cluster.Object, "status", "phase")
	return phase
}

// BackupName returns the name of the Backup a run creates for a Cluster: the
// Cluster's name, a dash, and the first eight characters of the run's UID.
// Because the name comes from the UID, a restarted controller finds the Backup
// it made.
func BackupName(cluster string, uid types.UID) string {
	short := string(uid)
	if len(short) > 8 {
		short = short[:8]
	}
	return cluster + "-" + short
}

// EnsureBackup creates a CloudNativePG Backup that asks for a base backup of a
// Cluster through the barman-cloud plugin. It is the same object the plugin's
// own on-demand path creates.
//
// Parameters:
//   - namespace is the run's namespace, which holds the Cluster.
//   - cluster is the Cluster's name.
//   - uid is the run's UID. BackupName builds the Backup's name from it.
//
// It returns the Backup's name. A Backup of that name that already exists
// counts as created, so a second call for the same run is safe. The Backup
// carries the label app.kubernetes.io/managed-by: backup-controller. It is
// created at the version the API server serves Backup at (see served.Kind),
// from c's RESTMapper. A failed lookup, and a create at a version the API
// server has stopped serving (see served.VersionGone), return an error and
// the caller tries again.
func EnsureBackup(ctx context.Context, c client.Client, namespace, cluster string, uid types.UID) (string, error) {
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
	backup.SetName(BackupName(cluster, uid))
	backup.SetLabels(map[string]string{backupv1alpha1.LabelManagedBy: backupv1alpha1.ManagedByValue})
	if err := served.VersionGone(c.RESTMapper(), gvk, c.Create(ctx, backup)); err != nil && !apierrors.IsAlreadyExists(err) {
		return "", fmt.Errorf("create Backup %s/%s: %w", namespace, backup.GetName(), err)
	}
	return backup.GetName(), nil
}

// BackupPhase is the state of a CloudNativePG Backup. BackupResult gets it
// from status.phase.
type BackupPhase int

const (
	// BackupWaiting is a Backup that has no phase yet, or a phase of
	// CloudNativePG 1.30 that can continue: pending, started, running,
	// finalizing or walArchivingFailing.
	BackupWaiting BackupPhase = iota
	// BackupUnknownPhase is a Backup whose status.phase is not a phase of
	// CloudNativePG 1.30. The run waits for completed or failed.
	BackupUnknownPhase
	// BackupCompleted is a Backup in the phase completed.
	BackupCompleted
	// BackupFailed is a Backup in the phase failed or invalid backup
	// definition. CloudNativePG does not continue such a Backup.
	BackupFailed
)

// BackupOutcome is the result that BackupResult gets from a Backup.
type BackupOutcome struct {
	// Phase is the state of the Backup.
	Phase BackupPhase
	// Message is the error from CloudNativePG for BackupFailed, and the
	// sentence of unknownBackupPhase for BackupUnknownPhase. It is empty for
	// the other phases.
	Message string
}

// BackupResult gets the status.phase of the Backup with the given name.
//
// Parameters:
//   - c reads the Backup. mapper finds the version at which the API server
//     serves Backup (see served.Get).
//   - namespace and name identify the Backup.
//
// It returns the outcome of the Backup. It returns an error if it cannot
// read the Backup. A read at a version that the API server does not serve
// now is also an error.
//
// The phases are those of CloudNativePG 1.30 (api/v1/backup_types.go:32-58):
//   - "completed" gives BackupCompleted.
//   - "failed" and "invalid backup definition" give BackupFailed. The
//     message is the error from status.error. CloudNativePG does not
//     continue a Backup with an invalid definition.
//   - No phase, "pending", "started", "running", "finalizing" and
//     "walArchivingFailing" give BackupWaiting. In these phases,
//     CloudNativePG has not reconciled the Backup yet, makes it, or tries it
//     again. The run waits up to its timeout.
//   - Any other value gives BackupUnknownPhase. The message names
//     status.phase and the value (see unknownBackupPhase). A new
//     CloudNativePG release can add a phase that does not change the
//     behaviour that the run uses. Thus the run waits for completed or
//     failed. The run shows the phase in its Ready message while it waits.
//     It also shows the phase in the message of the item if the run gets to
//     its timeout.
func BackupResult(ctx context.Context, c client.Reader, mapper meta.RESTMapper, namespace, name string) (BackupOutcome, error) {
	backup, err := served.Get(ctx, c, mapper, BackupGVK.GroupKind(), types.NamespacedName{Namespace: namespace, Name: name})
	if err != nil {
		return BackupOutcome{}, fmt.Errorf("get Backup %s/%s: %w", namespace, name, err)
	}
	phase, _, _ := unstructured.NestedString(backup.Object, "status", "phase")
	switch phase {
	case "completed":
		return BackupOutcome{Phase: BackupCompleted}, nil
	case "failed", "invalid backup definition":
		message, _, _ := unstructured.NestedString(backup.Object, "status", "error")
		if message == "" {
			message = fmt.Sprintf("the Backup %s/%s reports status.phase %q", namespace, name, phase)
		}
		return BackupOutcome{Phase: BackupFailed, Message: message}, nil
	case "", "pending", "started", "running", "finalizing", "walArchivingFailing":
		return BackupOutcome{Phase: BackupWaiting}, nil
	}
	return BackupOutcome{Phase: BackupUnknownPhase, Message: unknownBackupPhase(namespace, name, phase)}, nil
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
