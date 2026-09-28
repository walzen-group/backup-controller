package bootstrap

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// ClusterLabel is the label CloudNativePG puts on each instance pod and PVC it
// creates for a Cluster. Its value is the Cluster's name.
const ClusterLabel = "cnpg.io/cluster"

// InstanceLeft looks for an instance pod or PVC of a Cluster that is still in
// the namespace. A RestoreRun calls it after it deletes a Cluster. Until
// nothing is left, the run waits with reason WaitingForShutdown, and it
// resumes the workloads it stopped only after that. The webhook refuses a
// new Cluster of that name while one is left.
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
func InstanceLeft(ctx context.Context, c client.Reader, namespace, name string) (string, error) {
	selector := client.MatchingLabels{ClusterLabel: name}
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
