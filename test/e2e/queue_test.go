//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// readyMessage returns the message of a run's Ready condition, or an empty
// string when the run has none.
func readyMessage(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == backupv1alpha1.ConditionReady {
			return c.Message
		}
	}
	return ""
}

// TestARunWithoutALocalQueueWaitsForOne backs up a claim in a namespace that
// has no Kueue LocalQueue. The BackupRun stays Queued with a message that
// names the missing LocalQueue, and for a minute it starts nothing: no
// startedAt, no Workload and no ReplicationSource. Once the LocalQueue
// exists, Kueue admits the run and it succeeds. So a namespace whose
// manifests lack the LocalQueue can't escape the ClusterQueue's quota.
func TestARunWithoutALocalQueueWaitsForOne(t *testing.T) {
	t.Parallel()
	a := newApp(t, "queue")
	kubectl(t, "", "-n", a.ns, "delete", "localqueue", "backups", "--wait=true")
	a.publish(volumeManifests())
	a.write("queued")

	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: BackupRun\nmetadata:\n  name: waits\n  namespace: %s\nspec:\n  source: data", a.ns))
	var run backupv1alpha1.BackupRun
	get := func() error {
		run = backupv1alpha1.BackupRun{}
		return getJSON(a.ns, "backuprun", "waits", &run)
	}
	waitFor(t, "the run to name the missing LocalQueue", 2*time.Minute, func() (bool, string) {
		if err := get(); err != nil {
			return false, err.Error()
		}
		return strings.Contains(readyMessage(run.Status.Conditions), "has no Kueue LocalQueue"),
			fmt.Sprintf("%s %s", run.Status.Phase, readyMessage(run.Status.Conditions))
	}, a.describe)

	for end := time.Now().Add(time.Minute); time.Now().Before(end); time.Sleep(5 * time.Second) {
		if err := get(); err != nil {
			t.Fatal(err)
		}
		if run.Status.Phase != backupv1alpha1.RunPhaseQueued || run.Status.StartedAt != nil || run.Status.Workload != "" {
			t.Fatalf("the run started without a LocalQueue: phase %s, startedAt %v, workload %q\n%s",
				run.Status.Phase, run.Status.StartedAt, run.Status.Workload, a.describe())
		}
		if sources := kubectl(t, "", "-n", a.ns, "get", "replicationsources", "-o", "name"); strings.TrimSpace(sources) != "" {
			t.Fatalf("the run wrote %s without a LocalQueue\n%s", sources, a.describe())
		}
	}

	apply(t, fmt.Sprintf("apiVersion: kueue.x-k8s.io/v1beta2\nkind: LocalQueue\nmetadata:\n  name: backups\n  namespace: %s\nspec:\n  clusterQueue: backup", a.ns))
	var queue struct {
		Metadata metav1.ObjectMeta `json:"metadata"`
	}
	if err := getJSON(a.ns, "localqueue", "backups", &queue); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "BackupRun waits to end", 15*time.Minute, func() (bool, string) {
		if err := get(); err != nil {
			return false, err.Error()
		}
		return run.Status.Phase.Finished(), fmt.Sprintf("%s %s", run.Status.Phase, ready(run.Status.Conditions))
	}, a.describe)
	mustSucceed(t, "BackupRun", "waits", run.Status.Phase, run.Status.Conditions, a.describe)
	if run.Status.StartedAt == nil || run.Status.StartedAt.Before(&queue.Metadata.CreationTimestamp) {
		t.Fatalf("the run started at %v, before the LocalQueue existed at %v\n%s", run.Status.StartedAt, queue.Metadata.CreationTimestamp, a.describe())
	}
}
