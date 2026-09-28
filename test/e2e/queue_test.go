//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"sync"
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

// watchAdmitted counts the admitted Workloads every three seconds (see
// admittedWorkloads) until the returned function is called, which returns
// the highest count seen.
func watchAdmitted(t *testing.T) func() int {
	var mu sync.Mutex
	most := 0
	stop, done := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(done)
		for {
			if n, err := admittedWorkloads(t); err == nil {
				mu.Lock()
				most = max(most, n)
				mu.Unlock()
			}
			select {
			case <-stop:
				return
			case <-time.After(3 * time.Second):
			}
		}
	}()
	return func() int {
		close(stop)
		<-done
		mu.Lock()
		defer mu.Unlock()
		return most
	}
}

// TestTenNamespacesShareOneQueue checks that the ClusterQueue's quota of five
// bounds runs and populator restores alike. Ten namespaces reach their
// schedule at one tick: at most five BackupRuns are admitted at once, and all
// succeed. Then all ten claims are deleted and applied again at once, as a
// rebuild does, and the populator fills each from its backup: at most five
// restores are admitted at once, and every claim holds its file. The app
// writes nothing after the rebuild, so the file comes from the backup. It
// uses the whole quota, so it does not run in parallel with other scenarios.
func TestTenNamespacesShareOneQueue(t *testing.T) {
	const namespaces, quota = 10, 5
	apps := make([]*app, namespaces)
	for i := range apps {
		apps[i] = newApp(t, fmt.Sprintf("share-%02d", i))
		kubectl(t, volumeManifests(), "-n", apps[i].ns, "apply", "-f", "-")
	}
	for _, a := range apps {
		a.write("file of " + a.ns)
	}
	most := watchAdmitted(t)

	tick := time.Now().UTC().Add(2 * time.Minute).Truncate(time.Minute)
	schedule := fmt.Sprintf("CRON_TZ=UTC %d %d * * *", tick.Minute(), tick.Hour())
	for _, a := range apps {
		kubectl(t, "", "annotate", "--overwrite", "namespace", a.ns, backupv1alpha1.AnnotationSchedule+"="+schedule)
	}
	for _, a := range apps {
		waitFor(t, "the scheduled BackupRun of "+a.ns+" to end", 20*time.Minute, func() (bool, string) {
			var runs struct {
				Items []backupv1alpha1.BackupRun `json:"items"`
			}
			out, err := run(t.Context(), "", "-n", a.ns, "get", "backupruns", "-o", "json")
			if err != nil || jsonInto(out, &runs) != nil || len(runs.Items) == 0 {
				return false, "no run yet"
			}
			r := runs.Items[0]
			if r.Status.Phase.Finished() && r.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
				t.Fatalf("the scheduled run of %s ended %s: %s", a.ns, r.Status.Phase, ready(r.Status.Conditions))
			}
			return r.Status.Phase.Finished(), string(r.Status.Phase)
		}, a.describe)
	}
	backups := most()
	if backups > quota {
		t.Errorf("%d BackupRuns were admitted at once, want at most %d", backups, quota)
	}

	most = watchAdmitted(t)
	for _, a := range apps {
		kubectl(t, "", "-n", a.ns, "delete", "deployment", "app", "--wait=false")
		kubectl(t, "", "-n", a.ns, "delete", "pvc", "data", "--wait=false")
	}
	for _, a := range apps {
		waitFor(t, "the claim of "+a.ns+" to be gone", 5*time.Minute, func() (bool, string) {
			var claim struct{}
			return getJSON(a.ns, "pvc", "data", &claim) != nil, "still there"
		}, a.describe)
	}
	for _, a := range apps {
		kubectl(t, volumeManifests(), "-n", a.ns, "apply", "-f", "-")
	}
	for _, a := range apps {
		a.waitNote("the app of "+a.ns+" to read its file from the filled claim", "file of "+a.ns)
	}
	fills := most()
	if fills > quota {
		t.Errorf("%d populator restores were admitted at once, want at most %d", fills, quota)
	}
	t.Logf("at most %d BackupRuns and %d populator restores were admitted at once", backups, fills)
}
