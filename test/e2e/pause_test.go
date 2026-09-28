//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// controllerArgs returns the arguments of the controller's container.
func controllerArgs(t *testing.T) []string {
	t.Helper()
	var d struct {
		Spec struct {
			Template struct {
				Spec struct {
					Containers []struct {
						Args []string `json:"args"`
					} `json:"containers"`
				} `json:"spec"`
			} `json:"template"`
		} `json:"spec"`
	}
	if err := getJSON(controllerNamespace, "deployment", "backup-controller", &d); err != nil {
		t.Fatal(err)
	}
	return d.Spec.Template.Spec.Containers[0].Args
}

// setControllerArgs replaces the arguments of the controller's container and
// waits until the new controller pod runs.
func setControllerArgs(t *testing.T, args []string) {
	t.Helper()
	patch, err := json.Marshal([]map[string]any{{"op": "replace", "path": "/spec/template/spec/containers/0/args", "value": args}})
	if err != nil {
		t.Fatal(err)
	}
	kubectl(t, "", "-n", controllerNamespace, "patch", "deployment", "backup-controller", "--type=json", "-p", string(patch))
	kubectl(t, "", "-n", controllerNamespace, "rollout", "status", "deployment/backup-controller", "--timeout=3m")
}

// TestTheControllerWithPauseFinishesStartedRunsAndStartsNoNewOne pauses the
// controller with --pause, as an admin does before an upgrade, while a
// database restore is in the middle of its work: it has deleted the Cluster
// and waits for Flux to create it again. That run finishes during the pause
// and the database holds its rows. A run created during the pause waits with
// reason ControllerPaused and changes nothing. Once the controller runs
// without --pause, the waiting run goes through admission and backs up the
// database: its item records the base backup.
func TestTheControllerWithPauseFinishesStartedRunsAndStartsNoNewOne(t *testing.T) {
	a := newApp(t, "controller-pause")
	a.setUpDatabase("before the pause")
	base := a.backup("base", "database: db")
	mustSucceed(t, "BackupRun", "base", base.Status.Phase, base.Status.Conditions, a.describe)
	a.archived()

	args := controllerArgs(t)
	t.Cleanup(func() { setControllerArgs(t, args) })

	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: RestoreRun\nmetadata:\n  name: started\n  namespace: %s\nspec:\n  database: db\n", a.ns))
	waitFor(t, "RestoreRun started to delete Cluster db", 5*time.Minute, func() (bool, string) {
		var r backupv1alpha1.RestoreRun
		if err := getJSON(a.ns, "restorerun", "started", &r); err != nil {
			return false, err.Error()
		}
		for _, item := range r.Status.Items {
			if item.Kind == "Cluster" {
				return item.Phase == backupv1alpha1.ItemDeleted, string(item.Phase)
			}
		}
		return false, string(r.Status.Phase)
	}, a.describe)

	setControllerArgs(t, append(slices.Clone(args), "--pause"))
	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: BackupRun\nmetadata:\n  name: during-the-pause\n  namespace: %s\nspec:\n  database: db\n", a.ns))

	var started backupv1alpha1.RestoreRun
	waitFor(t, "RestoreRun started to finish during the pause", 15*time.Minute, func() (bool, string) {
		started = backupv1alpha1.RestoreRun{}
		if err := getJSON(a.ns, "restorerun", "started", &started); err != nil {
			return false, err.Error()
		}
		return started.Status.Phase.Finished(), string(started.Status.Phase) + " " + ready(started.Status.Conditions)
	}, a.describe)
	mustSucceed(t, "RestoreRun", "started", started.Status.Phase, started.Status.Conditions, a.describe)
	a.waitHealthy("the recovered Cluster db")
	if got := a.rows(); strings.Join(got, "|") != "before the pause" {
		t.Errorf("rows after the restore during the pause = %q, want [before the pause]", got)
	}

	var waiting backupv1alpha1.BackupRun
	if err := getJSON(a.ns, "backuprun", "during-the-pause", &waiting); err != nil {
		t.Fatal(err)
	}
	if reason := readyReason(waiting.Status.Conditions); reason != backupv1alpha1.ReasonControllerPaused || waiting.Status.StartedAt != nil {
		t.Fatalf("BackupRun during-the-pause: reason %q, startedAt %v; want %s and not started\n%s",
			reason, waiting.Status.StartedAt, backupv1alpha1.ReasonControllerPaused, a.describe())
	}

	setControllerArgs(t, args)
	waitFor(t, "BackupRun during-the-pause to finish after the pause", 15*time.Minute, func() (bool, string) {
		waiting = backupv1alpha1.BackupRun{}
		if err := getJSON(a.ns, "backuprun", "during-the-pause", &waiting); err != nil {
			return false, err.Error()
		}
		return waiting.Status.Phase.Finished(), string(waiting.Status.Phase)
	}, a.describe)
	mustSucceed(t, "BackupRun", "during-the-pause", waiting.Status.Phase, waiting.Status.Conditions, a.describe)
	// Only admission sets startedAt; the Workload's name is cleared when the
	// run ends.
	if waiting.Status.StartedAt == nil {
		t.Fatalf("BackupRun during-the-pause succeeded without being admitted\n%s", a.describe())
	}
	if len(waiting.Status.Items) != 1 || waiting.Status.Items[0].Kind != "Cluster" ||
		waiting.Status.Items[0].Phase != backupv1alpha1.ItemSucceeded || waiting.Status.Items[0].BaseBackup == "" {
		t.Fatalf("BackupRun during-the-pause succeeded without backing up Cluster db: items %+v\n%s", waiting.Status.Items, a.describe())
	}
}

// readyReason returns the reason of a run's Ready condition, or an empty
// string when it has none yet.
func readyReason(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == backupv1alpha1.ConditionReady {
			return c.Reason
		}
	}
	return ""
}
