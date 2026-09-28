//go:build e2e

package e2e

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// controllerNamespace is where hack/kind/controller.sh deploys the
// controller, and controllerSelector selects its pod.
const (
	controllerNamespace = "backup-system"
	controllerSelector  = "app.kubernetes.io/name=backup-controller"
)

// killController deletes the controller's pod at once, with no grace
// period, as a node failure or an OOM kill ends it. It waits up to three
// minutes until the Deployment runs a new pod.
func killController(t *testing.T) {
	t.Helper()
	kubectl(t, "", "-n", controllerNamespace, "delete", "pod", "-l", controllerSelector, "--grace-period=0", "--force", "--wait=false")
	kubectl(t, "", "-n", controllerNamespace, "rollout", "status", "deployment/backup-controller", "--timeout=3m")
}

// moverWatch records the largest number of mover pods that run at the same
// time in a namespace. A mover pod is any pod of VolSync or of the
// controller that reads or writes the restic repository.
type moverWatch struct {
	mu   sync.Mutex
	most int
	seen []string
	stop chan struct{}
	done chan struct{}
}

// isMover reports whether a pod name belongs to a mover: VolSync names its
// pods volsync-src-<source> and volsync-dst-<destination>, and the
// controller names its restore Jobs restore-<run>.
func isMover(name string) bool {
	return strings.HasPrefix(name, "volsync-") || strings.HasPrefix(name, "restore-")
}

// watchMovers starts to list the pods of the app's namespace once a second
// and to record the mover pods that run at the same time. Call end to stop.
func (a *app) watchMovers() *moverWatch {
	w := &moverWatch{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			var pods struct {
				Items []struct {
					Metadata struct{ Name string }  `json:"metadata"`
					Status   struct{ Phase string } `json:"status"`
				} `json:"items"`
			}
			if out, err := run(a.t.Context(), "", "-n", a.ns, "get", "pods", "-o", "json"); err == nil && jsonInto(out, &pods) == nil {
				var running []string
				for _, p := range pods.Items {
					if isMover(p.Metadata.Name) && p.Status.Phase == "Running" {
						running = append(running, p.Metadata.Name)
					}
				}
				w.mu.Lock()
				if len(running) > w.most {
					w.most, w.seen = len(running), running
				}
				w.mu.Unlock()
			}
			select {
			case <-w.stop:
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return w
}

// end stops the watch and returns the largest number of mover pods that ran
// at once, and their names at that moment.
func (w *moverWatch) end() (int, []string) {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.most, w.seen
}

// TestABackupAndARestoreOfOneClaimTakeTurns starts a backup and a restore of
// the same claim at the same moment. Two movers on one claim and one
// repository at once corrupt the claim or fight over restic's lock, so the
// runs take turns: never more than one mover pod runs, and both runs
// succeed.
func TestABackupAndARestoreOfOneClaimTakeTurns(t *testing.T) {
	t.Parallel()
	a := newApp(t, "turns")
	a.publish(volumeManifests())
	a.write("first")
	first := a.backup("first", "source: data")
	mustSucceed(t, "BackupRun", "first", first.Status.Phase, first.Status.Conditions, a.describe)
	a.write("second")

	movers := a.watchMovers()
	var wg sync.WaitGroup
	var backedUp backupv1alpha1.BackupRun
	var restored backupv1alpha1.RestoreRun
	wg.Add(2)
	go func() { defer wg.Done(); backedUp = a.backup("second", "source: data") }()
	go func() {
		defer wg.Done()
		restored = a.restore("back", "claim: data\npauseDuringRestore:\n  - {kind: Deployment, name: app}")
	}()
	wg.Wait()
	most, names := movers.end()

	mustSucceed(t, "BackupRun", "second", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)
	mustSucceed(t, "RestoreRun", "back", restored.Status.Phase, restored.Status.Conditions, a.describe)
	if most > 1 {
		t.Errorf("%d mover pods ran at once on claim data: %v", most, names)
	}
}

// TestTwoRestoresOfOneClusterDoNotFight creates two RestoreRuns of the same
// Cluster at the same moment. They take turns through the Cluster's Lease:
// one deletes and recovers the Cluster while the other waits with reason
// Busy, and then the other does the same. Both succeed. The later run deleted
// the Cluster the earlier run had recovered, so they never acted at once, and
// nothing replaces the Cluster after both finished.
func TestTwoRestoresOfOneClusterDoNotFight(t *testing.T) {
	t.Parallel()
	a := newApp(t, "two-restores")
	a.setUpDatabase("kept")
	base := a.backup("base", "database: db")
	mustSucceed(t, "BackupRun", "base", base.Status.Phase, base.Status.Conditions, a.describe)
	a.archived()

	var wg sync.WaitGroup
	runs := make([]backupv1alpha1.RestoreRun, 2)
	for i := range runs {
		wg.Add(1)
		go func() { defer wg.Done(); runs[i] = a.restore(fmt.Sprintf("restore-%d", i), "database: db") }()
	}
	wg.Wait()

	for _, r := range runs {
		mustSucceed(t, "RestoreRun", r.Name, r.Status.Phase, r.Status.Conditions, a.describe)
	}
	first, second := runs[0], runs[1]
	if second.Status.CompletedAt.Before(first.Status.CompletedAt) {
		first, second = second, first
	}
	deleted := func(r backupv1alpha1.RestoreRun) string {
		for _, item := range r.Status.Items {
			if item.Kind == "Cluster" {
				return item.ClusterUID
			}
		}
		return ""
	}
	if deleted(first) == deleted(second) {
		t.Errorf("both RestoreRuns deleted the Cluster with UID %s; the later one must delete the Cluster the earlier one recovered", deleted(first))
	}

	recovered := a.waitHealthy("the recovered Cluster db")
	time.Sleep(time.Minute)
	if now := a.waitHealthy("Cluster db a minute later"); now != recovered {
		t.Errorf("Cluster db was replaced again after both restores: UID %s, then %s", recovered, now)
	}
	if got := a.rows(); strings.Join(got, "|") != "kept" {
		t.Errorf("rows = %q, want [kept]", got)
	}
}

// TestAFailedBackupResumesTheApp backs up a namespace whose restic Secret
// holds the wrong password for an existing repository, so the mover fails.
// VolSync retries the mover up to its Job's backoff limit of 8 and then
// records the result Failed, about 20 minutes later. The run notices that
// result and ends Failed with a reason on the item, well before its
// 45-minute timeout, and the app runs again.
func TestAFailedBackupResumesTheApp(t *testing.T) {
	t.Parallel()
	a := newApp(t, "failed")
	a.publish(volumeManifests())
	a.write("state")
	first := a.backup("first", "source: data")
	mustSucceed(t, "BackupRun", "first", first.Status.Phase, first.Status.Conditions, a.describe)

	kubectl(t, "", "-n", a.ns, "patch", "secret", "restic", "--type=merge", "-p", `{"stringData":{"RESTIC_PASSWORD":"wrong"}}`)
	failed := a.backupWithin("wrong-password", "all: true\ntimeout: 45m", 50*time.Minute)
	if failed.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("BackupRun wrong-password ended %s, want Failed\n%s", failed.Status.Phase, a.describe())
	}
	if reason := readyReason(failed.Status.Conditions); reason == backupv1alpha1.ReasonTimedOut {
		t.Errorf("BackupRun wrong-password failed only at its timeout; the run has to notice the failed mover")
	}
	for _, item := range failed.Status.Items {
		if item.Kind == "ReplicationSource" && item.Phase != backupv1alpha1.ItemFailed {
			t.Errorf("item %s is %s, want Failed", item.Name, item.Phase)
		}
	}
	a.waitNote("the app to run again after the failed backup", "state")
}

// TestAKilledControllerFinishesThePausedBackup kills the controller while a
// paused backup has the app scaled to 0. The new controller pod finishes the
// run, the app gets its replicas back, and the snapshot holds the file.
func TestAKilledControllerFinishesThePausedBackup(t *testing.T) {
	t.Parallel()
	a := newApp(t, "killed")
	a.publish(volumeManifests())
	a.write("before the kill")

	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: BackupRun\nmetadata:\n  name: paused\n  namespace: %s\nspec:\n  all: true\n", a.ns))
	waitFor(t, "the run to pause the app", 5*time.Minute, func() (bool, string) {
		var d struct {
			Spec struct{ Replicas int } `json:"spec"`
		}
		if err := getJSON(a.ns, "deployment", "app", &d); err != nil {
			return false, err.Error()
		}
		return d.Spec.Replicas == 0, fmt.Sprintf("replicas=%d", d.Spec.Replicas)
	}, a.describe)
	killController(t)

	var run backupv1alpha1.BackupRun
	waitFor(t, "BackupRun paused to end after the kill", 15*time.Minute, func() (bool, string) {
		run = backupv1alpha1.BackupRun{}
		if err := getJSON(a.ns, "backuprun", "paused", &run); err != nil {
			return false, err.Error()
		}
		return run.Status.Phase.Finished(), string(run.Status.Phase) + " " + ready(run.Status.Conditions)
	}, a.describe)
	mustSucceed(t, "BackupRun", "paused", run.Status.Phase, run.Status.Conditions, a.describe)
	a.waitNote("the app to run again after the kill", "before the kill")
	if s := newest(t, a.snapshots()); !s.hasTag(pausedTag) {
		t.Errorf("snapshot %s has tags %v, want %s", s.ID, s.Tags, pausedTag)
	}
}

// TestAKilledControllerFinishesTheDatabaseRestore kills the controller right
// after a RestoreRun deleted the Cluster. The new controller pod lets the
// webhook recover the Cluster for the run, and the run succeeds with the
// rows back.
func TestAKilledControllerFinishesTheDatabaseRestore(t *testing.T) {
	t.Parallel()
	a := newApp(t, "killed-restore")
	a.setUpDatabase("survives")
	base := a.backup("base", "database: db")
	mustSucceed(t, "BackupRun", "base", base.Status.Phase, base.Status.Conditions, a.describe)
	a.archived()

	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: RestoreRun\nmetadata:\n  name: back\n  namespace: %s\nspec:\n  database: db\n", a.ns))
	waitFor(t, "the run to delete Cluster db", 5*time.Minute, func() (bool, string) {
		var r backupv1alpha1.RestoreRun
		if err := getJSON(a.ns, "restorerun", "back", &r); err != nil {
			return false, err.Error()
		}
		for _, item := range r.Status.Items {
			if item.Kind == "Cluster" {
				return item.Phase == backupv1alpha1.ItemDeleted, string(item.Phase)
			}
		}
		return false, string(r.Status.Phase)
	}, a.describe)
	killController(t)

	var r backupv1alpha1.RestoreRun
	waitFor(t, "RestoreRun back to end after the kill", 20*time.Minute, func() (bool, string) {
		r = backupv1alpha1.RestoreRun{}
		if err := getJSON(a.ns, "restorerun", "back", &r); err != nil {
			return false, err.Error()
		}
		return r.Status.Phase.Finished(), string(r.Status.Phase) + " " + ready(r.Status.Conditions)
	}, a.describe)
	mustSucceed(t, "RestoreRun", "back", r.Status.Phase, r.Status.Conditions, a.describe)
	a.waitHealthy("the recovered Cluster db")
	if got := a.rows(); strings.Join(got, "|") != "survives" {
		t.Errorf("rows = %q, want [survives]", got)
	}
}

// stuckManifests returns the app of volumeManifests with a pod that ignores
// SIGTERM and has a grace period of 15 minutes, so a paused app's pod stays
// and a BackupRun with all: true waits for it.
func stuckManifests() string {
	manifests := strings.Replace(volumeManifests(), "terminationGracePeriodSeconds: 1", "terminationGracePeriodSeconds: 900", 1)
	return strings.Replace(manifests, `command: [sleep, "86400"]`, `command: [sh, -c, "trap '' TERM; while true; do sleep 1; done"]`, 1)
}

// waitResumed waits up to three minutes until the Deployment app runs one
// replica again and the Flux Kustomization of the app is no longer
// suspended.
func (a *app) waitResumed() {
	a.t.Helper()
	waitFor(a.t, "the app and its Kustomization to be resumed", 3*time.Minute, func() (bool, string) {
		var d struct {
			Spec struct{ Replicas int } `json:"spec"`
		}
		var k struct {
			Spec struct{ Suspend bool } `json:"spec"`
		}
		if err := getJSON(a.ns, "deployment", "app", &d); err != nil {
			return false, err.Error()
		}
		if err := getJSON("flux-system", "kustomization", a.ns, &k); err != nil {
			return false, err.Error()
		}
		return d.Spec.Replicas == 1 && !k.Spec.Suspend, fmt.Sprintf("replicas=%d suspend=%v", d.Spec.Replicas, k.Spec.Suspend)
	}, a.describe)
}

// TestARunPastItsTimeoutGivesTheAppBack pauses an app whose pod ignores
// SIGTERM and has a grace period of 15 minutes, so the paused pod stays and
// the run waits for it. The run's timeout of 2 minutes passes first: the run
// ends Failed with reason TimedOut, scales the Deployment back to 1, and
// resumes the Flux Kustomization it suspended.
func TestARunPastItsTimeoutGivesTheAppBack(t *testing.T) {
	t.Parallel()
	a := newApp(t, "timeout")
	a.publish(stuckManifests())
	a.write("state")

	run := a.backupWithin("stuck", "all: true\ntimeout: 2m", 10*time.Minute)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonTimedOut {
		t.Fatalf("BackupRun stuck ended %s %s, want Failed TimedOut\n%s", run.Status.Phase, ready(run.Status.Conditions), a.describe())
	}
	if len(run.Status.Paused) == 0 {
		t.Fatalf("the run never paused the app, so the timeout test proves nothing\n%s", a.describe())
	}
	a.waitResumed()
}

// TestAnInvalidTimeoutDuringAPauseGivesTheAppBack pauses an app whose pod
// stays, so the BackupRun waits with the app down. The admin then changes
// the namespace's backup.wlz.li/timeout to a value that does not parse. The
// run can't tell its deadline any more: it ends Failed with reason Invalid
// and gives the app back.
func TestAnInvalidTimeoutDuringAPauseGivesTheAppBack(t *testing.T) {
	t.Parallel()
	a := newApp(t, "bad-timeout")
	a.publish(stuckManifests())
	a.write("state")

	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: BackupRun\nmetadata:\n  name: stuck\n  namespace: %s\nspec:\n  all: true\n", a.ns))
	var run backupv1alpha1.BackupRun
	waitFor(t, "the run to pause the app", 5*time.Minute, func() (bool, string) {
		run = backupv1alpha1.BackupRun{}
		if err := getJSON(a.ns, "backuprun", "stuck", &run); err != nil {
			return false, err.Error()
		}
		return run.Status.PausedAt != nil, string(run.Status.Phase) + " " + ready(run.Status.Conditions)
	}, a.describe)
	kubectl(t, "", "annotate", "--overwrite", "namespace", a.ns, backupv1alpha1.AnnotationTimeout+"=2 hours")

	waitFor(t, "BackupRun stuck to end", 5*time.Minute, func() (bool, string) {
		run = backupv1alpha1.BackupRun{}
		if err := getJSON(a.ns, "backuprun", "stuck", &run); err != nil {
			return false, err.Error()
		}
		return run.Status.Phase.Finished(), string(run.Status.Phase) + " " + ready(run.Status.Conditions)
	}, a.describe)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid {
		t.Fatalf("BackupRun stuck ended %s %s, want Failed Invalid\n%s", run.Status.Phase, ready(run.Status.Conditions), a.describe())
	}
	a.waitResumed()
}
