//go:build e2e

package e2e

import (
	"sync"
	"testing"
	"time"
)

// pausedTag is the restic tag a BackupRun gives a snapshot it took while the
// app was paused.
const pausedTag = "paused"

// replicaWatch records the replica counts that the Deployment app goes
// through while a test runs.
type replicaWatch struct {
	mu   sync.Mutex
	seen []int
	stop chan struct{}
	done chan struct{}
}

// watchReplicas starts to read spec.replicas of the Deployment app once a
// second, and records each value that differs from the one before. Call stop
// to end the watch and get the values.
func (a *app) watchReplicas() *replicaWatch {
	w := &replicaWatch{stop: make(chan struct{}), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		for {
			var d struct {
				Spec struct{ Replicas int } `json:"spec"`
			}
			if err := getJSON(a.ns, "deployment", "app", &d); err == nil {
				w.mu.Lock()
				if len(w.seen) == 0 || w.seen[len(w.seen)-1] != d.Spec.Replicas {
					w.seen = append(w.seen, d.Spec.Replicas)
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

// end stops the watch and returns the replica counts it saw, in order.
func (w *replicaWatch) end() []int {
	close(w.stop)
	<-w.done
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]int(nil), w.seen...)
}

// TestAPausedBackupPausesTheAppAndTagsTheSnapshot backs up a namespace with
// all: true. The run scales the annotated Deployment to 0 while it copies,
// and back to its replica count afterwards. The new snapshot holds the
// app's file, carries the paused tag, and its time is the moment the run
// resumed the app.
func TestAPausedBackupPausesTheAppAndTagsTheSnapshot(t *testing.T) {
	t.Parallel()
	a := newApp(t, "paused")
	a.publish(volumeManifests())
	a.write("paused state")

	watch := a.watchReplicas()
	run := a.backup("paused", "all: true")
	seen := watch.end()
	mustSucceed(t, "BackupRun", "paused", run.Status.Phase, run.Status.Conditions, a.describe)

	if len(seen) < 3 || seen[0] != 1 || seen[len(seen)-1] != 1 || !contains(seen, 0) {
		t.Errorf("replicas of Deployment app went %v, want 1, then 0 during the backup, then 1", seen)
	}
	resumed := run.Status.ResumedAt
	if resumed == nil {
		t.Fatalf("BackupRun paused records no resume time\n%s", a.describe())
	}
	s := newest(t, a.snapshots())
	if !s.hasTag(pausedTag) {
		t.Errorf("snapshot %s has tags %v, want %s", s.ID, s.Tags, pausedTag)
	}
	if !s.Time.Equal(resumed.Time) {
		t.Errorf("snapshot %s has time %s, want the resume time %s", s.ID, s.Time, resumed.Time)
	}
	a.waitNote("the app to run again with its file", "paused state")
}

// contains reports whether a list of replica counts holds a value.
func contains(list []int, value int) bool {
	for _, v := range list {
		if v == value {
			return true
		}
	}
	return false
}
