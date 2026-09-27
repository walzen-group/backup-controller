package populator

import (
	"context"
	"testing"

	"github.com/walzen-group/backup-controller/internal/restic"

	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/utils/ptr"
)

// A claim with no restore Job waits while the controller runs with --pause.
// The sync returns the library's "not yet" result with no error, and the
// callbacks create no Job and no Secret copy and write no status. An empty
// repository does not bind the claim empty either.
func TestAPausedPopulatorStartsNoRestore(t *testing.T) {
	ctx := context.Background()
	for name, snapshots := range map[string][]restic.Snapshot{"a snapshot": {monday}, "an empty repository": nil} {
		t.Run(name, func(t *testing.T) {
			ops := populatedOperations(t)
			callbacks := newCallbacks(ops, snapshots...)
			callbacks.Pause()
			if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
				t.Fatalf("sync while paused = %t, %v; want not done and no error", done, err)
			}
			if job := ops.job(t, "claim-123"); job != nil {
				t.Errorf("Job %s created while paused", job.Name)
			}
			if _, ok := ops.secrets[namespacedName(controllerNS, "claim-123")]; ok {
				t.Error("the Secret copy was created while paused")
			}
			if len(ops.statuses) != 0 {
				t.Errorf("status writes = %d while paused, want none", len(ops.statuses))
			}
		})
	}
}

// A claim whose restore Job exists goes on while the controller runs with
// --pause: the next sync resumes the recorded Job.
func TestAPausedPopulatorContinuesAClaimWithAJob(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	callbacks := newCallbacks(ops, monday)
	if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
		t.Fatalf("first sync = %t, %v; want not done", done, err)
	}
	ops.setJob(t, "claim-123", func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })

	callbacks.Pause()
	if done, err := librarySync(ctx, callbacks, params()); err != nil || done {
		t.Fatalf("sync while paused = %t, %v; want not done", done, err)
	}
	if ptr.Deref(ops.job(t, "claim-123").Spec.Suspend, true) {
		t.Error("the recorded Job is still suspended while paused, want it resumed")
	}
}
