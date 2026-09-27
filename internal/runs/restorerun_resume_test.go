package runs

import (
	"context"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The tests in this file cover the suspend at create: a run creates each
// restore Job suspended and resumes it only once a status write that
// records the Job's name and UID has gone through. A Job the run has not
// recorded, such as one whose create answered 504 and was stored later,
// never runs restic.

// storedItemJob returns the Job name and UID the stored run's first item
// records.
func storedItemJob(t *testing.T, c client.Client) (string, types.UID) {
	t.Helper()
	item := readRestoreRun(t, c).Status.Items[0]
	return item.Job, item.JobUID
}

// onlyJob returns the one restore Job in the test namespace.
func onlyJob(t *testing.T, c client.Client) *batchv1.Job {
	t.Helper()
	jobs := restoreJobs(t, c)
	if len(jobs) != 1 {
		t.Fatalf("restore Jobs = %v, want one", jobs)
	}
	return &jobs[0]
}

// A run creates its restore Job suspended, and resumes it in a later pass,
// once the status holding the Job's name and UID is stored. When the write
// that would record the Job is lost, the pass that takes the Job over
// records it and still leaves it suspended; the pass after that resumes it.
// That holds for an in-place and an into restore.
func TestARestoreJobIsResumedOnlyOnceTheRunRecordedIt(t *testing.T) {
	inPlace := func(t *testing.T) (*RestoreRunReconciler, client.Client) {
		r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
			claim(), volumeRestore(), repository())
		restoreStep(t, r) // plan
		return r, c
	}
	into := func(t *testing.T) (*RestoreRunReconciler, client.Client) {
		return restoreReconciler(t, nil, plannedInto(), sourceOnNode(), volumeRestore(), repository())
	}
	for name, tc := range map[string]struct {
		setup func(t *testing.T) (*RestoreRunReconciler, client.Client)
		lose  bool
	}{
		"in place":                    {setup: inPlace},
		"in place, status write lost": {setup: inPlace, lose: true},
		"into":                        {setup: into},
		"into, status write lost":     {setup: into, lose: true},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := tc.setup(t)
			if tc.lose {
				r.Client = loseNextStatusWrite(c)
				if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
					t.Fatal("the pass whose status write was lost succeeded, want the error returned")
				}
				r.Client = c
			} else {
				restoreStep(t, r) // creates the Job
			}
			job := onlyJob(t, c)
			if !suspendedJob(t, c, job.Name) {
				t.Fatalf("restore Job %s runs after the pass that created it, want it suspended until the run records it", job.Name)
			}
			if tc.lose {
				if _, uid := storedItemJob(t, c); uid != "" {
					t.Fatalf("stored item records Job UID %s after the lost write, want none", uid)
				}
				restoreStep(t, r) // takes the Job over
				if !suspendedJob(t, c, job.Name) {
					t.Fatalf("restore Job %s runs after the pass that took it over, want it suspended until that record is stored", job.Name)
				}
			}
			if name, uid := storedItemJob(t, c); name != job.Name || uid != job.UID {
				t.Fatalf("stored item records Job %s with UID %s, want %s with UID %s", name, uid, job.Name, job.UID)
			}

			restoreStep(t, r)
			if suspendedJob(t, c, job.Name) {
				t.Errorf("restore Job %s is still suspended once the run recorded it, want it resumed", job.Name)
			}
			if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.JobUID != job.UID {
				t.Errorf("item = %+v, want Running on Job UID %s", item, job.UID)
			}
		})
	}
}
