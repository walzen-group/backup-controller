package runs

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The tests in this file cover when a run resumes the restore Job it
// created suspended: only in a pass that goes on with the restore, only
// when the stored run still records the Job, and how the run reads a
// resume the API server did not take.

// resumeLog counts the patches that clear a Job's spec.suspend.
type resumeLog struct{ sent int }

// watchResumes returns a client over c that counts every patch that clears
// a Job's spec.suspend, and the log it counts in.
//
// Parameters:
//   - c is the test's client.
//   - answer, when not nil, sees each such patch before c does, with the
//     count so far, the patched Job and the underlying client. An error it
//     returns is the patch's answer, and the patch never reaches c. It
//     returns nil to let the patch through.
func watchResumes(c client.Client, answer func(n int, job client.Object, cl client.WithWatch) error) (client.Client, *resumeLog) {
	log := &resumeLog{}
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			data, err := patch.Data(obj)
			if err != nil {
				return err
			}
			if _, ok := obj.(*batchv1.Job); ok && bytes.Contains(data, []byte(`"suspend":false`)) {
				log.sent++
				if answer != nil {
					if err := answer(log.sent, obj, cl); err != nil {
						return err
					}
				}
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	}), log
}

// unresumed marks a restore Job as Build creates it and no pass has
// resumed it yet.
func unresumed(job *batchv1.Job) *batchv1.Job {
	job.Spec.Suspend = ptr.To(true)
	return job
}

// recordedInto returns the RestoreRun back-to-monday as an into restore
// whose item records its restore Job, and that Job, still suspended since
// its create.
func recordedInto(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job) {
	t.Helper()
	run := plannedInto()
	run.Status.Items[0].Job, run.Status.Items[0].JobUID = jobName(restoreUID, 0), jobUID
	return run, unresumed(restoreJobFor(t, run, run.Spec.Into, monday.ID))
}

// A run that ends in the pass that reads its recorded, still suspended
// restore Job never resumes it: a stop right after a resume could miss a
// pod the Job controller creates for it. That holds for an in-place run that
// times out or aborts, and for an into run that times out or finds its
// claim lost. The run ends with the reason of its ending, and the Job
// stays suspended until the run deletes it.
func TestARunThatEndsNeverResumesItsJob(t *testing.T) {
	late := func() time.Time { return frozen.Add(5 * time.Hour) }
	for name, tc := range map[string]struct {
		setup  func(t *testing.T) ([]client.Object, *batchv1.Job)
		now    func() time.Time
		reason string
	}{
		"in place, timed out": {setup: func(t *testing.T) ([]client.Object, *batchv1.Job) {
			run, job := quiescedMidRestore(t)
			return []client.Object{run, claim(), volumeRestore(), repository(), stoppedDeployment(), kustomization(true), unresumed(job)}, job
		}, now: late, reason: backupv1alpha1.ReasonTimedOut},
		"in place, aborted": {setup: func(t *testing.T) ([]client.Object, *batchv1.Job) {
			// spec.quiesce names a Deployment the namespace no longer
			// holds, and a second item is still Pending, so the pass
			// aborts before it restores anything.
			run, job := quiescedMidRestore(t)
			pending := runningOnJob(cacheN, 1)
			pending.Phase, pending.Job, pending.JobUID = backupv1alpha1.ItemPending, "", ""
			run.Status.Items = append(run.Status.Items, pending)
			return []client.Object{run, claim(), volumeRestore(), repository(), kustomization(true), unresumed(job)}, job
		}, now: frozenNow, reason: backupv1alpha1.ReasonFailed},
		"into, timed out": {setup: func(t *testing.T) ([]client.Object, *batchv1.Job) {
			run, job := recordedInto(t)
			return []client.Object{run, sourceOnNode(), volumeRestore(), repository(), intoClaim(run), job}, job
		}, now: late, reason: backupv1alpha1.ReasonTimedOut},
		"into, claim lost": {setup: func(t *testing.T) ([]client.Object, *batchv1.Job) {
			run, job := recordedInto(t)
			return []client.Object{run, sourceOnNode(), volumeRestore(), repository(), job}, job
		}, now: frozenNow, reason: backupv1alpha1.ReasonFailed},
	} {
		t.Run(name, func(t *testing.T) {
			objects, job := tc.setup(t)
			r, c := restoreReconciler(t, nil, objects...)
			r.Now = tc.now
			var log *resumeLog
			r.Client, log = watchResumes(c, nil)
			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}

			_, _ = r.Reconcile(context.Background(), req)
			ending := readRestoreRun(t, c).Status.Ending
			if ending == nil || ending.Reason != tc.reason {
				t.Fatalf("ending = %+v, want the run ending with reason %s", ending, tc.reason)
			}
			markSuspended(t, c, job)
			for range 3 {
				_, _ = r.Reconcile(context.Background(), req)
			}
			if log.sent != 0 {
				t.Errorf("the run sent %d resumes of its restore Job while it ended, want none", log.sent)
			}
			if jobs := restoreJobs(t, c); len(jobs) != 0 {
				t.Errorf("restore Jobs = %v, want the suspended Job deleted", jobs)
			}
		})
	}
}

// failResumes returns an answer for watchResumes that refuses the first
// resume with err and lets every later one through.
func failResumes(err error) func(int, client.Object, client.WithWatch) error {
	return func(n int, _ client.Object, _ client.WithWatch) error {
		if n == 1 {
			return err
		}
		return nil
	}
}

// A resume that fails for a reason a retry may fix, such as a 500 from an
// unreachable Kueue webhook, a 503 or a 504 Timeout, fails nothing: the pass
// returns the error, the item stays Running on its suspended Job, and the
// next pass resumes it.
func TestATransientResumeErrorIsRetried(t *testing.T) {
	for name, err := range map[string]error{
		"500 webhook down": apierrors.NewInternalError(errors.New(`failed calling webhook "mjob.kb.io": connect: connection refused`)),
		"503":              apierrors.NewServiceUnavailable("the server is currently unable to handle the request"),
		"504":              apierrors.NewTimeoutError("the request timed out", 0),
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }),
				claim(), volumeRestore(), repository())
			restoreStep(t, r) // plan
			restoreStep(t, r) // creates the Job
			r.Client, _ = watchResumes(c, failResumes(err))

			if _, got := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); got == nil {
				t.Fatal("the pass whose resume failed succeeded, want the error returned for a retry")
			}
			job := onlyJob(t, c)
			if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.JobUID != job.UID || !suspendedJob(t, c, job.Name) {
				t.Fatalf("item = %+v, Job suspended = %t; want the item Running on its suspended Job", item, suspendedJob(t, c, job.Name))
			}
			restoreStep(t, r)
			if suspendedJob(t, c, job.Name) {
				t.Errorf("restore Job %s is still suspended after the retry, want it resumed", job.Name)
			}
		})
	}
}
