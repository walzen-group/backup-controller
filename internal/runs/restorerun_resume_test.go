package runs

import (
	"context"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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

// landOnRestart returns a client over c that calls land at the first write
// to a Deployment's scale subresource, so a restore Job whose create timed
// out is stored while the run gives the app back. The Job is then all there
// is of the restore: the run has not recorded it. The function returned
// reports the landed Job as it was stored, or nil before it landed.
func landOnRestart(c client.Client, land func() *batchv1.Job) (client.Client, func() *batchv1.Job) {
	var landed *batchv1.Job
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if _, ok := deploymentScale(sub, obj, opts); ok && landed == nil {
				landed = land().DeepCopy()
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	}), func() *batchv1.Job { return landed }
}

// A restore Job whose create answered 504 Timeout and was stored only once
// the run had failed its single item never runs. The item's VolumeRestore
// is gone on the pass after the create, so the item fails and the run gives
// the app back in that pass. The Job lands while work restarts the app,
// after the item failed, or after the run finished. It stays suspended with
// no pod while the app runs: the run stops it before it finishes, or, once
// finished, when it is deleted.
func TestAJobThatLandsAfterTheAppIsBackNeverRuns(t *testing.T) {
	for name, whileRestarting := range map[string]bool{
		"while work restarts the app": true,
		"after the run finished":      false,
	} {
		t.Run(name, func(t *testing.T) {
			r, c := restoreReconciler(t, nil, quiescedRestore(), claim(), volumeRestore(), repository(),
				deployment(), kustomization(false))
			restoreStep(t, r) // plan
			restoreStep(t, r) // quiesce
			req := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}

			var land func() *batchv1.Job
			r.Client, land = createLandsLater(t, c)
			if _, err := r.Reconcile(context.Background(), req); err == nil {
				t.Fatal("the pass whose Job create timed out succeeded, want the timeout returned")
			}
			deleteVolumeRestore(t, c)
			r.Client = c
			var landed func() *batchv1.Job
			if whileRestarting {
				r.Client, landed = landOnRestart(c, land)
			}
			restoreStep(t, r)
			r.Client = c
			if whileRestarting {
				stored := landed()
				if stored == nil {
					t.Fatal("the run gave the app back without a Deployment scale write, want the Job landed during the restart")
				}
				if stored.Spec.Suspend == nil || !*stored.Spec.Suspend {
					t.Fatalf("the late restore Job %s could run while the app came back, want it stored suspended", stored.Name)
				}
			}
			if !whileRestarting {
				if phase := readRestoreRun(t, c).Status.Phase; phase != backupv1alpha1.RunPhaseFailed {
					t.Fatalf("phase = %q before the Job landed, want Failed", phase)
				}
				land()
			}
			if replicasOf(t, c) != 2 {
				t.Fatalf("replicas = %d, want the app back", replicasOf(t, c))
			}
			job := onlyJob(t, c)
			if !suspendedJob(t, c, job.Name) {
				t.Fatalf("the late restore Job %s runs beside the app, want it suspended: the run never recorded it", job.Name)
			}
			markSuspended(t, c, job)
			restoreStep(t, r)
			if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed {
				t.Errorf("item = %+v, want it still Failed", item)
			}

			if !whileRestarting {
				if !suspendedJob(t, c, job.Name) {
					t.Fatalf("the late restore Job %s runs beside the finished run, want it suspended", job.Name)
				}
				if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
					t.Fatal(err)
				}
				if err := c.Get(context.Background(), req.NamespacedName, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
					t.Errorf("RestoreRun = %v, want it gone", err)
				}
				if !cascadesTo(c, job) {
					t.Errorf("the delete of the finished run does not reach restore Job %s, want the garbage collector to delete it", job.Name)
				}
				return
			} else if phase := readRestoreRun(t, c).Status.Phase; phase != backupv1alpha1.RunPhaseFailed {
				t.Errorf("phase = %q once the late Job was stopped, want Failed", phase)
			}
			if jobs := restoreJobs(t, c); len(jobs) != 0 {
				t.Errorf("restore Jobs = %v, want the late Job deleted", jobs)
			}
		})
	}
}

// cascadesTo reports whether a delete through c, the strict client
// newClient built, found the Job among the deleted object's dependents, so
// the garbage collector deletes it.
func cascadesTo(c client.Client, job *batchv1.Job) bool {
	for _, cascade := range c.(*strictclient.Client).Cascades() {
		for _, dependent := range cascade.Dependents {
			if dependent.UID == job.UID {
				return true
			}
		}
	}
	return false
}
