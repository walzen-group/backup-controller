package runs

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The tests in this file check an into restore (spec.into), from a claim's
// backups or from a repository, through the controller's own restore Job
// (designs/restic-jobs.md J1, step 8). The run creates a plain claim and a
// Job that restores the full ID its checks recorded into that claim; the
// Job's terminal conditions alone decide the item's end, and the run stops
// the Job by the UID it recorded.

// sourceOnNode returns the app's claim, as claim does, with the node the
// scheduler selected for its volume.
func sourceOnNode() *corev1.PersistentVolumeClaim {
	source := claim()
	source.Annotations[selectedNodeAnnotation] = "worker-1"
	return source
}

// intoShapesOnJob are the two shapes of an into restore: from a claim's
// backups (see intoMonday) and from a repository alone (see fromRepository),
// with the claim each creates.
var intoShapesOnJob = []struct {
	name   string
	mutate func(*backupv1alpha1.RestoreRun)
	into   string
}{
	{"from a claim", intoMonday, "notes-data-monday"},
	{"from a repository", fromRepository, "scratch"},
}

// jobClaim returns the name of the claim the Job mounts as its data volume,
// and "" when it mounts none.
func jobClaim(job *batchv1.Job) string {
	for _, v := range job.Spec.Template.Spec.Volumes {
		if v.PersistentVolumeClaim != nil {
			return v.PersistentVolumeClaim.ClaimName
		}
	}
	return ""
}

// startedInto returns a reconciler whose into restore of the given shape
// selected monday and created its claim and restore Job, and the client.
func startedInto(t *testing.T, mutate func(*backupv1alpha1.RestoreRun)) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	r, c := restoreReconciler(t, nil, restoreRun(mutate), sourceOnNode(), volumeRestore(), repository())
	restoreStep(t, r) // plan
	restoreStep(t, r) // create
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.JobUID == "" {
		t.Fatalf("item = %+v; want Running on its restore Job", item)
	}
	return r, c
}

// An into restore creates a plain claim, with no data source, so no
// populator is involved, and one restore Job that restores the snapshot the
// checks selected by its full ID into that claim, with --delete, controlled
// by the run. The item records the Job's name and UID in the same status
// write, and no ReplicationDestination exists. From a claim, the new claim
// takes the source claim's size, class and node, and the Job the source's
// cache class and queue label. Before, the run wrote through a
// ReplicationDestination pinned to the snapshot's second.
func TestAnIntoRestoreCreatesAJobForTheFullSnapshotID(t *testing.T) {
	for _, shape := range intoShapesOnJob {
		t.Run(shape.name, func(t *testing.T) {
			_, c := startedInto(t, shape.mutate)

			run := readRestoreRun(t, c)
			scratch := &corev1.PersistentVolumeClaim{}
			get(t, c, ns, shape.into, scratch)
			if scratch.Spec.DataSourceRef != nil || scratch.Spec.DataSource != nil || !metav1.IsControlledBy(scratch, run) {
				t.Fatalf("claim = %+v, owners %v; want no data source and the run as controller", scratch.Spec, scratch.OwnerReferences)
			}
			jobs := restoreJobs(t, c)
			if len(jobs) != 1 {
				t.Fatalf("restore Jobs = %v, want one", jobs)
			}
			job := jobs[0]
			item := run.Status.Items[0]
			if item.SnapshotID != monday.ID || item.Job != job.Name || item.JobUID != job.UID || item.Job != jobName(restoreUID, 0) || item.Destination != "" {
				t.Fatalf("item = %+v, Job %s (UID %s); want it naming the Job and its UID, with monday's full ID and no destination", item, job.Name, job.UID)
			}
			restore := job.Spec.Template.Spec.Containers[0]
			if !metav1.IsControlledBy(&job, run) || job.Annotations[restorejob.AnnotationSnapshotID] != monday.ID ||
				!slices.Contains(restore.Args, monday.ID) || !slices.Contains(restore.Args, "--delete") || jobClaim(&job) != shape.into {
				t.Errorf("Job owners = %v, annotations = %v, args = %v, claim %q; want the run's Job restoring %s into %s with --delete",
					job.OwnerReferences, job.Annotations, restore.Args, jobClaim(&job), monday.ID, shape.into)
			}
			if names := destinations(t, c); len(names) != 0 {
				t.Errorf("destinations = %v, want none", names)
			}
			if shape.into != "notes-data-monday" {
				return
			}
			request := scratch.Spec.Resources.Requests[corev1.ResourceStorage]
			if request.Cmp(resource.MustParse("1Gi")) != 0 || scratch.Spec.StorageClassName == nil || *scratch.Spec.StorageClassName != "zfs" ||
				scratch.Annotations[selectedNodeAnnotation] != "worker-1" {
				t.Errorf("claim = %+v, annotations %v; want 1Gi of zfs on worker-1", scratch.Spec, scratch.Annotations)
			}
			if job.Spec.Template.Labels["kueue.x-k8s.io/queue-name"] != "backups" {
				t.Errorf("pod labels = %v, want the source's queue label", job.Spec.Template.Labels)
			}
		})
	}
}

// An into restore whose Job is Complete succeeds with no mover log to read,
// stops the Job once the item's end is stored, and ends Succeeded. No
// VolumeRestore is created.
func TestAnIntoRestoreSucceedsOnACompleteJob(t *testing.T) {
	for _, shape := range intoShapesOnJob {
		t.Run(shape.name, func(t *testing.T) {
			r, c := startedInto(t, shape.mutate)
			completeJob(t, c)
			run := stepUntilFinished(t, r, c, 3)

			if item := run.Status.Items[0]; run.Status.Phase != backupv1alpha1.RunPhaseSucceeded || item.Phase != backupv1alpha1.ItemSucceeded ||
				item.Reason != "" || item.JobUID != "" {
				t.Fatalf("phase = %q, item = %+v (%s); want Succeeded with its stopped Job no longer named", run.Status.Phase, item, readyMessage(run.Status.Conditions))
			}
			if jobs := restoreJobs(t, c); len(jobs) != 0 {
				t.Errorf("restore Jobs = %v, want the stopped one deleted", jobs)
			}
			key := types.NamespacedName{Namespace: ns, Name: shape.into}
			if err := c.Get(context.Background(), key, &backupv1alpha1.VolumeRestore{}); !apierrors.IsNotFound(err) {
				t.Errorf("VolumeRestore %s: %v, want none created", shape.into, err)
			}
		})
	}
}

// An into restore whose Job ends Failed through the FailJob rule fails the
// item with reason RestoreJobFailed and restic's exit code, read from the
// Job's own pod, and the run ends Failed.
func TestAnIntoRestoreFailsOnAFailedJobWithTheExitCode(t *testing.T) {
	r, c := startedInto(t, fromRepository)
	job := itemJob(t, c)
	pod := jobPodOf(job, "restore-pod", corev1.PodFailed)
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{Name: "restore", State: corev1.ContainerState{
		Terminated: &corev1.ContainerStateTerminated{ExitCode: 12, Message: "Fatal: wrong password or no key found"},
	}}}
	createPod(t, c, pod)
	endJob(t, c, job.Name, batchv1.JobFailed, "PodFailurePolicy", "Container restore for pod notes/restore-pod exited with code 12 matching FailJob rule at index 0")
	run := stepUntilFinished(t, r, c, 3)

	item := run.Status.Items[0]
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobFailed ||
		!strings.Contains(item.Message, "restic exited 12 (wrong password)") {
		t.Errorf("phase = %q, item = %+v; want Failed with reason RestoreJobFailed and exit code 12", run.Status.Phase, item)
	}
}

// A restore Job create the API server refuses fails an into restore's item
// with reason RestoreJobRefused and the server's message, says nothing was
// written to the new claim, and creates no Job.
func TestARefusedIntoJobCreateFailsTheItem(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan
	r.Client = refuseJobCreates(c, func(obj client.Object) error {
		return apierrors.NewForbidden(schema.GroupResource{Group: "batch", Resource: "jobs"}, obj.GetName(),
			errors.New("ValidatingAdmissionPolicy 'backup-controller-jobs' denied request"))
	})
	restoreStep(t, r)
	r.Client = c
	run := stepUntilFinished(t, r, c, 2)

	item := run.Status.Items[0]
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobRefused ||
		!strings.Contains(item.Message, "refused to create restore Job") || !strings.Contains(item.Message, "Nothing was written to claim scratch") {
		t.Errorf("phase = %q, item = %+v; want Failed with reason RestoreJobRefused, the server's refusal and nothing written", run.Status.Phase, item)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want none", jobs)
	}
}

// A pass that created an into restore's claim and Job and lost the status
// write that recorded the Job leaves the item without it. The next pass
// takes that Job over, records its name and UID, and creates no second Job.
func TestAnIntoRestoreTakesOverTheJobOfALostWrite(t *testing.T) {
	r, c := restoreReconciler(t, nil, restoreRun(fromRepository), repository())
	restoreStep(t, r) // plan
	r.Client = loseNextStatusWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose status write was lost succeeded, want the error returned")
	}
	r.Client = c
	jobs := restoreJobs(t, c)
	if len(jobs) != 1 {
		t.Fatalf("restore Jobs = %v, want the one the lost pass created", jobs)
	}
	restoreStep(t, r)

	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRunning || item.Job != jobs[0].Name || item.JobUID != jobs[0].UID {
		t.Errorf("item = %+v, want Running naming %s with UID %s", item, jobs[0].Name, jobs[0].UID)
	}
	if again := restoreJobs(t, c); len(again) != 1 || again[0].UID != jobs[0].UID {
		t.Errorf("restore Jobs = %v, want only the lost pass's", again)
	}
}

// plannedInto returns the RestoreRun back-to-monday as planIntoNewClaim
// leaves an into restore of the claim notes-data-monday: Running, holding
// its finalizer, with its item Running on monday's full ID and no Job yet.
func plannedInto() *backupv1alpha1.RestoreRun {
	started := metav1.NewTime(frozen)
	return restoreRun(intoMonday, func(r *backupv1alpha1.RestoreRun) {
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.StartedAt = &started
		r.Status.Target = r.Spec.Into
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: backupv1alpha1.ItemKindClaim, Name: r.Spec.Into, Phase: backupv1alpha1.ItemRunning,
			Snapshot: monday.ShortID(), SnapshotID: monday.ID, SnapshotTime: &metav1.Time{Time: monday.Time}}}
	})
}

// A restore Job the run created under the into item's name whose
// snapshot-id annotation is not the recorded full ID fails the item with
// reason RestoreJobFailed naming both IDs, and the run stops that Job. A
// Job of that name the run did not create fails the item with reason
// RestoreJobRefused and is left alone.
func TestAnIntoTakeoverChecksTheJob(t *testing.T) {
	t.Run("another ID", func(t *testing.T) {
		run := plannedInto()
		lost := restoreJobFor(t, run, run.Spec.Into, sunday.ID)
		r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(), intoClaim(run), lost)
		restoreStep(t, r)

		item := readRestoreRun(t, c).Status.Items[0]
		if item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobFailed || item.JobUID != lost.UID ||
			!strings.Contains(item.Message, sunday.ID) || !strings.Contains(item.Message, monday.ID) {
			t.Fatalf("item = %+v, want Failed with reason RestoreJobFailed naming both IDs and the Job", item)
		}
		if !suspendedJob(t, c, lost.Name) || readRestoreRun(t, c).Status.Phase.Finished() {
			t.Errorf("Job suspended = %t, phase = %q; want the Job stopped and the run waiting for it", suspendedJob(t, c, lost.Name), readRestoreRun(t, c).Status.Phase)
		}
		markSuspended(t, c, lost)
		if run := stepUntilFinished(t, r, c, 2); run.Status.Phase != backupv1alpha1.RunPhaseFailed {
			t.Errorf("phase = %q once the Job was stopped, want Failed", run.Status.Phase)
		}
	})
	t.Run("not the run's", func(t *testing.T) {
		run := plannedInto()
		foreign := restoreJobFor(t, run, run.Spec.Into, monday.ID)
		foreign.OwnerReferences = nil
		delete(foreign.Labels, restorejob.LabelRestoreRun)
		r, c := restoreReconciler(t, nil, run, sourceOnNode(), volumeRestore(), repository(), foreign)
		done := stepUntilFinished(t, r, c, 3)

		if item := done.Status.Items[0]; done.Status.Phase != backupv1alpha1.RunPhaseFailed || item.Phase != backupv1alpha1.ItemFailed ||
			item.Reason != backupv1alpha1.ItemReasonRestoreJobRefused || item.JobUID != "" {
			t.Errorf("phase = %q, item = %+v; want Failed with reason RestoreJobRefused naming no Job", done.Status.Phase, item)
		}
		kept := &batchv1.Job{}
		get(t, c, ns, foreign.Name, kept)
		if kept.Spec.Suspend != nil {
			t.Errorf("the foreign Job's suspend = %v, want it untouched", *kept.Spec.Suspend)
		}
		if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: run.Spec.Into}, &corev1.PersistentVolumeClaim{}); !apierrors.IsNotFound(err) {
			t.Errorf("claim %s: %v, want it never created", run.Spec.Into, err)
		}
	})
}

// intoOnJob returns the RestoreRun back-to-monday in the middle of an into
// restore of the claim notes-data-monday (see intoMonday): Running since the
// frozen time, holding its finalizer, with its item Running and naming the
// restore Job the run created for it with UID jobUID, and that Job, which
// restores monday's snapshot into the claim. The claim comes from
// intoClaim.
func intoOnJob(t *testing.T) (*backupv1alpha1.RestoreRun, *batchv1.Job) {
	t.Helper()
	run := plannedInto()
	run.Status.Items[0] = runningOnJob(run.Spec.Into, 0)
	return run, restoreJobFor(t, run, run.Spec.Into, monday.ID)
}

// An into restore whose Job someone deleted fails its item with reason
// RestoreJobDeleted, and the run never creates a second Job: Stop gates on
// the pods of the recorded UID. The Job's orphaned pod, which keeps that
// UID, holds the run and its Lease until it has ended.
func TestADeletedIntoRestoreJobFailsTheItemAndHoldsForItsPods(t *testing.T) {
	run, job := intoOnJob(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	lease := heldClaimLease(run, run.Spec.Into)
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), job, pod, lease)
	if err := c.Delete(context.Background(), job, client.PropagationPolicy(metav1.DeletePropagationOrphan)); err != nil {
		t.Fatal(err)
	}
	creates := 0
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if _, ok := obj.(*batchv1.Job); ok {
				creates++
			}
			return cl.Create(ctx, obj, opts...)
		},
	})
	restoreStep(t, r)
	restoreStep(t, r)

	waiting := readRestoreRun(t, c)
	if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonRestoreJobDeleted ||
		!strings.Contains(item.Message, "was deleted before it finished") {
		t.Errorf("item = %+v, want Failed with reason RestoreJobDeleted saying the Job was deleted", item)
	}
	if creates != 0 || len(restoreJobs(t, c)) != 0 {
		t.Errorf("Job creates = %d, restore Jobs = %v; want no second Job", creates, restoreJobs(t, c))
	}
	if waiting.Status.Phase.Finished() || !strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
		t.Fatalf("phase = %q, message = %q while pod %s runs; want the run waiting and naming the pod",
			waiting.Status.Phase, readyMessage(waiting.Status.Conditions), pod.Name)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, lease); err != nil {
		t.Errorf("get the claim Lease = %v, want it held while the pod runs", err)
	}

	setPodPhase(t, c, pod, corev1.PodFailed)
	if done := stepUntilFinished(t, r, c, 2); done.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q once the pod had ended, want Failed", done.Status.Phase)
	}
}

// An into restore whose Job name now holds a Job with another UID fails
// its item with reason RestoreJobDeleted, also when that Job is Complete,
// and one whose Job the run no longer controls fails it with reason
// RestoreJobFailed. The run never creates a second Job, and Stop gates on
// the recorded UID: a Job with another UID is neither suspended nor
// deleted, and the pod of the recorded UID holds the run and its Lease
// until it has ended. The first release of the Lease after that fails, so
// finish runs again once the stop has cleared the recorded UID: an into
// run gets no second stop otherwise, and that pass must still leave the
// Job under the name alone, since the item names the Job it stopped.
func TestAReplacedOrUncontrolledIntoRestoreJobFailsTheItem(t *testing.T) {
	for _, tc := range swappedJobs {
		t.Run(tc.name, func(t *testing.T) {
			run, job := intoOnJob(t)
			pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
			tc.swap(job)
			lease := heldClaimLease(run, run.Spec.Into)
			r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), job, pod, lease)
			var creates *int
			r.Client, creates = countJobCreates(c)
			restoreStep(t, r)
			if tc.stopped {
				markSuspended(t, c, job)
			}
			restoreStep(t, r)

			waiting := readRestoreRun(t, c)
			if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != tc.reason || item.JobUID != jobUID {
				t.Errorf("item = %+v, want Failed with reason %s, still naming UID %s", item, tc.reason, jobUID)
			}
			if *creates != 0 || suspendedJob(t, c, job.Name) != tc.stopped {
				t.Errorf("Job creates = %d, Job suspended = %t; want no second Job and suspended %t", *creates, suspendedJob(t, c, job.Name), tc.stopped)
			}
			if waiting.Status.Phase.Finished() || !strings.Contains(readyMessage(waiting.Status.Conditions), pod.Name) {
				t.Fatalf("phase = %q, message = %q while pod %s runs; want the run waiting and naming the pod",
					waiting.Status.Phase, readyMessage(waiting.Status.Conditions), pod.Name)
			}
			if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: lease.Name}, lease); err != nil {
				t.Errorf("get the claim Lease = %v, want it held while the pod runs", err)
			}

			setPodPhase(t, c, pod, corev1.PodFailed)
			r.Client = refuseLeaseDeletes(c)
			if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
				t.Fatal("the pass whose Lease release was refused succeeded, want the refusal returned")
			}
			if item := readRestoreRun(t, c).Status.Items[0]; item.JobUID != "" || item.Job != job.Name {
				t.Fatalf("item = %+v after the stop, want no UID and the stopped Job's name %s", item, job.Name)
			}
			r.Client = c
			if done := stepUntilFinished(t, r, c, 2); done.Status.Phase != backupv1alpha1.RunPhaseFailed {
				t.Errorf("phase = %q once the pod had ended, want Failed", done.Status.Phase)
			}
			expectSwappedJobLeft(t, c, tc, job)
		})
	}
}

// An into restore deleted while its Job restores stops the Job through the
// recorded UID and keeps its finalizer until the Job's pod has ended.
func TestADeletedIntoRestoreStopsItsJob(t *testing.T) {
	run, job := intoOnJob(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), job, pod)
	if err := c.Delete(context.Background(), readRestoreRun(t, c)); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	if !suspendedJob(t, c, job.Name) || len(readRestoreRun(t, c).Finalizers) == 0 {
		t.Fatalf("Job suspended = %t, finalizers = %v; want the Job stopped and the finalizer kept while its pod runs",
			suspendedJob(t, c, job.Name), readRestoreRun(t, c).Finalizers)
	}
	markSuspended(t, c, job)
	setPodPhase(t, c, pod, corev1.PodFailed)
	restoreStep(t, r)

	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "back-to-monday"}, &backupv1alpha1.RestoreRun{}); !apierrors.IsNotFound(err) {
		t.Errorf("get the run = %v, want it gone once the Job's pod had ended", err)
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want the stopped one deleted", jobs)
	}
}

// An abort of an into restore whose Job still runs stops the Job through the
// recorded UID and holds until the Job's pod has ended. The item gets the
// abort's message.
func TestAnAbortedIntoRestoreStopsItsJob(t *testing.T) {
	run, job := intoOnJob(t)
	pod := jobPodOf(job, "restore-pod", corev1.PodRunning)
	r, c := restoreReconciler(t, nil, run, intoClaim(run), sourceOnNode(), volumeRestore(), repository(), job, pod)
	if _, err := r.abort(context.Background(), readRestoreRun(t, c), backupv1alpha1.ReasonFailed, "the source claim went away"); err != nil {
		t.Fatal(err)
	}

	waiting := readRestoreRun(t, c)
	if item := waiting.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.HasPrefix(item.Message, "the source claim went away") {
		t.Errorf("item = %+v, want Failed with the abort's message", item)
	}
	if !suspendedJob(t, c, job.Name) || waiting.Status.Phase.Finished() {
		t.Fatalf("Job suspended = %t, phase = %q; want the Job stopped and the run waiting", suspendedJob(t, c, job.Name), waiting.Status.Phase)
	}
	markSuspended(t, c, job)
	setPodPhase(t, c, pod, corev1.PodFailed)
	if done := stepUntilFinished(t, r, c, 2); done.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q once the pod had ended, want Failed", done.Status.Phase)
	}
}

// An into restore checks the claim it created, never the in-place rule of
// the Leases: its Lease is on the source claim. A Job that completes into
// the run's own claim succeeds, also when the deadline passes just then;
// one that completes into a claim replaced by one the run did not create
// fails with reason ClaimLost.
func TestAnIntoRestoreChecksItsOwnClaimWhenItsJobCompletes(t *testing.T) {
	for name, tc := range map[string]struct {
		replace bool
		late    bool
		phase   backupv1alpha1.ItemPhase
		reason  backupv1alpha1.ItemReason
	}{
		"own claim":                {phase: backupv1alpha1.ItemSucceeded},
		"own claim at the timeout": {late: true, phase: backupv1alpha1.ItemSucceeded},
		"replaced claim":           {replace: true, phase: backupv1alpha1.ItemFailed, reason: backupv1alpha1.ItemReasonClaimLost},
	} {
		t.Run(name, func(t *testing.T) {
			run, job := intoOnJob(t)
			job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: corev1.ConditionTrue, Reason: "CompletionsReached"}}
			target := intoClaim(run)
			if tc.replace {
				target.OwnerReferences, target.UID = nil, "replaced-claim-uid"
			}
			r, c := restoreReconciler(t, nil, run, target, sourceOnNode(), volumeRestore(), repository(), job, heldClaimLease(run, run.Spec.Into))
			if tc.late {
				r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }
			}
			done := stepUntilFinished(t, r, c, 3)

			if item := done.Status.Items[0]; item.Phase != tc.phase || item.Reason != tc.reason {
				t.Errorf("item = %+v, want %s with reason %q", item, tc.phase, tc.reason)
			}
			if jobs := restoreJobs(t, c); len(jobs) != 0 {
				t.Errorf("restore Jobs = %v, want the run's deleted", jobs)
			}
		})
	}
}

// An into restore whose Job failed and whose final status write was lost
// after the Job was deleted ends Failed on the next pass. It does not take
// the missing Job for one never created, and creates no new Job.
func TestAnIntoRestoreWhoseFinalWriteWasLostIsNotStartedAgain(t *testing.T) {
	r, c := startedInto(t, fromRepository)
	endJob(t, c, itemJob(t, c).Name, batchv1.JobFailed, "BackoffLimitExceeded", "Job has reached the specified backoff limit")

	r.Client = loseFailedRunWrite(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose final write was lost succeeded, want the error returned")
	}
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Fatalf("restore Jobs = %v, want the item's deleted before the lost write", jobs)
	}
	r.Client = c
	restoreStep(t, r)

	expectItemFailed(t, c, "BackoffLimitExceeded")
	if jobs := restoreJobs(t, c); len(jobs) != 0 {
		t.Errorf("restore Jobs = %v, want no new Job", jobs)
	}
}

// refuseLeaseDeletes returns a client over c that refuses every delete of a
// Lease as Forbidden, so a run can't release its Leases.
func refuseLeaseDeletes(c client.Client) client.Client {
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, ok := obj.(*coordinationv1.Lease); ok {
				return apierrors.NewForbidden(coordinationv1.Resource("leases"), obj.GetName(), errors.New("delete refused"))
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
}
