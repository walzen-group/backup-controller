package populator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	orphanNamespace     = "app"
	orphanControllerNS  = "backup-system"
	orphanClaimUID      = types.UID("6f1d2c3a-0000-4000-8000-000000000001")
	orphanPrimeUID      = types.UID("6f1d2c3a-0000-4000-8000-0000000000f1")
	orphanJobUID        = types.UID("6f1d2c3a-0000-4000-8000-0000000000a1")
	orphanVolumeRestore = "nightly"
	pvcProtection       = "kubernetes.io/pvc-protection"
)

// orphanCRDs are the pinned CRDs of the custom kinds the orphan reconciler
// reads.
var orphanCRDs = []string{
	"../testinfra/crds/backup-controller/v0.8.1/backup.wlz.li_volumerestores.yaml",
}

// newOrphanClient builds a strict fake client, with the garbage collector
// on, holding objects.
func newOrphanClient(t *testing.T, objects ...client.Object) *strictclient.Client {
	t.Helper()
	return strictclient.Build(fake.NewClientBuilder().WithObjects(objects...), testScheme(t), strictclient.Options{
		Clock:          func() time.Time { return serverTime },
		CRDs:           orphanCRDs,
		GarbageCollect: true,
	})
}

// stuckClaim is an app claim being deleted that still carries the library's
// finalizer and names orphanVolumeRestore.
func stuckClaim() *corev1.PersistentVolumeClaim {
	group := backupv1alpha1.GroupVersion.Group
	deleted := metav1.NewTime(time.Date(2026, 9, 26, 6, 0, 0, 0, time.UTC))
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name: "data", Namespace: orphanNamespace, UID: orphanClaimUID,
			DeletionTimestamp: &deleted,
			Finalizers:        []string{ClaimFinalizer, pvcProtection},
		},
		Spec: corev1.PersistentVolumeClaimSpec{
			DataSourceRef: &corev1.TypedObjectReference{APIGroup: &group, Kind: "VolumeRestore", Name: orphanVolumeRestore},
		},
	}
}

// orphanJob is stuckClaim's restore Job as Populate creates it, owned by the
// prime claim, in the state the Job controller leaves it in right after the
// create: suspended, with Suspended=True, never resumed.
func orphanJob(t *testing.T) *batchv1.Job {
	t.Helper()
	job, err := restorejob.Build(restorejob.Spec{
		Name: JobName(orphanClaimUID), Namespace: orphanControllerNS,
		Origin:     restorejob.Origin{Kind: restorejob.OriginClaim, UID: orphanClaimUID},
		Owner:      metav1.OwnerReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: PrimeClaimName(orphanClaimUID), UID: orphanPrimeUID},
		SnapshotID: monday.ID, Claim: PrimeClaimName(orphanClaimUID), Repository: string(orphanClaimUID), Image: testImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	job.UID = orphanJobUID
	withCondition(job, batchv1.JobSuspended)
	return job
}

// leftovers are the library's and Populate's objects for stuckClaim in the
// controller namespace: the Secret copy, the prime and the restore Job.
func leftovers(t *testing.T) []client.Object {
	t.Helper()
	return []client.Object{
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: string(orphanClaimUID), Namespace: orphanControllerNS}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: PrimeClaimName(orphanClaimUID), Namespace: orphanControllerNS, UID: orphanPrimeUID}},
		orphanJob(t),
	}
}

// restorePod is a pod of the restore Job with the UID given, filling
// stuckClaim, as the Job controller creates it, on the node given (none for
// an unscheduled pod), in the phase given.
func restorePod(t *testing.T, jobUID types.UID, name, node string, phase corev1.PodPhase) *corev1.Pod {
	t.Helper()
	labels := map[string]string{batchv1.ControllerUidLabel: string(jobUID), batchv1.JobNameLabel: JobName(orphanClaimUID)}
	for key, value := range orphanJob(t).Spec.Template.Labels {
		labels[key] = value
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: orphanControllerNS, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: JobName(orphanClaimUID), UID: jobUID,
				Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}},
		},
		Spec:   corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "restore", Image: testImage}}},
		Status: corev1.PodStatus{Phase: phase},
	}
}

func newOrphanReconciler(c client.Client, reader client.Reader) (*OrphanReconciler, *events.FakeRecorder) {
	recorder := events.NewFakeRecorder(20)
	return &OrphanReconciler{Client: c, Reader: reader, Recorder: recorder, Namespace: orphanControllerNS}, recorder
}

func reconcileClaim(t *testing.T, r *OrphanReconciler) (ctrl.Result, error) {
	t.Helper()
	return r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: orphanNamespace, Name: "data"}})
}

func present(t *testing.T, c client.Reader, obj client.Object) bool {
	t.Helper()
	err := c.Get(context.Background(), client.ObjectKeyFromObject(obj), obj)
	if err != nil && !apierrors.IsNotFound(err) {
		t.Fatalf("get %s: %v", obj.GetName(), err)
	}
	return err == nil
}

func claimFinalizers(t *testing.T, c client.Reader) []string {
	t.Helper()
	claim := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: orphanNamespace, Name: "data"}, claim); err != nil {
		t.Fatalf("get claim: %v", err)
	}
	return claim.Finalizers
}

func drain(recorder *events.FakeRecorder) []string {
	var out []string
	for {
		select {
		case e := <-recorder.Events:
			out = append(out, e)
		default:
			return out
		}
	}
}

func assertLeftovers(t *testing.T, c client.Reader, want bool) {
	t.Helper()
	for _, obj := range leftovers(t) {
		if got := present(t, c, obj); got != want {
			t.Errorf("%T %s present = %v, want %v", obj, obj.GetName(), got, want)
		}
	}
}

// assertHeld checks that a pass requeued and left the Secret copy, the
// prime claim and the library's finalizer in place, with one Normal
// WaitingForMover event that holds the text given.
func assertHeld(t *testing.T, c client.Reader, recorder *events.FakeRecorder, res ctrl.Result, err error, text string) {
	t.Helper()
	if err != nil || res.RequeueAfter <= 0 {
		t.Fatalf("reconcile = %+v, %v; want a requeue", res, err)
	}
	for _, obj := range leftovers(t)[:2] {
		if !present(t, c, obj) {
			t.Errorf("%T %s deleted while a restore pod may write", obj, obj.GetName())
		}
	}
	if got := claimFinalizers(t, c); !slices.Contains(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s kept", got, ClaimFinalizer)
	}
	got := drain(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Normal WaitingForMover") || !strings.Contains(got[0], text) {
		t.Errorf("events = %q, want one Normal WaitingForMover with %q", got, text)
	}
}

// A claim being deleted whose VolumeRestore is gone gets its restore Job,
// its Secret copy and its prime deleted, then loses the library's finalizer,
// and a Warning event says so. Before the reconciler the claim stayed
// Terminating for good, because the library returns on the data source's
// NotFound before its cleanup (lib-volume-populator v3.3.0
// controller.go:661-671).
func TestAClaimWhoseVolumeRestoreIsGoneIsCleanedUp(t *testing.T) {
	c := newOrphanClient(t, append(leftovers(t), stuckClaim())...)
	r, recorder := newOrphanReconciler(c, c)

	if _, err := reconcileClaim(t, r); err != nil {
		t.Fatalf("reconcile: %v", err)
	}

	assertLeftovers(t, c, false)
	if got := claimFinalizers(t, c); !slices.Equal(got, []string{pvcProtection}) {
		t.Errorf("claim finalizers = %v, want only %s", got, pvcProtection)
	}
	got := drain(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Warning DataSourceGone") || !strings.Contains(got[0], ClaimFinalizer) {
		t.Errorf("events = %q, want one Warning DataSourceGone naming %s", got, ClaimFinalizer)
	}
}

// With only the library's finalizer left, removing it lets the claim go.
func TestTheLastFinalizerRemovedDeletesTheClaim(t *testing.T) {
	claim := stuckClaim()
	claim.Finalizers = []string{ClaimFinalizer}
	c := newOrphanClient(t, claim)
	r, _ := newOrphanReconciler(c, c)

	if _, err := reconcileClaim(t, r); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if present(t, c, stuckClaim()) {
		t.Error("the claim is still there")
	}
	if res, err := reconcileClaim(t, r); err != nil || res != (ctrl.Result{}) {
		t.Errorf("reconcile of the gone claim = %v, %v; want nothing", res, err)
	}
}

// A VolumeRestore that still exists, in any state, leaves the cleanup to the
// library: nothing is stopped or deleted and the finalizer stays.
func TestAClaimWhoseVolumeRestoreExistsIsLeftToTheLibrary(t *testing.T) {
	deleted := metav1.NewTime(time.Date(2026, 9, 26, 6, 30, 0, 0, time.UTC))
	for name, vr := range map[string]*backupv1alpha1.VolumeRestore{
		"live":        {ObjectMeta: metav1.ObjectMeta{Name: orphanVolumeRestore, Namespace: orphanNamespace}},
		"terminating": {ObjectMeta: metav1.ObjectMeta{Name: orphanVolumeRestore, Namespace: orphanNamespace, DeletionTimestamp: &deleted, Finalizers: []string{Finalizer}}},
	} {
		t.Run(name, func(t *testing.T) {
			c := newOrphanClient(t, append(leftovers(t), stuckClaim(), vr)...)
			r, recorder := newOrphanReconciler(c, c)
			if _, err := reconcileClaim(t, r); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			assertLeftovers(t, c, true)
			if got := claimFinalizers(t, c); !slices.Contains(got, ClaimFinalizer) {
				t.Errorf("claim finalizers = %v, want %s kept", got, ClaimFinalizer)
			}
			if got := drain(recorder); len(got) != 0 {
				t.Errorf("events = %q, want none", got)
			}
		})
	}
}

// A live read that fails for any reason but NotFound stops and deletes
// nothing and returns the error for a retry.
func TestAFailedVolumeRestoreReadDeletesNothing(t *testing.T) {
	c := newOrphanClient(t, append(leftovers(t), stuckClaim())...)
	reader := interceptor.NewClient(c, interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, ok := obj.(*backupv1alpha1.VolumeRestore); ok {
				return apierrors.NewServiceUnavailable("etcd is away")
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	r, _ := newOrphanReconciler(c, reader)

	if _, err := reconcileClaim(t, r); err == nil {
		t.Fatal("reconcile returned no error")
	}
	assertLeftovers(t, c, true)
	if got := claimFinalizers(t, c); !slices.Contains(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s kept", got, ClaimFinalizer)
	}
}

// X2: a restore Job that runs is suspended, and while its pod may still
// write the Secret copy, the prime and the finalizer stay and the claim is
// requeued with a WaitingForMover event that says what the stop waits for.
// Once the pod has ended the Job is deleted and the cleanup finishes.
func TestARunningRestoreJobIsStoppedAndWaitedFor(t *testing.T) {
	job := orphanJob(t)
	job.Spec.Suspend = ptr.To(false)
	job.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuspended, Status: corev1.ConditionFalse, Reason: "JobResumed"}}
	pod := restorePod(t, orphanJobUID, "restore-x7k2p", "node-a", corev1.PodRunning)
	c := newOrphanClient(t, append(leftovers(t)[:2], job, stuckClaim(), pod)...)
	r, recorder := newOrphanReconciler(c, c)
	ctx := context.Background()

	res, err := reconcileClaim(t, r)
	assertHeld(t, c, recorder, res, err, "waiting for the Job controller to suspend it")
	stored := &batchv1.Job{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(job), stored); err != nil || !ptr.Deref(stored.Spec.Suspend, false) {
		t.Fatalf("Job after the first pass: %v, suspend %v; want it suspended", err, stored.Spec.Suspend)
	}
	stored.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuspended, Status: corev1.ConditionTrue, Reason: "JobSuspended"}}
	if err := c.Status().Update(ctx, stored); err != nil {
		t.Fatal(err)
	}

	res, err = reconcileClaim(t, r)
	assertHeld(t, c, recorder, res, err, pod.Name)

	pod.Status.Phase = corev1.PodFailed
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	if _, err := reconcileClaim(t, r); err != nil {
		t.Fatalf("reconcile after the pod ended: %v", err)
	}
	assertLeftovers(t, c, false)
	if got := claimFinalizers(t, c); slices.Contains(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s removed", got, ClaimFinalizer)
	}
}

// A pod of a restore Job that a person deleted with its pods orphaned still
// holds the cleanup: the reconciler finds it by the claim's label and waits
// until it has ended, as Populate and Cleanup do. An unscheduled pod that is
// not being deleted could still be bound to a node, so it holds too.
func TestAPodOfADeletedJobHoldsTheCleanup(t *testing.T) {
	for name, pod := range map[string]*corev1.Pod{
		"running":     restorePod(t, "gone-job-uid", "restore-x7k2p", "node-a", corev1.PodRunning),
		"unscheduled": restorePod(t, "gone-job-uid", "restore-x7k2p", "", corev1.PodPending),
	} {
		t.Run(name, func(t *testing.T) {
			pod.OwnerReferences = nil
			c := newOrphanClient(t, append(leftovers(t)[:2], stuckClaim(), pod)...)
			r, recorder := newOrphanReconciler(c, c)

			res, err := reconcileClaim(t, r)
			assertHeld(t, c, recorder, res, err, pod.Name)

			pod.Status.Phase = corev1.PodSucceeded
			if err := c.Status().Update(context.Background(), pod); err != nil {
				t.Fatal(err)
			}
			if _, err := reconcileClaim(t, r); err != nil {
				t.Fatalf("reconcile after the pod ended: %v", err)
			}
			assertLeftovers(t, c, false)
		})
	}
}

// A pod of another claim's restore does not hold this claim.
func TestAnotherClaimsRestorePodDoesNotBlock(t *testing.T) {
	other := restorePod(t, "other-job-uid", "restore-other-abcde", "node-a", corev1.PodRunning)
	other.Labels[restorejob.LabelRestoreClaim] = "another-claim-uid"
	other.OwnerReferences = nil
	c := newOrphanClient(t, append(leftovers(t), stuckClaim(), other)...)
	r, _ := newOrphanReconciler(c, c)
	if _, err := reconcileClaim(t, r); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	assertLeftovers(t, c, false)
}

// Claims the library would never clean up, or would clean up itself, are
// left alone.
func TestClaimsOutsideTheOrphanCaseAreLeftAlone(t *testing.T) {
	cases := map[string]func(*corev1.PersistentVolumeClaim){
		"not being deleted":      func(p *corev1.PersistentVolumeClaim) { p.DeletionTimestamp = nil },
		"no library finalizer":   func(p *corev1.PersistentVolumeClaim) { p.Finalizers = []string{pvcProtection} },
		"cross-namespace source": func(p *corev1.PersistentVolumeClaim) { ns := "elsewhere"; p.Spec.DataSourceRef.Namespace = &ns },
		"another kind":           func(p *corev1.PersistentVolumeClaim) { p.Spec.DataSourceRef.Kind = "VolumeSnapshot" },
		"another group": func(p *corev1.PersistentVolumeClaim) {
			g := "snapshot.storage.k8s.io"
			p.Spec.DataSourceRef.APIGroup = &g
		},
		"no dataSourceRef":        func(p *corev1.PersistentVolumeClaim) { p.Spec.DataSourceRef = nil },
		"in controller namespace": func(p *corev1.PersistentVolumeClaim) { p.Namespace = orphanControllerNS },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			claim := stuckClaim()
			mutate(claim)
			c := newOrphanClient(t, append(leftovers(t), claim)...)
			r, recorder := newOrphanReconciler(c, c)
			if r.orphaned(claim) {
				t.Error("the predicate passes the claim")
			}
			req := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(claim)}
			if _, err := r.Reconcile(context.Background(), req); err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			assertLeftovers(t, c, true)
			got := &corev1.PersistentVolumeClaim{}
			if err := c.Get(context.Background(), req.NamespacedName, got); err != nil {
				t.Fatalf("get claim: %v", err)
			}
			if !slices.Equal(got.Finalizers, claim.Finalizers) {
				t.Errorf("finalizers = %v, want %v", got.Finalizers, claim.Finalizers)
			}
			if events := drain(recorder); len(events) != 0 {
				t.Errorf("events = %q, want none", events)
			}
		})
	}
}

// A conflict on the finalizer patch is returned for a retry, which then
// converges. A patch whose response is lost converges too: the next pass
// finds the claim without the finalizer and does nothing.
func TestTheFinalizerPatchConverges(t *testing.T) {
	t.Run("conflict", func(t *testing.T) {
		inner := newOrphanClient(t, append(leftovers(t), stuckClaim())...)
		failed := false
		c := interceptor.NewClient(inner, interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if !failed {
					failed = true
					return apierrors.NewConflict(schema.GroupResource{Resource: "persistentvolumeclaims"}, obj.GetName(), errors.New("changed"))
				}
				return cl.Patch(ctx, obj, patch, opts...)
			},
		})
		r, _ := newOrphanReconciler(c, inner)
		if _, err := reconcileClaim(t, r); !apierrors.IsConflict(err) {
			t.Fatalf("reconcile = %v, want the conflict", err)
		}
		if got := claimFinalizers(t, inner); !slices.Contains(got, ClaimFinalizer) {
			t.Errorf("claim finalizers = %v, want %s kept after the conflict", got, ClaimFinalizer)
		}
		if _, err := reconcileClaim(t, r); err != nil {
			t.Fatalf("retry: %v", err)
		}
		if got := claimFinalizers(t, inner); slices.Contains(got, ClaimFinalizer) {
			t.Errorf("claim finalizers = %v after the retry", got)
		}
	})
	t.Run("lost response", func(t *testing.T) {
		inner := newOrphanClient(t, append(leftovers(t), stuckClaim())...)
		lost := false
		c := interceptor.NewClient(inner, interceptor.Funcs{
			Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				if err := cl.Patch(ctx, obj, patch, opts...); err != nil {
					return err
				}
				if !lost {
					lost = true
					return apierrors.NewTimeoutError("response lost", 1)
				}
				return nil
			},
		})
		r, recorder := newOrphanReconciler(c, inner)
		if _, err := reconcileClaim(t, r); err == nil {
			t.Fatal("reconcile returned no error for the lost response")
		}
		if _, err := reconcileClaim(t, r); err != nil {
			t.Fatalf("retry: %v", err)
		}
		if got := claimFinalizers(t, inner); slices.Contains(got, ClaimFinalizer) {
			t.Errorf("claim finalizers = %v", got)
		}
		assertLeftovers(t, inner, false)
		if got := drain(recorder); len(got) != 0 {
			t.Errorf("events = %q, want none for a patch not known to have landed", got)
		}
	})
}

// The predicate passes a stuck claim on the create event the manager's
// initial list sends, which is how claims stuck before the upgrade are
// cleared on the first start.
func TestAClaimStuckBeforeStartPassesTheInitialList(t *testing.T) {
	r, _ := newOrphanReconciler(nil, nil)
	if !r.predicate().Create(event.CreateEvent{Object: stuckClaim()}) {
		t.Error("the initial list drops a stuck claim")
	}
	live := stuckClaim()
	live.DeletionTimestamp = nil
	if r.predicate().Create(event.CreateEvent{Object: live}) {
		t.Error("the initial list passes a live claim")
	}
}

// A deleted VolumeRestore enqueues the stuck claims in its namespace that
// name it, and no others.
func TestADeletedVolumeRestoreEnqueuesTheClaimsNamingIt(t *testing.T) {
	other := stuckClaim()
	other.Name, other.UID = "other", "6f1d2c3a-0000-4000-8000-000000000002"
	other.Spec.DataSourceRef.Name = "weekly"
	live := stuckClaim()
	live.Name, live.UID, live.DeletionTimestamp, live.Finalizers = "live", "6f1d2c3a-0000-4000-8000-000000000003", nil, []string{ClaimFinalizer}
	c := newOrphanClient(t, stuckClaim(), other, live)
	r, _ := newOrphanReconciler(c, c)

	got := r.claimsNaming(context.Background(), &backupv1alpha1.VolumeRestore{ObjectMeta: metav1.ObjectMeta{Name: orphanVolumeRestore, Namespace: orphanNamespace}})
	want := []ctrl.Request{{NamespacedName: types.NamespacedName{Namespace: orphanNamespace, Name: "data"}}}
	if !slices.Equal(got, want) {
		t.Errorf("requests = %v, want %v", got, want)
	}
}
