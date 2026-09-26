package populator

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	internalvolsync "github.com/walzen-group/backup-controller/internal/volsync"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
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
	orphanVolumeRestore = "nightly"
	pvcProtection       = "kubernetes.io/pvc-protection"
)

// orphanCRDs are the pinned CRDs of the custom kinds the orphan reconciler
// reads or deletes.
var orphanCRDs = []string{
	"../testinfra/crds/backup-controller/v0.8.1/backup.wlz.li_volumerestores.yaml",
	"../testinfra/crds/volsync/volsync.backube_replicationdestinations.yaml",
}

func orphanScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, volsyncv1alpha1.AddToScheme, backupv1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatalf("register types: %v", err)
		}
	}
	return s
}

// newOrphanClient builds a strict fake client holding objects.
func newOrphanClient(t *testing.T, objects ...client.Object) client.WithWatch {
	t.Helper()
	now := time.Date(2026, 9, 26, 7, 0, 0, 0, time.UTC)
	return strictclient.Build(fake.NewClientBuilder().WithObjects(objects...), orphanScheme(t), strictclient.Options{
		Clock: func() time.Time { return now },
		CRDs:  orphanCRDs,
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

// leftovers are the library's and Populate's objects for stuckClaim in the
// controller namespace: the destination, the Secret copy and the prime.
func leftovers() []client.Object {
	return []client.Object{
		&volsyncv1alpha1.ReplicationDestination{ObjectMeta: metav1.ObjectMeta{Name: DestinationName(orphanClaimUID), Namespace: orphanControllerNS}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: string(orphanClaimUID), Namespace: orphanControllerNS}},
		&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: PrimeClaimName(orphanClaimUID), Namespace: orphanControllerNS}},
	}
}

func moverPod(phase corev1.PodPhase) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: internalvolsync.MoverJobName(DestinationName(orphanClaimUID)) + "-x7k2p", Namespace: orphanControllerNS,
			Labels: map[string]string{"job-name": internalvolsync.MoverJobName(DestinationName(orphanClaimUID))},
		},
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

// deleteDestinationPass runs the pass that deletes the ReplicationDestination
// of stuckClaim, and checks that it requeues and goes no further. It drops
// the events that pass recorded, so a test reads only those of the passes
// after it.
func deleteDestinationPass(t *testing.T, r *OrphanReconciler, recorder *events.FakeRecorder) {
	t.Helper()
	res, err := reconcileClaim(t, r)
	if err != nil {
		t.Fatalf("reconcile that deletes the destination: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Fatalf("result of the pass that deletes the destination = %+v, want a requeue", res)
	}
	drain(recorder)
}

func assertLeftovers(t *testing.T, c client.Reader, want bool) {
	t.Helper()
	for _, obj := range leftovers() {
		if got := present(t, c, obj); got != want {
			t.Errorf("%T %s present = %v, want %v", obj, obj.GetName(), got, want)
		}
	}
}

// A claim being deleted whose VolumeRestore is gone gets the destination, the
// Secret copy and the prime deleted, then loses the library's finalizer, and
// a Warning event says so. The destination goes in the first pass, and the
// rest in the pass after it. Before the reconciler the claim stayed
// Terminating for good, because the library returns on the data source's
// NotFound before its cleanup (lib-volume-populator v3.3.0
// controller.go:661-671).
func TestAClaimWhoseVolumeRestoreIsGoneIsCleanedUp(t *testing.T) {
	c := newOrphanClient(t, append(leftovers(), stuckClaim())...)
	r, recorder := newOrphanReconciler(c, c)

	deleteDestinationPass(t, r, recorder)
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
// library: nothing is deleted and the finalizer stays.
func TestAClaimWhoseVolumeRestoreExistsIsLeftToTheLibrary(t *testing.T) {
	deleted := metav1.NewTime(time.Date(2026, 9, 26, 6, 30, 0, 0, time.UTC))
	for name, vr := range map[string]*backupv1alpha1.VolumeRestore{
		"live":        {ObjectMeta: metav1.ObjectMeta{Name: orphanVolumeRestore, Namespace: orphanNamespace}},
		"terminating": {ObjectMeta: metav1.ObjectMeta{Name: orphanVolumeRestore, Namespace: orphanNamespace, DeletionTimestamp: &deleted, Finalizers: []string{Finalizer}}},
	} {
		t.Run(name, func(t *testing.T) {
			c := newOrphanClient(t, append(leftovers(), stuckClaim(), vr)...)
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

// A live read that fails for any reason but NotFound deletes nothing and
// returns the error for a retry.
func TestAFailedVolumeRestoreReadDeletesNothing(t *testing.T) {
	c := newOrphanClient(t, append(leftovers(), stuckClaim())...)
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

// X2: the destination is deleted, and while its mover pod is still there the
// Secret copy, the prime and the finalizer stay and the claim is requeued
// with a WaitingForMover event naming the pod. Once the pod is gone the
// cleanup finishes.
func TestAMoverStillThereIsWaitedFor(t *testing.T) {
	for _, phase := range []corev1.PodPhase{corev1.PodRunning, corev1.PodPending, corev1.PodSucceeded} {
		t.Run(string(phase), func(t *testing.T) {
			pod := moverPod(phase)
			c := newOrphanClient(t, append(leftovers(), stuckClaim(), pod)...)
			r, recorder := newOrphanReconciler(c, c)

			deleteDestinationPass(t, r, recorder)
			res, err := reconcileClaim(t, r)
			if err != nil {
				t.Fatalf("reconcile: %v", err)
			}
			if res.RequeueAfter <= 0 {
				t.Errorf("result = %+v, want a requeue", res)
			}
			objs := leftovers()
			if present(t, c, objs[0]) {
				t.Error("the ReplicationDestination is still there")
			}
			for _, obj := range objs[1:] {
				if !present(t, c, obj) {
					t.Errorf("%T %s deleted while the mover pod is there", obj, obj.GetName())
				}
			}
			if got := claimFinalizers(t, c); !slices.Contains(got, ClaimFinalizer) {
				t.Errorf("claim finalizers = %v, want %s kept", got, ClaimFinalizer)
			}
			got := drain(recorder)
			if len(got) != 1 || !strings.HasPrefix(got[0], "Normal WaitingForMover") || !strings.Contains(got[0], pod.Name) {
				t.Errorf("events = %q, want one Normal WaitingForMover naming %s", got, pod.Name)
			}

			if err := c.Delete(context.Background(), pod); err != nil {
				t.Fatalf("delete pod: %v", err)
			}
			if _, err := reconcileClaim(t, r); err != nil {
				t.Fatalf("reconcile after the pod went: %v", err)
			}
			assertLeftovers(t, c, false)
			if got := claimFinalizers(t, c); slices.Contains(got, ClaimFinalizer) {
				t.Errorf("claim finalizers = %v, want %s removed", got, ClaimFinalizer)
			}
		})
	}
}

// X2: a mover Job that is still there holds the cleanup even with no pod,
// since it may not have started its first pod yet or be between two of its
// retries. The Secret copy, the prime and the finalizer stay, and the claim
// is requeued with a WaitingForMover event naming the Job. Once the Job is
// gone the cleanup finishes. Before, only the Job's pods were looked for.
func TestAMoverJobWithoutAPodIsWaitedFor(t *testing.T) {
	job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: internalvolsync.MoverJobName(DestinationName(orphanClaimUID)), Namespace: orphanControllerNS}}
	c := newOrphanClient(t, append(leftovers(), stuckClaim(), job)...)
	r, recorder := newOrphanReconciler(c, c)

	deleteDestinationPass(t, r, recorder)
	res, err := reconcileClaim(t, r)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue", res)
	}
	for _, obj := range leftovers()[1:] {
		if !present(t, c, obj) {
			t.Errorf("%T %s deleted while the mover Job is there", obj, obj.GetName())
		}
	}
	if got := claimFinalizers(t, c); !slices.Contains(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s kept", got, ClaimFinalizer)
	}
	got := drain(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Normal WaitingForMover") || !strings.Contains(got[0], "mover Job "+orphanControllerNS+"/"+job.Name) {
		t.Errorf("events = %q, want one Normal WaitingForMover naming Job %s", got, job.Name)
	}

	if err := c.Delete(context.Background(), job); err != nil {
		t.Fatalf("delete Job: %v", err)
	}
	if _, err := reconcileClaim(t, r); err != nil {
		t.Fatalf("reconcile after the Job went: %v", err)
	}
	assertLeftovers(t, c, false)
	if got := claimFinalizers(t, c); slices.Contains(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s removed", got, ClaimFinalizer)
	}
}

// X2: the pass that deletes the destination finishes nothing, even when it
// sees no mover pod and no mover Job. VolSync may be in the middle of a
// reconcile of that destination and create the Job after the look, so only
// the next pass looks. The Secret copy, the prime and the finalizer stay, the
// claim is requeued, and a WaitingForMover event names the destination.
// Before, that pass went straight on to the rest of the cleanup.
func TestThePassThatDeletesTheDestinationFinishesNothing(t *testing.T) {
	c := newOrphanClient(t, append(leftovers(), stuckClaim())...)
	r, recorder := newOrphanReconciler(c, c)

	res, err := reconcileClaim(t, r)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.RequeueAfter <= 0 {
		t.Errorf("result = %+v, want a requeue", res)
	}
	objs := leftovers()
	if present(t, c, objs[0]) {
		t.Error("the ReplicationDestination is still there")
	}
	for _, obj := range objs[1:] {
		if !present(t, c, obj) {
			t.Errorf("%T %s deleted in the pass that deleted the destination", obj, obj.GetName())
		}
	}
	if got := claimFinalizers(t, c); !slices.Contains(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s kept", got, ClaimFinalizer)
	}
	got := drain(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Normal WaitingForMover") || !strings.Contains(got[0], "deleted ReplicationDestination "+DestinationName(orphanClaimUID)) {
		t.Errorf("events = %q, want one Normal WaitingForMover naming the deleted destination", got)
	}

	if _, err := reconcileClaim(t, r); err != nil {
		t.Fatalf("second reconcile: %v", err)
	}
	assertLeftovers(t, c, false)
	if got := claimFinalizers(t, c); slices.Contains(got, ClaimFinalizer) {
		t.Errorf("claim finalizers = %v, want %s removed", got, ClaimFinalizer)
	}
}

// A pod of another claim's mover does not hold this claim.
func TestAnotherClaimsMoverDoesNotBlock(t *testing.T) {
	other := moverPod(corev1.PodRunning)
	other.Name = "volsync-dst-restore-other-abcde"
	other.Labels = map[string]string{"job-name": "volsync-dst-restore-other"}
	c := newOrphanClient(t, append(leftovers(), stuckClaim(), other)...)
	r, recorder := newOrphanReconciler(c, c)
	deleteDestinationPass(t, r, recorder)
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
			c := newOrphanClient(t, append(leftovers(), claim)...)
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
		inner := newOrphanClient(t, append(leftovers(), stuckClaim())...)
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
		r, recorder := newOrphanReconciler(c, inner)
		deleteDestinationPass(t, r, recorder)
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
		inner := newOrphanClient(t, append(leftovers(), stuckClaim())...)
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
		deleteDestinationPass(t, r, recorder)
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
