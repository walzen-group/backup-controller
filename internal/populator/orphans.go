package populator

import (
	"context"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	"k8s.io/klog/v2"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
)

// defaultMoverPoll is how long OrphanReconciler waits before it looks again
// at a restore Job that is still stopping.
const defaultMoverPoll = 30 * time.Second

// OrphanReconciler finishes the cleanup the populator library cannot do for a
// claim whose VolumeRestore is gone.
//
// The library looks a claim's data source up before anything else, and when
// the lookup says NotFound it records an event and returns, before the branch
// that cleans up after a deleted claim (lib-volume-populator v3.3.0
// populator-machinery/controller.go:661-671). A claim deleted after its
// VolumeRestore therefore keeps ClaimFinalizer, stays Terminating for good,
// and its prime claim, Secret copy and restore Job stay in the controller
// namespace. The reconciler does for such a claim what Cleanup and the
// library would have done.
type OrphanReconciler struct {
	// Client makes the writes, and lists claims from the manager's cache
	// when a VolumeRestore is deleted.
	Client client.Client
	// Reader is the uncached API reader. Every read that decides a stop or
	// a delete goes through it, so a stale cache cannot cause one.
	Reader client.Reader
	// Recorder writes the events on the claim.
	Recorder events.EventRecorder
	// Namespace is the controller namespace, where the prime claims, the
	// Secret copies and the restore Jobs live.
	Namespace string
	// MoverPoll is how long to wait before looking again at a restore Job
	// that is still stopping. Zero means 30 seconds.
	MoverPoll time.Duration
}

// SetupWithManager registers the reconciler with the manager mgr, and
// returns an error when the controller cannot be built.
//
// It watches two kinds. Of the claims, it passes on only those for which
// orphaned reports true. Of the VolumeRestores, it passes on only a deletion,
// which enqueues every claim that names the deleted VolumeRestore. The
// manager's initial list sends every claim through the predicate, so claims
// that were stuck before the process started are cleaned up on its first
// start.
func (r *OrphanReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&corev1.PersistentVolumeClaim{}, builder.WithPredicates(r.predicate())).
		Watches(&backupv1alpha1.VolumeRestore{},
			handler.EnqueueRequestsFromMapFunc(r.claimsNaming),
			builder.WithPredicates(predicate.Funcs{
				CreateFunc:  func(event.CreateEvent) bool { return false },
				UpdateFunc:  func(event.UpdateEvent) bool { return false },
				DeleteFunc:  func(event.DeleteEvent) bool { return true },
				GenericFunc: func(event.GenericEvent) bool { return false },
			})).
		Named("populator-orphans").
		Complete(r)
}

// predicate passes the claims for which orphaned reports true.
func (r *OrphanReconciler) predicate() predicate.Predicate {
	return predicate.NewPredicateFuncs(func(obj client.Object) bool {
		claim, ok := obj.(*corev1.PersistentVolumeClaim)
		return ok && r.orphaned(claim)
	})
}

// claimsNaming maps a deleted VolumeRestore to the claims in its namespace
// that name it and that orphaned reports on. It lists the claims from the
// manager's cache, and returns no request when the list fails; the claim's
// own events and the resync still reach the reconciler.
func (r *OrphanReconciler) claimsNaming(ctx context.Context, obj client.Object) []ctrl.Request {
	claims := &corev1.PersistentVolumeClaimList{}
	if err := r.Client.List(ctx, claims, client.InNamespace(obj.GetNamespace())); err != nil {
		klog.Errorf("list the claims naming VolumeRestore %s/%s: %v", obj.GetNamespace(), obj.GetName(), err)
		return nil
	}
	var requests []ctrl.Request
	for i := range claims.Items {
		claim := &claims.Items[i]
		if r.orphaned(claim) && claim.Spec.DataSourceRef.Name == obj.GetName() {
			requests = append(requests, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(claim)})
		}
	}
	return requests
}

// orphaned reports whether claim is one the reconciler may have to finish:
// it is being deleted, it carries ClaimFinalizer, its dataSourceRef names a
// VolumeRestore in its own namespace, and it is not in the controller
// namespace. The library works on no other claim: this binary does not set
// CrossNamespace (lib controller.go:647-658), and the library skips claims in
// its own namespace (lib controller.go:616).
func (r *OrphanReconciler) orphaned(claim *corev1.PersistentVolumeClaim) bool {
	ref := claim.Spec.DataSourceRef
	return claim.DeletionTimestamp != nil &&
		controllerutil.ContainsFinalizer(claim, ClaimFinalizer) &&
		ref != nil &&
		ref.APIGroup != nil && *ref.APIGroup == backupv1alpha1.GroupVersion.Group &&
		ref.Kind == "VolumeRestore" &&
		(ref.Namespace == nil || *ref.Namespace == claim.Namespace) &&
		claim.Namespace != r.Namespace
}

// Reconcile finishes the cleanup for the claim req names, when its
// VolumeRestore is gone.
//
// It reads the claim and its VolumeRestore through the uncached Reader. A
// claim orphaned does not report on, or a VolumeRestore that exists in any
// state, ends the pass without a write: the library can reach the
// VolumeRestore and cleans up itself. Only an authoritative NotFound for the
// VolumeRestore lets it go on. It then, in the controller namespace:
//
//  1. stops every restore Job of the claim, the way Cleanup does (see
//     stopRestore). While one of them may still write, the pass records a
//     Normal WaitingForMover event that says what the stop waits for, and
//     requeues after MoverPoll, because a restic restore stopped mid-way
//     leaves its repository lock and its pod may still write the prime;
//  2. deletes the Secret copy and the prime claim PrimeClaimName(uid);
//  3. removes ClaimFinalizer from the claim with a merge patch that carries
//     the resourceVersion it read, and records a Warning DataSourceGone event.
//
// Every delete ignores NotFound and the finalizer goes last, so a pass that
// fails anywhere is repeated from the start and converges. It returns an
// error when a read, the stop, a delete or the patch fails; a conflict on
// the patch is returned the same way.
func (r *OrphanReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	claim := &corev1.PersistentVolumeClaim{}
	if err := r.Reader.Get(ctx, req.NamespacedName, claim); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !r.orphaned(claim) {
		return ctrl.Result{}, nil
	}
	vrKey := types.NamespacedName{Namespace: claim.Namespace, Name: claim.Spec.DataSourceRef.Name}
	err := r.Reader.Get(ctx, vrKey, &backupv1alpha1.VolumeRestore{})
	switch {
	case err == nil:
		return ctrl.Result{}, nil
	case !apierrors.IsNotFound(err):
		return ctrl.Result{}, fmt.Errorf("get VolumeRestore %s: %w", vrKey, err)
	}

	state, err := stopRestore(ctx, NewJobs(r.Reader, r.Client), r.Namespace, claim.UID)
	if err != nil {
		return ctrl.Result{}, err
	}
	if !state.Stopped {
		r.Recorder.Eventf(claim, nil, corev1.EventTypeNormal, "WaitingForMover", "Cleanup",
			"VolumeRestore %s is gone; %s before the cleanup goes on", vrKey.Name, state)
		return ctrl.Result{RequeueAfter: r.poll()}, nil
	}
	return ctrl.Result{}, r.release(ctx, claim, vrKey.Name)
}

// poll returns MoverPoll, or defaultMoverPoll when it is not set.
func (r *OrphanReconciler) poll() time.Duration {
	if r.MoverPoll <= 0 {
		return defaultMoverPoll
	}
	return r.MoverPoll
}

// release deletes what the library and Populate left in the controller
// namespace for a claim whose restore Jobs are stopped, then removes
// ClaimFinalizer from the claim.
//
// Parameters:
//   - claim is the orphaned claim, as Reconcile read it.
//   - volumeRestore is the name of the VolumeRestore that is gone, for the
//     event.
//
// It returns an error when a delete or the patch fails, and nil when the
// claim is already gone at the patch.
func (r *OrphanReconciler) release(ctx context.Context, claim *corev1.PersistentVolumeClaim, volumeRestore string) error {
	secretName := SecretCopyName(claim.UID)
	secret := &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: secretName, Namespace: r.Namespace}}
	if err := r.Client.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete Secret copy %s/%s: %w", r.Namespace, secretName, err)
	}
	primeName := PrimeClaimName(claim.UID)
	prime := &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: primeName, Namespace: r.Namespace}}
	if err := r.Client.Delete(ctx, prime); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete prime claim %s/%s: %w", r.Namespace, primeName, err)
	}

	base := claim.DeepCopy()
	controllerutil.RemoveFinalizer(claim, ClaimFinalizer)
	if err := r.Client.Patch(ctx, claim, client.MergeFromWithOptions(base, client.MergeFromWithOptimisticLock{})); err != nil {
		if apierrors.IsNotFound(err) {
			return nil
		}
		return fmt.Errorf("remove finalizer %s from claim %s/%s: %w", ClaimFinalizer, claim.Namespace, claim.Name, err)
	}
	r.Recorder.Eventf(base, nil, corev1.EventTypeWarning, "DataSourceGone", "Cleanup",
		"VolumeRestore %s was deleted before the populator finished with this claim; stopped restore Job %s, deleted Secret copy %s and prime claim %s in %s, and removed finalizer %s",
		volumeRestore, JobName(claim.UID), secretName, primeName, r.Namespace, ClaimFinalizer)
	return nil
}
