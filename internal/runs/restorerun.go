package runs

import (
	"context"
	"fmt"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
)

// RestoreRunReconciler writes a chosen snapshot back into a volume.
//
// It has two modes and they share almost nothing. With Into set it creates a
// second claim and lets the populator fill it, which touches the app's volume
// not at all. Without it the restore is in place, which means a Direct-mode
// ReplicationDestination mounting the claim the app uses, and that one waits
// until nothing else has the claim mounted.
type RestoreRunReconciler struct {
	client.Client

	// Now is the clock, injected so tests can move time without sleeping.
	Now func() time.Time
}

// SetupWithManager registers the reconciler.
func (r *RestoreRunReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Now == nil {
		r.Now = time.Now
	}
	return ctrl.NewControllerManagedBy(mgr).
		For(&backupv1alpha1.RestoreRun{}).
		Named("restorerun").
		Complete(r)
}

// Reconcile drives one RestoreRun from submitted to finished.
func (r *RestoreRunReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	run := &backupv1alpha1.RestoreRun{}
	if err := r.Get(ctx, req.NamespacedName, run); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	if !run.DeletionTimestamp.IsZero() {
		return ctrl.Result{}, r.finalize(ctx, run)
	}
	if run.Status.Phase.Finished() {
		return r.expire(ctx, run)
	}
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		controllerutil.AddFinalizer(run, Finalizer)
		if err := r.Update(ctx, run); err != nil {
			return ctrl.Result{}, fmt.Errorf("add the finalizer to RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
		}
	}

	repository, err := r.repositoryFor(ctx, run)
	if err != nil {
		return ctrl.Result{}, r.fail(ctx, run, backupv1alpha1.ReasonInvalid, err.Error())
	}

	if run.Spec.Into != "" {
		return r.reconcileIntoNewClaim(ctx, run, repository)
	}
	return r.reconcileInPlace(ctx, run, repository)
}

// reconcileInPlace overwrites the claim the app already has.
//
// The mover mounts that claim and writes into it, and ReadWriteOnce restricts a
// claim to one node rather than to one pod, so Kubernetes would let the app
// mount it at the same time. Two writers on one filesystem is how the volume
// being restored is corrupted, so this waits rather than scaling anything: who
// stops the workload depends on what deployed it, and a controller that scaled
// a Flux-owned Deployment would be reverted on its next reconcile, mid-restore.
func (r *RestoreRunReconciler) reconcileInPlace(ctx context.Context, run *backupv1alpha1.RestoreRun, repository restoreSettings) (ctrl.Result, error) {
	run.Status.Target = run.Spec.Claim

	name := "restore-" + string(run.UID)
	destination := &volsyncv1alpha1.ReplicationDestination{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: name}
	err := r.Get(ctx, key, destination)

	switch {
	case apierrors.IsNotFound(err):
		holder, err := r.claimHolder(ctx, run.Namespace, run.Spec.Claim)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("look for a pod holding claim %s: %w", run.Spec.Claim, err)
		}
		if holder != "" {
			return ctrl.Result{RequeueAfter: pollInterval}, r.wait(ctx, run, backupv1alpha1.ReasonClaimInUse,
				fmt.Sprintf("claim %s is mounted by pod %s; stop the workload and this restore starts on its own", run.Spec.Claim, holder))
		}
		if err := r.Create(ctx, directDestination(run, repository, name)); err != nil {
			return ctrl.Result{}, fmt.Errorf("create ReplicationDestination %s: %w", key, err)
		}
		return ctrl.Result{RequeueAfter: pollInterval}, r.begin(ctx, run, name)

	case err != nil:
		return ctrl.Result{}, fmt.Errorf("get ReplicationDestination %s: %w", key, err)
	}

	if reason, failed := failedMover(destination); failed {
		return ctrl.Result{}, r.cleanupThen(ctx, run, destination, backupv1alpha1.ReasonFailed, reason)
	}
	if destination.Status == nil || destination.Status.LastManualSync != string(run.UID) {
		if expired, deadline := r.timedOut(run); expired {
			return ctrl.Result{}, r.cleanupThen(ctx, run, destination, backupv1alpha1.ReasonTimedOut,
				fmt.Sprintf("the mover had not finished by %s", deadline.Format(time.RFC3339)))
		}
		return ctrl.Result{RequeueAfter: pollInterval}, nil
	}
	return ctrl.Result{}, r.cleanupThen(ctx, run, destination, backupv1alpha1.ReasonSucceeded,
		fmt.Sprintf("claim %s holds the restored data", run.Spec.Claim))
}

// reconcileIntoNewClaim fills a second claim and leaves the app's alone.
//
// It writes a VolumeRestore carrying the chosen point in time and a claim
// naming it, so the ordinary populator path does the work. Nothing here has to
// stop, which is what makes this the shape to reach for when the question is
// whether an older backup is any better.
func (r *RestoreRunReconciler) reconcileIntoNewClaim(ctx context.Context, run *backupv1alpha1.RestoreRun, repository restoreSettings) (ctrl.Result, error) {
	run.Status.Target = run.Spec.Into

	vr := pointInTimeRestore(run, repository)
	if err := r.Create(ctx, vr); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("create VolumeRestore %s/%s: %w", vr.Namespace, vr.Name, err)
	}

	claim := scratchClaim(run, repository, vr.Name)
	if err := r.Create(ctx, claim); err != nil && !apierrors.IsAlreadyExists(err) {
		return ctrl.Result{}, fmt.Errorf("create PersistentVolumeClaim %s/%s: %w", claim.Namespace, claim.Name, err)
	}

	bound := &corev1.PersistentVolumeClaim{}
	key := types.NamespacedName{Namespace: run.Namespace, Name: run.Spec.Into}
	if err := r.Get(ctx, key, bound); err != nil {
		return ctrl.Result{}, fmt.Errorf("get PersistentVolumeClaim %s: %w", key, err)
	}
	if bound.Status.Phase == corev1.ClaimBound {
		return ctrl.Result{}, r.succeed(ctx, run,
			fmt.Sprintf("claim %s is bound and holds the restored data", run.Spec.Into))
	}
	if expired, deadline := r.timedOut(run); expired {
		return ctrl.Result{}, r.fail(ctx, run, backupv1alpha1.ReasonTimedOut,
			fmt.Sprintf("claim %s had not bound by %s", run.Spec.Into, deadline.Format(time.RFC3339)))
	}
	if run.Status.StartedAt == nil {
		return ctrl.Result{RequeueAfter: pollInterval}, r.begin(ctx, run, "")
	}
	return ctrl.Result{RequeueAfter: pollInterval}, nil
}

// claimHolder returns the name of a pod mounting the claim, empty when none is.
func (r *RestoreRunReconciler) claimHolder(ctx context.Context, namespace, claim string) (string, error) {
	pods := &corev1.PodList{}
	if err := r.List(ctx, pods, client.InNamespace(namespace)); err != nil {
		return "", err
	}
	for _, pod := range pods.Items {
		if !pod.DeletionTimestamp.IsZero() {
			continue
		}
		for _, volume := range pod.Spec.Volumes {
			if volume.PersistentVolumeClaim != nil && volume.PersistentVolumeClaim.ClaimName == claim {
				return pod.Name, nil
			}
		}
	}
	return "", nil
}

// cleanupThen removes the destination and then records the terminal phase, so
// nothing this run created outlives the run whichever way it ended.
func (r *RestoreRunReconciler) cleanupThen(ctx context.Context, run *backupv1alpha1.RestoreRun, destination *volsyncv1alpha1.ReplicationDestination, reason, message string) error {
	if err := r.Delete(ctx, destination); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("delete ReplicationDestination %s/%s: %w", destination.Namespace, destination.Name, err)
	}
	if reason == backupv1alpha1.ReasonSucceeded {
		return r.succeed(ctx, run, message)
	}
	return r.fail(ctx, run, reason, message)
}

func (r *RestoreRunReconciler) begin(ctx context.Context, run *backupv1alpha1.RestoreRun, destination string) error {
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseRunning
	run.Status.Destination = destination
	if run.Status.StartedAt == nil {
		run.Status.StartedAt = &now
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonRunning,
		fmt.Sprintf("restoring into claim %s", run.Status.Target))
	return r.writeStatus(ctx, run)
}

func (r *RestoreRunReconciler) wait(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	run.Status.Phase = backupv1alpha1.RunPhaseWaiting
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, reason, message)
	return r.writeStatus(ctx, run)
}

func (r *RestoreRunReconciler) succeed(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) error {
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseSucceeded
	run.Status.CompletedAt = &now
	run.Status.Destination = ""
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, backupv1alpha1.ReasonSucceeded, message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	return r.release(ctx, run)
}

func (r *RestoreRunReconciler) fail(ctx context.Context, run *backupv1alpha1.RestoreRun, reason, message string) error {
	now := metav1.NewTime(r.Now())
	run.Status.Phase = backupv1alpha1.RunPhaseFailed
	run.Status.CompletedAt = &now
	run.Status.Destination = ""
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionTrue, reason, message)
	if err := r.writeStatus(ctx, run); err != nil {
		return err
	}
	return r.release(ctx, run)
}

// finalize removes the destination on the way to deletion. A run deleted while
// its mover writes would otherwise leave a Direct-mode restore running against
// a claim with nothing tracking it.
func (r *RestoreRunReconciler) finalize(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	if run.Status.Destination != "" {
		destination := &volsyncv1alpha1.ReplicationDestination{
			ObjectMeta: metav1.ObjectMeta{Namespace: run.Namespace, Name: run.Status.Destination},
		}
		if err := r.Delete(ctx, destination); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete ReplicationDestination %s/%s: %w", run.Namespace, run.Status.Destination, err)
		}
	}
	return r.release(ctx, run)
}

func (r *RestoreRunReconciler) release(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if !controllerutil.ContainsFinalizer(run, Finalizer) {
		return nil
	}
	controllerutil.RemoveFinalizer(run, Finalizer)
	if err := r.Update(ctx, run); err != nil {
		return fmt.Errorf("remove the finalizer from RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// expire deletes a finished run once its TTL has passed. A run with no TTL is
// kept as the record of what was restored and when.
func (r *RestoreRunReconciler) expire(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	if run.Spec.TTLSecondsAfterFinished == nil || run.Status.CompletedAt == nil {
		return ctrl.Result{}, nil
	}
	deadline := run.Status.CompletedAt.Add(time.Duration(*run.Spec.TTLSecondsAfterFinished) * time.Second)
	if remaining := deadline.Sub(r.Now()); remaining > 0 {
		return ctrl.Result{RequeueAfter: remaining}, nil
	}
	if err := r.Delete(ctx, run); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, fmt.Errorf("delete the expired RestoreRun %s/%s: %w", run.Namespace, run.Name, err)
	}
	return ctrl.Result{}, nil
}

func (r *RestoreRunReconciler) timedOut(run *backupv1alpha1.RestoreRun) (bool, time.Time) {
	if run.Status.StartedAt == nil || run.Spec.Timeout == nil {
		return false, time.Time{}
	}
	deadline := run.Status.StartedAt.Add(run.Spec.Timeout.Duration)
	return !r.Now().Before(deadline), deadline
}

func (r *RestoreRunReconciler) writeStatus(ctx context.Context, run *backupv1alpha1.RestoreRun) error {
	if err := r.Status().Update(ctx, run); err != nil {
		return fmt.Errorf("set RestoreRun %s/%s status: %w", run.Namespace, run.Name, err)
	}
	return nil
}

// failedMover reports a destination whose mover gave up, with restic's message.
func failedMover(destination *volsyncv1alpha1.ReplicationDestination) (string, bool) {
	if destination.Status == nil || destination.Status.LatestMoverStatus == nil {
		return "", false
	}
	mover := destination.Status.LatestMoverStatus
	return mover.Logs, mover.Result == volsyncv1alpha1.MoverResultFailed
}
