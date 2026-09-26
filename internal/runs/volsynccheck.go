package runs

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// volsyncKinds are the VolSync kinds the controller reads and writes, at
// the one version its Go types come from: github.com/backube/volsync
// api/v1alpha1. VolSync has published no other version up to 0.16.0 and on
// its main branch (checked on 2026-09-26), so there is nothing the
// controller could fall back to, and it acts on no guess when v1alpha1 is
// gone.
var volsyncKinds = []schema.GroupVersionKind{
	volsyncv1alpha1.GroupVersion.WithKind("ReplicationSource"),
	volsyncv1alpha1.GroupVersion.WithKind("ReplicationDestination"),
}

// destinationKind is VolSync's ReplicationDestination by group and kind,
// which a RestoreRun looks up at whatever version the API server serves
// when it can't use v1alpha1 (see unservedMovers).
var destinationKind = volsyncv1alpha1.GroupVersion.WithKind("ReplicationDestination").GroupKind()

// volsyncCheckInterval is how long a RestoreRun held by volsyncUnsupported
// waits before it looks at the served versions and its movers again. An
// upgrade of VolSync or of the controller takes minutes, and a check costs
// no API call while the RESTMapper has the group cached.
const volsyncCheckInterval = time.Minute

// volsyncUnsupported reports whether the API server serves a VolSync kind
// the controller uses at another version and no longer at v1alpha1, the one
// version the controller's Go types come from.
//
// Parameters:
//   - mapper is the client's RESTMapper. The manager's mapper answers from
//     its cache; a lookup at v1alpha1 that misses makes it read the group's
//     discovery again, and served.VersionGone makes it forget the group after
//     the API server has answered a request at v1alpha1 with a 404.
//
// It returns a message that names each such kind with the versions the API
// server serves it at, and v1alpha1, or an empty string when every kind is
// served at v1alpha1. It also returns an empty string when it can't tell: when no
// version of a kind is served, VolSync is not installed, and the run's own
// VolSync requests report that; when the lookup fails, the requests report
// that failure too. So a run ends or holds on it only on a real
// incompatibility.
func volsyncUnsupported(mapper meta.RESTMapper) string {
	var unserved []string
	for _, gvk := range volsyncKinds {
		versions := servedElsewhere(mapper, gvk)
		if len(versions) == 0 {
			continue
		}
		unserved = append(unserved, fmt.Sprintf("%s at %s", gvk.Kind, strings.Join(versions, ", ")))
	}
	if len(unserved) == 0 {
		return ""
	}
	return fmt.Sprintf("the API server serves VolSync's %s and no longer at %s, the one version this backup-controller "+
		"reads and writes; no run creates, reads or deletes a VolSync object until %s is served again or a backup-controller "+
		"release that supports this VolSync version is installed (see docs/compatibility.md)",
		strings.Join(unserved, " and "), volsyncv1alpha1.GroupVersion, volsyncv1alpha1.GroupVersion)
}

// endForVolSync ends a BackupRun that may touch VolSync objects while
// volsyncUnsupported reports an incompatible VolSync.
//
// Parameters:
//   - run is the unfinished BackupRun, not being deleted.
//   - message is volsyncUnsupported's message, which becomes the Ready
//     message and each unfinished item's message.
//
// It returns what abort returns: nil once the run has ended, or the error
// of a restart or release that failed, for a retry.
//
// Giving the app back needs no VolSync object, so the run ends through
// abort with reason VolSyncUnsupported: it starts the workloads it stopped,
// resumes the Kustomizations it suspended and releases its Leases. A sync
// VolSync already started goes on under VolSync's own control, as after a
// run's timeout; a later run of the claim holds back while that sync's
// trigger is open.
func (r *BackupRunReconciler) endForVolSync(ctx context.Context, run *backupv1alpha1.BackupRun, message string) error {
	log.FromContext(ctx).Error(errors.New(message), "ending the run: VolSync is not served at the version this controller uses",
		"namespace", run.Namespace, "name", run.Name)
	return r.abort(ctx, run, backupv1alpha1.ReasonVolSyncUnsupported, message)
}

// endForVolSync ends or holds a RestoreRun that may touch VolSync objects
// while volsyncUnsupported reports an incompatible VolSync.
//
// Parameters:
//   - run is the unfinished RestoreRun, not being deleted.
//   - message is volsyncUnsupported's message.
//
// It returns the hold's result from holdForVolSync while a mover of the
// run may still write, and what abort returns once none can. A failed look
// at the movers comes back as an error, and nothing is changed.
//
// The run can't stop its movers through a version it has no types for, and
// giving the app back or releasing the claim while a mover writes into it
// would let the app or another run at a half-restored claim. So it holds
// while unservedMovers finds a mover that may still write, and otherwise
// ends through abort with reason VolSyncUnsupported, which gives the app
// back. A run that created nothing ends at once.
func (r *RestoreRunReconciler) endForVolSync(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) (ctrl.Result, error) {
	log.FromContext(ctx).Error(errors.New(message), "VolSync is not served at the version this controller uses",
		"namespace", run.Namespace, "name", run.Name)
	left, err := r.unservedMovers(ctx, run)
	if err != nil {
		return ctrl.Result{}, err
	}
	if waiting := left.message(); waiting != "" {
		return r.holdForVolSync(ctx, run, message+". "+waiting)
	}
	return r.abort(ctx, run, backupv1alpha1.ReasonVolSyncUnsupported, message)
}

// holdForVolSync holds a RestoreRun whose mover may still write while
// VolSync is not served at v1alpha1.
//
// Parameters:
//   - run is the RestoreRun. Its Ready condition is set and its status
//     written when the condition changed.
//   - message is the Ready message: volsyncUnsupported's message and the
//     mover the run waits for.
//
// It returns a result that looks again after volsyncCheckInterval, and the
// error of the status write, if any. It sets Ready to False with reason
// VolSyncUnsupported and changes nothing else: the phase, the workloads the
// run stopped and its Leases stay as they are.
func (r *RestoreRunReconciler) holdForVolSync(ctx context.Context, run *backupv1alpha1.RestoreRun, message string) (ctrl.Result, error) {
	if !readyChanged(run, backupv1alpha1.ReasonVolSyncUnsupported, message) {
		return ctrl.Result{RequeueAfter: volsyncCheckInterval}, nil
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonVolSyncUnsupported, message)
	return after(volsyncCheckInterval, r.writeStatus(ctx, run))
}

// stopMovers stops the run's movers when the run ends or is deleted, and
// returns those that are not gone yet (rule X2).
//
// Parameters:
//   - run is the RestoreRun that ends. An item whose mover is gone has its
//     destination cleared in place, and the caller writes the status.
//
// It returns what removeDestinations returns while VolSync serves
// v1alpha1, and what unservedMovers returns while volsyncUnsupported
// reports that it doesn't: the run can then neither read nor delete its
// destinations, and waits until a person has deleted them.
func (r *RestoreRunReconciler) stopMovers(ctx context.Context, run *backupv1alpha1.RestoreRun) (moverList, error) {
	if volsyncUnsupported(r.RESTMapper()) != "" {
		return r.unservedMovers(ctx, run)
	}
	return r.removeDestinations(ctx, run, anyItem)
}

// unservedMovers returns the run's movers that may still write while VolSync
// is not served at v1alpha1, without changing anything in the cluster.
//
// Parameters:
//   - run is the RestoreRun. An item whose named destination and mover are
//     gone has its destination cleared in place, as removeDestinations does,
//     and the caller writes the status.
//
// It returns the movers that may still write, in the order of
// status.items, and an empty list when none can. An error is a
// *releaseError from a failed look, and the caller changes nothing.
//
// Each item that names a destination is looked at, and so is each volume
// item that names none under the name its destination would have, since a
// pass that created it may have lost the status write (see lostDestination).
// A mover may still write while unservedMoverRemains finds its destination,
// its Job or a pod of it. A destination the item names counts as gone only
// once pollInterval has passed since the run first found it gone, as in
// removeDestinations, since VolSync may create the mover's Job while it
// handles the delete.
func (r *RestoreRunReconciler) unservedMovers(ctx context.Context, run *backupv1alpha1.RestoreRun) (moverList, error) {
	var left moverList
	for i := range run.Status.Items {
		item := &run.Status.Items[i]
		name := item.Destination
		if name == "" && item.Kind == "PersistentVolumeClaim" {
			name = destinationName(run.UID, i)
		}
		if name == "" {
			continue
		}
		now := r.Now() // before the look, as in removeDestinations
		kept, what, err := r.unservedMoverRemains(ctx, run.Namespace, name)
		if err != nil {
			return nil, destinationReleaseError(name, err)
		}
		mover := moverLeft{item: item.Name, destination: name, what: what, namespace: run.Namespace, unserved: true, kept: kept}
		if kept || what != "" {
			left = append(left, mover)
			continue
		}
		if item.Destination == "" {
			continue
		}
		if !r.stops.Gone(stopKey(run, name), now, pollInterval) {
			left = append(left, mover)
			continue
		}
		item.Destination = ""
	}
	return left, nil
}

// unservedMoverRemains returns what is left of the mover of the
// ReplicationDestination with the given name while VolSync is not served
// at v1alpha1, and "" once nothing of it is left.
//
// Parameters:
//   - namespace is the run's namespace, which holds the destination, the
//     mover's Job and its pods.
//   - destination is the name of the ReplicationDestination.
//
// It returns true while the destination exists at the version the API
// server serves. Otherwise it returns false and what moverRemains and
// ownedJob find, such as "Job <name>". A failed lookup or read comes back
// as an error.
//
// Whether an object exists needs no knowledge of its fields, so the
// destination is read as an unstructured object at the served version. The
// mover's Job is looked for both by the name VolSync 0.16 gives it and by
// its owner reference to the destination, so a VolSync release that names
// its Jobs another way still holds the run.
func (r *RestoreRunReconciler) unservedMoverRemains(ctx context.Context, namespace, destination string) (bool, string, error) {
	_, err := served.Get(ctx, r.Reader, r.RESTMapper(), destinationKind, types.NamespacedName{Namespace: namespace, Name: destination})
	switch {
	case err == nil:
		return true, "", nil
	case !apierrors.IsNotFound(err):
		return false, "", fmt.Errorf("get ReplicationDestination %s: %w", destination, err)
	}
	what, err := moverRemains(ctx, r.Reader, namespace, destination)
	if err != nil || what != "" {
		return false, what, err
	}
	what, err = ownedJob(ctx, r.Reader, namespace, destination)
	return false, what, err
}

// ownedJob returns "Job <name>" for a Job in the namespace whose owner
// references name the ReplicationDestination destination, and "" when there
// is none.
//
// Parameters:
//   - c is the uncached reader.
//   - namespace is the run's namespace.
//   - destination is the name of the ReplicationDestination.
//
// VolSync owns a mover's Job by its destination at every version so far, and
// the garbage collector deletes the Job once the destination is gone. A
// failed list comes back as an error.
func ownedJob(ctx context.Context, c client.Reader, namespace, destination string) (string, error) {
	jobs := &batchv1.JobList{}
	if err := c.List(ctx, jobs, client.InNamespace(namespace)); err != nil {
		return "", fmt.Errorf("list the Jobs of ReplicationDestination %s: %w", destination, err)
	}
	for _, job := range jobs.Items {
		for _, owner := range job.OwnerReferences {
			group, _, _ := strings.Cut(owner.APIVersion, "/")
			if group == destinationKind.Group && owner.Kind == destinationKind.Kind && owner.Name == destination {
				return "Job " + job.Name, nil
			}
		}
	}
	return "", nil
}

// unservedMessage returns the Ready message for a run that waits for the
// mover m while VolSync is not served at v1alpha1.
//
// The message names the destination and what is left of its mover. While
// the destination is there, it says how a person deletes it: the run can
// neither stop the mover nor read how it ended through a version it has no
// types for, and goes on by itself once the destination and the mover are
// gone.
func (m moverLeft) unservedMessage() string {
	switch {
	case m.kept:
		return fmt.Sprintf("ReplicationDestination %s, whose mover restores %s, is still there, and the run can neither stop that mover "+
			"nor read how it ended while VolSync is not served at %s. Once the mover is done, or may be stopped, delete it with "+
			"kubectl delete replicationdestinations.volsync.backube %s -n %s; the run gives the app back and lets other runs at the "+
			"claim once the mover's Job and pods are gone",
			m.destination, m.item, volsyncv1alpha1.GroupVersion, m.destination, m.namespace)
	case m.what == "":
		return fmt.Sprintf("ReplicationDestination %s, whose mover restored %s, is gone, and the run looks for the mover's Job and pods "+
			"once %s have passed since it found it gone", m.destination, m.item, pollInterval)
	}
	return fmt.Sprintf("the mover of ReplicationDestination %s, which restored %s, is not gone: its %s is still there. "+
		"The run gives the app back and lets other runs at the claim only after that", m.destination, m.item, m.what)
}

// serve wraps the reconciler's client and reader so that a request at a
// version the API server has stopped serving is an error the reconcile
// retries, never a NotFound (see served.Client). SetupWithManager calls it.
func (r *BackupRunReconciler) serve() {
	r.Client = served.Client(r.Client)
	r.Reader = served.Reader(r.Reader, r.Client)
}

// serve wraps the reconciler's client and reader as
// BackupRunReconciler.serve does. A ReplicationDestination read at a version
// VolSync no longer serves would otherwise read as a destination someone
// deleted, and the run would take its mover for stopped.
func (r *RestoreRunReconciler) serve() {
	r.Client = served.Client(r.Client)
	r.Reader = served.Reader(r.Reader, r.Client)
}

// VolSyncUnsupported is volsyncUnsupported for cmd/backup-controller, which
// logs the message once at startup, so an incompatible VolSync shows in the
// controller's log before any run reports it.
func VolSyncUnsupported(mapper meta.RESTMapper) string {
	return volsyncUnsupported(mapper)
}
