package runs

import (
	"context"
	"errors"
	"fmt"
	"strings"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
// served at v1alpha1. It also returns an empty string when it can't tell:
// when no version of a kind is served, VolSync is not installed, and the
// run's own VolSync requests report that; when the lookup fails, the
// requests report that failure too. So a BackupRun ends on it only on a
// real incompatibility.
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

// serve wraps the reconciler's client and reader so that a request at a
// version the API server has stopped serving is an error the reconcile
// retries, never a NotFound (see served.Client). SetupWithManager calls it.
func (r *BackupRunReconciler) serve() {
	r.Client = served.Client(r.Client)
	r.Reader = served.Reader(r.Reader, r.Client)
}

// serve wraps the reconciler's client and reader as
// BackupRunReconciler.serve does. A request for a VolSync object at a
// version VolSync no longer serves then fails with an error that names the
// kind and v1alpha1, which the run retries and shows on its Ready
// condition, and no caller that checks for NotFound reads it as an object
// that is gone.
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
