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
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
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

// volsyncCheckInterval is how long a run held by volsyncUnsupported waits
// before it looks at the served versions again. An upgrade of VolSync or of
// the controller takes minutes, and a check costs no API call while the
// RESTMapper has the group cached.
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
// that failure too. So the check holds a run only on a real incompatibility.
func volsyncUnsupported(mapper meta.RESTMapper) string {
	var unserved []string
	for _, gvk := range volsyncKinds {
		_, err := mapper.RESTMapping(gvk.GroupKind(), gvk.Version)
		var partial *apiutil.ErrResourceDiscoveryFailed
		if err == nil || !meta.IsNoMatchError(err) || errors.As(err, &partial) {
			continue
		}
		versions := served.Versions(mapper, gvk.GroupKind())
		if len(versions) == 0 {
			continue
		}
		unserved = append(unserved, fmt.Sprintf("%s at %s", gvk.Kind, strings.Join(versions, ", ")))
	}
	if len(unserved) == 0 {
		return ""
	}
	return fmt.Sprintf("the API server serves VolSync's %s and no longer at %s, the one version this backup-controller "+
		"reads and writes; the run changes nothing until %s is served again or a backup-controller release that supports this "+
		"VolSync version is installed (see docs/compatibility.md)",
		strings.Join(unserved, " and "), volsyncv1alpha1.GroupVersion, volsyncv1alpha1.GroupVersion)
}

// holdForVolSync holds a run that may touch VolSync objects while
// volsyncUnsupported reports an incompatible VolSync, and returns true when
// it did.
//
// Parameters:
//   - c writes the run's status.
//   - run is the BackupRun or RestoreRun.
//   - conditions is the run's condition list, which holdForVolSync updates.
//   - generation is the run's metadata.generation for the condition.
//
// It sets Ready to False with reason VolSyncUnsupported and the message of
// volsyncUnsupported, writing the status only when the condition changed,
// logs the message, and returns a result that checks again after
// volsyncCheckInterval. It returns an error when the status write fails.
// It changes nothing else: no VolSync object is created, read or deleted on
// a guess, no workload is stopped or started, and the run is not ended.
func holdForVolSync(ctx context.Context, c client.Client, run client.Object, conditions *[]metav1.Condition, generation int64) (bool, ctrl.Result, error) {
	message := volsyncUnsupported(c.RESTMapper())
	if message == "" {
		return false, ctrl.Result{}, nil
	}
	log.FromContext(ctx).Error(errors.New(message), "holding the run: VolSync is not served at the version this controller uses")
	ready := meta.FindStatusCondition(*conditions, backupv1alpha1.ConditionReady)
	if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != backupv1alpha1.ReasonVolSyncUnsupported || ready.Message != message {
		backupv1alpha1.SetReady(conditions, generation, metav1.ConditionFalse, backupv1alpha1.ReasonVolSyncUnsupported, message)
		if err := c.Status().Update(ctx, run); err != nil {
			return true, ctrl.Result{}, fmt.Errorf("set the status of run %s/%s: %w", run.GetNamespace(), run.GetName(), err)
		}
	}
	return true, ctrl.Result{RequeueAfter: volsyncCheckInterval}, nil
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
