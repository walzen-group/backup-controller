package runs

import (
	"context"
	"errors"
	"fmt"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/served"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// The code in this file shows on a RestoreRun's Ready condition why a run
// past its checks waits while VolSync no longer serves v1alpha1.

// restore makes one pass over a RestoreRun past its checks: through
// restoreIntoEmptyClaim for an into restore, and through work otherwise.
//
// Parameters:
//   - run is the RestoreRun, with a phase, no ending recorded, and not
//     being deleted.
//
// It returns what the pass returns. When the pass failed on a VolSync
// request at a version the API server no longer serves, the error is also
// shown on the run's Ready condition (see showVolSyncWait), and an error of
// that status write is joined to it.
//
// The run changes nothing on that error and retries it on every pass, with
// an app it has stopped kept stopped, until its spec.timeout: the deadline
// check at the start of each pass needs no VolSync object, so the run then
// ends TimedOut, stops its restore Jobs and gives the app back.
func (r *RestoreRunReconciler) restore(ctx context.Context, run *backupv1alpha1.RestoreRun) (ctrl.Result, error) {
	pass := r.work
	if run.Spec.Into != "" {
		pass = r.restoreIntoEmptyClaim
	}
	result, err := pass(ctx, run)
	kind, unserved := volsyncUnserved(err)
	if !unserved {
		return result, err
	}
	return result, errors.Join(err, r.showVolSyncWait(ctx, run, kind, err))
}

// volsyncUnserved finds the VolSync kind that a request failed for because
// the API server no longer serves the version it was sent at.
//
// Parameters:
//   - err is the error of a pass, or nil.
//
// It returns the kind and true for a *served.VersionGoneError, which a
// request sent at a version the API server has stopped serving gets, and
// for the *meta.NoKindMatchError the RESTMapper gives once it has read the
// group's discovery again, when either names VolSync's group, also when
// err wraps it. Any other error, and nil, give false.
func volsyncUnserved(err error) (schema.GroupKind, bool) {
	var kind schema.GroupKind
	var gone *served.VersionGoneError
	var noMatch *meta.NoKindMatchError
	switch {
	case errors.As(err, &gone):
		kind = gone.GVK.GroupKind()
	case errors.As(err, &noMatch):
		kind = noMatch.GroupKind
	}
	return kind, kind.Group == volsyncv1alpha1.GroupVersion.Group
}

// showVolSyncWait puts on a RestoreRun's Ready condition that the run waits
// for VolSync to serve v1alpha1 again.
//
// Parameters:
//   - run is the RestoreRun as this pass read it. Once the stored run
//     shows the condition, it is set on this copy too, so the reconcile
//     records an event when the reason changes.
//   - kind is the VolSync kind the pass's request failed for.
//   - err is the pass's error, which the message quotes.
//
// It returns the error of the read or of the status write, and nil when
// the write went through or was not needed.
//
// The condition has status False and reason VolSyncUnsupported, and its
// message names the kind, v1alpha1 and the versions the API server serves
// the kind at. It is written on the stored run, read through the uncached
// Reader, so nothing else the failed pass changed in its copy is written. A
// stored run that is gone, is being deleted, has recorded an ending or has
// finished keeps its condition, and so does the pass's copy. The copy also
// keeps its condition when the write fails, so no event announces a
// condition the status never held.
func (r *RestoreRunReconciler) showVolSyncWait(ctx context.Context, run *backupv1alpha1.RestoreRun, kind schema.GroupKind, err error) error {
	message := fmt.Sprintf("the API server serves VolSync's %s %s and no longer at %s, the one version this backup-controller "+
		"reads and writes; the run changes nothing and retries until that version is served again or spec.timeout has passed, "+
		"and then ends TimedOut and gives the app back (see docs/compatibility.md): %v",
		kind.Kind, served.Describe(r.RESTMapper(), kind), volsyncv1alpha1.GroupVersion, err)
	stored := &backupv1alpha1.RestoreRun{}
	if err := r.Reader.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
		return client.IgnoreNotFound(fmt.Errorf("read RestoreRun %s/%s to show the VolSync wait: %w", run.Namespace, run.Name, err))
	}
	if stored.UID != run.UID || stored.DeletionTimestamp != nil || stored.Status.Ending != nil || stored.Status.Phase.Finished() {
		return nil
	}
	backupv1alpha1.SetReady(&stored.Status.Conditions, stored.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonVolSyncUnsupported, message)
	if err := r.writeChangedStatus(ctx, stored); err != nil {
		return err
	}
	backupv1alpha1.SetReady(&run.Status.Conditions, run.Generation, metav1.ConditionFalse, backupv1alpha1.ReasonVolSyncUnsupported, message)
	return nil
}
