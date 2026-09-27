package runs

import (
	"context"
	"fmt"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// durablyRestarted reports whether the run's stored status shows that it gave
// the workloads back: status.restartedAt is set and, on a BackupRun,
// status.restartPending is cleared. A RestoreRun writes restartedAt only
// after its restart succeeded.
func durablyRestarted(run client.Object) bool {
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		return r.Status.RestartedAt != nil && !r.Status.RestartPending
	case *backupv1alpha1.RestoreRun:
		return r.Status.RestartedAt != nil
	}
	return false
}

// readStop reads a run again straight from the API server and puts the
// stored record of its stop and restart on the copy the caller holds. A
// caller that is about to start the workloads again calls it first, so that
// it decides on what the API server holds. stopOwed calls it before a stop
// for the same reason.
//
// Parameters:
//   - reader is the uncached Reader. The reconcilers read their run through
//     the informer cache, which can lag behind the run's own last writes: a
//     pass that reads a copy from before the restart was stored would start
//     the workloads again, under the stop of a run that took the namespace's
//     quiesce Lease since.
//   - run is the BackupRun or RestoreRun as the pass read it. Its
//     status.quiescedAt, status.quiesced, status.suspendedKustomizations,
//     status.restartedAt and, on a BackupRun, status.restartPending are
//     replaced with the stored ones when the stored run is newer.
//
// It returns the stored run's status.phase, which is the phase of run when
// run is as new as the stored one. It returns an error, and changes nothing,
// when the read fails, when the run is gone, or when the stored run with
// that name is another object (a different UID). The caller then changes no
// workload and the pass runs again.
//
// A copy whose resourceVersion matches the stored one is left as it is. A
// newer stored run changes only the fields above, so the caller's later
// status write still carries the old resourceVersion and fails with a
// conflict, and the next pass works from the stored run.
func readStop(ctx context.Context, reader client.Reader, run client.Object) (backupv1alpha1.RunPhase, error) {
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		stored := &backupv1alpha1.BackupRun{}
		if err := readStored(ctx, reader, r, stored, backupv1alpha1.KindBackupRun); err != nil {
			return "", err
		}
		if stored.ResourceVersion != r.ResourceVersion {
			r.Status.QuiescedAt, r.Status.Quiesced, r.Status.SuspendedKustomizations = stored.Status.QuiescedAt, stored.Status.Quiesced, stored.Status.SuspendedKustomizations
			r.Status.RestartedAt, r.Status.RestartPending = stored.Status.RestartedAt, stored.Status.RestartPending
		}
		return stored.Status.Phase, nil
	case *backupv1alpha1.RestoreRun:
		stored := &backupv1alpha1.RestoreRun{}
		if err := readStored(ctx, reader, r, stored, backupv1alpha1.KindRestoreRun); err != nil {
			return "", err
		}
		if stored.ResourceVersion != r.ResourceVersion {
			r.Status.QuiescedAt, r.Status.Quiesced, r.Status.SuspendedKustomizations = stored.Status.QuiescedAt, stored.Status.Quiesced, stored.Status.SuspendedKustomizations
			r.Status.RestartedAt = stored.Status.RestartedAt
		}
		return stored.Status.Phase, nil
	}
	return "", fmt.Errorf("read the stop of %T: not a run", run)
}

// readStored reads the stored copy of a run for readStop.
//
// Parameters:
//   - reader is the uncached Reader that readStop got.
//   - run is the run as the pass read it. Its namespace, name and UID are
//     used.
//   - stored receives the stored run. It has the same type as run.
//   - kind is BackupRun or RestoreRun, for the error messages.
//
// It returns an error when the read fails, when the run is gone, or when
// the stored run with that name has a different UID.
func readStored(ctx context.Context, reader client.Reader, run, stored client.Object, kind string) error {
	if err := reader.Get(ctx, client.ObjectKeyFromObject(run), stored); err != nil {
		return fmt.Errorf("read %s %s/%s again before changing its workloads: %w", kind, run.GetNamespace(), run.GetName(), err)
	}
	if stored.GetUID() != run.GetUID() {
		return fmt.Errorf("%s %s/%s is now another object (UID %s, was %s); no workload is changed for the old one",
			kind, run.GetNamespace(), run.GetName(), stored.GetUID(), run.GetUID())
	}
	return nil
}

// stopOwed reports whether a run whose cached copy shows a recorded plan
// without status.quiescedAt still owes the stop, as the API server holds the
// run. quiesce calls it before quiesce.Apply when it took the plan from the
// run's status.
//
// Parameters:
//   - reader is the uncached Reader. A cached copy can lag behind the pass
//     that stopped the workloads, recorded status.quiescedAt, gave the
//     workloads back and ended the run; a stop from that copy would leave the
//     app at 0 with no run left to start it again.
//   - run is the BackupRun or RestoreRun as the pass read it. readStop puts
//     the stored record of its stop and restart on it.
//
// It returns true when the stored run is not finished and still shows the
// plan with neither status.quiescedAt nor status.restartedAt set. A failed
// read comes back as the error from readStop, and the caller stops nothing.
func stopOwed(ctx context.Context, reader client.Reader, run client.Object) (bool, error) {
	phase, err := readStop(ctx, reader, run)
	if err != nil {
		return false, err
	}
	plan, quiescedAt, restartedAt := 0, (*metav1.Time)(nil), (*metav1.Time)(nil)
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		plan, quiescedAt, restartedAt = len(r.Status.Quiesced), r.Status.QuiescedAt, r.Status.RestartedAt
	case *backupv1alpha1.RestoreRun:
		plan, quiescedAt, restartedAt = len(r.Status.Quiesced), r.Status.QuiescedAt, r.Status.RestartedAt
	}
	return !phase.Finished() && plan > 0 && quiescedAt == nil && restartedAt == nil, nil
}

// waitingOn returns a hold of kind holdSourceBusy that names another run in
// the namespace of the run. The run must wait for that other run before it
// stops the workloads of the namespace. It returns the zero hold when there
// is no such run. A run calls it before it takes the quiesce Lease of the
// namespace, with nothing stopped.
//
// Parameters:
//   - reader lists the namespace's RestoreRuns, uncached. A failed list comes
//     back as an error, and nothing is decided on it.
//   - run is the asking run; a RestoreRun with its UID is skipped.
//
// The run it names is an unfinished RestoreRun with a Cluster item in phase
// Deleted. That RestoreRun waits for its Cluster to be created again, and a
// Kustomization this run suspends may be the one Flux needs to create it.
func waitingOn(ctx context.Context, reader client.Reader, run metav1.Object) (hold, error) {
	restores := &backupv1alpha1.RestoreRunList{}
	if err := reader.List(ctx, restores, client.InNamespace(run.GetNamespace())); err != nil {
		return hold{}, fmt.Errorf("list RestoreRuns in %s: %w", run.GetNamespace(), err)
	}
	for i := range restores.Items {
		other := &restores.Items[i]
		if other.UID == run.GetUID() || other.Status.Phase.Finished() {
			continue
		}
		if cluster := deletedCluster(other); cluster != "" {
			return sourceBusy("RestoreRun %s has deleted Cluster %s and waits for it to be created again; this run stops the workloads once that Cluster is back",
				other.Name, cluster), nil
		}
	}
	return hold{}, nil
}

// deletedCluster returns the name of a Cluster the run deleted and waits for
// its owner to create again, or "" when it has none.
func deletedCluster(run *backupv1alpha1.RestoreRun) string {
	for _, item := range run.Status.Items {
		if item.Kind == backupv1alpha1.ItemKindCluster && item.Phase == backupv1alpha1.ItemDeleted {
			return item.Name
		}
	}
	return ""
}

// acquireQuiesceLease takes the namespace's quiesce Lease for the run, so
// that one run at a time stops that namespace's workloads. A run calls it
// right before it plans a stop and holds the Lease until its stored status
// shows the workloads back (see releaseQuiesceLeases).
//
// Parameters:
//   - kind is BackupRun or RestoreRun.
//   - run is the run that takes the Lease.
//
// It returns the zero hold once the run holds the Lease. It returns a hold
// of kind holdSourceBusy that names the holder when another live run holds
// the Lease. It returns an error for a failed API call. The caller waits
// with reason SourceBusy and tries again on a later pass, with nothing
// stopped.
//
// Runs of both kinds take the same Lease, so a backup and a restore of
// different claims in one namespace wait for each other even when their
// workloads do not overlap. The one Lease covers every overlap: a namespace
// run stops every marked workload, and a restore's spec.quiesce usually
// lists some of them.
func acquireQuiesceLease(ctx context.Context, c client.Client, reader client.Reader, run metav1.Object, kind string) (hold, error) {
	holder := leaseHolder{kind: kind, run: run, scope: scopeQuiesce}
	return acquireLease(ctx, c, reader, holder, run.GetNamespace(), quiesceLeaseName)
}

// timedOutMessage returns the Ready message of a run that ends because its
// deadline passed.
//
// Parameters:
//   - deadline is the moment the run had to finish by, which the message
//     gives.
//   - conditions are the run's status.conditions before it ends.
//
// The message ends with the wait the run was in, if any (see waitedFor).
func timedOutMessage(deadline time.Time, conditions []metav1.Condition) string {
	return fmt.Sprintf("the run had not finished by %s", deadline.Format(time.RFC3339)) + waitedFor(conditions)
}

// waitedFor returns the end of a timed-out run's message that says what the
// run was waiting for.
//
// Parameters:
//   - conditions are the run's status.conditions before it ends.
//
// When the run was waiting, for another run (reason SourceBusy) or for the
// API server to serve VolSync's version again (reason VolSyncUnsupported),
// it returns "; it was waiting: " followed by that wait's own message.
// Otherwise it returns an empty string.
func waitedFor(conditions []metav1.Condition) string {
	ready := meta.FindStatusCondition(conditions, backupv1alpha1.ConditionReady)
	if ready == nil || (ready.Reason != backupv1alpha1.ReasonSourceBusy && ready.Reason != backupv1alpha1.ReasonVolSyncUnsupported) {
		return ""
	}
	return "; it was waiting: " + ready.Message
}
