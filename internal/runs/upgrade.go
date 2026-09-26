package runs

import (
	"fmt"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// runFormat names the format of a run's status that this release writes and
// reads, and goes into status.plannedBy with every plan. It changes only when
// a release changes what an unfinished run's status means; a patch release
// keeps it. A run planned under another format is not continued (see
// olderPlan).
const runFormat = "v0.9"

// olderPlan reports whether an unfinished run was planned by a release whose
// status format differs from runFormat, so this release must end it rather
// than continue it.
//
// Parameters:
//   - run is a *BackupRun or a *RestoreRun. Any other kind is never older.
//
// A run counts as planned once its status has a phase, items or quiescedAt.
// An older release left status.plannedBy empty. A run nothing has planned
// is new under this release and plans as usual, and a finished run is never
// older: it is left to its time to live. The reconcilers check this after a
// run's deletion is dealt with, so a run being deleted is put back by its
// finalizer as before.
func olderPlan(run client.Object) bool {
	var (
		phase      backupv1alpha1.RunPhase
		plannedBy  string
		items      int
		quiescedAt *metav1.Time
	)
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		phase, plannedBy, items, quiescedAt = r.Status.Phase, r.Status.PlannedBy, len(r.Status.Items), r.Status.QuiescedAt
	case *backupv1alpha1.RestoreRun:
		phase, plannedBy, items, quiescedAt = r.Status.Phase, r.Status.PlannedBy, len(r.Status.Items), r.Status.QuiescedAt
	default:
		return false
	}
	planned := phase != "" || items > 0 || quiescedAt != nil
	return planned && !phase.Finished() && plannedBy != runFormat
}

// stoppedState says what an older run's status records of the workloads it
// stopped, for the message it ends with (see upgradedMessage).
type stoppedState int

const (
	// stoppedNothing is a run with no workload or Kustomization recorded.
	stoppedNothing stoppedState = iota
	// stoppedNotBack is a run whose status records no finished restart. The
	// run's finish gives the workloads back before it records the message.
	stoppedNotBack
	// stoppedBackBefore is a run whose status records the restart as done,
	// so its finish gives nothing back.
	stoppedBackBefore
)

// upgradedMessage returns the Ready message of a run that ends with reason
// Upgraded (see olderPlan). The run's unfinished items carry it too.
//
// Parameters:
//   - kind is BackupRun or RestoreRun, the kind of run to create again.
//   - stopped is what the run's status records of the workloads it stopped.
//     A run whose status records the restart as done is told to check the
//     workloads: a v0.7.2 BackupRun CRD drops status.restartPending, so such
//     a status can hide a restart that failed.
//   - deleted names each Cluster the run deleted that has not been
//     recovered, which a new RestoreRun has to restore.
func upgradedMessage(kind string, stopped stoppedState, deleted []string) string {
	var b strings.Builder
	b.WriteString("this run was started by an older version of backup-controller, which this version does not continue.")
	switch stopped {
	case stoppedNotBack:
		b.WriteString(" The run gave back the workloads it had stopped.")
	case stoppedBackBefore:
		b.WriteString(" Its status says it had given back the workloads it stopped; check that each workload in status.quiesced " +
			"runs with the replicas recorded there, and that no Kustomization in status.suspendedKustomizations is still suspended.")
	}
	fmt.Fprintf(&b, " Create a new %s to run it again.", kind)
	for _, name := range deleted {
		fmt.Fprintf(&b, " Cluster %s was deleted by this run and has not been recovered; create a new RestoreRun for it.", name)
	}
	return b.String()
}
