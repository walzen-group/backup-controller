package quiesce

import (
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// specProblem tells what is wrong with an entry of spec.quiesce.
type specProblem int

const (
	// specKindUnsupported is an entry of a kind that the run cannot stop.
	specKindUnsupported specProblem = iota
	// specWorkloadMissing is an entry that names a workload the namespace
	// does not hold.
	specWorkloadMissing
)

// SpecError is an entry in a RestoreRun's spec.quiesce that the run
// cannot act on. The entry has a kind that the run cannot stop, or it names
// a workload that the namespace does not hold. A new read does not change
// it, so the run ends. Named returns a failed read of the API server as a
// plain error, and the run tries again.
type SpecError struct {
	// Ref is the entry of spec.quiesce.
	Ref backupv1alpha1.WorkloadRef

	// problem tells what is wrong with the entry.
	problem specProblem
}

// Error returns a sentence that names the entry and the problem.
func (e *SpecError) Error() string {
	if e.problem == specWorkloadMissing {
		return fmt.Sprintf("spec.quiesce lists %s %s, which this namespace does not hold", e.Ref.Kind, e.Ref.Name)
	}
	return fmt.Sprintf("spec.quiesce lists %s %s; only a Deployment or a StatefulSet can be stopped", e.Ref.Kind, e.Ref.Name)
}

// CrossNamespaceError is a Kustomization that applies a target and also
// applies a Deployment or a StatefulSet in another namespace. Plan refuses
// it. If the run suspends that Kustomization, then Flux does not manage the
// workloads of the other namespace.
type CrossNamespaceError struct {
	// Kustomization is the "namespace/name" key of the Kustomization.
	Kustomization string

	// Namespaces are the namespaces of the workloads that the Kustomization
	// applies, sorted. The list includes the namespace of the run.
	Namespaces []string
}

// Error returns a sentence that names the Kustomization and the
// namespaces, and tells a person what to do.
func (e *CrossNamespaceError) Error() string {
	return fmt.Sprintf("Kustomization %s applies workloads in namespaces %s; a run suspends the Kustomization while it stops workloads, "+
		"which would leave the other namespace's workloads unmanaged by Flux, so it refuses. Give each namespace its own Kustomization",
		e.Kustomization, joinAnd(e.Namespaces))
}

// InventoryProblem tells what is wrong with the status.inventory of a
// Kustomization.
type InventoryProblem int

const (
	// InventoryMissing is a Kustomization that has no list at
	// status.inventory.entries.
	InventoryMissing InventoryProblem = iota
	// InventoryEntryMalformed is an entry of status.inventory.entries
	// whose id does not have four parts.
	InventoryEntryMalformed
)

// InventoryError is a Kustomization whose status.inventory the run cannot
// read. inventoryIDs returns it, and Plan returns it with no change.
type InventoryError struct {
	// Kustomization is the "namespace/name" key of the Kustomization.
	Kustomization string

	// Problem tells what is wrong with the inventory.
	Problem InventoryProblem

	// Entry is the entry that is not correct, as fmt shows it with %v. It
	// is empty for InventoryMissing.
	Entry string
}

// Error returns a sentence that names the Kustomization, the field, and
// for InventoryEntryMalformed the entry.
func (e *InventoryError) Error() string {
	if e.Problem == InventoryEntryMalformed {
		return fmt.Sprintf("the Kustomization %s has an entry %s in status.inventory.entries whose id is not "+
			"\"<namespace>_<name>_<group>_<kind>\"; a Flux release may have changed the format (see docs/compatibility.md)", e.Kustomization, e.Entry)
	}
	return fmt.Sprintf("the Kustomization %s has no list at status.inventory.entries, where kustomize-controller records every object it "+
		"applies; either it has not applied yet or a Flux release moved the field (see docs/compatibility.md)", e.Kustomization)
}

// RestartError is a failure of Restart. It says which workload or
// Kustomization the run could not put back, so the run can name it on its
// Ready condition.
type RestartError struct {
	// action is what the run could not do, such as "give Deployment notes
	// its 2 replicas back".
	action string

	// err is the error from the API server or the RESTMapper.
	err error
}

// Error returns "could not ", the action, and the error.
func (e *RestartError) Error() string { return "could not " + e.action + ": " + e.err.Error() }

// Unwrap returns the error from the API server or the RESTMapper.
func (e *RestartError) Unwrap() error { return e.err }
