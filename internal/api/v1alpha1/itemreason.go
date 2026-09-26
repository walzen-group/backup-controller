package v1alpha1

// ItemReason is a one-word, machine-readable cause for an item's phase in a
// BackupRun or a RestoreRun. The item's message says the same in a sentence
// for a person; the reason is there for display and alerting, so nothing has
// to read the message to learn why an item ended.
//
// The CRD declares no enum for it: docs/api.md lists the reasons, and a
// release can add one without changing the CRD. An item that ended by a path
// which records no reason leaves it empty.
type ItemReason string

const (
	// ItemReasonTimedOut is an item the run failed because the run's timeout
	// ran out before the item finished.
	ItemReasonTimedOut ItemReason = "TimedOut"

	// ItemReasonMoverFailed is a volume backup whose VolSync mover reported
	// the result Failed.
	ItemReasonMoverFailed ItemReason = "MoverFailed"

	// ItemReasonRestoreJobFailed is a volume restore whose restore Job ended
	// with the condition Failed=True. The item's message carries restic's
	// exit code and what it means.
	ItemReasonRestoreJobFailed ItemReason = "RestoreJobFailed"

	// ItemReasonRestoreJobRefused is a volume restore whose restore Job the
	// API server refused to create, as Forbidden or Invalid. Nothing was
	// written to the claim.
	ItemReasonRestoreJobRefused ItemReason = "RestoreJobRefused"

	// ItemReasonSnapshotChanged is a volume restore whose selected snapshot
	// was no longer in the repository when the run checked it again right
	// before the restore started.
	ItemReasonSnapshotChanged ItemReason = "SnapshotChanged"

	// ItemReasonClaimLost is a volume restore whose claim stopped being the
	// run's while the restore ran: it was deleted, replaced, or taken by
	// something else.
	ItemReasonClaimLost ItemReason = "ClaimLost"

	// ItemReasonNoMoverSnapshot is a volume backup whose mover finished, but
	// the run found no snapshot the mover wrote in the repository.
	ItemReasonNoMoverSnapshot ItemReason = "NoMoverSnapshot"
)

// RunEnding records why a run ended, from the moment the run decided to end
// it. The run writes it in the same status write that fails the unfinished
// items, and every later pass ends the run with this reason and message, also
// one that waits for its movers to stop or retries a restart.
type RunEnding struct {
	// Reason is the Ready condition's reason the run ends with, such as
	// TimedOut.
	Reason string `json:"reason"`
	// Message is the Ready condition's message the run ends with.
	Message string `json:"message"`
}
