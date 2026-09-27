package runs

import (
	"errors"
	"fmt"
	"strings"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	"github.com/walzen-group/backup-controller/internal/restorejob"
)

// refusalError is an error no retry can fix: something an item needs is
// missing, refused or no longer the run's. The run fails the item with it,
// or ends with reason Invalid when it meets one while it plans; it never
// retries it.
type refusalError struct {
	// reason is why, as the item records it.
	reason backupv1alpha1.ItemReason
	// text says what is wrong, for a person. No decision reads it.
	text string
}

// Error returns the refusal's text.
func (e *refusalError) Error() string { return e.text }

// sparedKind says which object a refusal left untouched.
type sparedKind int

const (
	// sparedClaim is a refusal of a volume restore before anything was
	// written to its claim.
	sparedClaim sparedKind = iota
	// sparedCluster is a refusal of a database restore before the run
	// deleted anything of the Cluster.
	sparedCluster
)

// spared names what a refusal left untouched.
type spared struct {
	// kind is sparedClaim or sparedCluster.
	kind sparedKind
	// claim names the claim for sparedClaim.
	claim string
}

// sparedError is an error that holds a refusal, marked with what the run
// left as it was when it refused, so a person knows nothing needs repair.
// It unwraps to the error it marks, so errors.As still finds the
// *refusalError and its reason.
type sparedError struct {
	// err is the marked error: the refusal itself, or an error that wraps
	// it. Its text comes first in the message.
	err error
	// spared is what the run left untouched.
	spared spared
}

// Error returns the marked error's text, followed by the sentence that says
// what the run left untouched.
func (e *sparedError) Error() string {
	if e.spared.kind == sparedCluster {
		return e.err.Error() + ". The run deleted nothing"
	}
	return e.err.Error() + nothingWritten(e.spared.claim)
}

// Unwrap returns the marked error.
func (e *sparedError) Unwrap() error { return e.err }

// nothingWritten returns the end of the message of a restore that failed
// before its mover existed: nothing was written to the claim named claim,
// and a new RestoreRun selects again.
func nothingWritten(claim string) string {
	return fmt.Sprintf(". Nothing was written to claim %s. Create a new RestoreRun to select again", claim)
}

// refuse builds a refusal of an item.
//
// Parameters:
//   - reason is the ItemReason the item records when it fails with the
//     refusal.
//   - format and args build the refusal's text for a person, the way
//     fmt.Sprintf builds it.
//
// It returns a *refusalError whose message says only what is wrong;
// nothingWrittenTo and nothingDeleted add what the run left untouched.
func refuse(reason backupv1alpha1.ItemReason, format string, args ...any) error {
	return &refusalError{reason: reason, text: fmt.Sprintf(format, args...)}
}

// nothingWrittenTo marks a refusal of a volume restore as one that wrote
// nothing to the claim, so its message says so.
//
// Parameters:
//   - claim names the claim the item restores into.
//   - err is the error the start check returned. It may be nil.
//
// It returns err marked with the claim spared when err holds a refusal
// (see refusal), and any other error, nil included, unchanged (see
// withSpared).
func nothingWrittenTo(claim string, err error) error {
	return withSpared(err, spared{kind: sparedClaim, claim: claim})
}

// nothingDeleted marks a refusal of a database restore as one that deleted
// nothing of the Cluster, so its message says so.
//
// Parameters:
//   - err is the error the check returned. It may be nil.
//
// It returns err marked with the Cluster spared when err holds a refusal
// (see refusal), and any other error, nil included, unchanged (see
// withSpared).
func nothingDeleted(err error) error {
	return withSpared(err, spared{kind: sparedCluster})
}

// withSpared marks a refusal with what the run left untouched. It is the
// shared part of nothingWrittenTo and nothingDeleted.
//
// Parameters:
//   - err is the error a check returned: a refusal (see refusal), an error
//     that wraps one, any other error, or nil.
//   - what names the object the run left untouched.
//
// It returns a *sparedError that holds err whole, wrapper included, when
// err holds a refusal, so the message is err's own text followed by the
// sentence on what was spared. It returns any other error, nil included,
// unchanged, and an error already marked unchanged too, so the sentence
// never appears twice.
func withSpared(err error, what spared) error {
	var marked *sparedError
	if !refusal(err) || errors.As(err, &marked) {
		return err
	}
	return &sparedError{err: err, spared: what}
}

// refusal reports whether an error refuses an item before the run wrote
// anything for it.
//
// Parameters:
//   - err is the error a check or a create returned. It may be nil.
//
// It returns true when errors.As finds a *refusalError in err, or a
// *restorejob.SpecError, which Build returns for a restore Job spec it
// refuses before the run creates the Job.
func refusal(err error) bool {
	var refused *refusalError
	var badSpec *restorejob.SpecError
	return errors.As(err, &refused) || errors.As(err, &badSpec)
}

// invalidSpecError is a refusal of the run as a whole: its spec names
// nothing it can work on, or names it in a way no retry can fix. The run
// ends with reason Invalid, and no item records it, so it carries no
// ItemReason.
type invalidSpecError struct {
	// text says what is wrong with the spec, for a person.
	text string
}

// Error returns the text, which names the field of the spec and what is
// wrong with it.
func (e *invalidSpecError) Error() string { return e.text }

// invalidSpec builds a refusal of the run as a whole.
//
// Parameters:
//   - format and args build the refusal's text for a person, the way
//     fmt.Sprintf builds it. The text names the field of the spec and what
//     is wrong with it.
//
// It returns an *invalidSpecError, which ends the run with reason Invalid
// and fails no item. Only a function that only run-level sites call returns
// one, because asItemFailure does not take it.
func invalidSpec(format string, args ...any) error {
	return &invalidSpecError{text: fmt.Sprintf(format, args...)}
}

// itemFailure is how an item ends when an error fails it: the reason it
// records and the message a person reads.
type itemFailure struct {
	// reason is the ItemReason the item records, always a constant of the
	// API package.
	reason backupv1alpha1.ItemReason
	// message is the error's text, which the item records for a person.
	// No decision reads it.
	message string
}

// asItemFailure tells whether an error fails an item, and how.
//
// Parameters:
//   - err is the error a start check or a start returned. It may be nil.
//
// It returns the failure and true for an error on this list, checked in
// this order with errors.As: a *refusalError (its own reason), an
// invalidSettingError (SettingsInvalid), a *sourceHeldError for a source no
// run waits for (SourceAbandoned), a *restorejob.FailureError of a restore
// Job that ended Failed (RestoreJobFailed), a *restorejob.SpecError of a
// restore Job the run could not build (RestoreJobRefused), a
// *claimLostError of a claim that is no longer the run's (ClaimLost), and an
// *identifyError of a completed sync that left no record of when it ran
// (NoMoverSnapshot). The message is
// err.Error(). Any other error, and nil, gives false: the pass returns the
// error and retries. An *invalidSpecError is not on the list, because only
// run-level sites meet one, and an item that recorded it would carry a
// reason no item has.
func asItemFailure(err error) (itemFailure, bool) {
	var refused *refusalError
	if errors.As(err, &refused) {
		return itemFailure{reason: refused.reason, message: err.Error()}, true
	}
	var bad invalidSettingError
	if errors.As(err, &bad) {
		return itemFailure{reason: backupv1alpha1.ItemReasonSettingsInvalid, message: err.Error()}, true
	}
	var held *sourceHeldError
	if errors.As(err, &held) {
		return itemFailure{reason: backupv1alpha1.ItemReasonSourceAbandoned, message: err.Error()}, true
	}
	var jobFailed *restorejob.FailureError
	if errors.As(err, &jobFailed) {
		return itemFailure{reason: backupv1alpha1.ItemReasonRestoreJobFailed, message: err.Error()}, true
	}
	var badSpec *restorejob.SpecError
	if errors.As(err, &badSpec) {
		return itemFailure{reason: backupv1alpha1.ItemReasonRestoreJobRefused, message: err.Error()}, true
	}
	var lost *claimLostError
	if errors.As(err, &lost) {
		return itemFailure{reason: backupv1alpha1.ItemReasonClaimLost, message: err.Error()}, true
	}
	var unidentified *identifyError
	if errors.As(err, &unidentified) {
		return itemFailure{reason: backupv1alpha1.ItemReasonNoMoverSnapshot, message: err.Error()}, true
	}
	return itemFailure{}, false
}

// failBackupItem fails a BackupRun's item with an error, when the error is
// one that fails an item (see asItemFailure).
//
// Parameters:
//   - item is the item to fail, in the run's status.
//   - err is the error its start check or its start returned. It may be nil.
//
// It returns true when it failed the item: it set the phase Failed, the
// reason and the message. For any other error, and nil, it returns false
// and leaves the item as it was.
func failBackupItem(item *backupv1alpha1.BackupItem, err error) bool {
	failure, ok := asItemFailure(err)
	if ok {
		item.Phase, item.Reason, item.Message = backupv1alpha1.ItemFailed, failure.reason, failure.message
	}
	return ok
}

// failRestoreItem fails a RestoreRun's item with an error, when the error is
// one that fails an item (see asItemFailure).
//
// Parameters:
//   - item is the item to fail, in the run's status.
//   - err is the error its check returned. It may be nil.
//
// It returns true when it failed the item: it set the phase Failed, the
// reason and the message. For any other error, and nil, it returns false
// and leaves the item as it was.
func failRestoreItem(item *backupv1alpha1.RestoreItem, err error) bool {
	failure, ok := asItemFailure(err)
	if ok {
		item.Phase, item.Reason, item.Message = backupv1alpha1.ItemFailed, failure.reason, failure.message
	}
	return ok
}

// asRunRefusal reports whether an error refuses the run as a whole, so the
// caller ends the run with it rather than retrying.
//
// Parameters:
//   - err is the error a plan, quiesce or run-level check returned. It may
//     be nil.
//
// It returns true for an *invalidSpecError, a *refusalError, an
// invalidSettingError, a *quiesce.SpecError, a *quiesce.CrossNamespaceError
// and a *quiesce.InventoryError. It returns false for any other error and
// for nil. A *refusalError met here, such as a claim repositoryFor does not
// find while planIntoNewClaim plans, records no item reason: the run's
// reason says why it ended.
func asRunRefusal(err error) bool {
	var spec *invalidSpecError
	var refused *refusalError
	var bad invalidSettingError
	var quiesceSpec *quiesce.SpecError
	var crossNamespace *quiesce.CrossNamespaceError
	var inventory *quiesce.InventoryError
	return errors.As(err, &spec) || errors.As(err, &refused) || errors.As(err, &bad) ||
		errors.As(err, &quiesceSpec) || errors.As(err, &crossNamespace) || errors.As(err, &inventory)
}

// claimLoss is how the claim of a volume item stopped being the claim the
// run checked.
type claimLoss int

const (
	// lossUnleased is a run that holds no claim Lease for the item, so it
	// can not tell which claim the restore Job wrote into.
	lossUnleased claimLoss = iota
	// lossGone is a claim that was deleted and is gone.
	lossGone
	// lossDeleting is a claim that is being deleted.
	lossDeleting
	// lossReplaced is a claim whose UID is not the UID of a claim Lease of
	// the run for the item.
	lossReplaced
	// lossNotOwned is a claim of an into item that the run does not control.
	lossNotOwned
)

// claimLostError says that the claim a volume item restored is no longer
// the claim the run checked, so the item can not succeed. asItemFailure
// fails the item with reason ClaimLost.
type claimLostError struct {
	// claim names the claim, which is also the item's name.
	claim string
	// loss is how the run lost the claim.
	loss claimLoss
	// into is true for the claim of an into item, which the run created.
	// Error then gives the same sentence for each loss.
	into bool
	// found is the UID of the claim there now, for lossReplaced.
	found string
	// leased are the claim UIDs of the run's claim Leases for the item, for
	// lossReplaced.
	leased []string
}

// Error returns the sentence for a person that says how the run lost the
// claim and what to do next.
func (e *claimLostError) Error() string {
	if e.into {
		return fmt.Sprintf("claim %s was deleted (or replaced) while its restore Job wrote into it", e.claim)
	}
	switch e.loss {
	case lossUnleased:
		return fmt.Sprintf("the run holds no claim Lease for claim %s, so it can't tell whether its restore Job wrote into the claim that is there now. "+
			"Check the claim's data, and create a new RestoreRun to restore it", e.claim)
	case lossGone:
		return fmt.Sprintf("claim %s was deleted while its restore Job wrote into it, and the restored data went with it", e.claim)
	case lossDeleting:
		return fmt.Sprintf("claim %s was deleted while its restore Job wrote into it, and the restored data goes with it once the claim is released", e.claim)
	case lossReplaced:
		return fmt.Sprintf("claim %[1]s was replaced while its restore Job wrote into it: the claim there now (UID %[2]s) is not the one the run checked "+
			"and took its Lease on (UID %[3]s). The restore Job mounts claim %[1]s by name, so it may have written into it; check its data, "+
			"and create a new RestoreRun to restore it", e.claim, e.found, strings.Join(e.leased, ", "))
	case lossNotOwned:
		// Only an into item loses its claim this way, and Error returns
		// above for an into item.
	}
	return fmt.Sprintf("claim %s is no longer the claim the run checked", e.claim)
}
