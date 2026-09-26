package runs

import (
	"errors"
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
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
	// spared is what the run left as it was when it refused. Error states
	// it after text, so a person knows nothing needs repair.
	spared spared
}

// sparedKind says which object a refusal left untouched, if any.
type sparedKind int

const (
	// sparedNothing is a refusal that says nothing about what it left
	// untouched. Error adds nothing to the text.
	sparedNothing sparedKind = iota
	// sparedClaim is a refusal of a volume restore before anything was
	// written to its claim.
	sparedClaim
	// sparedCluster is a refusal of a database restore before the run
	// deleted anything of the Cluster.
	sparedCluster
)

// spared names what a refusal left untouched.
type spared struct {
	// kind is sparedNothing, sparedClaim or sparedCluster.
	kind sparedKind
	// claim names the claim for sparedClaim.
	claim string
}

// Error returns the refusal's text, followed by the sentence that says what
// the run left untouched when spared names something.
func (e *refusalError) Error() string {
	switch e.spared.kind {
	case sparedClaim:
		return e.text + nothingWritten(e.spared.claim)
	case sparedCluster:
		return e.text + ". The run deleted nothing"
	case sparedNothing:
	}
	return e.text
}

// refuse builds a refusal of an item.
//
// Parameters:
//   - reason is the ItemReason the item records when it fails with the
//     refusal.
//   - format and args build the refusal's text for a person, the way
//     fmt.Sprintf builds it.
//
// It returns a *refusalError that spares nothing; nothingWrittenTo and
// nothingDeleted add what the run left untouched.
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
// It returns a copy of a *refusalError in err with the claim spared, and any
// other error, nil included, unchanged.
func nothingWrittenTo(claim string, err error) error {
	return withSpared(err, spared{kind: sparedClaim, claim: claim})
}

// nothingDeleted marks a refusal of a database restore as one that deleted
// nothing of the Cluster, so its message says so.
//
// Parameters:
//   - err is the error the check returned. It may be nil.
//
// It returns a copy of a *refusalError in err with the Cluster spared, and
// any other error, nil included, unchanged.
func nothingDeleted(err error) error {
	return withSpared(err, spared{kind: sparedCluster})
}

// withSpared returns a copy of the *refusalError in err with what it spared
// set, and any other error unchanged. It is the shared part of
// nothingWrittenTo and nothingDeleted.
func withSpared(err error, what spared) error {
	var refused *refusalError
	if !errors.As(err, &refused) {
		return err
	}
	marked := *refused
	marked.spared = what
	return &marked
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

// invalidSpec builds a refusal of the run as a whole, with a text built from
// format and args the way fmt.Sprintf builds it. Only a function that only
// run-level sites call returns one.
func invalidSpec(format string, args ...any) error {
	return &invalidSpecError{text: fmt.Sprintf(format, args...)}
}

// itemFailure is how an item ends when an error fails it: the reason it
// records and the message a person reads.
type itemFailure struct {
	reason  backupv1alpha1.ItemReason
	message string
}

// asItemFailure tells whether an error fails an item, and how.
//
// Parameters:
//   - err is the error a start check or a start returned. It may be nil.
//
// It returns the failure and true for an error on this list, checked in
// this order with errors.As: a *refusalError (its own reason), an
// invalidSettingError (SettingsInvalid), and a *sourceHeldError for a source
// no run waits for (SourceAbandoned). The message is err.Error(). Any other
// error, and nil, gives false: the pass returns the error and retries. An
// *invalidSpecError is not on the list, because only run-level sites meet
// one, and an item that recorded it would carry a reason no item has.
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
	if errors.As(err, &held) && held.abandoned {
		return itemFailure{reason: backupv1alpha1.ItemReasonSourceAbandoned, message: err.Error()}, true
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
// It returns true for an *invalidSpecError, a *refusalError and an
// invalidSettingError, and false for any other error and for nil. A
// *refusalError met here, such as a claim repositoryFor does not find while
// planIntoNewClaim plans, records no item reason: the run's reason says why
// it ended.
func asRunRefusal(err error) bool {
	var spec *invalidSpecError
	var refused *refusalError
	var bad invalidSettingError
	return errors.As(err, &spec) || errors.As(err, &refused) || errors.As(err, &bad)
}
