package runs

import (
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// holdKind is the kind of thing that makes an item or a run wait. The zero
// value holdNone means that nothing makes it wait.
type holdKind int

const (
	// holdNone is the zero value: nothing holds the item or the run.
	holdNone holdKind = iota
	// holdSourceBusy is a wait for another run: another run holds a Lease
	// that the run needs, or a mover of another run uses the claim or the
	// repository.
	holdSourceBusy
	// holdClaimInUse is a wait for a pod: a pod mounts the claim that an
	// in-place restore writes into.
	holdClaimInUse
)

// hold is the reason that an item or a run waits for something outside it.
// The zero value means that nothing holds it. A decision reads kind through
// held, and never reads text.
type hold struct {
	// kind is what holds the item or the run. held decides on kind alone.
	kind holdKind
	// text says what holds the item or the run and what occurs next. The
	// run writes it to the Ready message.
	text string
}

// sourceBusy returns a hold of kind holdSourceBusy.
//
// Parameters:
//   - format and args make the text of the hold, as fmt.Sprintf makes them.
//     The text names the other run or the object that holds the claim or the
//     repository.
//
// It returns the hold.
func sourceBusy(format string, args ...any) hold {
	return hold{kind: holdSourceBusy, text: fmt.Sprintf(format, args...)}
}

// claimInUse returns a hold of kind holdClaimInUse.
//
// Parameters:
//   - format and args make the text of the hold, as fmt.Sprintf makes them.
//     The text names the claim and the pod that mounts it.
//
// It returns the hold.
func claimInUse(format string, args ...any) hold {
	return hold{kind: holdClaimInUse, text: fmt.Sprintf(format, args...)}
}

// held reports whether something holds the item or the run.
func (h hold) held() bool { return h.kind != holdNone }

// readyReason returns the reason for the Ready condition of a run that waits
// with this hold.
//
// It returns backupv1alpha1.ReasonSourceBusy for holdSourceBusy and
// backupv1alpha1.ReasonClaimInUse for holdClaimInUse. It returns an empty
// string for holdNone, because a run without a hold does not wait.
func (h hold) readyReason() string {
	switch h.kind {
	case holdSourceBusy:
		return backupv1alpha1.ReasonSourceBusy
	case holdClaimInUse:
		return backupv1alpha1.ReasonClaimInUse
	case holdNone:
		return ""
	}
	return ""
}
