package runs

import (
	"errors"
	"testing"
)

// A source write that a hold stopped gives the hold back. A *heldError with
// the zero hold is an error: the zero hold does not hold the run, and if
// ensureSource took it as a hold, the item would go Running with a tag that
// the write did not put on the source.
func TestAnEmptyHoldStopsTheItem(t *testing.T) {
	t.Parallel()
	busy := sourceBusy("ReplicationSource %s is busy", claimN)
	for name, tc := range map[string]struct {
		err   error
		hold  hold
		found bool
		bad   bool
	}{
		"no hold":       {err: errors.New("conflict")},
		"a busy source": {err: &heldError{hold: busy}, hold: busy, found: true},
		"the zero hold": {err: &heldError{}, found: true, bad: true},
	} {
		t.Run(name, func(t *testing.T) {
			h, found, bad := heldOf(tc.err)
			if h != tc.hold || found != tc.found || (bad != nil) != tc.bad {
				t.Errorf("heldOf = %+v, %v, %v; want %+v, %v, error %v", h, found, bad, tc.hold, tc.found, tc.bad)
			}
		})
	}
}
