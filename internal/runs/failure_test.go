package runs

import (
	"errors"
	"fmt"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

// Each error that fails an item gives its reason and its own text as the
// message, also when wrapped; an error that only ends the run, a wait and a
// plain error fail no item. The run-level list takes the run refusals and
// nothing else.
func TestAnErrorFailsAnItemOnlyWhenItIsOnTheList(t *testing.T) {
	refused := refuse(backupv1alpha1.ItemReasonClaimMissing, "the claim %s no longer exists", claimN)
	for _, tc := range []struct {
		name       string
		err        error
		failure    itemFailure
		fails      bool
		runRefusal bool
	}{
		{"refusal", refused, itemFailure{backupv1alpha1.ItemReasonClaimMissing, "the claim " + claimN + " no longer exists"}, true, true},
		{"wrapped refusal", fmt.Errorf("start: %w", refused),
			itemFailure{backupv1alpha1.ItemReasonClaimMissing, "start: the claim " + claimN + " no longer exists"}, true, true},
		{"invalid setting", invalidSettingError{"namespace notes has a bad value"},
			itemFailure{backupv1alpha1.ItemReasonSettingsInvalid, "namespace notes has a bad value"}, true, true},
		{"abandoned source", &sourceHeldError{abandoned: true, message: "no run waits"},
			itemFailure{backupv1alpha1.ItemReasonSourceAbandoned, "no run waits"}, true, false},
		{"busy source", &sourceHeldError{message: "another run waits"}, itemFailure{}, false, false},
		{"invalid spec", invalidSpec("nothing is marked"), itemFailure{}, false, true},
		{"plain error", errors.New("the API server timed out"), itemFailure{}, false, false},
		{"nil", nil, itemFailure{}, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure, fails := asItemFailure(tc.err)
			if failure != tc.failure || fails != tc.fails {
				t.Errorf("asItemFailure = %+v, %t; want %+v, %t", failure, fails, tc.failure, tc.fails)
			}
			if got := asRunRefusal(tc.err); got != tc.runRefusal {
				t.Errorf("asRunRefusal = %t, want %t", got, tc.runRefusal)
			}
		})
	}
}

// A refusal marked as sparing the claim or the Cluster says so after its
// text, keeps its reason, and leaves the unmarked refusal as it was; any
// other error, and nil, passes through unchanged.
func TestASparedRefusalSaysWhatTheRunLeftAlone(t *testing.T) {
	refused := refuse(backupv1alpha1.ItemReasonClaimDeleting, "claim %s is being deleted", claimN)

	written := nothingWrittenTo(claimN, refused)
	if want := "claim " + claimN + " is being deleted. Nothing was written to claim " + claimN + ". Create a new RestoreRun to select again"; written.Error() != want {
		t.Errorf("nothingWrittenTo = %q, want %q", written.Error(), want)
	}
	if failure, _ := asItemFailure(written); failure.reason != backupv1alpha1.ItemReasonClaimDeleting {
		t.Errorf("reason = %q, want ClaimDeleting", failure.reason)
	}
	if deleted := nothingDeleted(refused); deleted.Error() != "claim "+claimN+" is being deleted. The run deleted nothing" {
		t.Errorf("nothingDeleted = %q", deleted.Error())
	}
	if refused.Error() != "claim "+claimN+" is being deleted" {
		t.Errorf("the unmarked refusal became %q", refused.Error())
	}

	plain := errors.New("the API server timed out")
	if got := nothingWrittenTo(claimN, plain); !errors.Is(got, plain) {
		t.Errorf("nothingWrittenTo changed a plain error to %v", got)
	}
	if nothingWrittenTo(claimN, nil) != nil || nothingDeleted(nil) != nil {
		t.Error("a nil error came back as an error")
	}
}
