// Package reasonrule is the fixture of TestTheReasonRuleCatchesEveryShape.
// Each line with a trailing want comment gives an item a reason that is not
// a constant of the API package; no other line may be reported. Only the
// rule test loads it.
package reasonrule

import (
	"fmt"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
)

type item struct {
	Reason  backupv1alpha1.ItemReason
	Message string
}

type failure struct {
	reason  backupv1alpha1.ItemReason
	message string
}

func refuse(reason backupv1alpha1.ItemReason, format string, args ...any) error {
	return fmt.Errorf("%s: "+format, append([]any{reason}, args...)...)
}

const claimN = "notes-data"

func calls(forwarded backupv1alpha1.ItemReason) []error {
	return []error{
		refuse(backupv1alpha1.ItemReasonClaimMissing, "the claim %s no longer exists", claimN),
		refuse("the claim %s no longer exists", claimN),      // want reason
		refuse(backupv1alpha1.ItemReason("Made"), "made up"), // want reason
		refuse(forwarded, "forwarded"),                       // want reason
	}
}

func assignments(it *item) {
	it.Reason = backupv1alpha1.ItemReasonClaimMissing
	it.Reason = "ClaimGone"                             // want reason
	it.Message, it.Reason = "gone", "ClaimGone"         // want reason
	it.Reason = backupv1alpha1.ItemKindCluster          // want reason
	var declared backupv1alpha1.ItemReason = "Declared" // want reason
	defined := backupv1alpha1.ItemReason("Defined")     // want reason
	it.Reason = declared                                // want reason
	it.Reason += defined                                // want reason
}

func literals() []any {
	return []any{
		item{Reason: backupv1alpha1.ItemReasonClaimMissing, Message: "gone"},
		item{Reason: "ClaimGone", Message: "gone"}, // want reason
		failure{"ClaimGone", "gone"},               // want reason
		&failure{reason: backupv1alpha1.ItemReasonClaimMissing},
	}
}

func allowed(it *item, f failure) {
	it.Reason = f.reason
	it.Reason = "Listed" // want reason
}
