package runs

import (
	"fmt"
	"regexp"
	"strconv"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
)

// resticSpan matches a span in the form restic's --keep-within takes: one or
// more counts of years, months, days or hours, such as 30d or 1y6m.
var resticSpan = regexp.MustCompile(`^([0-9]+[ymdh])+$`)

// retention builds the restic retention policy for a claim's
// ReplicationSource from the claim's annotations: backup.wlz.li/retain-last,
// retain-hourly, retain-daily, retain-weekly, retain-monthly, retain-yearly
// and retain-within.
//
// It returns a refusal naming the claim and the annotation when a count isn't
// a positive integer, or when retain-within isn't a span such as 30d. It also
// returns a refusal when the claim sets none of them, because a policy with
// no rule would make restic keep every snapshot.
func retention(claim *corev1.PersistentVolumeClaim) (*volsyncv1alpha1.ResticRetainPolicy, error) {
	policy := &volsyncv1alpha1.ResticRetainPolicy{}
	counts := []struct {
		annotation string
		field      **int32
	}{
		{backupv1alpha1.AnnotationRetainHourly, &policy.Hourly},
		{backupv1alpha1.AnnotationRetainDaily, &policy.Daily},
		{backupv1alpha1.AnnotationRetainWeekly, &policy.Weekly},
		{backupv1alpha1.AnnotationRetainMonthly, &policy.Monthly},
		{backupv1alpha1.AnnotationRetainYearly, &policy.Yearly},
	}
	set := false

	// VolSync's CRD types the last count as a string and the other counts as
	// integers, so retain-last is parsed only to check it and is stored as
	// the annotation wrote it.
	if value, ok := claim.Annotations[backupv1alpha1.AnnotationRetainLast]; ok {
		if _, err := positiveCount(value); err != nil {
			return nil, refuse(backupv1alpha1.ItemReasonSettingsInvalid, "claim %s has %s %q, which is not a positive count", claim.Name, backupv1alpha1.AnnotationRetainLast, value)
		}
		policy.Last = &value
		set = true
	}
	for _, c := range counts {
		value, ok := claim.Annotations[c.annotation]
		if !ok {
			continue
		}
		n, err := positiveCount(value)
		if err != nil {
			return nil, refuse(backupv1alpha1.ItemReasonSettingsInvalid, "claim %s has %s %q, which is not a positive count", claim.Name, c.annotation, value)
		}
		*c.field = &n
		set = true
	}
	if value, ok := claim.Annotations[backupv1alpha1.AnnotationRetainWithin]; ok {
		if !resticSpan.MatchString(value) {
			return nil, refuse(backupv1alpha1.ItemReasonSettingsInvalid, "claim %s has %s %q, which is not a span such as 30d or 1y6m", claim.Name, backupv1alpha1.AnnotationRetainWithin, value)
		}
		policy.Within = &value
		set = true
	}

	if !set {
		return nil, refuse(backupv1alpha1.ItemReasonSettingsInvalid, "claim %s names no retention; set at least one of %s, %s, %s, %s, %s, %s or %s",
			claim.Name, backupv1alpha1.AnnotationRetainLast, backupv1alpha1.AnnotationRetainHourly,
			backupv1alpha1.AnnotationRetainDaily, backupv1alpha1.AnnotationRetainWeekly,
			backupv1alpha1.AnnotationRetainMonthly, backupv1alpha1.AnnotationRetainYearly,
			backupv1alpha1.AnnotationRetainWithin)
	}
	return policy, nil
}

// positiveCount parses a retention count from an annotation value. It returns
// an error unless the value is an integer of at least 1.
func positiveCount(value string) (int32, error) {
	n, err := strconv.ParseInt(value, 10, 32)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("not a positive count")
	}
	return int32(n), nil
}
