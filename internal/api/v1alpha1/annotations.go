package v1alpha1

// The annotations a namespace's objects carry to declare their backups. The
// controller reads nothing else to decide what to back up, so an object is
// backed up exactly when it says so.
const (
	// AnnotationSchedule, on a Namespace, is the five-field cron schedule the
	// namespace's backups run on.
	AnnotationSchedule = "backup.wlz.li/schedule"

	// AnnotationEnabled, set to "true" on a claim or a CloudNativePG Cluster,
	// includes it in the namespace's backups.
	AnnotationEnabled = "backup.wlz.li/enabled"

	// AnnotationRetainLast, on a claim, is how many snapshots its repository
	// keeps.
	AnnotationRetainLast = "backup.wlz.li/retain-last"

	// The age tiers restic keeps a snapshot for, on a claim: how many of the
	// newest hourly, daily, weekly, monthly and yearly snapshots survive a
	// prune, and a span such as 30d or 1y6m inside which every one does. A
	// claim sets any mix of these and retain-last, and at least one.
	AnnotationRetainHourly  = "backup.wlz.li/retain-hourly"
	AnnotationRetainDaily   = "backup.wlz.li/retain-daily"
	AnnotationRetainWeekly  = "backup.wlz.li/retain-weekly"
	AnnotationRetainMonthly = "backup.wlz.li/retain-monthly"
	AnnotationRetainYearly  = "backup.wlz.li/retain-yearly"
	AnnotationRetainWithin  = "backup.wlz.li/retain-within"

	// AnnotationQuiesce, set to "true" on a Deployment or a StatefulSet, stops
	// that workload while a namespace backup cuts its volumes' clones.
	AnnotationQuiesce = "backup.wlz.li/quiesce"

	// AnnotationRestoreAsOf, on a claim or a Cluster, is the moment every
	// automatic restore of that object goes back to, as RFC 3339.
	AnnotationRestoreAsOf = "backup.wlz.li/restore-as-of"

	// AnnotationRestoreRun, written by the bootstrap webhook on a Cluster it
	// recovered for a RestoreRun, names that run.
	AnnotationRestoreRun = "backup.wlz.li/restore-run"

	// LabelScheduledFor, on a BackupRun the scheduler created, is the schedule
	// tick it ran for, in Unix seconds.
	LabelScheduledFor = "backup.wlz.li/scheduled-for"

	// LabelManagedBy marks the ReplicationSources this controller writes.
	LabelManagedBy = "app.kubernetes.io/managed-by"

	// ManagedByValue is LabelManagedBy's value on those sources.
	ManagedByValue = "backup-controller"
)

// Enabled reports whether an object's annotations include it in backups.
func Enabled(annotations map[string]string) bool {
	return annotations[AnnotationEnabled] == "true"
}
