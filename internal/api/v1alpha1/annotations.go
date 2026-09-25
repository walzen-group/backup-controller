package v1alpha1

// The annotations and labels a namespace's objects carry to declare their
// backups. The controller decides what to back up from these keys alone, so an
// object is backed up exactly when it carries them.
const (
	// AnnotationSchedule, on a Namespace, holds the cron schedule the
	// namespace's backups run on. It takes the five standard fields and may
	// start with a CRON_TZ= prefix.
	AnnotationSchedule = "backup.wlz.li/schedule"

	// AnnotationTimeout, on a Namespace, is how long a BackupRun there may
	// work once admitted, as a Go duration such as 10h. A BackupRun's own
	// spec.timeout takes precedence over it. Without either, a run gets six
	// hours. RestoreRuns ignore this annotation.
	AnnotationTimeout = "backup.wlz.li/timeout"

	// AnnotationPruneIntervalDays, on a Namespace, is the number of days
	// between prunes of each repository the namespace's ReplicationSources
	// write. Without it, each repository is pruned every day.
	AnnotationPruneIntervalDays = "backup.wlz.li/prune-interval-days"

	// AnnotationEnabled, set to "true" on a claim or a CloudNativePG Cluster,
	// includes that object in the namespace's backups.
	AnnotationEnabled = "backup.wlz.li/enabled"

	// AnnotationRetainLast, on a claim, is the number of newest snapshots its
	// repository keeps.
	AnnotationRetainLast = "backup.wlz.li/retain-last"

	// The retention tiers restic applies to a claim's snapshots. The hourly,
	// daily, weekly, monthly and yearly keys each give a count of the newest
	// snapshots in that tier that survive a prune. The within key gives a span
	// such as 30d or 1y6m, and every snapshot inside that span survives. A
	// claim sets any mix of these and retain-last, and it has to set at least
	// one of them.
	AnnotationRetainHourly  = "backup.wlz.li/retain-hourly"
	AnnotationRetainDaily   = "backup.wlz.li/retain-daily"
	AnnotationRetainWeekly  = "backup.wlz.li/retain-weekly"
	AnnotationRetainMonthly = "backup.wlz.li/retain-monthly"
	AnnotationRetainYearly  = "backup.wlz.li/retain-yearly"
	AnnotationRetainWithin  = "backup.wlz.li/retain-within"

	// AnnotationQuiesce, set to "true" on a Deployment or a StatefulSet, makes
	// a BackupRun with all set scale that workload to zero while it cuts the
	// clones of the namespace's volumes.
	AnnotationQuiesce = "backup.wlz.li/quiesce"

	// AnnotationRestoreAsOf, on a claim or a Cluster, is an RFC 3339 time.
	// Every automatic restore of that object goes back to this moment.
	AnnotationRestoreAsOf = "backup.wlz.li/restore-as-of"

	// AnnotationRestoreRun holds the name of the RestoreRun a Cluster was
	// recovered for. The bootstrap webhook writes it onto the Cluster when it
	// sets up the recovery.
	AnnotationRestoreRun = "backup.wlz.li/restore-run"

	// LabelScheduledFor, on a BackupRun the scheduler created, is the schedule
	// tick the run was created for, in Unix seconds.
	LabelScheduledFor = "backup.wlz.li/scheduled-for"

	// LabelManagedBy marks the ReplicationSources and the CloudNativePG
	// Backups this controller creates.
	LabelManagedBy = "app.kubernetes.io/managed-by"

	// ManagedByValue is the value LabelManagedBy carries on those objects.
	ManagedByValue = "backup-controller"
)

// Enabled reports whether an object's annotations set backup.wlz.li/enabled to
// "true", which includes the object in its namespace's backups.
func Enabled(annotations map[string]string) bool {
	return annotations[AnnotationEnabled] == "true"
}
