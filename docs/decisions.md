# Decisions

Each entry states a decision, what goes wrong with the alternative, and what
the controller does.

## Restore the selected snapshot by ID with the controller's own Job

The controller restores a volume with its own restic Job, which restores the
snapshot ID the run selected. A VolSync ReplicationDestination selects the
snapshot again on its own, by time: when a backup writes a snapshot between the
run's check and the restore, VolSync restores that newer snapshot, and the run
reports the one it checked. The Job restores exactly the recorded ID, and its
Kubernetes condition, Complete or Failed, is its result.

## Find a backup's snapshot in the repository, never in a log

The controller finds the snapshot a sync wrote by listing the repository and
matching the sync's start and end, which VolSync records in the
ReplicationSource's status. A mover's log is text for people: VolSync may
shorten it, a restic release may reword it, and a controller that parses it
reports success with no snapshot when the line it looks for is missing. The
repository listing and the status fields are the data restic and VolSync
guarantee.

## Recover a database to a base backup or to the end of its archive

The webhook gives Postgres only two kinds of target: the end of a named base
backup (backupID with targetImmediate), or no target, which replays the whole
archive. A recovery to a clock time needs a committed transaction after that
time in the archive. A database that was idle after the time has none,
Postgres stops with "recovery ended before configured recovery target was
reached", and CloudNativePG retries the recovery forever while the app has no
database. A restore to a moment therefore lands on the newest base backup at or
before the moment, which is never ahead of it.

## Keep the app paused until the database backups completed

A BackupRun with all: true resumes the app once every volume's clone is cut and
every database Backup completed. When the app resumes as soon as the clones are
cut, the database Backup finishes after the app wrote again, and no base
backup holds the database as it was at the volume's moment. A synced restore
then has to fall back to an older base backup, or to a clock time. With the
pause held, the base backup that completes during the pause holds exactly the
paused moment, and the snapshots name it in a base-backup tag.

## Admit every run before it does any work

A run creates its Kueue Workload first and reads no repository, pauses no app
and starts no mover before Kueue admits it. When a schedule tick creates 70
BackupRuns at once and each run works before admission, the controller opens
70 restic repositories at once, each with a scrypt key derivation of up to
60 MiB, and exceeds its 512 MiB memory limit. With admission first, the
ClusterQueue quota of five backup-controller.wlz.li/run bounds the work,
the memory and the load on the cluster.

## Let runs take turns through Leases

A run holds one coordination.k8s.io Lease for each claim, repository, Cluster
and set of paused workloads it works on, and a second run waits with reason
Busy. Two movers on one claim corrupt it or fight over restic's lock, and two
restores of one Cluster delete each other's recovery. Refusing the second run
would make a scheduled backup fail whenever a restore runs; waiting lets it run
right after. The API server decides every race for a Lease, and a Lease whose
holder is gone or finished goes to the next run, so no crash blocks an object
for good.

## Record a step in the status before acting on it

A run writes what it is about to do into its status before it pauses a
workload, starts a Job or deletes a Cluster, and reads its status from the API
server at the start of each pass. When the controller acts first, a crash
between the act and the status write loses the record: the next pass finds a
workload at 0 replicas and records 0 as the count to restore. With the record
first, the next pass repeats the recorded step, and every step is safe to
repeat.

## Use the run's timeout as the only limit on a pause

The app stays paused until the backup no longer needs it, and at most until
the run's timeout, counted from admission. A separate limit on the pause that
starts the app again and fails the volumes not yet cut turns a hang into a
quiet partial backup. With the timeout alone, a hang ends the run Failed with
reason TimedOut, the app is resumed, and the namespace's last-success metric
stops moving, which fires the backup alert.

## Test against the real stack

Every feature has a scenario on a kind cluster that runs the real VolSync,
CloudNativePG with Postgres and the barman-cloud plugin, Kueue, Flux and an S3
service, and checks the data: the file in the claim, the rows in the database.
Tests against fakes of those components pass on the assumptions the fakes
encode; v0.10 passed such tests while every restore of an idle database looped
on prod. Unit tests cover only pure logic, such as the schedule arithmetic and
the snapshot selection.
