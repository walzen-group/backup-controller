# API reference

The controller serves three kinds in the group backup.wlz.li, version
v1alpha1, and reads a set of annotations. Each entry here says what a field
does; the flows that use them are in docs/backups.md, docs/restores.md and
docs/automatic-restore.md.

## Annotations

| Annotation | On | Value | Effect |
| --- | --- | --- | --- |
| backup.wlz.li/enabled | claim, Cluster | "true" | the object is backed up and restored with its namespace |
| backup.wlz.li/retain-last, -hourly, -daily, -weekly, -monthly, -yearly | claim | a positive count | restic's retention for the claim's repository; at least one retain annotation is required |
| backup.wlz.li/retain-within | claim | a span such as 30d or 1y6m | restic's --keep-within |
| backup.wlz.li/restore-as-of | claim | an RFC 3339 time | the populator fills the claim from the newest snapshot at or before this time |
| backup.wlz.li/pause-during-backup | Deployment, StatefulSet | "true" | a BackupRun with all: true pauses the workload |
| backup.wlz.li/schedule | Namespace | a cron schedule, optionally with CRON_TZ= | the scheduler creates a BackupRun with all: true at each tick |
| backup.wlz.li/timeout | Namespace | a Go duration such as 2h | the timeout of the namespace's BackupRuns without spec.timeout |
| backup.wlz.li/prune-interval-days | Namespace | a positive count | days between restic prunes of the namespace's repositories; 1 when unset |
| backup.wlz.li/bootstrap | Cluster | initdb | the webhook leaves the Cluster empty, when nothing is archived under its prefix |

The controller writes these on objects it creates:

| Key | On | Holds |
| --- | --- | --- |
| backup.wlz.li/scheduled-for (label) | BackupRun | the schedule tick in Unix seconds |
| backup.wlz.li/restore-run (annotation) | Cluster | the RestoreRun whose recovery the webhook wrote |
| backup.wlz.li/restore (label) | restore Job | the RestoreRun, or the namespace of the claim the populator fills |
| app.kubernetes.io/managed-by: backup-controller (label) | ReplicationSource | marks a source the controller writes; it never writes over another |

## BackupRun

| Spec field | Effect |
| --- | --- |
| source | back up one claim |
| database | back up one Cluster |
| all | back up every enabled claim and Cluster, and pause the marked workloads |
| timeout | how long the run may work after admission; else the namespace annotation, else 1 hour |
| ttlSecondsAfterFinished | delete the run that many seconds after it ends |

Set exactly one of source, database and all.

| Status field | Holds |
| --- | --- |
| phase | Queued, Running, Succeeded or Failed |
| workload | the Kueue Workload of the run |
| startedAt, completedAt | admission and end |
| pausedAt, resumedAt | when the run paused and resumed the app |
| paused | each paused workload with its replica count, and resumed once scaled back |
| suspendedKustomizations | the Flux Kustomizations the run suspended, as namespace/name |
| leases | the Leases the run holds or held |
| items | one entry per claim (kind ReplicationSource) and per Cluster (kind Cluster) |
| conditions | Ready, with the reason and the message |

| Item field | Holds |
| --- | --- |
| phase | Pending, Running, Succeeded, Failed or Skipped |
| message | why the item failed, was skipped, or waits |
| trigger | the manual trigger written into the ReplicationSource |
| snapshot, snapshotID, snapshotTime | the snapshot the sync wrote, after the move to resumedAt on a paused run |
| empty | the volume held no files, so there is no snapshot |
| backup | the CloudNativePG Backup the run created |
| baseBackup | barman's ID of that Backup's base backup |
| baseBackupWhilePaused | the base backup completed while the app was paused |

## RestoreRun

| Spec field | Effect |
| --- | --- |
| claim | the claim to restore in place, or the source claim of an into restore |
| repository | the Secret of a repository to restore from; needs into |
| into | a new claim to create and fill |
| intoSize | the size of that claim; else the source claim's size |
| database | the Cluster to restore |
| all | every enabled claim and Cluster |
| restoreAsOf | an RFC 3339 time to go back to |
| previous | snapshots to step back, for a single claim |
| syncDatabaseToVolume | volumes and databases to one paused moment; needs all |
| pauseDuringRestore | Deployments and StatefulSets to pause, each as kind and name |
| timeout | how long the run may work after admission; 4 hours when omitted |
| moverSecurityContext | the restore Jobs' pod security context |
| ttlSecondsAfterFinished | delete the run that many seconds after it ends |

| Status field | Holds |
| --- | --- |
| phase | Queued, Running, Succeeded or Failed |
| workload, startedAt, selectedAt, completedAt | the Workload, admission, the selection of the backups, and the end |
| syncedTo | the paused moment of a synced restore |
| pausedAt, resumedAt, paused, suspendedKustomizations | as on a BackupRun |
| leases | the Leases the run holds or held |
| items | one entry per claim (kind PersistentVolumeClaim) and per Cluster (kind Cluster) |
| conditions | Ready, with the reason and the message |

| Item field | Holds |
| --- | --- |
| phase | Pending, Running, Deleted, Recovering, Succeeded, Failed or Skipped |
| message | why the item failed, was skipped, or waits |
| job | the restore Job of a volume |
| snapshot, snapshotID, snapshotTime | the snapshot the volume restores |
| createdClaim | the run created the claim of an into restore |
| baseBackup | the base backup the database recovers from |
| stopAtBaseBackup | the recovery stops at the end of baseBackup; else it replays the whole archive |
| clusterUID | the UID of the Cluster the run deleted |

## VolumeRestore

| Spec field | Effect |
| --- | --- |
| repository | the Secret with RESTIC_REPOSITORY, RESTIC_PASSWORD and the S3 keys, in this namespace |
| restoreAsOf | the populator fills new claims from the newest snapshot at or before this time |
| cacheStorageClassName, cacheCapacity | the class and size of the backup mover's cache claim and clone |
| moverPodLabels | labels on the populator's restore Jobs; Kueue's queue label is left off, since the controller admits the restore itself |
| moverSecurityContext | the pod security context of the backup mover and the restore Jobs |

| Status field | Holds |
| --- | --- |
| conditions | Ready: True with reason Restored, or False with Queued, Restoring, RestoreFailed, NoBackupInReach or ControllerPaused |
| claims | per claim: name, UID, phase (Restoring, Restored, Failed), startedAt, and the snapshot it is filled from |

## Ready reasons

| Reason | Run | Meaning |
| --- | --- | --- |
| Queued | both | waiting for Kueue to admit the run, or for the namespace's LocalQueue when the message says it has none |
| ControllerPaused | both | the controller runs with --pause; the run has not started |
| Running | both | working, or waiting for paused pods to stop |
| Busy | both | another run holds a Lease this run needs |
| SourceBusy | BackupRun | a ReplicationSource is still busy with an earlier trigger |
| ClaimInUse | RestoreRun | a pod mounts a claim the run restores in place |
| WaitingForShutdown | RestoreRun | the deleted Cluster's pods or claims are still there |
| WaitingForRecreate | RestoreRun | waiting for the Cluster's owner to create it again |
| Succeeded | both | every item succeeded or was skipped |
| Failed | both | an item failed |
| Invalid | both | the spec can't work; nothing was changed |
| NoBackupInReach | RestoreRun | an item has no backup the run's moment reaches; nothing was changed |
| TimedOut | both | the timeout passed |
| Evicted | both | Kueue evicted the run's Workload |
