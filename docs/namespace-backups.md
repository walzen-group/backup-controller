# Namespace backups

From v0.5.0 the controller runs a namespace's backups itself. A namespace names
one schedule, each claim and CloudNativePG Cluster in it opts in, and one
BackupRun or RestoreRun covers one volume, one database, or all of them. VolSync
still moves every byte of a volume and the barman-cloud plugin every byte of a
database; the controller decides when, and in which order.

Every output on this page was produced on the walzen prod cluster on 2026-09-24,
by the canary the infrastructure repository keeps at
modules/testing/canary-namespace-backup. Its writer appends one timestamp a
minute to a file on its volume and inserts the same timestamp into its
database, and logs the last three entries of each.

## Declare what to back up

```yaml
apiVersion: v1
kind: Namespace
metadata:
  name: notes
  annotations:
    backup.wlz.li/schedule: "0 5 * * *"
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: notes-data
  annotations:
    backup.wlz.li/enabled: "true"
    backup.wlz.li/retain-last: "10"
---
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: notes-pg
  annotations:
    backup.wlz.li/enabled: "true"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: notes
  annotations:
    backup.wlz.li/quiesce: "true"
```

| Annotation | On | Decides |
| --- | --- | --- |
| backup.wlz.li/schedule | Namespace | when the namespace's backups run, five-field cron in UTC, or on a zone's clock with a prefix such as `CRON_TZ=Europe/Berlin 0 4 * * *` |
| backup.wlz.li/timeout | Namespace | how long a run there may work once admitted, a Go duration such as `10h`; 6h without it, and a run's own `spec.timeout` wins |
| backup.wlz.li/prune-interval-days | Namespace | days between prunes of each repository the namespace's sources write; 1 without it |
| backup.wlz.li/enabled | claim, Cluster | whether a run includes it; nothing else is read to find what to back up |
| backup.wlz.li/retain-last and the six other retain- annotations | claim | which snapshots the volume's repository keeps; see [Retention](#retention) |
| backup.wlz.li/quiesce | Deployment, StatefulSet | whether the workload stops while the volumes' clones are cut |
| backup.wlz.li/restore-as-of | claim, Cluster | the moment every automatic restore of it goes back to |

A claim still names its VolumeRestore in `dataSourceRef`; that object supplies
the repository Secret, the cache class and the mover security context. A
database's retention stays a window on its ObjectStore, because the barman-cloud
plugin accepts `retentionPolicy: "30d"` and no count.

### Retention

A claim sets any mix of seven annotations, and at least one. The controller
copies each onto the matching field of the retain block in the claim's
ReplicationSource, and VolSync's mover passes that block to `restic forget`:

| Annotation | retain field | restic flag | Value |
| --- | --- | --- | --- |
| backup.wlz.li/retain-last | last | `--keep-last` | a count |
| backup.wlz.li/retain-hourly | hourly | `--keep-hourly` | a count |
| backup.wlz.li/retain-daily | daily | `--keep-daily` | a count |
| backup.wlz.li/retain-weekly | weekly | `--keep-weekly` | a count |
| backup.wlz.li/retain-monthly | monthly | `--keep-monthly` | a count |
| backup.wlz.li/retain-yearly | yearly | `--keep-yearly` | a count |
| backup.wlz.li/retain-within | within | `--keep-within` | a span of y, m, d and h, such as 30d or 1y6m |

restic keeps a snapshot when any one of the flags keeps it. This claim holds
the seven newest daily snapshots, the newest of each of the last four weeks,
and the newest of each of the last six months:

```yaml
metadata:
  annotations:
    backup.wlz.li/enabled: "true"
    backup.wlz.li/retain-daily: "7"
    backup.wlz.li/retain-weekly: "4"
    backup.wlz.li/retain-monthly: "6"
```

A claim carrying none of the seven fails its item before the run writes a
source, and the message lists all seven. A count below 1, or a span restic
cannot read, fails the item with a message naming that annotation.

## Scheduled runs

At each tick of a Namespace's schedule the controller creates a BackupRun named
`scheduled-<yyyymmdd-hhmm>` with `all: true`, labelled with the tick and kept for
30 days. A tick missed while the controller was down runs once when it comes
back. While another run with `all: true` in the namespace is unfinished, the
tick waits for it.

A due tick also waits while no claim or Cluster in the namespace carries
`backup.wlz.li/enabled: "true"`, because a run created then fails with Invalid.
Flux writes a Namespace before the claims in it, so adding a schedule to an
existing namespace can make a tick due a second before its claims are marked.
Until one is, the controller records a Warning with reason NothingEnabled on the
Namespace, noted `the tick at 2026-09-23T05:00:00Z is due, but nothing in this
namespace is marked backup.wlz.li/enabled: "true"`, and creates no run. The
controller checks again as soon as a claim changes, and at least every five
minutes, which is how it finds a marked Cluster. `kubectl describe namespace
<name>` lists the Warning under Events.

A scheduled run carries no timeout of its own, so it gives up at the same point
a manual run without one does: `backup.wlz.li/timeout` on the Namespace, or six
hours after Kueue admits it. The clock starts at admission, so the time a run
spends in Queued does not count. On the deadline the run marks its unfinished
items Failed, restarts what it quiesced and releases its Workload; a VolSync
mover or a CloudNativePG Backup still in progress keeps running.

The canary's first scheduled run, on a `*/15` schedule, sampled every three
seconds; lines repeating the one before are left out:

```text
22:15:03 phase=Queued replicas=1/1 clone=
22:15:12 phase=Waiting replicas=0/ clone=
22:15:41 phase=Waiting replicas=0/ clone=Pending
22:15:46 phase=Running replicas=1/1 clone=Bound
22:16:00 phase=Succeeded replicas=1/1 clone=
```

Its status afterwards, trimmed to the fields this page discusses:

```yaml
items:
  - kind: ReplicationSource
    name: canary-namespace-backup-data
    phase: Succeeded
    snapshot: d1cb7739
    snapshotTime: "2026-09-24T22:15:48Z"
  - kind: Cluster
    name: canary-namespace-backup-pg
    phase: Succeeded
    backup: canary-namespace-backup-pg-646e30f9
quiesced:
  - kind: Deployment
    name: canary-namespace-backup
    replicas: 1
quiescedAt: "2026-09-24T22:15:10Z"
restartedAt: "2026-09-24T22:15:45Z"
```

## What a run with all set does

1. It creates one Kueue Workload, counted as one pod, in the namespace's
   LocalQueue, and waits for Admitted (phase Queued). The app keeps running
   while the run waits. It sets the Workload's PodsReady condition itself,
   because the Workload has no pods and Kueue's waitForPodsReady would evict it.
   A namespace with no LocalQueue starts at once.
2. It records whether the Flux Kustomization named in a quiesced workload's
   `kustomize.toolkit.fluxcd.io/name` label is suspended, suspends it if not,
   and scales every workload marked `backup.wlz.li/quiesce` to zero. It then
   waits until none of their pods is left, terminating ones included: a pod
   shutting down can still write.
3. It writes each enabled claim's ReplicationSource with the run's manual tag,
   and creates a CloudNativePG Backup for each enabled Cluster. A Cluster
   carrying `cnpg.io/hibernation: "on"` is skipped.
4. Once every clone claim `volsync-<claim>-src` is Bound, it gives each workload
   its replicas back and resumes only the Kustomizations it suspended. The
   upload continues from the clone.
5. It reads the snapshot each mover logged, `snapshot d1cb7739 saved`, and the
   time restic stamped on it from the repository itself, then deletes the
   Workload.

A finalizer performs step 4 and deletes the Workload on failure, on timeout and
when the run is deleted. The canary's writer was down 34 seconds, most of it the
pod's 30-second termination grace, because its shell loop does not handle
SIGTERM.

### Sources the controller writes

A source is owned by its claim and named after it. The mover is placed by the
PersistentVolume's own node affinity, so it runs whether or not the app's pod
does, and it carries no queue label, since the run's Workload already admitted
it. The canary's source after the 22:45 scheduled run, trimmed to spec and the
status fields that show it idle:

```yaml
metadata:
  labels:
    app.kubernetes.io/managed-by: backup-controller
  ownerReferences:
  - kind: PersistentVolumeClaim
    name: canary-namespace-backup-data
spec:
  restic:
    cacheStorageClassName: zfs-ephemeral
    copyMethod: Clone
    moverAffinity:
      nodeAffinity:
        requiredDuringSchedulingIgnoredDuringExecution:
          nodeSelectorTerms:
          - matchExpressions:
            - key: openebs.io/nodeid
              operator: In
              values:
              - talos-unraid-quasar-w-1
    moverResources:
      requests:
        cpu: 500m
    pruneIntervalDays: 1
    repository: canary-namespace-backup-data-restic
    retain:
      last: "10"
    storageClassName: zfs-ephemeral
  sourcePVC: canary-namespace-backup-data
  trigger:
    manual: backuprun-a9158795-edb7-4e4e-826e-ef6bbf50a8f8
status:
  conditions:
  - message: Waiting for manual trigger
    reason: WaitingForManual
    type: Synchronizing
  lastManualSync: backuprun-a9158795-edb7-4e4e-826e-ef6bbf50a8f8
```

The tag stays on the source after the backup. VolSync syncs a source with no
trigger at all in a tight loop, and one whose tag equals `lastManualSync` not at
all, so a spent tag is how a source waits for the next run, as the status above
shows. A run that finds a
source still completing another run's tag waits for it (reason SourceBusy). A
source of the same name the controller did not write is left alone, and the
item fails naming it.

## Back up now

| Run | Backs up | Measured |
| --- | --- | --- |
| `source: <claim>` | one volume | snapshot defb7a4d, stamped 22:09:58, in 21 seconds |
| `database: <cluster>` | one database | Backup canary-namespace-backup-pg-11ec704d |
| `all: true` | everything enabled, with quiesce | snapshot afb3f27f and a Backup, the writer down from 22:27:37 to 22:28:12 |

A CEL rule on the CRD accepts exactly one of the three. The claim or Cluster
has to carry `backup.wlz.li/enabled: "true"`.

## Restore

| Run | Restores |
| --- | --- |
| `claim: <claim>` | one volume, in place, once nothing mounts it |
| `claim:` with `into: <new claim>` | one volume into a new claim; the app keeps running |
| `database: <cluster>` | one database |
| `all: true` | every enabled volume in place, then every enabled database |

Each accepts `restoreAsOf`. A volume restores the newest snapshot taken at or
before it, and a database replays WAL to it exactly. Left out, a volume restores
its newest snapshot and a database the end of its WAL archive.

### Checks before anything is touched

The run lists each claim's snapshots and each Cluster's base backups first. An
item no backup reaches fails the run before any volume is overwritten or any
Cluster deleted:

```text
PersistentVolumeClaim canary-namespace-backup-data: no snapshot at or before 2026-09-24T21:00:00Z; the oldest, defb7a4d, is from 2026-09-24T22:09:58Z
```

```text
Cluster canary-namespace-backup-pg: no base backup finished by 2026-09-24T22:12:00Z; the oldest, 20260924T221544, finished at 2026-09-24T22:15:45Z
```

Without the first check, VolSync's mover prints `No eligible snapshots found`,
exits 0, and the restore reports success having written nothing.

### A database restore

The run marks the item Deleted, then deletes the Cluster, and waits in phase
Waiting:

```text
recreate canary-namespace-backup-pg to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it
```

When Flux or tofu creates the Cluster again, the bootstrap webhook finds the
run, writes the run's moment as the recovery target and names the run on the
Cluster:

```yaml
metadata:
  annotations:
    backup.wlz.li/restore-run: db-2219
spec:
  bootstrap:
    recovery:
      source: backup-controller
      database: canary
      owner: canary
      recoveryTarget:
        targetTime: "2026-09-24T22:19:00Z"
```

The run follows the Cluster to `Cluster in healthy state`. The canary's writer
kept running through it; its report before and after:

```text
2026-09-24T22:22:33Z db=[18 2026-09-24T22:21:23Z,2026-09-24T22:22:24Z,2026-09-24T22:22:33Z] file=[6 2026-09-24T22:21:23Z,2026-09-24T22:22:24Z,2026-09-24T22:22:33Z]
2026-09-24T22:24:33Z db=[14 2026-09-24T22:17:47Z,2026-09-24T22:18:25Z,2026-09-24T22:24:33Z] file=[7 2026-09-24T22:22:24Z,2026-09-24T22:22:33Z,2026-09-24T22:24:33Z]
```

The rows from 22:20:25 to 22:22:33 are gone, the table ends at 22:18:25, the
last tick before the target, and new writes continue. A Cluster that declares
its own recovery while a run waits for it is refused, so the two targets cannot
race.

### Restore a whole namespace

`all: true` with `restoreAsOf: "2026-09-24T22:16:30Z"`, the writer stopped, then
the canary applied again to recreate the Cluster. The writer's first report:

```text
start db=[10 2026-09-24T22:14:38Z,2026-09-24T22:15:38Z,2026-09-24T22:15:47Z] file=[9 2026-09-24T22:13:38Z,2026-09-24T22:14:38Z,2026-09-24T22:15:38Z]
```

The volume holds snapshot d1cb7739, whose clone was cut at 22:15:41; the
database holds every row to 22:16:30. The row at 22:15:47 is in the database
and not on the volume: each side goes to its own newest point at or before the
moment, by design.

## Automatic restore

A claim filled by its VolumeRestore and a Cluster created by Flux or tofu
restore with no run. The canary destroyed and applied again:

```text
start db=[13 2026-09-24T22:27:08Z,2026-09-24T22:28:14Z,2026-09-24T22:29:14Z] file=[10 2026-09-24T22:14:38Z,2026-09-24T22:15:38Z,2026-09-24T22:27:08Z]
```

The volume came back from the newest snapshot, afb3f27f, and the database to the
end of its archive. The 22:30:14 row had not been archived when the canary was
destroyed; `archive_timeout` bounds that window.

With `backup.wlz.li/restore-as-of: "2026-09-24T22:16:30Z"` on the claim and the
Cluster, the same destroy and apply came back to that moment on both sides:

```text
start db=[10 2026-09-24T22:14:38Z,2026-09-24T22:15:38Z,2026-09-24T22:15:47Z] file=[9 2026-09-24T22:13:38Z,2026-09-24T22:14:38Z,2026-09-24T22:15:38Z]
```

The webhook refuses a Cluster pinned before every base backup. The canary
applied with `backup.wlz.li/restore-as-of: "2026-09-24T21:00:00Z"` failed on:

```text
admission webhook "bootstrap.backup.wlz.li" denied the request: annotation backup.wlz.li/restore-as-of asks for 2026-09-24T21:00:00Z, and no base backup in prod-cluster-backup-cnpg/canary-namespace-backup/canary-namespace-backup-pg/base/ finished by then; the oldest, 20260924T221544, finished at 2026-09-24T22:15:45Z.
```

A claim pinned before every snapshot stays Pending, with reason NoBackupInReach
on its VolumeRestore, once a pod is scheduled for it. The populator's test
covers that; on the canary the writer depends on the refused Cluster, so no pod
was scheduled and the populator never ran.

The annotation pins every later creation too, so it raises the
`backup_controller_restore_pinned` series while it is set.

## Metrics

The manager serves these on port 8081, beside the populator library's own
listener on 8080:

| Series | Holds |
| --- | --- |
| backup_controller_namespace_last_success_timestamp_seconds | the namespace's newest successful run with all set, or its creation time |
| backup_controller_namespace_schedule_interval_seconds | seconds between two ticks of its schedule |
| backup_controller_namespace_schedule_invalid | 1 while the schedule does not parse |
| backup_controller_restore_pinned | 1 per claim or Cluster carrying backup.wlz.li/restore-as-of |

Each series names the namespace it describes in its namespace label, so a
scrape has to honour the target's labels; the infrastructure repository's
PodMonitor sets honorLabels for that reason.

## Reading a restic repository

The restore checks and each BackupRun's snapshot time read the repository's own
files through the S3 client, in internal/restic: the key file opened with scrypt
and the repository password, snapshots decrypted with AES-256-CTR and checked
with Poly1305-AES, and repository version 2's zstd header handled. Its tests read
a fixture restic 0.19.1 wrote. The controller runs no restic binary and pins no
image beside VolSync's.
