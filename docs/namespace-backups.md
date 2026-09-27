# Namespace backups

From v0.5.0 the controller runs the backups of a namespace itself. A namespace
names one schedule, and each claim and CloudNativePG Cluster in it opts in. One
BackupRun or RestoreRun covers one volume, one database, or all of them. VolSync
still moves every byte of a volume. The barman-cloud plugin still moves every
byte of a database. The controller decides when, and in which order.

The walzen prod cluster made every output on this page on 2026-09-24. The
source is the canary that the infrastructure repository keeps at
modules/testing/canary-namespace-backup. Each minute, its writer appends one
timestamp to a file on its volume and inserts the same timestamp into its
database. The writer logs the last three entries of each.

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
| backup.wlz.li/timeout | Namespace | how long a run there may work after admission, a Go duration such as `10h`. Without it, 6h. The `spec.timeout` of the run wins. |
| backup.wlz.li/max-quiesce | Namespace | how long a run with `all: true` may keep its stopped workloads stopped, a Go duration such as `20m`. Without it, 10m, counted from the `status.quiescedAt` of the run. |
| backup.wlz.li/prune-interval-days | Namespace | days between prunes of each repository that the sources of the namespace write. Without it, 1. |
| backup.wlz.li/enabled | claim, Cluster | whether a run includes it. The controller reads nothing else to find what to back up. |
| backup.wlz.li/retain-last and the six other retain- annotations | claim | which snapshots the repository of the volume keeps. See [Retention](#retention). |
| backup.wlz.li/quiesce | Deployment, StatefulSet | whether the workload stops while the volumes' clones are cut, and while a RestoreRun restores in place |
| backup.wlz.li/restore-as-of | claim, Cluster | the moment every automatic restore of it goes back to |

A claim still names its VolumeRestore in `dataSourceRef`. That object gives the
repository Secret, the cache class and the mover security context. The retention
of a database stays a window on its ObjectStore, because the barman-cloud plugin
accepts `retentionPolicy: "30d"` and no count.

### Retention

A claim sets any mix of seven annotations, and at least one. The controller
copies each annotation to the matching field of the retain block in the
ReplicationSource of the claim. The VolSync mover gives that block to
`restic forget`:

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

If a claim has none of the seven, its item fails before the run writes a
source. The message lists all seven. A count below 1, or a span that restic
cannot read, fails the item. The message names that annotation.

## Scheduled runs

At each tick of the schedule of a Namespace, the controller creates a
BackupRun named `scheduled-<yyyymmdd-hhmm>` with `all: true`. The run has a
label with the tick, and the controller keeps the run for 30 days. If the
controller was down at a tick, that tick runs once when the controller is back.
While the controller runs with `--pause`, it creates no run, and a new run
waits with reason `Paused` ([upgrading.md](upgrading.md#pause-the-controller)).
While another run with `all: true` in the namespace is unfinished, the tick
waits for it.

A due tick also waits while no claim or Cluster in the namespace has
`backup.wlz.li/enabled: "true"`, because a run created then fails with Invalid.
Flux writes a Namespace before the claims in it. Thus, if you add a schedule to
an existing namespace, a tick can become due a second before its claims have
the mark. Until one has it, the controller creates no run. It records a Warning
with reason NothingEnabled on the Namespace, with the note `the tick at
2026-09-23T05:00:00Z is due, but nothing in this namespace is marked
backup.wlz.li/enabled: "true"`. The controller checks again when a claim
changes, and at least every five minutes. The five-minute check is how it finds
a marked Cluster. `kubectl describe namespace <name>` lists the Warning under
Events.

A scheduled run has no timeout of its own. Thus it stops at the same point as a
manual run without a timeout: `backup.wlz.li/timeout` on the Namespace, or six
hours after Kueue admits it. The clock starts at admission, so the time that a
run spends in Queued does not count. At the deadline, the run does these steps:

- It marks its unfinished items Failed.
- It restarts the workloads that it quiesced.
- It releases its Workload.

A VolSync mover or a CloudNativePG Backup that is still in progress continues
to run. If the start of an item failed each time, the item ends with the
deadline message and then its last error: `<deadline message>; last error:
<error>`.

The first scheduled run of the canary used a `*/15` schedule, sampled every
three seconds. The output leaves out lines that repeat the line before:

```text
22:15:03 phase=Queued replicas=1/1 clone=
22:15:12 phase=Waiting replicas=0/ clone=
22:15:41 phase=Waiting replicas=0/ clone=Pending
22:15:46 phase=Running replicas=1/1 clone=Bound
22:16:00 phase=Succeeded replicas=1/1 clone=
```

This is its status after the run, on v0.5.4, cut to the fields that this page
discusses. A run on v0.6.0 or later also moves the snapshot to `restartedAt`, as
[Quiesced snapshots](#quiesced-snapshots) shows:

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

1. It creates one Kueue Workload, counted as one pod, in the LocalQueue of the
   namespace. It waits for Admitted (phase Queued). The app continues to run
   while the run waits. The run sets the PodsReady condition of the Workload
   itself. The Workload has no pods, and the Kueue waitForPodsReady feature
   would evict it. A namespace with no LocalQueue starts at once.
2. It writes its plan into the status before it changes anything. The plan has
   two parts:
   - `status.quiesced` holds every workload marked `backup.wlz.li/quiesce`,
     with the replicas that it has now.
   - `status.suspendedKustomizations` holds the Flux Kustomizations to suspend.

   Before that write, the run gets the quiesce Lease of the namespace. While
   another run holds the Lease and has not yet given the workloads of this
   namespace back, the run waits with reason SourceBusy
   ([One quiesce at a time](#one-quiesce-at-a-time)). Then it suspends those
   Kustomizations and scales the workloads to zero. It waits until none of
   their pods is left, also pods that shut down, because a pod that shuts
   down can still write. A pod in phase Succeeded or Failed, such as an evicted
   pod, does not count, because its containers ended.
3. It writes the ReplicationSource of each enabled claim with the manual tag of
   the run. It creates a CloudNativePG Backup for each enabled Cluster. It skips
   a Cluster that has `cnpg.io/hibernation: "on"`.
4. When the clone of every volume is cut, it records `restartedAt` with
   `restartPending: true` and writes the status. Then it gives each workload its
   replicas back, resumes only the Kustomizations that it suspended, and clears
   `restartPending`. The upload continues from the clone.

   If a restart fails, the run reports it at once, with reason RestartFailed and
   a Warning event. The pass runs again with the controller-runtime backoff. The
   message says that the run still backs up and that you must not delete it.
   It lists what to scale and resume manually: `The run is still backing up; do
   not delete it. To give the app back now, scale Deployment notes to 2 and
   resume Kustomization flux-system/notes yourself; the run then goes on by
   itself.` A restart skips a workload that is already at its recorded count.
   It also skips a Kustomization that is not suspended. Thus, the manual steps
   let the run continue.
5. When VolSync completes the trigger of the run, the run finds the snapshot
   of each volume in the repository itself. It takes the newest snapshot that a
   mover wrote (host `volsync`, the one path `/data`, no original) with a time
   in the window of the sync. The window goes from
   `lastSyncTime - lastSyncDuration` to `lastSyncTime`. The run makes the window
   one second wider for the whole-second `lastSyncTime`. It also makes the
   window 5 seconds wider on each side for the clocks of the mover node and the
   VolSync manager. A snapshot that another BackupRun recorded for the claim is
   never a candidate. The item records the full ID of the snapshot in
   `snapshotID`, its short ID, and the time that restic put on it. If step 2
   stopped a workload, the run writes the snapshot again at `restartedAt`, as
   the next section describes. Then it deletes the Workload.

On the canary, the writer was down for 34 seconds. Most of that time was the
30-second termination grace of the pod, because its shell loop does not handle
SIGTERM.

The run never reads the mover log to find the snapshot. Thus a VolSync that
keeps no log, or a log that the run could not read, changes nothing.

A sync can leave more than one snapshot. VolSync retries a failed mover pod in
the same sync. A pod that saved its snapshot and then failed at `forget` leaves
that snapshot. The newest snapshot is from the attempt that succeeded. The
message of the item names the others: `the sync also left snapshot 2edf5bab
from failed attempts; they stay in the repository until retention forgets
them`.

If a sync has no snapshot in its window, it backed up a claim that held only
lost+found. The VolSync mover skips such a claim and still reports success.
An S3 listing can show the write of the mover late. Thus the item records the
first empty listing in `noSnapshotListedAt` and waits: `the repository listed
no snapshot of the sync at <time>; the run lists it again after 11s before it
takes the volume for empty`. The item succeeds with `empty: true` only when the
second listing also finds no snapshot. The message is then `the volume held no
files, so VolSync took no snapshot`. If the ReplicationSource status of a
completed sync has no `lastSyncTime` or `lastSyncDuration`, the item fails with
reason NoMoverSnapshot. The run cannot tell which snapshot the sync wrote.

A repository holds the backups of one claim. The window tells two syncs of
different claims apart only when the claim Lease and the repository Lease stop
them from running at the same time. One run may take the repository Lease for
two of its own claims. Thus two claims that share a repository could each take
the snapshot of the other. Also, the `restic forget --host volsync` of VolSync
after each backup would apply the retention of one claim to the snapshots of the
other. The walzen infrastructure repository declares one repository per volume.

A finalizer does step 4 and deletes the Workload on failure, on timeout and when
you delete the run. If a step there fails, the run stays unfinished and tries
again. It reports reason RestartFailed while the app is still down. It reports
reason ReleaseFailed when the app is back and only a Lease or the Workload is
left:

```text
could not release the Leases it holds on its claims and repositories: <error>. The run retries until it can, and this namespace's schedule waits for it. Fix the cause, or delete the Leases labelled backup.wlz.li/lease-holder-uid=<uid> yourself; either way the run then finishes by itself.
```

The run deletes the Workload only after it gives the app back. Thus it keeps
its place in the queue while it still owes the app its replicas. The run reports
a failed Workload delete in the same way. The message starts `could not delete
its Kueue Workload backuprun-<uid>, which holds the run's place in the queue`
and tells you to delete the Workload manually. After a failed restart, a run
that you delete says that the deletion completes by itself. It also says that a
person can remove the finalizer `backup.wlz.li/run-cleanup` if the deletion does
not complete after the app runs again.

The run writes each plan before it acts on it. If a pass stops the workloads and
then loses its status write, the next pass starts again from the recorded plan.
A new read of the workloads at that point would find them at zero replicas, and
the run would give back nothing. For the same reason, the run writes
`restartedAt` before the restart. A pass that finds `restartPending` set does
the restart again with the recorded moment. After the stored status shows that
the restart is done, no pass and no finish does it again. Thus the run never
scales up a workload that another run stopped after it. If a stop fails
partway, the run does these steps:

1. It cuts the plan to the workloads at zero and the suspended Kustomizations.
2. It starts those again.
3. It ends Failed.

`quiescedAt` and `restartedAt` are whole seconds.

The workloads stay stopped for at most the `backup.wlz.li/max-quiesce` of the
namespace, counted from `quiescedAt`. Without the annotation, the limit is ten
minutes. When the limit ends, the run gives the workloads back. It fails each
volume item whose clone VolSync did not cut:

```text
VolSync had not cut the clone volsync-notes-data-src by 2026-09-26T09:41:00Z, when the backup.wlz.li/max-quiesce limit of 10m ran out and the workloads were given back
```

If the start of an item did not get that far, the item fails too. The message
is `not started before the workloads were given back at <time>, when the
backup.wlz.li/max-quiesce limit of 10m ran out, so the clone
volsync-notes-data-src was never cut`, then the last start error of the item if
it has one. The rest of the run continues, and the upload continues for each
clone that VolSync cut. A VolSync that cuts no clone never holds a stopped
workload for longer than the limit.

If the annotation is not a positive Go duration, a run with `all: true` fails
before it stops anything: `namespace notes has backup.wlz.li/max-quiesce
"soon", which is not a duration such as 20m`. If the value changes to a value
that does not parse while the app is down, the run aborts and gives the
workloads back.

A clone claim `volsync-<claim>-src` counts as cut in step 4 when these
conditions are true:

- It is Bound.
- It is not in deletion.
- Its creation time is after `quiescedAt`.

VolSync marks a sync done before its cleanup deletes the clone. Thus the clone
of the previous sync can still be Bound when a run starts. That clone holds the
data from before the app stopped. The app stays down until the own clone of the
run exists.

### Which Kustomization a run suspends

The kustomize-controller labels `kustomize.toolkit.fluxcd.io/name` and
`kustomize.toolkit.fluxcd.io/namespace` on a workload name the Kustomization
that applied it. The run suspends that Kustomization only when its
`status.inventory.entries` lists the workload with the id
`<namespace>_<name>_apps_<Kind>`, such as `notes_notes_apps_Deployment`. Anyone
who can edit a workload can set its labels. To suspend a Kustomization is a
write in another namespace. Thus the labels alone are not sufficient.

The run scales a workload down and suspends nothing in these conditions:

- The workload has no labels.
- The Kustomization of the workload is gone.
- The Kustomization of the workload does not list it.

The run leaves out a Kustomization that is already suspended. The run did not
suspend it, and must not resume it.

A Kustomization can also list a Deployment or a StatefulSet in another
namespace in its inventory. Then the run ends with reason `Invalid` before it
stops anything, whether the Kustomization is already suspended or not:

```text
Kustomization flux-system/apps applies workloads in namespaces notes and wiki; a run suspends the Kustomization while it stops workloads, which would leave the other namespace's workloads unmanaged by Flux, so it refuses. Give each namespace its own Kustomization
```

While the run held the Kustomization suspended, Flux would not correct the
workloads of the other namespace. Also, a run in that namespace could resume the
Kustomization while this run held its app at zero. Flux could then scale the app
back up during the backup. Give each namespace whose workloads a run stops its
own Kustomization.

If a read of a Kustomization fails, the run tries again at the next pass. The run
never takes a version that Flux stops to serve as a deleted Kustomization. The
run reports `the API server no longer serves Kustomization at kustomize.toolkit.fluxcd.io/v1 (...);
the controller looks up the served version again and retries, and a restart of
the controller also clears the versions it has cached`. It then learns again
what the API server serves, and tries again. The controller finds the kind by
group and kind. Thus a Flux upgrade that serves the Kustomization at another
version changes nothing here, for a BackupRun and for a RestoreRun.

### Quiesced snapshots

restic puts the moment when `restic backup` starts on a snapshot as its time.
The mover starts it when the clone is Bound. At that point, step 4 gives the app
its replicas back. Thus the time can come after the app wrote again. A database
recovered to that time would hold rows that the volume does not have.

For this reason, a run that stopped workloads writes the snapshot of each volume
again after the mover is done. The new snapshot has the `restartedAt` of the run
as its time, and the tag `quiesced`. It names the snapshot that the mover wrote
as its `original`. The controller then deletes that original snapshot.
`restic rewrite --forget --new-time` makes the same change, and
`restic tag --add` adds the tag.

Nothing wrote between the stop of the last pod and `restartedAt`. The
controller reads the clock at the start of the reconcile that gives the replicas
back. It drops the fraction of a second. It writes that moment to the status
before it scales anything up. That reconcile runs after the reconcile that found
the pods gone. The rewritten snapshot has the same whole-second time, which is
the precision that the VolSync mover compares at. Under v0.7.x and earlier, the
rewritten snapshot kept the fraction, and the status showed it cut to the
second.

This is the 11:45 scheduled run on the prod canary, on v0.6.0. Its mover wrote
this log in the `status.latestMoverStatus.logs` of the ReplicationSource:

```text
using parent snapshot 2193fdf9
Added to the repository: 13.948 KiB (1.560 KiB stored)
processed 1 files, 13.494 KiB in 0:00
snapshot da4d7eb4 saved
Restic completed in 2s
```

The run recorded the rewritten snapshot, at `restartedAt`:

```yaml
items:
  - kind: ReplicationSource
    name: canary-namespace-backup-data
    phase: Succeeded
    snapshot: 04833d50
    snapshotTime: "2026-09-25T11:45:48Z"
quiesced:
  - kind: Deployment
    name: canary-namespace-backup
    replicas: 1
quiescedAt: "2026-09-25T11:45:10Z"
restartedAt: "2026-09-25T11:45:48Z"
```

The 12:00 run's mover, restic 0.18.1, took the rewritten snapshot as its parent:

```text
using parent snapshot 04833d50
Added to the repository: 14.256 KiB (1.582 KiB stored)
processed 1 files, 13.802 KiB in 0:00
snapshot 563cd92c saved
Restic completed in 3s
```

The controller holds the exclusive lock of restic while it writes. The restic
`prune` holds the same lock. Any other lock in locks/ makes the rewrite wait,
shared locks included. The exception is a lock older than the 30-minute stale
limit of restic.

The controller deletes a stale lock with the username `backup-controller`,
whatever host and PID it names. If a new controller pod replaced an old pod
before the old pod removed its lock, that lock has a hostname that no later pod
has. That lock would stop every prune. As an exclusive lock, it would also stop
every backup, because VolSync runs `restic unlock` only when
`spec.restic.unlock` changes.

After a rewrite, the controller tries three times, a second apart, to remove its
own lock. While the lock of another process is held, the item of the volume
stays Running with a message that names the host of the lock. The run tries
again at its next poll. Thus it never reports Succeeded for a snapshot without
the tag.

### Sources the controller writes

A source has its claim as owner and has the name of the claim. The node
affinity of the PersistentVolume places the mover. Thus the mover runs whether
the pod of the app runs or not. The mover has no queue label, because the
Workload of the run already admitted it.

A run writes the spec of the source only when it finds the source idle. It
leaves a source that already has the tag of the run as it is. Thus a change to
the retention or the affinities gets to the source on the next run that finds
it idle. That can be a later run than the run that found it busy. This is the
source of the canary after the 22:45 scheduled run, cut to spec and to the
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
trigger in a tight loop. It does not sync a source whose tag equals
`lastManualSync`. Thus a spent tag is how a source waits for the next run, as
the status above shows. Each tag that the controller writes sets
`spec.restic.unlock` to the same value. Thus VolSync runs `restic unlock` before
that backup. restic removes only the locks that it counts as stale. For a lock
from another host, that is 30 minutes. Thus a lock that a killed mover left
clears on the next run by itself.

If a run finds a source that still completes the tag of another run, it waits
for it. The reason is SourceBusy, and the message names that run:
`ReplicationSource canary-namespace-backup-data is still completing the backup
of BackupRun scheduled-20260924-2245`. If VolSync started a sync of a source
with no open tag, the run waits in the same way. The message is
`ReplicationSource canary-namespace-backup-data is still syncing; VolSync has
not recorded the end of its last sync`. If the controller did not write a source
with the same name, the run leaves that source alone. The item fails and names
it.

A mover Job that fails leaves the tag open. For as long as the tag stays,
VolSync writes the logs of the Job into `status.latestMoverStatus`, deletes the
Job and starts another. The run fails the item at once with the mover logs when
all these conditions are true:

- The source still has the tag of the run.
- The source did not complete the tag.
- The source reports a Failed mover in a sync that VolSync started after the
  `startedAt` of the run.

Thus the failure shows on the run before its timeout. The run leaves the
ReplicationSource alone. The reason of the item is MoverFailed. Its message is
`Mover logs: ` and then the logs as VolSync kept them, whatever they say. A
mover that failed on a lock shows the restic message `repository is already
locked` there.

A later run waits only for a tag that some run still needs. A run does not wait
for a tag in these conditions:

- The run of the tag failed, timed out or was deleted.
- v0.7.x or v0.8.x wrote the tag.

VolSync would then complete the new trigger with that older backup. Thus the run
fails that item at once and continues with the rest of the namespace. The
message names the run or the trigger of the tag and what VolSync does with it.
It also tells how to give that backup up: `To give that backup up, delete the
ReplicationSource canary-namespace-backup-data while no pod of Job
volsync-src-canary-namespace-backup-data is running; the next backup unlocks the
repository first.` The run leaves the source as it is. The item continues to
fail in that way until you delete the source.

While a run waits for the tag of a live run, the later runs of a quiesced
namespace back up nothing. A run checks every source before it stops the app.

While VolSync continues to retry, an item that the run fails names the data
that a snapshot of that sync holds. VolSync retries the sync with the clone that
it cut when the sync started. restic puts the time of the retry on the snapshot
of a retry. Thus that snapshot can hold data older than its time. The message
ends `VolSync keeps retrying the sync it started at 2026-09-26T09:41:00Z with
the clone it cut then, so a snapshot this sync saves later holds the data of
2026-09-26T09:41:00Z, whatever time restic stamps on it.` If VolSync did not
cut the clone yet, the message ends `VolSync goes on with the sync it started
at 2026-09-26T09:41:00Z and has not cut its clone yet, so a snapshot this sync
saves later holds the data of the moment it cuts the clone, whatever time
restic stamps on it.`

Such a snapshot has no `quiesced` tag. Thus a `syncDatabaseToVolume` restore
never chooses it. A plain `restoreAsOf` may choose it. The volume then holds the
data that VolSync captured for that claim, with a time later than the data.

v0.8.0 and v0.8.1 deleted the ReplicationSource at this point. When VolSync
reports the failure, it already started the next mover Job. Thus the delete
killed that mover during the backup. PID 1 of the mover is bash, which ignores
SIGTERM. Thus restic stopped by SIGKILL and left its lock in the repository.
Each later `restic forget` of that claim then failed until someone ran
`restic unlock`. v0.8.2 removed the delete.

When you upgrade to v0.8.2, run `restic unlock` once against the repository of
each claim that had a mover failure under v0.8.0 or v0.8.1. (`restic list locks`
shows whether a repository holds one.) Without the unlock, each retry saves a
snapshot and fails at `forget` with `repository is already locked`. VolSync then
retries again. Thus the repository gets several hundred snapshots a day that
restic never forgets. A plain `restic unlock` removes only locks older than 30
minutes. A lock left under v0.8.1 is that old by the time you run it.

A source that still has such a tag, and whose retries fail on that lock, never
finishes by itself. Under v0.9.0, each later run fails that item at once with
the message above. To solve this, do the step that the message names. Delete
the ReplicationSource while no pod of its Job runs. The next backup then
unlocks the repository.

## Back up now

| Run | Backs up | Measured |
| --- | --- | --- |
| `source: <claim>` | one volume | snapshot defb7a4d, stamped 22:09:58, in 21 seconds |
| `database: <cluster>` | one database | Backup canary-namespace-backup-pg-11ec704d |
| `all: true` | everything enabled, with quiesce | snapshot afb3f27f and a Backup, the writer down from 22:27:37 to 22:28:12 |

A CEL rule on the CRD accepts exactly one of the three. The claim or Cluster
must have `backup.wlz.li/enabled: "true"`.

A BackupRun ends with reason Invalid only for a spec that no retry can fix:

- A named claim or Cluster is missing or has no mark.
- Nothing in the namespace has the mark.

If the installed CRD does not have a field that the controller writes, the run
ends with reason CRDOutdated before it changes anything.
[architecture.md](architecture.md#objects-the-controller-writes) has the check.
After the run has its items, an item fails only because of what the item itself
holds:

- `the claim <name> no longer exists`.
- `claim <name> is bound to the PersistentVolume <pv>, which does not exist`.
- The retain- annotations of a claim do not parse.
- A source has a tag that no run waits for
  ([Sources the controller writes](#sources-the-controller-writes)).
- The API server rejects a source or a Backup as invalid.

A retry may fix some errors, such as a CloudNativePG webhook that the run cannot
reach. A start that fails with such an error no longer ends the pass. The item
stays Pending with `not started yet: <error>`, and the error goes in
`lastStartError`. A start that succeeds clears `lastStartError`. A run that
gives the item up adds the error to the item message.

The Ready condition gets reason Retrying with a message that names each such
item and each wait: `retrying the start of notes-pg: <error>; ReplicationSource
notes-data is still completing the backup of BackupRun scheduled-20260926-0900`.
The run continues to work. The quiesced workloads still come back when the
clones are cut. The run tries the item again on each pass until the timeout of
the run. Without a retry, a run that waits on another run shows reason
SourceBusy and names each wait in its message. A timeout or a 5xx from the API
server while the run plans, stops workloads or starts an item returns as an
error. The next reconcile tries again.

A cluster without the CloudNativePG CRDs holds no Clusters. The scheduler and
each run with `all: true` find none there. A BackupRun with `database: <name>`
ends Invalid with `no Cluster <name> in this namespace; the cluster has no
CloudNativePG CRDs`.

## One mover at a time

A backup and a restore of the same claim or repository never run at the same
time. Immediately before a run triggers its mover or creates its restore Job, it
gets two `coordination.k8s.io` Leases:

- The claim Lease, with the name `backup-controller-claim-<claim uid>`.
- The repository Lease, with the name `backup-controller-repo-<secret uid>`.

The create of a Lease is atomic. If two runs get to that moment at the same
instant, the API server admits one, and the other waits.

The run writes the Lease in its own namespace. The Lease names its holder in
`backup.wlz.li/lease-holder-kind` (BackupRun or RestoreRun),
`backup.wlz.li/lease-holder-uid` and `backup.wlz.li/lease-holder-name`. It lists
the items of the holder in `backup.wlz.li/lease-items`. A run releases its
Leases when an item finishes. It takes over a Lease whose holder finished or no
longer exists. A Lease counts as held while the item of the holder, of the kind
that gets the Lease, is Pending or Running. Thus a Cluster item with the same
name as a claim does not keep the Lease of that claim.

Two BackupRuns of the same Cluster also never run at the same time. Immediately
before a Cluster item creates its CloudNativePG Backup, it gets the Cluster
Lease, with the name `backup-controller-cluster-<cluster uid>` and the label
`backup.wlz.li/lease-scope: cluster`. The Cluster item holds it while it is
Pending or Running, and the run releases it when the item ends. The other run's
Cluster item stays Pending with reason SourceBusy and creates no Backup until
then.

If a run finds another run that holds one of the Leases, it waits with reason
SourceBusy:

```text
BackupRun scheduled-20260926-0900 holds Lease backup-controller-claim-3f2a1c7e-9d2b for notes-data; this run starts once that run has finished with it
```

Immediately before a run triggers its mover or creates its restore Job, it also
looks for work of the other kind on the claim or its repository:

- A BackupRun looks for a restore Job of the controller whose
  `backup.wlz.li/claim` or `backup.wlz.li/repository` annotation names them.
- A RestoreRun looks for a ReplicationSource whose backup is in progress.

It waits for such work with reason SourceBusy too. A restore Job holds the claim
while its RestoreRun is unfinished and the item of the run did not finish:

```text
RestoreRun notes-back-to-friday is restoring claim notes-data from repository notes-restic-data with restore Job restore-<run uid>-0; this run starts once that restore has finished
```

After that point, and for a Job of the populator or of a run that is gone, the
Job holds them in two conditions. The Job controller may still start a pod for
it, or one of its pods may still run restic:

```text
restore Job restore-<run uid>-0 of RestoreRun notes-back-to-friday may still write claim notes-data from repository notes-restic-data; this run starts once the Job is stopped and its pods have ended
```

A namespace BackupRun does both checks for each claim before it stops anything.
The checks are the Leases and the work of the other kind. A read that fails
there returns an error and stops nothing. Thus the run never stops an app for a
backup that then waits, or on a read that it could not make.

No Lease guards a claim that does not exist, and the item reports it. A backup
fails the item with `the claim <name> no longer exists`. If a repository Secret
does not exist, the item fails before any mover or restore Job exists. A mover
started without the repository Lease would run without a guard when the Secret
appears:

```text
repository Secret notes-restic-data does not exist in this namespace, so the run can't take the Lease that keeps other runs' movers off the repository
```

A namespace BackupRun fails such an item in the checks it makes before it stops
anything. When those checks leave no item to back up, the run never stops the
app.

Any other failed read of the claim or the Secret is an error. The run tries
again with nothing stopped.

The run releases the Lease of a finished item on its next pass. That release is
best effort. The run logs a failure and continues. Thus it never holds the app
past the quiesce limit. When the run finishes, it releases each claim and
repository Lease that it still holds. The quiesce Leases go as
[One quiesce at a time](#one-quiesce-at-a-time) describes.

## One quiesce at a time

Two runs never stop the workloads of one namespace at the same time. Before a
run writes a stop plan, it gets the `coordination.k8s.io` Lease
`backup-controller-quiesce` in its own namespace. Only two kinds of run get it:

- A BackupRun with `all: true` that has at least one target.
- A RestoreRun in place that has a workload to stop: one marked
  `backup.wlz.li/quiesce` or one that its `spec.quiesce` lists.

A run with `source:` or `database:` gets no quiesce Lease.

The Lease has the same holder labels as the claim and repository Leases
(`backup.wlz.li/lease-holder-kind`, `backup.wlz.li/lease-holder-uid` and
`app.kubernetes.io/managed-by: backup-controller`). The label
`backup.wlz.li/lease-scope: quiesce` is the mark that tells it apart. The name
of the holder is in the `backup.wlz.li/lease-holder-name` annotation, and
`spec.holderIdentity` holds the UID of the holder. The Lease has no
ownerReferences. The Lease is stale when its holder finished, is gone, or stored
the restart that gave the workloads back. The next run then gets it with an
update that has the resourceVersion that it read.

If a run finds another run that holds the Lease of the namespace, it waits with
reason SourceBusy:

```text
BackupRun scheduled-20260926-0300 has stopped the workloads of this namespace (Lease backup-controller-quiesce); this run stops them once that run has given them back
```

Before it gets the Lease, a run that will stop workloads also waits while an
unfinished RestoreRun in its namespace has a Cluster item in phase Deleted. The
Kustomization that this run would suspend may be the one that Flux needs to
create that Cluster again:

```text
RestoreRun notes-back-to-friday has deleted Cluster notes-pg and waits for it to be created again; this run stops the workloads once that Cluster is back
```

If a run has no more time while it waits, it ends with nothing stopped. The
message names the wait:

```text
the run had not finished by 2026-09-26T10:00:00Z; it was waiting: BackupRun scheduled-20260926-0300 has stopped the workloads of this namespace (Lease backup-controller-quiesce); this run stops them once that run has given them back
```

The quiesce Lease stays when the claim and repository Leases go. The run
deletes it at three points:

- On the pass that finds that the stored status shows the restart done.
- In `finish`, after the terminal status write.
- In `finalize`, after the run drops the finalizer.

Each release is best effort. The run logs a failure and continues. A Lease that
stays is stale under the rule above, so the next run that wants it gets it.

Do not delete the Lease manually while its holder still runs. That opens again
the bug that the Lease prevents. The next run gets the Lease and records the
zero replicas that the holder stopped. Both runs can then end with the app still
at zero.

A run judges the restart of another run only by the stored status of that run.
After the status records the restart as done, the holder never gives the app
back again. Thus if someone scales a workload to zero after that, it stays at
zero, and the next run records zero as its count.

## Restore

| Run | Restores |
| --- | --- |
| `claim: <claim>` | one volume, in place, once nothing mounts it |
| `claim:` with `into: <new claim>` | one volume into a new claim, while the app continues to run |
| `repository:` with `into: <new claim>` and `intoSize` | a repository no claim here owns, into a new claim |
| `database: <cluster>` | one database |
| `all: true` | every enabled volume in place, then every enabled database |

Each accepts `restoreAsOf`. A volume restores the newest snapshot taken at or
before it. A database replays WAL exactly to it. Without `restoreAsOf`, a volume
restores its newest snapshot, and a database restores the end of its WAL
archive.

A run with `all: true` also accepts `syncDatabaseToVolume`. It recovers the
databases to the moment that the snapshot of the volumes holds. Each run except
an `into` restore stops the workloads marked `backup.wlz.li/quiesce` while it
restores, the same ones that a BackupRun with `all: true` stops. It also
accepts `quiesce`, a list of more workloads to stop. [Restore a whole
namespace to one moment](#restore-a-whole-namespace-to-one-moment) uses both.

### Checks before anything is touched

First, the run lists the snapshots of each claim and the base backups of each
Cluster. If no backup reaches an item, the run fails before it overwrites a
volume or deletes a Cluster:

```text
PersistentVolumeClaim canary-namespace-backup-data: no snapshot at or before 2026-09-24T21:00:00Z; the oldest, defb7a4d, is from 2026-09-24T22:09:58Z
```

```text
Cluster canary-namespace-backup-pg: no base backup finished by 2026-09-24T22:12:00Z; the oldest, 20260924T221544, finished at 2026-09-24T22:15:45Z
```

The run uses only the snapshots with the layout that a VolSync mover writes:
host `volsync` and paths exactly `[/data]`. If a repository holds no such
snapshot, the run refuses with reason NoBackupInReach before it creates
anything. The restore Job restores the selected snapshot by its full ID. Thus
the Job restores each of two snapshots in one second as selected.

[restores.md](restores.md#which-snapshot-a-run-restores) has the rules.
[Restore a whole namespace to one moment](#restore-a-whole-namespace-to-one-moment)
has the rule that a `syncDatabaseToVolume` run adds. Immediately before the run
creates a restore Job, it lists the repository one more time. The item fails
with reason SnapshotChanged in two conditions:

- The retention of a backup removed the snapshot after the checks.
- That backup rewrote the snapshot at another time.

The message has the cause and then `. Nothing was written to claim <claim>.
Create a new RestoreRun to select again`.

If the repository Secret is missing, the check fails with `no repository Secret
<name> in this namespace`. A retry may fix some errors, such as a timeout from
the API server or a repository that the controller cannot open. If a check
fails with such an error, the phase stays empty. Ready gets reason Retrying with
the error as the message, and the controller tries again. If the run still
retries after its `timeout` from creation, or from `status.resumedAt` when it
waited Paused, it ends Failed with reason TimedOut.
Thus a wrong repository password shows in `kubectl get rrun` and ends the run.
If the same kind of error occurs while a volume item waits to start, the run
tries again and leaves the item as it was.

While a backup of a claim or its repository is in progress, the checks leave the
run unplanned. Ready is False with reason SourceBusy and the message of the
other run. If the run did not pass its checks by its creation (or its
`status.resumedAt` after a pause) plus `timeout`, it
ends Failed with reason TimedOut and `the run had not passed its checks by
<time>: <message>`.

### A database restore

The run marks the item Deleted and records the UID of the Cluster in
`status.items[].clusterUID`. Then it deletes the Cluster.
[architecture.md](architecture.md#a-database-restore) tells how the UID tells the
old Cluster apart from the recovered one. The run never deletes a Cluster that
has `backup.wlz.li/bootstrap: initdb`, as
[restores.md](restores.md#starting-a-database-empty) describes.

The delete of the Cluster only starts the shutdown of its instance pod.
Postgres does a smart shutdown. It refuses new connections and waits for open
sessions until the `smartShutdownTimeout` of the Cluster, 180 seconds by
default. On the Flux canary on 2026-09-25, the old pod was gone a little over
three minutes after the delete. While the pod runs, the Services of the Cluster
still send clients to it. Also, a new Cluster cannot take its pod and PVC names.

Thus the run waits with reason WaitingForShutdown until no pod or PVC with the
label `cnpg.io/cluster: <name>` stays in the namespace. Its message names the
pod or PVC that it waits for. After that, the run does these steps:

1. It gives the workloads that it stopped back.
2. It resumes the Kustomizations that it suspended.
3. It waits in phase Waiting until the Cluster is created again:

```text
recreate canary-namespace-backup-pg to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it
```

While the item is Deleted and nothing created the Cluster again, a BackupRun
with `all: true` in this namespace waits before it stops workloads. Thus the
Kustomization that creates the Cluster stays as Flux has it
([One quiesce at a time](#one-quiesce-at-a-time)).

When Flux or tofu creates the Cluster again, the bootstrap webhook finds the
run. It writes the moment of the run as the recovery target and names the run
on the Cluster:

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

The run follows the Cluster to `Cluster in healthy state`. The writer of the
canary continued to run through it. This is its report before and after:

```text
2026-09-24T22:22:33Z db=[18 2026-09-24T22:21:23Z,2026-09-24T22:22:24Z,2026-09-24T22:22:33Z] file=[6 2026-09-24T22:21:23Z,2026-09-24T22:22:24Z,2026-09-24T22:22:33Z]
2026-09-24T22:24:33Z db=[14 2026-09-24T22:17:47Z,2026-09-24T22:18:25Z,2026-09-24T22:24:33Z] file=[7 2026-09-24T22:22:24Z,2026-09-24T22:22:33Z,2026-09-24T22:24:33Z]
```

The rows from 22:20:25 to 22:22:33 are gone. The table ends at 22:18:25, the
last tick before the target, and new writes continue. While a run waits for a
Cluster, a Cluster that declares its own bootstrap method, such as `recovery` or
`pg_basebackup`, is refused. Thus the two sources cannot race.

The run leaves alone a Cluster that comes back without the recovery of this
run. The run skips or deletes again only the Cluster with the UID that the run
recorded. Any other Cluster with the name of the item fails the item:

```text
Cluster canary-namespace-backup-pg came back carrying backup.wlz.li/bootstrap: initdb after this run deleted it, so it started empty and nothing was restored. Remove the annotation from its manifest and create a new RestoreRun to recover it. The run leaves the Cluster alone
```

```text
Cluster canary-namespace-backup-pg (UID 3f2a1c7e-9d2b-4e6f-8a01-2c3d4e5f6071) came back without this run's recovery: it archives nowhere. The run does not delete a Cluster it did not recover
```

The reason after the colon names what the Cluster does instead:

- It archives nowhere.
- It has the opt-out annotation.
- It declares its own bootstrap method, such as `pg_basebackup`.
- `RestoreRun <name> recovered it`.
- The bootstrap webhook did not mark it.

If the Cluster came back with the opt-out annotation or with its own bootstrap,
the message says that it started empty or from that bootstrap. It also says that
nothing was restored.

An item can have no recorded `clusterUID`, from a run that found no Cluster when
it started. Such an item says that it cannot tell the old Cluster apart from a
new one. It leaves this Cluster alone too. While the run waits for the Cluster
to come back, a delete or a replacement of the Cluster fails the item. The
message is `the recovered Cluster was deleted` or `the recovered Cluster was
replaced by one this run did not recover`.

### Restore a whole namespace

This run used `all: true` with `restoreAsOf: "2026-09-24T22:16:30Z"`. The writer
was stopped, and then the canary was applied again to create the Cluster again.
This is the first report of the writer:

```text
start db=[10 2026-09-24T22:14:38Z,2026-09-24T22:15:38Z,2026-09-24T22:15:47Z] file=[9 2026-09-24T22:13:38Z,2026-09-24T22:14:38Z,2026-09-24T22:15:38Z]
```

The volume holds snapshot d1cb7739, whose clone VolSync cut at 22:15:41. The
database holds every row to 22:16:30. The row at 22:15:47 is in the database
and is not on the volume. By design, each side goes to its own newest point at
or before the moment.

### Restore a whole namespace to one moment

If you add `syncDatabaseToVolume: true` to a run with `all: true`, the databases
recover to the time on the snapshot of the volumes. The run stops the workloads
marked `backup.wlz.li/quiesce` and the ones under `quiesce`, and gives them
back itself. This is
the run used on the prod canary on v0.7.0:

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: synced
  namespace: canary-namespace-backup
spec:
  all: true
  syncDatabaseToVolume: true
  quiesce:
    - kind: Deployment
      name: canary-namespace-backup
```

1. At its checks, the run keeps only the snapshots with the tag `quiesced`. It
   selects the newest at or before `restoreAsOf`, or the newest of all without
   `restoreAsOf`. It records the time of that snapshot as `status.syncedTo`.
   The run fails in these conditions:
   - A claim has no such snapshot. The message is `the repository holds no
     snapshot tagged quiesced; only a BackupRun that stopped the workloads
     writes one`.
   - The snapshots of two claims have different times.
   - The namespace does not hold a listed workload.
2. It suspends the Flux Kustomization of each workload that it stops, by
   the rule in [Which Kustomization a run suspends](#which-kustomization-a-run-suspends).
   It scales the workload to zero and waits until no pod of it is left.
3. It restores each volume from the quiesced snapshot that it selected, whose
   time is `syncedTo`. A restore Job restores that snapshot by its full ID.
4. It deletes each Cluster and waits until the instance pods and PVCs of the
   Cluster are gone, as [A database restore](#a-database-restore) describes.
   Then it gives the workloads their replicas back and resumes the
   Kustomizations that it suspended. A suspended Kustomization would never
   create the Cluster again. When the run resumes it, the Kustomization also
   applies the replicas again. The app starts before the new Cluster is ready.
   It cannot connect to its database until the Cluster is ready.
5. When the Cluster is created again, the webhook sets its `targetTime` to
   `syncedTo`.

This is the status of the run while it waited for the apply of the canary.
`kubectl get restorerun synced -o jsonpath=...` printed it with a label on each
field:

```text
Waiting syncedTo=2026-09-25T12:30:46Z quiescedAt=2026-09-25T12:31:22Z restartedAt=2026-09-25T12:32:14Z
WaitingForRecreate: recreate canary-namespace-backup-pg to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it
PersistentVolumeClaim canary-namespace-backup-data Succeeded e36a856c
Cluster canary-namespace-backup-pg Deleted  20260925T123045
```

`syncedTo` is the `restartedAt` of the 12:30 scheduled run. These are the last
report of the writer before the restore, and its first report after it:

```text
2026-09-25T12:30:48Z db=[718 2026-09-25T12:28:52Z,2026-09-25T12:29:52Z,2026-09-25T12:30:48Z] file=[704 2026-09-25T12:28:52Z,2026-09-25T12:29:52Z,2026-09-25T12:30:48Z]
start db=[717 2026-09-25T12:27:52Z,2026-09-25T12:28:52Z,2026-09-25T12:29:52Z] file=[703 2026-09-25T12:27:52Z,2026-09-25T12:28:52Z,2026-09-25T12:29:52Z]
```

Both sides end at 12:29:52, the last tick before the 12:30 run stopped the
writer. The 12:30:48 tick is gone from both. The counts stay 14 apart. An
earlier automatic restore, which aligns nothing, brought this database back 14
ticks ahead of the volume. The writer got its replica back while the Cluster
still recovered. Its log shows connection errors until CloudNativePG reported
the Cluster healthy.

## Automatic restore

Two objects restore with no run: a claim that its VolumeRestore fills, and a
Cluster that Flux or tofu creates.

When the namespace has quiesced snapshots, both come back to one moment. The
populator and the bootstrap webhook read the same data: the snapshots of each
repository that a VolumeRestore of the namespace names. Both give it to one
function, `restic.SyncedMoment` (internal/restic/synced.go):

1. In each repository, it takes the newest snapshot with the tag `quiesced`
   and the layout of the mover. With `backup.wlz.li/restore-as-of`, it takes
   the newest at or before that moment. A repository with no such snapshot
   does not count, for example the repository of a claim added after the last
   quiesced run.
2. The time of these snapshots is the moment. A quiesced BackupRun gives all
   its snapshots one time, the `restartedAt` of the run.
3. The populator fills each claim from the quiesced snapshot of that moment
   (internal/populator/select.go `selectSnapshot`). A claim whose repository
   has no such snapshot gets the newest snapshot, as before.
4. The webhook recovers each Cluster to that moment
   (internal/bootstrap/synced.go `automaticTarget`). If no base backup
   finished by then, it refuses the Cluster, as it does for a pin.

The webhook uses the moment only when the volumes come back with the
Cluster. If a claim whose `dataSourceRef` names a VolumeRestore is Bound, the
volumes are live, and only the Cluster comes back. The populator library binds
such a claim only after its restore Job filled it, so a claim that is absent or
still Pending comes back with the Cluster. With a live volume, the webhook
recovers the Cluster to the end of its archive, or to its
`backup.wlz.li/restore-as-of`. A moment older than the live volumes would lose
database writes.

If two repositories give two different times, a quiesced run did not rewrite
every snapshot. Then nothing picks a moment. The claim stays Pending with reason
NoBackupInReach, and the webhook refuses the Cluster. Both messages name the two
repositories and their snapshots. Set `backup.wlz.li/restore-as-of` on the
claims and the Cluster to a moment that both repositories reach.

A namespace with no quiesced snapshot restores as the rest of this section
shows. This is the canary after a destroy and a new apply, recorded on v0.9.x,
before the rule above:

```text
start db=[13 2026-09-24T22:27:08Z,2026-09-24T22:28:14Z,2026-09-24T22:29:14Z] file=[10 2026-09-24T22:14:38Z,2026-09-24T22:15:38Z,2026-09-24T22:27:08Z]
```

The volume came back from the newest snapshot, afb3f27f. The database came back
to the end of its archive. The 22:30:14 row was not in the archive when the
canary was destroyed. `archive_timeout` sets the limit of that window.

With `backup.wlz.li/restore-as-of: "2026-09-24T22:16:30Z"` on the claim and the
Cluster, the same destroy and apply came back to that moment on both sides:

```text
start db=[10 2026-09-24T22:14:38Z,2026-09-24T22:15:38Z,2026-09-24T22:15:47Z] file=[9 2026-09-24T22:13:38Z,2026-09-24T22:14:38Z,2026-09-24T22:15:38Z]
```

The webhook refuses a Cluster pinned before every base backup. The apply of the
canary with `backup.wlz.li/restore-as-of: "2026-09-24T21:00:00Z"` failed with:

```text
admission webhook "bootstrap.backup.wlz.li" denied the request: annotation backup.wlz.li/restore-as-of asks for 2026-09-24T21:00:00Z, and no base backup in prod-cluster-backup-cnpg/canary-namespace-backup/canary-namespace-backup-pg/base/ finished by then; the oldest, 20260924T221544, finished at 2026-09-24T22:15:45Z.
```

A claim pinned before every snapshot stays Pending after a pod is scheduled for
it. Its VolumeRestore shows reason NoBackupInReach. The populator checks the own
`spec.restoreAsOf` of a VolumeRestore in the same way. The test of the populator
covers that. On the canary, the writer depends on the refused Cluster. Thus no
pod was scheduled, and the populator never ran.

The annotation also pins each later creation. Thus it raises the
`backup_controller_restore_pinned` series while it is set.

## Metrics

The manager serves these series on port 8081, next to the own listener of the
populator library on 8080:

| Series | Holds |
| --- | --- |
| backup_controller_namespace_last_success_timestamp_seconds | the namespace's newest successful run with all set, or its creation time |
| backup_controller_namespace_schedule_interval_seconds | seconds between two ticks of its schedule |
| backup_controller_namespace_schedule_invalid | 1 while the schedule does not parse |
| backup_controller_restore_pinned | 1 per claim or Cluster carrying backup.wlz.li/restore-as-of |

Each series names the namespace that it describes in its namespace label. Thus
a scrape must honour the labels of the target. For that reason, the PodMonitor
of the infrastructure repository sets honorLabels.

## Reading a restic repository

The restore checks and the snapshot time of each BackupRun read the files of the
repository itself through the S3 client, in internal/restic:

- The key file is opened with scrypt and the repository password.
- The snapshots are decrypted with AES-256-CTR and checked with Poly1305-AES.
- The zstd header of repository version 2 is handled.

A quiesced snapshot is written in the same way, in reverse. Its JSON goes in
uncompressed, which restic reads in repository versions 1 and 2. It is encrypted
under a random IV and stored under the SHA-256 of the encrypted file.

The lock follows internal/restic/lock.go of restic 0.18.1:

1. Check locks/ for other locks.
2. Write its own lock.
3. Wait 200 ms.
4. Check again.
5. On a conflict, remove its own lock.

A lock with the hostname and PID of the controller itself is a lock that it
failed to remove before. The next rewrite deletes it, because the restic `prune`
never skips a stale lock. The next rewrite also deletes each lock older than 30
minutes with the username `backup-controller`, as
[Quiesced snapshots](#quiesced-snapshots) describes.

The tests read a fixture that restic 0.19.1 wrote, and rewrite a copy of it. The
controller process runs no restic binary. Its restore Jobs run restic from the
image that the installer gives as `--restore-image`. In prod, that is the mover
image of VolSync.
