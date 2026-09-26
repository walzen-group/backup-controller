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
| backup.wlz.li/max-quiesce | Namespace | how long a run with `all: true` may keep the workloads it stopped stopped, a Go duration such as `20m`; 10m without it, counted from the run's `status.quiescedAt` |
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
mover or a CloudNativePG Backup still in progress keeps running. An item whose
start kept failing ends with the deadline message and its last error after it:
`<deadline message>; last error: <error>`.

The canary's first scheduled run, on a `*/15` schedule, sampled every three
seconds; lines repeating the one before are left out:

```text
22:15:03 phase=Queued replicas=1/1 clone=
22:15:12 phase=Waiting replicas=0/ clone=
22:15:41 phase=Waiting replicas=0/ clone=Pending
22:15:46 phase=Running replicas=1/1 clone=Bound
22:16:00 phase=Succeeded replicas=1/1 clone=
```

Its status afterwards, on v0.5.4, trimmed to the fields this page discusses. A
run on v0.6.0 or later also moves the snapshot to `restartedAt`, as
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

1. It creates one Kueue Workload, counted as one pod, in the namespace's
   LocalQueue, and waits for Admitted (phase Queued). The app keeps running
   while the run waits. It sets the Workload's PodsReady condition itself,
   because the Workload has no pods and Kueue's waitForPodsReady would evict it.
   A namespace with no LocalQueue starts at once.
2. It writes its plan into the status before it changes anything: every
   workload marked `backup.wlz.li/quiesce` in `status.quiesced`, with the
   replicas it has now, and the Flux Kustomizations to suspend in
   `status.suspendedKustomizations`. Before that write it takes the
   namespace's quiesce Lease and one Lease per planned Kustomization, and waits
   with reason SourceBusy while another run holds one, or while another
   unfinished run still owes this namespace's workloads their replicas
   ([One quiesce at a time](#one-quiesce-at-a-time)). Then it suspends those
   Kustomizations and scales the workloads to zero, and waits until none of
   their pods is left, terminating ones included: a pod shutting down can
   still write. A pod in phase Succeeded or Failed, such as an evicted one,
   does not count, because its containers have ended.
3. It writes each enabled claim's ReplicationSource with the run's manual tag,
   and creates a CloudNativePG Backup for each enabled Cluster. A Cluster
   carrying `cnpg.io/hibernation: "on"` is skipped.
4. Once every volume's clone is cut, it records `restartedAt` with
   `restartPending: true` and writes the status. Then it gives each workload its
   replicas back, resumes only the Kustomizations it suspended, and clears
   `restartPending`. The upload continues from the clone. A restart that fails
   is reported at once, with reason RestartFailed and a Warning event, and the
   pass runs again with controller-runtime's backoff. The message says that the
   run is still backing up and must not be deleted, and lists what to scale and
   resume by hand: `The run is still backing up; do not delete it. To give the
   app back now, scale Deployment notes to 2 and resume Kustomization
   flux-system/notes yourself; the run then goes on by itself.` A restart skips
   a workload that already stands at its recorded count and a Kustomization
   that is not suspended, so doing that by hand lets the run go on.
5. It reads the snapshot each mover logged, `snapshot d1cb7739 saved`, and the
   time restic stamped on it from the repository itself. When step 2 stopped a
   workload, it writes the snapshot again at `restartedAt`, as the next section
   describes. Then it deletes the Workload.

A finalizer performs step 4 and deletes the Workload on failure, on timeout and
when the run is deleted. A failure there is reported with reason RestartFailed
while the app is still down, and with reason ReleaseFailed when the app is back
and only a Lease or the Workload is left: `could not release the Leases it holds
on its claims and repositories: ... Fix the cause, or delete the Leases labelled
backup.wlz.li/lease-holder-uid=<uid> yourself; either way the run then finishes
by itself.` The Leases go before the Workload, so the run keeps its place in the
queue while it still owes the app its replicas. A Workload that resists
deletion is reported the same way: `could not delete its Kueue Workload
backuprun-<uid>, which holds the run's place in the queue: ...`, with the same
offer to delete it by hand. A run being deleted says that
the deletion completes by itself, and that a person can remove the finalizer
`backup.wlz.li/run-cleanup` if it does not once the app runs again. The canary's
writer was down 34 seconds, most of it
the pod's 30-second termination grace, because its shell loop does not handle
SIGTERM.

The run writes each plan before it acts on it, so a pass that stops the
workloads and then loses its status write is retried from the recorded plan.
Reading the workloads again at that point would find them at zero replicas, and
the run would give back nothing. For the same reason `restartedAt` is written
before the restart, and a pass that finds `restartPending` set repeats the
restart with the recorded moment. A run that v0.8.x stopped under the v0.7.2
BackupRun CRD, which stored no `restartPending`, still gives the app back: the
pass that records the moment restarts in any case. When a stop fails partway,
the run cuts the plan down to the workloads that stand at zero and the
Kustomizations that are suspended, starts those again, and ends Failed.
`quiescedAt` and `restartedAt` are whole seconds.

The workloads stay stopped for at most the namespace's
`backup.wlz.li/max-quiesce`, ten minutes without it, counted from
`quiescedAt`. When that runs out, the run gives the workloads back and fails
every volume item whose clone VolSync had not cut:

```text
VolSync had not cut the clone volsync-notes-data-src by 2026-09-26T09:41:00Z, when the backup.wlz.li/max-quiesce limit of 10m ran out and the workloads were given back
```

An item whose start never got that far is failed too, with `not started before
the workloads were given back at <time>, when the backup.wlz.li/max-quiesce
limit of 10m ran out, so the clone volsync-notes-data-src was never cut`
followed by its last start error when it has one. The rest of the run goes on,
the upload continues for every clone that was cut, and a stopped workload is
never held for longer than the limit by a VolSync that cuts no clone. An
annotation that is not a positive Go duration fails a run with `all: true`
before it stops anything: `namespace notes has backup.wlz.li/max-quiesce
"soon", which is not a duration such as 20m`. A value changed to one that no
longer parses while the app is down aborts the run, which gives the workloads
back.

A clone claim `volsync-<claim>-src` counts as cut in step 4 when it is Bound,
not being deleted, and was created after `quiescedAt`. VolSync marks a sync
done before its cleanup deletes the clone, so the previous sync's clone can
still be Bound when a run starts, holding the data from before the app stopped.
The app stays down until the run's own clone exists.

### Which Kustomization a run suspends

The kustomize-controller labels `kustomize.toolkit.fluxcd.io/name` and
`kustomize.toolkit.fluxcd.io/namespace` on a workload name the Kustomization
that applied it. The run suspends that Kustomization only when its
`status.inventory.entries` lists the workload, with the id
`<namespace>_<name>_apps_<Kind>`, such as `notes_notes_apps_Deployment`. Anyone
who can edit a workload can set its labels, and suspending a Kustomization is a
write in another namespace, so the labels alone are not enough. A workload
without the labels, whose Kustomization is gone, or whose Kustomization does
not list it, is scaled down with nothing suspended. A Kustomization that is
already suspended is left out, because the run did not suspend it and must not
resume it; one another run holds a quiesce Lease for makes this run wait
instead, even when this run would leave it out
([One quiesce at a time](#one-quiesce-at-a-time)). A failed read of a
Kustomization is retried at the next pass, and a
version Flux stops serving is never read as a deleted Kustomization: the run
reports `the API server no longer serves Kustomization at kustomize.toolkit.fluxcd.io/v1 (...);
the controller looks up the served version again and retries, and a restart of
the controller also clears the versions it has cached`, relearns what the API
server serves, and retries. The kind is resolved by group and kind, so a Flux
upgrade that serves the Kustomization at another version changes nothing here,
for a BackupRun and a RestoreRun alike.

### Quiesced snapshots

restic stamps a snapshot with the moment `restic backup` starts. The mover
starts it once the clone is Bound, which is when step 4 gives the app its
replicas back, so the stamp can fall after the app has written again, and a
database recovered to it would hold rows the volume lacks.

A run that stopped workloads therefore writes each volume's snapshot again once
the mover is done. The new snapshot carries the run's `restartedAt` as its time
and the tag `quiesced`, and names the snapshot the mover wrote as its
`original`; the controller then deletes that one. `restic rewrite --forget
--new-time` makes the same change, and `restic tag --add` the tag. Nothing
wrote between the last pod stopping and `restartedAt`: the controller reads the
clock at the start of the reconcile that gives the replicas back, drops the
fraction of a second, and writes that moment to the status before it scales
anything up. That reconcile runs after the one that found the pods gone. The
rewritten snapshot carries the same whole-second time, which is the precision
VolSync's mover compares at. Under v0.7.x and earlier the rewritten snapshot
kept the fraction, and the status showed it cut to the second.

The 11:45 scheduled run on the prod canary, on v0.6.0. Its mover logged, in the
ReplicationSource's `status.latestMoverStatus.logs`:

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

The controller holds restic's exclusive lock while it writes, and restic's own
`prune` holds the same one. Any other lock in locks/ keeps the rewrite waiting,
shared ones included, apart from a lock older than restic's 30-minute stale
limit. The controller deletes a stale lock whose username is
`backup-controller`, whatever host and PID it names: a controller pod replaced
before it removed its lock leaves one with a hostname no later pod has, and
that lock would stop every prune, and as an exclusive lock every backup, since
VolSync runs `restic unlock` only when `spec.restic.unlock` changes. The
controller tries three times, a second apart, to remove its own lock after a
rewrite. While another process's lock is held, the volume's item stays Running with a message naming
the lock's host, and the run tries again at its next poll, so it never reports
Succeeded for a snapshot left untagged.

### Sources the controller writes

A source is owned by its claim and named after it. The mover is placed by the
PersistentVolume's own node affinity, so it runs whether or not the app's pod
does, and it carries no queue label, since the run's Workload already admitted
it. A run writes the source's spec only when it finds the source idle: one that
already carries the run's tag is left as it is, so a change to the retention or
the affinities reaches the source on the next run that finds it idle, which can
be a later run than the one that met it busy. The canary's source after the
22:45 scheduled run, trimmed to spec and the status fields that show it idle:

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
shows. Every tag the controller writes sets `spec.restic.unlock` to the same
value, so VolSync runs `restic unlock` before that backup. restic removes only
the locks it counts as stale, which is 30 minutes for a lock from another host,
so a lock a killed mover left behind clears on the next run by itself.

A run that finds a
source still completing another run's tag waits for it, with reason SourceBusy
and a message naming that run: `ReplicationSource
canary-namespace-backup-data is still completing the backup of BackupRun
scheduled-20260924-2245`. A source VolSync started syncing with no tag at all,
which a run from an older version can leave behind, waits the same way with
`ReplicationSource canary-namespace-backup-data is still syncing; VolSync has
not recorded the end of its last sync`. A
source of the same name the controller did not write is left alone, and the
item fails naming it.

A mover Job that fails leaves the tag open. VolSync writes the Job's logs into
`status.latestMoverStatus`, deletes the Job and starts another, for as long as
the tag stays. When the source still carries the run's tag, has not completed
it, and reports a Failed mover in a sync VolSync started after the run's
`startedAt`, the run fails the item with the mover's logs, so the run reports
the failure at once instead of at its timeout. It leaves the ReplicationSource
alone. A mover that failed on a lock is reported with the explanation first:
`The restic repository is locked (repository is already locked): a lock that
another restic process holds or left behind, such as one of a mover that was
killed, keeps restic from taking the lock it needs. ... The next backup does so
by itself once the lock is older than 30 minutes. Mover logs: ...`

A later run waits only for a tag some run still needs. When the tag's run
failed, timed out or was deleted, or the tag was written by v0.7.x or v0.8.x, no
run waits for it any more, and VolSync would complete the new trigger with that
older backup. The run then fails that item at once and goes on with the rest of
the namespace, naming the run or the trigger the tag belongs to, what VolSync is
doing with it, and how to give that backup up: `To give that backup up, delete
the ReplicationSource canary-namespace-backup-data while no pod of Job
volsync-src-canary-namespace-backup-data is running; the next backup unlocks the
repository first.` The source is left as it is, and the item goes on failing
that way until the source is deleted. While a run waits for a live run's tag, a
quiesced namespace's later runs back up nothing, because a run checks every
source before it stops the app.

An item the run fails while VolSync keeps retrying names the data a snapshot of
that sync holds. VolSync retries the sync with the clone it cut when the sync
started, and restic stamps a retry's snapshot with the retry's time, so that
snapshot can hold data older than its time. The message ends `VolSync keeps
retrying the sync it started at 2026-09-26T09:41:00Z with the clone it cut then,
so a snapshot this sync saves later holds the data of 2026-09-26T09:41:00Z,
whatever time restic stamps on it.` When VolSync has not cut the clone yet, it
reads `VolSync goes on with the sync it started at 2026-09-26T09:41:00Z and has
not cut its clone yet, so a snapshot this sync saves later holds the data of the
moment it cuts the clone, whatever time restic stamps on it.` Such a snapshot
carries no `quiesced` tag, so a `syncDatabaseToVolume` restore never chooses it;
a plain `restoreAsOf` may, and the volume then holds the data VolSync captured
for that claim, stamped later than the data is from.

v0.8.0 and v0.8.1 deleted the ReplicationSource at this point. By the time
VolSync reports the failure it has already started the next mover Job, so the
delete killed that mover mid-backup. The mover's PID 1 is bash, which ignores
SIGTERM, so restic died by SIGKILL and left its lock in the repository, and
every later `restic forget` of that claim failed until someone ran
`restic unlock`. v0.8.2 removed the delete.

When you upgrade to v0.8.2, run `restic unlock` once against the repository of
every claim that had a mover failure under v0.8.0 or v0.8.1 (`restic list locks`
shows whether a repository holds one). Without it, each retry saves a snapshot,
fails at `forget` with `repository is already locked`, and VolSync retries
again, so the repository grows by several hundred snapshots a day that are
never forgotten. A plain `restic unlock` removes only locks older than 30
minutes, which a lock left under v0.8.1 is by the time you run it. A source
still carrying such a tag, whose retries fail on that lock, never finishes on
its own: under v0.9.0 every later run fails that item at once with the message
above, and the way out is the one the message names, delete the
ReplicationSource while no pod of its Job runs, after which the next backup
unlocks the repository.

## Back up now

| Run | Backs up | Measured |
| --- | --- | --- |
| `source: <claim>` | one volume | snapshot defb7a4d, stamped 22:09:58, in 21 seconds |
| `database: <cluster>` | one database | Backup canary-namespace-backup-pg-11ec704d |
| `all: true` | everything enabled, with quiesce | snapshot afb3f27f and a Backup, the writer down from 22:27:37 to 22:28:12 |

A CEL rule on the CRD accepts exactly one of the three. The claim or Cluster
has to carry `backup.wlz.li/enabled: "true"`.

A BackupRun ends with reason Invalid only for a spec no retry can fix: a named
claim or Cluster that is missing or not marked, or nothing marked in the
namespace. A run whose installed CRD lacks a field the controller writes ends
before it changes anything, with reason CRDOutdated; [architecture.md](architecture.md#objects-the-controller-writes)
has the check. Once the run has its items, an item fails only for what the item
itself holds: `the claim <name> no longer exists`, `claim <name> is bound to
the PersistentVolume <pv>, which does not exist`, a claim's retain- annotations
that don't parse, a source holding a tag no run waits for
([Sources the controller writes](#sources-the-controller-writes)), or a source
or Backup the API server rejects as invalid.

A start that fails with an error a retry may fix, such as a CloudNativePG
webhook that cannot be reached, no longer ends the pass. The item stays Pending
with `not started yet: <error>`, the Ready condition takes reason Retrying with
a message that names every such item and every wait, `retrying the start of
notes-pg: <error>; ReplicationSource notes-data is still completing the backup
of BackupRun scheduled-20260926-0900`, and the run keeps working: the quiesced
workloads still come back once the clones are cut, and the item is tried again
on every pass until the run's timeout. Without a retry, a run waiting on another
run shows reason SourceBusy and names every wait in its message. A timeout or a
5xx from the API server while the run plans, stops workloads or starts an item
is returned, and the next reconcile tries again.

A cluster without the CloudNativePG CRDs holds no Clusters. The scheduler and
every run with `all: true` find none there, and a BackupRun with
`database: <name>` ends Invalid with `no Cluster <name> in this namespace; the
cluster has no CloudNativePG CRDs`.

## One mover at a time

A backup and a restore of the same claim or repository never run at once. Each
run takes a `coordination.k8s.io` Lease for the claim and one for the
repository right before it creates its mover object, the claim's named
`backup-controller-claim-<claim uid>` and the repository's
`backup-controller-repo-<secret uid>`. Creating a Lease is atomic, so of two
runs that reach that moment in the same instant the API server admits one, and
the other waits. The Lease is written in the run's namespace, names its holder
in `backup.wlz.li/lease-holder-kind` (BackupRun or RestoreRun),
`backup.wlz.li/lease-holder-uid` and `backup.wlz.li/lease-holder-name`, and
lists the holder's items in `backup.wlz.li/lease-items`. A run releases its
Leases once an item finishes, and takes over a Lease whose holder has finished
or no longer exists. A Lease counts as held while the holder's item of the kind
that takes it is Pending or Running, so a Cluster item that happens to share a
claim's name does not keep that claim's Lease.

A run that finds another run holding either Lease waits with reason SourceBusy:

```text
BackupRun scheduled-20260926-0900 holds Lease backup-controller-claim-3f2a1c7e-9d2b for notes-data; this run starts once that run has finished with it
```

A RestoreRun that started before the Leases existed is found through its mover
object instead, and a BackupRun reports that hold as well: `RestoreRun
notes-back-to-friday is restoring claim notes-data from repository
notes-restic-data with ReplicationDestination restore-3f2a1c7e; this run starts
once that restore has finished`. An `into` restore still on the populator path,
one a v0.8.1 controller planned, is found through its VolumeRestore instead:
`RestoreRun notes-back-to-friday is restoring the backups of claim notes-data
from repository notes-restic-data into claim notes-back-to-friday through
VolumeRestore notes-back-to-friday; this run starts once that restore has
finished`. A namespace run checks every claim this way, and for the Lease of the
claim and its repository, before it stops anything, and a read that fails there
comes back as an error and stops nothing: an app is never stopped for a backup
that then waits, or on a read that could not be made.

A claim or a repository Secret that does not exist takes no Lease: the reads
that resolve the Lease names treat NotFound as no Lease and every other
failure as an error, which is retried with nothing stopped. The item goes on
and reports the missing object itself: a claim that is gone fails the backup's
item with `the claim <name> no longer exists`, and a repository Secret that is
gone fails the mover, whose failure the run reports with the mover's logs. The
Lease of an item that has finished is released on the run's next pass, and that
release is best effort: a failure is logged and the run goes on, so it never
holds the app past the quiesce limit, and the run releases every claim and
repository Lease it still holds when it finishes. The quiesce Leases go as
[One quiesce at a time](#one-quiesce-at-a-time) describes.

## One quiesce at a time

Two runs never stop one namespace's workloads at once. A run that is about to
write a stop plan first takes the `coordination.k8s.io` Lease
`backup-controller-quiesce` in the run's own namespace, and one Lease per Flux
Kustomization its plan needs, named
`backup-controller-kustomization-<Kustomization uid>` in the Kustomization's
namespace. Only a BackupRun with `all: true` that has at least one target takes
the namespace's Lease, and only a RestoreRun whose `spec.quiesce` lists a
workload takes one; a run with `source:` or `database:` takes none, and neither
does one that already has a plan in `status.quiesced`, which is what lets a run
a v0.8.x controller left in flight finish without a Lease.

The run takes the Kustomization Leases after the namespace's Lease and in
`namespace/name` order, for every Kustomization whose `status.inventory.entries`
lists one of its targets, and on that pass it releases the Kustomization Leases
it holds that the set no longer names. A Kustomization another run holds makes
this run wait even when this run would leave it out because it is already
suspended, which is the fail-closed direction: the workloads this run needs
stopped stay up until the holder has resumed it.

The Leases carry the holder labels the claim and repository Leases carry
(`backup.wlz.li/lease-holder-kind`, `backup.wlz.li/lease-holder-uid` and
`app.kubernetes.io/managed-by: backup-controller`) and, in
`backup.wlz.li/lease-scope: quiesce`, the mark that tells them apart. The
holder's name and namespace are in the `backup.wlz.li/lease-holder-name` and
`backup.wlz.li/lease-holder-namespace` annotations, because a Kustomization
Lease lives in the Kustomization's namespace, and `spec.holderIdentity` holds
the holder's UID. They carry no ownerReferences. A Lease whose holder has
finished, is gone, or has stored the restart that gave the workloads back is
stale, and the next run takes it over with an update carrying the
resourceVersion it read.

A run that finds another run holding the namespace's Lease waits with reason
SourceBusy:

```text
BackupRun scheduled-20260926-0300 has stopped the workloads of this namespace (Lease backup-controller-quiesce); this run stops them once that run has given them back
```

A run about to stop workloads also waits for another unfinished run of its
namespace whose plan is not given back, which covers a run a v0.8.x controller
left without a Lease. The message names the workload that run mentions first,
and says `stopped this namespace's workloads and has not given them back yet`
when its plan names none:

```text
RestoreRun back-to-monday stopped Deployment notes and has not given it back yet; this run stops the workloads once that run has given them back
```

A run about to stop workloads waits while an unfinished RestoreRun in the
namespace has a Cluster item in phase Deleted as well, because the
Kustomization this run would suspend may be the one Flux needs to create that
Cluster again:

```text
RestoreRun notes-back-to-friday has deleted Cluster notes-pg and waits for it to be created again; this run stops the workloads once that Cluster is back
```

A Kustomization Lease another run holds is reported the same way, naming the
workload this run needs it for:

```text
BackupRun wiki/scheduled-20260926-0300 has suspended Kustomization flux-system/apps, which also applies Deployment notes; this run stops its workloads once that run has resumed it
```

A run that runs out of time while it waits ends with nothing stopped, and the
message names the wait:

```text
the run had not finished by 2026-09-26T10:00:00Z; it was waiting: RestoreRun back-to-monday stopped Deployment notes and has not given it back yet; this run stops the workloads once that run has given them back
```

The quiesce Leases are not released with the claim and repository Leases. They
go on the pass that finds the stored status showing every workload back and
the plan reading back, in `finish` after the terminal status write, and in
`finalize` after the finalizer is dropped. Each release is best effort: a
failure is logged and the run goes on, and a Lease left behind is stale under
the rule above and is taken over by the next run that wants it. Deleting one
by hand while its holder is still running reopens the bug the Leases prevent:
the next run takes the Lease, records the zero replicas the holder stopped,
and both runs can end with the app still at zero.

A run a v0.8.x controller left in flight with a recorded plan holds no Lease
and continues without one, and a new run waits for it through the status check
above; one that has not planned yet takes the Lease on its first pass under
v0.9. During the controller upgrade itself, an old pod that is still
terminating takes no Lease either, so wait until it is gone before creating
runs in a namespace. A run that finds the workloads already stopped records the
counts it finds.

## Restore

| Run | Restores |
| --- | --- |
| `claim: <claim>` | one volume, in place, once nothing mounts it |
| `claim:` with `into: <new claim>` | one volume into a new claim; the app keeps running |
| `repository:` with `into: <new claim>` and `intoSize` | a repository no claim here owns, into a new claim |
| `database: <cluster>` | one database |
| `all: true` | every enabled volume in place, then every enabled database |

Each accepts `restoreAsOf`. A volume restores the newest snapshot taken at or
before it, and a database replays WAL to it exactly. Left out, a volume restores
its newest snapshot and a database the end of its WAL archive.

A run with `all: true` also accepts `syncDatabaseToVolume`, which recovers the
databases to the moment the volumes' snapshot holds. Every run except an `into`
restore accepts `quiesce`, a list of workloads the run stops while it restores.
The `backup.wlz.li/quiesce` annotation plays no part in a restore. [Restore a
whole namespace to one moment](#restore-a-whole-namespace-to-one-moment) uses
both.

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

The run then checks that the snapshot it selected is one the mover will restore
when it is pinned to that snapshot's second, and refuses the run with reason
NoBackupInReach, before anything is created, when it is not:

```text
snapshot 6e473100 (2026-09-26T09:03:53Z) shares its second with snapshot 2edf5bab, and VolSync's mover picks by the whole second, so it would restore 2edf5bab. Choose 2edf5bab or a snapshot in another second
```

[restores.md](restores.md#which-snapshot-a-run-restores) has every case,
including the one a `syncDatabaseToVolume` run adds. Right before the run
creates a destination, it lists the repository once more: a snapshot a backup's
retention removed after the checks, a snapshot that backup rewrote at another
time, or one the mover would now pick instead, fails the item with the cause
and `. Nothing was written to claim <claim>. Create a new RestoreRun to select
again`.

A missing repository Secret fails the check with `no repository Secret <name>
in this namespace`. A check that fails with an error a retry may fix, such as a
timeout from the API server or a repository the controller can't open, leaves
the phase empty and sets Ready to reason Retrying with the error as the
message, and the controller tries again. A run still retrying once its
`timeout` has passed since it was created ends Failed with reason TimedOut, so
a wrong repository password shows up in `kubectl get rrun` and ends the run.
The same kind of error while a volume item waits to start is retried with the
item left as it was. While a backup of a claim or its repository is in
progress, the checks leave the run unplanned and Ready False with reason
SourceBusy and the other run's message; a run that has still not passed its
checks by its creation plus `timeout` ends Failed with reason TimedOut and
`the run had not passed its checks by <time>: <message>`.

### A database restore

The run marks the item Deleted and records the Cluster's UID in
`status.items[].clusterUID`, then deletes the Cluster;
[architecture.md](architecture.md#a-database-restore) says how the UID tells the
old Cluster from the recovered one. A Cluster carrying
`backup.wlz.li/bootstrap: initdb` is never deleted, as
[restores.md](restores.md#starting-a-database-empty) describes. Deleting the Cluster
only starts its instance pod's shutdown: Postgres does a smart shutdown, which
refuses new connections and waits for open sessions until the Cluster's
`smartShutdownTimeout`, 180 seconds by default. On the Flux canary on
2026-09-25 the old pod was gone a little over three minutes after the delete. While it runs, the Cluster's
Services still send clients to it, and a new Cluster cannot take its pod and
PVC names.

So the run waits, with reason WaitingForShutdown, until no pod or PVC labelled
`cnpg.io/cluster: <name>` is left in the namespace. Its message names the one
it is waiting for. Only then does it give the workloads under `quiesce` back,
resume the Kustomizations it suspended, and wait in phase Waiting for the
Cluster to be created again:

```text
recreate canary-namespace-backup-pg to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it
```

While the item stands Deleted and the Cluster has not been created again, a
BackupRun with `all: true` in this namespace waits before it stops workloads,
so the Kustomization that creates the Cluster stays as Flux has it
([One quiesce at a time](#one-quiesce-at-a-time)).

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
its own bootstrap method, such as `recovery` or `pg_basebackup`, while a run
waits for it is refused, so the two sources cannot race.

A Cluster that comes back without this run's recovery is left alone. Only the
Cluster carrying the UID the run recorded is skipped or deleted again; any
other Cluster of the item's name fails the item:

```text
Cluster canary-namespace-backup-pg came back carrying backup.wlz.li/bootstrap: initdb after this run deleted it, so it started empty and nothing was restored. Remove the annotation from its manifest and create a new RestoreRun to recover it. The run leaves the Cluster alone
```

```text
Cluster canary-namespace-backup-pg (UID 3f2a1c7e-9d2b-4e6f-8a01-2c3d4e5f6071) came back without this run's recovery: it archives nowhere. The run does not delete a Cluster it did not recover
```

The reason after the colon names what the Cluster is doing instead: it archives
nowhere, it carries the opt-out annotation, it declares its own bootstrap
method such as `pg_basebackup`, `RestoreRun <name> recovered it`, or the
bootstrap webhook did not mark it. A Cluster that came back carrying the
opt-out annotation or declaring its own bootstrap says that it started empty or
from that bootstrap and that nothing was restored. An item with no recorded
`clusterUID`, from a run that started under v0.7.2 or found no Cluster when it
started, says that it cannot tell the old Cluster from a new one, and leaves
this one alone too. While the run waits for the Cluster to be created again, a
Cluster that is deleted or replaced fails the item with `the recovered Cluster
was deleted` or `the recovered Cluster was replaced by one this run did not
recover`.

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

### Restore a whole namespace to one moment

Add `syncDatabaseToVolume: true` to a run with `all: true`, and the databases
recover to the time on the volumes' snapshot. Add the app's workloads under
`quiesce`, and the run stops them and gives them back itself. The run used on
the prod canary on v0.7.0:

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

1. At its checks the run keeps only the snapshots tagged `quiesced`, selects the
   newest at or before `restoreAsOf` (the newest of all without one), and
   records its time as `status.syncedTo`. A claim with no such snapshot fails
   the run with `the repository holds no snapshot tagged quiesced; only a
   BackupRun that stopped the workloads writes one`, and so do two claims whose
   snapshots carry different times. A listed workload the namespace does not
   hold fails it too.
2. It suspends each listed workload's Flux Kustomization, by the rule in
   [Which Kustomization a run suspends](#which-kustomization-a-run-suspends),
   scales the workload to zero, and waits until no pod of it is left.
3. It restores each volume with `restoreAsOf` set to the time of the quiesced
   snapshot it selected, which is `syncedTo`, so the mover selects that
   snapshot.
4. It deletes each Cluster and waits until the Cluster's instance pods and
   PVCs are gone, as [A database restore](#a-database-restore) describes. Then
   it gives the workloads their replicas back and resumes the Kustomizations it
   suspended. A suspended Kustomization would never create the Cluster again,
   and resuming it reapplies the replicas as well. The app starts before the
   new Cluster is ready, and can't connect to its database until it is.
5. When the Cluster is created again, the webhook sets its `targetTime` to
   `syncedTo`.

The run's status while it waited for the canary's apply, printed by `kubectl
get restorerun synced -o jsonpath=...` with each field labelled:

```text
Waiting syncedTo=2026-09-25T12:30:46Z quiescedAt=2026-09-25T12:31:22Z restartedAt=2026-09-25T12:32:14Z
WaitingForRecreate: recreate canary-namespace-backup-pg to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it
PersistentVolumeClaim canary-namespace-backup-data Succeeded e36a856c
Cluster canary-namespace-backup-pg Deleted  20260925T123045
```

`syncedTo` is the 12:30 scheduled run's `restartedAt`. The writer's last report
before the restore, and its first after it:

```text
2026-09-25T12:30:48Z db=[718 2026-09-25T12:28:52Z,2026-09-25T12:29:52Z,2026-09-25T12:30:48Z] file=[704 2026-09-25T12:28:52Z,2026-09-25T12:29:52Z,2026-09-25T12:30:48Z]
start db=[717 2026-09-25T12:27:52Z,2026-09-25T12:28:52Z,2026-09-25T12:29:52Z] file=[703 2026-09-25T12:27:52Z,2026-09-25T12:28:52Z,2026-09-25T12:29:52Z]
```

Both sides end at 12:29:52, the last tick before the 12:30 run stopped the
writer, and the 12:30:48 tick is gone from both. The counts stay 14 apart
because an earlier automatic restore, which aligns nothing, brought this
database back 14 ticks ahead of the volume. The writer got its replica back
while the Cluster was still recovering, and its log shows connection errors
until CloudNativePG reported the Cluster healthy.

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
on its VolumeRestore, once a pod is scheduled for it. The populator
checks a VolumeRestore's own `spec.restoreAsOf` the same way. The populator's test
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
with Poly1305-AES, and repository version 2's zstd header handled.

A quiesced snapshot is written the same way in reverse: its JSON goes in
uncompressed, which restic reads in repository versions 1 and 2, encrypted under
a random IV and stored under the SHA-256 of the encrypted file. The lock follows
restic 0.18.1's internal/restic/lock.go: check locks/ for other locks, write
its own, wait 200 ms, check again, and remove its own on a conflict. A lock
carrying the controller's own hostname and PID is one it failed to remove
earlier, and the next rewrite deletes it, because restic's `prune` never skips
a stale lock. The next rewrite also deletes any lock older than 30 minutes
that carries the username `backup-controller`, as
[Quiesced snapshots](#quiesced-snapshots) describes.

The tests read, and rewrite a copy of, a fixture restic 0.19.1 wrote. The
controller runs no restic binary and pins no image beside VolSync's.
