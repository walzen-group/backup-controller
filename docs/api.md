# API

Three kinds, all `backup.wlz.li/v1alpha1` and namespaced. The annotations a
namespace declares its backups with are in
[namespace-backups.md](namespace-backups.md), and which restore to reach for is
in [restores.md](restores.md).

| Kind | Short name | Is |
| --- | --- | --- |
| VolumeRestore | `vrestore` | a standing declaration of where a volume's backups live, named by the claim's `dataSourceRef` |
| BackupRun | `brun` | one backup now, of a volume, a database or the namespace, and the record of every scheduled one |
| RestoreRun | `rrun` | one restore, of a volume, a database or the namespace, to the newest backup or a chosen moment |

The group matches the convention kuport set with `kuport.wlz.li`. v0.1.0 shipped
the VolumeRestore CRD under that group, so changing the group or a kind now
changes every claim in the infrastructure repository with it.

## VolumeRestore

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: notes-data
  namespace: notes
spec:
  # The Secret in this namespace holding the restic repository URL, its
  # password and the object store keys.
  repository: notes-restic-data

  # Optional. Restore the newest snapshot taken at or before this time, in
  # RFC3339. Left out, the newest snapshot in the repository is used.
  restoreAsOf: "2026-09-13T00:00:00Z"

  # Optional, and set it. VolSync's mover provisions a metadata cache claim
  # for every backup and restore. Left out, that claim comes from the
  # cluster's default storage class, and a default whose reclaim policy is
  # Retain keeps the cache dataset after the run that made it.
  cacheStorageClassName: zfs-ephemeral

  # Optional. The size of that cache claim. Left out, VolSync picks its own
  # default.
  cacheCapacity: 2Gi

  # Optional. Labels put on a restore's mover pod, so the restore is admitted
  # by the cluster's backup queue.
  moverPodLabels:
    kueue.x-k8s.io/queue-name: backups

  # Optional. The user the movers run as, for an app whose files only that
  # user can read.
  moverSecurityContext:
    runAsUser: 26
    runAsGroup: 26
    fsGroup: 26
```

The populator reads it when a claim naming it is created, and every BackupRun
and RestoreRun reads it for the claim it belongs to. A claim with no
`dataSourceRef`, bound to a volume by name, is described by the VolumeRestore
carrying the claim's own name.

| Field | Required | Reaches |
| --- | --- | --- |
| `repository` | yes | `spec.restic.repository` of every source and destination for the claim; the populator copies the Secret into its own namespace first |
| `restoreAsOf` | no | the populator's ReplicationDestination `spec.restic.restoreAsOf`, unless the claim carries `backup.wlz.li/restore-as-of`, which wins |
| `cacheStorageClassName` | no | `cacheStorageClassName` of every source and destination, and the source's clone class |
| `cacheCapacity` | no | `cacheCapacity` of the claim's ReplicationSource, of the populator's destination, and of the destination of a RestoreRun that names the claim in `claim`; unset, VolSync makes the cache 1Gi |
| `moverPodLabels` | no | the restore destinations' `moverPodLabels`; a source carries none, because its run was admitted as a whole |
| `moverSecurityContext` | no | `moverSecurityContext` of every source and destination |

Add a field only when a VolSync field has to be reachable from a claim, and
name it after the field it reaches.

### Status

```yaml
status:
  conditions:
    - type: Ready
      status: "False"
      reason: Restoring
      message: waiting for ReplicationDestination restore-3f2a1c7e in backup-system
  claims:
    - name: notes-data
      uid: 3f2a1c7e-...
      phase: Restoring
      startedAt: "2026-09-14T09:12:03Z"
```

| Field | Holds |
| --- | --- |
| `conditions[type=Ready]` | False while any claim naming this object is being filled, True when none is |
| `claims[]` | one entry per claim currently being filled from this object, with the phase, Restoring or Failed, and when it started |

A finished fill leaves no entry. What a reader wants from the status is whether
something is happening now and where to look.

| Ready reason | When |
| --- | --- |
| Restoring | claims are being filled and none of them has failed; the message lists the ReplicationDestination of every claim in `status.claims`, in that order, and the controller's namespace, for example `waiting for ReplicationDestinations restore-3f2a1c7e, restore-9b04d2aa in backup-system` |
| RestoreFailed | a claim's mover failed; the message is `claim <name>: ` followed by the mover's logs |
| NoBackupInReach | the moment the claim's `backup.wlz.li/restore-as-of` or `spec.restoreAsOf` asks for is older than every snapshot; the message is `claim <name>: ` followed by the reason, which names where the moment came from. The claim stays Pending, and the controller creates no destination |
| Restored | no claim is being filled |

The populator sets Ready from every entry in `status.claims`, so all the claims
that fill from one VolumeRestore report the same condition. While any entry is
Failed, a pass for another claim leaves Ready as it is. A RestoreFailed or
NoBackupInReach therefore stays on the VolumeRestore while its other claims
restore or finish. RestoreFailed also stays while VolSync retries the failed
claim's mover, until a sync succeeds and the claim's entry goes back to
Restoring.

While any claim is listed in `status.claims`, the VolumeRestore carries the
finalizer `backup.wlz.li/volume-populator`. The library calls the populator for
a claim only once the claim's prime claim, `prime-<uid>` in the controller's
namespace, is bound. On that first call the populator adds the finalizer,
before it copies the Secret or creates the destination, and it removes the
finalizer once `status.claims` is empty. A VolumeRestore deleted mid-restore
therefore stays Terminating until its claims finish or are deleted. Each
claim's cleanup deletes that claim's ReplicationDestination and Secret copy, and
the cleanup that empties `status.claims` removes the finalizer, which lets the
deletion finish. A VolumeRestore that is already being deleted without the
finalizer starts no new restore.

Until the prime claim binds, the VolumeRestore carries no finalizer, and a
delete removes it at once. The library looks a claim's VolumeRestore up before
it cleans up after the claim, and it stops when the VolumeRestore is gone. A
claim deleted after its VolumeRestore would then keep the library's finalizer
`backup.wlz.li/populate-target-protection` and stay Terminating. The
controller's orphan reconciler releases such a claim;
[architecture.md](architecture.md#a-claim-whose-volumerestore-is-gone) lists its
steps and the events it records on the claim.

### Validation the CRD carries

| Rule | Why |
| --- | --- |
| `repository` is a required, non-empty DNS-1123 name | a missing repository leaves claims Pending with nothing to read |
| `restoreAsOf` is an RFC 3339 date-time when present | VolSync rejects it later and less clearly |
| `cacheStorageClassName` is a non-empty name of at most 253 characters when present | an empty string reaches VolSync as a class name no provisioner answers, and the mover's cache claim stays Pending |
| `moverPodLabels` holds at most 8 entries, each with label syntax | the API server installs the label rules only for a map whose size is declared |

## BackupRun

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: before-upgrade
  namespace: notes
spec:
  all: true
```

| Field | Required | Holds |
| --- | --- | --- |
| `source` | one of the three | the claim to back up, which has to carry `backup.wlz.li/enabled: "true"`; its ReplicationSource has the same name |
| `database` | one of the three | the CloudNativePG Cluster to take a base backup of |
| `all` | one of the three | every enabled claim and Cluster in the namespace, with the workloads marked `backup.wlz.li/quiesce` stopped until the clones are cut, one run at a time in the namespace, and for at most the namespace's `backup.wlz.li/max-quiesce`, ten minutes by default |
| `timeout` | no | how long the run may work once admitted, e.g. `10h`. Omitted, the namespace's `backup.wlz.li/timeout`, and 6h without one |
| `ttlSecondsAfterFinished` | no | delete the run that long after it finishes. Omitted, it stays as the record |

A CEL rule on the CRD accepts exactly one of `source`, `database` and `all`. A
scheduled run is an ordinary BackupRun named `scheduled-<yyyymmdd-hhmm>` with
`all: true` and a 30-day TTL.

| Status field | Holds |
| --- | --- |
| `phase` | Queued, Running, Waiting, Succeeded or Failed; the column `kubectl get brun` prints |
| `workload` | the Kueue Workload admitting the run, while it exists |
| `startedAt`, `completedAt` | when Kueue admitted the run, and when it finished |
| `quiescedAt`, `restartedAt` | when the run stopped and restarted the quiesced workloads, in whole seconds |
| `restartPending` | true from the moment the run records `restartedAt` until it has given every workload its replicas back and resumed every Kustomization; a pass that finds it set repeats the restart and keeps the recorded `restartedAt`. Once `restartedAt` is set and this field is false, the run never restarts the workloads again |
| `quiesced[]` | the workloads the run stops, each with the replica count it had before the run touched it, which the run gives back |
| `suspendedKustomizations[]` | the Flux Kustomizations the run suspends, as namespace/name; it resumes exactly these |
| `items[]` | one per volume and database: kind, name, phase, message, the manual `trigger`, the restic `snapshot` and its `snapshotTime`, `empty` for a volume with no files, and the CloudNativePG `backup` |
| `conditions[type=Ready]` | the reason and message, kstatus-compatible, so a Flux Kustomization with `wait: true` can gate on the run |

The run writes `quiesced[]` and `suspendedKustomizations[]` as its plan, before
it suspends or scales anything, so both lists can name a workload that is still
running for the length of one pass. When a stop fails partway, the run cuts both
lists down to what is in effect, the workloads standing at zero replicas and the
Kustomizations that are suspended, and then gives those back. Which
Kustomizations a run suspends at all is in
[namespace-backups.md](namespace-backups.md#which-kustomization-a-run-suspends).

A run that stopped workloads rewrites each volume's snapshot to its
`restartedAt` and tags it `quiesced`, so `items[].snapshot` is the rewritten
ID and `snapshotTime` equals `restartedAt`. Without stopped workloads,
`snapshotTime` is the time restic stamped when it started reading the clone.
[namespace-backups.md](namespace-backups.md#quiesced-snapshots) shows a run
doing it.

### Ready reasons

| Ready reason | When |
| --- | --- |
| Queued | the run waits for Kueue to admit it |
| Running | work is under way; the message is `backing up`, or `waiting for pod <pod> to stop before the clones are cut` while a pod of a workload the run stopped is still terminating |
| SourceBusy | another run holds the run's claim or repository, holds this namespace's quiesce Lease, or has deleted a Cluster this run waits to see created again; the message names that run, what it holds, and every wait |
| Retrying | an item could not start with an error a retry may fix, such as a Backup a CloudNativePG webhook refuses; the message names every such item and its error |
| RestartFailed | the run could not give a stopped workload its replicas back or resume a Kustomization it suspended, so the app is still down; when the Lease release or the Workload delete failed as well, the reason stays RestartFailed and the message names both |
| ReleaseFailed | the app is back, and the run cannot finish because it could not release its Leases or delete its Kueue Workload |
| CRDOutdated | the run ended before it changed anything, because the CRD of its kind lacks a field the controller writes, is not installed, or may not be read by the controller; the message names the field or the permission |
| Invalid | the spec names something no retry can fix, such as a claim that is not marked `backup.wlz.li/enabled`, or workloads to stop whose Flux Kustomization also applies workloads in another namespace |
| Succeeded | the run finished with every item done |
| Failed | the run finished with a failed item, or past its timeout; a run that passed its deadline while waiting for another run carries that wait: `the run had not finished by <deadline>; it was waiting: <the wait>` |

Whenever a run's Ready condition moves to a new reason, the controller records
an event on the run with that reason, and the condition's message as its note.
A run that has finished with any reason but Succeeded, such as Invalid or
Failed, records a Warning, and so do RestartFailed and ReleaseFailed, which the
run reports while it is unfinished; Queued, Running, SourceBusy, Retrying and
Succeeded record Normal events. `kubectl describe brun <name>` lists them under
Events. Because events.k8s.io/v1 rejects a note over 1024 bytes, the controller
cuts a longer message at that length, and the full text stays on the condition.

## RestoreRun

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: notes-back-to-monday
  namespace: notes
spec:
  all: true
  restoreAsOf: "2026-09-22T00:00:00Z"
```

| Field | Required | Holds |
| --- | --- | --- |
| `claim` | one of `claim`/`repository`, `database` and `all` | the claim whose repository to restore from, and the claim to write into unless `into` names another |
| `repository` | the same | the restic Secret in this namespace to restore from, in place of the one the claim's VolumeRestore names. With `claim` as well, the run restores that claim in place from this repository; without `claim`, the run needs `into` and `intoSize` and fills a new claim |
| `into` | no | a claim to create and fill, leaving the source untouched; only with `claim` or `repository`. No claim of that name may exist yet, and with `claim` no VolumeRestore of that name either, since a VolumeRestore describes the backups of the claim of its name; the run creates no VolumeRestore for it. The run creates it empty and writes the selected snapshot into it through its own ReplicationDestination with `copyMethod: Direct`, so the mover's log confirms what the claim holds. With `claim` the run copies the source claim's size and the node its volume is on, and the scheduler places the mover pod with that volume; with `repository` there is no source node, and the mover pod is the claim's first consumer |
| `intoSize` | with `repository` and `into` | the size of that claim; omitted with `claim`, the source claim's request |
| `database` | one of the three | the Cluster to restore; the run deletes it and it recovers when it is created again. A Cluster carrying `backup.wlz.li/bootstrap: initdb`, or declaring its own bootstrap method such as `pg_basebackup`, ends the run Invalid, and so does a Cluster another unfinished RestoreRun is restoring |
| `all` | one of the three | every enabled claim in place, then every enabled Cluster; a Cluster carrying `backup.wlz.li/bootstrap: initdb`, or declaring its own bootstrap method, gets a Skipped item. A Cluster another unfinished RestoreRun is restoring ends the run Invalid, and a run whose every item is Skipped ends Failed with reason NoBackupInReach |
| `restoreAsOf` | no | the moment to restore to. A volume restores the newest snapshot at or before it, a database replays WAL to it exactly. Omitted, the newest snapshot and the end of the archive |
| `previous` | no | how many snapshots further back than the selected one; one volume only |
| `syncDatabaseToVolume` | no | with `all` only: recover the databases to the moment of the volumes' newest `quiesced` snapshot at or before `restoreAsOf`, so the files and the rows agree. Refused when a volume has no such snapshot, or two volumes' snapshots are from different moments |
| `quiesce` | no | Deployments and StatefulSets, as `{kind, name}`, to stop while the run restores; not with `into`. The run waits, with the workloads still running, while a backup of one of its claims uploads, another run holds the Lease of one of its claims, or another run has stopped this namespace's workloads. A volume item whose repository Secret is gone fails before anything is stopped, and a run left with no item to restore stops nothing; it gives the workloads back once the volumes are restored and the databases deleted. A workload whose Flux Kustomization also applies workloads in another namespace ends the run Invalid before anything is stopped |
| `timeout` | no | how long to wait for the movers and the recovered databases, counted from when the run passes its checks, or from its creation while its checks keep failing or while it waits for a backup of its repository; defaults to `4h` |
| `moverSecurityContext` | no | passed to the restore's ReplicationDestination; omitted, the source claim's VolumeRestore supplies it |
| `ttlSecondsAfterFinished` | no | delete the run that long after it finishes; an `into` claim goes with it |

| Status field | Holds |
| --- | --- |
| `phase` | Queued, Running, Waiting, Succeeded or Failed; the column `kubectl get rrun` prints |
| `target` | the claim an `into` restore creates and fills |
| `startedAt`, `completedAt` | when the run passed its checks and began, and when it finished |
| `syncedTo` | the moment a `syncDatabaseToVolume` run restores the volumes and recovers the databases to |
| `quiescedAt`, `restartedAt`, `quiesced[]`, `suspendedKustomizations[]` | when the run stopped and gave back the workloads `quiesce` lists, the replicas it gave each back, and the Flux Kustomizations it suspended and resumed |
| `items[]` | one per volume and per database: kind, name, phase (Pending, Running, Deleted, Recovering, Succeeded, Failed, Skipped), message, the `destination` while it exists, the `snapshot` and `snapshotTime` a volume restores, the `baseBackup` a recovery starts from, and the `clusterUID` of the Cluster a database item deletes |
| `conditions[type=Ready]` | the reason and message, kstatus-compatible; [Ready reasons of a RestoreRun](#ready-reasons-of-a-restorerun) lists them |

`items[].snapshotTime` is the time of the snapshot the run's checks selected,
and the mover gets it as `restoreAsOf`;
[restores.md](restores.md#which-snapshot-a-run-restores) says why the run
refuses a snapshot VolSync's mover would not restore when pinned to that
second, and how a run that lost its selection before it created anything ends.
`items[].clusterUID` is the UID of the Cluster a database item deletes, written
with the Deleted mark; [architecture.md](architecture.md#a-database-restore)
shows how the run tells the old Cluster from the recovered one by it. Ready
reason Retrying marks a run whose checks keep failing with an error a retry may
fix, such as a repository with the wrong password, with the phase still empty;
[namespace-backups.md](namespace-backups.md#checks-before-anything-is-touched)
has how long it retries.

A volume item succeeds only when the destination's `status.latestMoverStatus`
names the recorded snapshot in its log. A mover that restored another snapshot,
that found none, or whose log names none, fails the item and the message says
what the claim holds;
[restores.md](restores.md#what-the-mover-restored) lists the messages. A run
writes only into a claim it created itself: a claim named `spec.into`, or a
ReplicationDestination named restore-<first 8 characters of the run's UID>-<item
index>, that the run does not control fails the run, and the message says what
to do instead.

### Ready reasons of a RestoreRun

| Ready reason | When |
| --- | --- |
| Retrying | the checks failed with an error a retry may fix, such as a repository with the wrong password; the phase stays empty and the message holds the error |
| Running | work is under way; the message is `restoring`, `restoring into claim <into>` for an `into` restore, or `waiting for pod <pod> to stop before anything is restored` while a pod of a workload the run stopped is still terminating |
| SourceBusy | a backup of the run's claim or repository is in progress, another run holds the Lease of either, another run has stopped this namespace's workloads, or a RestoreRun has deleted a Cluster in this namespace and waits for it to be created again; the message names that run and what it holds |
| ClaimInUse | an in-place restore waits for its claim: `claim <claim> is mounted by pod <pod>; stop the workload and this restore starts on its own`, or `ReplicationDestination <name> is restoring into claim <claim>` while another restore writes into it |
| WaitingForShutdown | the run waits for something to be gone before it goes on: the instance pods and PVCs of a Cluster it deleted, or the Job and pods of a restore mover it stopped; the app stays stopped and the run keeps its Leases until then |
| WaitingForRecreate | the run deleted a Cluster and waits for its owner to create it again: `recreate <cluster> to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it` |
| RestartFailed | the run could not give a workload it stopped its replicas back, could not resume a Kustomization it suspended, or could not stop a restore mover while the app is down; the message names what failed and what to scale, resume or delete by hand |
| ReleaseFailed | the app is back, and the run cannot finish because it could not release its Leases or stop a restore mover; the message says what to delete by hand |
| CRDOutdated | the same as on a [BackupRun](#ready-reasons): the CRD of its kind lacks a field the controller writes, is not installed, or may not be read by the controller |
| Invalid | the spec names something no retry can fix, such as an `into` claim that already exists, a Cluster carrying `backup.wlz.li/bootstrap: initdb` named in `database`, or a Cluster another unfinished RestoreRun is restoring |
| NoBackupInReach | the run ended before it deleted or wrote anything, because an item has no backup at or before `restoreAsOf`, or its selected snapshot is one VolSync's mover would not restore; also a run that restored nothing because every item was Skipped |
| TimedOut | the run had not finished by its `timeout`, had not passed its checks by then (`the run had not passed its checks by <deadline>: <message>`), or its `into` claim had not been restored by then (`claim <into> had not been restored by <deadline>`); a run that was waiting for another run carries that wait: `the run had not finished by <deadline>; it was waiting: <the wait>` |
| Succeeded | every item holds the restored data |
| Failed | an item failed; the message names each failed item and its message |

A RestoreRun records an event at each new Ready reason, the same way a
[BackupRun](#backuprun) does.

Six CEL rules on the CRD: exactly one of `claim` or `repository`, `database`
and `all`; `previous` only with one volume; `into` only with `claim` or
`repository`; `intoSize` with `repository` and `into`; `syncDatabaseToVolume`
only with `all`; `quiesce` not with `into`. The fourth rejects a run without
`intoSize` with `into from a repository needs intoSize, because there is no
source claim to copy a size from`.

The `quiesced[]` and `suspendedKustomizations[]` of a RestoreRun follow the
same rules as a [BackupRun's](#backuprun): written as the plan before anything
is patched, and cut down to what is in effect after a failed stop.
