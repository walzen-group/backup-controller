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
| `cacheCapacity` | no | `cacheCapacity` of the claim's ReplicationSource and of the populator's destination; an in-place RestoreRun's destination leaves it to VolSync |
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
| Restoring | a claim is being filled; the message names the ReplicationDestination and the controller's namespace |
| RestoreFailed | the mover failed; the message holds its logs, and the reason stays while VolSync retries the mover, until a sync succeeds |
| NoBackupInReach | the moment the claim's `backup.wlz.li/restore-as-of` or `spec.restoreAsOf` asks for is older than every snapshot; the message names which of the two it came from, the claim stays Pending, and the controller creates no destination |
| Restored | no claim is being filled |

While any claim is listed in `status.claims`, the VolumeRestore carries the
finalizer `backup.wlz.li/volume-populator`. The populator adds it before it
creates anything for a claim, and removes it once `status.claims` is empty.
A VolumeRestore deleted mid-restore therefore stays Terminating until its
claims finish or are deleted. Each claim's cleanup deletes that claim's
ReplicationDestination and Secret copy, and the cleanup that empties
`status.claims` removes the finalizer, which lets the deletion finish. A
VolumeRestore that is already being deleted without the
finalizer starts no new restore.

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
| `all` | one of the three | every enabled claim and Cluster in the namespace, with the workloads marked `backup.wlz.li/quiesce` stopped until the clones are cut |
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
| `restartPending` | true from the moment the run records `restartedAt` until it has given every workload its replicas back and resumed every Kustomization; a pass that finds it set repeats the restart and keeps the recorded `restartedAt` |
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

Whenever a run's Ready condition moves to a new reason, the controller records
an event on the run with that reason, and the condition's message as its note.
A run that finishes with any reason but Succeeded, such as Invalid or Failed,
records a Warning; Queued, Running, SourceBusy and Succeeded record Normal
events. `kubectl describe brun <name>` lists them under Events. Because
events.k8s.io/v1 rejects a note over 1024 bytes, the controller cuts a longer
message at that length, and the full text stays on the condition.

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
| `repository` | the same | the restic Secret in this namespace, for a repository no claim here owns; needs `into` and `intoSize` |
| `into` | no | a claim to create and fill, leaving the source untouched; only with `claim` or `repository`. With `claim`, a VolumeRestore fills it on the source claim's node; with `repository`, a mover writes into it directly, and the scheduler places it with the mover pod |
| `intoSize` | with `repository` and `into` | the size of that claim; omitted with `claim`, the source claim's request |
| `database` | one of the three | the Cluster to restore; the run deletes it and it recovers when it is created again. A Cluster carrying `backup.wlz.li/bootstrap: initdb`, or declaring its own bootstrap method such as `pg_basebackup`, ends the run Invalid |
| `all` | one of the three | every enabled claim in place, then every enabled Cluster; a Cluster carrying `backup.wlz.li/bootstrap: initdb`, or declaring its own bootstrap method, gets a Skipped item |
| `restoreAsOf` | no | the moment to restore to. A volume restores the newest snapshot at or before it, a database replays WAL to it exactly. Omitted, the newest snapshot and the end of the archive |
| `previous` | no | how many snapshots further back than the selected one; one volume only |
| `syncDatabaseToVolume` | no | with `all` only: recover the databases to the moment of the volumes' newest `quiesced` snapshot at or before `restoreAsOf`, so the files and the rows agree. Refused when a volume has no such snapshot, or two volumes' snapshots are from different moments |
| `quiesce` | no | Deployments and StatefulSets, as `{kind, name}`, to stop while the run restores; not with `into`. The run gives them back once the volumes are restored and the databases deleted |
| `timeout` | no | how long to wait for the movers and the recovered databases, counted from when the run passes its checks, or from its creation while its checks keep failing; defaults to `4h` |
| `moverSecurityContext` | no | passed to the restore's ReplicationDestination; omitted, the source claim's VolumeRestore supplies it |
| `ttlSecondsAfterFinished` | no | delete the run that long after it finishes; an `into` claim and its VolumeRestore go with it |

| Status field | Holds |
| --- | --- |
| `phase` | Queued, Running, Waiting, Succeeded or Failed; the column `kubectl get rrun` prints |
| `target` | the claim an `into` restore creates and fills |
| `startedAt`, `completedAt` | when the run passed its checks and began, and when it finished |
| `syncedTo` | the moment a `syncDatabaseToVolume` run restores the volumes and recovers the databases to |
| `quiescedAt`, `restartedAt`, `quiesced[]`, `suspendedKustomizations[]` | when the run stopped and gave back the workloads `quiesce` lists, the replicas it gave each back, and the Flux Kustomizations it suspended and resumed |
| `items[]` | one per volume and per database: kind, name, phase (Pending, Running, Deleted, Recovering, Succeeded, Failed, Skipped), message, the `destination` while it exists, the `snapshot` and `snapshotTime` a volume restores, the `baseBackup` a recovery starts from, and the `clusterUID` of the Cluster a database item deletes |
| `conditions[type=Ready]` | the reason and message, e.g. NoBackupInReach, ClaimInUse, Retrying, or `recreate <cluster> to finish the restore` |

`items[].snapshotTime` is the time of the snapshot the run's checks selected,
and the mover gets it as `restoreAsOf`;
[restores.md](restores.md#which-snapshot-a-run-restores) says why.
`items[].clusterUID` is the UID of the Cluster a database item deletes, written
with the Deleted mark; [architecture.md](architecture.md#a-database-restore)
shows how the run tells the old Cluster from the recovered one by it. Ready
reason Retrying marks a run whose checks keep failing with an error a retry may
fix, such as a repository with the wrong password, with the phase still empty;
[namespace-backups.md](namespace-backups.md#checks-before-anything-is-touched)
has how long it retries.

A RestoreRun records an event at each new Ready reason, the same way a
[BackupRun](#backuprun) does.

Six CEL rules on the CRD: exactly one of `claim` or `repository`, `database`
and `all`; `previous` only with one volume; `into` only with `claim` or
`repository`; `intoSize` with `repository` and `into`; `syncDatabaseToVolume`
only with `all`; `quiesce` not with `into`. The fourth rejects a run without
`intoSize` with `into from a repository needs intoSize, because there is no
source claim to copy a size from`, and a run the API server admitted before that
rule existed ends Invalid at its checks, naming `spec.intoSize`.

The `quiesced[]` and `suspendedKustomizations[]` of a RestoreRun follow the
same rules as a [BackupRun's](#backuprun): written as the plan before anything
is patched, and cut down to what is in effect after a failed stop.
