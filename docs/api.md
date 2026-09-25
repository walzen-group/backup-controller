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
| `restoreAsOf` | no | the populator's ReplicationDestination `spec.restic.restoreAsOf` |
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
| `quiescedAt`, `restartedAt`, `quiesced[]` | when the run stopped and restarted the quiesced workloads, and the replicas it gave each back |
| `suspendedKustomizations[]` | the Flux Kustomizations the run suspended, as namespace/name; it resumes exactly these |
| `items[]` | one per volume and database: kind, name, phase, message, the manual `trigger`, the restic `snapshot` and its `snapshotTime`, `empty` for a volume with no files, and the CloudNativePG `backup` |
| `conditions[type=Ready]` | the reason and message, kstatus-compatible, so a Flux Kustomization with `wait: true` can gate on the run |

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
| `repository` | the same | the restic Secret in this namespace, for a repository no claim here owns; needs `into` |
| `into` | no | a claim to create and fill, leaving the source untouched; only with `claim` or `repository` |
| `intoSize` | no | the size of that claim; omitted, the source claim's request |
| `database` | one of the three | the Cluster to restore; the run deletes it and it recovers when it is created again |
| `all` | one of the three | every enabled claim in place, then every enabled Cluster |
| `restoreAsOf` | no | the moment to restore to. A volume restores the newest snapshot at or before it, a database replays WAL to it exactly. Omitted, the newest snapshot and the end of the archive |
| `previous` | no | how many snapshots further back than the selected one; one volume only |
| `syncDatabaseToVolume` | no | with `all` only: recover the databases to the moment of the volumes' newest `quiesced` snapshot at or before `restoreAsOf`, so the files and the rows agree. Refused when a volume has no such snapshot, or two volumes' snapshots are from different moments |
| `quiesce` | no | Deployments and StatefulSets, as `{kind, name}`, to stop while the run restores; not with `into`. The run gives them back once the volumes are restored and the databases deleted |
| `timeout` | no | how long to wait for the movers and the recovered databases; defaults to `4h` |
| `moverSecurityContext` | no | passed to the restore's ReplicationDestination; omitted, the source claim's VolumeRestore supplies it |
| `ttlSecondsAfterFinished` | no | delete the run that long after it finishes; an `into` claim and its VolumeRestore go with it |

| Status field | Holds |
| --- | --- |
| `phase` | Queued, Running, Waiting, Succeeded or Failed; the column `kubectl get rrun` prints |
| `target` | the claim an `into` restore creates and fills |
| `startedAt`, `completedAt` | when the run passed its checks and began, and when it finished |
| `syncedTo` | the moment a `syncDatabaseToVolume` run restores the volumes and recovers the databases to |
| `quiescedAt`, `restartedAt`, `quiesced[]`, `suspendedKustomizations[]` | when the run stopped and gave back the workloads `quiesce` lists, the replicas it gave each back, and the Flux Kustomizations it suspended and resumed |
| `items[]` | one per volume restored in place and per database: kind, name, phase (Pending, Running, Deleted, Recovering, Succeeded, Failed, Skipped), message, the `destination` while it exists, the `snapshot` and the `baseBackup` a recovery starts from |
| `conditions[type=Ready]` | the reason and message, e.g. NoBackupInReach, ClaimInUse, or `recreate <cluster> to finish the restore` |

A RestoreRun records an event at each new Ready reason, the same way a
[BackupRun](#backuprun) does.

Five CEL rules on the CRD: exactly one of `claim` or `repository`, `database`
and `all`; `previous` only with one volume; `into` only with `claim` or
`repository`; `syncDatabaseToVolume` only with `all`; `quiesce` not with `into`.
