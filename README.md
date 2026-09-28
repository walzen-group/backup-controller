# backup-controller

A Kubernetes controller that backs up a namespace's volumes and CloudNativePG
databases on a schedule, and brings them back: on demand with a RestoreRun,
and on its own when a namespace or a Cluster is created again. VolSync's
restic mover writes every volume backup and the barman-cloud plugin every
database backup; the controller decides when each runs, restores volumes with
restic, and decides where each database recovers to.

| Job | Acts on | Doc |
| --- | --- | --- |
| Back up a namespace on a schedule or on demand, with the app paused | a Namespace's backup.wlz.li/schedule, a BackupRun | docs/backups.md |
| Restore claims and databases to the newest backup or to a moment | a RestoreRun | docs/restores.md |
| Fill a new claim from its repository | a claim whose dataSourceRef names a VolumeRestore | docs/automatic-restore.md |
| Recover a new Cluster from its archive | a CloudNativePG Cluster at its create, through a mutating webhook | docs/automatic-restore.md |

## How a namespace is backed up

A namespace names its schedule in backup.wlz.li/schedule, and each claim and
Cluster opts in with backup.wlz.li/enabled: "true". At every tick the
controller creates a BackupRun, and Kueue admits at most five runs at once
across the cluster. The run pauses the workloads marked
backup.wlz.li/pause-during-backup, has VolSync back up each claim and
CloudNativePG write a base backup of each database, and resumes the app once
every clone is cut and every database backup completed. Each volume snapshot
then carries the resume time, the tag paused, and the ID of each base backup
taken during the pause.

## How it restores

A RestoreRun selects every snapshot and base backup before it changes anything,
and fails with reason NoBackupInReach when one is missing. It restores each
volume with a restic Job by snapshot ID, and recovers each database by
deleting the Cluster and letting its owner create it again. The webhook then
recovers the new Cluster to the end of a base backup or to the end of its
archive: targets Postgres always reaches, never ahead of the chosen moment.
With syncDatabaseToVolume, volumes and databases come back to the same paused
moment.

When Flux rebuilds a namespace without a RestoreRun, the claims and the
Clusters come back to the namespace's newest paused moment when every volume
has one, and to their newest backups otherwise.

## Rules the controller keeps

- It reads no log to decide anything. It finds snapshots in the restic
  repository, base backups in the S3 archive, and results in the status of
  VolSync, CloudNativePG and its own Jobs.
- Two runs never act on the same claim, repository, Cluster or paused app at
  once; they take turns through coordination.k8s.io Leases.
- A run records each step in its status before it acts, so a controller that
  restarts anywhere finishes the run, and a failed or timed-out run resumes the
  app it paused.
- Two Clusters never archive into the same bucket and prefix.
- It uses the API versions the cluster serves for VolSync, CloudNativePG,
  Kueue and Flux, and fails with a message that names the object and field when
  something it needs is missing.

## Documents

| Doc | For |
| --- | --- |
| docs/backups.md | what gets backed up, the schedule, pausing the app, and how a BackupRun runs |
| docs/restores.md | every kind of RestoreRun, and what each database recovers to |
| docs/automatic-restore.md | the populator, and the webhook's decision on a new Cluster |
| docs/operations.md | flags, load and the queue, Leases, timeouts, pausing for an upgrade, failures and metrics |
| docs/api.md | every field, annotation and reason |
| docs/installing.md | what the cluster needs, the release assets, and the permissions |
| docs/upgrading.md | the upgrade from v0.10.1 |
| docs/decisions.md | why the controller works this way, and what the alternatives break |
| hack/kind/README.md | the test cluster and the end-to-end scenarios |

## Why the controller fills claims itself

An app whose claim names a data source in dataSourceRef comes back filled
whenever the claim is created again, with no procedure for anyone to follow.
VolSync's own populator fills such a claim by cloning a VolumeSnapshot. On
zfs-localpv, and on any copy-on-write storage, a volume provisioned from a
snapshot is a clone that holds its origin open for as long as it exists: every
block the app overwrites keeps its old version in the origin, and zfs destroy
on the snapshot answers "snapshot has dependent clones". The controller fills
an ordinary empty volume with a restic restore instead, then hands that volume
to the claim, so no snapshot and no clone remain.

## Tests

Every feature has a scenario on a kind cluster with the real VolSync,
CloudNativePG, barman-cloud plugin, Kueue, Flux and an S3 service; each
scenario checks the file in the claim or the rows in the database.

```
make kind-up
make kind-deploy
make e2e
```

hack/kind/README.md describes the test cluster. `make check` runs the unit
tests, vet, lint and the build.
