# backup-controller

A Kubernetes controller that schedules and runs a namespace's backups, and
restores volumes and CloudNativePG databases from them. VolSync's mover writes
every volume backup. A restic Job of the controller's own writes every volume
restore. The barman-cloud plugin moves every database byte.

It does three jobs in one binary:

| Job | What it acts on | Page |
| --- | --- | --- |
| fill a new claim from its restic repository, with no ZFS clone behind it | a claim whose `dataSourceRef` names a VolumeRestore | [docs/overview.md](docs/overview.md) |
| back up and restore on schedule or on demand | a Namespace's `backup.wlz.li/schedule`, BackupRun, RestoreRun | [docs/namespace-backups.md](docs/namespace-backups.md) |
| recover a new database from its archive | a CloudNativePG Cluster at creation, through a mutating webhook | [docs/restores.md](docs/restores.md) |

A namespace names its schedule in `backup.wlz.li/schedule`. Each claim and
Cluster opts in with `backup.wlz.li/enabled`. At every tick, the controller
creates a BackupRun that Kueue admits as one unit. The run does these steps:

- It stops the workloads marked `backup.wlz.li/quiesce` while VolSync cuts the
  clones of the volumes.
- It writes each claim's ReplicationSource.
- It asks CloudNativePG for a base backup of each database.

A run that stopped workloads moves each volume's snapshot to the moment it
started them again, and tags the snapshot `quiesced`.

A RestoreRun restores one volume, one database or the whole namespace. It
restores to the newest backup or to a chosen moment. It can stop the workloads
that it lists while it restores. With `syncDatabaseToVolume`, it recovers the
databases to the moment that the quiesced snapshot of the volumes holds.

Status, 2026-09-25: released at v0.7.0, which runs on the walzen prod cluster.
The canary at the infrastructure repository's
modules/testing/canary-namespace-backup ran every mode on 2026-09-24:

- a scheduled run.
- each form of BackupRun and RestoreRun.
- an automatic restore of the volume and the database after someone destroyed
  the namespace.

On 2026-09-25, the canary ran the quiesced snapshot rewrite. It also ran a
synced, quiesced RestoreRun. That run restored the volume and the database, and
both ended on the same tick.
[docs/namespace-backups.md](docs/namespace-backups.md) quotes those runs.

## Releases

| Tag | Change |
| --- | --- |
| v0.1.0 | the VolumeRestore populator |
| v0.1.1 | cacheStorageClassName and cacheCapacity passthroughs |
| v0.1.2 | pods get, list, watch for the library's pod informer |
| v0.2.0 | BackupRun and RestoreRun, and a stop to the populator's status write loop |
| v0.2.1 | a startup panic on a doubled `--kubeconfig` flag |
| v0.2.2 | an error loop in the populator's cleanup |
| v0.2.3 | a logger for controller-runtime |
| v0.2.4 | the selected node carried onto an `into:` scratch claim |
| v0.3.0 | the bootstrap webhook: a new Cluster recovers from its object store |
| v0.3.1 | the public CA roots in the image |
| v0.3.2 | an object store verified through its own `endpointCA` |
| v0.3.3 | the database and owner carried into a recovery |
| v0.4.0 | a Cluster refused when another database already archives to its prefix |
| v0.4.1 | dry-run requests admitted without reading the object store |
| v0.4.2 | `initdb` dropped from updates to a recovered Cluster |
| v0.5.0 | namespace backups: the scheduler, BackupRun and RestoreRun with `database` and `all`, quiesce, controller-written ReplicationSources, the restore checks, the metrics |
| v0.5.1 | a base backup named by its directory |
| v0.5.2 | retention tiers from the `retain-` annotations |
| v0.5.3 | the zone database compiled in, so a `CRON_TZ=` schedule loads its zone |
| v0.5.4 | `backup.wlz.li/timeout` and `backup.wlz.li/prune-interval-days` per namespace, and a six-hour default timeout |
| v0.5.5 | an event on a run at each new Ready reason, a Warning when it fails |
| v0.5.6 | a due tick waits until the namespace has something marked enabled |
| v0.6.0 | quiesced snapshots moved to the run's `restartedAt` and tagged `quiesced`, and `syncDatabaseToVolume` on a RestoreRun |
| v0.7.0 | `quiesce` on a RestoreRun: the run stops the workloads it lists while it restores |
| v0.7.1 | a database restore gives the app back only once the old Cluster's instance pods and PVCs are gone |
| v0.7.2 | a RestoreRun that cannot stop a workload fails immediately with reason `Failed`. A retime whose delete failed uses its copy again. An `endpointURL` of `host:port` reads as HTTPS |
| v0.8.0 | a restore from a repository with `into:` writes through a Direct ReplicationDestination. A restore keeps the snapshot its checks selected, and leaves an opted-out Cluster alone. A quiesce records its plan before it stops anything. A failed mover fails its item. The webhook ignores failed base backups and compares endpoints. The controller gets health probes and a Recreate rollout |
| v0.8.1 | a RestoreRun leaves a Cluster alone when its owner declares its own bootstrap method, such as `pg_basebackup` |
| v0.8.2 | a failed mover no longer deletes the ReplicationSource. The delete killed the retry mover and left a restic lock that stopped every later `forget`. Upgrading needs `restic unlock` on each repository that had a mover failure under v0.8.0 or v0.8.1, see [namespace-backups.md](docs/namespace-backups.md) |
| v0.9.0 | every volume restore, a RestoreRun's and the populator's, runs in a restic Job of the controller's own. The Job restores the selected snapshot by its full ID. It reports success only on the Job's `Complete=True`, with restic's exit code and last lines on a failure. The controller creates the Job suspended, and resumes it after its owner has recorded it. To stop the Job, the controller suspends it, which ends restic with SIGTERM. Then it waits until none of its pods can still write, and deletes it. Two snapshots in one second are each restorable. Only snapshots with a VolSync mover's layout are candidates. A backup finds its snapshot in the repository by the time window of its sync, and reads no mover log anywhere. It records a claim as empty only after two listings. An admission policy lets the controller's ServiceAccount create only restore Jobs of that shape. Quiesce sets replicas through the `scale` subresource. The ClusterRole holds no write verb on workloads and nothing on ReplicationDestinations. Items carry a typed `reason`. A backup and a restore of one claim or repository never run at the same time, through a Lease per claim and per repository. Two runs never stop the workloads of one namespace at the same time, through a quiesce Lease per namespace. Two in-place restores of one claim run one after the other. A second RestoreRun for a Cluster that another run restores ends Invalid. A run that cannot give its app back reports `RestartFailed`. A run that cannot release its Leases, Workload or restore Job reports `ReleaseFailed`. Neither run finishes until it can. A restore whose items were all Skipped ends with reason `NoBackupInReach`. A run reads the installed CRD first. It ends with reason `CRDOutdated` when the CRD does not have a field the controller writes. A run gives a quiesced app back at `backup.wlz.li/max-quiesce`, ten minutes by default. It then fails the volume items whose clones were not cut. An item that VolSync keeps retrying names what data a later snapshot of that sync holds. Every new trigger unlocks the repository first. The webhook refuses a database that could never archive into the archive under its prefix. It compares bucket and prefix only. A run refuses a Flux Kustomization that applies workloads of two namespaces. The controller holds no code for runs an older version started. A `--pause` flag holds new runs for an upgrade, and the deadlines of a paused run count from the end of the pause. The webhook and the runs read barman, the barman-cloud plugin, CloudNativePG, Flux, restic, VolSync and Kueue data with the rules of their pinned source, cited in docs/compatibility.md. A BackupRun whose Kueue Workload is evicted gives the app back and ends with reason `Evicted`. Upgrading: from v0.7.x directly, through the infra units, while no run is active. The volsync unit declares the restic image, and the backup-controller unit passes it as `--restore-image`. See [docs/upgrading.md](docs/upgrading.md#v0102) |
| v0.9.1 | the tests decode each CRD file one time per test binary. The controller is the same as v0.9.0 |
| v0.10.0 | an in-place RestoreRun stops the workloads marked `backup.wlz.li/quiesce` together with its `spec.quiesce`. An automatic restore of a whole app, a claim of the populator and a Cluster of the webhook, comes back to one quiesced moment, and a namespace whose repositories give two moments waits for a `restore-as-of`. Two BackupRuns of one Cluster take their Backups one after the other, through a Cluster Lease. A run no longer reads the installed CRD, and the reason `CRDOutdated` and the `get` on CustomResourceDefinitions are gone: the CRDs and the Deployment come from one release asset. An item names a lost claim, a sync without a snapshot and a setting that does not parse with the reasons `ClaimLost`, `NoMoverSnapshot` and `SettingsInvalid`. The controller has 10,392 lines of code and 12,346 of tests, down from 11,249 and 24,863 in v0.9.1, with the same behaviour otherwise. Upgrading: from v0.7.x directly, through the infra units, while no run is active. See [docs/upgrading.md](docs/upgrading.md#v0102) |
| v0.10.1 | the webhook waits for its `backup.info` GETs in flight to finish before it answers. v0.10.0 cancelled them, and a cancelled GET could still reach the store after the answer. Upgrading: as for v0.10.0. See [docs/upgrading.md](docs/upgrading.md#v0102) |
| v0.10.2 | hotfix: a RestoreRun stops only the workloads in its spec.quiesce list again, as in v0.9.1. v0.10.0 and v0.10.1 also stopped every workload with the annotation backup.wlz.li/quiesce, which only a BackupRun with all: true should read. Upgrading: as for v0.10.1. See [docs/upgrading.md](docs/upgrading.md#v0102) |

The early fixes below explain behaviour that is still in the code.

v0.1.2 adds `pods: get, list, watch` to the ClusterRole. The populator library
builds a pod informer whether or not the controller uses a populator pod. The
library waits for the informer's cache to sync before the controller runs at
all. Thus, under v0.1.1, the reflector failed every few seconds and every
claim stayed Pending. Only a cluster showed this. The offline check compared
deploy/rbac.yaml against the table in [docs/packaging.md](docs/packaging.md),
and the two agreed with each other.

v0.2.0 stops a write loop. The library has no early return for a claim that
it already populated. Thus it calls the cleanup callback on every resync for
the life of the claim. The callback wrote VolumeRestore status each time,
although nothing changed.

v0.2.2 stops an error loop that existed from v0.1.0. The library deletes the
prime claim after it calls the cleanup callback. Thus every pass after the pass
that finishes a restore arrives without the prime claim, and the callback
rejected that. The library puts the claim in its queue again on error. Thus
one restored claim failed several times a second for as long as it existed,
and emitted PopulatorFinished again each time. The purpose of the cleanup is
to delete objects, and a prime claim that is gone is the state that the
cleanup works towards.

v0.2.4 carries the source claim's selected node onto the scratch claim that a
RestoreRun creates with `into:`. On a WaitForFirstConsumer class, the populator
library waits for `volume.kubernetes.io/selected-node` before it fills a
claim. The scheduler writes that annotation when it schedules a pod that uses
the claim. A scratch claim has no pod, so nothing ever wrote the annotation
and the claim stayed Pending. The walzen test cluster showed this: eight
minutes with no prime claim and nothing in the controller's log. Every class
on that cluster binds WaitForFirstConsumer.

The walzen infrastructure repository installs each release through its
cluster/backup-controller unit, described in
[docs/integration.md](docs/integration.md).

## Why it exists

An app whose claim names a VolSync ReplicationDestination in `dataSourceRef`
comes back filled each time someone creates the claim again. Nobody has to
remember a procedure. That behaviour is the reason the walzen-group infrastructure
repository writes every backed-up volume that way.

VolSync's own populator fills the claim with a clone of a VolumeSnapshot. On
zfs-localpv, and on any copy-on-write storage, a volume provisioned from a
snapshot is a clone. The clone holds its origin open for as long as the clone
exists. Every block that the app overwrites keeps its previous version in that
origin, and nothing can release it. `zfs destroy` on the snapshot answers
`snapshot has dependent clones`. A volume that rewrites itself grows
toward twice its own size. A second dataset, the destination's restored copy,
stays on the pool for the life of the claim.

This controller keeps the behaviour without the clone. A Job runs
`restic restore` directly into an ordinary empty volume. The Job uses the same
restic image that VolSync uses for backups. Then the controller hands that
volume to the app's claim. Nothing takes a VolumeSnapshot, no clone exists,
nothing stays pinned, and no destination keeps a permanent restored copy.

Scheduling came later, in v0.5.0. VolSync's per-source schedules started every
mover at the same minute, and Kueue admitted them one pod at a time. Thus the
backups of a namespace's two volumes could be hours apart. Also, nothing could
stop an app for its backup.

## What it is not

It does not move data itself. VolSync's mover writes every volume backup. The
controller's restore Job runs restic for every volume restore. The
barman-cloud plugin moves every database byte. The controller decides when
each one runs, writes the objects that start them, and reads the results.

It does not replace VolSync or the barman-cloud plugin, and it keeps no state
of its own. Every run is an object in the cluster. The backups are the restic
repositories and barman archives that the two tools already write.

## Documents

| Document | For |
| --- | --- |
| [docs/overview.md](docs/overview.md) | the fill problem, the populator mechanism, and what changes for an app |
| [docs/architecture.md](docs/architecture.md) | the three parts of the binary, every object the controller writes, and what runs where |
| [docs/api.md](docs/api.md) | VolumeRestore, BackupRun and RestoreRun, field by field |
| [docs/namespace-backups.md](docs/namespace-backups.md) | the annotations, the scheduler, the runs, quiesce and the metrics, measured on the prod canary |
| [docs/restores.md](docs/restores.md) | what fills a claim, what overwrites one, how a database restores, and which to reach for |
| [docs/packaging.md](docs/packaging.md) | the release: image, rendered manifests, Helm chart, RBAC |
| [docs/releasing.md](docs/releasing.md) | how to make a release: the local e2e cluster, the checks before every tag, the tag and push |
| [docs/upgrading.md](docs/upgrading.md) | what to check and apply when moving a cluster to a new release |
| [docs/integration.md](docs/integration.md) | how the infrastructure repository installs and uses it |
| [docs/decisions.md](docs/decisions.md) | each choice, each rejected option, and what would have gone wrong with it |
| [docs/implementation-plan.md](docs/implementation-plan.md) | the original build plan for v0.1, kept as a record |
