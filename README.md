# backup-controller

A Kubernetes controller that schedules and runs a namespace's backups, and
brings volumes and CloudNativePG databases back from them, with VolSync and the
barman-cloud plugin moving every byte.

It does three jobs in one binary:

| Job | What it acts on | Page |
| --- | --- | --- |
| fill a new claim from its restic repository, with no ZFS clone behind it | a claim whose `dataSourceRef` names a VolumeRestore | [docs/overview.md](docs/overview.md) |
| back up and restore on schedule or on demand | a Namespace's `backup.wlz.li/schedule`, BackupRun, RestoreRun | [docs/namespace-backups.md](docs/namespace-backups.md) |
| recover a new database from its archive | a CloudNativePG Cluster at creation, through a mutating webhook | [docs/restores.md](docs/restores.md) |

A namespace names its schedule in `backup.wlz.li/schedule`, and each claim and
Cluster opts in with `backup.wlz.li/enabled`. At every tick the controller
creates a BackupRun that Kueue admits as one unit. The run stops the workloads
marked `backup.wlz.li/quiesce` while the volumes' clones are cut, writes each
claim's ReplicationSource, and asks CloudNativePG for a base backup of each
database. A RestoreRun restores one volume, one database or the whole namespace
to the newest backup or a chosen moment.

Status, 2026-09-25: released at v0.5.6. v0.5.4 runs on the walzen prod cluster,
where the canary at the infrastructure repository's
modules/testing/canary-namespace-backup ran every mode on 2026-09-24: a
scheduled run, each form of BackupRun and RestoreRun, and automatic restore of
the volume and the database after the namespace was destroyed.
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

The early fixes below explain behaviour that is still in the code.

v0.1.2 adds `pods: get, list, watch` to the ClusterRole. The populator library
builds a pod informer whether or not a populator pod is used, and waits for its
cache to sync before the controller runs at all, so under v0.1.1 the reflector
failed every few seconds and every claim stayed Pending. Only a cluster showed
this: the offline check compared deploy/rbac.yaml against the table in
[docs/packaging.md](docs/packaging.md), and the two agreed with each other.

v0.2.0 stops a write loop: the library has no early return for a claim it has
already populated, so it calls the cleanup callback on every resync for the life
of the claim, and the callback was writing VolumeRestore status each time
without anything having changed.

v0.2.2 stops an error loop that had been there since v0.1.0. The library deletes
the prime claim after calling the cleanup callback, so every pass after the one
that finishes a restore arrives without it, and the callback rejected that. The
library requeues on error, so one restored claim erred several times a second
for as long as it existed, re-emitting PopulatorFinished as it went. Cleanup
exists to remove things, and the prime claim being gone is the state it works
towards.

v0.2.4 carries the source claim's selected node onto the scratch claim a
RestoreRun creates with `into:`. On a WaitForFirstConsumer class the populator
library waits for `volume.kubernetes.io/selected-node` before it fills a claim,
and the scheduler writes that annotation when a pod using the claim is
scheduled. A scratch claim has no pod, so nothing ever wrote it and the claim
stayed Pending: observed on the walzen test cluster, eight minutes with no prime
claim and nothing in the controller's log. Every class on that cluster binds
WaitForFirstConsumer.

The walzen infrastructure repository installs each release through its
cluster/backup-controller unit, described in
[docs/integration.md](docs/integration.md).

## Why it exists

An app whose claim names a VolSync ReplicationDestination in `dataSourceRef`
comes back filled whenever the claim is recreated, with no procedure for anyone
to remember. That behaviour is the reason the walzen-group infrastructure
repository writes every backed-up volume that way.

VolSync's own populator fills the claim by cloning a VolumeSnapshot. On
zfs-localpv, and on any copy-on-write storage, a volume provisioned from a
snapshot is a clone that holds its origin open for as long as it exists. Every
block the app overwrites keeps its previous version in that origin, and nothing
can release it: `zfs destroy` on the snapshot answers `snapshot has dependent
clones`. A volume that rewrites itself drifts toward holding twice its own size,
and a second dataset, the destination's restored copy, stays on the pool for the
life of the claim.

This controller keeps the behaviour and removes the clone. It fills an ordinary
empty volume by running a VolSync restore directly into it, then hands that
volume to the app's claim. No VolumeSnapshot is taken, no clone exists, nothing
is pinned, and the destination's permanent restored copy is gone.

Scheduling came later, in v0.5.0. VolSync's per-source schedules started every
mover at the same minute and Kueue admitted them one pod at a time, so a
namespace's two volumes could be backed up hours apart, and an app could not be
stopped for its backup at all.

## What it is not

It does not move data. VolSync's mover writes and reads every volume byte, and
the barman-cloud plugin every database byte. The controller decides when each
runs, writes the objects that start them, and reads the results.

It does not replace VolSync or the barman-cloud plugin, and it keeps no state of
its own: every run is an object in the cluster, and the backups are the restic
repositories and barman archives the two tools already write.

## Documents

| Document | For |
| --- | --- |
| [docs/overview.md](docs/overview.md) | the fill problem, the populator mechanism, and what changes for an app |
| [docs/architecture.md](docs/architecture.md) | the three parts of the binary, every object the controller writes, and what runs where |
| [docs/api.md](docs/api.md) | VolumeRestore, BackupRun and RestoreRun, field by field |
| [docs/namespace-backups.md](docs/namespace-backups.md) | the annotations, the scheduler, the runs, quiesce and the metrics, measured on the prod canary |
| [docs/restores.md](docs/restores.md) | what fills a claim, what overwrites one, how a database restores, and which to reach for |
| [docs/packaging.md](docs/packaging.md) | the release: image, rendered manifests, Helm chart, RBAC |
| [docs/integration.md](docs/integration.md) | how the infrastructure repository installs and uses it |
| [docs/decisions.md](docs/decisions.md) | why this shape rather than the alternatives that were rejected |
| [docs/implementation-plan.md](docs/implementation-plan.md) | the original build plan for v0.1, kept as a record |
