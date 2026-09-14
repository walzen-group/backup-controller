# backup-controller

A Kubernetes volume populator that fills a new PersistentVolumeClaim from a
restic repository by asking VolSync to restore into it, so the resulting volume
is an ordinary dataset with nothing behind it.

It carries three kinds. A VolumeRestore is a standing declaration that fills a
claim as the claim is created, always from the newest backup, with no operator
involved. A BackupRun takes one backup now. A RestoreRun writes a chosen
snapshot back, either into the volume the app already has or into a second one
beside it. [docs/restores.md](docs/restores.md) says which answers which
question.

Status, 2026-09-14: released at v0.2.0. v0.1.2 is installed on the walzen test
cluster and has restored a volume there end to end; the two run kinds in v0.2.0
have passing tests and have not yet been exercised on a cluster.
The Go module and its flake dev shell, the VolumeRestore API with its generated
CRD, the internal/volsync and internal/populator packages with their fake-client
tests, the binary in cmd/backup-controller bound to the populator library's
provider callbacks, the deploy/ tree, the Helm chart under chart/ and the
release workflow all exist, and their gates pass.

v0.1.0 was the first release. v0.1.1 adds the cacheStorageClassName and
cacheCapacity passthroughs: VolSync's mover provisions a metadata cache claim
for every restore, and without a class named for it that claim comes from the
cluster's default storage class. Where that default reclaims Retain, each
restore leaves a cache dataset on the pool, which is the shape this project
exists to remove.

v0.1.2 adds `pods: get, list, watch` to the ClusterRole. The populator library
builds a pod informer whether or not a populator pod is used, and waits for its
cache to sync before the controller runs at all, so under v0.1.1 the reflector
failed every few seconds and every claim stayed Pending. Only a cluster showed
this: the offline check compared deploy/rbac.yaml against the table in
[docs/packaging.md](docs/packaging.md), and the two agreed with each other.

v0.2.0 adds the BackupRun and RestoreRun kinds and the manager that reconciles
them, beside the populator's own loop. It also stops a write loop: the library
has no early return for a claim it has already populated, so it calls the
cleanup callback on every resync for the life of the claim, and the callback was
writing VolumeRestore status each time without anything having changed.

The walzen infrastructure repository installs the release through a terragrunt
unit and consumes it from its backup module, described in
[docs/integration.md](docs/integration.md). Nothing has restored a volume on a
cluster yet, and
[.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md](.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md)
is the run that settles it.

## The problem it exists for

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

## What it is not

It does not move data. VolSync's mover does every byte, with the repository
credentials and the backup queue it already uses. This controller creates one
object, waits for it, and deletes it.

It does not replace VolSync, and an app can keep using VolSync's own populator
on any volume where the clone is acceptable.

## Documents

| Document | For |
| --- | --- |
| [docs/overview.md](docs/overview.md) | the problem, the mechanism, and what changes for an app |
| [docs/architecture.md](docs/architecture.md) | the object flow, the library it builds on, and what runs where |
| [docs/api.md](docs/api.md) | the custom resource: every field, its status, and a worked example |
| [docs/packaging.md](docs/packaging.md) | the release: image, rendered manifests, Helm chart, and what each asset has to contain |
| [docs/integration.md](docs/integration.md) | how the infrastructure repository installs and consumes it |
| [docs/decisions.md](docs/decisions.md) | why this shape rather than the alternatives that were rejected |
| [docs/implementation-plan.md](docs/implementation-plan.md) | the work, in order, with what proves each step |
