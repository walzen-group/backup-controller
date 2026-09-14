# backup-controller

A Kubernetes volume populator that fills a new PersistentVolumeClaim from a
restic repository by asking VolSync to restore into it, so the resulting volume
is an ordinary dataset with nothing behind it.

Status, 2026-09-14: implemented and unreleased. The Go module and its flake dev
shell, the `VolumeRestore` API with its generated CRD, the `internal/volsync`
and `internal/populator` packages with their fake-client tests, the binary in
`cmd/backup-controller` bound to the populator library's provider callbacks, the
`deploy/` tree, the Helm chart under `chart/` and the release workflow all exist
and their gates pass. No tag has been cut, so ghcr.io holds no image and no
release asset exists yet, and the infrastructure repository work in
[docs/integration.md](docs/integration.md) is still nobody's done work. The
cluster proof is a runbook rather than a run:
[.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md](.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md).

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
