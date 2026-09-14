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

Status, 2026-09-14: released at v0.2.4. VolumeRestore and BackupRun are proven
on the walzen test cluster. A canary's claim was destroyed and refilled from
restic, with the repository showing the file's four lines before and five after,
and a BackupRun took an off-schedule backup in 31 seconds and left the source's
schedule exactly as it found it. RestoreRun's `into:` mode is fixed in v0.2.4
and has not been rerun; its in-place mode has not run on a cluster at all.

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

v0.2.4 carries the source claim's selected node onto the scratch claim a
RestoreRun creates with `into:`. On a WaitForFirstConsumer class the populator
library waits for `volume.kubernetes.io/selected-node` before it fills a claim,
and the scheduler writes that annotation when a pod using the claim is
scheduled. A scratch claim has no pod, so nothing ever wrote it and the claim
stayed Pending: observed on the walzen test cluster, eight minutes with no prime
claim and nothing in the controller's log. Every class on that cluster binds
WaitForFirstConsumer.

v0.2.3 gives controller-runtime a logger. Without one it discards every line the
BackupRun and RestoreRun reconcilers produce, so a run that failed would have
said nothing anywhere. The manager now logs through klog like the rest of the
binary.

v0.2.2 stops an error loop that had been there since v0.1.0. The library deletes
the prime claim after calling the cleanup callback, so every pass after the one
that finishes a restore arrives without it, and the callback rejected that. The
library requeues on error, so one restored claim erred several times a second
for as long as it existed, re-emitting PopulatorFinished as it went. Cleanup
exists to remove things, and the prime claim being gone is the state it works
towards.

v0.2.1 fixes a startup panic in v0.2.0. Importing controller-runtime brings in
pkg/client/config, whose init registers a `--kubeconfig` flag, and main declared
a second one, so the binary died with `flag redefined: kubeconfig` before it did
anything. Every gate passed: nothing in the test path calls main. The binary now
reuses whichever flag is registered, and cmd has tests that exercise the default
FlagSet where the collision happened.

The walzen infrastructure repository installs the release through a terragrunt
unit and consumes it from its backup module, described in
[docs/integration.md](docs/integration.md).
[.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md](.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md)
is the cluster run, whose install and terragrunt canary steps have now been
performed.

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
| [docs/api.md](docs/api.md) | the custom resources: every field, their status, and a worked example |
| [docs/restores.md](docs/restores.md) | what fills a claim, what overwrites one, and which to reach for |
| [docs/packaging.md](docs/packaging.md) | the release: image, rendered manifests, Helm chart, and what each asset has to contain |
| [docs/integration.md](docs/integration.md) | how the infrastructure repository installs and consumes it |
| [docs/decisions.md](docs/decisions.md) | why this shape rather than the alternatives that were rejected |
| [docs/implementation-plan.md](docs/implementation-plan.md) | the work, in order, with what proves each step |
