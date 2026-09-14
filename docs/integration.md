# Integration with the infrastructure repository

The consumer is walzen-group's infrastructure repository, checked out at
~/repos/walzen-2-infra-test in the environment this was designed in. This page
says what has to be written there, and it is written so the work can be done
without reading the design conversation.

## How the install arrives

kuport is the pattern to copy, and it exists in that repository today:

| Piece | kuport's | backup-controller's |
| --- | --- | --- |
| module | `modules/networking/kuport/opentofu` | `modules/cluster/backup-controller/opentofu` |
| unit | `environments/*/networking/kuport` | `environments/*/cluster/backup-controller` |
| version input | `kuport_version: "v0.4.0"` | `backup_controller_version: "v0.1.1"` |

The module belongs under `cluster/` rather than `networking/`, beside volsync
and zfs-localpv, because it is a storage-path component and it has to be applied
after both.

## The module

Copy `modules/networking/kuport/opentofu/main.tf` and change the names. It is
forty lines and every one of them carries:

```hcl
locals {
  # The release is tagged with a v and its assets are named without one.
  asset_version = trimprefix(var.backup_controller_version, "v")
  release_url   = "https://github.com/${var.repository}/releases/download/${var.backup_controller_version}/backup-controller-${local.asset_version}.yaml"

  documents = data.kubectl_file_documents.release.manifests

  crds       = { for id, doc in local.documents : id => doc if yamldecode(doc).kind == "CustomResourceDefinition" }
  namespaces = { for id, doc in local.documents : id => doc if yamldecode(doc).kind == "Namespace" }
  workload   = { for id, doc in local.documents : id => doc if !contains(["CustomResourceDefinition", "Namespace"], yamldecode(doc).kind) }
}
```

The three groups matter. CRDs are applied first with a `wait_for` on
`Established`, because a kind that is not servable yet cannot be created in the
same apply. The workload is applied with `wait_for_rollout = false`, because a
controller that cannot come up would otherwise hold the apply open for the
provider's whole timeout; the rollout is checked after the apply, in the
module's README.

The `http` data source carries a postcondition on `status_code == 200`, so a
version string naming a release that does not exist fails with a message naming
the asset rather than a parse error.

### The two Kueue objects the module also writes

Every restore's mover pod runs in the controller's namespace, beside the prime
claim it mounts, where the old path ran it in the app's namespace. Kueue reaches
a pod only when two conditions hold in that pod's own namespace, and the release
manifest satisfies neither for backup-system:

| Condition | Where it comes from today |
| --- | --- |
| the namespace carries `kueue-managed=true` | the kueue unit's `managedJobsNamespaceSelector` matches that label, and the release's Namespace document has no labels but its own |
| a LocalQueue of the name in the mover's `queue-name` label exists there | modules/cluster/volsync/opentofu/repository writes one per app namespace, and backup-system is nobody's app namespace |

Each missing piece fails differently. Without the label, Kueue's pod webhook
never sees the mover, so it runs immediately and outside the backup quota, and
the cluster's limit on concurrent movers stops meaning anything. Without the
LocalQueue, a labelled mover is held for a queue that does not resolve and is
never admitted, so the claim stays Pending with nothing on it saying why.

The module writes both: it merges the label into the release's Namespace
document before applying it, and creates the LocalQueue from a `queue_name`
input naming the ClusterQueue the kueue unit owns.

## The unit

`environments/test/cluster/backup-controller/inputs.yaml`:

```yaml
# backup_controller_version: the release tag. The unit downloads that release's
# rendered manifests, which hold the CRD, the RBAC and the image pinned by
# digest, so this one string moves the schema and the controller together.
# renovate: datasource=github-releases depName=walzen-group/backup-controller
backup_controller_version: "v0.1.1"

# queue_name: the ClusterQueue the kueue unit owns. The unit labels
# backup-system kueue-managed and writes a LocalQueue of this name there, so a
# restore's mover is admitted from the same quota as every other mover.
queue_name: backups
```

The renovate comment is what keeps the version current, and it is the same
comment kuport's inputs.yaml carries.

`terragrunt.hcl` takes a dependency on the volsync unit and the zfs-localpv
unit, so the CRD it creates objects against and the storage class it provisions
into both exist first.

## What changes on the terragrunt backup path

modules/cluster/volsync/opentofu/repository writes the restic Secret, the
ReplicationSource, the LocalQueue and, for a dynamic claim, a
ReplicationDestination in Snapshot mode whose `status.latestImage` the claim's
`dataSourceRef` reads. That last object is the one this controller replaces, and
the module gains two independent switches in place of the single one it has:

| Switch | Writes | Acts when |
| --- | --- | --- |
| `populate` | a VolumeRestore, and the `data_source_ref` output naming it | a claim is created |
| `restore_into` | a ReplicationDestination in Direct mode, on a changed `restore_trigger` | the trigger changes, and the mover overwrites the named claim in place |

A dynamic claim sets `populate` and comes back filled whenever it is recreated.
A fixed-name claim leaves `populate` off, because a claim bound to a named
volume is never populated. Either shape may set `restore_into`, which is how a
volume is walked back to an older snapshot without being deleted, so the
`restore:` block in a unit's inputs.yaml keeps the meaning it has today on both.

The module sets `cacheStorageClassName` on the VolumeRestore from the same
`ephemeral_storage_class` input its own destinations use. Left unset, the
mover's cache claim comes from the cluster's default class, which on this
cluster is zfs and reclaims Retain.

## What changes in the Flux backup component

The component at `flux/templates/backups/pvc` writes a restic Secret, a
ReplicationSource and a ReplicationDestination for one claim, and the claim's
four annotations configure them. Three of those objects stay exactly as they
are. The destination and two annotations go.

| Today | After |
| --- | --- |
| the component renders a ReplicationDestination in Snapshot mode | the component renders a VolumeRestore |
| the claim's `dataSourceRef` names the ReplicationDestination | it names the VolumeRestore |
| `backup.walzen.org/restore-trigger` re-runs the destination | gone; see below for what covers each operation it served |
| `backup.walzen.org/volume-at` places the restore mover | gone; the scheduler's node is carried onto the prime claim |
| `at-database-worker`, `at-pinned-worker` and `volume-at-label` components place the mover | gone, with the annotation they read |
| the ReplicationSource, the restic Secret, the queue component | unchanged |

The two remaining annotations, `schedule` and `retain-last`, still configure the
ReplicationSource through the same kustomize replacements.

That is a large simplification of the component and of the documentation around
it, and it is worth doing in one change rather than leaving both paths in place.
Both paths can coexist while it is proven, because a claim chooses which one it
is on by what its `dataSourceRef` names.

### What replaces the restore trigger

The removed annotation drove one object through three different operations, and
each one now has its own shape. Write all three into the Flux documentation,
because an admin reaching for a restore is reaching for one of them and the
wrong choice discards data.

| To do this | Do this |
| --- | --- |
| rebuild the volume from the newest backup | delete the claim. Flux recreates it, and the controller fills it |
| read an older snapshot beside the live volume | add a second VolumeRestore with `restoreAsOf` set and a second claim naming it, and mount that claim from a throwaway pod |
| write an older snapshot into the claim the app already has | add a ReplicationDestination in Direct mode pointed at that claim, with the workload scaled to zero |

The first discards what the volume holds at that moment, so back the current
state up before reaching for it: setting `volsync.backube/use-copy-trigger` on
the ReplicationSource takes a backup on demand, and the state you are about to
discard becomes a snapshot you can restore later.

The third is unchanged by this project. A Direct-mode restore mounts an existing
claim and overwrites it, and it does not care whether that claim was provisioned
dynamically or bound to a volume by name, so moving an app onto a VolumeRestore
takes nothing away from it.

## Docs in the infrastructure repository that have to change

Search before writing. Every file naming the removed annotations or the
destination's role in a restore is part of the change:

```
grep -rn 'volume-at\|restore-trigger\|dataSourceRef' --include='*.md' --include='*.yaml' .
```

| File | What changes |
| --- | --- |
| `docs/cluster/backups-terragrunt.md` | the "Restore a dynamic volume" section, whose steps refresh a destination's image and then delete the claim by hand |
| `docs/cluster/backups-flux.md` | the per-volume option table, the claim field table, the whole "Why the volume has to follow the database" section, and the three-value volume-at section |
| `docs/cluster/backups.md` | "What the automatic refill occupies on disk", which describes the clone and its pinned blocks |
| `docs/cluster/flux.md` | the substitution and claim tables |
| `docs/getting-started/sections/*` | the tutorials, which walk a reader through the annotations |
| `modules/cluster/zfs-localpv/opentofu/README.md` | the passage on clones holding their source snapshot open, which stops being something an app hits |
| `docs/agent/specs/backup-docs-and-tutorials-restructure.md` | the plan that assumes the snapshot path |

## Proving it on the test cluster

The terragrunt canary comes first. `environments/test/deployments/canary-backup`
is one busybox pod appending a timestamp to a file on every start, on a 1Gi
volume, and it is the only backed-up terragrunt deployment, so a full destroy
and apply of that unit costs nothing and takes a minute. Move it to a dynamic
claim on a VolumeRestore before any app is touched.

| Step | Check |
| --- | --- |
| install the unit | `kubectl get crd volumerestores.backup.wlz.li` and the controller's Deployment Available |
| apply the canary on a dynamic claim | the claim binds, the pod starts, `kubectl get zfsvolume` shows its volume with an empty `snapname` |
| wait for a backup, then destroy and apply the unit | the file comes back holding every line it had, plus one for the new start |
| read the pool | `zfs list -t all -o name,used,refer,origin -r zfspv-pool` shows no clone and no snapshot for that volume, and no cache dataset left over |
| watch the queue | the restore's mover pod is admitted by Kueue like every other mover |

The Flux canaries follow once those five rows pass. `flux/templates/tutorials/`
holds app-with-two-volumes, app-with-a-postgres-database-and-volumes and
app-pinned-to-a-worker, each running under `flux/test/`, and app-with-two-volumes
is the one to move first: it has no database and no node pinning to confuse a
first result.

Record the pool numbers beside the same measurement taken on a volume still on
the snapshot path, and put both in the infrastructure repository's backups
documentation. The paragraph that sent everyone here stated a mechanism with no
magnitude.
