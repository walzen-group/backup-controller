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
| version input | `kuport_version: "v0.4.0"` | `backup_controller_version: "v0.1.0"` |

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

## The unit

`environments/test/cluster/backup-controller/inputs.yaml`:

```yaml
# backup_controller_version: the release tag. The unit downloads that release's
# rendered manifests, which hold the CRD, the RBAC and the image pinned by
# digest, so this one string moves the schema and the controller together.
# renovate: datasource=github-releases depName=walzen-group/backup-controller
backup_controller_version: "v0.1.0"
```

The renovate comment is what keeps the version current, and it is the same
comment kuport's inputs.yaml carries.

`terragrunt.hcl` takes a dependency on the volsync unit and the zfs-localpv
unit, so the CRD it creates objects against and the storage class it provisions
into both exist first.

## What changes in the Flux backup component

The component at `flux/templates/backups/pvc` writes a restic Secret, a
ReplicationSource and a ReplicationDestination for one claim, and the claim's
four annotations configure them. Three of those objects stay exactly as they
are. The destination and two annotations go.

| Today | After |
| --- | --- |
| the component renders a ReplicationDestination in Snapshot mode | the component renders a VolumeRestore |
| the claim's `dataSourceRef` names the ReplicationDestination | it names the VolumeRestore |
| `backup.walzen.org/restore-trigger` re-runs the destination | gone; deleting the claim is the trigger |
| `backup.walzen.org/volume-at` places the restore mover | gone; the scheduler's node is carried onto the prime claim |
| `at-database-worker`, `at-pinned-worker` and `volume-at-label` components place the mover | gone, with the annotation they read |
| the ReplicationSource, the restic Secret, the queue component | unchanged |

The two remaining annotations, `schedule` and `retain-last`, still configure the
ReplicationSource through the same kustomize replacements.

That is a large simplification of the component and of the documentation around
it, and it is worth doing in one change rather than leaving both paths in place.
Both paths can coexist while it is proven, because a claim chooses which one it
is on by what its `dataSourceRef` names.

## Docs in the infrastructure repository that have to change

Search before writing. Every file naming the removed annotations or the
destination's role in a restore is part of the change:

```
grep -rn 'volume-at\|restore-trigger\|dataSourceRef' --include='*.md' --include='*.yaml' .
```

| File | What changes |
| --- | --- |
| `docs/cluster/backups-flux.md` | the per-volume option table, the claim field table, the whole "Why the volume has to follow the database" section, and the three-value volume-at section |
| `docs/cluster/backups.md` | "What the automatic refill occupies on disk", which describes the clone and its pinned blocks |
| `docs/cluster/flux.md` | the substitution and claim tables |
| `docs/getting-started/sections/*` | the tutorials, which walk a reader through the annotations |
| `modules/cluster/zfs-localpv/opentofu/README.md` | the passage on clones holding their source snapshot open, which stops being something an app hits |
| `docs/agent/specs/backup-docs-and-tutorials-restructure.md` | the plan that assumes the snapshot path |

## Proving it on the test cluster

The canary apps are the rehearsal. `flux/templates/tutorials/` holds
app-with-two-volumes, app-with-a-postgres-database-and-volumes and
app-pinned-to-a-worker, each running as a canary under `flux/test/`.

| Step | Check |
| --- | --- |
| install the unit | `kubectl get crd volumerestores.backup.wlz.li` and the controller's Deployment Available |
| move one canary's claim to a VolumeRestore | the claim binds, the app starts, `kubectl get zfsvolume` shows the app's volume with an empty `snapname` |
| delete that claim | Flux recreates it, the controller refills it, the app comes back with its data |
| read the pool | `zfs list -t all -o name,used,refer,origin -r zfspv-pool` shows no clone and no snapshot for that volume |
| watch the queue | the restore's mover pod is admitted by Kueue like every other mover |

The last two are the whole point of the project. Record the before and after
numbers in the infrastructure repository's backups documentation, because the
paragraph that sent everyone here stated a mechanism with no magnitude.
