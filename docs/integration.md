# Integration with the infrastructure repository

The consumer is walzen-group's infrastructure repository. It installs each
release through a terragrunt unit and declares backups on both of its delivery
paths, Flux apps and terragrunt units. Its own docs/cluster/backups/ is the
admin's guide; this page says what that repository writes for the controller
and why.

## How the install arrives

kuport is the pattern it copies:

| Piece | kuport's | backup-controller's |
| --- | --- | --- |
| module | `modules/networking/kuport/opentofu` | `modules/cluster/backup-controller/opentofu` |
| unit | `environments/*/networking/kuport` | `environments/*/cluster/backup-controller` |
| version input | `kuport_version` | `backup_controller_version: "v0.5.4"`, kept current by a Renovate comment |

The unit depends on networking/cilium, cluster/volsync, cluster/zfs-localpv and
cluster/kueue, so the ReplicationDestination kind it creates objects against, the
storage classes it provisions into and the ClusterQueue its LocalQueue names all
exist first.

The module downloads the release's rendered manifest and applies it in three
groups:

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

CRDs are applied first with a `wait_for` on `Established`, because a kind that
is not servable yet cannot be created in the same apply. The workload is applied
with `wait_for_rollout = false`, because a controller that cannot come up would
otherwise hold the apply open for the provider's whole timeout. The `http` data
source carries a postcondition on `status_code == 200`, so a version string
naming a release that does not exist fails with a message naming the asset.

### The two Kueue objects the module also writes

A fill's mover pod runs in the controller's namespace, beside the prime claim it
mounts. Kueue reaches a pod only when two conditions hold in that pod's own
namespace, and the release manifest satisfies neither for backup-system:

| Condition | Where it comes from |
| --- | --- |
| the namespace carries `kueue-managed=true` | the kueue unit's `managedJobsNamespaceSelector` matches that label, and the release's Namespace document has no labels but its own |
| a LocalQueue of the name in the mover's `queue-name` label exists there | backup-system is nobody's app namespace, so no app writes one |

Without the label, Kueue's pod webhook never sees the mover, so it runs at once
and outside the backup quota. Without the LocalQueue, a labelled mover is held
for a queue that does not resolve and is never admitted, and the claim stays
Pending with nothing on it saying why. The module merges the label into the
release's Namespace document and creates the LocalQueue from its `queue_name`
input.

## What a namespace carries

Both delivery paths write the same annotations and objects:

| Object | Carries | Flux app | terragrunt unit |
| --- | --- | --- | --- |
| Namespace | `kueue-managed`, `backup.wlz.li/schedule`, and optionally `backup.wlz.li/timeout` and `backup.wlz.li/prune-interval-days` | the backups/queue component, from `BACKUP_SCHEDULE`, `BACKUP_TIMEOUT` and `BACKUP_PRUNE_INTERVAL_DAYS` | the module that writes the Namespace, from the unit's inputs |
| LocalQueue | the namespace's queue | the backups/queue component | the volsync repository or cloudnative-pg backup module |
| claim | `backup.wlz.li/enabled` and the `retain-` annotations; a `dataSourceRef` to its VolumeRestore for a dynamic claim | the claim's pvc.yaml | the module that writes the claim, from the repository module's `claim_annotations` output |
| VolumeRestore, restic Secret | the repository | the backups/pvc component | modules/cluster/volsync/opentofu/repository |
| Cluster | `backup.wlz.li/enabled` | the backups/postgres component | modules/cluster/cloudnative-pg/opentofu/backup's `annotations` output |
| ObjectStore, its Secret | the archive | the backups/postgres component | the same module |

Neither path writes a ReplicationSource or a base backup trigger; the controller
writes the sources and requests the base backups at each run.

A Flux app in a namespace another unit writes gets its schedule from that unit.
Grafana runs in monitoring, which the kube-prometheus unit writes, so that unit's
`backup_schedule` input carries it. A fixed-name claim a Helm chart creates,
such as seaweedfs' filer claim, is annotated by its unit through server-side
apply with `apply_only`, and its VolumeRestore carries the claim's own name.

## What has to exist first

The bootstrap webhook acts on a Cluster only at the moment it is created, and
CloudNativePG runs `initdb` at once when nothing rewrote the bootstrap. A
Cluster created before the webhook is registered comes up empty beside a full
archive. Once registered, the webhook refuses a Cluster it cannot decide on, so
the gap is the first install on a new cluster.

| Path | What holds it back |
| --- | --- |
| terragrunt deployment units | environments/\<env\>/deployments.hcl makes every deployment depend on every cluster unit |
| cluster/authentik/db, cluster/seaweedfs | a `dependencies` entry on cluster/backup-controller |
| Flux apps | the `backup-controller-ready` Kustomization in flux/environments/\<env\>/, which health-checks the controller's Deployment; each app Kustomization lists it in `dependsOn` |

The Flux unit cannot depend on backup-controller in terragrunt: volsync writes
its substitution sources into flux-system and backup-controller depends on
volsync, so that edge would close a cycle.

## Where the runs were proven

The infrastructure repository's docs/agent/investigations/namespace-backups-canary.md
records every mode on the prod canary on 2026-09-24, and
[namespace-backups.md](namespace-backups.md) quotes the same runs.
