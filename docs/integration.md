# Integration with the infrastructure repository

The consumer is walzen-group's infrastructure repository. It installs each
release through a terragrunt unit. It declares backups on both of its delivery
paths, Flux apps and terragrunt units. Its own docs/cluster/backups/ is the
admin's guide. This page tells what that repository writes for the controller,
and why.

## How the install arrives

kuport is the pattern it copies:

| Piece | kuport's | backup-controller's |
| --- | --- | --- |
| module | `modules/networking/kuport/opentofu` | `modules/cluster/backup-controller/opentofu` |
| unit | `environments/*/networking/kuport` | `environments/*/cluster/backup-controller` |
| version input | `kuport_version` | `backup_controller_version: "v0.5.4"`, kept current by a Renovate comment |

The unit depends on networking/cilium, cluster/volsync, cluster/zfs-localpv and
cluster/kueue. Thus these objects exist first:

- The ReplicationSource kind that its runs write.
- The storage classes that it provisions into.
- The ClusterQueue that its LocalQueue names.

From v0.9.0, the volsync unit also gives it the restic image that its restore
Job runs. The module appends the image to the controller's args as
`--restore-image`. [upgrading.md](upgrading.md#step-2-edit-the-infra-units)
has the change.

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

The module applies the CRDs first, with a `wait_for` on `Established`. The
reason is that the same apply cannot create an object of a kind that the API
server cannot serve yet. The module applies the workload with
`wait_for_rollout = false`. Otherwise, a controller that cannot start would
keep the apply open for the whole timeout of the provider. The `http` data
source carries a postcondition on `status_code == 200`. Thus a version string
that names a release that does not exist fails with a message that names the
asset.

### The two Kueue objects the module also writes

The mover pod of a fill runs in the controller's namespace, beside the prime
claim it mounts. Kueue reaches a pod only when two conditions are true in the
namespace of that pod. The release manifest satisfies neither for
backup-system:

| Condition | Where it comes from |
| --- | --- |
| the namespace carries `kueue-managed=true` | the kueue unit's `managedJobsNamespaceSelector` matches that label. The release's Namespace document has no labels but its own |
| a LocalQueue of the name in the mover's `queue-name` label exists there | backup-system is the namespace of no app, so no app writes one |

Without the label, Kueue's pod webhook never sees the mover. The mover then runs
immediately and outside the backup quota. Without the LocalQueue, Kueue holds a
labelled mover for a queue that does not resolve and never admits it. The claim
then stays Pending, and nothing on it tells why. The module merges the label
into the release's Namespace document. It creates the LocalQueue from its
`queue_name` input.

## What a namespace carries

Both delivery paths write the same annotations and objects:

| Object | Carries | Flux app | terragrunt unit |
| --- | --- | --- | --- |
| Namespace | `kueue-managed`, `backup.wlz.li/schedule`, and optionally `backup.wlz.li/timeout` and `backup.wlz.li/prune-interval-days` | the backups/queue component, from `BACKUP_SCHEDULE`, `BACKUP_TIMEOUT` and `BACKUP_PRUNE_INTERVAL_DAYS` | the module that writes the Namespace, from the unit's inputs |
| LocalQueue | the namespace's queue | the backups/queue component | the volsync repository or cloudnative-pg backup module |
| claim | `backup.wlz.li/enabled` and the `retain-` annotations. A dynamic claim also has a `dataSourceRef` to its VolumeRestore | the claim's pvc.yaml | the module that writes the claim, from the repository module's `claim_annotations` output |
| VolumeRestore, restic Secret | the repository | the backups/pvc component | modules/cluster/volsync/opentofu/repository |
| Cluster | `backup.wlz.li/enabled` | the backups/postgres component | modules/cluster/cloudnative-pg/opentofu/backup's `annotations` output |
| ObjectStore, its Secret | the archive | the backups/postgres component | the same module |

Neither path writes a ReplicationSource or a base backup trigger. The controller
writes the sources and requests the base backups at each run.

A Flux app in a namespace that another unit writes gets its schedule from that
unit. Grafana runs in monitoring, which the kube-prometheus unit writes. Thus
the `backup_schedule` input of that unit carries the schedule. A Helm chart can
create a claim with a fixed name, such as the filer claim of seaweedfs. Its unit
annotates that claim through server-side apply with `apply_only`. The
VolumeRestore of the claim carries the claim's own name.

## What has to exist first

The bootstrap webhook acts on a Cluster only at the moment of its creation.
CloudNativePG runs `initdb` immediately if nothing rewrote the bootstrap. A
Cluster that someone creates before the webhook registration starts empty
beside a full archive. After its registration, the webhook refuses a Cluster
that it cannot decide on. Thus the gap is the first install on a new cluster.

| Path | What holds it back |
| --- | --- |
| terragrunt deployment units | environments/\<env\>/deployments.hcl makes every deployment depend on every cluster unit |
| cluster/authentik/db, cluster/seaweedfs | a `dependencies` entry on cluster/backup-controller |
| Flux apps | the `backup-controller-ready` Kustomization in flux/environments/\<env\>/, which does a health check of the controller's Deployment. Each app Kustomization lists it in `dependsOn` |

The controller's Deployment reports Ready only when its webhook server accepts
TLS connections. The readiness probe on `/readyz` does this check.
[architecture.md](architecture.md#process-probes-and-rollout) describes the
probe. Thus `backup-controller-ready` keeps the apps back until the webhook can
answer.

The Flux unit cannot depend on backup-controller in terragrunt. volsync writes
its substitution sources into flux-system, and backup-controller depends on
volsync. Thus that edge would close a cycle.

## Where the runs were proven

The infrastructure repository's docs/agent/investigations/namespace-backups-canary.md
records every mode on the prod canary on 2026-09-24.
[namespace-backups.md](namespace-backups.md) quotes the same runs.
