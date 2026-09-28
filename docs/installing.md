# Installing

## What the cluster needs

| Component | Why |
| --- | --- |
| VolSync with restic | the backup movers of the claims |
| CloudNativePG with the barman-cloud plugin | the databases, their base backups and WAL archives |
| Kueue, with a ClusterQueue that covers backup-controller.wlz.li/run | admission of every run; docs/operations.md shows the quota |
| cert-manager | the certificate of the controller's webhook |
| An S3 service | the restic repositories and the barman archives |
| Flux, optional | the controller suspends an app's Kustomization while the app is paused, and waits for Flux to create a deleted Cluster again |

## Install

Each release attaches the whole install as one file, rendered from deploy/
with the image pinned by digest. It holds the CRDs, the backup-system
Namespace, the ServiceAccount, the ClusterRole and its binding, the
Deployment, and the webhook's Service, Issuer, Certificate and
MutatingWebhookConfiguration. The terragrunt module applies the CRDs first and
waits for them to be established, then the Namespace, then the rest.

| Release asset | Contents |
| --- | --- |
| backup-controller-X.Y.Z.yaml | the whole install |
| crds-X.Y.Z.yaml | the CRDs alone |
| backup-controller-X.Y.Z.tgz, and oci://ghcr.io/walzen-group/backup-controller | the Helm chart |

The Deployment needs two settings from the installer: --restore-image with
VolSync's mover image, and --pause while an upgrade runs (docs/operations.md).

The image is ghcr.io/walzen-group/backup-controller, built from scratch with
the public CA roots and Go's zone database. It runs as a non-root user with no
capabilities and a read-only root filesystem.

## What has to exist first

The webhook acts on a Cluster only when the Cluster is created. A Cluster
created before the webhook is registered runs initdb and comes up empty,
even when its archive holds a database. Order the install so the controller
is ready before any Cluster is created:

| Path | Ordering |
| --- | --- |
| terragrunt deployment units | environments/ENV/deployments.hcl makes every deployment depend on every cluster unit |
| terragrunt units with a Cluster | a dependencies entry on cluster/backup-controller |
| Flux apps | the backup-controller-ready Kustomization health-checks the controller's Deployment, and each app Kustomization lists it in dependsOn |

## What a namespace carries

| Object | Carries |
| --- | --- |
| Namespace | backup.wlz.li/schedule, and optionally backup.wlz.li/timeout and backup.wlz.li/prune-interval-days |
| LocalQueue | the namespace's queue, pointing at the ClusterQueue |
| Claim | backup.wlz.li/enabled: "true", the retain annotations, and a dataSourceRef to its VolumeRestore |
| VolumeRestore and the restic Secret | the repository of the claim |
| Deployment or StatefulSet | backup.wlz.li/pause-during-backup: "true" when the app must stop during a backup |
| Cluster | backup.wlz.li/enabled: "true" |
| ObjectStore and its Secret | the archive of the Cluster |

The controller writes the ReplicationSources and the CloudNativePG Backups; no
manifest declares them. The controller namespace also needs a LocalQueue
named after the queue label in the VolumeRestores' moverPodLabels, so Kueue
admits the populator's restore Jobs.

## Permissions

The ClusterRole in deploy/rbac.yaml grants:

| API group | Resources | Verbs | For |
| --- | --- | --- | --- |
| core | persistentvolumeclaims | get, list, watch, create, patch, delete | the populator, into restores |
| core | persistentvolumes, pods, namespaces | read | node placement, paused pods, schedules |
| core | secrets | get, create, delete | reading repositories and archives; the populator's Secret copy |
| core, events.k8s.io | events | create, patch | events on runs and claims |
| storage.k8s.io | storageclasses | read | the populator library |
| backup.wlz.li | backupruns, restoreruns, volumerestores and their status | read and write | the three kinds |
| backup.wlz.li | backupruns/finalizers, restoreruns/finalizers | update | Workloads and restore Jobs that name their run as owner |
| volsync.backube | replicationsources | get, list, watch, create, update, patch | the backup movers |
| postgresql.cnpg.io | backups; clusters | get, create; get, list, delete | base backups; database restores |
| barmancloud.cnpg.io | objectstores | get | where a Cluster archives |
| apps | deployments, statefulsets | get, list, patch | pausing workloads |
| kustomize.toolkit.fluxcd.io | kustomizations | get, patch | suspending Flux during a pause |
| kueue.x-k8s.io | workloads, workloads/status, localqueues | get, create, delete, update, list | admission |
| coordination.k8s.io | leases | get, create, update, delete | the runs' Leases |
| batch | jobs | get, create, delete | the restore Jobs |
