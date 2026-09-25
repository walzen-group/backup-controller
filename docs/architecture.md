# Architecture

## Three parts in one binary

| Part | Package | Acts on |
| --- | --- | --- |
| the populator | internal/populator, on lib-volume-populator | a claim whose `dataSourceRef` names a VolumeRestore, while it is Pending |
| the run manager | internal/runs, on controller-runtime | BackupRun and RestoreRun objects, and the scheduler that creates a BackupRun at each tick of a Namespace's `backup.wlz.li/schedule` |
| the bootstrap webhook | internal/bootstrap | a CloudNativePG Cluster on CREATE, and a recovered one on UPDATE |

internal/volsync builds the ReplicationDestinations both the populator and a
RestoreRun create, and internal/restic reads a repository's snapshots for the
restore checks and each BackupRun's `snapshotTime`. Databases below covers the
database side of all three parts; the sections after it are about the
populator, and the volume runs are in [namespace-backups.md](namespace-backups.md).

## Objects the controller writes

In the app's namespace:

| Object | Written by | Lifetime |
| --- | --- | --- |
| BackupRun `scheduled-<yyyymmdd-hhmm>` | the scheduler, once per tick | deleted 30 days after it finishes |
| Kueue Workload, one per run, named in the run's `status.workload` | each BackupRun, which also writes its `PodsReady` condition | deleted when the run ends |
| ReplicationSource named after each claim marked `backup.wlz.li/enabled` | each BackupRun, owned by the claim and labelled `app.kubernetes.io/managed-by: backup-controller` | stays, and goes with the claim |
| CloudNativePG Backup `<cluster>-<suffix>` | each BackupRun, one per Cluster marked `backup.wlz.li/enabled` | stays, as CloudNativePG's backup record |
| ReplicationDestination | an in-place RestoreRun | deleted when the restore ends |
| VolumeRestore and a scratch claim named by `into:` | a RestoreRun with `into:`, both owned by the run | deleted with the RestoreRun, the claim's dataset included |

In the controller's namespace, for each claim the populator fills: a copy of the
repository Secret and a ReplicationDestination, both deleted when the fill ends,
and the prime claim the library creates and hands over.

On objects the controller does not own:

| Object | Write | When |
| --- | --- | --- |
| Deployment or StatefulSet marked `backup.wlz.li/quiesce` | `spec.replicas` to 0, then back to the recorded value | during a run with `all: true` |
| the workload's Flux Kustomization | `spec.suspend` on, then off, only when the run found it running | the same |
| a new CloudNativePG Cluster | `bootstrap` swapped for `recovery`, an `externalClusters` entry, the `cnpg.io/skipEmptyWalArchiveCheck` annotation, and `backup.wlz.li/restore-run` when a run waits for it | on CREATE, in the webhook |
| a recovered Cluster | the `initdb` a GitOps tool applies again, dropped | on UPDATE, in the webhook |
| a Cluster in a database RestoreRun | deleted, so it is created again and recovered | when the restore starts |
| a claim being filled | the library's `backup.wlz.li` annotations and finalizer | while the fill runs |

Its own BackupRuns and RestoreRuns carry a finalizer, which releases whatever a
run changed when the run fails, times out or is deleted. The sources it writes
make VolSync create the clone claim `volsync-<claim>-src`, the restic cache
claims and the mover Jobs; those are VolSync's objects.

## Databases

The controller moves no database byte. The barman-cloud plugin archives WAL and
writes base backups; the controller asks it for a base backup, reads what it
wrote, and decides how a new Cluster bootstraps. It reads CloudNativePG objects
as unstructured, so it carries no dependency on CloudNativePG's Go module.

### Where a database's backups are

Every database operation starts by finding the archive. `bootstrap.Archiver`
reads the Cluster's `spec.plugins`, takes the `barman-cloud.cloudnative-pg.io`
entry with `isWALArchiver: true`, and returns its `barmanObjectName` and
`serverName`. An unset `serverName` means the Cluster's own name, the same
default barman uses. A Cluster with no such entry archives nowhere, and every
database operation leaves it alone.

`bootstrap.ResolveLocation` then turns the store's name into an S3 location:

| Read | From | Becomes |
| --- | --- | --- |
| the ObjectStore | the Cluster's namespace, by `barmanObjectName` | |
| `spec.configuration.destinationPath` | `s3://prod-cluster-backup-cnpg/canary-namespace-backup` | bucket `prod-cluster-backup-cnpg`, and the prefix `canary-namespace-backup/canary-namespace-backup-pg` once the server name is appended |
| `spec.configuration.endpointURL` | `https://…` or `http://…`; a bare host reads as HTTPS | the S3 endpoint |
| `s3Credentials.accessKeyId`, `secretAccessKey` | the Secret and keys they name | the key pair |
| `endpointCA`, when present | the Secret and key it names | a PEM bundle added to the image's public roots |

`bootstrap.S3Prober` answers two questions at that location with the minio
client. HasBaseBackup lists `<prefix>/base/` with MaxKeys 1, so a store holding
years of backups costs one object. BaseBackups lists the whole of `base/`, reads
every `backup.info`, keeps the ones whose `status` is `DONE`, and orders them by
`end_time`. barman-cloud writes no `backup_id` into backup.info, so the ID is the
directory's name, such as `20260924T221544`.

### A base backup

A run with `database:` or `all: true` has one item per enabled Cluster. For
each, it:

1. skips the Cluster when it carries `cnpg.io/hibernation: "on"`, because
   CloudNativePG fails a Backup of a hibernated Cluster and the Backup stays
   failed after it wakes;
2. creates a CloudNativePG Backup named `<cluster>-<first 8 characters of the
   run's UID>`, with `method: plugin` and `pluginConfiguration.name:
   barman-cloud.cloudnative-pg.io`, labelled
   `app.kubernetes.io/managed-by: backup-controller`. The name comes from the
   run, so a restarted controller finds the Backup it made;
3. reads the Backup's `status.phase` on every reconcile, and marks the item
   Succeeded on `completed`, or Failed with `status.error` on `failed`.

The Backup stays after the run; it is CloudNativePG's record of the base backup,
and the plugin's retention window decides when the backup itself is deleted.

### A database restore

```mermaid
sequenceDiagram
    autonumber
    participant run as RestoreRun
    participant cnpg as CloudNativePG
    participant owner as Flux or tofu
    participant hook as bootstrap webhook

    run->>run: checks: BaseBackups at the location,<br/>one finished at or before restoreAsOf
    Note over run: volumes in the run restore first;<br/>a failed one leaves the databases running
    run->>run: marks the item Deleted
    run->>cnpg: deletes the Cluster
    Note over run: Waiting: recreate <cluster><br/>to finish the restore
    owner->>hook: creates the Cluster again
    hook->>hook: finds this run waiting, item Deleted
    hook-->>owner: bootstrap.recovery to restoreAsOf,<br/>backup.wlz.li/restore-run: <run>
    run->>run: sees the annotation, item Recovering
    cnpg-->>run: phase "Cluster in healthy state"
    run->>run: item Succeeded
```

The item is marked Deleted before the Cluster is deleted, because the webhook
recovers a Cluster for a run only when that run's item says Deleted. A Cluster
that comes back without `backup.wlz.li/restore-run` naming the run was not
recovered for it, so the run deletes it again. A recovered Cluster deleted
before it turns healthy fails the item.

### A Cluster being created

The webhook's checks run in this order: dry-run requests pass unchanged, then
the `backup.wlz.li/bootstrap: initdb` opt-out, then a Cluster that archives
nowhere, then ResolveLocation, whose failure refuses the Cluster. Next come the
shared-archive check across every Cluster on the cluster, the waiting RestoreRun
and a recovery the Cluster declares itself, the target from the run or from
`backup.wlz.li/restore-as-of`, HasBaseBackup, and BaseBackups when there is a
target. [restores.md](restores.md) has what each outcome does.

A Cluster it recovers gets, in one patch:

| Field | Value |
| --- | --- |
| `spec.bootstrap.recovery` | `source: backup-controller`, `recoveryTarget.targetTime` when there is a target, and the `database`, `owner` and `secret` its `initdb` named |
| `spec.bootstrap.initdb` | removed |
| `spec.externalClusters` | an entry `backup-controller` naming the same plugin, `barmanObjectName` and `serverName` |
| `cnpg.io/skipEmptyWalArchiveCheck` | `enabled`, so the database archives into the prefix it recovered from |
| `backup.wlz.li/restore-run` | the run's name, when a RestoreRun waits for it |

Carrying `database` and `owner` over matters: CloudNativePG defaults a
recovery's database and owner to `app`, so a Cluster created with database
`canary` would come back with its data in `canary`, an empty `app` beside it,
and the `<cluster>-app` Secret pointing at the empty one.

On UPDATE of a Cluster whose stored recovery has `source: backup-controller`,
the webhook drops the `initdb` a GitOps tool applies from its source again,
which CloudNativePG would otherwise refuse as a second bootstrap method.

## Built on lib-volume-populator

The controller is a thin provider on top of
[kubernetes-csi/lib-volume-populator](https://github.com/kubernetes-csi/lib-volume-populator),
which owns the whole PersistentVolumeClaim dance. Its `populator-machinery`
package exposes two entry points:

```go
func RunController(masterURL, kubeconfig, imageName, httpEndpoint, metricsPath,
  namespace, prefix string, gk schema.GroupKind, gvr schema.GroupVersionResource,
  mountPath, devicePath string, populatorArgs func(bool, *unstructured.Unstructured)
  ([]string, error))

func RunControllerWithConfig(vpcfg VolumePopulatorConfig)
```

Use the second. `VolumePopulatorConfig` takes either a `PodConfig` or a
`ProviderFunctionConfig`, and this controller takes the second of those, which
is what keeps a pod of our own out of the design entirely:

| Callback | This controller's implementation |
| --- | --- |
| `PopulateFn` | create the ReplicationDestination pointed at the prime claim |
| `PopulateCompleteFn` | report true when the destination's `status.lastManualSync` matches the trigger it was given |
| `PopulateCleanupFn` | delete the ReplicationDestination |

Each callback receives `PopulatorParams`, which carries the Kubernetes client,
the original claim, the prime claim, the storage class, the data source object
and an event recorder. go.mod pins the library at
`github.com/kubernetes-csi/lib-volume-populator/v3 v3.3.0`.

## What the library does around those three calls

| Step | Detail |
| --- | --- |
| watches | claims whose `dataSourceRef` names the GroupKind the controller registers |
| creates the prime claim | named `prime-<uid of the app's claim>`, in the controller's own namespace, with the app claim's access modes, resources and storage class, and no data source |
| carries the node over | when the storage class binds WaitForFirstConsumer, the app claim's `volume.kubernetes.io/selected-node` annotation is copied onto the prime claim |
| calls the provider | `PopulateFn`, then `PopulateCompleteFn` until it returns true |
| rebinds | once the prime claim is bound, the library patches the PersistentVolume's `claimRef` to name the app's claim, and records the data source reference in an annotation on the PV |
| cleans up | deletes the prime claim and calls `PopulateCleanupFn` |

A mutator hook can alter the prime claim before it is created. This controller
does not need one today; [decisions.md](decisions.md) records why the storage
class is copied rather than chosen.

## The node the volume lands on

The copied `selected-node` annotation is the reason this design also settles a
placement problem the snapshot path has.

With VolSync's populator, two independent decisions pick a worker. The scheduler
picks one for the app's pod and writes it onto the claim; the restore mover is
placed by its own affinity and the clone is made wherever it ran. They can
disagree, and the volume wins, so the pod follows the volume rather than the
schedule. The walzen test cluster showed exactly that on 2026-09-13: a claim
annotated `selected-node: talos-unraid-w-2` whose PersistentVolume sat on
talos-unraid-w-1.

Here the annotation is carried onto the prime claim, so the volume is created on
the worker the scheduler chose for the pod, and the mover follows the volume it
has to mount. One decision, made once.

## Object flow for one restore

```mermaid
sequenceDiagram
    autonumber
    participant sched as scheduler
    participant lib as populator library
    participant ctl as backup-controller
    participant vs as VolSync

    Note over sched: claim notes-data is Pending,<br/>its dataSourceRef names a VolumeRestore
    sched->>sched: tries to place the app's pod,<br/>annotates the claim with selected-node

    lib->>lib: creates prime-<uid> in backup-system<br/>same class, size and selected-node, no data source
    lib->>ctl: PopulateFn

    ctl->>ctl: copies the repository Secret into backup-system
    ctl->>vs: creates ReplicationDestination restore-<uid><br/>copyMethod Direct, destinationPVC prime-<uid>
    vs->>vs: the mover runs, queued like every other mover
    vs-->>ctl: status.lastManualSync == <uid>

    lib->>ctl: PopulateCompleteFn
    ctl-->>lib: true

    lib->>lib: patches the PersistentVolume's claimRef<br/>from prime-<uid> to notes-data
    lib->>ctl: PopulateCleanupFn
    ctl->>vs: deletes the ReplicationDestination
    ctl->>ctl: deletes the copied Secret
    lib->>lib: deletes the prime claim

    Note over sched: claim notes-data is Bound,<br/>holding the restored data
```

Every object the restore created is gone by the end. What survives is the
PersistentVolume, now bound to the app's claim, and it is an ordinary dataset
with no origin.

## Why the repository Secret is copied

VolSync resolves `spec.restic.repository` as a Secret in the ReplicationDestination's
own namespace. The prime claim lives in the controller's namespace, so the
destination does too, and the app's repository Secret is in the app's namespace.

The controller copies that Secret into its own namespace for the length of the
restore and deletes it in `PopulateCleanupFn`. The copy is named for the claim's
UID, so two restores never collide, and it exists only while a restore is
running.

[decisions.md](decisions.md) records the alternative that was weighed, which is
a repository Secret that lives in the controller's namespace from the start.

## Failure behaviour

| Case | What happens |
| --- | --- |
| the repository has no snapshot yet, a first deploy | VolSync completes with nothing to write; the prime claim binds empty, the app starts on an empty volume, which matches what the snapshot path does today |
| the restore fails | `PopulateCompleteFn` keeps returning false, the prime claim never binds, the app's claim stays Pending and its pod does not start. The reason is in the ReplicationDestination's status and events |
| the controller is down | claims stay Pending. Nothing is half-written and no app starts on an empty volume |
| the controller restarts mid-restore | every object is named from the app claim's UID, so the next reconcile finds the existing destination rather than creating a second one |
| two apps restore at once | each destination carries the backup queue label, so Kueue admits them the way it admits every other mover |
| the app's claim is deleted while the app runs | the pod loses its volume and stays down until the refill completes, which is what deleting a claim already does |
| the app's claim is deleted mid-restore | the library's own garbage collection removes the prime claim; `PopulateCleanupFn` removes the destination and the Secret copy |

## What runs where for one fill

| Object | Namespace | Lifetime |
| --- | --- | --- |
| the controller Deployment | the controller's namespace | always |
| the webhook Service, its cert-manager Issuer and Certificate, and the MutatingWebhookConfiguration | the controller's namespace, and cluster-scoped for the configuration | always |
| the VolumeRestore | the app's namespace | as long as the app declares it |
| the prime claim | the controller's namespace | one restore |
| the ReplicationDestination | the controller's namespace | one restore |
| the copied repository Secret | the controller's namespace | one restore |
| the mover's restic metadata cache claim | the controller's namespace | one restore, and VolSync deletes it with the destination |
| the restored PersistentVolume | cluster-scoped | the life of the app's claim |
