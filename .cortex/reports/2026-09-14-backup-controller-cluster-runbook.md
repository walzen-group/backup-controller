# backup-controller: the cluster runbook

Date: 2026-09-14. Audience: the admin who runs the walzen test cluster.

Milestones 1 through 5 of [docs/implementation-plan.md](../../docs/implementation-plan.md)
are implemented, their gates pass, and v0.1.2 is released. This page is the
cluster run: every step, what it should print, and what a wrong answer means.

## What this runbook proves

A PersistentVolumeClaim filled from a restic repository by VolSync's mover, with
no VolumeSnapshot taken and no clone left behind, on a real cluster with real
data in it. The offline work proved that the controller creates the right
objects. Only the cluster shows that a volume filled this way binds, that the
app starts on it, and that the pool holds one dataset afterwards instead of
three.

The magnitude question is the other half. The paragraph that started this
project described a mechanism with no numbers in it: a clone holds its origin
open, and a volume that rewrites itself drifts toward holding twice its own
size. Nobody has measured that drift on this pool. Step 13 is where the number
comes from, and it matters as much as the passing rows above it.

## Before the first canary

| Prerequisite | What it is for |
| --- | --- |
| The VolumeRestore CRD is applied and Established | the API server cannot serve a kind it has not registered, and the claim's dataSourceRef names that kind |
| VolSync is installed and its ReplicationDestination CRD is served | the controller creates a ReplicationDestination and VolSync's mover does every byte of the restore |
| zfs-localpv is installed and its storage class provisions | the prime claim the library creates needs the same class the app's claim asks for |
| The controller Deployment reports Available | a controller that is not running leaves every claim Pending with no error on the claim |
| The repository Secret is in the canary's namespace | the controller reads it there and copies it into its own namespace for the length of one restore |
| backup-system carries kueue-managed=true and holds a LocalQueue named backups | the mover runs in that namespace now, and Kueue reaches a pod only where both are true |

The cluster/backup-controller unit writes the last row itself, so installing the
unit satisfies it. The rest is the state of a working test cluster.

### Why the Kueue row is new

Every restore's mover pod runs in backup-system, beside the prime claim it
mounts. The snapshot path ran it in the app's namespace, where the volsync
repository module had already labelled the namespace and written a LocalQueue.
Neither exists in backup-system by default, and each one fails its own way.
Without the label, Kueue's webhook never sees the mover, so it runs immediately
and outside the backup quota. Without the LocalQueue, a labelled mover waits for
a queue that does not resolve and is never admitted, so the claim stays Pending
with nothing on it saying why.

## Install

### Step 1: apply the unit

Run this from environments/test/cluster/backup-controller.

```sh
terragrunt apply
```

Expected result: the CRD, the Namespace with its kueue-managed label, the
LocalQueue, the ServiceAccount, the ClusterRole, the ClusterRoleBinding and the
Deployment are created.

The unit downloads the release's rendered manifests over HTTP and applies the
CRD first, waiting on its Established condition, so the kind is servable before
anything creates an object against it. A failure naming a 404 means
backup_controller_version in inputs.yaml points at a release that does not
exist.

### Step 2: confirm the CRD is Established

```sh
kubectl get crd volumerestores.backup.wlz.li -o jsonpath='{.status.conditions[?(@.type=="Established")].status}{"\n"}'
```

Expected result: `True`.

Anything else means the API server rejected the schema. Read the CRD's status
conditions before going on.

### Step 3: wait for the rollout

```sh
kubectl -n backup-system rollout status deployment/backup-controller --timeout=120s
```

Expected result: `deployment "backup-controller" successfully rolled out`.

The unit applies the workload with wait_for_rollout off, so a controller that
cannot come up fails here rather than holding the apply open for the provider's
whole timeout. A rollout that never completes is usually the image: run
`kubectl -n backup-system describe pod -l app.kubernetes.io/name=backup-controller`
and read the events.

### Step 4: confirm the controller is watching

```sh
kubectl -n backup-system logs deployment/backup-controller | head -5
```

Expected result: the line `starting backup-controller: watching backup.wlz.li/v1alpha1 VolumeRestore`.

That line is the binary's own, logged before it hands control to the populator
library, and the envtest run in milestone 4 read the same line. A client error
in its place means the ServiceAccount token or the ClusterRoleBinding is wrong,
so check both before touching a canary.

## The terragrunt canary

environments/test/deployments/canary-backup is one busybox pod appending a UTC
timestamp to /data/hello.txt on every start, on a 1Gi volume. It is the only
backed-up terragrunt deployment, so destroying and applying it takes a minute
and risks nothing. Move it to a dynamic claim before any app is touched.

### Step 5: apply the canary on a dynamic claim

Run this from environments/test/deployments/canary-backup, with `volume` set to
null in inputs.yaml.

```sh
terragrunt apply
```

Expected result: the namespace, a Bound claim, the Deployment, the restic
Secret, the ReplicationSource, the LocalQueue and a VolumeRestore all exist, and
the pod writes its first timestamp.

The repository holds no snapshot on the very first apply, so VolSync completes
with nothing to write and the claim binds empty. The snapshot path behaves the
same way on a first deploy.

### Step 6: confirm this controller filled the claim

```sh
kubectl -n canary-backup get pvc
```

Expected result: the claim is `Bound`, and no claim named `volsync-*-dest` is
listed beside it.

A `volsync-*-dest` claim means the unit is still on VolSync's own populator, so
the dataSourceRef never changed. A claim stuck `Pending` means the controller
never filled it: read the controller's log and the VolumeRestore's Ready
condition, whose message names the ReplicationDestination and the namespace it
is in.

### Step 7: wait for a backup and record the file

```sh
kubectl -n canary-backup get replicationsource canary-backup
```

Expected result: LAST SYNC is within 15 minutes of the apply.

```sh
kubectl -n canary-backup exec deploy/canary-backup -- cat /data/hello.txt
```

Expected result: one timestamp per container start, oldest first. Keep the
output; step 9 compares against it.

### Step 8: destroy the unit

```sh
terragrunt destroy
```

Expected result: the namespace, the claim, the Deployment and the VolSync
objects are gone. A dynamic claim takes its dataset with it, because the zfs
class reclaims the volume with the claim.

The destroy returns before the namespace has finished terminating, and Step 9
fails while that is still going:

```
Error: canary-backup/canary-backup-restic failed to run apply: error when creating
"/tmp/1763270912kubectl_manifest.yaml": secrets "canary-backup-restic" is forbidden:
unable to create new content in namespace canary-backup because it is being terminated
```

Wait for it to go before Step 9:

```sh
kubectl get ns canary-backup
```

Expected result: `Error from server (NotFound): namespaces "canary-backup" not found`.
It took about two minutes on the test cluster.

### Step 9: apply it again and read the file

```sh
terragrunt apply
```

Expected result: the claim is created, stays Pending while the restore runs, and
binds. The Deployment's rollout wait holds the apply until the pod is running,
so one apply covers the restore.

```sh
kubectl -n canary-backup exec deploy/canary-backup -- cat /data/hello.txt
```

Expected result: every line from step 7, plus one new line for this start.

This is the row the whole project is for. A file holding one line means the
restore wrote nothing, and the ReplicationDestination's status in backup-system
says why.

## The observation rows

Run these with the canary from step 9 still running.

### Step 10: the volume carries no snapshot

```sh
kubectl get zfsvolume -A -o custom-columns='NAME:.metadata.name,SNAP:.spec.snapname'
```

Expected result: the canary's volume is listed with an empty `SNAP` column.

A populated `snapname` means the volume was provisioned as a clone of a
snapshot, which is the behaviour this project exists to remove. Stop and report
it: the claim was filled by the snapshot path.

### Step 11: the pool holds no clone, no snapshot and no leftover cache

Run this on the node holding the volume.

```sh
zfs list -t all -o name,used,refer,origin -r zfspv-pool
```

Expected result: the canary's dataset is listed with an empty `origin`, no
snapshot appears for it, and no cache dataset from the restore survives.

A dataset with an origin is a clone, and it holds that origin open for as long
as it exists. `zfs destroy` on the origin snapshot answers `snapshot has
dependent clones`, which is the failure the whole design removes.

A surviving cache dataset means the VolumeRestore carried no
cacheStorageClassName, so VolSync provisioned the mover's cache from the default
class, which on this cluster is zfs and reclaims Retain.

### Step 12: Kueue admitted the mover pod

The mover exists only while a restore runs, so read this during step 9 rather
than after it.

```sh
kubectl -n backup-system get pods -l kueue.x-k8s.io/queue-name=backups
```

Expected result: the restore's mover pod is listed and reaches `Running`.

A mover that stays `Pending` with no Kueue admission means the LocalQueue in
backup-system is missing, or named differently from the label the VolumeRestore
carries.

### Step 13: record both volumes

Step 11's command is also the measurement, and it has to run against two volumes
on the same pool: the canary now on a VolumeRestore, and a volume still on the
snapshot path.

```sh
zfs list -t all -o name,used,refer,origin -r zfspv-pool
```

Record, for each of the two volumes:

| Column | What it says |
| --- | --- |
| used | the space the dataset and its descendants hold on the pool |
| refer | the space the dataset's own data holds |
| origin | the snapshot a clone was created from, empty for a volume with no clone behind it |

Also record whether a snapshot exists for each volume, which the `-t all` flag
lists.

One volume proves nothing about drift. The volume on the snapshot path has an
origin, and the gap between its `used` and its `refer` is the space that origin
still holds. The pair of numbers turns the design's mechanism claim into a
magnitude, so both go into the infrastructure repository's backups
documentation, in docs/cluster/backups.md, under the section describing what the
automatic refill occupies on disk.

## Restoring an older snapshot

Step 9 restored the newest backup, which is what recreating a claim always does.
An admin asking for a restore usually wants something narrower, and the three
operations differ in what they discard.

| To do this | Do this |
| --- | --- |
| rebuild the volume from the newest backup | delete the claim, or destroy and apply the unit |
| read an older snapshot beside the live volume | add a second VolumeRestore with restoreAsOf set and a second claim naming it, then mount that claim from a throwaway pod |
| write an older snapshot into the claim the app already has | set `restore:` in the unit's inputs.yaml and apply, which scales the workload to zero and has VolSync's mover overwrite the claim in place |

The first discards whatever the volume holds at that moment. Before reaching for
it to test a theory, back the current state up so the theory can be walked back.
A ReplicationSource accepts one trigger, so an on-demand run means swapping the
schedule for a manual one and putting the schedule back afterwards:

```sh
kubectl -n canary-backup patch replicationsource canary-backup --type=merge -p '{"spec":{"trigger":{"manual":"manual-2026-09-14","schedule":null}}}'
kubectl -n canary-backup get replicationsource canary-backup -o jsonpath='{.status.lastManualSync}{"\n"}'
terragrunt apply
```

Expected result: the trigger string from the second command once the mover has
finished, and the schedule restored by the apply, which reads the patch as
drift. A manual trigger left in place makes the source report healthy while
taking no further backups.

A manual ZFS snapshot on the node preserves the state too, and only on that
node's pool.

The third is unchanged by this project. A Direct-mode restore mounts an existing
claim and overwrites it, and it works the same whether that claim was
provisioned dynamically or bound to a named volume, so a dynamic claim keeps the
in-place restore it always had. The steps are in the infrastructure repository's
docs/cluster/backups-terragrunt.md.

## The Flux canary

Run this only after steps 5 through 13 have passed on the terragrunt canary.

The canaries are in `flux/templates/tutorials/`, running under `flux/test/`:
app-with-two-volumes, app-with-a-postgres-database-and-volumes, and
app-pinned-to-a-worker. Start with app-with-two-volumes, which has no database
in it and no node pinning to confuse a first result.

Substitute the canary's name for `${APP}` and its namespace for `${NAMESPACE}`
in every command below.

### Step 14: rewrite the claim

The claim keeps its two ReplicationSource annotations and gains a dataSourceRef
naming the VolumeRestore:

```yaml
# base/volume-data/pvc.yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: ${APP}
  annotations:
    backup.walzen.org/schedule: ${BACKUP_SCHEDULE}
    backup.walzen.org/retain-last: "10"
spec:
  accessModes: [ReadWriteOnce]
  storageClassName: zfs
  resources:
    requests:
      storage: 1Gi
  dataSourceRef:
    apiGroup: backup.wlz.li
    kind: VolumeRestore
    name: ${APP}
```

### Step 15: add the VolumeRestore

```yaml
# base/volume-data/restore.yaml
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: ${APP}
spec:
  repository: ${APP}-restic
  cacheStorageClassName: zfs-ephemeral
  moverPodLabels:
    kueue.x-k8s.io/queue-name: ${BACKUP_QUEUE}
```

cacheStorageClassName is the field step 11 checks for. zfs-ephemeral reclaims
Delete, so the mover's cache dataset goes when the restore ends.

### Step 16: delete the ReplicationDestination from the app's base

Remove the destination the backups/pvc component rendered in Snapshot mode, and
remove two annotations from the claim with it.

| Annotation removed | What did its job before | What does that job now |
| --- | --- | --- |
| backup.walzen.org/restore-trigger | re-ran the ReplicationDestination when an admin changed its value | the three operations under "Restoring an older snapshot" above, each with its own shape |
| backup.walzen.org/volume-at | placed the restore mover on a chosen node | the scheduler picks the node for the app's pod, and the library copies volume.kubernetes.io/selected-node onto the prime claim |

The ReplicationSource, the restic Secret and the queue component stay exactly as
they are. [docs/integration.md](../../docs/integration.md) has the full change
to the Flux component, including the three placement components that go with the
volume-at annotation.

Both paths can run side by side while this is proven, because each claim chooses
its path by what its dataSourceRef names.

### Step 17: repeat the observation rows

Run steps 6 and 10 through 12 against `${NAMESPACE}` and `${APP}`.

Expected result: the same answers the terragrunt canary gave.

### Step 18: delete the claim and watch it refill

```sh
kubectl -n ${NAMESPACE} scale deployment/${APP} --replicas=0
```

Expected result: the pod terminates. A claim in use by a running pod stays in
`Terminating` until the pod releases it, so we need the scale to come first.

```sh
kubectl -n ${NAMESPACE} delete pvc ${APP}
```

Expected result: Flux recreates the claim and the controller fills it. Scale the
Deployment back up and the app comes back with its data.

## What would make this project wrong

Each of these means the design is wrong rather than the implementation. Stop and
report it. Do not work around it on the cluster.

### The prime claim cannot follow the app

The library creates the prime claim in backup-system and copies the app claim's
storage class, size and access modes onto it. For a WaitForFirstConsumer class
it also copies `volume.kubernetes.io/selected-node`. Watch for a prime claim
that lands on a different node from the app's pod, which leaves the pod
unschedulable with the volume it needs on another machine.

```sh
kubectl -n backup-system get pvc -o custom-columns='NAME:.metadata.name,NODE:.metadata.annotations.volume\.kubernetes\.io/selected-node,CLASS:.spec.storageClassName'
```

Expected result: the prime claim carries the app's storage class and the node the scheduler picked for the app's pod.

### The restored volume is unusable

VolSync's Direct restore writes into an empty claim, and the mover's security
context decides the ownership of what it writes. Watch for an app that starts
and then fails on permissions against its own data.

```sh
kubectl -n ${NAMESPACE} logs deployment/${APP}
```

Expected result: the app reads its restored data without a permission error.

A permission error that moverSecurityContext cannot correct means the Direct
path leaves the volume in a state the app cannot use.

### The rebound PersistentVolume is orphaned

The library patches the PersistentVolume's claimRef from the prime claim to the
app's claim. zfs-localpv has to keep recognising that volume as its own, or
deleting the claim later leaves the dataset on the pool with nothing tracking
it. Step 8 already exercises this on the terragrunt canary, and step 11 reads
the pool afterwards.

```sh
zfs list -r zfspv-pool
```

Expected result: the dataset for that volume is gone from the pool after the claim is deleted.

A dataset that survives the delete is a leak, and it grows with every
delete-and-refill cycle.

### A large restore outruns the library's timeouts

The library requeues the claim for as long as `PopulateCompleteFn` reports
false, so a long restore should survive on requeues alone. Do not stage a large
restore to find out. A timeout firing inside the library shows up as a claim
that returns to `Pending`, or as a ReplicationDestination deleted and recreated
mid-restore, and both are visible in a restore of any size. Watch for them
during step 9, and again the first time a real volume is restored in the normal
course of running the cluster.

## What could not be proven offline

| Gap | What was proven instead |
| --- | --- |
| the container image builds | a static linux/amd64 binary build and hadolint against the Dockerfile, both clean. No container daemon exists in the development environment, and the `docker build` runs in CI's image job |
| the restore completes and the claim rebinds | an envtest control plane showed the copied repository Secret, the prime claim named `prime-<uid>`, and the ReplicationDestination with `copyMethod: Direct`. No VolSync controller runs offline, so no mover moved data and nothing set `status.lastManualSync` |
| a large volume's restore against the library's requeue behaviour | nothing offline, and nothing staged on the cluster either. The symptoms above are what to watch for |
