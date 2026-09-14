# backup-controller: the cluster runbook

Date: 2026-09-14. Audience: the admin who runs the walzen test cluster.

Milestones 1 through 5 of [docs/implementation-plan.md](../../docs/implementation-plan.md)
are implemented in this repository, and milestone 6 is the canary run on the
test cluster. No command in this epic touched a cluster, so this page is the
run itself: every step, what it should print, and what a wrong answer means.

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
size. Nobody has measured that drift on this pool. Step 12 is where the number
comes from, and it matters as much as the passing rows above it.

## Before the first canary

| Prerequisite | What it is for |
| --- | --- |
| The VolumeRestore CRD is applied and Established | the API server cannot serve a kind it has not registered, and the claim's dataSourceRef names that kind |
| VolSync is installed and its ReplicationDestination CRD is served | the controller creates a ReplicationDestination and VolSync's mover does every byte of the restore |
| zfs-localpv is installed and its storage class provisions | the prime claim the library creates needs the same class the app's claim asks for |
| The controller image is in ghcr.io | the Deployment pulls `ghcr.io/walzen-group/backup-controller` and nothing on the cluster builds it |
| The controller Deployment reports Available | a controller that is not running leaves every claim Pending with no error on the claim |
| The repository Secret is in the canary's namespace | the controller reads it there and copies it into its own namespace for the length of one restore |

No release exists yet. No tag has been cut, so ghcr.io holds no image and the
GitHub Releases page holds no asset, which leaves two install paths and they
arrive in different files.

| Path | Where the manifests come from | When to use it |
| --- | --- | --- |
| From a checkout | deploy/ in this repository, rendered with kustomize | before the first tag, which is where the project stands today |
| From a release | backup-controller-<version>.yaml and crds-<version>.yaml, downloaded from the release | after the first tag, and it is the path [docs/integration.md](../../docs/integration.md) automates in the terragrunt module |

Cut the first tag before running the release path. The image the Deployment
names does not exist until the release workflow has pushed it.

## Install

### Step 1: apply the CRDs

```sh
kubectl apply -f deploy/crds/
```

Expected result: `customresourcedefinition.apiextensions.k8s.io/volumerestores.backup.wlz.li created`.

The CRDs go in first and on their own. A kind that is not servable yet cannot
have objects created against it, so applying the whole tree in one command
races the API server's registration.

### Step 2: wait for the CRD to be Established

```sh
kubectl wait --for=condition=Established crd/volumerestores.backup.wlz.li --timeout=60s
```

Expected result: `customresourcedefinition.apiextensions.k8s.io/volumerestores.backup.wlz.li condition met`.

A timeout here means the API server rejected the schema. Read the CRD's status
conditions before applying anything else.

### Step 3: apply the install

```sh
kubectl apply -k deploy/
```

Expected result: the Namespace, the ServiceAccount, the ClusterRole, the ClusterRoleBinding and the Deployment all report `created`.

### Step 4: wait for the rollout

```sh
kubectl -n backup-system rollout status deployment/backup-controller --timeout=120s
```

Expected result: `deployment "backup-controller" successfully rolled out`.

A rollout that never completes is usually the image. Run `kubectl -n
backup-system describe pod -l app.kubernetes.io/name=backup-controller` and read
the events: `ImagePullBackOff` means no tag has been pushed to ghcr.io yet, and
the release workflow has to run before this step can pass.

### Step 5: confirm the controller is watching

```sh
kubectl -n backup-system logs deployment/backup-controller | head -5
```

Expected result: the line `starting backup-controller: watching backup.wlz.li/v1alpha1 VolumeRestore`.

That line is the binary's own, logged before it hands control to the populator
library, and the envtest run in milestone 4 read the same line. If the log
instead shows a client error, the ServiceAccount token or the ClusterRoleBinding
is wrong, so check both before touching a canary.

## Move one canary to a VolumeRestore

The canaries are in `flux/templates/tutorials/`, running under `flux/test/`:
app-with-two-volumes, app-with-a-postgres-database-and-volumes, and
app-pinned-to-a-worker. Start with app-with-two-volumes, which has no database
in it and no node pinning to confuse a first result.

Substitute the canary's name for `${APP}` and its namespace for `${NAMESPACE}`
in every command below.

### Step 6: rewrite the claim

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

### Step 7: add the VolumeRestore

```yaml
# base/volume-data/restore.yaml
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: ${APP}
spec:
  repository: ${APP}-restic
  moverPodLabels:
    kueue.x-k8s.io/queue-name: ${BACKUP_QUEUE}
```

### Step 8: delete the ReplicationDestination from the app's base

Remove the destination the backups/pvc component rendered in Snapshot mode, and
remove two annotations from the claim with it.

| Annotation removed | What did its job before | What does that job now |
| --- | --- | --- |
| backup.walzen.org/restore-trigger | re-ran the ReplicationDestination when an operator changed its value | deleting the claim is the trigger, because a recreated claim is populated again from scratch |
| backup.walzen.org/volume-at | placed the restore mover on a chosen node | the scheduler picks the node for the app's pod, and the library copies volume.kubernetes.io/selected-node onto the prime claim |

The ReplicationSource, the restic Secret and the queue component stay exactly as
they are. [docs/integration.md](../../docs/integration.md) has the full change
to the Flux component, including the three placement components that go with the
volume-at annotation.

Both paths can run side by side while this is proven, because each claim chooses
its path by what its dataSourceRef names.

## The observation rows

These five rows are milestone 6's table. Run them in order after Flux has
reconciled the canary and its pod is Running.

### Step 9: the claim is Bound with no destination claim beside it

```sh
kubectl -n ${NAMESPACE} get pvc
```

Expected result: the app's claim is `Bound`, and no claim named `volsync-*-dest` is listed.

A `volsync-*-dest` claim means the app is still on VolSync's own populator, so
the dataSourceRef in step 6 did not reach the cluster. A claim stuck `Pending`
means the controller never filled it; read the controller's log and the
VolumeRestore's status conditions, which name the ReplicationDestination in the
Ready condition's message.

### Step 10: the volume carries no snapshot

```sh
kubectl get zfsvolume -A -o custom-columns='NAME:.metadata.name,SNAP:.spec.snapname'
```

Expected result: the app's volume is listed with an empty `SNAP` column.

A populated `snapname` means the volume was provisioned as a clone of a
snapshot, which is the behavior this project exists to remove. Stop here and
report it: the claim was filled by the snapshot path, not by this controller.

### Step 11: the pool holds no clone and no snapshot

Run this on the node holding the volume.

```sh
zfs list -t all -o name,used,refer,origin -r zfspv-pool
```

Expected result: the app's dataset is listed with an empty `origin`, and no snapshot appears for it.

A dataset with an origin is a clone, and it holds that origin open for as long
as it exists. `zfs destroy` on the origin snapshot answers `snapshot has
dependent clones`, which is the failure the whole design removes.

### Step 12: delete the claim and watch it refill

```sh
kubectl -n ${NAMESPACE} delete pvc ${APP}
```

Expected result: Flux recreates the claim, the controller fills it again, and the app comes back with its data.

Watch the refill as it happens:

```sh
kubectl -n backup-system get replicationdestination -w
```

Expected result: a ReplicationDestination named `restore-<claim uid>` appears, and it is deleted once the restore completes.

The app's pod has to be scaled to zero before the delete, because a claim in use
by a running pod stays in `Terminating` until the pod releases it.

### Step 13: the mover pod is admitted by Kueue

```sh
kubectl -n backup-system get pods -l kueue.x-k8s.io/queue-name=${BACKUP_QUEUE}
```

Expected result: the restore's mover pod is listed and reaches `Running`, admitted like every other mover.

A mover pod that stays `Pending` with no Kueue admission means moverPodLabels in
step 7 does not carry the queue name the cluster's ClusterQueue selects on.

## The measurement

Step 11's command is also the measurement, and it has to run twice on the same
pool: once against the canary now on a VolumeRestore, and once against a volume
still on the snapshot path.

### Step 14: record both volumes

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
origin, and the gap between its `used` and its `refer` is the space its origin
still holds. The pair of numbers is what turns the design's mechanism claim into
a magnitude, so both go into the infrastructure repository's backups
documentation, in docs/cluster/backups.md, under the section describing what the
automatic refill occupies on disk.

## What would make this project wrong

Each of these means the design is wrong rather than the implementation. Stop and
report it. Do not work around it on the cluster.

### The prime claim cannot follow the app

The library creates the prime claim in the controller's namespace and copies the
app claim's storage class, size and access modes onto it. For a
WaitForFirstConsumer class it also copies `volume.kubernetes.io/selected-node`.
Watch for a prime claim that lands on a different node from the app's pod, which
leaves the pod unschedulable with the volume it needs on another machine.

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
it.

```sh
kubectl -n ${NAMESPACE} delete pvc ${APP}
zfs list -r zfspv-pool
```

Expected result: the dataset for that volume is gone from the pool after the claim is deleted.

A dataset that survives the delete is a leak, and it grows with every
delete-and-refill cycle.

### A large restore outruns the library's timeouts

This is the most likely of the four and the first to test, because a fifty
gigabyte restore is the case the project exists for. Run it before the small
canaries are declared a success.

### Step 15: restore a fifty gigabyte volume and time it

```sh
date -u +%FT%TZ && kubectl -n ${NAMESPACE} delete pvc ${APP} && kubectl -n ${NAMESPACE} wait --for=jsonpath='{.status.phase}'=Bound pvc/${APP} --timeout=4h && date -u +%FT%TZ
```

Expected result: the claim reaches `Bound` and the two timestamps bracket the wall time of the restore.

Record that wall time. The library requeues the claim while
`PopulateCompleteFn` reports false, so a long restore should survive on requeues
alone. A claim that goes back to `Pending`, or a ReplicationDestination that is
deleted and recreated mid-restore, means a timeout fired inside the library and
the design needs revisiting rather than a longer `--timeout` here.

## What this epic could not prove

| Gap | What was proven instead |
| --- | --- |
| the container image builds | a static linux/amd64 binary build and hadolint against the Dockerfile, both clean. No container daemon exists in the development environment, and the `docker build` runs in CI's image job |
| a tag produces the four release assets | the kustomize render, the `@sha256:` digest grep and `helm package` all run locally against a stand-in digest. Publishing to ghcr.io and creating a GitHub Release need a tag and credentials that do not exist here |
| the restore completes and the claim rebinds | an envtest control plane showed the copied repository Secret, the prime claim named `prime-<uid>`, and the ReplicationDestination with `copyMethod: Direct`. No VolSync controller runs offline, so no mover moved data and nothing set `status.lastManualSync` |
| a large restore's wall time against the library's timeouts | nothing offline. Step 15 on the cluster is the only place this is measurable |
