# Upgrading

This doc covers the upgrade from v0.10.1 to v1.0.1. v1.0.1 keeps
the three kinds and most of their fields, renames the pause fields, and sends
all its restic work through one Kueue queue.

## What changes

| v0.10.1 | v1.0.1 |
| --- | --- |
| Workload annotation backup.wlz.li/quiesce: "true" | backup.wlz.li/pause-during-backup: "true" |
| RestoreRun spec.quiesce | spec.pauseDuringRestore |
| Run status quiescedAt, restartedAt, quiesced | pausedAt, resumedAt, paused |
| Snapshot tag quiesced | paused |
| Namespace annotation backup.wlz.li/max-quiesce | removed; the run's timeout is the only limit on the pause |
| Cluster annotation backup.wlz.li/restore-as-of | removed; a Cluster created without a RestoreRun follows the paused-moment rule (docs/automatic-restore.md) |
| Default BackupRun timeout | 1 hour, counted from Kueue admission |
| A database restore to a moment replays WAL to that time | it recovers to the end of the newest base backup at or before the moment (docs/restores.md) |
| Volume restores through a VolSync ReplicationDestination | the controller's own restore Job, with the image from --restore-image |

Snapshots tagged quiesced by v0.10.1 do not count as paused snapshots. The
first scheduled backup after the upgrade writes a paused snapshot of each
namespace whose app is marked for pausing.

## Step 1: Pause the controller

Follow "Pause the controller for an upgrade" in docs/operations.md: set pause:
true, apply the unit, and wait until no run is working.

Expected result: the listing in that procedure prints nothing.

## Step 2: Check the queue

Every BackupRun, RestoreRun and populator restore asks Kueue for one
backup-controller.wlz.li/run. The ClusterQueue behind the namespaces'
LocalQueues, and behind the LocalQueue in backup-system, needs a quota for
it; docs/operations.md shows the ClusterQueue.

```
kubectl get clusterqueue backups -o jsonpath='{.spec.resourceGroups[*].coveredResources}'
```

Expected result: the list contains backup-controller.wlz.li/run. A missing
quota leaves every run and every populator restore waiting.

```
kubectl -n backup-system get localqueue -o jsonpath='{.items[*].spec.clusterQueue}'
```

Expected result: backups, the same ClusterQueue as the namespaces use.

The pods quota that v0.10.1 needed on that ClusterQueue can go. The
controller admits the populator's restores itself and removes the Kueue queue
label from their Jobs, so a queue label in a VolumeRestore's moverPodLabels
no longer does anything.

## Step 3: Rename the pause annotations and fields

In the app manifests, replace:

| Find | Replace with |
| --- | --- |
| backup.wlz.li/quiesce: "true" | backup.wlz.li/pause-during-backup: "true" |
| quiesce: (in a RestoreRun spec) | pauseDuringRestore: |
| backup.wlz.li/max-quiesce | delete the annotation; set backup.wlz.li/timeout if a namespace needs more than 1 hour |
| backup.wlz.li/restore-as-of on a Cluster | delete the annotation |

```
grep -rn 'quiesce\|backup.wlz.li/restore-as-of' <infra repo>
```

Expected result: only claims carry backup.wlz.li/restore-as-of, and nothing
names quiesce.

## Step 4: Update the controller unit

| Setting | Value |
| --- | --- |
| Image | v1.0.1 |
| --restore-image | VolSync's mover image, as the unit passes it today |
| Memory limit | 512Mi |
| CRDs | apply config/crd of v1.0.1 |
| ClusterRole | deploy/rbac.yaml of v1.0.1, which adds coordination.k8s.io leases (get, create, update, delete) and batch jobs (get, create, delete) and update on backupruns/finalizers and restoreruns/finalizers, and drops replicationdestinations |

The controller stops at startup when --restore-image is empty.

## Step 5: Apply and check

Apply the unit with pause still true.

```
kubectl -n backup-system logs deploy/backup-controller | grep 'paused:'
```

Expected result: `paused: new runs wait, runs in progress finish`.

## Step 6: End the pause

Set pause: false and apply the unit. Each namespace schedule creates one run
for the newest tick it missed.

```
kubectl get backupruns -A --sort-by=.metadata.creationTimestamp | tail
```

Expected result: new runs reach Succeeded. A run that stays Queued points at
a missing quota (step 2); a run that fails names the item and the reason
(docs/operations.md).
