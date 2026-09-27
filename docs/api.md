# API

The API has three kinds. All three are `backup.wlz.li/v1alpha1` and
namespaced. [namespace-backups.md](namespace-backups.md) lists the annotations
that a namespace uses to declare its backups.
[restores.md](restores.md) tells which restore to use.

| Kind | Short name | Is |
| --- | --- | --- |
| VolumeRestore | `vrestore` | a standing declaration of where a volume's backups live, named by the claim's `dataSourceRef` |
| BackupRun | `brun` | one backup now, of a volume, a database or the namespace, and the record of every scheduled one |
| RestoreRun | `rrun` | one restore, of a volume, a database or the namespace, to the newest backup or a chosen moment |

The group follows the convention that kuport set with `kuport.wlz.li`. v0.1.0
shipped the VolumeRestore CRD under that group. A change to the group or to a
kind now changes every claim in the infrastructure repository with it.

## VolumeRestore

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: VolumeRestore
metadata:
  name: notes-data
  namespace: notes
spec:
  # The Secret in this namespace holding the restic repository URL, its
  # password and the object store keys.
  repository: notes-restic-data

  # Optional. Restore the newest snapshot taken at or before this time, in
  # RFC3339. Left out, the newest snapshot in the repository is used.
  restoreAsOf: "2026-09-13T00:00:00Z"

  # Optional, and set it. Every backup mover and every restore Job gets a
  # claim for restic's metadata cache. Left out, that claim comes from the
  # cluster's default storage class, and a default whose reclaim policy is
  # Retain keeps the cache dataset after the run that made it.
  cacheStorageClassName: zfs-ephemeral

  # Optional. The size of that cache claim. Left out, it is 1Gi.
  cacheCapacity: 2Gi

  # Optional. Labels put on the pod of a restore Job, so the restore is
  # admitted by the cluster's backup queue.
  moverPodLabels:
    kueue.x-k8s.io/queue-name: backups

  # Optional. The user the backup movers and the restore Jobs run as, for an
  # app whose files only that user can read.
  moverSecurityContext:
    runAsUser: 26
    runAsGroup: 26
    fsGroup: 26
```

The populator reads the VolumeRestore when someone creates a claim that names
it. Every BackupRun and RestoreRun reads it for the claim it belongs to. A claim
with no `dataSourceRef` binds to a volume by name. The VolumeRestore with the
claim's own name describes that claim.

| Field | Required | Reaches |
| --- | --- | --- |
| `repository` | yes | `spec.restic.repository` of the claim's ReplicationSource. It is also the Secret that every restore Job for the claim reads its environment from. The populator first copies the Secret into its own namespace |
| `restoreAsOf` | no | the moment the populator uses to select its snapshot. If the claim carries `backup.wlz.li/restore-as-of`, the annotation wins |
| `cacheStorageClassName` | no | `cacheStorageClassName` of the ReplicationSource and the source's clone class, and the class of each restore Job's ephemeral cache volume |
| `cacheCapacity` | no | `cacheCapacity` of the claim's ReplicationSource. It is also the size of the cache volume of the populator's restore Job, and of the Job of a RestoreRun that names the claim in `claim`. Unset, it is 1Gi |
| `moverPodLabels` | no | labels on the pod of every restore Job for the claim. The controller's own labels win over a key they share. A source carries none, because Kueue admitted its run as a whole |
| `moverSecurityContext` | no | `moverSecurityContext` of the ReplicationSource, and the pod `securityContext` of every restore Job for the claim. The [admission policy](packaging.md#admission-policy-on-the-restore-jobs) refuses a restore Job whose pod security context sets sysctls, SELinux options or an unconfined seccomp or AppArmor profile |

Add a field only when a claim must reach a setting of the backup mover or the
restore Job. Name the field after the VolSync field it matches.

### Status

```yaml
status:
  conditions:
    - type: Ready
      status: "False"
      reason: Restoring
      message: waiting for restore Job restore-3f2a1c7e-... (snapshot 6e473100) in backup-system
  claims:
    - name: notes-data
      uid: 3f2a1c7e-...
      phase: Restoring
      startedAt: "2026-09-14T09:12:03Z"
```

| Field | Holds |
| --- | --- |
| `conditions[type=Ready]` | False while the populator fills any claim that names this object. True when it fills none |
| `claims[]` | one entry for each claim that the populator fills from this object now. The entry holds the phase, Restoring or Failed, and the time the fill started |

A finished fill leaves no entry. A reader wants two things from the status:
whether something happens now, and where to look.

| Ready reason | When |
| --- | --- |
| Restoring | the populator fills claims, and none of them failed. The message lists the restore Job `restore-<claim uid>` of every claim in `status.claims`, in that order. Each Job has the short ID of the snapshot it restores and, while the Job's newest pod waits, the reason. The controller's namespace comes last: `waiting for restore Jobs restore-<uid> (snapshot 6e473100), restore-<uid> (snapshot 2edf5bab; waiting: restore: ImagePullBackOff: "...") in backup-system` |
| RestoreFailed | a claim's restore Job failed. The message is `claim <name>: ` and then `the restore Job failed (<condition reason>): restic exited <code> (<meaning>) in container <unlock or restore>: "<restic's last lines>"` |
| RestoreJobRefused | the API server refused to create or resume a claim's restore Job with 403 Forbidden or 422 Invalid. An example is the admission policy that refuses the VolumeRestore's `moverSecurityContext`. The message is `claim <name>: ` and then the API server's answer. The claim stays Pending, and every sync tries again |
| NoBackupInReach | one of two conditions is true. The moment that the claim's `backup.wlz.li/restore-as-of` or `spec.restoreAsOf` asks for is older than every snapshot. Or the repository holds snapshots and none has the layout a VolSync mover writes (host `volsync`, paths exactly `[/data]`). The message is `claim <name>: ` and then the reason. The reason names where the moment came from or the snapshots it passed over. The claim stays Pending, and the controller creates no restore Job |
| Restored | the populator fills no claim |

The populator sets Ready from every entry in `status.claims`. Thus all the
claims that fill from one VolumeRestore report the same condition. While any
entry is Failed, a pass for another claim leaves Ready as it is. A
RestoreFailed or NoBackupInReach therefore stays on the VolumeRestore while its
other claims restore or finish.

After a RestoreFailed, the populator does these steps:

1. It stops the failed Job.
2. When the Job is gone, it selects the snapshot again and creates a new Job.
3. The claim's entry goes back to Restoring.

The populator retries a repository that continues to fail in that way until the
repository answers. An example is a repository behind an S3 outage. The claim
stays Pending all the time.

While `status.claims` lists any claim, the VolumeRestore carries the finalizer
`backup.wlz.li/volume-populator`. The library calls the populator for a claim
only when the claim's prime claim, `prime-<uid>` in the controller's namespace,
is bound. On that first call, the populator adds the finalizer before it copies
the Secret or creates the restore Job. It removes the finalizer when
`status.claims` is empty. Thus a VolumeRestore that someone deletes during a
restore stays Terminating until its claims finish or someone deletes them.

The cleanup of each claim does three things:

1. It stops that claim's restore Job.
2. It waits until no pod of the Job can still write.
3. It deletes the Secret copy.

The cleanup that empties `status.claims` removes the finalizer, and then the
deletion can finish. A VolumeRestore that is already in deletion without the
finalizer starts no new restore.

Until the prime claim binds, the VolumeRestore carries no finalizer, and a
delete removes it immediately. Before the library cleans up after a claim, it
gets the claim's VolumeRestore. If the VolumeRestore is gone, the library
stops. A claim that someone deletes after its VolumeRestore would then keep the
library's finalizer `backup.wlz.li/populate-target-protection` and stay
Terminating. The controller's orphan reconciler releases such a claim.
[architecture.md](architecture.md#a-claim-whose-volumerestore-is-gone) lists its
steps and the events it records on the claim.

### Validation the CRD carries

| Rule | Why |
| --- | --- |
| `repository` is a required, non-empty DNS-1123 name | a missing repository leaves claims Pending with nothing to read |
| `restoreAsOf` is an RFC 3339 date-time when present | otherwise the populator refuses it later, on each claim, with NoBackupInReach |
| `cacheStorageClassName` is a non-empty name of at most 253 characters when present | an empty string is a class name that no provisioner answers, and the cache claim of the mover or the restore Job stays Pending |
| `moverPodLabels` holds at most 8 entries, each with label syntax | the API server installs the label rules only for a map with a declared size |

## BackupRun

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: before-upgrade
  namespace: notes
spec:
  all: true
```

| Field | Required | Holds |
| --- | --- | --- |
| `source` | one of the three | the claim to back up. The claim must carry `backup.wlz.li/enabled: "true"`. Its ReplicationSource has the same name |
| `database` | one of the three | the CloudNativePG Cluster to take a base backup of |
| `all` | one of the three | every enabled claim and Cluster in the namespace. The run stops the workloads marked `backup.wlz.li/quiesce` until it cuts the clones. Only one run at a time in the namespace stops its workloads. The stop lasts at most the namespace's `backup.wlz.li/max-quiesce`, ten minutes by default |
| `timeout` | no | how long the run may work after admission, e.g. `10h`. The same time also limits the wait for Kueue to admit the run. That wait counts from the creation of the run, or from `status.resumedAt` when the run waited with reason Paused. Omitted, the namespace's `backup.wlz.li/timeout`, and 6h without one |
| `ttlSecondsAfterFinished` | no | delete the run that long after it finishes. Omitted, it stays as the record |

A CEL rule on the CRD accepts exactly one of `source`, `database` and `all`. A
scheduled run is an ordinary BackupRun with the name
`scheduled-<yyyymmdd-hhmm>`, `all: true` and a 30-day TTL.

| Status field | Holds |
| --- | --- |
| `phase` | Queued, Running, Waiting, Succeeded or Failed. `kubectl get brun` prints it as a column |
| `workload` | the Kueue Workload that admits the run, while it exists |
| `startedAt`, `completedAt` | when Kueue admitted the run, and when it finished |
| `resumedAt` | when the controller first worked on the run after the run waited with reason Paused. The wait for Kueue counts from this time. A run that never waited Paused has no `resumedAt` |
| `quiescedAt`, `restartedAt` | when the run stopped and restarted the quiesced workloads, in whole seconds |
| `restartPending` | true from the moment the run records `restartedAt`. It stays true until the run gives every workload its replicas back and resumes every Kustomization. A pass that finds it set repeats the restart and keeps the recorded `restartedAt`. When `restartedAt` is set and this field is false, the run never restarts the workloads again |
| `quiesced[]` | the workloads the run stops. Each has the replica count it had before the run touched it, which the run gives back |
| `suspendedKustomizations[]` | the Flux Kustomizations the run suspends, as namespace/name. The run resumes exactly these |
| `items[]` | one for each volume and database. Each item holds these fields: kind, name, phase, message, the manual `trigger`, the restic snapshot's short ID in `snapshot`, its full ID in `snapshotID` and its `snapshotTime`. It also holds `empty` for a volume with no files, `noSnapshotListedAt` while an empty volume waits for its second listing, the CloudNativePG `backup`, the item's `reason`, and `lastStartError` |
| `ending` | the Ready reason and message the run ends with. The status write that decides to end the run records them, for example after a timeout. Every later pass ends the run with them, also a pass that retries a restart |
| `conditions[type=Ready]` | the reason and message, kstatus-compatible. A Flux Kustomization with `wait: true` can thus gate on the run |

The run writes `quiesced[]` and `suspendedKustomizations[]` as its plan, before
it suspends or scales anything. Thus for the length of one pass, both lists can
name a workload that still runs. If a stop fails partway, the run cuts both
lists to what is in effect. These are the workloads at zero replicas and the
suspended Kustomizations. The run then gives those back.
[namespace-backups.md](namespace-backups.md#which-kustomization-a-run-suspends)
tells which Kustomizations a run suspends.

The run finds the snapshot of each volume in the repository. It is the newest
snapshot that a VolSync mover wrote (host `volsync`, paths exactly `[/data]`,
no original) with a time in the window of the sync that completed the run's
trigger. [namespace-backups.md](namespace-backups.md#what-a-run-with-all-set-does)
gives the window. A run that stopped workloads rewrites that snapshot to its
`restartedAt` and tags it `quiesced`. Then `items[].snapshot` and `snapshotID`
name the rewritten snapshot, and `snapshotTime` equals `restartedAt`. Without
stopped workloads, `snapshotTime` is the time that restic stamped when it
started to read the clone.
[namespace-backups.md](namespace-backups.md#quiesced-snapshots) shows a run
that does this.

`items[].lastStartError` holds the error of the item's last failed start while
the item stays Pending and the run tries again. A start that succeeds clears
it. When the run gives the item up, it adds the error to the item's message.
`items[].noSnapshotListedAt` is when the run first listed the repository after
the sync completed and found no snapshot of it. An S3 listing can lag a write.
Thus the item succeeds with `empty: true` only when a second listing, at least
11 seconds later, also finds none.

### Item reasons

`items[].reason`, on a BackupRun and on a RestoreRun, is a one-word cause for
the item's phase, for display and alerts. The item's message says the same in
a sentence. The CRD declares no enum, thus a release can add a reason. An item
that ended by a path that records no reason leaves it empty
(internal/api/v1alpha1/itemreason.go).

| Reason | Kind | Item |
| --- | --- | --- |
| TimedOut | both | the run's timeout ran out before the item finished. This includes the items of a BackupRun that Kueue did not admit within its timeout |
| MoverFailed | BackupRun | VolSync's backup mover reported the result Failed. The message is `Mover logs: ` and the logs as VolSync kept them |
| NoMoverSnapshot | BackupRun | the sync completed, but the ReplicationSource status has no `lastSyncTime` or `lastSyncDuration`, or records a negative duration. Thus the run cannot tell which snapshot in the repository the sync wrote |
| ClaimMissing | both | the claim does not exist when the run checks or starts the item |
| ClaimNotBound | BackupRun | the claim is not bound to a volume yet, so there is no node to place the mover on |
| VolumeMissing | BackupRun | the claim is bound to a PersistentVolume that does not exist |
| NoNodeAffinity | BackupRun | the PersistentVolume declares no node affinity to place the mover by |
| VolumeRestoreMissing | both | the claim has no VolumeRestore to name its repository |
| RepositorySecretMissing | both | the repository Secret does not exist |
| SettingsInvalid | BackupRun | a retention annotation on the claim, or a setting on the namespace, does not parse |
| SourceNotManaged | BackupRun | the claim has a ReplicationSource the controller did not write |
| SourceRefused | BackupRun | the API server refused the ReplicationSource as invalid |
| SourceAbandoned | BackupRun | the ReplicationSource still retries a backup that no run waits for |
| ClusterMissing | both | the Cluster does not exist when the run starts or checks the item |
| ClusterHibernated | BackupRun | the run skipped a hibernated Cluster |
| BackupRefused | BackupRun | the API server refused the CloudNativePG Backup as invalid |
| BackupFailed | BackupRun | the CloudNativePG Backup ended in the phase `failed`. The message carries the error from CloudNativePG. A Backup in the phase `invalid backup definition` is not at its end: CloudNativePG checks it again, and the run waits up to its timeout |
| RunEnded | both | the run ended before the item finished, for a cause other than the timeout. The run's `status.ending` says why the run ended |
| CRDOutdated | BackupRun | the installed BackupRun CRD lacks a field the controller writes, or the controller may not read that CRD. The run ended with the Ready reason CRDOutdated before it changed anything |
| NotStarted | BackupRun | the run did not start the item before the `backup.wlz.li/max-quiesce` limit ran out. The run then gave the workloads back |
| CloneNotCut | BackupRun | VolSync did not cut the clone before the `backup.wlz.li/max-quiesce` limit ran out. The run then gave the workloads back |
| RestoreJobFailed | RestoreRun | the restore Job ended with `Failed=True`, and the message carries restic's exit code and its meaning. The reason also applies in two other cases. The run's own Job restores a snapshot other than the one the run selected, or the run no longer controls the Job |
| RestoreJobDeleted | RestoreRun | someone deleted the restore Job before it finished, or its name now holds a Job with another UID. The run records no result from it. The run gives nothing back until no pod of the Job can still write |
| RestoreJobRefused | RestoreRun | the run created no restore Job, or the Job never ran. The API server refused the create or the resume as Forbidden or Invalid. Or the controller could not build the Job's spec. Or a Job that the run did not create holds its name. The run wrote nothing to the claim |
| SnapshotChanged | RestoreRun | the selected snapshot was no longer in the repository when the run listed it again immediately before it created the Job |
| ClaimLost | RestoreRun | the claim stopped being the run's: someone deleted it, replaced it, or took it. For a spec.into item the run also checks the claim before it resumes the restore Job. A loss found then says that the Job wrote nothing. A claim that is no longer controlled by the run gives its own message |
| ClaimDeleting | RestoreRun | the claim was in deletion when the run was about to start the restore |
| IntoClaimTaken | RestoreRun | the name that `spec.into` gives holds a claim, or a VolumeRestore, that the run did not create. The run finds it before it creates the claim. It also finds it when it takes over a restore Job that an earlier pass created and did not record. The run then stops that Job before the Job writes. The run wrote nothing to that claim |
| ClusterRestoredElsewhere | RestoreRun | another unfinished RestoreRun restores the Cluster |
| NoBackupInReach | RestoreRun | no backup is in reach of the run's moment. No snapshot or base backup is at or before the moment, or `spec.previous` reaches past the oldest one. For a synced restore, the items can also have no quiesced moment in common. The run wrote and deleted nothing |
| ClusterArchivesNowhere | RestoreRun | the Cluster archives its WAL nowhere, so it has no backup to restore. The run skips the item |
| OtherItemFailed | RestoreRun | the run left the item alone because another item failed. Before the restore starts, the cause is another item with no backup in reach. Later, the cause is a failed volume restore, and the run leaves the Cluster running |
| ClusterLeftAlone | RestoreRun | the Cluster opts out of the bootstrap webhook or declares its own bootstrap method. The run cannot make the next creation of the Cluster its recovery, so it does not delete the Cluster. The item is Skipped. The run records this reason at plan for a Cluster that `backup.wlz.li/enabled` selects, before it deletes a Pending Cluster, and for an old Cluster that a Deleted item finds still there. A run whose `spec.database` names such a Cluster ends with the Ready reason Invalid instead, and no item records the reason |
| ClusterNotRecovered | RestoreRun | the Cluster came back without the run's recovery, or someone deleted or replaced the recovered Cluster. The item fails. The run records this reason for a Deleted item that finds a new Cluster without the mark of the run, and for a Recovering item whose Cluster someone deletes or replaces. The message says why, such as an opt-out annotation or an own `spec.bootstrap` on the new Cluster. The run leaves that Cluster alone |
| ClusterVersionUnsupported | RestoreRun | the API server serves CloudNativePG's Cluster at another version and no longer at `postgresql.cnpg.io/v1`. The rules of the bootstrap webhook name that version. The run fails each Pending Cluster item with this reason, at plan or in a later pass, before it deletes the Cluster. At plan, the run also skips each other Pending item with this reason. The run deleted nothing for the item. When the run then ends with a failed item, its Ready reason is ClusterVersionUnsupported in place of Failed |

### Ready reasons

| Ready reason | When |
| --- | --- |
| Queued | the run waits for Kueue to admit it |
| Paused | the controller runs with `--pause`, and the run has started no work. The run keeps its phase, takes no Lease, and creates or stops nothing. The message is `the controller runs with --pause; this run starts when the controller runs without it` ([upgrading.md](upgrading.md#pause-the-controller)) |
| Running | the run does its work. The message is `backing up`, or `waiting for pod <pod> to stop before the clones are cut` while a pod of a workload the run stopped still terminates |
| SourceBusy | another run holds one of these: the run's claim or repository, or this namespace's quiesce Lease. Or another run deleted a Cluster that this run waits to see again. The message names that run, what it holds, and every wait |
| Retrying | an item could not start, with an error that a retry may fix. An example is a Backup that a CloudNativePG webhook refuses. The message names every such item and its error |
| RestartFailed | the run could not give a stopped workload its replicas back or resume a Kustomization it suspended. Thus the app is still down. If the Lease release also failed, the reason stays RestartFailed and the message names both. The run deletes its Kueue Workload only after the restart succeeds. Thus it keeps its place in the queue while the app is down |
| ReleaseFailed | the app is back. The run cannot finish because it could not release its Leases or delete its Kueue Workload |
| CRDOutdated | the run ended before it changed anything. The cause is that the CRD of its kind is not installed, lacks a field the controller writes, or may not be read by the controller. The message names the field or the permission |
| VolSyncUnsupported | the run ended because the API server no longer serves VolSync's ReplicationSource at v1alpha1. Before that, it gave back the workloads it stopped. The message names the kind and the versions served |
| Invalid | the spec names something that no retry can fix. Examples are a claim that is not marked `backup.wlz.li/enabled`, or workloads to stop whose Flux Kustomization also applies workloads in another namespace |
| Succeeded | the run finished with every item done |
| Failed | the run finished with a failed item, or past its timeout. A run that passed its deadline while it waited for another run carries that wait: `the run had not finished by <deadline>; it was waiting: <the wait>` |

When a run's Ready condition changes to a new reason, the controller records an
event on the run. The event has that reason, and the condition's message as its
note. These reasons record a Warning:

- Any reason but Succeeded on a finished run, such as Invalid or Failed.
- RestartFailed and ReleaseFailed, which the run reports while it is not
  finished.

Queued, Paused, Running, SourceBusy, Retrying and Succeeded record Normal events.
`kubectl describe brun <name>` lists them under Events. events.k8s.io/v1
rejects a note over 1024 bytes. Thus the controller cuts a longer message at
that length, and the full text stays on the condition.

## RestoreRun

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: RestoreRun
metadata:
  name: notes-back-to-monday
  namespace: notes
spec:
  all: true
  restoreAsOf: "2026-09-22T00:00:00Z"
```

| Field | Required | Holds |
| --- | --- | --- |
| `claim` | one of `claim`/`repository`, `database` and `all` | the claim whose repository to restore from. It is also the claim to write into, unless `into` names another |
| `repository` | the same | the restic Secret in this namespace to restore from, in place of the one the claim's VolumeRestore names. With `claim` as well, the run restores that claim in place from this repository. Without `claim`, the run needs `into` and `intoSize` and fills a new claim |
| `into` | no | a claim to create and fill, and the source stays untouched. Only with `claim` or `repository`. No claim of that name may exist yet. With `claim`, no VolumeRestore of that name may exist either. The reason is that a VolumeRestore describes the backups of the claim with its name. The run creates no VolumeRestore for it. The run creates the claim empty. Its own restore Job then writes the selected snapshot into it and restores that snapshot by its full ID. With `claim`, the run copies the source claim's size and the node its volume is on, and the scheduler places the Job's pod with that volume. With `repository` there is no source node, and the Job's pod is the claim's first consumer |
| `intoSize` | with `repository` and `into` | the size of that claim. Omitted with `claim`, the source claim's request |
| `database` | one of the three | the Cluster to restore. The run deletes it, and it recovers when its owner creates it again. The run ends Invalid for a Cluster that carries `backup.wlz.li/bootstrap: initdb` or declares its own bootstrap method such as `pg_basebackup`. It also ends Invalid for a Cluster that another unfinished RestoreRun restores |
| `all` | one of the three | every enabled claim in place, then every enabled Cluster. A Cluster that carries `backup.wlz.li/bootstrap: initdb`, or declares its own bootstrap method, gets a Skipped item. A Cluster that another unfinished RestoreRun restores ends the run Invalid. A run whose every item is Skipped ends Failed with reason NoBackupInReach |
| `restoreAsOf` | no | the moment to restore to. A volume restores the newest snapshot at or before it, a database replays WAL to it exactly. Omitted, the newest snapshot and the end of the archive |
| `previous` | no | how many snapshots further back than the selected one. One volume only |
| `syncDatabaseToVolume` | no | with `all` only. The run recovers the databases to the moment of the volumes' newest `quiesced` snapshot at or before `restoreAsOf`, so the files and the rows agree. It is refused when a volume has no such snapshot, or when the snapshots of two volumes are from different moments |
| `quiesce` | no | Deployments and StatefulSets, as `{kind, name}`, to stop while the run restores. Not with `into`. The run waits, with the workloads still running, in three cases. A backup of one of its claims uploads. Another run holds the Lease of one of its claims. Or another run stopped this namespace's workloads. A volume item whose repository Secret is gone fails before the run stops anything. A run with no item left to restore stops nothing. The run gives the workloads back after it restores the volumes and deletes the databases. A workload whose Flux Kustomization also applies workloads in another namespace ends the run Invalid before the run stops anything |
| `timeout` | no | how long to wait for the restore Jobs and the recovered databases. The time counts from when the run passes its checks. While its checks continue to fail, or while it waits for a backup of its repository, the time counts from its creation. When the run waited with reason Paused, it counts from `status.resumedAt`, so the time in the pause does not count. Defaults to `4h` |
| `moverSecurityContext` | no | the pod `securityContext` of the run's restore Jobs. Omitted, the source claim's VolumeRestore supplies it. The admission policy refuses one that sets sysctls, SELinux options or an unconfined profile, and the item fails with reason RestoreJobRefused |
| `ttlSecondsAfterFinished` | no | delete the run that long after it finishes. An `into` claim goes with it |

| Status field | Holds |
| --- | --- |
| `phase` | Queued, Running, Waiting, Succeeded or Failed. `kubectl get rrun` prints it as a column |
| `target` | the claim an `into` restore creates and fills |
| `startedAt`, `completedAt` | when the run passed its checks and began, and when it finished |
| `resumedAt` | when the controller first worked on the run after the run waited with reason Paused. Until the run passes its checks, its `timeout` counts from this time. A run that never waited Paused has no `resumedAt` |
| `syncedTo` | the moment a `syncDatabaseToVolume` run restores the volumes and recovers the databases to |
| `quiescedAt`, `restartedAt`, `quiesced[]`, `suspendedKustomizations[]` | when the run stopped and gave back the workloads `quiesce` lists, the replicas it gave back to each, and the Flux Kustomizations it suspended and resumed |
| `items[]` | one for each volume and each database. Each item holds these fields: kind, name, phase (Pending, Running, Deleted, Recovering, Succeeded, Failed, Skipped), message and the item's `reason`. It also holds the `snapshot` (short ID), `snapshotID` (full ID) and `snapshotTime` a volume restores, and the restore `job` and its `jobUID`. For a database, it holds the `baseBackup` a recovery starts from, the `clusterUID` of the Cluster the item deletes, and `clusterLeftDeleted` |
| `ending` | the Ready reason and message the run ends with. The status write that decides to end the run records them. Every later pass ends the run with them, also a pass that waits for a restore Job to stop or retries a restart |
| `conditions[type=Ready]` | the reason and message, kstatus-compatible. [Ready reasons of a RestoreRun](#ready-reasons-of-a-restorerun) lists them |

`items[].snapshotID` is the full ID of the snapshot that the run's checks
selected. The restore Job restores exactly that ID. For display, `snapshot`
keeps the short form and `snapshotTime` the time of the snapshot.
[restores.md](restores.md#which-snapshot-a-run-restores) tells how the run
selects. It also tells how a run ends if it lost its selection before it
created anything.

`items[].job` names the item's restore Job in the run's namespace,
`restore-<run uid>-<item index>`. The name stays after the run stops the Job,
as the record that the item had a Job. `items[].jobUID` is the UID of that Job,
and the run stops the Job by it. A pod of a Job that someone deleted keeps the
UID in its `batch.kubernetes.io/controller-uid` label. Thus the run gives
nothing back until each such pod has ended. The run clears `jobUID` when the
Job is stopped.

`items[].clusterLeftDeleted` is true on a database item if the run deleted its
Cluster and nothing created the Cluster again before the run ended.
`items[].clusterUID` is the UID of the Cluster that a database item deletes.
The run writes it with the Deleted mark.
[architecture.md](architecture.md#a-database-restore) shows how the run uses it
to tell the old Cluster from the recovered one.

Ready reason Retrying marks a run whose checks continue to fail with an error
that a retry may fix. An example is a repository with the wrong password. The
phase stays empty.
[namespace-backups.md](namespace-backups.md#checks-before-anything-is-touched)
tells how long the run retries.

A volume item succeeds only when its restore Job has the condition
`Complete=True`. The Job controller adds that condition after restic exited 0
for the item's snapshot and the pod has ended. `Failed=True` fails the item with
reason RestoreJobFailed and restic's exit code.
[restores.md](restores.md#how-a-restore-job-ends) lists the messages. An
in-place item also fails if someone deleted or replaced its claim while the Job
wrote. That section lists those messages too.

A run writes only into a claim that it created itself:

- A claim named `spec.into` that the run does not control fails the run with
  reason IntoClaimTaken.
- A Job with the item's Job name that the run did not create fails the item
  with reason RestoreJobRefused. The run does not touch that Job.

### Ready reasons of a RestoreRun

| Ready reason | When |
| --- | --- |
| Paused | the same as on a [BackupRun](#ready-reasons): the controller runs with `--pause`, and the run has not passed its checks. The phase stays empty, and the run creates no restore Job and stops no workload |
| Retrying | the checks failed with an error that a retry may fix, such as a repository with the wrong password. The phase stays empty, and the message holds the error |
| Running | the run does its work. The message is `restoring`, `restoring into claim <into>` for an `into` restore, or `waiting for pod <pod> to stop before anything is restored` while a pod of a workload the run stopped still terminates |
| SourceBusy | a backup of the run's claim or repository is in progress. Or another run holds the Lease of either. Or another run stopped this namespace's workloads. It is also true if a RestoreRun deleted a Cluster in this namespace and waits for its owner to create it again. The message names that run and what it holds |
| ClaimInUse | an in-place restore waits for its claim: `claim <claim> is mounted by pod <pod>; stop the workload and this restore starts on its own`. The pod can be the restore Job pod of another run that still writes into the claim |
| WaitingForShutdown | the run waits for something to be gone before it continues. That is the instance pods and PVCs of a Cluster it deleted, or the pods of a restore Job it stopped: `waiting for the restore Job of claim <claim>, which the run stopped, to end: restore Job <job>: waiting for pods <pod> to end. The run gives the app back and lets other runs at the claim only after that`. Until then, the app stays stopped and the run keeps its Leases |
| WaitingForRecreate | the run deleted a Cluster and waits for its owner to create it again: `recreate <cluster> to finish the restore: resume the app's Flux Kustomization, or apply the terragrunt unit that declares it` |
| VolSyncUnsupported | the API server no longer serves VolSync's ReplicationSource at v1alpha1. The run changes nothing and retries every pass. At `timeout` it ends TimedOut and gives the app back ([compatibility.md](compatibility.md#following-the-versions-the-api-server-serves)) |
| ClusterVersionUnsupported | the API server no longer serves CloudNativePG's Cluster at v1, so the bootstrap webhook would not see a new Cluster. A run that deleted no Cluster ends with this reason. A run that already deleted one waits with it. A run that failed a Cluster item for this cause ends with this reason, also when it ends in a later pass |
| RestartFailed | the run could not give a workload it stopped its replicas back. Or it could not resume a Kustomization it suspended. Or it could not stop a restore Job while the app is down. The message names what failed and what to scale, resume or delete by hand |
| ReleaseFailed | the app is back. The run cannot finish because it could not release its Leases or stop a restore Job. The message tells what to delete by hand |
| CRDOutdated | the same as on a [BackupRun](#ready-reasons): the CRD of its kind is not installed, lacks a field the controller writes, or may not be read by the controller |
| Invalid | the spec names something that no retry can fix. Examples are an `into` claim that already exists, a Cluster carrying `backup.wlz.li/bootstrap: initdb` named in `database`, or a Cluster that another unfinished RestoreRun restores |
| NoBackupInReach | the run ended before it deleted or wrote anything. An item has no backup at or before `restoreAsOf`. Or the repository of an item holds no snapshot with the layout a VolSync mover writes. The reason also applies to a run that restored nothing because every item was Skipped |
| TimedOut | the run had not finished by its `timeout`. Or it had not passed its checks by then (`the run had not passed its checks by <deadline>: <message>`). Or its `into` claim had not been restored by then (`claim <into> had not been restored by <deadline>`). A run that waited for another run carries that wait: `the run had not finished by <deadline>; it was waiting: <the wait>`. An item whose restore Job still ran gets the timeout's message. When the Job's newest pod waited, the message also gets `; the restore Job's pod was waiting: <container>: <reason>: "<message>"` |
| Succeeded | every item holds the restored data |
| Failed | an item failed, and no Cluster item failed with the item reason ClusterVersionUnsupported. The message names each failed item and its message |

A RestoreRun records an event at each new Ready reason, the same as a
[BackupRun](#backuprun). A run that deleted a Cluster can itself be deleted.
It then records a Warning event with reason ClusterLeftDeleted for each such
Cluster that its owner has not created again yet. It records the event
immediately before it removes its finalizer. If the read of such a Cluster
fails, the run logs the error and records no event for it. It then removes
its finalizer. The note says that the webhook
now recovers that Cluster to the end of its archive, or to the time in its own
`backup.wlz.li/restore-as-of` annotation.
[restores.md](restores.md#databases-restore-themselves) quotes it.

The CRD has six CEL rules:

1. Exactly one of `claim` or `repository`, `database` and `all`.
2. `previous` only with one volume.
3. `into` only with `claim` or `repository`.
4. `intoSize` with `repository` and `into`.
5. `syncDatabaseToVolume` only with `all`.
6. `quiesce` not with `into`.

The fourth rule rejects a run without `intoSize` with this message: `into from a
repository needs intoSize, because there is no source claim to copy a size
from`.

The `quiesced[]` and `suspendedKustomizations[]` of a RestoreRun follow the
same rules as those of a [BackupRun](#backuprun). The run writes them as the
plan before it suspends or scales anything. After a failed stop, it cuts them
to what is in effect.
