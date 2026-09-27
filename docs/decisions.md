# Decisions

Each entry records what was chosen, what other options there were, and what
would have gone wrong with them. A reader who finds one of the rejected options
should be able to tell from the entry alone why the option was rejected.

## Build a populator of our own, and leave VolSync unpatched

VolSync's populator requires `copyMethod: Snapshot` because it fills the prime
claim from `status.latestImage`. A change that lets it restore directly into an
empty prime claim is a modest change to their controller. That change would make
this repository unnecessary.

The update cadence is the reason we rejected it. A fork cannot take a VolSync
release until someone rebases the fork onto that release. VolSync is the
component that does the cluster's backups. A backup tool that gets patches late
is a worse position than a small controller of our own.

The walzen infrastructure already carries one fork, of the Helm provider. That
fork is tolerable because it waits on a pull request that will merge. Here
upstream may well decline. Their populator is snapshot-based by design, so that
one restore can seed any number of claims from a single image. A Direct
populator restores once per claim.

An operator can also adopt a controller one volume at a time, because a claim
chooses its path by what `dataSourceRef` names. A fork changes the behaviour
for every app at once.

## Fill the claim through the library's provider callbacks

`lib-volume-populator` accepts one of two configurations:

- A `PodConfig`, where the library runs a pod of yours against the prime claim.
- A `ProviderFunctionConfig` of three callbacks.

Up to v0.8.x the callbacks created a VolSync ReplicationDestination and waited,
so VolSync's mover did every byte. From v0.9.0 they create the controller's own
restore Job
([Restore through the controller's own Job](#restore-through-the-controllers-own-job)).

The callbacks still decide. The library builds and runs a `PodConfig` pod. The
callbacks let the controller create the same Job that a RestoreRun creates,
from one builder. That Job has its suspend at create, its failure policy, its
queue label and its stop. The callbacks also let the controller read the result
from the Job's conditions. The controller's own process still mounts no volume
and runs no restic.

## Define a data source kind of our own

A claim could continue to name `kind: ReplicationDestination` in
`dataSourceRef`, and nothing in the app would change.

VolSync registers its own populator for that kind. Two populators that watch
the same kind would both act on the same claims, and the result depends on which one
gets there first. No setting on the claim can pick one of the two, so the data
source is a kind of ours.

The kind gives a second advantage. A claim that says `kind: VolumeRestore` says
what fills it. A reader can tell an app on the old path from an app on the new
path by reading either claim.

## Copy the repository Secret for the length of a restore

The restore Job reads the repository Secret through `envFrom`, which names a
Secret in the Job's own namespace. The prime claim is created in the
controller's namespace, so the populator's Job is there too. The app's
repository Secret is in the app's namespace.

The controller copies that Secret into its namespace, with the claim's UID as
the name. It deletes the copy in `PopulateCleanupFn`. This gives the controller
a ClusterRole with `get` on Secrets, which is the one line in the install worth
arguing about.

The alternative is a repository Secret that is in the controller's namespace
from the start. The same environment unit that publishes the backup credentials
today would write it. It needs no Secret read across namespaces. We did not
take it, for these reasons:

- The repository URL differs per volume, so it would mean one Secret per volume
  in a namespace no app owns.
- It would need a naming convention that ties the Secrets to the volumes.
- The app's own Secret already exists and already says which repository the
  volume uses.

Revisit this if the ClusterRole's reach becomes the objection. To narrow `get`
on Secrets to the namespaces that carry a VolumeRestore, you need a Role per
namespace and something to create it. That is more moving parts than the copy.

## Keep the prime claim's storage class and node

The library copies the app claim's access modes, resources and storage class.
For a WaitForFirstConsumer class, it also copies the
`volume.kubernetes.io/selected-node` annotation. A mutator hook could change
any of them before the prime claim is created.

The controller changes none of them. The volume the app gets must be the volume
the app asked for. The node must be the one the scheduler picked for the pod,
or the pod and its volume go to different workers. That last case is not
hypothetical. The snapshot path caused exactly that on the walzen test cluster
on 2026-09-13: a claim annotated for one worker got a volume on another worker.

## Schedule in the controller, and admit a namespace as one unit

From v0.5.0 the controller creates a BackupRun at each tick of a Namespace's
`backup.wlz.li/schedule`, and Kueue admits the run as one Workload.

With VolSync's own per-source schedules, every mover started at the same cron
minute. Kueue admitted them one pod at a time, in the order it picked. The
backups of one namespace's two volumes could run hours apart, with the rest of
the queue in between. A workload stopped for its backup stayed stopped through
all of it. Also, a mover that podAffinity placed with the app's pod never ran
while the app was scaled to zero.

The controller now writes each source with a manual trigger, and it places the
mover by the volume's own node. Thus a stopped workload no longer blocks its
backup, and the controller cuts a namespace's volumes together. The
infrastructure repository's docs/agent/specs/namespace-backups.md holds the
full decision record. That record includes the per-namespace admission that
the admin chose on 2026-09-24.

## Move a quiesced snapshot's time by writing the repository in Go

From v0.6.0 a BackupRun that stopped workloads writes each volume's snapshot
again. The new snapshot has the run's `restartedAt` as its time and the tag
`quiesced`. Thus a restore can recover a database to the moment the volume
holds. restic stamps a snapshot when `restic backup` starts, which is after the
run gave the app back. VolSync's mover passes no `--time`.

These alternatives were possible:

- Keep the app down until restic stamps its time. This would move nothing, but
  nothing tells the controller when that is. VolSync's entry.sh runs `restic
  cat config` before `restic backup`, and both write a lock with the mover
  pod's hostname. Thus a lock under locks/ does not say which command wrote it.
  The mover's log does say it. But to read it, the controller needs access to
  pods/log, and it must parse restic's output.
- A Job that runs `restic rewrite`. This needs a pod per volume per run, and a
  restic image to pin or to discover from VolSync.
- A file next to the repository that pairs each snapshot with a moment. This
  needs retention of its own that follows restic's `forget`.

A BackupRun already records the pairing, but a rebuilt cluster has none of
these records.

The controller already opens the master key to list snapshots. To write a
snapshot is the same format in reverse, plus restic's lock protocol. Thus it
needs no pod, no image and no second object. The admin chose this on
2026-09-25. [namespace-backups.md](namespace-backups.md#quiesced-snapshots)
shows a rewrite on the prod canary.

## Let a RestoreRun list the workloads it stops

From v0.7.0 a RestoreRun stops the Deployments and StatefulSets that its
`quiesce` list names. The `backup.wlz.li/quiesce` annotation stays the
BackupRun's. A workload stopped for a backup is not always one to stop for a
restore. The admin decided on 2026-09-25 that a restore reads its own
definition.

The run gives the workloads back when the volumes are restored and the
databases are deleted. It cannot wait for the databases to recover, for these
reasons:

- A deleted Cluster comes back only when Flux or tofu creates it again.
- A Kustomization that the run suspended creates nothing.
- When the run resumes the Kustomization, Flux applies the workload's replicas
  again together with the Cluster.

## Restore a repository into a plain claim the restore Job fills

A RestoreRun with `repository:` and `into:` creates a plain claim of
`intoSize` with no data source. It also creates a restore Job whose pod writes
the selected snapshot into that claim. Every `into` restore uses that path,
from `claim:` and from `repository:` alike.

The earlier releases did it differently:

- v0.8.0 wrote through a ReplicationDestination with `copyMethod: Direct` for a
  restore from `repository:`.
- Until v0.8.x a run with `claim:` and `into:` wrote a VolumeRestore and a
  claim that names it in `dataSourceRef`, and the populator filled that claim.

The populator path is what v0.7.x ran for both shapes. The run writes a
VolumeRestore and a claim that names it in `dataSourceRef`. On a
WaitForFirstConsumer class, the populator library fills a claim only when
`volume.kubernetes.io/selected-node` is set on it. Only the scheduler sets that
annotation, when it places a pod that uses the claim.

A run with `claim:` copies the source claim's node onto the new claim, which is
the v0.2.4 fix in the README. A run with `repository:` has no source claim to
copy from. No pod uses the new claim until the restore is done, so nothing
ever writes the annotation. The claim stays Pending, the controller's log says
nothing, and the run ends TimedOut. Every class on the walzen cluster binds
WaitForFirstConsumer.

The controller could pick a node and write the annotation itself. To pick a
node that works, it would have to repeat the scheduler's checks of topology,
capacity and taints. It would also have to keep these checks the same as the
scheduler's. A node that fails a check that the controller skipped gets a
volume that the provisioner cannot create or that no pod can reach. The claim
does not say why.

With the restore Job, the Job's pod is the claim's first consumer. The
scheduler places the pod, and the provisioner makes the claim's volume on the
pod's node. An `into` restore from a `claim:` also has a node to copy, and it
copies that node. Thus the copy goes to the pool that holds the original, and
the scheduler places the pod with the volume.

The claim is Bound while the Job still writes into it. Thus the run's Succeeded
phase is what says the data is there, and that phase requires the Job's
condition `Complete=True`. [restores.md](restores.md#submitting-a-restore)
shows the run.

## Leave an opted-out Cluster out of a RestoreRun

From v0.8.0 a RestoreRun does not delete a Cluster that carries
`backup.wlz.li/bootstrap: initdb`. Under `all: true` its item is Skipped. A run
whose `database:` names it ends Invalid.

A run that treated it like any other Cluster would delete it. Flux or tofu
would then create it again, and it would still carry the annotation. The
webhook admits an opted-out Cluster as written, empty, and never writes
`backup.wlz.li/restore-run` on it. The run would find a new Cluster that does
not name it, delete that one too, and repeat until its timeout. The operator
would see the run end TimedOut and the database come back empty each time.

The webhook could ignore the opt-out while a run waits for the Cluster. But the
annotation is how the Cluster's owner asks for an empty database on purpose.
That owner would get a recovered database that it did not ask for.

The opt-out means that the controller stays out of that database's bootstrap.
A database restore works only through the bootstrap, so the run leaves the
Cluster alone. Under `all: true` the rest of the namespace still restores, and
the message of the Skipped item names the annotation. A run that names the
Cluster can do nothing, so it ends Invalid before it touches anything.

Two later cases have their own results:

- A Cluster that gets the annotation after the run marked it Deleted is Skipped
  and never deleted again.
- A Cluster created again with the annotation after the run deleted it fails
  the item. It started empty, and the run says so and leaves it alone.

From v0.9.0 the same is true for any Cluster that comes back without the run's
recovery, whatever the reason. The cause is that the run deletes only the
Cluster whose UID it recorded.
[restores.md](restores.md#starting-a-database-empty) has the messages.

From v0.8.1 the same is true for a Cluster whose owner declares its own
bootstrap method, such as `pg_basebackup` or a `recovery` with a source other
than `backup-controller`. The webhook refuses such a Cluster while a run waits
for it. Thus a run that deleted it would keep the database down until its
timeout and restore nothing.

## Refuse a new database over an archive it could never archive into

From v0.9.0 the webhook refuses a Cluster that would start empty when a WAL
file already exists under `<prefix>/wals/`. This includes two cases:

- A Cluster with no completed base backup to recover from and nothing that asks
  for a recovery.
- A Cluster that carries `backup.wlz.li/bootstrap: initdb`.

The refusal tells the owner to delete the old archive or pick a new
`serverName`.

v0.8.x admitted such a Cluster as written. CloudNativePG started it with
`initdb`. The barman-cloud plugin's empty-archive check then failed every
`archive_command`, because barman fails that check on any WAL file under
`<prefix>/wals/`. The database served traffic while its ContinuousArchiving
condition read False and `pg_wal` filled the volume. Nothing refused or failed
loudly until that volume was full. The opt-out did this every time. Its
documented purpose is to discard a database, which is exactly when its prefix
still holds the old WAL.

The webhook applies barman's own test: it lists `wals/` until the first key
whose name is a WAL file by barman's filter. Thus the webhook and the plugin
agree on every prefix. A prefix that holds only failed base backups, or other
objects, and no WAL is admitted, because the plugin archives into it. An
earlier draft refused any object under the prefix. It was simpler, but it
refused prefixes that barman accepts, and the controller applies the rule of
the pinned upstream source. [compatibility.md](compatibility.md) has the source
lines. A barman upgrade re-checks the filter there.

v0.7.x recovered whenever anything was under `base/`. That fails loudly in the
bootstrap Job when no backup is DONE. It still starts a WAL-only prefix and an
opted-out Cluster empty, and those two never archive.

Another option is to start the database with `initdb` and add
`cnpg.io/skipEmptyWalArchiveCheck`. This lets the database archive, but it
destroys the old archive. barman uploads each segment by its name, and a new
database on timeline 1 writes the same names. Thus the old base backups lose
the WAL they need. They still read DONE, and the next create of the Cluster
would recover from the mixed archive.

An opted-out Cluster now needs its ObjectStore, its Secrets and the object
store to answer. v0.8.x admitted it without reading anything. An outage
refuses the create, which Flux retries, and a Cluster that cannot archive is
never created.

## Compare only bucket and prefix for a shared archive

From v0.9.0 the webhook's shared-archive check calls two Clusters' archives the
same when both of these match:

- The bucket, with letter case ignored.
- The prefix, exactly.

The endpointURL that either ObjectStore names does not matter. v0.8.x also
required the same endpoint host. v0.7.x compared bucket and prefix as this
release does.

A comparison of endpoints cannot tell two names for one service from two
services. v0.8.x changed the host to lowercase and dropped the scheme. It
called each of these pairs different, although both names reach one store:

| Endpoint A | Endpoint B |
| --- | --- |
| `https://s3.example.com` | `https://s3.example.com:443` |
| `http://minio.minio.svc:9000` | `http://minio.minio.svc.cluster.local:9000` |
| an empty endpointURL, the AWS default | `https://s3.amazonaws.com` |
| `https://s3.amazonaws.com` | `https://s3.eu-central-1.amazonaws.com`, for a bucket in that region |
| `https://s3.example.com` | `https://s3.example.com.` |

These pairs also reach one store:

- An Ingress host and the in-cluster Service of one MinIO.
- An IP and a DNS name.
- A CNAME and its target.

For each such pair v0.8.x admitted the second Cluster, and both databases
archived into one prefix. barman writes each segment by its name. Thus the WAL
of the two databases overwrote each other, and every base backup behind them
lost the segments it needed. Nothing reported it until a restore failed.

To make endpoints canonical, the webhook would need a list of aliases that
continues to grow. AWS alone has regional, dual-stack, FIPS, virtual-hosted and
VPC endpoint names. The list would still call an Ingress host and a Service
name different. A DNS lookup of both names would add a DNS dependency to every
Cluster create. It would also admit the Ingress and Service pair, because they
resolve to different addresses for one store.

An annotation that names the other Cluster and declares the two archives
separate would trust the owner's claim. A manifest copied to a new app carries
the claim with it, to a pair that the claim was never about. We know of no
deployment of this controller that needs two S3 services with the same bucket
name and prefix. Thus the webhook has no override.

The webhook refuses two genuinely different services that share a bucket name
and prefix, as v0.7.x did. The refusal says why. It tells the owner to give one
of the two its own prefix in `destinationPath`. For a Cluster that is not yet
created, this changes nothing else.

## Read namespace settings from annotations, and treat an empty one as absent

The timeout and the prune interval are beside the schedule, as
`backup.wlz.li/timeout` and `backup.wlz.li/prune-interval-days` on the
Namespace. An empty value means the controller's default: six hours and one
day.

A Flux app writes its Namespace through a kustomize component, and Flux's
substitution cannot leave a key out. `${BACKUP_TIMEOUT:=""}` renders the key
with an empty value. If the controller refused an empty value, the component
would have to write the defaults out. A later change to the defaults here would
then leave every Flux app pinned to the old defaults.

## Compile the zone database into the binary

The image is `FROM scratch` and holds no /usr/share/zoneinfo, so
`time.LoadLocation` fails inside it. A schedule such as
`CRON_TZ=Europe/Berlin 0 4 * * *` would then fail to parse, and the namespace
would raise NamespaceBackupScheduleInvalid. Since v0.5.3 the binary imports
`time/tzdata`, which adds the zone database to the binary. A test fails the
build if the import is dropped.

The alternative is to copy zoneinfo from the build stage into the image. That
puts a second source of zone data beside Go's own. Also, anyone who rewrites
the Dockerfile can lose it.

## Take retention as restic's own tiers

A claim carries `backup.wlz.li/retain-last` and any of the `-hourly`, `-daily`,
`-weekly`, `-monthly`, `-yearly` and `-within` annotations. The controller
copies each one onto the field of the same name in VolSync's retain block.

Until v0.5.2 only `last` existed. Take a volume kept as seven daily, four
weekly and six monthly snapshots. That version cut it down to its seven newest
snapshots, and six months of history to seven weeks.

## Report status on the VolumeRestore itself

A restore that runs and a restore that failed must both be visible without
controller logs. The object carries kstatus-compatible conditions and a list of
the claims it fills. Thus `kubectl get` answers the question, and a Flux
Kustomization with `wait: true` can gate on it.

## Read the installed CRD before a run changes anything

From v0.9.0 a run reads the CustomResourceDefinition of its kind directly from
the API server before it stops a workload, creates a mover or deletes a
Cluster. The run ends with reason CRDOutdated when the schema lacks a field the
controller writes. The API server drops an undeclared field from every write.
Thus a run under an old CRD could stop an app, record nothing to start it
again, and report Succeeded.

The check walks the fields that the controller writes, as the Go types give
them, and finds each one in the installed schema. Thus an install that applied
the CRDs of the current release passes the check, whatever the previous release
was. The CRDs carry no release number, and the API version `v1alpha1` stays the
same from one release to the next. Thus neither says which fields the installed
schema declares.

A failed read of another kind is retried. But the run fails closed when it may
not read the CRD at all, because it then cannot tell whether its writes
survive. Helm upgrades no CRD on its own. That is why every release ships the
CRDs in the same asset and the message names the fix.

## Write every trigger into spec.restic.unlock too

From v0.9.0 every ReplicationSource that the controller writes carries
`spec.restic.unlock` with the same value as its manual trigger. VolSync runs
`restic unlock` before a backup only while that field differs from
`status.restic.lastUnlocked`. A mover writes that status field when its Job
succeeds. Thus the value must move with every trigger.

A restic that finds a lock left by a killed mover refuses the `forget` after
the backup. The repository then grows by snapshots that nothing deletes.
`restic unlock` deletes only the locks it counts as stale, 30 minutes for a lock
from another host. Thus the trigger clears the lock left behind without an
operator. It does not delete a lock that belongs to a mover that still runs.

## Take one Lease per claim and repository

From v0.9.0 a BackupRun and a RestoreRun each take two `coordination.k8s.io`
Leases immediately before they create their mover object:

- One Lease on the claim they work on.
- One Lease on its repository Secret.

The run releases them when the item ends. The create of a Lease is atomic. Of
two runs that get to that point in the same instant, the API server admits
exactly one. The other run waits with reason SourceBusy.

The alternative was to look for the other run's mover object, which is what the
controller did first. That object is a ReplicationSource that carries a live
run's trigger, or a ReplicationDestination that carries a live run's UID. The
read and the create that follows it are two calls. Two runs that pass the read
together both create their mover. Two movers on one volume write the same
files, and the clone of one run can be cut while the other writes.

The Lease closes that window and does not otherwise hold either run back. It
names its holder by UID, so a run created again under the same name does not
inherit it. When the holder of a Lease is finished or gone, the next run that
wants the Lease takes it over.

## Take one quiesce Lease per namespace

From v0.9.0 a run that is about to stop a namespace's workloads acquires the
`coordination.k8s.io` Lease `backup-controller-quiesce` in that namespace. It
does this before it records the plan in its status. The run holds the Lease
until its stored status shows every workload back and every Kustomization
resumed. A run that finds the Lease held waits with reason SourceBusy, and the
workloads continue to run.

The alternative was a check of the status of the other runs alone, which is
what the controller did first. A BackupRun and a RestoreRun reconcile at the
same time. Thus both can read the workloads' replicas as unchanged, write a
plan, and stop them. The second run then records the zero replicas at which the
first run stopped them. Whichever run restarts last decides whether the app
comes back, and both runs end Succeeded. A Lease settles the race in the API
server, which admits one create.

A Lease per workload would need an order among the Leases. It would only let
two runs whose workloads do not overlap run side by side. But a namespace run
stops every workload the namespace marks, and a restore's list overlaps it in
the ordinary case.

The quiesce Lease stays when the claim and repository Leases go. The run
deletes it when the stored status shows the restart done, at one of these
points:

- At the top of a later pass.
- In finish, after the terminal status write.
- In finalize, after the finalizer is dropped.

A release together with the restart could let another run stop the workloads
while this run's status write that clears `restartPending` was still lost. This
run would then scale the workloads up again under the other run's stop.

A run judges its own restart, and the restart of another run, by the stored
status alone. A run could also read the workloads back and repeat a restart
whose workloads are below their recorded counts. That repeat would scale up an
app that a second run stopped since then. The second run, which holds the Lease
by then, would find its app running in the middle of the backup. With the
status as the only record, a workload that someone scales to zero after a run's
restart stays at zero.

## Refuse a Kustomization that applies two namespaces

From v0.9.0 a run ends with reason Invalid, before it stops anything, in one
case. That case is when the Flux Kustomization of one of its workloads also
lists a Deployment or a StatefulSet in another namespace in its inventory.

A run suspends a workload's Kustomization so that Flux does not scale the
workload up again while it is stopped. Take one Kustomization that applies two
namespaces:

1. A run in the first namespace suspends it. The workloads of the second
   namespace have no drift correction for that time.
2. A run in the second namespace finds the Kustomization already suspended and
   leaves it out of its plan.
3. The resume of the first run then lets Flux scale the app of the second
   namespace up again, in the middle of the backup or restore of that run.

A Lease per Kustomization, acquired in a fixed order across namespaces, would
keep the two runs apart. But a run whose set of targets changed between two
passes could then hold one Lease while it waits for another. Two runs could
wait on each other until both timed out. Every app on the walzen cluster has a
Kustomization of its own, so the refusal asks for the layout the cluster
already uses.

## Upgrade only while no run is active

From v0.9.0 the upgrade procedure requires that no BackupRun or RestoreRun is
active when the image moves. The controller has no code for a run that an older
version started. The new version continues a run left active as if it had
planned that run itself.

The first alternative was to continue each older run under the new code. A
run's status records what the release that wrote it meant. v0.9.0 changed that
meaning in some places:

- v0.7.2 and v0.8.x took no Leases.
- v0.7.2 recorded no snapshot time on a restore item.
- v0.8.1 ran an `into` restore through a VolumeRestore it created for the run.

Each of those needed a code path of its own that no new run used. The path
that read older plans back from the workloads repeated restarts under another
run's stop.

The second alternative was to end each older run on the first reconcile of the
new version, with a reason of its own, through the finish that every failed run
runs. That still needed these additions:

- A field on every run that records the release that planned it.
- A wait in every new run while an older run ends.
- A release step for the VolumeRestore that an older `into` restore created.

A backup lasts minutes. A restore that recovers a Cluster or writes a large
volume can last hours. Thus an operator can wait for the active runs to finish
before the image moves. The new controller then holds no code for temporary
objects that an older version left.

Objects that outlive a run carry across versions as before: VolumeRestores,
schedules, repositories, snapshots, Clusters and the CRDs. Each run checks the
CRDs before it changes anything.
[upgrading.md](upgrading.md#step-1-upgrade-only-while-no-run-is-active) has the
check to run before an upgrade.

## Restore through the controller's own Job

From v0.9.0 every volume restore runs in a Job that the controller builds
itself. This includes a RestoreRun's restore, in place or `into`, and the
populator's fill. The container's command is `restic restore
<full snapshot ID>`, and the Job's conditions are the only result
([architecture.md](architecture.md#how-the-restore-job-is-built)).

v0.8.x restored through VolSync's ReplicationDestination. It accepts only
`restoreAsOf` and `previous`, and no snapshot ID. The controller pinned the
mover to the second of the selected snapshot. To know what the mover would
pick, it copied the selection logic of VolSync's entry.sh, with its
whole-second epoch map and its grep for /data. It refused a snapshot that
shared its second with another, because the mover could restore either.

After the mover, VolSync reports Successful whatever the mover did:

- A mover that found no snapshot in reach exits 0.
- When VolSync cannot read the pod, it clears the mover's log and still reports
  success.

The run had to read the filtered log to learn which snapshot was restored.
VolSync keeps 1024 bytes of that log by default. A VolSync installed with
`MOVER_LOG_MAX_BYTES` 0 left every restore unconfirmed. The populator did not
read the log at all, and it bound whatever the mover wrote. The mover's PID 1
is a shell. Thus the restic of a stopped mover died by SIGKILL and left its
lock.

K8up was the other ready-made restic runner. Its restore reports success for a
restore that failed. That puts the controller back where it read VolSync's log.

The controller's own Job names the snapshot by its full ID. restic resolves
that ID to exactly that snapshot file or fails, so two snapshots in one second
are each restorable. `Complete=True` from the Job controller is positive
evidence that restic exited 0 for that ID, and no log text decides anything.
restic is the container's command, so a stop gets to it as SIGTERM. restic then
deletes its lock and exits 130.

The Job keeps the part of VolSync's mover that a restore needs:

- The mover's security settings and its privileged-movers rule.
- The cache.
- The queue label on the pod.

A fix that VolSync makes to its own restore mover no longer gets to the
controller's restores.

## Declare the restore image once, in the installer

The controller's `--restore-image` has no default, and the controller refuses
to start without it. The walzen infrastructure repository declares one
`restic_image`, pinned by digest, in its volsync unit. The unit passes it to
VolSync's chart as `restic.image` and adds it to the controller's args as
`--restore-image`. Thus the restic that writes the backups is the one that
restores them, and a bump of that one input moves both.

These alternatives were rejected:

- A default of VolSync's own mover image in the controller would pin a VolSync
  version inside the controller. After a VolSync bump in infra, backups would
  run with one restic and restores with another until someone released the
  controller. Nothing would say so.
- To read VolSync's `--restic-container-image` from its Deployment at run time
  would tie the controller to the Deployment args of another project.
- The official `restic/restic` image is a second image, and a different build
  from the one that writes the backups.

The controller checks no repository format. A restic that cannot open the
repository, such as one older than the repository's format, fails the Job. The
item shows restic's exit code and its message. Then the operator bumps the
image.

## Create every restore Job suspended

restorejob.Build creates every restore Job with `spec.suspend: true`. Its owner
records the Job's name and UID. For a RestoreRun the record is in the item's
status. For the populator it is on the prime claim. A later pass that reads
that record back from the API server resumes the Job.

A Job that is not suspended at create starts restic when the Job controller
sees it.
A create that the API server answers with an error can still be stored a
moment later. An example is a 504 on a request that outlived its deadline. By
then the run may have failed the item, restarted the app or finished. Nothing
records the Job, so nothing stops it, and restic writes into a claim that the
app mounts again. The same is true for a Job whose recording status write was
lost.

A suspended Job gets no pod until its owner resumes it. The owner resumes only
a Job that its stored record names, while that record still says that the Job
continues. A Job that no record names stays suspended with no pod. It stays so until
the owner's stop finds it by name or the garbage collector deletes it with its
owner.

Kueue ignores the suspended Job, because the queue label is only on the pod
template. Kueue admits the pod when the Job is resumed. The owner writes one
extra patch per restore.

## Narrow the Jobs grant with an admission policy

From v0.9.0 the controller's ServiceAccount holds create, patch and delete on
`jobs` in every namespace, because RestoreRuns run in the app's namespace. The
release adds the ValidatingAdmissionPolicy `backup-controller-restore-jobs`.
This policy lets that ServiceAccount create and change only Jobs of the restore
Job's shape. It lets the ServiceAccount delete only Jobs labelled as its own
([packaging.md](packaging.md#admission-policy-on-the-restore-jobs) lists the
checks).

RBAC alone cannot say which Jobs. A rule names resources and verbs, and reads
nothing in the object. With RBAC alone, a bug in the controller or its token in
other hands could create a privileged pod with a hostPath mount in any
namespace. That is root on that node.

The policy changes the grant into "may run a restricted pod that mounts
claims". It sets these conditions:

- No host namespaces.
- No node name.
- No ServiceAccount token.
- Only claims, emptyDir and ephemeral claims as volumes.
- At most the capabilities CHOWN, DAC_OVERRIDE and FOWNER, which a privileged
  mover gets.

Under v0.8.x the same ServiceAccount could get as much access through VolSync's
ReplicationDestination.

The policy leaves the image and the environment open, and we accept both:

- It does not check the image. `--restore-image` is the installer's choice. To
  pin it in the policy would need a second declaration, kept equal to the flag
  by hand.
- It does not limit the environment. restic reads `RESTIC_PASSWORD_COMMAND` and
  an rclone program from its environment, which comes from a Secret. The same
  ServiceAccount can create Secrets for the populator's copy.

What the policy guarantees is true for any image and any environment. The
policy also matches the ServiceAccount by its username. Thus an install that
renames the namespace or the ServiceAccount, and does not change the policy,
runs with RBAC alone. The upgrade check in
[upgrading.md](upgrading.md#step-5-change-the-infra-units) reads the policy
back.

## Write replicas through the scale subresource

From v0.9.0 quiesce reads a workload's Scale. It writes the replica count back
through the `scale` subresource of Deployments and StatefulSets. The
ClusterRole grants these verbs:

- get and list on the workloads.
- get and update on `deployments/scale` and `statefulsets/scale`.

It grants no write verb on the workloads themselves.

Up to v0.8.x quiesce sent a merge patch of `spec.replicas` to the workload.
Thus the ClusterRole held `patch` on every Deployment and StatefulSet in the
cluster. That verb lets the ServiceAccount change the image, command or
volumes of any workload. The scale subresource can change only the replica
count. The update carries no resourceVersion, which both kinds accept as
unconditional. Thus a status write by the workload's controller between the
read and the write does not fail the scale.

## Find a backup's snapshot in the repository

From v0.9.0 a BackupRun finds the snapshot that its sync wrote by a list of the
repository. It takes the newest snapshot that a VolSync mover wrote in the
window from the sync's start to its end, as `lastSyncTime` and
`lastSyncDuration` record them
([namespace-backups.md](namespace-backups.md#what-a-run-with-all-set-does)).

Up to v0.8.x the run took the snapshot ID from `snapshot <id> saved` in the
mover's log. It treated `Directory is empty skipping backup` as an empty claim.
When VolSync cannot read the pod, it clears the log and still reports success.
It also keeps only the last 1024 bytes of the log. A log with neither line left
the item Running until its timeout. Without stopped workloads, the item ended
Succeeded with no snapshot.

The repository holds the snapshot, whatever VolSync kept of the log. A window
with no snapshot is an empty claim. An S3 listing can lag a write. Thus the run
lists twice, a poll interval apart, before it records the claim as empty.

## Give the app back when the quiesce limit runs out

From v0.9.0 a run with `all: true` keeps its quiesced workloads stopped for a
`backup.wlz.li/max-quiesce`, ten minutes by default. The limit starts at
`status.quiescedAt`. When the limit expires, the run starts the workloads
again. It fails every volume item whose clone VolSync did not cut, and names
the clone that never appeared.

A run could wait until its `timeout`, six hours by default. That keeps an app
and its database down for six hours because one mover is stuck. Nothing on the
run would say that the run holds the app. The limit puts a bound on that. The
volumes whose clones were cut continue to upload after the app runs again.
Thus a slow mover fails only its own item.

## Fail an item whose source tag no run waits for

A ReplicationSource holds the tag of the run that last used it. VolSync tries
the sync of that tag again until a mover succeeds. Then this sequence can
occur:

1. A later run writes its own trigger.
2. VolSync completes the new trigger with the older backup that the tag still
   holds.
3. The new run records a snapshot of data from before the older run started.

From v0.9.0 a run that finds such an open tag fails that item immediately. The
message names the run or trigger that the tag belongs to, and what VolSync does
with it. It also says how to abandon that backup.

The earlier behaviour was to wait, and this blocked the whole namespace. A
namespace run checks every source before it stops the app. Thus one tag that no
run waited for kept every run of that namespace in SourceBusy until its
timeout. When the run fails the item, it leaves the source alone. VolSync still
finishes by itself when the mover succeeds. The message says how to abandon the
backup when the mover never succeeds.
