# Decisions

Each entry records what was chosen, what else was on the table, and what would
have gone wrong. A reader who arrives at one of the rejected options should be
able to tell from the entry alone why it was rejected.

## Build a populator of our own, and leave VolSync unpatched

VolSync's populator requires `copyMethod: Snapshot` because it fills the prime
claim from `status.latestImage`. Teaching it to restore directly into an empty
prime claim is a modest change to their controller, and it would leave this
repository unnecessary.

It was rejected on update cadence. A fork cannot take a VolSync release until
someone has rebased onto it, and VolSync is the component performing the
cluster's backups. Being slow to patch the backup tool is a worse position than
owning a small controller. The walzen infrastructure already carries one fork,
of the Helm provider, and that one is tolerable because it waits on a pull
request that will merge. Here upstream may well decline: their populator is
snapshot-based by design, so that one restore can seed any number of claims from
a single image, and a Direct populator restores once per claim.

A controller can also be adopted one volume at a time, because a claim chooses
its path by what `dataSourceRef` names. A fork changes behaviour for every app
at once.

## Fill the claim through the library's provider callbacks

`lib-volume-populator` accepts either a `PodConfig`, where the library runs a
pod of yours against the prime claim, or a `ProviderFunctionConfig` of three
callbacks.

Up to v0.8.x the callbacks created a VolSync ReplicationDestination and
waited, so VolSync's mover did every byte. From v0.9.0 they create the
controller's own restore Job
([Restore through the controller's own Job](#restore-through-the-controllers-own-job)).
The callbacks still decide: a `PodConfig` pod is built and run by the library,
while the callbacks let the controller create the same Job a RestoreRun
creates, from one builder, with its suspend at create, its failure policy, its
queue label and its stop, and read its result from the Job's conditions. The
controller's own process still mounts no volume and runs no restic.

## Define a data source kind of our own

A claim could keep naming `kind: ReplicationDestination` in `dataSourceRef`, and
nothing in the app would change.

VolSync registers its own populator for that kind. Two populators watching the
same kind would both act on the same claims, and the result depends on which one
gets there first. No setting on the claim can pick one of the two, so the data
source is a kind of ours.

The kind carries a second advantage: a claim that says `kind: VolumeRestore`
says what fills it, and an app on the old path and an app on the new one are
distinguishable by reading either claim.

## Copy the repository Secret for the length of a restore

The restore Job reads the repository Secret through `envFrom`, which names a
Secret in the Job's own namespace. The prime claim is created in the
controller's namespace, so the populator's Job is there too, and the app's
repository Secret is in the app's namespace.

The controller copies that Secret into its namespace, named for the claim's UID,
and deletes it in `PopulateCleanupFn`. That gives the controller a ClusterRole
with `get` on Secrets, which is the one line in the install worth arguing about.

The alternative is a repository Secret that lives in the controller's namespace
from the start, written by the same environment unit that publishes the backup
credentials today. It needs no Secret read across namespaces. It was not taken
because the repository URL differs per volume, so it would mean one Secret per
volume in a namespace no app owns, and a naming convention tying them together;
the app's own Secret already exists and already says which repository the volume
uses.

Revisit this if the ClusterRole's reach becomes the objection. Narrowing `get`
on Secrets to the namespaces that carry a VolumeRestore needs a Role per
namespace and something to create it, which is more moving parts than the copy.

## Keep the prime claim's storage class and node

The library copies the app claim's access modes, resources, storage class and,
for a WaitForFirstConsumer class, its `volume.kubernetes.io/selected-node`
annotation. A mutator hook could change any of them before the prime claim is
created.

None is changed. The volume the app ends up with has to be the volume the app
asked for, and the node has to be the one the scheduler picked for the pod, or
the pod and its volume end up on different workers. That last case is not
hypothetical: the snapshot path produced exactly that on the walzen test cluster
on 2026-09-13, a claim annotated for one worker whose volume was created on
another.

## Schedule in the controller, and admit a namespace as one unit

From v0.5.0 the controller creates a BackupRun at each tick of a Namespace's
`backup.wlz.li/schedule`, and Kueue admits the run as one Workload.

With VolSync's own per-source schedules, every mover started at the same cron
minute and Kueue admitted them one pod at a time in the order it picked. One
namespace's two volumes could be backed up hours apart with the rest of the queue
in between, and a workload stopped for its backup stayed stopped through all of
it. A mover placed by podAffinity to the app's pod also never ran while the app
was scaled to zero.

The controller now writes each source with a manual trigger and places its
mover by the volume's own node, so a stopped workload no longer blocks its
backup and a namespace's volumes are cut together. The infrastructure
repository's docs/agent/specs/namespace-backups.md holds the full decision
record, with the per-namespace admission the admin chose on 2026-09-24.

## Move a quiesced snapshot's time by writing the repository in Go

From v0.6.0 a BackupRun that stopped workloads writes each volume's snapshot
again with the run's `restartedAt` as its time and the tag `quiesced`, so a
restore can recover a database to the moment the volume holds. restic stamps a
snapshot when `restic backup` starts, after the run has given the app back, and
VolSync's mover passes no `--time`.

Keeping the app down until restic has stamped its time would move nothing, but
nothing tells the controller when that is. VolSync's entry.sh runs `restic cat
config` before `restic backup`, and both write a lock with the mover pod's
hostname, so a lock under locks/ does not say which command wrote it. The
mover's log does say it, and reading it would give the controller access to
pods/log and make it parse restic's output. A Job running `restic rewrite` needs a pod per volume per run
and a restic image to pin or to discover from VolSync. A file pairing each
snapshot with a moment, stored next to the repository, needs retention of its
own that follows restic's `forget`. A BackupRun already records the pairing,
and a rebuilt cluster has none of them.

The controller already opens the master key to list snapshots. Writing a
snapshot is the same format in reverse, plus restic's lock protocol, so it
needs no pod, no image and no second object. The admin chose this on
2026-09-25. [namespace-backups.md](namespace-backups.md#quiesced-snapshots)
shows a rewrite on the prod canary.

## Let a RestoreRun list the workloads it stops

From v0.7.0 a RestoreRun stops the Deployments and StatefulSets its `quiesce`
list names. The `backup.wlz.li/quiesce` annotation stays the BackupRun's: a
workload stopped for a backup is not always one to stop for a restore, and the
admin decided on 2026-09-25 that a restore reads its own definition.

The run gives the workloads back once the volumes are restored and the
databases deleted. It cannot wait for the databases to recover: a deleted
Cluster comes back only when Flux or tofu creates it again, a Kustomization the
run suspended creates nothing, and resuming it reapplies the workload's
replicas along with the Cluster.

## Restore a repository into a plain claim the restore Job fills

A RestoreRun with `repository:` and `into:` creates a plain claim of
`intoSize` with no data source, and a restore Job whose pod writes the selected
snapshot into that claim. Every `into` restore goes through that path, from
`claim:` and from `repository:` alike. v0.8.0 wrote through a
ReplicationDestination with `copyMethod: Direct` there, and until v0.8.x a run
with `claim:` and `into:` wrote a VolumeRestore and a claim naming it in
`dataSourceRef`, and the populator filled that claim.

The populator path is what v0.7.x ran for both shapes: the run writes a
VolumeRestore and a claim naming it in `dataSourceRef`. On a
WaitForFirstConsumer class, the populator library fills a claim only once
`volume.kubernetes.io/selected-node` is set on it, and only the scheduler sets
that annotation, when it places a pod that uses the claim. A run with `claim:`
copies the source claim's node onto the new claim, which is the v0.2.4 fix in
the README. A run with `repository:` has no source claim to copy from, and no
pod uses the new claim until the restore is done, so nothing ever writes the
annotation. The claim stays Pending, the controller's log says nothing, and the
run ends TimedOut. Every class on the walzen cluster binds WaitForFirstConsumer.

The controller could pick a node and write the annotation itself. To pick one
that works, it would have to repeat the scheduler's checks of topology,
capacity and taints, and keep them in step with the scheduler. A node that
fails a check the controller skipped gets a volume the provisioner cannot
create or no pod can reach, and the claim does not say why.

With the restore Job, the Job's pod is the claim's first consumer. The
scheduler places the pod, and the claim is provisioned on the pod's node. An
`into` restore from a `claim:` has a node to copy as well, and copies it, so
the copy lands on the pool that holds the original and the scheduler places
the pod with the volume. The claim is Bound while the Job still writes into it,
so the run's Succeeded phase is what says the data is there, and that phase
requires the Job's condition `Complete=True`.
[restores.md](restores.md#submitting-a-restore) shows the run.

## Leave an opted-out Cluster out of a RestoreRun

From v0.8.0 a RestoreRun does not delete a Cluster that carries
`backup.wlz.li/bootstrap: initdb`. Under `all: true` its item is Skipped, and a
run whose `database:` names it ends Invalid.

A run that treated it like any other Cluster would delete it, and Flux or tofu
would create it again, still carrying the annotation. The webhook admits an
opted-out Cluster as written, empty, and never writes
`backup.wlz.li/restore-run` on it. The run would find a new Cluster that does
not name it, delete that one too, and repeat until its timeout. The operator
would see the run end TimedOut and the database come back empty each time.

The webhook could instead ignore the opt-out while a run waits for the
Cluster. The annotation is how the Cluster's owner asks for an empty database
on purpose, and that owner would get a recovered one it did not ask for.

The opt-out means the controller stays out of that database's bootstrap, and a
database restore works only through the bootstrap, so the run leaves the
Cluster alone. Under `all: true` the rest of the namespace still restores, and
the Skipped item's message names the annotation. A run that names the Cluster
has nothing it could do, so it ends Invalid before it touches anything. A
Cluster that gains the annotation after the run marked it Deleted is Skipped and
never deleted again, while one created again with the annotation after the run
deleted it fails the item: it started empty, and the run says so and leaves it
alone. From v0.9.0 the same holds for any Cluster that comes back without the
run's recovery, whatever the reason, because the run deletes only the Cluster
whose UID it recorded.
[restores.md](restores.md#starting-a-database-empty)
has the messages.

From v0.8.1 the same holds for a Cluster whose owner declares its own bootstrap
method, such as `pg_basebackup` or a `recovery` with a source other than
`backup-controller`. The webhook refuses such a Cluster while a run waits for
it, so a run that deleted it would keep the database down until its timeout
and restore nothing.

## Refuse a new database over an archive it could never archive into

From v0.9.0 the webhook refuses a Cluster that would start empty when
anything already exists under its prefix. That covers a Cluster with no
completed base backup to recover from and nothing asking for a recovery, and a
Cluster carrying `backup.wlz.li/bootstrap: initdb`. The refusal tells the owner
to delete the old archive or pick a new `serverName`.

v0.8.x admitted such a Cluster as written. CloudNativePG started it with
`initdb`, and the barman-cloud plugin's empty-archive check then failed every
`archive_command`, because barman fails that check on any WAL file under
`<prefix>/wals/`. The database served traffic while its ContinuousArchiving
condition read False and `pg_wal` filled the volume. Nothing refused or failed
loudly until that volume was full.
The opt-out did this every time: its documented purpose is discarding a
database, which is exactly when its prefix still holds the old WAL.

Refusing only when a WAL file exists would mirror barman's check exactly. The
webhook would have to copy barman's filter for WAL file names and list
`wals/` until the first match. A future barman that counts one more kind of
file as WAL would make the two disagree, and the webhook would admit a Cluster
that never archives, which is the failure this decision prevents. Refusing on
any object needs one listing call and no knowledge of barman's file names.

It refuses one prefix that barman would accept: one holding only failed base
backups and no WAL. That store arises only when a base backup started while
the archive held no WAL, or when someone deleted the WAL by hand. It holds
nothing a recovery can use, so deleting it discards nothing, and the refusal
says to do that.

Recovering whenever anything is under `base/`, as v0.7.x did, fails loudly in
the bootstrap Job when no backup is DONE. It still starts a WAL-only prefix
and an opted-out Cluster empty, and those two never archive.

Starting the database with `initdb` and adding
`cnpg.io/skipEmptyWalArchiveCheck` lets it archive, and destroys the old
archive. barman uploads each segment by its name, and a new database on
timeline 1 writes the same names, so the old base backups lose the WAL they
need. They still read DONE, and the next create of the Cluster would recover
from the mixed archive.

An opted-out Cluster now needs its ObjectStore, its Secrets and the object
store to answer, where v0.8.x admitted it without reading anything. An outage
refuses the create, which Flux retries, and a Cluster that cannot archive is
never created.

## Compare only bucket and prefix for a shared archive

From v0.9.0 the webhook's shared-archive check calls two Clusters' archives
the same when the bucket matches, ignoring letter case, and the prefix matches
exactly, whatever endpointURL either ObjectStore names. v0.8.x also required
the same endpoint host, and v0.7.x compared bucket and prefix as this release
does.

Comparing endpoints cannot tell two names for one service apart from two
services. v0.8.x lowercased the host and dropped the scheme, and called each
of these pairs different although both names reach one store:

| Endpoint A | Endpoint B |
| --- | --- |
| `https://s3.example.com` | `https://s3.example.com:443` |
| `http://minio.minio.svc:9000` | `http://minio.minio.svc.cluster.local:9000` |
| an empty endpointURL, the AWS default | `https://s3.amazonaws.com` |
| `https://s3.amazonaws.com` | `https://s3.eu-central-1.amazonaws.com`, for a bucket in that region |
| `https://s3.example.com` | `https://s3.example.com.` |

An Ingress host and the in-cluster Service of one MinIO, an IP and a DNS name,
or a CNAME and its target reach one store too. For each such pair v0.8.x
admitted the second Cluster, and both databases archived into one prefix.
barman writes each segment by its name, so the two databases' WAL overwrote
each other and every base backup behind them lost the segments it needed.
Nothing reported it until a restore failed.

Canonicalizing endpoints would need a list of aliases that keeps growing (AWS
alone has regional, dual-stack, FIPS, virtual-hosted and VPC endpoint names),
and it would still call an Ingress host and a Service name different.
Resolving both names in DNS would add a DNS dependency to every Cluster
create. It would also admit the Ingress and Service pair, since they resolve to
different addresses for one store.

An annotation naming the other Cluster, declaring the two archives separate,
would trust the owner's claim. A manifest copied to a new app carries the
claim with it, to a pair it was never about. No deployment of this controller
is known to need two S3 services with the same bucket name and prefix, so the
webhook has no override.

Two genuinely different services that share a bucket name and prefix are
refused, as they were under v0.7.x. The refusal says why and tells the owner
to give one of the two its own prefix in `destinationPath`, which for a
Cluster being created changes nothing else.

## Read namespace settings from annotations, and treat an empty one as absent

The timeout and the prune interval live beside the schedule, as
`backup.wlz.li/timeout` and `backup.wlz.li/prune-interval-days` on the
Namespace, and an empty value means the controller's default: six hours and one
day.

A Flux app writes its Namespace through a kustomize component, and Flux's
substitution cannot leave a key out: `${BACKUP_TIMEOUT:=""}` renders the key
with an empty value. Had the controller refused an empty value, the component
would have had to write the defaults out, and a later change to the defaults
here would have left every Flux app pinned to the old ones.

## Compile the zone database into the binary

The image is `FROM scratch` and holds no /usr/share/zoneinfo, so
`time.LoadLocation` fails inside it. A schedule such as
`CRON_TZ=Europe/Berlin 0 4 * * *` would then fail to parse, and the namespace
would raise NamespaceBackupScheduleInvalid. Since v0.5.3 the binary imports
`time/tzdata`, which adds the zone database to it, and a test fails the build if
the import is dropped. The alternative, copying zoneinfo from the build stage
into the image, puts a second source of zone data beside Go's own and is lost
by anyone who rewrites the Dockerfile.

## Take retention as restic's own tiers

A claim carries `backup.wlz.li/retain-last` and any of the `-hourly`, `-daily`,
`-weekly`, `-monthly`, `-yearly` and `-within` annotations, each copied onto the
field of the same name in VolSync's retain block. Until v0.5.2 only `last`
existed, which cut a volume kept as seven daily, four weekly and six monthly
snapshots down to its seven newest, and six months of history to seven weeks.

## Report status on the VolumeRestore itself

A restore that is running, and a restore that has failed, both have to be
visible without reading controller logs. The object carries kstatus-compatible
conditions and a list of the claims being filled, so `kubectl get` answers the
question and a Flux Kustomization with `wait: true` can gate on it.

## Read the installed CRD before a run changes anything

From v0.9.0 a run reads the CustomResourceDefinition of its kind straight from
the API server before it stops a workload, creates a mover or deletes a
Cluster, and ends with reason CRDOutdated when the schema lacks a field the
controller writes. The API server drops an undeclared field from every write,
so a run under an old CRD could stop an app, record nothing to start it again,
and report Succeeded.

The check walks the fields the controller writes out of the Go types, and looks
each one up in the installed schema, so an install that applied the CRDs of the
current release passes it whatever the previous release was. The CRDs carry
no release number, and the API version `v1alpha1` stays the same from one
release to the next, so neither says which fields the installed schema
declares. A failed read of
another kind is retried, but the run fails closed when it may not read the CRD
at all, since it then cannot tell whether its writes survive. Helm upgrades no
CRD on its own, which is why every release ships them in the same asset and the
message names the fix.

## Write every trigger into spec.restic.unlock too

From v0.9.0 every ReplicationSource the controller writes carries
`spec.restic.unlock` with the same value as its manual trigger. VolSync runs
`restic unlock` before a backup only while that field differs from
`status.restic.lastUnlocked`, which a mover writes once its Job succeeds, so
the value has to move with every trigger. A restic that finds a lock left by a
killed mover refuses the `forget` after the backup, and the repository then
grows by snapshots nothing deletes. `restic unlock` removes only the locks it
counts as stale, 30 minutes for a lock from another host, so this clears the
lock left behind without an operator and without removing a lock that belongs
to a mover still running.

## Take one Lease per claim and repository

From v0.9.0 a BackupRun and a RestoreRun each take a `coordination.k8s.io`
Lease on the claim they work on and one on its repository Secret, right before
they create their mover object, and release it once the item ends. Creating a
Lease is atomic, so of two runs that reach that point in the same instant the
API server admits exactly one, and the other waits with reason SourceBusy.

The alternative was to look for the other run's mover object, which is what the
controller did first: a ReplicationSource carrying a live run's trigger, or a
ReplicationDestination carrying a live run's UID. That read and the create that
follows it are two calls, and two runs that pass the read together both create
their mover; two movers on one volume write the same files, and one run's clone
can be cut while the other writes. The Lease closes that window without holding
either run back otherwise: it names its holder by UID, so a run created again
under the same name does not inherit it, and a Lease whose holder has finished
or is gone is taken over by the next run that wants it.

## Take one quiesce Lease per namespace

From v0.9.0 a run that is about to stop a namespace's workloads acquires the
`coordination.k8s.io` Lease `backup-controller-quiesce` in that namespace
before it records the plan in its status. The run holds the Lease until its
stored status shows every workload back and every Kustomization resumed, and a
run that finds the Lease held waits with reason SourceBusy and the workloads
running.

The alternative was a check of the other runs' status alone, which is what the
controller did first. A BackupRun and a RestoreRun reconcile at the same time,
so both can read the workloads' replicas as unchanged, write a plan, and stop
them. The second then records the zero replicas the first stopped them at, and
whichever restarts last decides whether the app comes back; both runs end
Succeeded. A Lease settles the race in the API server, which admits one create.
A Lease per workload would need an ordering among the Leases and would only let
two runs whose workloads do not overlap run side by side, while a namespace run
stops every workload the namespace marks and a restore's list overlaps it in
the ordinary case.

The quiesce Lease stays when the claim and repository Leases go. The run
deletes it once the stored status shows the restart done: at the top of a
later pass, in finish after the terminal status write, and in finalize after
the finalizer is dropped. A release together with the restart would let
another run stop the workloads while this run's status write that clears
`restartPending` was still lost, and this run would then scale them back up
under the other run's stop.

A run judges its own restart, and another run's, by the stored status alone.
A run could also read the workloads back and repeat a restart whose workloads
stand below their recorded counts. That repeat would scale up an app a second
run had stopped since, and the second run, which holds the Lease by then,
would find its app running mid-backup. With the status as the only record, a
workload someone scales to zero after a run's restart stays at zero.

## Refuse a Kustomization that applies two namespaces

From v0.9.0 a run ends with reason Invalid, before it stops anything, when the
Flux Kustomization of one of its workloads also lists a Deployment or a
StatefulSet in another namespace in its inventory.

A run suspends a workload's Kustomization so that Flux does not scale the
workload back up while it is stopped. With one Kustomization applying two
namespaces, a run in the first namespace suspends it, and the second
namespace's workloads are left without drift correction for that time. A run
in the second namespace would find the Kustomization already suspended and
leave it out of its plan; the first run's resume then lets Flux scale the
second namespace's app back up in the middle of that run's backup or restore.
A Lease per Kustomization, acquired in a fixed order across namespaces, would
keep the two runs apart. A run whose set of targets changed between two passes
could then hold one Lease while it waited for another, and two runs could wait
on each other until both timed out. Every app on the walzen cluster has a Kustomization
of its own, so the refusal asks for the layout the cluster already uses.

## Upgrade only while no run is active

From v0.9.0 the upgrade procedure requires that no BackupRun or RestoreRun is
active when the image moves, and the controller has no code for a run that an
older version started. The new version carries on a run left active as if it
had planned that run itself.

The first alternative was to continue each older run under the new code. A
run's status records what the release that wrote it meant, and v0.9.0 changed
that meaning in places: v0.7.2 and v0.8.x took no Leases, v0.7.2 recorded no
snapshot time on a restore item, and v0.8.1 ran an `into` restore through a
VolumeRestore it created for the run. Each of those needed a code path of its
own that no new run used, and the path that read older plans back from the
workloads repeated restarts under another run's stop.

The second alternative was to end each older run on the new version's first
reconcile, with a reason of its own, through the finish every failed run runs.
That still needed a field on every run recording the release that planned it,
a wait in every new run for an older run being ended, and a release step for
the VolumeRestore an older `into` restore had created.

A backup lasts minutes, and a restore that recovers a Cluster or writes a large
volume can last hours, so an operator can wait for the active runs to
finish before moving the image, and the new controller then holds no code for
temporary objects an older version left. Objects that outlive a run carry
across versions as before: VolumeRestores, schedules, repositories, snapshots,
Clusters and the CRDs, which each run checks before it changes anything.
[upgrading.md](upgrading.md#step-1-upgrade-only-while-no-run-is-active) has the
check to run before an upgrade.

## Restore through the controller's own Job

From v0.9.0 every volume restore, a RestoreRun's in place or `into` and the
populator's fill, runs in a Job the controller builds itself: `restic restore
<full snapshot ID>` as the container's command, and the Job's conditions as
the only result ([architecture.md](architecture.md#how-the-restore-job-is-built)).

VolSync's ReplicationDestination, which v0.8.x restored through, accepts only
`restoreAsOf` and `previous` and no snapshot ID. The controller pinned the
mover to the selected snapshot's second, and to know what the mover would pick
it copied the selection logic of VolSync's entry.sh, with its whole-second
epoch map and its grep for /data. It refused a snapshot that shared its second
with another, since the mover could restore either. After the mover, VolSync
reports Successful whatever the mover did: a mover that found no snapshot in
reach exits 0, and VolSync clears the mover's log and still reports success
when it cannot read the pod. The run had to read the filtered log, of which
VolSync keeps 1024 bytes by default, to learn which snapshot was restored, and
a VolSync installed with `MOVER_LOG_MAX_BYTES` 0 left every restore
unconfirmed. The populator did not read the log at all and bound whatever the
mover wrote. The mover's PID 1 is a shell, so a stopped mover's restic died by
SIGKILL and left its lock.

K8up was the other ready-made restic runner. Its restore reports success for a
restore that failed, which puts the controller back where it read VolSync's
log.

The controller's own Job names the snapshot by its full ID, which restic
resolves to exactly that snapshot file or fails, so two snapshots in one
second are each restorable. `Complete=True` from the Job controller is
positive evidence that restic exited 0 for that ID, and no log text decides
anything. restic is the container's command, so a stop reaches it as SIGTERM,
and it removes its lock and exits 130. What the Job keeps of VolSync's mover is
the part a restore needs: the mover's security settings and its
privileged-movers rule, the cache, and the queue label on the pod. A fix VolSync
makes to its own restore mover no longer reaches the controller's restores.

## Declare the restore image once, in the installer

The controller's `--restore-image` has no default, and the controller refuses
to start without it. The walzen infrastructure repository declares one
`restic_image`, pinned by digest, in its volsync unit, passes it to VolSync's
chart as `restic.image`, and appends it to the controller's args as
`--restore-image`, so the restic that writes the backups is the one that
restores them and a bump of that one input moves both.

A default of VolSync's own mover image in the controller would pin a VolSync
version inside the controller. A VolSync bump in infra would then run backups
with one restic and restores with another until someone released the
controller, and nothing would say so. Reading VolSync's
`--restic-container-image` from its Deployment at run time would tie the
controller to another project's Deployment args. The official `restic/restic`
image is a second image and a different build from the one that writes the
backups.

The controller checks no repository format. A restic that cannot open the
repository, such as one older than the repository's format, fails the Job, and
the item shows restic's exit code and its message; then the image is bumped.

## Create every restore Job suspended

restorejob.Build creates every restore Job with `spec.suspend: true`. Its owner
records the Job's name and UID, a RestoreRun in the item's status and the
populator on the prime claim, and a later pass that reads that record back
from the API server resumes the Job.

A Job created running starts restic as soon as the Job controller sees it. A
create that the API server answers with an error, such as a 504 on a request
that outlived its deadline, can still be stored a moment later. By then the run
may have failed the item, restarted the app or finished, and nothing records
the Job, so nothing stops it: restic writes into a claim the app mounts again.
The same holds for a Job whose recording status write was lost.

A suspended Job gets no pod until its owner resumes it, and the owner resumes
only a Job its stored record names, while that record still goes on with it. A
Job no record names stays suspended with no pod, until the owner's stop finds
it by name or the garbage collector deletes it with its owner. Kueue ignores
the suspended Job, because the queue label is only on the pod template, and
admits the pod once the Job is resumed. The owner writes one extra patch per
restore.

## Narrow the Jobs grant with an admission policy

From v0.9.0 the controller's ServiceAccount holds create, patch and delete on
`jobs` in every namespace, because RestoreRuns run in the app's namespace. The
release adds the ValidatingAdmissionPolicy `backup-controller-restore-jobs`,
which lets that ServiceAccount create and change only Jobs of the restore Job's
shape and delete only Jobs labelled as its own
([packaging.md](packaging.md#admission-policy-on-the-restore-jobs) lists the
checks).

RBAC alone cannot say which Jobs: a rule names resources and verbs, and reads
nothing in the object. With RBAC alone, a bug in the controller or its token in
other hands could create a privileged pod with a hostPath mount in any
namespace, which is root on that node. The policy turns the grant into "may
run a restricted pod that mounts claims": no host namespaces, no node name, no
ServiceAccount token, only claims, emptyDir and ephemeral claims as volumes,
and at most the capabilities CHOWN, DAC_OVERRIDE and FOWNER a privileged mover
gets. Under v0.8.x the same ServiceAccount could reach as much through
VolSync's ReplicationDestination.

The policy leaves the image and the environment open, and we accept both. It
does not check the
image: `--restore-image` is the installer's choice, and pinning it in the
policy would need a second declaration kept equal to the flag by hand. It does
not bound the environment either: restic reads `RESTIC_PASSWORD_COMMAND` and
an rclone program from its environment, which comes from a Secret, and the same
ServiceAccount can create Secrets for the populator's copy. What the policy
guarantees holds for any image and any environment. The policy also matches the
ServiceAccount by its username, so an install that renames the namespace or
the ServiceAccount without changing the policy runs with RBAC alone; the
upgrade check in [upgrading.md](upgrading.md#step-5-change-the-infra-units)
reads the policy back.

## Write replicas through the scale subresource

From v0.9.0 quiesce reads a workload's Scale and writes the replica count back
through the `scale` subresource of Deployments and StatefulSets, and the
ClusterRole grants get and list on the workloads and get and update on
`deployments/scale` and `statefulsets/scale`, with no write verb on the
workloads themselves.

Up to v0.8.x quiesce sent a merge patch of `spec.replicas` to the workload, so
the ClusterRole held `patch` on every Deployment and StatefulSet in the
cluster. That verb lets the ServiceAccount change any workload's image,
command or volumes. The scale subresource can change nothing but the replica
count. The update carries no resourceVersion, which both kinds accept as
unconditional, so a status write by the workload's controller between the
read and the write does not fail the scale.

## Find a backup's snapshot in the repository

From v0.9.0 a BackupRun finds the snapshot its sync wrote by listing the
repository: the newest snapshot a VolSync mover wrote in the window from the
sync's start to its end, as `lastSyncTime` and `lastSyncDuration` record them
([namespace-backups.md](namespace-backups.md#what-a-run-with-all-set-does)).

Up to v0.8.x the run took the snapshot ID from `snapshot <id> saved` in the
mover's log and treated `Directory is empty skipping backup` as an empty
claim. VolSync clears the log and still reports success when it cannot read
the pod, and keeps only its last 1024 bytes, so a log with neither line left
the item Running until its timeout, or, without stopped workloads, Succeeded
with no snapshot. The repository holds the snapshot whatever VolSync kept of
the log. A window with no snapshot is an empty claim, and because an S3
listing can lag a write, the run lists twice, a poll interval apart, before it
records the claim as empty.

## Give the app back when the quiesce limit runs out

From v0.9.0 a run with `all: true` keeps its quiesced workloads stopped for a
`backup.wlz.li/max-quiesce`, ten minutes by default, counted from
`status.quiescedAt`. When the limit runs out the run gives the workloads back
and fails every volume item whose clone VolSync has not cut, naming the clone
that never appeared.

A run could instead wait until its `timeout`, six hours by default. That leaves
an app and its database down for six hours because one mover is stuck, with
nothing on the run saying the app is what is being held. The limit bounds that,
and the volumes whose clones were cut go on uploading after the app is running,
so a slow mover fails only its own item.

## Fail an item whose source tag no run waits for

A ReplicationSource holds the tag of the run that last used it, and VolSync
retries that tag's sync until a mover succeeds. A later run writes its own
trigger, VolSync completes the new trigger with the older backup the tag still
holds, and the new run records a snapshot of data from before the older run
started. From v0.9.0 a run that finds such an open tag fails that item at once,
names the run or trigger the tag belongs to, what VolSync is doing with it, and
how to give that backup up.

Waiting was the earlier behaviour, and it held up the whole namespace: a
namespace run checks every source before it stops the app, so one tag no run
waited for kept every run of that namespace in SourceBusy until its timeout. Failing the
item leaves the source alone, and VolSync still finishes on its own once the
mover succeeds; the message says how to give the backup up when it never will.
