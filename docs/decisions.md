# Decisions

Each entry records what was chosen, what else was on the table, and what would
have gone wrong. A reader who arrives at one of the rejected options should be
able to tell from the entry alone why it was rejected.

## Build a populator rather than patch VolSync

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

## Use the library's provider callbacks rather than a populator pod

`lib-volume-populator` accepts either a `PodConfig`, where the library runs a
pod of yours against the prime claim, or a `ProviderFunctionConfig` of three
callbacks.

A pod would have to do the restore, which means restic in an image of ours, the
repository credentials mounted into it, a second implementation of something
VolSync already does, and a restore that runs outside the cluster's backup
queue. Every one of those is a standing cost.

The callbacks let the fill step be "create a ReplicationDestination and wait",
so VolSync's own mover does every byte with the credentials and the queue it
already uses. The controller holds no repository credentials, mounts no volume
and contains no restic code.

## Define a kind of our own rather than reuse ReplicationDestination

A claim could keep naming `kind: ReplicationDestination` in `dataSourceRef`, and
nothing in the app would change.

VolSync registers its own populator for that kind. Two populators watching the
same kind would both act on the same claims, and the result depends on which one
gets there first. That is a conflict rather than a preference, so the data source
is a kind of ours.

The kind carries a second advantage: a claim that says `kind: VolumeRestore`
says what fills it, and an app on the old path and an app on the new one are
distinguishable by reading either claim.

## Copy the repository Secret for the length of a restore

VolSync resolves `spec.restic.repository` in the ReplicationDestination's own
namespace. The prime claim is created in the controller's namespace, so the
destination is there too, and the app's repository Secret is in the app's
namespace.

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

## Restore a repository into a plain claim through a Direct ReplicationDestination

From v0.8.0 a RestoreRun with `repository:` and `into:` creates a plain claim
of `intoSize` with no data source, and a ReplicationDestination with
`copyMethod: Direct` whose mover writes the selected snapshot into that claim.
A run with `claim:` and `into:` keeps the populator path.

The populator path is what v0.7.x ran for both shapes: the run writes a
VolumeRestore and a claim naming it in `dataSourceRef`. On a
WaitForFirstConsumer class, the populator library fills a claim only once
`volume.kubernetes.io/selected-node` is set on it, and only the scheduler sets that annotation, when it places a pod that uses the claim. A run
with `claim:` copies the source claim's node onto the new claim, which is the
v0.2.4 fix in the README. A run with `repository:` has no source claim to copy
from, and no pod uses the new claim until the restore is done, so nothing ever
writes the annotation. The claim stays Pending, the controller's log says
nothing, and the run ends TimedOut. Every class on the walzen cluster binds
WaitForFirstConsumer.

The controller could pick a node and write the annotation itself. To pick one
that works, it would have to repeat the scheduler's checks of topology,
capacity and taints, and keep them in step with the scheduler. A node that
fails a check the controller skipped gets a volume the provisioner cannot
create or no pod can reach, and the claim does not say why.

With a Direct destination, the mover pod is the claim's first consumer. The
scheduler places the mover, and the claim is provisioned on the mover's node,
the way VolSync places a destination claim it creates itself. The claim is
Bound while the mover still writes into it, so the run's Succeeded phase is
what says the data is there. [restores.md](restores.md#submitting-a-restore)
shows the run.

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
Cluster that gains the annotation after the run marked it Deleted, or comes back
with it, is Skipped and never deleted again. [restores.md](restores.md#starting-a-database-empty)
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

## Compare bucket and prefix, and never the endpoint, for a shared archive

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

## Report status on the VolumeRestore rather than only in logs

A restore that is running, and a restore that has failed, both have to be
visible without reading controller logs. The object carries kstatus-compatible
conditions and a list of the claims being filled, so `kubectl get` answers the
question and a Flux Kustomization with `wait: true` can gate on it.
