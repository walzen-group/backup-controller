# Upgrading

Each section lists what to check before moving a cluster to that release, and
what to do during and after the upgrade. The Releases table in the
[README](../README.md#releases) lists what each release changed.

## v0.9.0

### Step 1: Upgrade only while no run is active

Move the image only while no BackupRun or RestoreRun is active
([decisions.md](decisions.md#upgrade-only-while-no-run-is-active)). A run lasts
minutes, so wait for the running ones to finish.

List the runs that have not finished:

```
kubectl get backupruns,restoreruns -A -o json | jq -r '.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed") | "\(.kind)\t\(.metadata.namespace)/\(.metadata.name)\t\(.status.phase // "not planned")"'
```

Expected result: no output.

For each line, wait until the run finishes, or delete it:

```
kubectl -n <namespace> delete backuprun <name>
```

Delete a RestoreRun with `restorerun` in place of `backuprun`. The old
controller's finalizer scales the workloads the run stopped back up, resumes
the Kustomizations it suspended and deletes the objects it created, and the
deletion completes once it has. Run the listing again until it prints nothing,
then move the image while the old controller's pod is still the one running.
A namespace schedule can start a new run in the meantime, so move the image
between two ticks of the schedules you know about.

v0.9.0 carries on a run that was still active at the upgrade as if it had
planned that run itself, and does not check what the older version wrote into
its status.

### Step 2: Check that no two Clusters share an archive

From v0.9.0 the webhook compares only bucket and prefix when it looks for a
shared archive, whatever endpointURL each ObjectStore names
([decisions.md](decisions.md#compare-bucket-and-prefix-and-never-the-endpoint-for-a-shared-archive)).
v0.8.x also compared the endpoint, so it may have admitted two Clusters that
write one archive through two spellings of one endpoint. The check runs on
every Cluster create, a disaster-recovery rebuild included, and it refuses the
second of such a pair while the first exists.

List every archive with the Clusters that write it:

```
jq -r --slurpfile stores <(kubectl get objectstores.barmancloud.cnpg.io -A -o json) '
  ($stores[0].items | map({key: "\(.metadata.namespace)/\(.metadata.name)", value: (.spec.configuration.destinationPath // "")}) | from_entries) as $dest
  | [.items[] as $c
     | first(($c.spec.plugins // [])[] | select(.name == "barman-cloud.cloudnative-pg.io" and .isWALArchiver == true and (.parameters.barmanObjectName // "") != "")) as $p
     | ($dest["\($c.metadata.namespace)/\($p.parameters.barmanObjectName)"] // "") as $d
     | select($d | startswith("s3://"))
     | ($d | ltrimstr("s3://") | split("/")) as $parts
     | (($p.parameters.serverName // "") | if . == "" then $c.metadata.name else . end) as $server
     | {archive: (($parts[0] | ascii_downcase) + "/" + ([$parts[1:][], $server] | map(select(. != "")) | join("/"))),
        cluster: "\($c.metadata.namespace)/\($c.metadata.name) (ObjectStore \($p.parameters.barmanObjectName), \($d))"}]
  | group_by(.archive)[]
  | "\(length)\t\(.[0].archive)\t\(map(.cluster) | join(", "))"
' <(kubectl get clusters.postgresql.cnpg.io -A -o json) | sort -rn
```

Each line gives the number of Clusters, the archive as bucket and prefix, and
each Cluster with its ObjectStore and that store's destinationPath. The query
follows the webhook's rules: the first barman-cloud plugin entry with
`isWALArchiver: true`, the ObjectStore in the Cluster's own namespace, the
server name defaulting to the Cluster's name, and the bucket compared without
letter case.

Expected result: every line starts with 1.

A line starting with 2 or more names Clusters that write one archive. If their
ObjectStores reach one S3 service, the two databases' WAL is already
interleaved: take a fresh base backup of each once they archive to separate
prefixes. If they reach different services, give one of the two its own
prefix in destinationPath before you upgrade. Either way, move the Cluster
whose archive matters less, since the one that moves starts a new archive
([restores.md](restores.md#refusing-a-shared-archive)).

### Step 3: Check the S3 credentials

The webhook now lists the whole server prefix `<prefix>/`, where v0.8.x listed
only `<prefix>/base/`. Each ObjectStore's credential needs `s3:ListBucket` on
`<prefix>/`. A bucket policy that allows the listing on `base/` and `wals/`
alone refuses it, and every create of that Cluster then fails with an HTTP 500
ending in `(HTTP 403 AccessDenied)`.
[architecture.md](architecture.md#where-a-databases-backups-are) lists the
permissions.

### Step 4: Apply the CRDs, the RBAC and the image together

Three rules are added, and one of them refuses work until it is applied:

- the ClusterRole gains `list` on `objectstores.barmancloud.cnpg.io`;
- it gains `get` on `customresourcedefinitions.apiextensions.k8s.io`, named
  `backupruns.backup.wlz.li`, `restoreruns.backup.wlz.li` and
  `volumerestores.backup.wlz.li`;
- it gains get, list, create, update and delete on `leases` in
  `coordination.k8s.io`, which the runs take before they start a mover.

One rule loses a verb: `create` on `volumerestores.backup.wlz.li` goes, because
v0.9.0 creates no VolumeRestore.

Apply deploy/rbac.yaml, or upgrade the chart, in the same change that moves the
image. A v0.9.0 image running under the v0.8.x ClusterRole refuses every
Cluster create with an HTTP 500 while any other Cluster archives, until the
`list` rule is there.

The CRDs go with the image as well. A run reads the installed CRD of its kind
before it changes anything and ends with reason CRDOutdated when the schema
lacks a field the controller writes, since the API server drops that field from
every write. Apply the release manifest server-side, or the CRD manifest:

```
kubectl apply --server-side -f https://github.com/walzen-group/backup-controller/releases/download/<version>/backup-controller-<version>.yaml
```

Helm upgrades no CRD on its own, so an install moved with `helm upgrade` alone
keeps the CRDs of its first install.

A cluster that ran v0.8.0 or v0.8.1 under the v0.7.2 BackupRun CRDs needs a look
before the upgrade: that schema has no `status.restartPending`, so a status
write there drops the field, and a run that stopped workloads could finish
while the app was still at zero replicas. List the workloads that are marked
for quiesce and stand at zero:

```
kubectl get deployments,statefulsets -A -o json | jq -r '.items[] | select((.metadata.annotations? // {})["backup.wlz.li/quiesce"] == "true" and ((.spec.replicas? // 1) == 0)) | "\(.metadata.namespace)/\(.kind)/\(.metadata.name)"'
```

and the Flux Kustomizations that are suspended:

```
kubectl get kustomizations.kustomize.toolkit.fluxcd.io -A -o json | jq -r '.items[] | select(.spec.suspend? == true) | "\(.metadata.namespace)/\(.metadata.name)"'
```

Expected result: only workloads and Kustomizations you stopped by hand. Scale
each one back up and resume each Kustomization. v0.9.0 gives back only what
its own runs record as stopped, so this listing is where an app that a failed
restart under the old CRD left at zero shows up.

### Step 5: Check how long an app may stay down

A run with `all: true` now keeps an app stopped for at most the namespace's
`backup.wlz.li/max-quiesce`, ten minutes without it, counted from the run's
`status.quiescedAt`. A namespace whose mover regularly needs longer should set
the annotation before the upgrade, or the run gives the app back and fails the
volume item whose clone VolSync had not cut by then, with the clone named in
the message. The annotation holds a Go duration, such as `20m`.

### Step 6: Check for Kustomizations that apply two namespaces

A run that stops workloads now refuses, with reason `Invalid` and before it
stops anything, when the Flux Kustomization of one of its workloads also
applies a Deployment or a StatefulSet in another namespace
([namespace-backups.md](namespace-backups.md#flux-kustomizations)). List such
Kustomizations:

```
kubectl get kustomizations.kustomize.toolkit.fluxcd.io -A -o json | jq -r '.items[] | ([.status.inventory.entries[]?.id | split("_") | select(length == 4 and .[2] == "apps" and (.[3] == "Deployment" or .[3] == "StatefulSet")) | .[0]] | unique) as $ns | select($ns | length > 1) | "\(.metadata.namespace)/\(.metadata.name)\t\($ns | join(", "))"'
```

Expected result: no output, or only Kustomizations whose workloads no run
stops. Give each namespace whose workloads a run stops its own Kustomization.

### Step 7: Check what VolSync keeps of a restore mover's log

A volume restore now succeeds only when the log of the mover that completed its
trigger names the snapshot the run's checks selected, so a VolSync that cuts
that log too short leaves every restore unconfirmed, and the run fails it.
VolSync keeps the last `MOVER_LOG_MAX_BYTES` bytes of the filtered log, 1024 by
default. Leave the default, or set it to at least a few kilobytes; a value of 0
leaves the log empty and every restore then fails with `the mover finished, but
its logs name no snapshot, so the run cannot confirm what claim <claim> holds`.

### Step 8: Find Clusters v0.8.x admitted over an old archive

v0.8.x admitted a Cluster as `initdb` over a prefix holding WAL but no
completed base backup, and over any prefix when it carried
`backup.wlz.li/bootstrap: initdb`. Such a Cluster runs and serves traffic, and
its archiving has failed since it started
([restores.md](restores.md#a-database-that-could-never-archive)). v0.9.0 acts
only on creates, so it does not repair a Cluster that already exists.

List the Clusters whose archiving fails:

```
kubectl get clusters.postgresql.cnpg.io -A -o json | jq -r '.items[] | .status.conditions[]? as $c | select($c.type == "ContinuousArchiving" and $c.status == "False" and $c.reason == "ContinuousArchivingFailing") | "\(.metadata.namespace)/\(.metadata.name)\t\($c.message)"'
```

Expected result: no output.

Each line names a Cluster and its condition's message. With CloudNativePG
1.30.0 and plugin-barman-cloud 0.15.0, a Cluster started over an old archive
shows this message:

```text
rpc error: code = Unknown desc = unexpected failure invoking barman-cloud-wal-archive: exit status 1
```

The message leaves out the cause, and ContinuousArchivingFailing has other
causes too, such as wrong credentials. The plugin-barman-cloud container in
the Cluster's primary pod logs the cause. Find the primary pod:

```
kubectl -n <namespace> get clusters.postgresql.cnpg.io <cluster> -o jsonpath='{.status.currentPrimary}'
```

Search that container's log:

```
kubectl -n <namespace> logs <primary pod> -c plugin-barman-cloud | grep 'Expected empty archive'
```

Expected result for a Cluster started over an old archive: a JSON line for
every attempt PostgreSQL made to archive a segment, whose msg field ends in:

```text
ERROR: WAL archive check failed for server <serverName>: Expected empty archive
```

Only this container logs that text. The condition and the postgres
container's log carry the rpc error above.

When the search prints nothing, the Cluster's archiving fails for another
reason. The same container's log names it:

| Line in the plugin-barman-cloud log | What failed |
| --- | --- |
| `ERROR: Barman cloud WAL archive check exception: <what the store answered>`, for example `Permission denied when accessing bucket '<bucket>'. Verify that the configured credentials have sufficient permissions: <answer>` | the check reached the store and the store refused it, because of the credentials the ObjectStore names or of a bucket those credentials may not create or use |
| `ERROR: Can't connect to cloud provider: <error>` | the check cannot reach the endpointURL the ObjectStore names |
| `ERROR: Barman cloud WAL archiver exception: <what the store answered>` | the check passed and the upload of a segment was refused, for example after the credentials changed or the bucket was deleted |
| `no permission to download the backup credentials, retrying` | the sidecar may not read the ObjectStore or the Secret it names |

A line naming something else, such as a missing ObjectStore or Secret, names
its own cause. The condition's `exit status` tells the same classes apart
with CloudNativePG 1.30.0, plugin-barman-cloud 0.15.0 and barman 3.20.0: 1
when the check refused a prefix that holds WAL, 2 when it could not reach
the endpoint, and 4 for the store's other answers and for a refused upload.

Step 9 repairs only a Cluster whose log shows the `Expected empty archive`
line. For any other failure, repair the cause and read the log again: a
Cluster that has never archived keeps the marker file in PGDATA, so once the
store answers, the check prints its verdict on the prefix, and a line with
`Expected empty archive` means the Cluster needs step 9 after all.

### Step 9: Give each such Cluster an empty archive

The database in such a Cluster holds writes no backup has captured, so
deleting the Cluster loses them. Repair the Cluster in place. It archives to
`<destinationPath>/<serverName>/`: destinationPath comes from the ObjectStore
its barmanObjectName names, and serverName defaults to the Cluster's name.
Pick one of two repairs:

| The old archive | Repair |
| --- | --- |
| should stay | set a new serverName in the Cluster's barman-cloud plugin parameters |
| is worth nothing | delete everything under `<destinationPath>/<serverName>/` |

To keep the old archive, add a serverName no archive uses yet, such as
`<cluster>-v2`, to the plugin entry in the manifest Flux applies:

```yaml
spec:
  plugins:
  - name: barman-cloud.cloudnative-pg.io
    isWALArchiver: true
    parameters:
      barmanObjectName: <store>
      serverName: <cluster>-v2
```

To discard the old archive, delete it. Keep the trailing slash, so that a
sibling prefix such as `<serverName>-old/` stays:

```
aws s3 rm --recursive --endpoint-url <endpointURL> <destinationPath>/<serverName>/
```

Check the condition:

```
kubectl -n <namespace> get clusters.postgresql.cnpg.io <cluster> -o jsonpath='{.status.conditions[?(@.type=="ContinuousArchiving")].reason}'
```

Expected result, within a minute of the change: `ContinuousArchivingSuccess`.
The instance keeps running through the repair.

PostgreSQL keeps every segment its archive_command failed on in pg_wal, marked
.ready, and retries the oldest. While the marker file .check-empty-wal-archive
exists in PGDATA, the plugin runs barman-cloud-check-wal-archive before each
attempt. Against an empty prefix the check passes, and the plugin archives the
waiting segments in order, starting with the first one the database wrote
after initdb. The instance deletes the marker once ContinuousArchiving is
True. On the e2e cluster, a Cluster recovered from the repaired archive held
every row written before the repair.

### Step 10: Take a base backup

The new archive holds WAL but no base backup, so a recovery has nothing to
start from yet. Create a BackupRun naming the database:

```yaml
apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: after-archive-repair
  namespace: <namespace>
spec:
  database: <cluster>
```

Check the run:

```
kubectl -n <namespace> get brun after-archive-repair
```

Expected result: the phase column shows Succeeded.

A run that ends Failed with `the Cluster <cluster> is not marked
backup.wlz.li/enabled: "true"` names a Cluster without that annotation. Add
the annotation and create the run again.

### When the serverName changes back

Set the new serverName in the Cluster's manifest in Git, so that every apply of
the manifest keeps it. Two behaviours depend on the serverName the Cluster
carries:

- The instance deleted the marker when archiving succeeded, so the plugin no
  longer checks the prefix. A Cluster whose serverName goes back to the old one
  archives its new segments into the old archive, and ContinuousArchiving
  stays True. barman overwrites the old segments that have the same names.
- When a Cluster is recreated, the webhook finds its base backups under the
  serverName the new Cluster declares, so a disaster-recovery rebuild from the
  manifest in Git looks in the archive that manifest names.

The serverName belongs in the Cluster. A validation rule in the
ObjectStore CRD refuses an ObjectStore that sets one in spec.configuration:

```text
spec.configuration.serverName: Forbidden: use the 'serverName' plugin parameter in the Cluster resource
```

hack/e2e/cnpg/empty-archive-repair.sh starts a Cluster over an old archive on
the e2e cluster, repairs it either way step 9 gives, runs step 10, and recovers
a second Cluster from the repaired archive to count its rows.
