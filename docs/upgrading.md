# Upgrading

Each section lists what to check before moving a cluster to that release, and
what to do during and after the upgrade. The Releases table in the
[README](../README.md#releases) lists what each release changed.

## v0.9.0

### Step 1: Check that no two Clusters share an archive

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

### Step 2: Check the S3 credentials

The webhook now lists the whole server prefix `<prefix>/`, where v0.8.x listed
only `<prefix>/base/`. Each ObjectStore's credential needs `s3:ListBucket` on
`<prefix>/`. A bucket policy that allows the listing on `base/` and `wals/`
alone refuses it, and every create of that Cluster then fails with an HTTP 500
ending in `(HTTP 403 AccessDenied)`.
[architecture.md](architecture.md#where-a-databases-backups-are) lists the
permissions.

### Step 3: Apply the RBAC with the image

The ClusterRole gains `list` on `objectstores.barmancloud.cnpg.io`. Apply
deploy/rbac.yaml, or upgrade the chart, in the same change that moves the
image. A v0.9.0 image running under the v0.8.x ClusterRole refuses every
Cluster create with an HTTP 500 while any other Cluster archives, until the
RBAC is applied.

### Step 4: Find Clusters v0.8.x admitted over an old archive

v0.8.x admitted a Cluster as `initdb` over a prefix holding WAL but no
completed base backup, and over any prefix when it carried
`backup.wlz.li/bootstrap: initdb`. Such a Cluster runs, and its archiving has
failed since it started
([restores.md](restores.md#a-database-that-could-never-archive)). v0.9.0 acts
only on creates, so it does not repair a Cluster that already exists.

List the Clusters whose archiving fails:

```
kubectl get clusters.postgresql.cnpg.io -A -o json | jq -r '.items[] | .status.conditions[]? as $c | select($c.type == "ContinuousArchiving" and $c.status == "False" and $c.reason == "ContinuousArchivingFailing") | "\(.metadata.namespace)/\(.metadata.name)\t\($c.message)"'
```

Expected result: no output.

ContinuousArchivingFailing has other causes too, such as wrong credentials.
For this one, the condition's message or the instance pod's log names
`Expected empty archive`.

The database in such a Cluster holds writes no backup has captured, so
deleting the Cluster loses them. Point its archive at an empty prefix instead:
set a `serverName` no archive uses yet to keep the old archive, or delete
everything under the old prefix if it is worth nothing. PostgreSQL keeps every
segment it could not archive in `pg_wal`, so the plugin archives them once its
check passes. When ContinuousArchiving turns True, take a base backup with a
BackupRun naming the database.
