# Upgrading

Each section lists what to check before moving a cluster to that release, and
what to do during and after the upgrade. The Releases table in the
[README](../README.md#releases) lists what each release changed.

## v0.9.0

### Step 1: Upgrade only while no run is active

Move the image only while no BackupRun or RestoreRun is active
([decisions.md](decisions.md#upgrade-only-while-no-run-is-active)). A backup
lasts minutes, and a restore that recovers a Cluster or writes a large volume
can last hours, so wait for the running ones to finish.

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
between two ticks of the schedules you know about. The Deployment's Recreate
strategy stops the old pod before the new one starts, so the two versions never
reconcile a run at the same moment.

v0.9.0 restores through a Job of its own and neither reads nor deletes
ReplicationDestinations. Check that nothing of the old restore path is in
flight, a destination or a claim waiting for the populator:

```
kubectl get replicationdestinations.volsync.backube -A
```

```
kubectl get pvc -A -o json | jq -r '.items[] | select(.spec.dataSourceRef.kind == "VolumeRestore" and .status.phase != "Bound") | "\(.metadata.namespace)/\(.metadata.name)"'
```

Expected result: `No resources found`, and no output. A destination left in
backup-system or in an app namespace keeps its mover and its claim to itself
under v0.9.0, and for a claim still waiting on a VolumeRestore the new
populator creates its restore Job into a prime claim that the old
destination's mover may still be writing. Wait for a pending
claim to bind, or delete it, and delete a destination only once its run or
claim is gone, before you upgrade.

v0.9.0 has no code for a run an older version started. It carries on a run that
was still active at the upgrade as if it had planned that run itself, and it
does not check what the older version wrote into that run's status. The older
version took no Leases and ran an `into` restore from a claim through the
populator, so such a run can leave an app at zero replicas, or have a claim
written by two movers. Delete a run you find active after the upgrade, and then
run the zero-replica listing in
[step 4](#step-4-apply-the-crds-the-rbac-and-the-image-together).

### Step 2: Check that no two Clusters share an archive

From v0.9.0 the webhook compares only bucket and prefix when it looks for a
shared archive, whatever endpointURL each ObjectStore names
([decisions.md](decisions.md#compare-only-bucket-and-prefix-for-a-shared-archive)).
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

Against v0.8.x the ClusterRole gains four rules and one verb on an existing
rule. The list holds for v0.8.0, v0.8.1 and v0.8.2, whose rules differ only in
the `delete` on replicationsources that v0.8.2 dropped. Until the new `list`
verb is applied, the webhook refuses every Cluster create while any other
Cluster archives:

- `list` on `objectstores.barmancloud.cnpg.io`, the added verb, which the
  webhook's shared-archive check uses to read every ObjectStore in one call;
- `get` on `customresourcedefinitions.apiextensions.k8s.io`, named
  `backupruns.backup.wlz.li` and `restoreruns.backup.wlz.li`, for the CRD
  check below;
- get, list, create, update and delete on `leases` in `coordination.k8s.io`,
  for the Lease a run acquires on a claim and its repository before it starts a
  mover or a restore Job, and the Lease `backup-controller-quiesce` it acquires
  before it stops a namespace's workloads;
- get, list, create, patch and delete on `jobs` in `batch`, for the restore
  Job of a RestoreRun and of the populator: created suspended, resumed and
  suspended with a patch, deleted with Foreground propagation, and listed by a
  backup that waits for a restore of its claim;
- get and update on `deployments/scale` and `statefulsets/scale` in `apps`,
  through which quiesce sets a workload's replica count.

Four rules lose verbs:

- `patch` on `deployments` and `statefulsets` goes; the rule keeps get and
  list, and the controller writes no field of a workload but the replica count
  through `scale`;
- the rule on `replicationdestinations.volsync.backube` goes entirely, since no
  restore creates a ReplicationDestination any more;
- `create` on `volumerestores.backup.wlz.li` goes, because a RestoreRun with
  `into:` no longer creates a VolumeRestore. The VolumeRestores a claim names
  in `dataSourceRef`, such as the one a Flux template writes for each backed-up
  claim, keep working unchanged: the populator fills a new claim from them, and
  every run reads a claim's repository from its VolumeRestore, as before;
- `delete` on `replicationsources.volsync.backube` goes (v0.8.2 already
  dropped it), because a failed mover no longer deletes the claim's source.

[packaging.md](packaging.md#rbac-the-controller-needs) lists every rule and its
caller.

The release also carries the ValidatingAdmissionPolicy and
ValidatingAdmissionPolicyBinding `backup-controller-restore-jobs`
(deploy/admissionpolicy.yaml), which narrow the new `jobs` rule to Jobs of the
restore Job's shape
([packaging.md](packaging.md#admission-policy-on-the-restore-jobs)). They need
Kubernetes 1.30 or later.

Apply deploy/rbac.yaml and deploy/admissionpolicy.yaml, or upgrade the chart,
in the same change that moves the image. Under the v0.8.x ClusterRole, a v0.9.0
webhook answers each of those Cluster creates with an HTTP 500, a v0.9.0 run
ends with reason CRDOutdated because it may not read its CRD, a quiesce fails
with Forbidden on `deployments/scale`, and every restore Job create fails with
Forbidden. The controller also needs `--restore-image`, which step 5 adds; the
release manifest leaves it out.

The CRDs go with the image as well. A run reads the installed CRD of its kind
before it changes anything and ends with reason CRDOutdated when the schema
lacks a field the controller writes, since the API server drops that field from
every write. Apply the release manifest server-side, or the CRD manifest:

```
kubectl apply --server-side -f https://github.com/walzen-group/backup-controller/releases/download/<version>/backup-controller-<version>.yaml
```

Helm upgrades no CRD on its own, so an install moved with `helm upgrade` alone
keeps the CRDs of its first install.

Two behaviours of v0.8.x could leave an app at zero replicas with its run
Succeeded:

- A namespace backup and a RestoreRun with `quiesce`, or two namespace backups,
  could stop the same workload. The second run recorded the zero replicas the
  first had stopped it at, and whichever run restarted last decided whether the
  app came back.
- On a cluster where v0.8.0 or v0.8.1 ran under the v0.7.2 BackupRun CRD,
  whose schema has no `status.restartPending`, a status write dropped that
  field, and a run whose restart failed could
  finish while the app was still at zero replicas.

v0.9.0 gives back only what its own runs record as stopped, so it does not
repair either case. Before and after the upgrade, list the workloads that are
marked for quiesce and stand at zero:

```
kubectl get deployments,statefulsets -A -o json | jq -r '.items[] | select((.metadata.annotations? // {})["backup.wlz.li/quiesce"] == "true" and ((.spec.replicas? // 1) == 0)) | "\(.metadata.namespace)/\(.kind)/\(.metadata.name)"'
```

and the Flux Kustomizations that are suspended:

```
kubectl get kustomizations.kustomize.toolkit.fluxcd.io -A -o json | jq -r '.items[] | select(.spec.suspend? == true) | "\(.metadata.namespace)/\(.metadata.name)"'
```

Expected result: only workloads and Kustomizations you stopped by hand. Scale
each other workload back to the replicas its app runs with, and resume each
other Kustomization. A workload that a RestoreRun's `quiesce` lists and that
carries no `backup.wlz.li/quiesce` annotation does not appear in the first
listing; compare those with the `status.quiesced` of the RestoreRuns that list
them.

### Step 5: Change the infra units

This step is for the walzen infrastructure repository (~/repos/infra), read at
its commit 6315b4e; paths below are relative to it. Before you edit, run
`git status` in ~/repos/infra. Someone else's uncommitted edits in the files
below stay as they are: add your lines next to them, and commit only the
files you changed. Item 9 replaces the version line whatever it holds, since
infra moves straight to v0.9.0. v0.9.0 restores with its
own restic Job, which must run the same restic image VolSync backs up with, so
the image is declared once, in the volsync unit, and both units read it. The
controller refuses to start without `--restore-image`, and the release
manifest leaves the flag out for the installer to append.

1. In environments/prod/cluster/volsync/inputs.yaml, add below `chart_version`:

   ```yaml
   # restic_image: <tag>@sha256:<digest>, the image VolSync runs its restic mover in
   # and backup-controller runs its restore Job in. One declaration for both, so
   # the restic that writes the backups is the one that restores them. Keep the
   # tag at the chart's appVersion; the digest decides, and a bump moves both.
   # renovate: datasource=docker
   restic_image: "quay.io/backube/volsync:0.16.0@sha256:0d03a6aad57569eba2c0eaa0848cf4a908d9744b372ff2224bb291a320f36d76"
   ```

   Add the same lines below `chart_version` in
   environments/test/cluster/volsync/inputs.yaml (line 5). The test
   environment is parked (environments/test/.disable; root.hcl:16-20 defines
   `parked`, and the exclude block at root.hcl:86-90 skips a parked
   environment), but its volsync unit uses the same module, and
   `restic_image` has no default, so without the input the unit would fail the
   moment the environment is unparked.

2. In modules/cluster/volsync/opentofu/variables.tf, add:

   ```hcl
   variable "restic_image" {
     description = "Image VolSync runs its restic mover in, pinned by digest. backup-controller restores with the same image. See the module README."
     type        = string

     validation {
       condition     = strcontains(var.restic_image, "@sha256:")
       error_message = "restic_image is pinned by digest: <repository>:<tag>@sha256:<digest>."
     }
   }
   ```

3. In modules/cluster/volsync/opentofu/main.tf, pass the image to the chart in
   `helm_release.volsync`, the way every other chart module passes values
   (modules/cluster/kueue/opentofu/main.tf:51), and update the comment above
   it (main.tf:1-4), which says "Chart defaults everywhere":

   ```hcl
     # The restic mover's image, declared once in the unit's inputs and shared
     # with backup-controller's restore Job. The chart replaces the whole
     # reference with it. See the README.
     values = [yamlencode({
       restic = { image = var.restic_image }
     })]
   ```

4. In modules/cluster/volsync/opentofu/outputs.tf, add:

   ```hcl
   output "restic_image" {
     description = "Image VolSync runs its restic mover in; backup-controller passes it to its restore Job."
     value       = var.restic_image
   }
   ```

5. In environments/prod/cluster/backup-controller/terragrunt.hcl, add the
   dependency, keep `../volsync` in the `dependencies` block, and replace the
   `inputs = yamldecode(...)` line (terragrunt.hcl:20):

   ```hcl
   # The restic image VolSync backs up with, which the controller's restore Job runs.
   # Shallow merge so a read-only command works before the volsync unit has applied
   # the output: that unit already has outputs, and terragrunt uses mock outputs
   # only for a dependency with none. A real apply uses the actual value, because
   # the dependency makes that unit apply first.
   dependency "volsync" {
     config_path = "../volsync"

     mock_outputs = {
       restic_image = "quay.io/backube/volsync:0.16.0@sha256:0d03a6aad57569eba2c0eaa0848cf4a908d9744b372ff2224bb291a320f36d76"
     }
     mock_outputs_merge_strategy_with_state  = "shallow"
     mock_outputs_allowed_terraform_commands = ["init", "validate", "plan", "show", "output", "state", "console", "destroy"]
   }

   inputs = merge(yamldecode(file("${get_terragrunt_dir()}/inputs.yaml")), {
     restore_image = dependency.volsync.outputs.restic_image
   })
   ```

   The volsync unit has outputs today (namespace, flux_substitution_sources,
   release), so without the shallow merge a plan run before the volsync change
   is applied reads the real outputs, finds no `restic_image`, and fails. The
   comment above `dependencies` (terragrunt.hcl:10-11) names "volsync's
   ReplicationDestination kind"; reword it to the restic image.

6. In environments/test/cluster/backup-controller/terragrunt.hcl, add the same
   `dependency "volsync"` block and replace the `inputs` line
   (terragrunt.hcl:21) the same way; its `dependencies` block already lists
   `../volsync` (terragrunt.hcl:13-15), and the comment above it
   (terragrunt.hcl:10-12) names the ReplicationDestination kind too; reword it
   the same way. In environments/test/cluster/backup-controller/inputs.yaml, set
   `backup_controller_version: "v0.9.0"` (inputs.yaml:5 pins `v0.5.6`). Both
   changes are for the day the test environment is unparked: the module now
   requires `restore_image` and appends `--restore-image` to every release's
   Deployment, and v0.5.6 exits at start on a flag it does not know, just as
   v0.9.0 exits without it.

7. In modules/cluster/backup-controller/opentofu/variables.tf, add:

   ```hcl
   variable "restore_image" {
     description = "Image the controller's restore Job runs restic in, passed as --restore-image; the controller refuses to start without it. The volsync unit's restic_image. See the README."
     type        = string

     validation {
       condition     = strcontains(var.restore_image, "@sha256:")
       error_message = "restore_image is the restic image pinned by digest, <repository>:<tag>@sha256:<digest>, never empty: the controller refuses to start without it."
     }
   }
   ```

   With no default, a unit that passes nothing fails the plan with "No value
   for required variable", and an empty string or a tag without a digest fails
   the validation, so the module never installs the controller without the
   flag.

8. In modules/cluster/backup-controller/opentofu/main.tf, replace the
   `workload` local (main.tf:10) so it appends the flag to the `controller`
   container's args of the Deployment document, for every release and with no
   condition, the way the `namespaces` local patches the Namespace document
   (main.tf:20-36). The document keeps its key, so `kubectl_manifest.workload`
   keeps its address: no new resource and no `moved` block.

   ```hcl
     # The restore Job's image, from the volsync unit, so restores run the restic
     # that wrote the backups: appended to the controller container's args, the
     # way the Namespace document below gets its label. The release leaves the
     # flag out, and the controller refuses to start without it. See the README.
     workload = {
       for id, doc in local.documents : id => (
         yamldecode(doc).kind == "Deployment" ? yamlencode(merge(yamldecode(doc), {
           spec = merge(yamldecode(doc).spec, {
             template = merge(yamldecode(doc).spec.template, {
               spec = merge(yamldecode(doc).spec.template.spec, {
                 containers = [
                   for c in yamldecode(doc).spec.template.spec.containers :
                   c.name == "controller" ? merge(c, { args = concat(c.args, ["--restore-image=${var.restore_image}"]) }) : c
                 ]
               })
             })
           })
         })) : doc
       ) if !contains(["CustomResourceDefinition", "Namespace"], yamldecode(doc).kind)
     }
   ```

9. In environments/prod/cluster/backup-controller/inputs.yaml, set
   `backup_controller_version: "v0.9.0"`.

10. In modules/cluster/backup-controller/opentofu/README.md, list under
    workload in the "What the unit installs" table the
    ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding the release
    now carries, and say that the Deployment gets `--restore-image` from the
    volsync unit. The intro and "Restores run as root" say that the
    controller's restore Job writes the claim, still as root in namespaces
    annotated privileged-movers. The volsync module's README documents
    `restic_image`: add rows to its Interface table for the `restic_image`
    input and the `restic_image` output, and add a `restore_image` input row
    to the Interface table of the backup-controller module README. In
    modules/cluster/volsync/opentofu/README.md, line 24 ("The chart defaults
    keep ...") needs a sentence saying that the unit sets the chart's
    restic.image to `restic_image`, and line 199 says backup-controller
    fills a claim "with a Direct restore into an empty volume"; reword it to
    the restore Job.
    flux/templates/backups/pvc/volumerestore.yaml:7-8 says the controller "has
    VolSync restore into it"; reword it to the restore Job as well.

Apply the volsync unit first, then backup-controller. CI's apply after the
merge does this in terragrunt's dependency order, and so does
`terragrunt run --all apply` from a shell.

#### Check the images and the admission policy

Run the checks from ~/repos/infra with the prod kubeconfig. infra's .envrc sets
`KUBECONFIG` only in a shell that loads it, and without it kubectl reads
whatever cluster the current context names:

```
export KUBECONFIG=.output/prod/kubeconfig
```

```
kubectl -n volsync-system get deploy volsync -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].args}' | tr ',' '\n' | grep restic-container-image
```

```
kubectl -n backup-system get deploy backup-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="controller")].args}' | tr ',' '\n' | grep restore-image
```

```
kubectl get validatingadmissionpolicy,validatingadmissionpolicybinding backup-controller-restore-jobs
```

```
kubectl get validatingadmissionpolicy backup-controller-restore-jobs -o jsonpath='{.spec.matchConditions[0].expression}{"\n"}'
```

Expected result: both images are the `restic_image` value (each printed
line is JSON-quoted), the second command prints exactly one
`--restore-image`, both policy objects exist, and the match condition names `system:serviceaccount:backup-system:backup-controller`, the
namespace and ServiceAccount the Deployment runs as. A policy object that is
missing, or a match condition that names another namespace or ServiceAccount,
leaves the controller's Job grant as wide as RBAC alone: every Job its
ServiceAccount creates is admitted. Apply the backup-controller unit again:
the release asset carries both objects, and the unit applies them through
`kubectl_manifest.workload`. Outside infra, apply the release's
deploy/admissionpolicy.yaml with the username changed to match an install
that renamed either.

A backup-controller pod in CrashLoopBackOff whose previous log says `refusing
to start: --restore-image is required` means the append in item 8 is missing:
the unit ran an older module, or someone applied the release asset's
Deployment by hand.

```
kubectl -n backup-system logs deploy/backup-controller -c controller --previous
```

### Step 6: Check how long an app may stay down

A run with `all: true` now keeps an app stopped for at most the namespace's
`backup.wlz.li/max-quiesce`, ten minutes without it, counted from the run's
`status.quiescedAt`. A namespace whose mover regularly needs longer should set
the annotation before the upgrade, or the run gives the app back and fails the
volume item whose clone VolSync had not cut by then, with the clone named in
the message. The annotation holds a Go duration, such as `20m`.

### Step 7: Check for Kustomizations that apply two namespaces

A run that stops workloads now refuses, with reason `Invalid` and before it
stops anything, when the Flux Kustomization of one of its workloads also
applies a Deployment or a StatefulSet in another namespace
([namespace-backups.md](namespace-backups.md#which-kustomization-a-run-suspends)). List such
Kustomizations:

```
kubectl get kustomizations.kustomize.toolkit.fluxcd.io -A -o json | jq -r '.items[] | ([.status.inventory.entries[]?.id | split("_") | select(length == 4 and .[2] == "apps" and (.[3] == "Deployment" or .[3] == "StatefulSet")) | .[0]] | unique) as $ns | select($ns | length > 1) | "\(.metadata.namespace)/\(.metadata.name)\t\($ns | join(", "))"'
```

Expected result: no output, or only Kustomizations whose workloads no run
stops. Give each namespace whose workloads a run stops its own Kustomization.

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

### Ready reasons that are new or mean more

A dashboard or an alert that reads a run's Ready reason sees three new reasons
and seven that cover more cases than before, and a VolumeRestore has one new
reason. api.md has the full table for a
[BackupRun](api.md#ready-reasons), for a
[RestoreRun](api.md#ready-reasons-of-a-restorerun) and for a
[VolumeRestore](api.md#status).

| Reason | From v0.9.0 |
| --- | --- |
| CRDOutdated | new: the run ended before it changed anything, because the installed CRD of its kind lacks a field the controller writes, or the controller may not read that CRD |
| RestartFailed | new: the run could not give its app back, and stays unfinished until it can; the app is still down |
| ReleaseFailed | new: the app is back, and the run stays unfinished until it can release its Leases, its Kueue Workload or a restore Job it stopped |
| RestoreJobRefused | new on a VolumeRestore: the API server refused to create or resume a claim's restore Job, such as the admission policy refusing its `moverSecurityContext` |
| SourceBusy | also a backup and a restore of the same claim or repository, a Lease another run holds on either, another run that has stopped this namespace's workloads, and a RestoreRun that has deleted a Cluster and waits for it to be created again |
| NoBackupInReach | also a repository that holds no snapshot with the layout a VolSync mover writes, and a RestoreRun whose every item was Skipped |
| ClaimInUse | also the restore Job pod of another run writing into the claim of an in-place restore |
| WaitingForShutdown | also a RestoreRun waiting for the pods of a restore Job it stopped to end |
| TimedOut | also an `into` restore whose restore Job had not finished by `spec.timeout` |
| VolSyncUnsupported | also a RestoreRun that waits, with nothing changed, while the API server does not serve ReplicationSource at v1alpha1 |
| Retrying | also on a BackupRun, for an item whose start failed with an error a retry may fix |

Items carry a new field, `status.items[].reason`, a one-word cause for the
item's phase such as RestoreJobFailed or MoverFailed; the
[item reasons](api.md#item-reasons) table lists them. A run records why it
ended in `status.ending`.
