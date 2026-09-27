# Upgrading

Each section lists what to check before you move a cluster to that release.
It also lists what to do during and after the upgrade. The Releases table in
the [README](../README.md#releases) lists what each release changed.

## v0.9.0

### Step 1: Upgrade only while no run is active

Move the image only while no BackupRun or RestoreRun is active
([decisions.md](decisions.md#upgrade-only-while-no-run-is-active)). A backup
lasts minutes. A restore that recovers a Cluster or writes a large volume can
last hours. Wait for the runs that are active to finish.

List the runs that have not finished:

```
kubectl get backupruns,restoreruns -A -o json | jq -r '.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed") | "\(.kind)\t\(.metadata.namespace)/\(.metadata.name)\t\(.status.phase // "not planned")"'
```

Expected result: no output.

For each line, wait until the run finishes, or delete it:

```
kubectl -n <namespace> delete backuprun <name>
```

To delete a RestoreRun, use `restorerun` in place of `backuprun`. The old
controller's finalizer does these steps:

1. It scales the workloads that the run stopped to their replicas again.
2. It resumes the Kustomizations that the run suspended.
3. It deletes the objects that the run created.

The deletion completes after these steps. Run the listing
again until it prints nothing. Then move the image while the pod of the old
controller is still the pod that runs. A namespace schedule can start a new run
in the meantime. Thus, move the image between two ticks of the schedules that
you know about. The Recreate strategy of the Deployment stops the old pod
before the new pod starts. Thus, the two versions never reconcile a run at the
same moment.

v0.9.0 restores through a Job of its own. It does not read or delete
ReplicationDestinations. Make sure that no part of the old restore path is
still active: no destination, and no claim that waits for the populator:

```
kubectl get replicationdestinations.volsync.backube -A
```

```
kubectl get pvc -A -o json | jq -r '.items[] | select(.spec.dataSourceRef.kind == "VolumeRestore" and .status.phase != "Bound") | "\(.metadata.namespace)/\(.metadata.name)"'
```

Expected result: `No resources found`, and no output. Under v0.9.0, a
destination in backup-system or in an app namespace keeps its mover and its
claim to itself. For a claim that still waits on a VolumeRestore, the new
populator creates its restore Job into a prime claim. The mover of the old
destination may still write to that prime claim. Before you upgrade, do these
steps:

1. Wait for a pending claim to bind, or delete it.
2. Delete a destination only after its run or claim is gone.

v0.9.0 has no code for a run that an older version started. It continues a
run that was still active at the upgrade as if it planned that run itself. It
does not check what the older version wrote into the status of that run. The
older version took no Leases. It ran an `into` restore from a claim through the
populator. Thus, such a run can leave an app at zero replicas, or two movers
can write the same claim. Delete a run that you find active after the upgrade.
Then run the zero-replica listing in
[step 4](#step-4-apply-the-crds-the-rbac-and-the-image-together).

### Step 2: Check that no two Clusters share an archive

From v0.9.0, the webhook compares only bucket and prefix when it looks for a
shared archive. The endpointURL that each ObjectStore names has no effect
([decisions.md](decisions.md#compare-only-bucket-and-prefix-for-a-shared-archive)).
v0.8.x also compared the endpoint. Thus, it may have admitted two Clusters that
write one archive through two spellings of one endpoint. The check runs on
every Cluster create. This includes a disaster-recovery rebuild. It refuses
the second Cluster of such a pair while the first exists.

List every archive with the Clusters that write it:

```
jq -r --slurpfile stores <(kubectl get objectstores.barmancloud.cnpg.io -A -o json) '
  ($stores[0].items | map({key: "\(.metadata.namespace)/\(.metadata.name)", value: (.spec.configuration.destinationPath // "")}) | from_entries) as $dest
  | [.items[] as $c
     | [($c.spec.plugins // [])[] | select(.name == "barman-cloud.cloudnative-pg.io")] as $entries
     | [$entries[] | select(.enabled != false)] as $enabled
     | select($enabled | length > 0)
     | ($entries[-1].parameters.barmanObjectName // "") as $store
     | select($store != "")
     | ($dest["\($c.metadata.namespace)/\($store)"] // "") as $d
     | select($d | startswith("s3://"))
     | ($d | ltrimstr("s3://") | split("/")) as $parts
     | (reduce ($enabled[] | (.parameters // {}) | select(has("serverName")) | .serverName) as $s ($c.metadata.name; $s)) as $server
     | {archive: (($parts[0] | ascii_downcase) + "/" + ([$parts[1:][], $server] | map(select(. != "")) | join("/"))),
        cluster: "\($c.metadata.namespace)/\($c.metadata.name) (ObjectStore \($store), \($d))"}]
  | group_by(.archive)[]
  | "\(length)\t\(.[0].archive)\t\(map(.cluster) | join(", "))"
' <(kubectl get clusters.postgresql.cnpg.io -A -o json) | sort -rn
```

Each line gives these values:

- The number of Clusters.
- The archive as bucket and prefix.
- Each Cluster with its ObjectStore and the destinationPath of that store.

The query follows the rules of the webhook:

- It skips a Cluster with no enabled barman-cloud plugin entry.
- It uses the ObjectStore that `barmanObjectName` of the last barman-cloud
  entry names, in the namespace of the Cluster.
- The server name is the name of the Cluster by default. A `serverName` in an
  enabled entry replaces it, also when it is empty, and the last one wins.
- It compares the bucket without letter case.

Expected result: every line starts with 1.

A line that starts with 2 or more names Clusters that write one archive. If
their ObjectStores reach one S3 service, the WAL of the two databases is
already interleaved. In that case, take a new base backup of each database
after they archive to separate prefixes. If they reach different services,
give one of the two its own prefix in destinationPath before you upgrade. In
both cases, move the Cluster whose archive is less important. The Cluster that
moves starts a new archive
([restores.md](restores.md#refusing-a-shared-archive)).

### Step 3: Check the S3 credentials

The webhook now lists the full server prefix `<prefix>/`. v0.8.x listed only
`<prefix>/base/`. The credential of each ObjectStore needs `s3:ListBucket` on
`<prefix>/`. A bucket policy that allows the listing only on `base/` and
`wals/` refuses it. Then every create of that Cluster fails with an HTTP 500
that ends in `(HTTP 403 AccessDenied)`.
[architecture.md](architecture.md#where-a-databases-backups-are) lists the
permissions.

### Step 4: Apply the CRDs, the RBAC and the image together

Compared to v0.8.x, the ClusterRole gets four new rules and one new verb on a
rule that exists. The list applies to v0.8.0, v0.8.1 and v0.8.2. Their
rules differ only in the `delete` on replicationsources, which v0.8.2 dropped.
Until you apply the new `list` verb, the webhook refuses every Cluster create
while any other Cluster archives. The new rules and the new verb are:

- `list` on `objectstores.barmancloud.cnpg.io`. This is the new verb. The
  shared-archive check of the webhook uses it to read every ObjectStore in one
  call.
- `get` on `customresourcedefinitions.apiextensions.k8s.io`, with the names
  `backupruns.backup.wlz.li` and `restoreruns.backup.wlz.li`. This rule is for
  the CRD check below.
- get, list, create, update and delete on `leases` in `coordination.k8s.io`.
  A run acquires a Lease on a claim and its repository before it starts a
  mover or a restore Job. A run also acquires the Lease
  `backup-controller-quiesce` before it stops the workloads of a namespace.
- get, list, create, patch and delete on `jobs` in `batch`, for the restore
  Job of a RestoreRun and of the populator. The controller creates the Job
  suspended, and it resumes and suspends the Job with a patch. It deletes the
  Job with Foreground propagation. A backup that waits for a restore of its
  claim lists the Jobs.
- get and update on `deployments/scale` and `statefulsets/scale` in `apps`.
  Quiesce sets the replica count of a workload through these subresources.

Four rules lose verbs:

- The rule on `deployments` and `statefulsets` loses `patch`. The rule keeps
  get and list. The controller writes only the replica count of a workload, through
  `scale`.
- The ClusterRole loses the full rule on
  `replicationdestinations.volsync.backube`, because no restore creates a ReplicationDestination now.
- The rule on `volumerestores.backup.wlz.li` loses `create`, because a
  RestoreRun with `into:` does not create a VolumeRestore now. The VolumeRestores that a
  claim names in `dataSourceRef` continue to work with no change. An example is
  the VolumeRestore that a Flux template writes for each backed-up claim. The
  populator fills a new claim from them. Every run reads the repository of a
  claim from its VolumeRestore, as before.
- The rule on `replicationsources.volsync.backube` loses `delete` (v0.8.2
  already dropped it), because a failed mover does not delete the source of the claim
  now.

[packaging.md](packaging.md#rbac-the-controller-needs) lists every rule and its
caller.

The release also contains the ValidatingAdmissionPolicy and
ValidatingAdmissionPolicyBinding `backup-controller-restore-jobs`
(deploy/admissionpolicy.yaml). They limit the new `jobs` rule to Jobs with the
shape of the restore Job
([packaging.md](packaging.md#admission-policy-on-the-restore-jobs)). They need
Kubernetes 1.30 or later.

Apply deploy/rbac.yaml and deploy/admissionpolicy.yaml in the same change that
moves the image. As an alternative, upgrade the chart in that change. Under the
v0.8.x ClusterRole, these failures occur:

- A v0.9.0 webhook answers each of those Cluster creates with an HTTP 500.
- A v0.9.0 run ends with reason CRDOutdated, because it may not read its CRD.
- A quiesce fails with Forbidden on `deployments/scale`.
- Every restore Job create fails with Forbidden.

The controller also needs `--restore-image`, which step 5 adds. The release
manifest does not contain it.

Apply the CRDs together with the image too. Before a run changes anything, it
reads the installed CRD of its kind. The run ends with reason CRDOutdated when
the schema does not have a field that the controller writes, because the API
server drops that field from every write. Apply the release manifest
server-side, or the CRD manifest:

```
kubectl apply --server-side -f https://github.com/walzen-group/backup-controller/releases/download/<version>/backup-controller-<version>.yaml
```

Helm does not upgrade a CRD on its own. Thus, if you move an install only with
`helm upgrade`, it keeps the CRDs of its first install.

Two behaviours of v0.8.x could leave an app at zero replicas while its run was
Succeeded:

- A namespace backup and a RestoreRun with `quiesce` could stop the same
  workload. Two namespace backups could also do this. The second run recorded
  zero replicas, because the first run stopped the workload before. The run
  that restarted last decided if the app came back.
- On some clusters, v0.8.0 or v0.8.1 ran under the v0.7.2 BackupRun CRD. The
  schema of that CRD has no `status.restartPending`, so a status write dropped
  that field. Then a run whose restart failed could finish while the app was
  still at zero replicas.

v0.9.0 gives back only the workloads that its own runs record as stopped. Thus,
it does not repair these two cases. Before and after the upgrade, list the
workloads that have the quiesce mark and are at zero replicas:

```
kubectl get deployments,statefulsets -A -o json | jq -r '.items[] | select((.metadata.annotations? // {})["backup.wlz.li/quiesce"] == "true" and ((.spec.replicas? // 1) == 0)) | "\(.metadata.namespace)/\(.kind)/\(.metadata.name)"'
```

Also list the Flux Kustomizations that have `suspend` set:

```
kubectl get kustomizations.kustomize.toolkit.fluxcd.io -A -o json | jq -r '.items[] | select(.spec.suspend? == true) | "\(.metadata.namespace)/\(.metadata.name)"'
```

Expected result: only workloads and Kustomizations that you stopped by hand.
Scale each other workload to the replicas that its app runs with. Resume each
other Kustomization. The first listing does not show a workload that the
`quiesce` of a RestoreRun lists and that has no `backup.wlz.li/quiesce`
annotation. Compare those workloads with the `status.quiesced` of the
RestoreRuns that list them.

### Step 5: Change the infra units

This step is for the walzen infrastructure repository (~/repos/infra), as read
at its commit 6315b4e. The paths below are relative to it. Before you edit, run
`git status` in ~/repos/infra. Do not change uncommitted edits of other persons
in the files below. Add your lines next to them, and commit only the files
that you changed. Item 9 replaces the version line, whatever value it has,
because infra moves directly to v0.9.0.

v0.9.0 restores with its own restic Job. This Job must run the same restic
image that VolSync backs up with. Thus, the volsync unit declares the image
once, and both units read it. The controller refuses to start without
`--restore-image`. The release manifest does not contain the flag, so the
installer must append it.

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
   environment has the state parked (environments/test/.disable). root.hcl:16-20
   defines `parked`, and the exclude block at root.hcl:86-90 skips a parked
   environment. But the volsync unit of the test environment uses the same
   module, and `restic_image` has no default. Thus, without the input, the unit
   would fail at the moment the parked state of the environment ends.

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

3. In modules/cluster/volsync/opentofu/main.tf, give the image to the chart in
   `helm_release.volsync`. Do this the same way that every other chart module
   gives values (modules/cluster/kueue/opentofu/main.tf:51). Also update the
   comment above it (main.tf:1-4), which says "Chart defaults everywhere":

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

5. In environments/prod/cluster/backup-controller/terragrunt.hcl, do these
   steps:

   - Add the dependency.
   - Keep `../volsync` in the `dependencies` block.
   - Replace the `inputs = yamldecode(...)` line (terragrunt.hcl:20).

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

   The volsync unit has outputs now (namespace, flux_substitution_sources,
   release). Without the shallow merge, a plan that runs before you apply the
   volsync change reads the real outputs. It finds no `restic_image` and fails.
   The comment above `dependencies` (terragrunt.hcl:10-11) names "volsync's
   ReplicationDestination kind". Change its words to name the restic image.

6. In environments/test/cluster/backup-controller/terragrunt.hcl, add the same
   `dependency "volsync"` block. Replace the `inputs` line (terragrunt.hcl:21)
   the same way. Its `dependencies` block already lists `../volsync`
   (terragrunt.hcl:13-15). The comment above it (terragrunt.hcl:10-12) also
   names the ReplicationDestination kind. Change its words the same way.

   In environments/test/cluster/backup-controller/inputs.yaml, set
   `backup_controller_version: "v0.9.0"` (inputs.yaml:5 pins `v0.5.6`). Both
   changes are for the day when the parked state of the test environment ends. The
   module now requires `restore_image`, and it appends `--restore-image` to the
   Deployment of every release. v0.5.6 exits at start on a flag that it does
   not know. v0.9.0 exits in the same way without the flag.

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

   The variable has no default. Thus, a unit that gives no value fails the
   plan with "No value for required variable". An empty string, or a tag
   without a digest, fails the validation. Thus, the module never installs the
   controller without the flag.

8. In modules/cluster/backup-controller/opentofu/main.tf, replace the
   `workload` local (main.tf:10). The new local appends the flag to the args of
   the `controller` container in the Deployment document. It does this for
   every release and with no condition. It works the same way as the
   `namespaces` local, which patches the Namespace document (main.tf:20-36).
   The document keeps its key, so `kubectl_manifest.workload` keeps its
   address. There is no new resource and no `moved` block.

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

10. Update the READMEs:

    - In modules/cluster/backup-controller/opentofu/README.md, in the "What
      the unit installs" table, list under workload the
      ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding that the
      release now contains. Also write that the Deployment gets
      `--restore-image` from the volsync unit.
    - In the same README, the intro and "Restores run as root" must say that
      the restore Job of the controller writes the claim. It still writes as
      root in namespaces with the annotation privileged-movers.
    - The README of the volsync module documents `restic_image`. Add rows to
      its Interface table for the `restic_image` input and the `restic_image`
      output.
    - Add a `restore_image` input row to the Interface table of the
      backup-controller module README.
    - In modules/cluster/volsync/opentofu/README.md, line 24 ("The chart
      defaults keep ...") needs a sentence which says that the unit sets the
      restic.image of the chart to `restic_image`.
    - In the same README, line 199 says that backup-controller fills a claim
      "with a Direct restore into an empty volume". Change its words to name
      the restore Job.
    - flux/templates/backups/pvc/volumerestore.yaml:7-8 says that the
      controller "has VolSync restore into it". Change its words to name the
      restore Job too.

Apply the volsync unit first, then backup-controller. The CI apply after the
merge does this in the dependency order of terragrunt.
`terragrunt run --all apply` from a shell does the same.

#### Check the images and the admission policy

Run the checks from ~/repos/infra with the prod kubeconfig. The .envrc of infra
sets `KUBECONFIG` only in a shell that loads it. Without it, kubectl reads the
cluster that the current context names, whichever cluster that is:

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

Expected result:

- Both images are the `restic_image` value. Each printed line has JSON quotes.
- The second command prints exactly one `--restore-image`.
- Both policy objects exist.
- The match condition names
  `system:serviceaccount:backup-system:backup-controller`. This is the
  namespace and ServiceAccount that the Deployment runs as.

A policy object can be missing, or a match condition can name another
namespace or ServiceAccount. Then the Job permission of the controller is as
wide as RBAC alone. The API server admits every Job that its ServiceAccount
creates. In that case, apply the backup-controller unit again. The release
asset contains both objects, and the unit applies them through
`kubectl_manifest.workload`. Outside infra, apply the
deploy/admissionpolicy.yaml of the release. If an install renamed the
namespace or the ServiceAccount, change the username to match.

A backup-controller pod can be in CrashLoopBackOff with the previous log
`refusing to start: --restore-image is required`. This means that the append
in item 8 is missing. The unit ran an older module, or a person applied the
Deployment of the release asset by hand.

```
kubectl -n backup-system logs deploy/backup-controller -c controller --previous
```

### Step 6: Check how long an app may stay down

A run with `all: true` now keeps an app stopped for a maximum time. This time
is the `backup.wlz.li/max-quiesce` of the namespace, or ten minutes without
the annotation. The time starts at the `status.quiescedAt` of the run. If the
mover of a namespace usually needs more time, set the annotation before the
upgrade. If not, the run gives the app back when the time ends. It then fails
each volume item whose clone VolSync did not cut before that time. The message
names the clone. The annotation holds a Go duration, such as `20m`.

### Step 7: Check for Kustomizations that apply two namespaces

A run that stops workloads now refuses to continue when the Flux Kustomization
of one of its workloads also applies a Deployment or a StatefulSet in another
namespace. The run refuses with reason `Invalid`, before it stops anything
([namespace-backups.md](namespace-backups.md#which-kustomization-a-run-suspends)).
List such Kustomizations:

```
kubectl get kustomizations.kustomize.toolkit.fluxcd.io -A -o json | jq -r '.items[] | ([.status.inventory.entries[]?.id | split("_") | select(length == 4 and .[2] == "apps" and (.[3] == "Deployment" or .[3] == "StatefulSet")) | .[0]] | unique) as $ns | select($ns | length > 1) | "\(.metadata.namespace)/\(.metadata.name)\t\($ns | join(", "))"'
```

Expected result: no output, or only Kustomizations whose workloads no run
stops. If a run stops the workloads of a namespace, give that namespace its own
Kustomization.

### Step 8: Find Clusters v0.8.x admitted over an old archive

v0.8.x admitted a Cluster as `initdb` over a prefix that holds WAL but no
completed base backup. It also admitted such a Cluster over any prefix when
the Cluster had `backup.wlz.li/bootstrap: initdb`. Such a Cluster runs and
serves traffic, but its archiving has failed since it started
([restores.md](restores.md#a-database-that-could-never-archive)). v0.9.0 acts
only on creates. Thus, it does not repair a Cluster that already exists.

List the Clusters whose archiving fails:

```
kubectl get clusters.postgresql.cnpg.io -A -o json | jq -r '.items[] | .status.conditions[]? as $c | select($c.type == "ContinuousArchiving" and $c.status == "False" and $c.reason == "ContinuousArchivingFailing") | "\(.metadata.namespace)/\(.metadata.name)\t\($c.message)"'
```

Expected result: no output.

Each line names a Cluster and the message of its condition. With CloudNativePG
1.30.0 and plugin-barman-cloud 0.15.0, a Cluster that started over an old
archive shows this message:

```text
rpc error: code = Unknown desc = unexpected failure invoking barman-cloud-wal-archive: exit status 1
```

The message does not give the cause. ContinuousArchivingFailing also has
other causes, such as wrong credentials. The plugin-barman-cloud container in
the primary pod of the Cluster logs the cause. Find the primary pod:

```
kubectl -n <namespace> get clusters.postgresql.cnpg.io <cluster> -o jsonpath='{.status.currentPrimary}'
```

Search that container's log:

```
kubectl -n <namespace> logs <primary pod> -c plugin-barman-cloud | grep 'Expected empty archive'
```

Expected result for a Cluster that started over an old archive: a JSON line
for each attempt of PostgreSQL to archive a segment. The msg field of each
line ends in:

```text
ERROR: WAL archive check failed for server <serverName>: Expected empty archive
```

Only this container logs that text. The condition and the log of the postgres
container contain the rpc error above.

When the search prints nothing, the archiving of the Cluster fails for another
reason. The log of the same container names it:

| Line in the plugin-barman-cloud log | What failed |
| --- | --- |
| `ERROR: Barman cloud WAL archive check exception: <what the store answered>`, for example `Permission denied when accessing bucket '<bucket>'. Verify that the configured credentials have sufficient permissions: <answer>` | the check reached the store and the store refused it. The cause is the credentials that the ObjectStore names, or a bucket that those credentials may not create or use |
| `ERROR: Can't connect to cloud provider: <error>` | the check cannot reach the endpointURL that the ObjectStore names |
| `ERROR: Barman cloud WAL archiver exception: <what the store answered>` | the check passed and the store refused the upload of a segment. For example, this occurs after the credentials changed or after someone deleted the bucket |
| `no permission to download the backup credentials, retrying` | the sidecar may not read the ObjectStore or the Secret that it names |

A line that names something else, such as a missing ObjectStore or Secret,
names its own cause. The `exit status` of the condition also shows the class of
failure. With CloudNativePG 1.30.0, plugin-barman-cloud 0.15.0 and barman
3.20.0, the values are:

- 1 when the check refused a prefix that holds WAL.
- 2 when the check could not reach the endpoint.
- 4 for the other answers of the store and for a refused upload.

Step 9 repairs only a Cluster whose log shows the `Expected empty archive`
line. For all other failures, repair the cause and read the log again. A
Cluster that never archived keeps the marker file in PGDATA. Thus, when the
store answers, the check prints its result for the prefix. A line with
`Expected empty archive` then means that the Cluster needs step 9 too.

### Step 9: Give each such Cluster an empty archive

The database in such a Cluster holds writes that no backup contains. If you
delete the Cluster, these writes are lost. Repair the Cluster in place. It
archives to `<destinationPath>/<serverName>/`. The destinationPath comes from
the ObjectStore that its barmanObjectName names. The serverName is the name of
the Cluster by default. Select one of two repairs:

| The old archive | Repair |
| --- | --- |
| should stay | set a new serverName in the Cluster's barman-cloud plugin parameters |
| is worth nothing | delete everything under `<destinationPath>/<serverName>/` |

To keep the old archive, add a serverName that no archive uses yet, such as
`<cluster>-v2`. Add it to the plugin entry in the manifest that Flux applies:

```yaml
spec:
  plugins:
  - name: barman-cloud.cloudnative-pg.io
    isWALArchiver: true
    parameters:
      barmanObjectName: <store>
      serverName: <cluster>-v2
```

To discard the old archive, delete it. Keep the slash at the end, so that a
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

PostgreSQL keeps in pg_wal each segment on which its archive_command failed.
It marks these segments .ready and tries the oldest again. While the marker
file .check-empty-wal-archive exists in PGDATA, the plugin runs
barman-cloud-check-wal-archive before each attempt. Against an empty prefix,
the check passes. The plugin then archives the segments that wait, in order.
It starts with the first segment that the database wrote after initdb. The
instance deletes the marker when ContinuousArchiving is True. On the e2e
cluster, a Cluster recovered from the repaired archive held every row written
before the repair.

### Step 10: Take a base backup

The new archive holds WAL but no base backup. Thus, a recovery has no start
point yet. Create a BackupRun that names the database:

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

A run can end Failed with `the Cluster <cluster> is not marked
backup.wlz.li/enabled: "true"`. This message names a Cluster without that
annotation. Add the annotation and create the run again.

### When the serverName changes back

Set the new serverName in the manifest of the Cluster in Git. Then every apply
of the manifest keeps it. Two behaviours depend on the serverName of the
Cluster:

- The instance deleted the marker when archiving succeeded. Thus, the plugin
  does not check the prefix now. A Cluster whose serverName goes back to the
  old one archives its new segments into the old archive.
  ContinuousArchiving stays True. barman overwrites the old segments that have
  the same names.
- When you create a Cluster again, the webhook finds its base backups under
  the serverName that the new Cluster declares. Thus, a disaster-recovery
  rebuild from the manifest in Git looks in the archive that the manifest
  names.

The serverName must be in the Cluster. A validation rule in the ObjectStore
CRD refuses an ObjectStore that sets a serverName in spec.configuration:

```text
spec.configuration.serverName: Forbidden: use the 'serverName' plugin parameter in the Cluster resource
```

hack/e2e/cnpg/empty-archive-repair.sh does these steps on the e2e cluster:

1. It starts a Cluster over an old archive.
2. It repairs the Cluster in each of the two ways that step 9 gives.
3. It runs step 10.
4. It recovers a second Cluster from the repaired archive and counts its rows.

### Ready reasons that are new or mean more

A dashboard or an alert that reads the Ready reason of a run sees three new
reasons. It also sees seven reasons that cover more cases than before. A
VolumeRestore has one new reason. api.md has the full table for a
[BackupRun](api.md#ready-reasons), for a
[RestoreRun](api.md#ready-reasons-of-a-restorerun) and for a
[VolumeRestore](api.md#status).

| Reason | From v0.9.0 |
| --- | --- |
| CRDOutdated | new: the run ended before it changed anything. The installed CRD of its kind does not have a field that the controller writes, or the controller may not read that CRD |
| RestartFailed | new: the run could not give its app back. It stays unfinished until it can. The app is still down |
| ReleaseFailed | new: the app is back. The run stays unfinished until it can release its Leases, its Kueue Workload or a restore Job that it stopped |
| RestoreJobRefused | new on a VolumeRestore: the API server refused to create or resume the restore Job of a claim. An example is the admission policy that refuses its `moverSecurityContext` |
| SourceBusy | also these cases: a backup and a restore of the same claim or repository. A Lease that another run holds on the claim or the repository. Another run that stopped the workloads of this namespace. A RestoreRun that deleted a Cluster and waits until the Cluster exists again |
| NoBackupInReach | also a repository that holds no snapshot with the layout that a VolSync mover writes. Also a RestoreRun in which each item was Skipped |
| ClaimInUse | also the restore Job pod of another run that writes into the claim of an in-place restore |
| WaitingForShutdown | also a RestoreRun that waits for the pods of a restore Job that it stopped to end |
| TimedOut | also an `into` restore whose restore Job did not finish by `spec.timeout` |
| VolSyncUnsupported | also a RestoreRun that waits, with nothing changed, while the API server does not serve ReplicationSource at v1alpha1 |
| Retrying | also on a BackupRun, for an item whose start failed with an error that a retry may fix |

Items have a new field, `status.items[].reason`. It is a one-word cause for
the phase of the item, such as RestoreJobFailed or MoverFailed. The
[item reasons](api.md#item-reasons) table lists them. A run records why it
ended in `status.ending`.
