# Upgrading

Each section lists what to check before you move a cluster to that release.
It also lists what to do during and after the upgrade. The Releases table in
the [README](../README.md#releases) lists what each release changed.

Every change to the cluster is an edit of the walzen infrastructure repository
(~/repos/infra) and an apply of its units. The paths below are relative to
that repository. The guide gives no other install path. The `kubectl`
commands only read the cluster.

## Pause the controller

From v0.9.0, the controller has a `--pause` flag. While the controller runs
with the flag, each run that started work goes on to its end. New BackupRuns and RestoreRuns
wait with Ready reason `Paused` and touch nothing. The scheduler creates no
BackupRun, and the populator starts no restore for a new VolumeRestore claim.
Use the flag to upgrade the controller or a dependency while no run is active.

A v0.7.x controller has no `--pause` flag. Thus, the upgrade from v0.7.x to
v0.10.0 waits for the active runs to finish
([step 1](#step-1-check-the-preconditions)). Use this section for each upgrade
after v0.10.0.

### Add the pause input to the unit

This is a new input. The backup-controller unit at infra commit 6315b4e does
not have it. Make these edits one time, in the same change as the v0.10.0
upgrade ([step 2](#step-2-edit-the-infra-units)), with `pause: false`.

1. New: in modules/cluster/backup-controller/opentofu/variables.tf, add:

   ```hcl
   variable "pause" {
     description = "Start the controller with --pause: new runs and restores wait, and runs in progress finish. Set it before an upgrade of the controller or of a dependency. See the README."
     type        = bool
     default     = false
   }
   ```

2. New: in modules/cluster/backup-controller/opentofu/main.tf, in the
   `workload` local that step 2 item 8 writes, change the `args` line of the
   `controller` container. The module appends the flag the same way as
   `--restore-image`:

   ```hcl
   c.name == "controller" ? merge(c, { args = concat(c.args, ["--restore-image=${var.restore_image}"], var.pause ? ["--pause"] : []) }) : c
   ```

3. New: in environments/prod/cluster/backup-controller/inputs.yaml, add below
   `queue_name`:

   ```yaml
   # pause: true starts the controller with --pause. New runs and restores wait,
   # and runs in progress finish. Set it before an upgrade, and false after it.
   pause: false
   ```

4. New: add a `pause` input row to the Interface table in
   modules/cluster/backup-controller/opentofu/README.md.

### Pause, upgrade and resume

1. In environments/prod/cluster/backup-controller/inputs.yaml, set
   `pause: true`. Apply the unit ([step 3](#step-3-apply-the-units)). The
   change restarts the pod.
2. Make sure that the pod logs `paused: new runs wait, runs in progress finish`:

   ```
   kubectl -n backup-system logs deploy/backup-controller -c controller | grep 'paused:'
   ```

3. List the runs that are active. A run that waits with reason `Paused` has
   started nothing, so the listing leaves it out:

   ```
   kubectl get backupruns,restoreruns -A -o json | jq -r '.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed") | select(([.status.conditions[]? | select(.type == "Ready") | .reason] | first) != "Paused") | "\(.kind)\t\(.metadata.namespace)/\(.metadata.name)\t\(.status.phase // "not planned")"'
   ```

   Expected result: no output. Run the listing again until it prints nothing.
4. Upgrade the controller or the dependency through its unit.
5. Set `pause: false` in the same inputs.yaml, and apply the unit. The runs
   that waited then start. A namespace schedule creates one run for the newest
   tick that it missed.

## v0.10.0

This section moves a cluster from v0.7.x directly to v0.10.0. It covers
v0.7.0, v0.7.1 and v0.7.2, which have the same RBAC, Deployment and webhook
configuration. Prod runs v0.7.0: infra commit 46dd6a3 applied it. Do not
install v0.8.x or v0.9.x on the way. v0.10.0 replaces both.

Infra commit 6315b4e sets `backup_controller_version` to `v0.8.0`, but prod did
not apply it. Item 9 of step 2 replaces the version line, whatever value it
has.

### Step 1: Check the preconditions

The admission policy of the release needs Kubernetes 1.30 or later. Prod runs
the `kubernetes_version` of environments/prod/infra/talos/cluster.yaml:

```
kubectl version
```

Expected result: the server version is v1.30 or later.

Move the image only while no BackupRun or RestoreRun is active
([decisions.md](decisions.md#upgrade-only-while-no-run-is-active)). A backup
lasts minutes. A restore that recovers a Cluster or writes a large volume can
last hours. List the runs that have not finished:

```
kubectl get backupruns,restoreruns -A -o json | jq -r '.items[] | select(.status.phase != "Succeeded" and .status.phase != "Failed") | "\(.kind)\t\(.metadata.namespace)/\(.metadata.name)\t\(.status.phase // "not planned")"'
```

Expected result: no output. If the listing prints a line, wait for that run to
finish, and run the listing again.

v0.10.0 restores through a Job of its own. It does not read or delete the
ReplicationDestinations of v0.7.x. The v0.7.x populator keeps its destinations
in backup-system. Make sure that no v0.7.x restore of a claim is still active:

```
kubectl -n backup-system get replicationdestinations.volsync.backube
```

```
kubectl get pvc -A -o json | jq -r '.items[] | select(.spec.dataSourceRef.kind == "VolumeRestore" and .status.phase != "Bound") | "\(.metadata.namespace)/\(.metadata.name)"'
```

Expected result: `No resources found`, and no output. If a claim waits on a
VolumeRestore, wait until it binds. Then run both commands again.

A namespace schedule can start a new run between this check and the apply.
Thus, merge the change of step 2 between two ticks of the schedules.

### Step 2: Edit the infra units

This step reads ~/repos/infra at commit 6315b4e. Before you edit, run
`git status` in ~/repos/infra. Do not change uncommitted edits of other persons
in the files below. Commit only the files that you changed.

v0.10.0 restores with its own restic Job. This Job must run the same restic
image that VolSync backs up with. Thus, the volsync unit declares the image
one time, and both units read it. The controller refuses to start without
`--restore-image`. The release manifest does not contain the flag, so the
backup-controller module appends it.

1. In environments/prod/cluster/volsync/inputs.yaml, add below `chart_version`
   (line 4):

   ```yaml
   # restic_image: <tag>@sha256:<digest>, the image VolSync runs its restic mover in
   # and backup-controller runs its restore Job in. One declaration for both, so
   # the restic that writes the backups is the one that restores them. Keep the
   # tag at the chart's appVersion; the digest decides, and a bump moves both.
   # renovate: datasource=docker
   restic_image: "quay.io/backube/volsync:0.16.0@sha256:0d03a6aad57569eba2c0eaa0848cf4a908d9744b372ff2224bb291a320f36d76"
   ```

   Add the same lines below `chart_version` in
   environments/test/cluster/volsync/inputs.yaml (line 5). The marker
   environments/test/.disable parks the test environment (root.hcl:19-20 and
   the exclude block at root.hcl:86-90). The volsync unit of the test environment
   uses the same module, and `restic_image` has no default. Without the input,
   the unit fails when the test environment is no longer parked.

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
   `helm_release.volsync`. Do this the same way as the kueue module
   (modules/cluster/kueue/opentofu/main.tf:51). Also change the comment above
   the resource (main.tf:1-4), which says "Chart defaults everywhere":

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
   dependency below. Keep `../volsync` in the `dependencies` block. Replace
   the `inputs = yamldecode(...)` line (terragrunt.hcl:20):

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

   The volsync unit already has outputs (namespace, flux_substitution_sources,
   release). Without the shallow merge, a plan before the volsync apply reads
   the real outputs, finds no `restic_image`, and fails. The comment above
   `dependencies` (terragrunt.hcl:10-11) names "volsync's
   ReplicationDestination kind". Change it to name the restic image.

6. In environments/test/cluster/backup-controller/terragrunt.hcl, add the same
   `dependency "volsync"` block. Replace the `inputs` line (terragrunt.hcl:21)
   the same way. Its `dependencies` block already lists `../volsync`
   (terragrunt.hcl:13-15). The comment above it (terragrunt.hcl:10-12) names
   the ReplicationDestination kind. Change it to name the restic image.

   In environments/test/cluster/backup-controller/inputs.yaml, set
   `backup_controller_version: "v0.10.0"` (inputs.yaml:5 pins `v0.5.6`). The
   module now requires `restore_image` and appends `--restore-image` for every
   release. v0.5.6 exits at start on a flag that it does not know.

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

   The variable has no default. A unit that gives no value fails the plan with
   "No value for required variable". An empty string, or a tag without a
   digest, fails the validation.

8. In modules/cluster/backup-controller/opentofu/main.tf, replace the
   `workload` local (main.tf:10). The new local appends the flag to the args
   of the `controller` container in the Deployment document. It works the same
   way as the `namespaces` local (main.tf:20-36). The document keeps its key,
   so `kubectl_manifest.workload` keeps its address:

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

   Also add the `pause` input in the same change
   ([Add the pause input to the unit](#add-the-pause-input-to-the-unit)). Its
   `args` line replaces the `args` line above.

9. In environments/prod/cluster/backup-controller/inputs.yaml, set
   `backup_controller_version: "v0.10.0"`.

10. Update the READMEs and one comment:

    - In modules/cluster/backup-controller/opentofu/README.md, in "What the
      unit installs", list under workload the ValidatingAdmissionPolicy and
      ValidatingAdmissionPolicyBinding `backup-controller-restore-jobs`. Write
      that the Deployment gets `--restore-image` from the volsync unit.
    - In the same README, the intro and "Restores run as root" must say that
      the restore Job of the controller writes the claim. It still writes as
      root in namespaces with the annotation privileged-movers.
    - Add a `restore_image` input row to the Interface table of the same
      README.
    - In modules/cluster/volsync/opentofu/README.md, add rows to the
      Interface table for the `restic_image` input and output. Line 24 ("The
      chart defaults keep ...") needs a sentence which says that the unit sets
      the restic.image of the chart to `restic_image`.
    - In the same README, line 199 says that backup-controller fills a claim
      "with a Direct restore into an empty volume". Change it to name the
      restore Job.
    - flux/templates/backups/pvc/volumerestore.yaml:7-8 says that the
      controller "has VolSync restore into it". Change it to name the restore
      Job.

### Step 3: Apply the units

Apply through CI, as docs/ci/ci-quickstart.md in infra tells:

1. Push a branch with the change, and open a pull request.
2. Read the plan comment. The volsync unit changes `helm_release.volsync` in
   place. The backup-controller unit changes the CRDs, the ClusterRole and the
   Deployment, and adds the admission policy and its binding.
3. Merge the pull request. apply.yml applies the volsync unit first, then
   backup-controller, in the dependency order of terragrunt.

To apply from a shell, use "Apply with dependencies" in
docs/iac/manual-ops.md of infra. Run it in
environments/prod/cluster/backup-controller:

```
terragrunt run --all apply --queue-include-external
```

The apply replaces these objects from the release asset, in one change:

- The CRDs.
- The ClusterRole
  ([packaging.md](packaging.md#rbac-the-controller-needs)). It gets `list` on
  objectstores, `update` on volumerestores, and `update` on the finalizers of
  runs and claims. It gets new rules for the two run CRDs, Leases, Jobs, and
  the `scale` subresource of Deployments and StatefulSets. It loses `patch` on
  Deployments and StatefulSets, `create` on volumerestores, and every verb on
  ReplicationDestinations.
- The ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding
  `backup-controller-restore-jobs`. They limit the `jobs` rule to Jobs with the
  shape of the restore Job
  ([packaging.md](packaging.md#admission-policy-on-the-restore-jobs)).
- The Deployment. It gets the Recreate strategy, health probes, `GOMEMLIMIT`,
  a memory request of 256Mi and a limit of 512Mi. Recreate stops the old pod
  before the new pod starts.

### Step 4: Check the upgrade

Run the checks from ~/repos/infra with the prod kubeconfig. The .envrc of infra
sets `KUBECONFIG` only in a shell that loads it:

```
export KUBECONFIG=.output/prod/kubeconfig
```

```
kubectl -n backup-system rollout status deployment/backup-controller --timeout=120s
```

Expected result: `deployment "backup-controller" successfully rolled out`.

```
kubectl -n volsync-system get deploy volsync -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].args}' | tr ',' '\n' | grep restic-container-image
```

```
kubectl -n backup-system get deploy backup-controller -o jsonpath='{.spec.template.spec.containers[?(@.name=="controller")].args}' | tr ',' '\n' | grep restore-image
```

```
kubectl get validatingadmissionpolicy backup-controller-restore-jobs -o jsonpath='{.spec.matchConditions[0].expression}{"\n"}'
```

```
kubectl get validatingadmissionpolicybinding backup-controller-restore-jobs
```

Expected result:

- Both images are the `restic_image` value. Each printed line has JSON quotes.
- The second command prints exactly one `--restore-image`.
- The match condition names
  `system:serviceaccount:backup-system:backup-controller`.
- The binding exists.

If the pod is in CrashLoopBackOff, read its previous log:

```
kubectl -n backup-system logs deploy/backup-controller -c controller --previous
```

The log line `refusing to start: --restore-image is required` means that the
unit ran without the edit of step 2 item 8.

v0.10.0 has no code for a run that v0.7.x started
([decisions.md](decisions.md#upgrade-only-while-no-run-is-active)). Run the
listing of step 1 again. Expected result: no output, or only runs that v0.10.0
created after the apply. Delete a run that started before the apply. Then make
sure that its app runs.

### Step 5: Find Clusters that v0.7.x admitted over an old archive

v0.7.x admitted a Cluster as `initdb` over a prefix that holds WAL but no base
backup. Its webhook listed only `<prefix>/base/` and did not look at `wals/`.
It also admitted any Cluster with `backup.wlz.li/bootstrap: initdb` as written.
Such a Cluster runs and serves traffic, but each archive attempt fails from its
start ([restores.md](restores.md#a-database-that-could-never-archive)).
v0.10.0 acts only on creates. Thus, it does not repair a Cluster that exists.

List the Clusters whose archiving fails:

```
kubectl get clusters.postgresql.cnpg.io -A -o json | jq -r '.items[] | .status.conditions[]? as $c | select($c.type == "ContinuousArchiving" and $c.status == "False" and $c.reason == "ContinuousArchivingFailing") | "\(.metadata.namespace)/\(.metadata.name)"'
```

Expected result: no output.

For each line, search the plugin-barman-cloud container of the primary pod.
Find the primary pod:

```
kubectl -n <namespace> get clusters.postgresql.cnpg.io <cluster> -o jsonpath='{.status.currentPrimary}'
```

```
kubectl -n <namespace> logs <primary pod> -c plugin-barman-cloud | grep 'Expected empty archive'
```

Expected result for a Cluster that started over an old archive: a JSON line
for each attempt to archive a segment. The msg field of each line ends in:

```text
ERROR: WAL archive check failed for server <serverName>: Expected empty archive
```

Only such a Cluster needs steps 6 and 7. If the search prints nothing, the
same log names the other cause of the failure.

### Step 6: Give each such Cluster an empty archive

The database in such a Cluster holds writes that no backup contains. If you
delete the Cluster, these writes are lost. Repair the Cluster in place. It
archives to `<destinationPath>/<serverName>/`. The destinationPath comes from
the ObjectStore that its barmanObjectName names. The serverName is the name of
the Cluster by default. Select one of two repairs:

| The old archive | Repair |
| --- | --- |
| should stay | set a new serverName in the barman-cloud plugin parameters of the Cluster |
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

Keep the new serverName in Git. A disaster-recovery rebuild from the manifest
then looks in the archive that the manifest names. A Cluster whose serverName
goes back to the old one archives into the old archive again.

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
The instance keeps running through the repair. PostgreSQL keeps each segment
that it could not archive, and the plugin archives them in order after the
check passes. hack/e2e/cnpg/empty-archive-repair.sh does steps 6 and 7 on the
e2e cluster.

### Step 7: Take a base backup

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

### What changes for a v0.7.x cluster

- Every volume restore runs in a restic Job of the controller in
  backup-system. No restore creates a ReplicationDestination.
- A run with `all: true` keeps an app stopped for a maximum time: the
  `backup.wlz.li/max-quiesce` of the namespace, or ten minutes. If the mover
  of a namespace usually needs more time, set the annotation to a Go duration,
  such as `20m`. Otherwise the run fails the volume items whose clone VolSync
  did not cut in that time.
- The webhook lists the full server prefix `<prefix>/`, where v0.7.x listed
  `<prefix>/base/`. The S3 credential needs `s3:ListBucket` on the bucket. A
  policy that covers the whole bucket gives it.
- The webhook refuses an empty database over a prefix that holds WAL. A base
  backup counts only when its `backup.info` has `begin_time` and `end_time`
  ([compatibility.md](compatibility.md)).
- A backup and a restore of one claim or repository never run at the same
  time. Two runs never stop the workloads of one namespace at the same time.
  A run that cannot start for this reason waits with reason `SourceBusy`.
- New Ready reasons: `Paused`, `RestartFailed`,
  `ReleaseFailed`, and `Evicted` on a BackupRun. A VolumeRestore has the new
  reason `RestoreJobRefused`. Items have a new field, `status.items[].reason`.
  api.md has the full tables for a [BackupRun](api.md#ready-reasons), a
  [RestoreRun](api.md#ready-reasons-of-a-restorerun), a
  [VolumeRestore](api.md#status) and the [items](api.md#item-reasons).
- An in-place RestoreRun stops the workloads marked `backup.wlz.li/quiesce`
  together with the ones its `spec.quiesce` lists. It stops them before any
  restore Job or Cluster delete, and gives them back at the end. A run with
  `into:` stops nothing.
- An automatic restore of a whole app, a claim that the populator fills and a
  Cluster that the webhook admits with no RestoreRun, comes back to one
  quiesced moment: the time of the newest quiesced snapshot of each
  repository of the namespace, at or before `backup.wlz.li/restore-as-of`.
  When the repositories give two different times, the claim stays Pending
  with reason `NoBackupInReach` and the webhook refuses the Cluster, until a
  `restore-as-of` that both reach is set. A Cluster next to a claim that
  existed at that moment recovers as before, to the pin or to the end of the
  archive.
- Two BackupRuns of one Cluster take their CloudNativePG Backups one after
  the other, through the Lease `backup-controller-cluster-<cluster uid>`. The
  second waits with reason `SourceBusy`.
