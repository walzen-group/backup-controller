# Compatibility

The controller depends on the exact behaviour of the components that walzen
runs beside it: Kubernetes, Flux, VolSync and the restic inside its mover,
Kueue, CloudNativePG, the barman-cloud plugin and barman, cert-manager and
RustFS. Infra upgrades each of them over time, and we maintain the controller
against the new versions. When an upgrade changes a behaviour that this page
lists, we must change the controller to match. Every bump follows the
procedure in "When infra bumps a dependency".

## Versions built and tested against

versions.json at the repository root is the source for these versions.
flake.nix reads its pins and hashes, and the fixture tests read its versions.
TestE2EPinsMatchVersions (internal/testinfra/versions/e2epins_test.go) fails
when a hack/e2e/<component>/pins.json names a different version.

| Component | Version | Where prod sets it |
| --- | --- | --- |
| Kubernetes | 1.36.3 (the e2e cluster runs 1.36.4) | environments/prod/infra/talos/cluster.yaml |
| etcd | 3.6.14 in tests (Talos runs its own) | Talos v1.14 |
| Flux | 2.9.5 | environments/prod/cluster/flux/inputs.yaml |
| cert-manager | 1.21.2 | environments/prod/cluster/cert-manager/inputs.yaml |
| CloudNativePG | 1.30.0 (chart 0.29.0) | environments/prod/cluster/cloudnative-pg/inputs.yaml |
| plugin-barman-cloud | 0.15.0 (chart 0.8.0) | environments/prod/cluster/barman-cloud-plugin/inputs.yaml |
| barman | 3.20.0 | the plugin's sidecar image (sidecar-requirements.in) |
| PostgreSQL operand | 18.6 | spec.imageName of every prod Cluster |
| Kueue | 0.19.5 | environments/prod/cluster/kueue/inputs.yaml |
| VolSync | 0.16.0 | environments/prod/cluster/volsync/inputs.yaml |
| restic in the VolSync mover | 0.18.1 | mover-restic/SOURCE_VERSIONS in VolSync 0.16.0 |
| restic in the restore Job | 0.18.1, from the same image | `restic_image` in environments/prod/cluster/volsync/inputs.yaml from the v0.9.0 rollout on, which the backup-controller unit passes as `--restore-image` ([upgrading.md](upgrading.md#step-5-change-the-infra-units)) |
| restic run by hand | 0.19.1 | the operator's shell |
| RustFS | 1.0.0 | the S3 store behind shared-secrets/backup/backup.yaml |

The controller also builds against Go modules that go.mod pins:
k8s.io/api v0.36.0, k8s.io/apimachinery v0.36.0, k8s.io/client-go v0.36.0,
k8s.io/apiextensions-apiserver v0.36.0, controller-runtime v0.24.1,
lib-volume-populator v3.3.0 and github.com/backube/volsync v0.16.0.

## Following the versions the API server serves

An upgrade of Flux, Kueue, VolSync, CloudNativePG or the barman-cloud plugin
must not stop the controller. Most upgrades change nothing that the controller
uses. The controller therefore follows the version that the API server serves.
It fails loudly only when it meets a real incompatibility.

| Kinds | How the controller finds the version | When the version it used is gone |
| --- | --- | --- |
| Flux Kustomization<br>Kueue Workload and LocalQueue<br>CloudNativePG Cluster and Backup<br>barman-cloud ObjectStore | served.Kind finds the kind by group and kind in the RESTMapper of the manager, which caches the answer. Every request goes out at that version (internal/served/served.go). | The API server answers "404 page not found". served.VersionGone turns that answer into an error that the run retries. It also makes the mapper read discovery again, and the next pass uses the new version. The 404 never counts as a missing object. |
| VolSync ReplicationSource | The controller has Go types for volsync.backube/v1alpha1 only, from github.com/backube/volsync v0.16.0. It reads and writes no other VolSync kind. Restores run in its own Job. | volsyncUnsupported (internal/runs/volsynccheck.go) checks every unfinished BackupRun that is not a database-only run. The run ends Failed with Ready reason VolSyncUnsupported and a message that names the kind and the versions served. It first starts the workloads it stopped, because that needs no VolSync object. A RestoreRun reads ReplicationSources only to wait for a backup in progress, and it sends those reads at v1alpha1 as always. Each read fails. The run then shows reason VolSyncUnsupported on its Ready condition and changes nothing. It retries on every pass until v1alpha1 is served again or `spec.timeout` has passed. At the timeout it ends TimedOut, stops its restore Jobs and gives the app back. An app that it has already stopped stays stopped until then (see [restores](restores.md#when-volsync-stops-serving-v1alpha1)). The controller also logs the message one time at startup. |
| Core Kubernetes kinds (apps/v1, v1, batch/v1, coordination/v1, apiextensions.k8s.io/v1) | Fixed. These versions are GA and Kubernetes keeps serving them. | served.Client and served.Reader turn the 404 into a retried error for every kind, typed ones included. |

VolSync has published only v1alpha1, in every tag up to v0.16.0 and on its
main branch as of 2026-09-26. Its api/ directory holds v1alpha1 alone, and the
CRDs under config/crd/bases serve that version only. The controller therefore
has no other version to fall back to. It ends a BackupRun or lets a RestoreRun
retry as the table says. It never sends a request at a version it guessed.

Every create, update and patch that the controller sends carries
fieldValidation=Strict (clientOptions in cmd/backup-controller/main.go).
Without that parameter, kube-apiserver uses Warn. It then drops a field that
the CRD does not declare and answers with a warning only (k8s.io/apiserver
v0.36.3 pkg/endpoints/handlers/rest.go:409-414, and the apiextensions-apiserver
v0.36.0 custom resource decoder, pkg/apiserver/customresource_handler.go:1190-1192
and 1338-1340). With Strict, a field that a new release renamed or removed
makes the write fail. The run then reports an error such as `unknown field
"spec.renamedField"`. controller-runtime v0.24.1 adds the parameter to every
Create, Update and Patch of the client and of its Status and SubResource
writers (pkg/client/client.go:124-126, pkg/client/fieldvalidation.go).
TestEnvtestTheControllersWritesRefuseUnknownFields in
cmd/backup-controller/strict_envtest_test.go checks both halves against the
pinned kube-apiserver. One of its checks is a merge patch with the shape of the
Kustomization suspend.

After infra bumps any of these projects, run `make e2e` against the new pins
(see "When infra bumps a dependency" below). The unit and envtest suites check
that the controller follows a new version with unchanged fields. Only the e2e
run shows if the new release changed a field that the controller reads.

## Behaviours the controller relies on

Each row names the file and line where we read the behaviour, in the source
of the version in the table above. Paths are relative to the repository of
that project. The paths of the populator library are relative to
populator-machinery/controller.go in lib-volume-populator v3.3.0.

We checked the line numbers below again on 2026-09-26 against the release tag
that each table names:

- Flux v2.9.5 (dd233c4)
- restic v0.18.1 (7d0aa7f) and v0.19.1 (6aa3a51)
- barman release/3.20.0 (ce7aa25)
- plugin-barman-cloud v0.15.0 (f94016f)
- CloudNativePG v1.30.0 (4b5e244)
- VolSync v0.16.0 (22b97b0)
- kustomize-controller v1.9.5 (d5d5d2b)
- Kueue v0.19.5 (8e60d76)
- cert-manager v1.21.2 (922a06a)
- the module versions that go.mod pins (k8s.io/apimachinery v0.36.0,
  k8s.io/client-go v0.36.0, k8s.io/apiextensions-apiserver v0.36.0,
  controller-runtime v0.24.1, fluxcd/pkg/ssa v0.76.2, fluxcd/cli-utils v1.2.3)

We read the kube-apiserver of Kubernetes 1.36.3 from k8s.io/apiserver v0.36.3,
the module published at that release.

### Kubernetes

| Behaviour | Source |
| --- | --- |
| The API server refuses a new finalizer on an object that already has a deletionTimestamp. | apimachinery v0.36.0 pkg/api/validation/objectmeta.go:121-127, 315-318 |
| The pod template of a Job is immutable. VolSync answers the immutable-field error with a delete of the Job. For this reason, a spec change on a busy ReplicationSource kills its mover. | VolSync internal/controller/utils/reconcile.go:52-69 |
| With background cascading deletion, the owner goes first. The garbage collector deletes its dependents after that. | kubernetes.io/docs/concepts/architecture/garbage-collection, "Background cascading deletion" |
| The API server drops status fields that its CRD does not declare, and controller-runtime decodes the response into the object it sent. After Status().Update, an undeclared field reads back as its zero value. | envtest 1.36.3, internal/testinfra/strictclient |
| A list of a kind whose CRD is absent returns a no-match error (meta.IsNoMatchError). kueue.LocalQueue treats that error like NotFound, so a cluster without Kueue runs without admission. | envtest 1.36.3; internal/kueue/kueue.go:46-60 |
| The admission call carries the deadline of the API server in the timeout query parameter. The bootstrap webhook answers inside that budget. | k8s.io/apiserver v0.36.3 (the module of Kubernetes 1.36.3; go.mod does not pin it, and the build graph selects v0.36.0, where this range is the same) pkg/admission/plugin/webhook/mutating/dispatcher.go:276-295 |
| A create of an object that exists fails with AlreadyExists, and a failed create commits nothing. The create in etcd is a put with expected revision 0. Thus, if two runs create the same Lease at the same instant, exactly one wins. The claim, repository and quiesce Leases depend on this. | k8s.io/apiserver v0.36.3 pkg/storage/etcd3/store.go:317-326 and pkg/storage/errors/storage.go:63-64 |
| An update that carries a resourceVersion other than the stored one fails with Conflict. A delete whose UID or resourceVersion precondition does not match the stored object also fails with Conflict. Thus a run that takes a stale Lease never overwrites the run that took it first. | k8s.io/apiserver v0.36.3 pkg/registry/generic/registry/store.go:743-744; pkg/storage/interfaces.go:150-163 with pkg/storage/errors/storage.go:103-104 (a mismatched delete precondition becomes an invalid-object error, which InterpretDeleteError turns into Conflict) |
| The API server answers a request at a version it does not serve with a plain-text "404 page not found" body from its not-found handler. This applies to a discovery read and to an object request. client-go turns that answer into a NotFound whose details carry an UnexpectedServerResponse cause, which apierrors.IsUnexpectedServerError reports. The mapper of controller-runtime v0.24.1 has no Reset. A lookup of a kind that its group does not have makes the mapper read the discovery of every cached version of the group. The mapper drops the group from its cache when one of those versions answers 404. served.VersionGone and served.Rediscover (internal/served/served.go) depend on both behaviours. | k8s.io/apiextensions-apiserver v0.36.0 pkg/apiserver/customresource_handler.go:313-316 and pkg/apiserver/customresource_discovery.go:36-47 (an unserved version goes to the delegate), pkg/apiserver/apiserver.go:172-175 (the delegate is Go's http.NotFoundHandler when the CRD server has none, whose body is "404 page not found": go1.26.7 net/http/server.go:2322); k8s.io/apiserver v0.36.3 pkg/server/config.go:823 and pkg/server/handler.go:73-77, 121-154; k8s.io/client-go v0.36.0 rest/request.go:1242-1255, 1326-1361 (a text body goes through newUnstructuredResponseError); apimachinery v0.36.0 pkg/api/errors/errors.go:448-450, 501-507, 761-771; controller-runtime v0.24.1 pkg/client/apiutil/restmapper.go:55, 122-132, 158-216, 309-331; internal/served/served.go; internal/runs/versiongone_envtest_test.go |
| Since Kubernetes 1.31, the Job controller adds the terminal condition of a Job, Complete or Failed, only after every pod of the Job has ended. SuccessCriteriaMet or FailureTarget comes first. Read in internal/restorejob reads the result of the restore Job from Complete=True and Failed=True alone. Thus a terminal condition also says that no pod of the Job still runs. | kubernetes.io/docs/concepts/workloads/controllers/job/#terminal-job-conditions |
| A Job with spec.suspend true gets no pod from the Job controller. If a Job is suspended while it runs, the Job controller deletes its active pods and sets the condition Suspended=True. A Job created suspended gets Suspended=True immediately after the create. The controller creates every restore Job suspended and resumes it after its owner has recorded it. The stop suspends the Job and waits for Suspended=True. | kubernetes.io/docs/concepts/workloads/controllers/job/#suspending-a-job; internal/restorejob/stop_test.go (TestStopOfAJobNeverResumedDeletesIt) |
| The Job controller creates no pod for a Job that has a deletionTimestamp, and it never marks such a Job Suspended. Thus, for a Job that someone deleted with Foreground propagation, the stop sends no suspend and goes directly to its pods. | Kubernetes v1.36.3 source pkg/controller/job/job_controller.go:1138 |
| A podFailurePolicy needs restartPolicy Never. A FailJob rule on exit codes ends the Job immediately. An Ignore rule on a pod condition does not count the pod against backoffLimit. A rule without containerName applies to every container and init container. With a podFailurePolicy, podReplacementPolicy is Failed, so a replacement pod starts only after the old one has fully ended. | kubernetes.io/docs/concepts/workloads/controllers/job/#pod-failure-policy and #pod-replacement-policy; internal/restorejob/spec.go:282-303 |
| The Job controller gives every pod of a Job the label batch.kubernetes.io/controller-uid with the UID of the Job. The label stays on the pod after the Job is gone. The stop and Read select the pods of a restore Job by this label. Thus a pod of an earlier Job of the same name never counts. | kind, in the restic-jobs design review; internal/restorejob/stop_test.go (TestStopAndReadIgnoreAnOlderJobsPods) |
| A Job deleted through the API with no propagation policy orphans its pods. With Foreground propagation, the Job stays with the foregroundDeletion finalizer until the garbage collector has deleted its pods. The stop deletes a restore Job in this way. For a Job that someone deleted with Orphan or Background, the stop waits on the pods itself. | kubernetes.io/docs/concepts/architecture/garbage-collection/#foreground-deletion; internal/restorejob/stopgone_test.go |
| terminationMessagePolicy FallbackToLogsOnError puts the tail of the log of a failed container into the message of its terminated state. The tail is at most 2048 bytes or 80 lines. The message of the item shows the last lines of restic from there. | kubernetes.io/docs/tasks/debug/debug-application/determine-reason-pod-failure/#customizing-the-termination-message |
| A merge patch that carries metadata.uid fails with 422 Invalid ("metadata.uid: field is immutable") if the name now holds an object with another UID. The resume and the suspend of a restore Job carry its UID in this way. Thus a Job created again under the name stays untouched. | kind with Kubernetes 1.36, in the LATE review; internal/runs/restore_volume.go:307-311 |
| The scale subresource of Deployments and StatefulSets reads and writes spec.replicas as an autoscaling/v1 Scale. RBAC names it deployments/scale and statefulsets/scale. The API server copies only spec.replicas from the Scale into the object. Both kinds allow an unconditional update, which is an update with an empty resourceVersion. A Scale whose UID is different from the UID of the stored object fails with Conflict. | kubernetes.io/docs/reference/access-authn-authz/rbac/#referring-to-resources; Kubernetes v1.36.3 source pkg/registry/apps/deployment/strategy.go:148 and pkg/registry/apps/statefulset/strategy.go:186; internal/quiesce/stop.go |
| A ValidatingAdmissionPolicy (admissionregistration.k8s.io/v1, GA since 1.30) evaluates CEL over object (null on DELETE), oldObject (null on CREATE) and request.userInfo.username. A matchCondition that is false skips the policy. failurePolicy Fail denies the request on an expression error. A binding with validationActions [Deny] rejects the request. The API server type-checks only the expressions that read object or oldObject directly, and only for a rule that names a version. For this reason, the rule names batch v1 with matchPolicy Equivalent. | kubernetes.io/docs/reference/access-authn-authz/validating-admission-policy/; deploy/admissionpolicy.yaml; cmd/backup-controller/admissionpolicy_envtest_test.go |
| The delete of a pod that has no node sets its grace period to 0. A pods/binding request for a pod that has a deletionTimestamp fails with "pod ... is being deleted, cannot be assigned to a host". The stop gate of the restore Job (Stop in internal/restorejob) lets such a pod pass, because it can no longer start restic. An unscheduled pod without a deletionTimestamp holds the gate, because the scheduler can still bind it. | Kubernetes v1.36.3 source pkg/registry/core/pod/strategy.go:179-181 and pkg/registry/core/pod/storage/storage.go:235-236; on the docker-desktop cluster (Kubernetes 1.36.4) the unscheduled pod of a suspended restore Job was gone within seconds |

### Flux 2.9.5

Flux 2.9.5 ships kustomize-controller v1.9.5. Its go.mod pins
github.com/fluxcd/pkg/ssa v0.76.2 and github.com/fluxcd/cli-utils v1.2.3. The
rows below cite those modules where the behaviour is in them.

| Behaviour | Source |
| --- | --- |
| kustomize-controller puts the labels kustomize.toolkit.fluxcd.io/name and kustomize.toolkit.fluxcd.io/namespace on every object it applies. | kustomize-controller internal/controller/kustomization_controller.go:458-462; fluxcd/pkg ssa/manager.go:66-78; internal/quiesce/quiesce.go:62-68; internal/quiesce/plan.go:59-60 |
| The status.inventory.entries of a Kustomization holds one entry per applied object, with id "<namespace>_<name>_<group>_<kind>" (for a Deployment, notes_notes_apps_Deployment). A quiesce suspends a Kustomization only when that list holds the workload. A run refuses a Kustomization whose list also holds an apps Deployment or StatefulSet of another namespace, already suspended or not. | kustomize-controller internal/controller/kustomization_controller.go:482 and internal/inventory/inventory.go:39-50 (the id is ObjMetadata.String()); fluxcd/cli-utils pkg/object/objmetadata.go:32, 118-128; internal/quiesce/plan.go:55-203 |
| spec.suspend: true stops the reconcile of the Kustomization by kustomize-controller. It then applies no new revision and does no drift correction. A workload scaled to 0 without its knowledge stays stopped. After the resume of the Kustomization, the controller applies its manifest again and gives the workload the replicas of the manifest back. For this reason, a run leaves out a Kustomization that a person suspended. A run also refuses a Kustomization that applies workloads of another namespace, because a suspend would stop the drift correction there. | kustomize-controller internal/controller/kustomization_controller.go:183-187 (the reconcile returns while suspended); fluxcd.io/flux/components/kustomize/kustomizations/#suspend; internal/quiesce/plan.go:55-100 |

### VolSync 0.16.0

The Go paths in both VolSync tables are relative to internal/controller in
the VolSync module.

| Behaviour | Source |
| --- | --- |
| A sync is in progress while status.lastSyncStartTime is set. | statemachine/machine.go:162-174 |
| When a sync completes, VolSync copies the value that spec.trigger.manual holds at that moment into status.lastManualSync. It writes lastManualSync only then. It starts a new sync only when the manual tag is different from it. | statemachine/machine.go:213-214, 225-242 |
| VolSync writes latestMoverStatus and lastManualSync in the same status update. | mover/restic/mover.go:648-650; statemachine/machine.go:103-118, 185-215 |
| The mover Job has backoffLimit 8. At 8 failed pods, VolSync writes the logs into status.latestMoverStatus with result Failed and deletes the Job with background propagation. It creates a new Job on the next reconcile. VolSync retries a failed mover forever. | mover/restic/mover.go:355-356, 609-618; utils/podlogs.go:166-169, 186-191 |
| VolSync creates the clone volsync-<source>-src one time per sync, and every Job and pod of that sync uses it again. Of the steps of VolSync, only Cleanup after a completed sync deletes the clone. The ReplicationSource is also the controller owner of the clone, so the delete of the source deletes the clone through garbage collection. | mover/restic/mover.go:171-188, 270-294; mover/mover.go:29 (the volsync- prefix); volumehandler/volumehandler.go:336-358; utils/cleanup.go:36-39, 50-58 |
| The ReplicationSource is the controller owner of its Job, so the delete of the source deletes the running mover pod. | mover/restic/mover.go:347 |
| Each reconcile builds the Job again. A change to spec.restic.unlock, the retention or the affinity changes the pod template and kills a running mover. VolSync sets the affinity only when the copy method is Direct. | mover/restic/mover.go:346, 362, 373-380, 404, 523-531 |
| A prune becomes due pruneIntervalDays after lastPruned. When lastPruned is empty, it becomes due that long after the creation of the source. | mover/restic/mover.go:378, 656-667 |
| When spec.restic.unlock is different from status.restic.lastUnlocked, every mover pod runs restic unlock before backup. VolSync writes lastUnlocked only after a Job succeeds. | mover/restic/mover.go:373-376, 631-635, 669-674 |
| When a sync completes, VolSync sets lastSyncTime and sets lastSyncDuration to the time since lastSyncStartTime on the same clock. It then clears lastSyncStartTime. A backup looks for the snapshot of its sync in the window [lastSyncTime - lastSyncDuration - 5s, lastSyncTime + 1s + 5s]. It fails the item with reason NoMoverSnapshot when either field is missing. | statemachine/machine.go:196-219; internal/runs/backup_identify.go:62-97 in this repository |
| A mover pod that saved its snapshot and then failed, at forget or at its end, leaves that snapshot. The retry of the Job saves another snapshot inside the same sync. A backup records the newest snapshot in the window and names the others in the message of the item. | mover/restic/mover.go:355; mover-restic/entry.sh:154-167 in the module root |
| latestMoverStatus holds only result and logs. On a successful Job, VolSync clears the logs and writes Successful. It leaves the logs empty when it cannot read the pod. MOVER_LOG_MAX_BYTES=0 makes every stored log empty. The controller decides nothing on the logs. A failed backup item shows them in its message. | api/v1alpha1/common_types.go:134-137 in the module root; utils/podlogs.go:37-40, 175-222 |
| In a namespace with the annotation volsync.backube/privileged-movers: "true" in any letter case, the mover runs as user 0 with DAC_OVERRIDE, CHOWN and FOWNER added. The restore Job follows the same annotation in the same way (PrivilegedMovers in internal/restorejob). | utils/namespace.go:44-50; mover/restic/mover.go:588-606 |
| The chart gives the mover image as --restic-container-image. When the value restic.image is set, it replaces the full reference. The infra volsync unit sets it from restic_image, which is also the image of the restore Job. | helm/volsync/templates/_helpers.tpl:69-78 in the module root |

The mover script, mover-restic/entry.sh in the same module:

| Behaviour | Source |
| --- | --- |
| entry.sh runs under set -e and is PID 1 of the pod with no trap. Thus the delete of a pod kills restic with SIGKILL, and restic leaves its lock. | entry.sh:7; mover/restic/mover.go:486 |
| A backup mover runs restic forget with the retention of the source after every backup. It runs prune on its interval. | entry.sh:154-167, 371-379 |
| Its unlock step runs plain restic unlock (--remove-all is commented out). This command removes only stale locks. | entry.sh:173-174 |
| The backup mover runs restic backup --host volsync --exclude='lost+found' . in /data. Thus every snapshot that a mover writes has host volsync and the one path /data. restic.MoverLayout accepts exactly those as restore candidates. | entry.sh:62, 154-158; internal/restic/repository.go:201-219 in this repository |
| A mover whose /data holds only lost+found exits 0 without a backup, and VolSync reports the sync as successful. A backup that finds no snapshot in the window of its sync on two listings at least 11 seconds apart counts the claim as empty. | entry.sh:81-87 |

### restic

The VolSync mover runs restic 0.18.1, and the operator runs 0.19.1 by hand.
The conformance tests replay repositories recorded with both versions, so each
row cites both releases. Where the two columns give the same lines, the cited
code is identical in both.

| Behaviour | Source in 0.18.1 | Source in 0.19.1 |
| --- | --- | --- |
| backup and restore get a shared lock, and forget gets an exclusive lock. | cmd/restic/cmd_backup.go:513; cmd/restic/cmd_forget.go:189; cmd/restic/cmd_restore.go:134 | cmd/restic/cmd_backup.go:534; cmd/restic/cmd_forget.go:193; cmd/restic/cmd_restore.go:149 |
| restic refuses an exclusive lock while any other lock exists, stale or not. forget then exits 11 with "repository is already locked". | internal/restic/lock.go:56, 160-216 (the branch at 188); cmd/restic/main.go:211-212 | internal/restic/lock.go:56, 160-216 (the branch at 188); cmd/restic/main.go:231-232 |
| restic does not retry a lock by default. | cmd/restic/global.go:108 | internal/global/global.go:101 |
| A lock is stale when it is older than 30 minutes, or when it names this host and a dead PID. A mover pod never shares a hostname with a dead one, so for movers only the 30 minutes apply. | internal/restic/lock.go:252-288 | internal/restic/lock.go:252-288 |
| A live restic refreshes its lock every 5 minutes. When a refresh does not succeed in time, the lock monitor cancels the context of the command and restic exits. | internal/repository/lock.go:29, 37, 161-170, 183-243; internal/restic/lock.go:309-330 | internal/repository/lock.go:29, 37, 161-170, 183-243; internal/restic/lock.go:309-330 |
| restic unlock removes only stale locks. unlock --remove-all removes every lock. | cmd/restic/cmd_unlock.go:45-65; internal/repository/lock.go:274-294 | cmd/restic/cmd_unlock.go:49-70; internal/repository/lock.go:274-294 |
| restic snapshots sorts by full time with an unstable sort. | cmd/restic/cmd_snapshots.go:100; internal/restic/snapshot.go:260 | cmd/restic/cmd_snapshots.go:115; internal/data/snapshot.go:261 |
| restic rewrite records the old snapshot as original and can change the tree. The retime of the controller writes the same fields. | cmd/restic/cmd_rewrite.go:252-254 | cmd/restic/cmd_rewrite.go:254-256 |

The restore Job runs restic 0.18.1 from the mover image of VolSync. It uses
the command lines that internal/restorejob/spec.go:331-342 builds:

1. `restic unlock` in the init container.
2. `restic restore <full ID> --target /data
   --include-xattr user.* --retry-lock 30m --delete`.

These rows cite 0.18.1 only. Paths are relative to the restic repository.

| Behaviour | Source in 0.18.1 |
| --- | --- |
| restore parses a full 64-character hex ID and loads that snapshot file directly. The --host, --tag and --path filters apply only to latest. A missing snapshot ends restore with "failed to find snapshot" and exit 1. | internal/restic/snapshot_find.go:91-122; cmd/restic/cmd_restore.go:140-147 |
| Exit codes: 0 success, 1 failure, 3 unreadable source data, 10 no repository, 11 already locked, 12 wrong password, 130 cancelled. The restore Job fails immediately on 10 and 12. ExitMeaning in internal/restorejob/read.go names each code for the message of the item. | cmd/restic/main.go:203-224; doc/075_scripting.rst:44-69 |
| restic handles SIGTERM itself: it cancels its context, removes its lock and exits 130. A stop before restic opens the repository ends it with exit 1 ("config cannot be loaded: context canceled"). The e2e run of TestStoppingARestoreEndsResticAtOnce showed this. | cmd/restic/cleanup.go:17-22; cmd/restic/main.go:203-224; test/e2e/restore_job_test.go in this repository |
| A non-exclusive lock, which restore gets, fails only on an exclusive lock. restic does not skip a stale lock when it checks, so a stale exclusive lock blocks every restore. The plain restic unlock of the init container removes such a lock when it is older than 30 minutes. | cmd/restic/cmd_restore.go:134; internal/restic/lock.go:155-215 |
| --retry-lock is a global option. restic tries again to get the lock for that time, and then exits 11. A lock that becomes stale during the wait still blocks it. Only the unlock init container of the next pod removes that lock. | cmd/restic/global.go:108; cmd/restic/lock.go:19 |
| --delete exists since 0.17.0, and --include-xattr since 0.18.0. | CHANGELOG.md:1247-1249, 428 |
| As root, a failed lchown makes the restore of that file fail. As another user, restic ignores permission errors from the restore of metadata. The restore Job runs as root only in a namespace with the annotation privileged-movers. | internal/fs/node.go:232-252 |
| backup gives a snapshot the time at which the backup starts. | cmd/restic/cmd_backup.go:500-503 |

### Kueue 0.19.5

| Behaviour | Source |
| --- | --- |
| The pod webhook mpod.kb.io of the chart has failurePolicy Fail when the pod integration is on (prod turns it on). Its namespaceSelector is managedJobsNamespaceSelector (kueue-managed: "true" in prod). While the webhook of Kueue is down, the API server refuses every pod create in those namespaces, mover pods included. | charts/kueue/templates/webhook/manifests.yaml:19-20, 307-341 |
| With manageJobsWithoutQueueName false, Kueue gates only objects that carry the kueue.x-k8s.io/queue-name label. A managed namespace that holds a LocalQueue named default is the exception. There, the webhooks of Kueue write queue-name: default onto an unlabelled Job or pod whose owner Kueue does not already manage. Kueue then gates it. | Kueue v0.19.5 source pkg/controller/jobs/pod/pod_webhook.go:145-162; pkg/controller/jobframework/defaults.go:59-89; pkg/controller/jobframework/reconciler.go:368-379; pkg/controller/constants/constants.go:27; hack/e2e/kueue/values.yaml (prod's settings) |
| Kueue admits the Workload of a BackupRun in kueue.x-k8s.io/v1beta2 against its LocalQueue. Kueue reads the pod template to count quota and never runs its image. | internal/kueue/kueue.go:24-31 |
| The pod integration can stop a pod through eviction, preemption, deactivation or the waitForPodsReady timeout. Kueue then first sets the pod condition TerminationTarget=True, with the stop reason as the condition reason. After that, it deletes the pod. It does not set DisruptionTarget. The podFailurePolicy of the restore Job ignores both conditions. A pod that Kueue stops does not count against the backoffLimit of 3, and the Job starts a replacement after that pod has ended. | Kueue v0.19.5 source pkg/controller/jobs/pod/pod_controller.go:70, 527-583; internal/restorejob/spec.go |
| With manageJobsWithoutQueueName false, Kueue ignores a Job without the queue-name label. A restore Job carries the queue label only on its pod template. The labels of the Job itself are never empty, because the API server copies the labels of the template onto a Job created with none. Kueue therefore leaves the suspended Job alone. After the resume, Kueue admits its pod through the pod integration, like the pod of a backup mover. In a managed namespace that holds a LocalQueue named default, the job webhook of Kueue would write queue-name: default onto the restore Job. Kueue would then suspend and resume the Job itself, against the suspend of the controller. No prod namespace has a LocalQueue named default. That is a declared fact, which we check before a release and never in code. | Kueue v0.19.5 source pkg/controller/jobframework/reconciler.go:368-374; pkg/controller/jobs/job/job_webhook.go:107; internal/restorejob/spec.go:200-211 |

### CloudNativePG 1.30.0

| Behaviour | Source |
| --- | --- |
| After initdb, the instance creates the marker file .check-empty-wal-archive. It removes the file only after the ContinuousArchiving condition is True. | pkg/management/postgres/initdb.go:355-360; internal/management/controller/instance_controller.go:1100-1110 |
| When a Cluster enables a WAL archive plugin, the instance manager gives every segment to the plugin and skips its own empty-archive check. It sends the plugin no CheckEmptyWalArchive decision. Thus the plugin reads the marker and the cnpg.io/skipEmptyWalArchiveCheck annotation itself (next section). | pkg/management/postgres/archiver/archiver.go:165-176; pkg/utils/labels_annotations.go:203-205, 539-544; internal/cnpi/plugin/client/wal.go:66-69, 122-126 (neither request sets CheckEmptyWalArchive) |
| A pg_basebackup bootstrap creates the same marker after the clone. | internal/cmd/manager/instance/pgbasebackup/cmd.go:151-156 |
| A failing archive_command sets ContinuousArchiving False with reason ContinuousArchivingFailing. pg_wal then grows until the volume is full. | pkg/management/postgres/webserver/local.go:246-262 |

### plugin-barman-cloud 0.15.0 and barman 3.20.0

| Behaviour | Source |
| --- | --- |
| While the marker exists and the Cluster does not have cnpg.io/skipEmptyWalArchiveCheck: enabled, the plugin runs barman-cloud-check-wal-archive before it archives each segment. It gives no --timeline. | plugin internal/cnpgi/common/wal.go:159-178, 239-254; internal/cnpgi/common/check.go:32-53; github.com/cloudnative-pg/barman-cloud v0.6.0 (the plugin's go.mod pin) pkg/archiver/archiver.go:139-166 |
| barman-cloud-check-wal-archive lists <server>/wals/ and fails with "Expected empty archive" on any WAL file. A prefix that holds only FAILED base backups and no WAL passes. | barman src/barman/clients/cloud_check_wal_archive.py:60-65; src/barman/cloud.py:2421-2446; src/barman/xlog.py:567-574 |
| barman uploads each segment to <server>/wals/<hash dir>/<name> unconditionally. Thus a new database on timeline 1 overwrites the segments of an old archive. | src/barman/clients/cloud_walarchive.py:308-333 |
| barman names a backup directory by its start time and marks it STARTED, then FAILED or DONE. | src/barman/cloud.py:1629, 1703-1707, 1730, 1751-1768 |
| Retention puts a backup that is not DONE in the class NONE and deletes only OBSOLETE ones. Thus barman never deletes a FAILED backup. | src/barman/retention_policies.py:190-218; src/barman/clients/cloud_backup_delete.py:424-428 |
| A base/<id>/ directory without backup.info counts as no backup. | src/barman/cloud.py:2541-2551; src/barman/cloud_providers/aws_s3.py:511-514 |
| A declared recovery whose Cluster also archives to an ObjectStore (barmanObjectName set) fails at start when that target archive is not empty. The exception is a Cluster that carries cnpg.io/skipEmptyWalArchiveCheck: enabled. | plugin internal/cnpgi/restore/restore.go:105-120, 249-297, 306-311 |

### lib-volume-populator v3.3.0

| Behaviour | Source |
| --- | --- |
| The library reads the data source from its informer cache first. On NotFound, it records PopulatorDataSourceNotFound and returns before any other step. Thus, without the VolumeRestore, it never removes its own claim finalizer. | controller.go:662-671 |
| A missing StorageClass also returns nil. So does a WaitForFirstConsumer class when the claim has no volume.kubernetes.io/selected-node. | controller.go:677-703 |
| For a claim in deletion, the library skips population and runs cleanup. | controller.go:743, 976 |
| Population creates the prime claim and adds the claim finalizer. It calls Populate only after the prime has spec.volumeName. | controller.go:779, 791, 804-809 |
| Cleanup runs PopulateCleanupFn, deletes the prime, and then removes the claim finalizer. Each step returns on error, and the claim goes back into the queue. | controller.go:980-1008 |
| The name of the prime is "prime-" + the UID of the claim. The claim finalizer is <Prefix>/populate-target-protection (backup.wlz.li/populate-target-protection here). | controller.go:67, 69, 318, 720; this repository's cmd/backup-controller/main.go:135 and internal/populator/names.go:9 |

### cert-manager 1.21.2

| Behaviour | Source |
| --- | --- |
| A self-signed Issuer issues and renews the serving certificate of the webhook. It writes the certificate itself as ca.crt into its Secret. The cainjector reads ca.crt from the Secret of the Certificate that cert-manager.io/inject-ca-from names. It writes that value into the caBundle of every webhook in that MutatingWebhookConfiguration. | cert-manager pkg/controller/certificaterequests/selfsigned/selfsigned.go:217-221; pkg/controller/cainjector/sources.go:78-153; pkg/controller/cainjector/injectables.go:94-112; this repository's deploy/webhook.yaml:9-35, 79 |
| The controller pod reads the renewed tls.crt and tls.key without a restart. The webhook server of controller-runtime watches both files and loads them again. | controller-runtime v0.24.1 pkg/webhook/server.go:201-217; this repository's chart/templates/deployment.yaml:61-67 |

### RustFS 1.0.0

| Behaviour | Source |
| --- | --- |
| ListObjectsV2 obeys prefix, delimiter and max-keys. GET returns backup.info bodies as barman wrote them. The S3Prober of the webhook and the S3 backend of restic depend on those answers. |
| A listing may lag a write. The list quorum of the RustFS configuration decides if a ListObjectsV2 immediately after a PUT shows the object. Thus a backup that finds no snapshot of its sync lists the repository a second time, at least 11 seconds after the first. It does this before it records the claim as empty. That second listing finds a snapshot that shows up late. A listing that lags longer than that would record a volume that held files as empty. | internal/runs/backup_identify.go:169-206 (relistAfter and noSnapshotListed); RustFS documentation on list quorum | internal/testinfra/s3fake (replays of recorded RustFS answers) |

## Fields the controller reads from other projects

Each row says what the controller does when the field is missing. A release
that moves or renames the field looks like that to the controller. "Loud"
means that the run fails or reports an error that names the field.
"Legitimate" means that the missing field is a normal state and the controller
waits. Each of those waits ends at the timeout of the run, so none of them can
report success.

| Project | Object and field | Read in | When the field is missing |
| --- | --- | --- | --- |
| CloudNativePG | Backup status.phase | BackupResult, internal/cnpg/cnpg.go | Legitimate while empty or pending, started, running, finalizing or walArchivingFailing (the phases in CloudNativePG 1.30 api/v1/backup_types.go:32-58). Any other value, such as a phase that a later CloudNativePG release adds, keeps the item Running. The run names status.phase and the value in its Ready message while it waits. It names them in the message of the item if it reaches its timeout. |
| CloudNativePG | Backup status.error | backupResult | Legitimate: a failed Backup without it gets a message that names its phase. |
| CloudNativePG | Cluster status.phase | Phase, internal/cnpg/cnpg.go | Legitimate: a RestoreRun waits for "Cluster in healthy state" and counts no other phase as healthy. |
| CloudNativePG | Cluster metadata annotations cnpg.io/hibernation, backup.wlz.li/enabled | internal/cnpg/cnpg.go | Legitimate: annotations are optional. |
| CloudNativePG | Cluster spec.plugins (name, isWALArchiver, parameters.barmanObjectName, parameters.serverName), spec.bootstrap, spec.externalClusters | Archiver and declaredBootstrap, internal/bootstrap/cluster.go | Legitimate while the API server serves Cluster at postgresql.cnpg.io/v1. deploy/webhook.yaml registers apiVersions ["v1"] with matchPolicy Equivalent. Thus the API server converts a Cluster created at any other served version to v1 before it calls the webhook (kubernetes.io, Dynamic Admission Control, "Matching requests: matchPolicy"). When v1 is no longer served, the API server skips the webhook and a new Cluster starts empty. clusterWebhookBlind (internal/runs/clusterwebhook.go) logs that at startup. A RestoreRun then deletes no Cluster, and it ends or waits with reason ClusterVersionUnsupported. A run that fails its Cluster item in one pass and ends in a later pass ends with reason Failed instead. The message of the item then names the unserved version. The rules name v1 alone because the handler reads and patches the fields of v1. At another version, the API server would drop a patched field that the schema does not have, and the Cluster would still start empty. |
| barman-cloud | ObjectStore spec.configuration.destinationPath | storeLocation, internal/bootstrap/archive.go | Loud: the webhook refuses the create, and a RestoreRun fails its check. Both name the field. |
| barman-cloud | ObjectStore spec.configuration.endpointURL | storeLocation | Legitimate: barman then uses the endpoint of AWS S3. |
| barman-cloud | ObjectStore spec.configuration.s3Credentials.{accessKeyId,secretAccessKey}.{name,key} | credential, internal/bootstrap/archive.go | Loud: the error names the field. |
| barman-cloud | ObjectStore spec.configuration.endpointCA.{name,key} | endpointCA, internal/bootstrap/archive.go | Legitimate when name is absent: the endpoint then needs a public authority. Loud when name is set and key is not. |
| Flux | Kustomization status.inventory.entries[].id | inventoryIDs, internal/quiesce/plan.go | Loud: the controller refuses the run before the run stops anything, if two conditions are true. The kustomize-controller labels of a workload name the Kustomization. The list is missing, or an id is not "<namespace>_<name>_<group>_<kind>". |
| Flux | Kustomization spec.suspend | SetSuspend and Plan, internal/quiesce/stop.go and internal/quiesce/plan.go | Loud: SetSuspend reads the value back from the answer to the patch. It fails when that value is different from the value it set. |
| Kueue | Workload status.conditions Admitted and PodsReady | Admitted and MarkPodsReady, internal/kueue/kueue.go | Legitimate while the Workload waits in its queue. Loud after the timeout of the run has passed since its creation: awaitAdmission fails the run and names Kueue, the Workload and the LocalQueue. |
| Kueue | LocalQueue metadata.name | LocalQueue, internal/kueue/kueue.go | Legitimate: a namespace with no LocalQueue runs without admission. Kueue 0.19.5 serves v1beta1 and v1beta2, and every field the controller uses is the same in both. |
| VolSync | ReplicationSource status.lastSyncStartTime | inUse, internal/runs/sources.go; syncGoesOn, internal/runs/backuprun.go | Legitimate: no sync runs. The field comes from the v1alpha1 Go type. A rename would come as a new version, which volsyncUnsupported catches. |
| VolSync | ReplicationSource status.lastManualSync | lastManual, internal/runs/trigger.go | Legitimate: VolSync writes it only when a manual sync completes. A run waits for its own tag there. |
| VolSync | ReplicationSource status.latestMoverStatus.result and .logs | moverFailed, internal/runs/trigger.go; collectVolume, internal/runs/backuprun.go | Legitimate: no mover has finished yet. The result decides a failed backup item. The item only shows the logs in its message, and an empty log changes nothing. |
| VolSync | ReplicationSource status.lastSyncTime and status.lastSyncDuration | windowOf, internal/runs/backup_identify.go | Loud: a completed sync without either field fails the item with reason NoMoverSnapshot and names the field. The run cannot tell which snapshot the sync wrote. |

## When infra bumps a dependency

### Step 1: Change the pin

Change the entry of the component in versions.json, with its hashes. Also
change every hack/e2e/<component>/pins.json that names it.

```
nix develop -c go test ./internal/testinfra/versions/
```

Expected result: `ok`. A failure names the pins.json value that is different
from versions.json.

### Step 2: Re-record the fixtures

```
make fixtures
```

This command records again the restic repositories, the barman stores with
the answers of RustFS, and the pinned third-party CRDs. Until it runs, the
tests that check provenance (TestTheRecordingsCameFromThePinnedTools) fail.

### Step 3: Run the unit and envtest suites

```
make check
```

```
make envtest
```

Expected result: both pass. If a conformance or recorded-store test fails,
the new version behaves differently from a row in this doc.

### Step 4: Run the e2e suites

```
make e2e-fetch e2e-up e2e
```

The three targets do these steps:

1. e2e-fetch downloads the new images and charts.
2. e2e-up installs every component into the docker-desktop cluster and runs
   its smoke check.
3. e2e runs every test tagged e2e.

Expected result: every component's check and every test passes.

### Step 5: Re-read the bumped component's source

Open the source of the new version at each file and line that this page lists
for that component. Check that each behaviour still holds. Update the line
numbers in this doc while you check. The tests cover only part of each table,
so this step is necessary even when steps 3 and 4 pass.

### Step 6: Fix and release

Change the controller where a behaviour moved. Add a test that fails on the
old code. Then release. A release follows [releasing.md](releasing.md): every
check there, `make e2e` and `make demo` included, passes locally first.

## Which tests catch which behaviour

| Test | Behaviours it checks | Runs in |
| --- | --- | --- |
| internal/restic/conformance_test.go | restic's snapshot listing, the entry.sh snapshot pick, retime against restic rewrite, the lock of the killed mover and exit 11 of forget, lock staleness | make check, against repositories recorded with restic 0.18.1 and 0.19.1 |
| internal/testinfra/barmanstore/store_test.go | the result of barman-cloud-check-wal-archive on each recorded store, and the S3Prober against it | make check, against stores recorded with barman 3.20.0 and RustFS |
| internal/testinfra/s3fake replay tests | the answers of RustFS to listing and GET, and the requests of the S3Prober | make check |
| internal/testinfra/versions | versions.json against every e2e pins.json | make check |
| internal/testinfra/strictclient differential suites | pruning, defaulting, finalizers and garbage collection of the API server, strict client against the real one | make envtest (TestDifferentialAgainstEnvtest, Kubernetes 1.36.3) and make e2e (TestDifferentialAgainstCluster, Kubernetes 1.36.4) |
| internal/populator/orphans_envtest_test.go | the data-source lookup order of the populator library: a claim whose VolumeRestore was deleted while the prime was Pending stays Terminating until the controller releases it | make envtest |
| test/e2e | a BackupRun against real VolSync, restic, Kueue and RustFS with the mover log of VolSync off. The restore Job against the real Job controller: a restore by full ID of each of two snapshots in one second, a missing snapshot that fails with exit 1, a stale lock that the unlock init container removes, restic that ends with exit 130 when the run is deleted, and the admission policy and the restore image in place | make e2e |
| internal/restorejob tests | the Job shape the admission policy expects, Read on terminal conditions and container statuses, and the stop gate against the strict client's Job and garbage-collection rules | make check |
| cmd/backup-controller/admissionpolicy_envtest_test.go | the checks of the admission policy, one refused Job per check, and the writes of the ServiceAccount to workloads only through `scale` | make envtest |

The rows for the VolSync state machine, the CloudNativePG marker file, the
webhook of Kueue and the Flux inventory have no recorded fixture. Only the e2e
run in step 4 and the source reading in step 5 check them.
