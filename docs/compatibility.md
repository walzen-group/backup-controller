# Compatibility

The controller depends on the exact behaviour of the components walzen runs
beside it: Kubernetes, Flux, VolSync and the restic inside its mover, Kueue,
CloudNativePG, the barman-cloud plugin and barman, cert-manager and RustFS.
Infra upgrades each of them over time, and the controller is maintained
against the new versions. When an upgrade changes a behaviour listed below, we
need to change the controller to match. Every bump goes through the procedure
in "When infra bumps a dependency".

## Versions built and tested against

versions.json at the repository root is the source for these versions.
flake.nix reads its pins and hashes, the fixture tests read its versions, and
TestE2EPinsMatchVersions (internal/testinfra/versions/e2epins_test.go) fails when a
hack/e2e/<component>/pins.json names a different version.

| Component | Version | Where prod sets it |
| --- | --- | --- |
| Kubernetes | 1.36.3 (the e2e cluster runs 1.36.4) | environments/prod/infra/talos/cluster.yaml |
| etcd | 3.6.14 in tests; Talos runs its own | Talos v1.14 |
| Flux | 2.9.5 | environments/prod/cluster/flux/inputs.yaml |
| cert-manager | 1.21.2 | environments/prod/cluster/cert-manager/inputs.yaml |
| CloudNativePG | 1.30.0 (chart 0.29.0) | environments/prod/cluster/cloudnative-pg/inputs.yaml |
| plugin-barman-cloud | 0.15.0 (chart 0.8.0) | environments/prod/cluster/barman-cloud-plugin/inputs.yaml |
| barman | 3.20.0 | the plugin's sidecar image (sidecar-requirements.in) |
| PostgreSQL operand | 18.6 | spec.imageName of every prod Cluster |
| Kueue | 0.19.5 | environments/prod/cluster/kueue/inputs.yaml |
| VolSync | 0.16.0 | environments/prod/cluster/volsync/inputs.yaml |
| restic in the VolSync mover | 0.18.1 | mover-restic/SOURCE_VERSIONS in VolSync 0.16.0 |
| restic run by hand | 0.19.1 | the operator's shell |
| RustFS | 1.0.0 | the S3 store behind shared-secrets/backup/backup.yaml |

The controller also builds against Go modules that go.mod pins:
k8s.io/api v0.36.0, controller-runtime v0.24.1, lib-volume-populator v3.3.0
and github.com/backube/volsync v0.16.0.

## Behaviours the controller relies on

Each row names the file and line where the behaviour was read, in the source
of the version in the table above. Paths are relative to that project's
repository; the populator library's paths are relative to
populator-machinery/controller.go in lib-volume-populator v3.3.0.

### Kubernetes

| Behaviour | Source |
| --- | --- |
| The API server refuses a new finalizer on an object that already has a deletionTimestamp. | apimachinery v0.36.0 pkg/api/validation/objectmeta.go:121-127, 315-318 |
| A Job's pod template is immutable. VolSync answers the immutable-field error by deleting the Job, which is why a spec change on a busy ReplicationSource kills its mover. | VolSync internal/controller/utils/reconcile.go:52-69 |
| With background cascading deletion, the owner goes first and the garbage collector deletes its dependents afterwards. | kubernetes.io/docs/concepts/architecture/garbage-collection, "Background cascading deletion" |
| The API server drops status fields its CRD does not declare, and controller-runtime decodes the response into the object it sent. After Status().Update, an undeclared field reads back as its zero value. | envtest 1.36.3, internal/testinfra/strictclient |
| Listing a kind whose CRD is absent returns a no-match error (meta.IsNoMatchError). localQueue treats that error like NotFound, so a cluster without Kueue runs without admission. | envtest 1.36.3; internal/runs/kueue.go:30-45 |
| The admission call carries the API server's deadline in the timeout query parameter. The bootstrap webhook answers inside that budget. | k8s.io/apiserver pkg/admission/plugin/webhook/mutating/dispatcher.go:276-293 |

### Flux

| Behaviour | Source |
| --- | --- |
| kustomize-controller labels every object it applies with kustomize.toolkit.fluxcd.io/name and kustomize.toolkit.fluxcd.io/namespace. | internal/runs/quiesce.go:26-33 |
| A Kustomization's status.inventory.entries holds one entry per applied object, with id "<namespace>_<name>_<group>_<kind>" (for a Deployment, notes_notes_apps_Deployment). A quiesce suspends a Kustomization only when that list holds the workload. | Flux kustomize-controller inventory docs; internal/runs/quiesce.go:226-240 |
| spec.suspend: true stops kustomize-controller from scaling a stopped workload back up. | internal/runs/quiesce.go:22-25 |

### VolSync 0.16.0

| Behaviour | Source |
| --- | --- |
| A sync is in progress while status.lastSyncStartTime is set. | internal/controller/statemachine/machine.go:162-174 |
| When a sync completes, VolSync copies whatever spec.trigger.manual holds at that moment into status.lastManualSync. It writes lastManualSync only then, and starts a new sync only when the manual tag differs from it. | statemachine/machine.go:213-214, 225-242 |
| VolSync writes latestMoverStatus and lastManualSync in the same status update. | mover/restic/mover.go:648-650; statemachine/machine.go:103-118, 185-215 |
| The mover Job has backoffLimit 8. At 8 failed pods VolSync writes the logs into status.latestMoverStatus with result Failed, deletes the Job with background propagation, and creates a new Job on the next reconcile. A failing mover is retried forever. | mover/restic/mover.go:355-356, 609-618 |
| VolSync creates the clone volsync-<source>-src once per sync, and every Job and pod of that sync reuses it. Only Cleanup after a completed sync deletes the clone. | mover/restic/mover.go:171-188, 270-294 |
| The ReplicationSource is the controller owner of its Job, so deleting the source deletes the running mover pod. | mover/restic/mover.go:347 |
| Each reconcile rebuilds the Job; a change to spec.restic.unlock, the retention or the affinity alters the pod template and kills a running mover. | mover/restic/mover.go:346, 362, 370-380, 404 |
| A prune falls due pruneIntervalDays after lastPruned, or after the source's creation when lastPruned is empty. | mover/restic/mover.go:378, 656-667 |
| When spec.restic.unlock differs from status.restic.lastUnlocked, every mover pod runs restic unlock before backup. lastUnlocked is written only after a Job succeeds. | mover/restic/mover.go:373-376, 631-635, 669-674 |
| The restore Job is named volsync-dst-<destination>, and VolSync puts no finalizer on a ReplicationDestination. | mover/restic/mover.go:333-341 |
| enableFileDeletion sets RESTORE_OPTIONS=--delete, so a restore removes every file the snapshot lacks. | mover/restic/mover.go:396-398, 409 |
| VolSync pins a mover to the node of a Running (or else Pending) pod that uses the claim, skipping its own pods. | utils/affinity.go:57-92 |
| The logs of a successful mover pass a filter that keeps lines such as "restoring", "No eligible", "ERROR" and "Restic completed in" and drops "Selected restic snapshot with id". The filtered log keeps its last 1024 bytes (MOVER_LOG_MAX_BYTES). | mover/restic/logfilter.go:26-43; utils/podlogs.go:40, 177-228 |

The mover script, mover-restic/entry.sh in the same module:

| Behaviour | Source |
| --- | --- |
| entry.sh runs under set -e and is the pod's PID 1 with no trap, so a deleted pod kills restic by SIGKILL and restic leaves its lock. | entry.sh:7; mover/restic/mover.go:486 |
| A backup mover runs restic forget with the source's retention after every backup, and prune on its interval. | entry.sh:154-167, 371-379 |
| Its unlock step runs plain restic unlock (--remove-all is commented out), which removes only stale locks. | entry.sh:173-174 |
| The restore mover keeps only snapshot lines containing /data, maps each whole-second epoch to the last snapshot listed with that epoch, and picks the newest epoch at or before RESTORE_AS_OF, then steps back SELECT_PREVIOUS. restic.AtOrBefore reproduces this selection. | entry.sh:265-320, 284-306 |
| With no snapshot in reach, the mover prints "No eligible snapshots found" and "=== No data will be restored ===" and exits 0, so VolSync reports success for a restore that wrote nothing. | entry.sh:334-342 |
| Otherwise it prints "Selected restic snapshot with id: <id>" and runs restic restore. | entry.sh:346-350 |

### restic

The line numbers are from restic 0.19.1. The mover runs 0.18.1, and the
conformance tests replay repositories recorded with both versions.

| Behaviour | Source |
| --- | --- |
| backup and restore acquire a shared lock, and forget acquires an exclusive lock. | cmd/restic/cmd_backup.go:513; cmd/restic/cmd_forget.go:189; cmd/restic/cmd_restore.go:134 |
| An exclusive lock is refused while any other lock exists, stale or not; forget then exits 11 with "repository is already locked". | internal/restic/lock.go:160-216 (the branch at 188) |
| restic does not retry a lock by default. | cmd/restic/global.go:108 |
| A lock is stale when it is older than 30 minutes, or when it names this host and a dead PID. A mover pod never shares a hostname with a dead one, so for movers only the 30 minutes apply. | internal/restic/lock.go:252-288 |
| A live restic refreshes its lock every 5 minutes and exits when it cannot. | internal/repository/lock.go:29, 37; internal/restic/lock.go:309-331 |
| restic unlock removes only stale locks; unlock --remove-all removes every lock. | cmd/restic/cmd_unlock.go:45-65; internal/repository/lock.go:274-294 |
| restic snapshots sorts by full time with an unstable sort. | cmd/restic/cmd_snapshots.go:100; internal/restic/snapshot.go:260 |
| restic rewrite records the old snapshot as original and can change the tree. The controller's retime writes the same fields. | cmd/restic/cmd_rewrite.go:252-254 |

### Kueue 0.19.5

| Behaviour | Source |
| --- | --- |
| The chart's pod webhook mpod.kb.io has failurePolicy Fail when the pod integration is on (prod turns it on), and its namespaceSelector is managedJobsNamespaceSelector (kueue-managed: "true" in prod). While Kueue's webhook is down, the API server refuses every pod create in those namespaces, mover pods included. | chart templates/webhook/manifests.yaml:307-340 |
| With manageJobsWithoutQueueName false, Kueue gates only objects that carry the kueue.x-k8s.io/queue-name label. | hack/e2e/kueue/values.yaml (prod's settings) |
| A BackupRun's Workload in kueue.x-k8s.io/v1beta2 is admitted against its LocalQueue; Kueue reads the pod template to count quota and never runs its image. | internal/runs/kueue.go:21-28 |

### CloudNativePG 1.30.0

| Behaviour | Source |
| --- | --- |
| After initdb, the instance creates the marker file .check-empty-wal-archive, and removes it only once the ContinuousArchiving condition is True. | pkg/management/postgres/initdb.go:355-360; internal/management/controller/instance_controller.go:1143-1157 |
| While the marker exists and the Cluster lacks cnpg.io/skipEmptyWalArchiveCheck: enabled, the archiver asks the plugin to check that the archive is empty. | pkg/utils/labels_annotations.go:532-534; pkg/management/postgres/archiver/archiver.go:312-319 |
| A pg_basebackup bootstrap creates the same marker after cloning. | internal/cmd/manager/instance/pgbasebackup/cmd.go:151-156 |
| A failing archive_command sets ContinuousArchiving False with reason ContinuousArchivingFailing, and pg_wal grows until the volume is full. | pkg/management/postgres/webserver/local.go:264-272 |

### plugin-barman-cloud 0.15.0 and barman 3.20.0

| Behaviour | Source |
| --- | --- |
| The plugin runs barman-cloud-check-wal-archive before archiving each segment while the marker exists, and passes no --timeline. | plugin internal/cnpgi/common/wal.go:158-178, 239-254; internal/cnpgi/common/check.go:32-53 |
| barman-cloud-check-wal-archive lists <server>/wals/ and fails with "Expected empty archive" on any WAL file. A prefix holding only FAILED base backups and no WAL passes. | barman src/barman/clients/cloud_check_wal_archive.py:60-65; src/barman/cloud.py:2421-2446; src/barman/xlog.py:567-574 |
| barman uploads each segment to <server>/wals/<hash dir>/<name> unconditionally, so a new database on timeline 1 overwrites an old archive's segments. | src/barman/clients/cloud_walarchive.py:308-333 |
| barman names a backup directory by its start time and marks it STARTED, then FAILED or DONE. | src/barman/cloud.py:1629, 1703-1707, 1751-1768 |
| Retention classes a non-DONE backup as NONE and deletes only OBSOLETE ones, so barman never deletes a FAILED backup. | src/barman/retention_policies.py:190-218; src/barman/clients/cloud_backup_delete.py:424-428 |
| A base/<id>/ directory without backup.info counts as no backup. | src/barman/cloud.py:2541-2551; src/barman/cloud_providers/aws_s3.py:511-514 |
| A declared recovery fails at start when the target archive is not empty. | plugin internal/cnpgi/restore/restore.go:105-119, 249-294 |

### lib-volume-populator v3.3.0

| Behaviour | Source |
| --- | --- |
| The library reads the data source from its informer cache first. On NotFound it records PopulatorDataSourceNotFound and returns, before any other step, so without the VolumeRestore it never removes its own claim finalizer. | controller.go:662-671 |
| A missing StorageClass, or a WaitForFirstConsumer class with no volume.kubernetes.io/selected-node on the claim, also returns nil. | controller.go:677-703 |
| For a claim being deleted, the library skips population and runs cleanup. | controller.go:743, 976 |
| Population creates the prime claim, adds the claim finalizer, and calls Populate only once the prime has spec.volumeName. | controller.go:779, 791, 804-809 |
| Cleanup runs PopulateCleanupFn, deletes the prime, then removes the claim finalizer; each step returns on error and the claim is requeued. | controller.go:980-1008 |
| The prime is named "prime-" + the claim's UID, and the claim finalizer is <Prefix>/populate-target-protection (backup.wlz.li/populate-target-protection here). | controller.go:67, 69, 720; cmd/backup-controller/main.go:42 |

### cert-manager 1.21.2

| Behaviour | Source |
| --- | --- |
| A self-signed Issuer issues and renews the webhook's serving certificate, and the cainjector copies its CA into the MutatingWebhookConfiguration that carries cert-manager.io/inject-ca-from. The controller pod reads the renewed tls.crt and tls.key without a restart. | deploy/webhook.yaml:1-30, 71; chart/templates/deployment.yaml:57-59 |

### RustFS 1.0.0

| Behaviour | Source |
| --- | --- |
| ListObjectsV2 honours prefix, delimiter and max-keys, and GET returns backup.info bodies as barman wrote them. The webhook's S3Prober and restic's S3 backend depend on those answers. | internal/testinfra/s3fake (replays of recorded RustFS answers) |

## When infra bumps a dependency

### Step 1: Change the pin

Change the component's entry in versions.json, with its hashes, and every
hack/e2e/<component>/pins.json that names it.

```
nix develop -c go test ./internal/testinfra/versions/
```

Expected result: `ok`. A failure names the pins.json value that disagrees
with versions.json.

### Step 2: Re-record the fixtures

```
make fixtures
```

This re-records the restic repositories, the barman stores with RustFS's
answers, and the pinned third-party CRDs. Until it has run, the tests that
check provenance (TestTheRecordingsCameFromThePinnedTools) fail.

### Step 3: Run the unit and envtest suites

```
make check
```

```
make envtest
```

Expected result: both pass. A failing conformance or recorded-store test
means the new version behaves differently from a row in this doc.

### Step 4: Run the e2e suites

```
make e2e-fetch e2e-up e2e
```

e2e-fetch downloads the new images and charts, e2e-up installs every component
into the docker-desktop cluster and runs its smoke check, and e2e runs every
test tagged e2e.

Expected result: every component's check and every test passes.

### Step 5: Re-read the bumped component's source

Open the new version's source at each file and line listed for that component
above, and check that each behaviour still holds. Update the line numbers in
this doc as you go. The tests cover only part of each table, so this step is
required even when steps 3 and 4 pass.

### Step 6: Fix and release

Change the controller where a behaviour moved, add a test that fails on the
old code, and release. A release requires `make e2e` passing locally.

## Which tests catch which behaviour

| Test | Behaviours it checks | Runs in |
| --- | --- | --- |
| internal/restic/conformance_test.go | restic's snapshot listing, the entry.sh snapshot pick, retime against restic rewrite, the killed mover's lock and forget's exit 11, lock staleness | make check, against repositories recorded with restic 0.18.1 and 0.19.1 |
| internal/testinfra/barmanstore/store_test.go | barman-cloud-check-wal-archive's verdict on each recorded store, and the S3Prober against it | make check, against stores recorded with barman 3.20.0 and RustFS |
| internal/testinfra/s3fake replay tests | RustFS's answers to listing and GET, and the S3Prober's requests | make check |
| internal/testinfra/versions | versions.json against every e2e pins.json | make check |
| internal/testinfra/strictclient differential suites | the API server's pruning, defaulting, finalizers and garbage collection, strict client against the real one | make envtest (TestDifferentialAgainstEnvtest, Kubernetes 1.36.3) and make e2e (TestDifferentialAgainstCluster, Kubernetes 1.36.4) |
| internal/populator/orphans_envtest_test.go | the populator library's data-source lookup order: a claim whose VolumeRestore was deleted while the prime was Pending stays Terminating until the controller releases it | make envtest |
| test/e2e | a BackupRun against real VolSync, restic, Kueue and RustFS | make e2e |

The VolSync state machine, the CloudNativePG marker file, Kueue's webhook and
the Flux inventory rows have no recorded fixture. Only the e2e run in step 4
and the source reading in step 5 check them.
