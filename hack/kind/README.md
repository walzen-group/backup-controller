# Test cluster

The end-to-end scenarios under test/e2e run on the kind cluster of Docker
Desktop, kubectl context docker-desktop. They drive the controller the way
a user does and check the data itself: the file in a claim, the rows in a
database. No part of Kubernetes, S3, restic, VolSync or CloudNativePG is
faked.

## Set up the cluster

1. Install the stack. Each step is safe to run again.

   ```
   hack/kind/up.sh
   ```

   Expected result: the last line reads `done: csi certmanager s3 cnpg volsync kueue flux cluster`.

2. Build the controller from this tree and deploy it.

   ```
   hack/kind/controller.sh deploy
   ```

   Expected result: the last line names the image it runs,
   `running backup-controller:dev-<id>`.

Run step 2 again after every change to the code. It builds a new image, and
the Deployment rolls to it.

## Run the scenarios

All of them:

```
make e2e
```

One of them:

```
go test -tags e2e -count=1 -timeout 60m -v ./test/e2e/ -run TestADeletedClusterComesBackWithItsData
```

A failed scenario prints the runs, Clusters, claims, pods and events of its
namespace and the end of the controller's log. Each scenario deletes its
namespace when it ends.

## Run the scenarios a change touches

The whole suite runs for about 40 minutes and the load scenario for about
15, so a change runs only the scenarios that exercise the code it changes,
plus the scenarios it adds or edits.

```
make e2e-affected
```

The target compares the working tree with the newest version tag (set BASE=
for another base), and hack/kind/affected.sh picks the scenarios from the table
below:

| Change | Scenarios |
| --- | --- |
| a function in a Go file, with a row for file:function | that row |
| any other change in a Go file | every row whose path the file starts with |
| only the import block of a Go file | none; the functions that use the import changed too |
| a Test function in test/e2e | that Test function |
| a helper in test/e2e | the Test functions that call it, directly or through other helpers |
| docs/, Markdown, or a row that reads none | none |
| a path no row matches | the smoke row, with a warning |

TestSeventyNamespacesBackUpAtOneTick measures the controller's memory under
load. Only the rows that change what the controller holds in memory or how
much work it does at once name it, and a changed test helper never selects
it; a change to logic alone never runs it.

<!-- affected:start -->
| Path | Scenarios |
| --- | --- |
| smoke | TestABackupPausesTheMarkedAppAndTagsTheSnapshot, TestAClaimRestoresInPlace, TestThePopulatorFillsARecreatedClaim, TestTheWebhookRefusesASecondClusterOnOneArchive |
| .github/ | none |
| .golangci.yaml | none |
| Makefile | none |
| flake.nix | none |
| flake.lock | none |
| nix/ | none |
| Dockerfile | smoke |
| .dockerignore | smoke |
| cmd/backup-controller/ | smoke |
| cmd/backup-controller/main.go:populatorQueue | TestAPopulatorRestoreWaitsForTheQueue |
| deploy/ | smoke |
| chart/ | smoke |
| config/crd/ | smoke |
| go.mod | smoke |
| go.sum | smoke |
| hack/kind/ | smoke |
| internal/api/ | smoke |
| internal/admission/ | TestTenNamespacesShareOneQueue, TestARunWithoutALocalQueueWaitsForOne, TestARestoredDatabaseHoldsWhatWasArchived |
| internal/lease/ | TestOneOfManyRunsTakesALease, TestABackupAndARestoreOfOneClaimTakeTurns, TestTwoRestoresOfOneClusterDoNotFight |
| internal/runs/leases.go | TestABackupAndARestoreOfOneClaimTakeTurns, TestTwoRestoresOfOneClusterDoNotFight |
| internal/runs/queued.go | TestARunWithoutALocalQueueWaitsForOne |
| internal/runs/backuprun.go:Reconcile | TestTheControllerWithPauseFinishesStartedRunsAndStartsNoNewOne, TestABackupPausesTheMarkedAppAndTagsTheSnapshot |
| internal/runs/backuprun.go:admit | TestARunWithoutALocalQueueWaitsForOne, TestTenNamespacesShareOneQueue |
| internal/runs/backuprun.go:endIfEvicted | TestABackupPausesTheMarkedAppAndTagsTheSnapshot |
| internal/runs/backuprun.go:release | TestABackupPausesTheMarkedAppAndTagsTheSnapshot |
| internal/runs/restorerun.go:Reconcile | TestTheControllerWithPauseFinishesStartedRunsAndStartsNoNewOne, TestARestoredDatabaseHoldsWhatWasArchived |
| internal/runs/restorerun.go:admit | TestARestoredDatabaseHoldsWhatWasArchived |
| internal/runs/restorerun.go:endIfEvicted | TestARestoredDatabaseHoldsWhatWasArchived |
| internal/runs/restorerun.go:release | TestARestoredDatabaseHoldsWhatWasArchived, TestAClaimRestoresInPlace |
| internal/runs/backuprun.go | TestABackupPausesTheMarkedAppAndTagsTheSnapshot, TestAKilledControllerFinishesThePausedBackup, TestARunPastItsTimeoutGivesTheAppBack, TestARebuildAfterAFailedDatabaseBackupComesBackToTheLastPausedMoment |
| internal/runs/pause.go | TestABackupPausesTheMarkedAppAndTagsTheSnapshot, TestAClaimRestoresInPlace |
| internal/runs/sources.go | TestABackupPausesTheMarkedAppAndTagsTheSnapshot, TestAFailedBackupResumesTheApp |
| internal/runs/trigger.go | TestABackupPausesTheMarkedAppAndTagsTheSnapshot |
| internal/runs/schedule.go | TestTenNamespacesShareOneQueue |
| internal/runs/namespace_settings.go | TestAnInvalidTimeoutDuringAPauseGivesTheAppBack |
| internal/runs/cnpg.go | TestARestoredDatabaseHoldsWhatWasArchived, TestARebuildBringsAHibernatedDatabaseBackWithItsLastRows |
| internal/runs/restorerun.go | TestAClaimRestoresInPlace, TestARestoredDatabaseHoldsWhatWasArchived, TestTwoRestoresOfOneClusterDoNotFight, TestAKilledControllerFinishesTheDatabaseRestore |
| internal/runs/restore_items.go | TestAClaimRestoresInPlace, TestASnapshotRestoresIntoANewClaim, TestADatabaseRestoreKeepsTheAppPausedUntilTheOldClusterIsGone |
| internal/runs/restore_select.go | TestADatabaseRestoreToAMomentLandsOnTheBaseBackupBeforeIt, TestASyncedRestoreBringsTheFileAndTheRowsBackToThePausedMoment, TestASyncedRestoreOfAnIdleAppMatchesTheBackupThatPausedIt |
| internal/runs/ | TestABackupPausesTheMarkedAppAndTagsTheSnapshot, TestAClaimRestoresInPlace |
| internal/bootstrap/instance.go | TestADeletedClusterComesBackWithItsData, TestADatabaseRestoreKeepsTheAppPausedUntilTheOldClusterIsGone |
| internal/bootstrap/ | TestTheWebhookRefusesASecondClusterOnOneArchive, TestTheWebhookRefusesAnEmptyDatabaseOverAnArchiveWithoutABackup, TestAnOptedOutClusterStartsEmptyOnlyOverAnEmptyArchive, TestADeletedClusterComesBackWithItsData, TestARebuiltNamespaceComesBackToThePausedMoment, TestAClusterDeletedAfterARebuildKeepsTheRowsWrittenSince, TestARebuildWithTheClaimsFilledFirstComesBackToThePausedMoment |
| internal/synced/ | TestARebuiltNamespaceComesBackToThePausedMoment, TestARebuildComesBackToTheNewestBackupWhenARepositoryIsStale |
| internal/populator/ | TestThePopulatorFillsARecreatedClaim, TestAFailedPopulatorRestoreRunsAgainOnceItsJobIsDeleted, TestAPopulatorRestoreWaitsForTheQueue |
| internal/restorejob/ | TestAClaimRestoresInPlace, TestThePopulatorFillsARecreatedClaim |
| internal/volsync/ | TestThePopulatorFillsARecreatedClaim |
| internal/restic/ | TestABackupPausesTheMarkedAppAndTagsTheSnapshot, TestAClaimRestoresInPlace, TestThePopulatorFillsARecreatedClaim |
| internal/restic/keycache.go | TestSeventyNamespacesBackUpAtOneTick |
| internal/restic/repository.go:snapshotFiles | TestSeventyNamespacesBackUpAtOneTick, TestABackupPausesTheMarkedAppAndTagsTheSnapshot |
| internal/restic/s3.go:open | TestSeventyNamespacesBackUpAtOneTick, TestABackupPausesTheMarkedAppAndTagsTheSnapshot |
| test/e2e/harness_test.go | smoke |
| test/e2e/app_test.go | smoke |
<!-- affected:end -->

## What the cluster holds

| Component | Version | Why |
| --- | --- | --- |
| csi-driver-host-path, external-snapshotter | v1.18.0, v8.6.0 | claims, snapshots and clones for VolSync (prod runs zfs-localpv) |
| cert-manager | v1.21.2 | the certificate of the controller's webhook |
| RustFS | 1.0.0 | S3, namespace s3, buckets volsync and postgres |
| CloudNativePG, barman-cloud plugin | 1.30.0, v0.15.0 | the databases and their archives |
| VolSync | 0.16.0 | the backup movers, with restic 0.18.1 |
| Kueue | 0.19.5 | ClusterQueue backup, which admits 5 runs at once |
| Flux | v2.9.5 | applies each test app from the in-cluster registry |
| registry | 3.0.0 | holds the test apps as OCI artifacts |

hack/kind/versions.env pins these versions for the test cluster only; the
controller itself works with the versions the cluster serves.
