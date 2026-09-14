# task-3: the populator against a fake

## Context

Milestone 3 of `docs/implementation-plan.md`, and the task that carries the
design's weight: `docs/architecture.md` names the three provider callbacks and
what each one does, and `docs/decisions.md` records why the callbacks are used in
place of a populator pod. This task writes two packages:

- `internal/volsync`, pure functions that build a ReplicationDestination from a
  `VolumeRestore` and a prime claim, build the repository Secret copy, and read
  completion from the destination's status. Structs in, structs out, no client.
- `internal/populator`, the three callbacks behind a narrow interface for the
  Kubernetes operations, so the tests run against a fake.

The library's real signatures are in
`.cortex/plans/2026-09-14-backup-controller/library-contract-findings.md`, written
by task 0. Read it first. Where it reports `DIFFERENT`, the finding wins over
`docs/architecture.md`; where it reports `unanswered`, say so in your report and
use the closest thing the source does specify.

The reference for Go style and test shape is `/home/nixos/repos/kuport`, which
has table tests over an interface in `internal/reconcile/` and goldens under
`internal/datapath/testdata/`.

## Target

- `internal/volsync/volsync.go`
- `internal/volsync/secret.go`
- `internal/volsync/volsync_test.go`
- `internal/populator/populator.go`
- `internal/populator/populator_test.go`
- `go.mod` and `go.sum`: the library dependency and, for the typed destination,
  `github.com/backube/volsync/api/v1alpha1`

Non-goals: `cmd/`, `Dockerfile`, `internal/api/` (task 2 owns the types),
`deploy/`, `chart/`, `config/`. No envtest: this task's proof is the fake, and
the API-server proof belongs to task 4.

## Change

### `internal/volsync`

```go
// Trigger is the value the controller puts in spec.trigger.manual and later
// compares against status.lastManualSync.
func Trigger(claim *corev1.PersistentVolumeClaim) string

// New builds the ReplicationDestination that restores into the prime claim.
func New(vr *backupv1alpha1.VolumeRestore, claim *corev1.PersistentVolumeClaim,
	primeClaim string, namespace string) *volsyncv1alpha1.ReplicationDestination

// Complete reports whether the destination has finished the sync this
// controller triggered.
func Complete(rd *volsyncv1alpha1.ReplicationDestination, trigger string) bool

// SecretCopy builds the repository Secret copy the destination resolves
// in the controller's namespace.
func SecretCopy(repo *corev1.Secret, claimUID types.UID, namespace string) *corev1.Secret
```

`New` fills, with names taken from the design:

| Destination field | Value | Source |
| --- | --- | --- |
| `metadata.name` | `restore-<claim uid>` | `docs/architecture.md`'s object flow |
| `metadata.namespace` | the controller's namespace | `docs/decisions.md` |
| `spec.copyMethod` | `Direct` | `docs/overview.md`'s five steps |
| `spec.destinationPVC` | the prime claim's name | same |
| `spec.trigger.manual` | the claim's UID | `docs/architecture.md` |
| `spec.restic.repository` | `vr.Spec.Repository` | `docs/api.md`'s field table |
| `spec.restic.restoreAsOf` | `vr.Spec.RestoreAsOf` | same |
| `spec.restic.moverPodLabels` | `vr.MoverLabels()`, which returns the `map[string]string` VolSync's field takes | same |
| `spec.restic.moverSecurityContext` | `vr.Spec.MoverSecurityContext` | same |

`SecretCopy` names the copy with the claim's UID and places it in the
controller's namespace, carrying the repository Secret's `Data` and `Type`
(`docs/decisions.md`: the copy exists for the length of one restore, and its name
comes from the claim's UID so two restores never collide).

`Complete` compares `rd.Status.LastManualSync` with the trigger. Finding (j) is
CONFIRMED: on a repository holding no snapshot, VolSync prints no eligible
snapshots, exits success and still updates `status.lastManualSync`, so the
comparison covers that run and the function needs no branch of its own. Cover the
empty, equal and different cases in the table test.

Finding (k) is CONFIRMED: VolSync records a failed mover as
`status.latestMoverStatus.result: Failed`, so implement:

```go
// Failure reports whether the destination's mover has failed, and the reason
// string this controller copies into its own status.
func Failure(rd *volsyncv1alpha1.ReplicationDestination) (reason string, failed bool)
```

reading that field. The claim stays Pending either way, and the `VolumeRestore`
carries `RestoreFailed`.

Dependency choice: the library pin from task 0 is
`github.com/kubernetes-csi/lib-volume-populator/v3 v3.3.0`, and VolSync is
`github.com/backube/volsync v0.16.0`. Use the typed
`github.com/backube/volsync/api/v1alpha1` if it builds against the `k8s.io/*`
versions task 2 pinned. If it drags an incompatible `k8s.io` release train into
`go mod tidy`, say so in the report with the version numbers and carry
`unstructured.Unstructured` with the GVR instead.

### `internal/populator`

One interface for everything the callbacks do to the cluster, so the tests fake
the cluster and nothing else:

```go
type Operations interface {
	GetReplicationDestination(ctx context.Context, namespace, name string) (*volsyncv1alpha1.ReplicationDestination, error)
	CreateReplicationDestination(ctx context.Context, rd *volsyncv1alpha1.ReplicationDestination) error
	DeleteReplicationDestination(ctx context.Context, namespace, name string) error
	GetSecret(ctx context.Context, namespace, name string) (*corev1.Secret, error)
	CreateSecret(ctx context.Context, secret *corev1.Secret) error
	DeleteSecret(ctx context.Context, namespace, name string) error
	SetStatus(ctx context.Context, vr *backupv1alpha1.VolumeRestore) error
}
```

Add `GetVolumeRestore` when task 0's finding (e) says the callbacks receive only
a reference to the data source object.

The three functions match the library's callback signatures from task 0's
findings, and behave like this:

| Callback | Behavior |
| --- | --- |
| Populate | read the app's repository Secret in the app's namespace, create the copy in the controller's namespace, build the destination from `internal/volsync`, create it when absent, and report `Restoring` on the `VolumeRestore` status with the destination's name in the condition message |
| Complete | return `volsync.Complete(...)` for the destination, returning false with a `RestoreFailed` condition on the `VolumeRestore` when `volsync.Failure(...)` reports one |
| Cleanup | delete the destination and the Secret copy, tolerating `NotFound` on both |

The behaviors `docs/architecture.md`'s failure table promises, and what proves
each:

| Case | Requirement |
| --- | --- |
| the controller restarts mid-restore | Populate called twice with the same claim leaves exactly one destination, unmodified, so a running mover is not disturbed |
| the destination already exists | reuse it; do not create or update it |
| the app's claim is deleted mid-restore | cleanup tolerates `NotFound` for both objects |
| the restore fails | Complete returns false and the app's claim stays Pending; the `VolumeRestore` carries `RestoreFailed` |

Status writes go through task 2's helper and constants: `Ready` False with reason
`Restoring` while filling, `Ready` False with reason `RestoreFailed` on a failed
mover, and one `claims[]` entry carrying the claim's name, UID, `Restoring` or
`Failed` phase, and the time the restore started.

### Tests

Table tests over a fake `Operations`. The cases `docs/implementation-plan.md`
names, plus the boundary cases the behaviors above need:

| Test | Case |
| --- | --- |
| `TestPopulateCopiesRepositorySecret` | the secret appears in the controller's namespace with the repository's data |
| `TestPopulateNamesFromClaimUID` | destination `restore-<uid>` and secret `<uid>` for a claim whose UID is known |
| `TestPopulateReusesExistingDestination` | a second Populate with the destination present creates nothing and leaves it byte-identical |
| `TestPopulateReportsRestoring` | the status write carries phase `Restoring` and the destination's name |
| `TestCompleteTriggerMatching` | `lastManualSync` equal, different, and empty |
| `TestCompleteReportsFailedMover` | when `Failure` reports a failure, Complete returns false and the status carries `RestoreFailed` (skip and record the case if `Failure` does not exist) |
| `TestCleanupDeletesDestinationAndSecret` | both deleted |
| `TestCleanupToleratesMissingObjects` | both already gone, no error |
| `TestNewPassesThroughSpecFields` | all four `VolumeRestore` fields land on the destination, `copyMethod` is `Direct`, `destinationPVC` is the prime claim |

`TestNewPassesThroughSpecFields` is the resource's contract: those four fields
exist so a claim can reach those four ReplicationDestination fields, and a
`VolumeRestore` whose `restoreAsOf` never reaches the destination is a silent
data-loss bug. Write it as one assertion per field, and write the empty cases
too: a `VolumeRestore` with only `repository` set must leave the other three
fields nil rather than writing zero values into the destination.

Do not test that a mock was called. Assert what the API server would receive and
what the `VolumeRestore` status would hold.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- The library version comes from task 0's findings; do not pick your own.
- Everything the callbacks do to the cluster goes through `Operations`. No
  concrete client in either package.
- Do not modify `internal/api/`. If a type or constant there does not fit, report
  it rather than editing past your spec.
- `go mod tidy` must leave a clean `go.sum`.

## Acceptance

From `/home/nixos/repos/backup-controller`:

```sh
nix develop -c go build ./...
nix develop -c go vet ./...
nix develop -c golangci-lint run
nix develop -c go test ./... -race -v 2>&1 | tee .tmp/task3-tests.log
grep -c '^--- PASS' .tmp/task3-tests.log
nix develop -c go mod tidy && git status --short go.mod go.sum
```

Expected: every test above appears as a `PASS` line in `.tmp/task3-tests.log`,
the suite is green under `-race`, and `go mod tidy` changes nothing.

The end-to-end behavior of these callbacks against a real API server is task 4's
envtest proof, and the claim rebind with a real mover is task 6's runbook. This
task's gate is the fake, and the report says so.

## Emission discipline

Spend at most 20 minutes reading task 0's findings and `docs/architecture.md`,
then write code. Every 10 minutes of work must leave a file on disk.

## Report to

`meta-w3`, one `A2A:` steer: files written, `git status --short`, the pinned
library version, which of the three option decisions you took and why (typed
destination or unstructured, `Failure` present or absent, the empty
`lastManualSync` branch), the test names with their pass lines, and any
deviation. Then stop.
