# backup-controller: whole-branch review

Date: 2026-09-14. Scope: the whole working tree of
/home/nixos/repos/backup-controller against the design record in docs/.

## The answer

The tree implements milestones 1 through 5 and every gate passes. Five defects
were found and repaired during this review, four of them blocking: the module
did not compile, its ReplicationDestination named a repository Secret that would
never resolve on a cluster, a test fixture dereferenced a nil map entry, and a
finished restore left its VolumeRestore reporting Ready False forever. Each fix
carries a test that fails without it.

Two of the twelve checks produced no finding at all and are listed as clean
below. Nothing outstanding blocks the cluster run in
[the runbook](2026-09-14-backup-controller-cluster-runbook.md).

The review found no evidence that any task report overstated what the tree
holds, with one exception recorded under check 12: wave 3 delivered code that
did not build, so its work had never run.

## Commands run, with results

Every command ran from the repository root in the flake's dev shell.

| Command | Result |
| --- | --- |
| `nix develop -c go vet ./...` | exit 0, silent |
| `nix develop -c go test ./... -race -count=1` | exit 0, internal/populator and internal/volsync ok |
| `nix develop -c golangci-lint run` | exit 0, `0 issues.` |
| `nix develop -c go build ./...` | exit 0, silent |
| `nix develop -c make verify` | exit 0, `diff -ru config/crd .tmp/crd` prints nothing |
| `nix develop -c kustomize build deploy/` | exit 0, render written to .tmp/render-deploy.yaml |
| `nix develop -c helm lint chart/` | exit 0, 1 chart linted, 0 failed |
| `nix develop -c hadolint Dockerfile` | exit 0, silent |
| `nix develop -c actionlint` | exit 0, silent |
| `KUBEBUILDER_ASSETS=... go test -tags envtest ./internal/api/...` | exit 0, 6.573s, the CRD validation suite passes against a real API server |
| `kubectl-validate` on the deploy render, the chart render and the digest-pinned render | all three print OK |
| `bash .tmp/envtest/run.sh` | exit 0, `ALL OBSERVATIONS MATCH`, SIGTERM exit status 0, no panic |

The kubectl-validate binary came from the nixpkgs revision in flake.lock,
ef34387ddd751e1ab8857adf4676492d32eb24ec.

## Findings

### 1. The destination named a repository Secret that does not exist in its namespace

Rank: blocking. Repaired, with a test.

internal/volsync/volsync.go set the ReplicationDestination's
`spec.restic.repository` to `vr.Spec.Repository`, which is the Secret's name in
the app's namespace. The destination lives in the controller's namespace, and
VolSync resolves that field there:
[docs/architecture.md:108](../../docs/architecture.md) states it, and
[docs/decisions.md:59](../../docs/decisions.md) records the copy that exists
because of it. The copy is named for the claim UID, so the destination has to
name the UID.

On a cluster every restore would have failed with VolSync unable to find the
Secret. No offline gate caught it, because the fake never resolved the name.

The field now reads `SecretCopyName(types.UID(trigger))` at
internal/volsync/volsync.go:39, and internal/volsync/secret.go gained
SecretCopyName so the copy's name has one definition that both the writer and
the reader use. The assertion in
internal/volsync/volsync_test.go:50 checks the copied name, and the envtest run
observes it end to end: `the repository the destination resolves =
c4c02f23-b562-4a56-b7d1-154bcfc3807c`.

### 2. A finished restore never reported Ready

Rank: blocking. Repaired, with two tests.

`Cleanup` deleted the destination and the Secret copy and then returned without
writing status, so the claims[] entry and the Ready False condition that
`Populate` wrote stayed on the object forever. The constant ReasonRestored at
internal/api/v1alpha1/conditions.go:24 was declared and never referenced
anywhere, which is what surfaced it.

[docs/api.md](../../docs/api.md)'s status table says the Ready condition is
"False while any claim naming this object is being filled, True when none is",
and that claims[] holds "one entry per claim currently being populated". The
same page states that a restore that finished leaves no entry. A Flux
Kustomization with `wait: true` gates on that condition, so an object stuck at
Ready False holds the Kustomization open after the restore has succeeded.

`Cleanup` now retires the claim's entry and sets the condition at
internal/populator/populator.go:122-138, with retireClaimStatus at
internal/populator/populator.go:187. Two tests cover it:
TestCleanupRetiresTheClaimAndReportsRestored asserts Ready True with reason
Restored and an empty claims list, and TestCleanupLeavesOtherClaimsRestoring
asserts that retiring one of two claims leaves the other reported and the object
not Ready. Both fail against the previous code.

### 3. The module did not compile

Rank: blocking. Repaired.

Wave 3 left the tree in a state where `go build ./...` failed three ways: go.sum
had no entries for the two new dependencies, `MoverPodLabels` and
`MoverSecurityContext` were written as direct fields of
ReplicationDestinationResticSpec when VolSync declares them on the embedded
MoverConfig struct
(/home/nixos/go/pkg/mod/github.com/backube/volsync@v0.16.0/api/v1alpha1/common_types.go:152-165),
and `vr.MoverLabels()` was called on the VolumeRestore when the helper is on
VolumeRestoreSpec (internal/api/v1alpha1/volumerestore_types.go:57).

`go mod tidy` filled go.sum, and the literal now nests MoverConfig at
internal/volsync/volsync.go:41. The wave-2 hand-back had already flagged the
MoverLabels receiver as an open item for the orchestrator; the correction never
reached the code.

### 4. A test fixture dereferenced a nil map entry

Rank: blocking. Repaired.

Three tests in internal/populator/populator_test.go read
`ops.destinations[...]` from a fixture that never created that entry, so
TestCompleteTriggerMatching panicked with a nil pointer dereference and the two
Cleanup tests asserted against an empty cluster. The fixture
restoringOperations at internal/populator/populator_test.go:260 now seeds the
copied Secret and the destination a completed Populate leaves behind, and the
three tests start from it.

TestCleanupDeletesDestinationAndSecret is the one that mattered: against the old
fixture it deleted nothing and still passed, because both deletes tolerate
NotFound.

### 5. The Ready message is a bare object name

Rank: note.

internal/populator/populator.go:77 sets the Ready condition's message to the
destination's name alone, such as `restore-c4c02f23`. The example in
[docs/api.md](../../docs/api.md) reads `waiting for ReplicationDestination
restore-3f2a1c7e in backup-system`, which tells an operator what the name is and
where to look for it. Task 3's spec asked only for "the destination's name in
the condition message", which the code satisfies, so this is a note rather than
a defect.

### 6. Two tests assert less than their names claim

Rank: note.

TestTriggerUsesClaimUID at internal/volsync/volsync_test.go:69 asserts a string
conversion and nothing else. Trigger's only branch, the nil claim, is untested,
so a plausible bug in it fails no test.

No test covers Populate's error path when the app's repository Secret is
missing, which is the first thing that goes wrong on a real cluster when the
backups component has not run yet. The behavior is correct in the code at
internal/populator/populator.go:51-54; nothing pins it.

Neither is worth blocking on. Both are cheap to add when someone next touches
the package.

### 7. A file exists that no task's Target listed

Rank: note.

cmd/backup-controller/operations.go holds the concrete client behind the
Operations interface. Task 3 defined that interface and task 4's Target named
only cmd/backup-controller/main.go, so no task in
[00-index.md](../plans/2026-09-14-backup-controller/00-index.md) owned the
implementation. Without it the interface has no production implementation and
the binary cannot be built, so the file is a gap in the plan rather than
unrequested work. It sits inside task 4's directory.

Two deviations from task 4's spec are recorded here rather than as findings.
The VolSync ReplicationDestination CRD comes from the module cache at the pinned
v0.16.0 rather than from an HTTP fetch of a GitHub tag, which is the same bytes
with go.sum verifying them and no network dependency, sha256
503cb28f7eafc73b9c3332cdc084aca3149f8520e3b265769caf02a47f51251d. The binary
does not call `ctrl.SetupSignalHandler()`, because RunControllerWithConfig
registers its own SIGTERM handler and closes the stop channel itself
(/home/nixos/go/pkg/mod/github.com/kubernetes-csi/lib-volume-populator/v3@v3.3.0/populator-machinery/controller.go:253-260);
a second closer on the same channel panics, which task 4's acceptance treats as
a failure. The reason is in a comment at cmd/backup-controller/main.go:57-62.

## The twelve checks

| # | Check | Result |
| --- | --- | --- |
| 1 | The three callbacks match architecture.md's table | finding 2. Populate and Complete hold; Cleanup did not report the end of a restore, now repaired |
| 2 | The prime claim keeps class, access modes, size and node, with no mutator | clean |
| 3 | The Secret copy is named from the claim UID, lives in the controller namespace, deleted in cleanup | holds, and finding 1 is the destination failing to read it back |
| 4 | The VolumeRestore fields match api.md one for one | holds, with the recorded MoverPodLabelValue deviation |
| 5 | The status shape matches api.md | findings 2 and 5 |
| 6 | The ClusterRole matches packaging.md's table | clean |
| 7 | The release workflow keeps its guards and produces four assets | holds |
| 8 | The rendered install is one asset with CRD, Namespace and workload | holds |
| 9 | Every "Before writing code" row is confirmed in the findings file | holds |
| 10 | The four "what would make this project wrong" items are ruled out or named unproven | holds |
| 11 | Test quality | findings 4 and 6 |
| 12 | Scope | finding 7, plus the wave-3 build breakage in finding 3 |

Checks 2 and 6 produced no finding.

### Check 2, in detail

The library creates the prime claim and copies AccessModes, Resources,
StorageClassName and VolumeMode, and copies
volume.kubernetes.io/selected-node for a WaitForFirstConsumer class
(populator-machinery/controller.go:719-785, confirmed in
[library-contract-findings.md](../plans/2026-09-14-backup-controller/library-contract-findings.md)
row d). cmd/backup-controller/main.go sets no MutatorConfig, so no hook changes
those fields. The envtest run read the prime claim back from the API server and
observed the app claim's storage class csi-standard, its size 1Gi, and no data
source on it.

### Check 6, in detail

deploy/rbac.yaml carries eight rules and
[docs/packaging.md](../../docs/packaging.md)'s table has eight rows. Every row
is present with the verbs the table names, and no rule grants anything the table
does not list. The secrets rule is `get, create, delete` as specified, and the
comment above it names the reason the grant cannot be narrowed.

### Check 9, in detail

All three rows of implementation-plan.md's "Before writing code" table are
CONFIRMED in the findings file: RunControllerWithConfig and
ProviderFunctionConfig with their three callbacks (row a), the PopulatorParams
fields (row b), and the prime claim's name, namespace and selected node (row d).
The two DIFFERENT findings are followed rather than worked around. The library
serves no health route, and deploy/deployment.yaml carries no probe with the
reason in a comment. ImageName belongs to PodConfig alone, and
cmd/backup-controller/main.go declares no image flag and passes no image name,
which contradicts milestone 4's own text asking for "the image name the library
wants even in provider-function mode". The finding wins, and
[docs/implementation-plan.md](../../docs/implementation-plan.md)'s milestone 4
line is now stale.

### Check 10, in detail

None of the four conditions is ruled out by the code, and all four are named as
unproven. The runbook gives each one a subsection with the command that would
show it, and the fourth, a large restore outrunning the library's timeouts, has
its own numbered step with a wall-time measurement.

## What this review could not check

| Gap | Why |
| --- | --- |
| the container image builds | no container daemon in this environment. hadolint is clean and the static linux/amd64 binary builds, `file` reporting an ELF 64-bit statically linked executable |
| the release workflow runs | it needs a tag and ghcr.io credentials. Its render, digest grep and chart packaging were exercised locally against a stand-in digest |
| a restore moves data | no VolSync controller runs offline, so nothing sets status.lastManualSync and the claim never rebinds. The runbook carries those rows |
