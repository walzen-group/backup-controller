# task-0: verify the library contract

## Context

`docs/implementation-plan.md` opens with a table of three facts the design rests
on, and says why they need checking: the plan was written from
`lib-volume-populator`'s master branch rather than from a tag, and three later
tasks are built on them. `docs/architecture.md` repeats the same warning: "Verify
the exact field names against the version pinned in go.mod before writing against
them". This task establishes those facts against source, with citations, before
task 3 and task 4 are written.

A finding that the source differs from the design is a successful outcome here.
`docs/implementation-plan.md` states the rule: "If any differs, say so before
building around it rather than working around it quietly." This task feeds the
orchestrator, which decides before task 3 is dispatched.

## Target

One file:
`.cortex/plans/2026-09-14-backup-controller/library-contract-findings.md`.

Scratch work happens under `.tmp/contract/`.

Non-goals: no repository source file changes of any kind. No `go.mod` at the
root, no package under `internal/`, no edit to `flake.nix` or the `Makefile`.
This task reads a library and writes one findings document.

## Change

### Step 1: choose the version to pin

```sh
git ls-remote --tags https://github.com/kubernetes-csi/lib-volume-populator.git
nix develop -c go list -m -versions github.com/kubernetes-csi/lib-volume-populator
```

Prefer the newest tagged release whose source satisfies the checks below. When
the repository carries no usable tag, pin the pseudo-version of a named commit
and say so in the findings; a pseudo-version is acceptable when it is written
down with its commit hash.

### Step 2: read the source

```sh
mkdir -p .tmp/contract
cd .tmp/contract
nix develop -c go mod init contract
nix develop -c go get github.com/kubernetes-csi/lib-volume-populator@<version>
nix develop -c go env GOMODCACHE
```

Read the module's `populator-machinery/` package from the module cache
path, plus its `go.mod` and whatever else the answers need.

### Step 3: answer these, each with `file:line` and the quoted signature

| # | Question | Why it matters |
| --- | --- | --- |
| a | Does `RunControllerWithConfig` exist, what is its signature, what are `VolumePopulatorConfig`'s fields, and how does it select `ProviderFunctionConfig`? | task 4's `main.go` |
| b | What are `PopulatorParams`' exact field names and types: the Kubernetes client, the original claim, the prime claim, the storage class, the data source object, the event recorder? | task 3's callbacks |
| c | What are the signatures and error semantics of `PopulateFn`, `PopulateCompleteFn` and `PopulateCleanupFn`? What does the library do when a callback returns an error, and when `PopulateCompleteFn` returns true? | task 3 and task 4 |
| d | Where is the prime claim created, what is it named, and which fields does it copy from the app claim? Confirm the name is `prime-<uid of the app claim>` and that `volume.kubernetes.io/selected-node` is copied for a `WaitForFirstConsumer` class | `docs/architecture.md`'s node-placement claim, and `docs/decisions.md`'s prime-claim decision |
| e | How is the data source object fetched into `PopulatorParams`? Does the library read it, or does a callback receive only a reference? | task 3 |
| f | Does the library patch the `PersistentVolume`'s `claimRef` to the app's claim and record the data source annotation? What runs after `PopulateCompleteFn` returns true, and what is deleted? | `docs/architecture.md`'s rebind step |
| g | What does the library's `go.mod` require: Go version and `k8s.io/*` versions? | task 2's dependency pin and task 3's `go mod tidy` |
| h | Does the library expose an HTTP health endpoint beside the metrics path, and at what path? | whether task 5 can put a liveness or readiness probe on the Deployment |
| i | What image name is required, and is it required in provider-function mode where no pod of ours runs? | task 4's `--image` flag |

### Step 4: VolSync's completion semantics

`docs/architecture.md`'s failure table claims that a repository holding no
snapshot yet still completes: "VolSync completes with nothing to write; the prime
claim binds empty, the app starts on an empty volume". Task 3's
`PopulateCompleteFn` reads `status.lastManualSync` against the trigger it set, so
the claim's fate depends on VolSync setting that field on such a run.

Read VolSync's source for that path (`github.com/backube/volsync`, the
`ReplicationDestination` controller and its restic mover) and answer:

| # | Question |
| --- | --- |
| j | Does VolSync set `status.lastManualSync` to the trigger when the repository holds no snapshot at all, or does it leave the field empty? |
| k | What does it set when the mover fails: a condition, an event, a status field? Name the field `PopulatorCompleteFn` could read to report `RestoreFailed` on our own status, or state that none exists and the claim then sits Pending with the reason visible only in VolSync's objects |

`docs/architecture.md`'s failure table is the design's own statement of this
behavior, so a finding that contradicts it goes to the orchestrator with the
citation, and the claim is reported as wrong rather than repaired quietly.

### Step 5: write the findings

`library-contract-findings.md` carries, in this order:

1. The version pinned, why that version, and the exact string task 3 should use.
2. A table: question, verdict `CONFIRMED` or `DIFFERENT`, the citation, the
   quoted signature or field list.
3. For every `DIFFERENT`: what the design assumed, what the source says, and the
   consequence for task 2, task 3 and task 4.
4. A short "what task 3 must know" section and a "what task 4 must know" section,
   each a list of the exact identifiers those tasks should use.
5. What the design got right, so a reader can tell the checks were run rather
   than skipped.

Write with the catalyst doc writing convention: a sentence states what a thing is
and does, em and en dashes stay out, banned filler words stay out.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- The only files this task creates or modifies are
  `.cortex/plans/2026-09-14-backup-controller/library-contract-findings.md` and
  files under `.tmp/contract/`. `git status --short` must show nothing else.
- Do not add a dependency to the root `go.mod`.
- A question the source does not answer is reported as unanswered with what you
  looked at. Mark anything you infer rather than read as `[INFERENCE]`.

## Acceptance

```sh
cd /home/nixos/repos/backup-controller
cat .tmp/contract/go.mod
git status --short
```

Expected: `.tmp/contract/go.mod` names the pinned
`github.com/kubernetes-csi/lib-volume-populator` version; `git status --short`
shows only the findings file (plus `.tmp/` if it is not yet ignored, which task 1
arranges). Every claim in the findings document carries a `file:line` citation
from the module cache, and every question in steps 3 and 4 has a verdict line,
including the ones answered "unanswered".

## Emission discipline

Spend at most 15 minutes locating the right files in the module cache, then write
the findings incrementally: after each group of questions, append what you have.
Every 10 minutes of work must leave content on disk.

## Report to

`meta-w2`, one `A2A:` steer: the pinned version, the verdict table, every
`DIFFERENT` finding with its consequence, and the file path. Then stop.
