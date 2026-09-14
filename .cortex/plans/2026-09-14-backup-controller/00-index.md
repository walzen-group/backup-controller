> **Status: COMPLETE** (2026-09-14)

> Tasks 0 through 8 have landed. Waves 3 through 6 were finished in one session
> after the delegate runtime ran out of budget mid-wave-3, so waves 3, 4, 5 and 6
> have no per-wave hand-back JSON; their evidence is the final review at
> `.cortex/reports/2026-09-14-backup-controller-final-review.md`, which reran
> every whole-change gate first-hand. That review found and repaired five
> defects, four of them blocking, including a wave-3 delivery that did not
> compile. Every gate in the whole-change verification section below exits 0.

# backup-controller: milestones 1-5

Execution plan for implementing [docs/implementation-plan.md](../../../docs/implementation-plan.md)
milestones 1 through 5 in `/home/nixos/repos/backup-controller`. Milestone 6
(cluster canary) and milestone 7 (infrastructure repository) are out of scope by
the user's decision of 2026-09-14; M6's commands ship as a runbook report
(task 6) and M7 is not this repo's work.

Each implementer receives only its own task doc plus the global constraints
below. The orchestrator dispatches a wave, hands monitoring and verification to
that wave's meta-agent, and audits the meta's hand-back.

## Goal

A repository that builds, tests and installs: the VolumeRestore API and its
generated CRD, the `internal/volsync` and `internal/populator` packages with
their fake-client tests, the binary bound to
`lib-volume-populator`'s provider callbacks, and the install assets (`deploy/`, `chart/`, release
workflow) the infrastructure repository consumes by release tag.

## Source spec and resolved questions

Design record: [docs/](../../../docs/), primarily `architecture.md` (the library
contract), `api.md` (the resource), `packaging.md` (release assets and RBAC),
`overview.md` (the mechanism).

| Question | Answer | Source |
| --- | --- | --- |
| Which milestones? | 1-5. M6 is the admin's to run, M7 is the other repository | user, 2026-09-14 |
| Cluster for M6? | Out of scope, no cluster commands. Ship a runbook | user, 2026-09-14 |
| Group, version, kind? | `backup.wlz.li/v1alpha1`, kind `VolumeRestore`, short name `vrestore` | `docs/api.md`; settle-with-Sam is a human gate this plan does not reopen |
| Models? | `opencode-go/gpt-5.6-luna` for the contract-defining tasks (0, 3, 4, 8); `opencode-go/deepseek-v4.1-flash` for every other worker and every meta-agent | user constraint 2026-09-14, split chosen by the orchestrator after the split question was cancelled |
| Commits? | None. Work stays in the working tree; `git add -A` for a diffable report is allowed, `git commit` is not | catalyst default, commit question cancelled |
| Image build locally? | Not observable; see the environment table | `docker info` fails in this WSL distro |

## Revision notes

**2026-09-14, wave 1 relaunched.** The first dispatch of wave 1 (`2026-09-14-bc-w1`)
carried `workspace: { "label": "backup-controller" }`. Two live workspaces
matched that label, mine and an older idle session's, and c2d resolves a label
with `workspaces.find(...)`, so it took the first match. The two agents were
created as siblings of that older session; when its workspace closed, all three
tabs went with it. `c2d status` read `present: false` for both agents, `herdr
agent get` returned `agent_not_found`, and the repository held no file from the
worker.

Every wave now dispatches with a label unique to this epic
(`backup-controller-epic`) and `create_cwd` pointing at the repository, so c2d
creates the workspace rather than matching another session's. Wave 1 was
re-dispatched as `2026-09-14-bc-w1b` into that workspace. No task spec changed.

The user settled the two follow-up calls on 2026-09-14: the failure is recorded
here and nowhere else (no kit incident, no c2m note), and every wave for this
epic keeps dispatching to the dedicated `backup-controller-epic` workspace.

**2026-09-14, wave 1 complete; five spec-text corrections.** Task 1 landed and
passed verification (`meta-w1`, hand-back received, gate evidence at
`.cortex/reports/handbacks/w1-gate-evidence.txt`). The meta's own measurements
corrected five errors in the task-1 and task-2 text, all of them mine, all now
fixed in the specs:

| Correction | Was | Now |
| --- | --- | --- |
| `actionlint` step form | `actionlint .github/workflows/` | bare `actionlint`, which detects the project; the directory form exits 3 |
| lock stability check | `nix flake lock --no-update` | the flag does not exist in nix 2.34.8; `nix flake lock` twice with an md5 comparison of `flake.lock` |
| controller-tools version | 0.21.0 | 0.22.0, which is what the locked nixpkgs resolves to |
| where `make check` stops | the lint step | the vet step, since make halts on the first failing prerequisite |
| `make` invocation | bare `make` | `nix develop -c make`, because make lives in the dev shell |

The empty-module reds (vet exit 1, test exit 1, lint exit 5) were re-measured
first-hand by the meta and match. Task 2 closes that gap.

**2026-09-14, recorded gate exception.** Task 1's acceptance named `nix flake
lock --no-update`, which no nix on this host accepts (exit 1, `unrecognised flag
--no-update`). On the user's explicit confirmation the gate is satisfied by the
substitute evidence: `nix flake lock` run twice with `flake.lock` unchanged at md5
`7bcde4baf18fd68adec731bb9fbbaf0f`, and `nix flake metadata` resolving lock
version 7 at nixpkgs rev `ef34387ddd751e1ab8857adf4676492d32eb24ec`. The
exception, its substitute, and the confirmation are recorded here so the skip is
auditable.

## Environment premises (all observed 2026-09-14, not assumed)

| Premise | Evidence | Consequence for every task |
| --- | --- | --- |
| The toolchain arrives by flake | `nix develop -c` in `~/repos/kuport` printed go1.26.7, golangci-lint 2.13.2, kustomize v5.8.1, helm v4.2.4, kubectl v1.37.0; `yq` was absent there, so this repo's flake adds `yq-go` | Every command runs `nix develop -c ...` from the repo root. There is no `go` on the host PATH |
| No Kubernetes cluster is reachable | `kubectl get --raw /version` and `kubectl get nodes` both returned nothing before a 15 s timeout; the only context is `docker-desktop` | No task runs a cluster command, not even `--dry-run=server`. Offline validation is `kubectl-validate`, kustomize, helm |
| envtest control planes are on disk | `~/.local/share/kubebuilder-envtest/k8s/1.37.0-linux-amd64/` holds `kube-apiserver`, `etcd`, `kubectl` | Task 4's start/watch/SIGTERM and population proof runs against a real API server with no network |
| No container daemon | `docker info` → "The command 'docker' could not be found in this WSL 2 distro" | Task 4 proves the image by building the static binary directly plus `hadolint`; the `docker build` itself is a CI gate, recorded as an unobservable gap |
| Network reaches Go modules and nixpkgs | `curl -sI https://proxy.golang.org` → HTTP/2 200 | module downloads and `nix shell` work |
| `~/repos/kuport` is the reference implementation | its `flake.nix`, `Makefile`, `Dockerfile`, `deploy/`, `chart/`, and both workflows exist locally | tasks 1, 4 and 5 copy its shape rather than inventing one |

## Global constraints

Every task doc repeats these in its Constraints section; they are stated once
here as the full form.

1. **Root**: `/home/nixos/repos/backup-controller`. All file paths absolute or
   repo-relative from that root.
2. **Toolchain**: run every Go, kustomize, helm, kubectl, controller-gen and yq
   command as `nix develop -c <command>` from the repo root. Never install a
   tool on the host, never `go install`, never a version pinned only in a
   README.
3. **No cluster commands.** Nothing talks to a Kubernetes API server except the
   envtest control plane task 4 starts from the local assets directory. Do not
   run `kubectl` against a discovered context.
4. **No commits, no push, no branch or history operations.** `git add -A` is
   allowed so `git diff --cached --stat` is a real diff summary; `git commit`,
   `git push`, `git reset`, `git rebase` and `git checkout` are not.
5. **Scratch goes to `.tmp/`** (gitignored, created in task 1). Never `/tmp`,
   never `docs/`, never `.cortex/`. Renders, logs and one-off scripts live
   there.
6. **`.cortex/` is the orchestrator's.** A delegate writes only the findings
   file its spec names there, or nothing at all.
7. **No other models.** `opencode-go/gpt-5.6-luna` or
   `opencode-go/deepseek-v4.1-flash` only, as named per task.
8. **Style**: user-facing prose follows `catalyst-v2-writing-docs`. Commit
   messages, comments and docs follow the same rules: a sentence states what a
   thing is and what it does, em and en dashes are out, and the banned filler
   words stay out.
9. **Report to the wave's meta-agent**, not to the orchestrator: one `A2A:`
   steer to the meta named in the spec, then stop. Do not read peer state, do
   not chase acknowledgements.
10. **Report format**: files changed, `git status --short`, `git diff --cached
    --stat`, the gate commands and their observed output, deviations named.
    Acceptance criteria are not negotiable; a blocker is reported with the
    criteria intact.
11. **A test earns its place only where a plausible bug would fail it.**
    It asserts behavior, boundaries, transitions and real errors.

## Verification environment, stated once

Every task's acceptance commands are locally runnable in the pinned toolchain:
`nix develop -c go vet|test|lint|build`, `controller-gen` through make,
`kustomize build`, `helm lint|template`, `kubectl-validate` resolved from the
flake's locked nixpkgs revision, `hadolint`, and envtest from the local asset
directory. Where the design document's proof names GitHub Actions or a live
cluster, the task doc names the local equivalent and records the part that
cannot be observed here.

## Task table

| Doc | Task | Model | Owns (files) | Depends on |
| --- | --- | --- | --- | --- |
| [task-0](task-0-verify-library-contract.md) | Verify the library contract | luna | `.cortex/plans/.../library-contract-findings.md` | task 1 |
| [task-1](task-1-repo-scaffold.md) | Repository stands up | deepseek | `go.mod`, `flake.nix`, `flake.lock`, `.envrc`, `.gitignore`, `Makefile`, `.github/workflows/ci.yaml` | - |
| [task-2](task-2-api-and-crd.md) | API and CRD | deepseek | `internal/api/v1alpha1/`, `config/crd/`, `config/samples/` | 1 |
| [task-3](task-3-populator.md) | volsync and populator packages | luna | `internal/volsync/`, `internal/populator/`, `go.mod` dependency | 0, 2 |
| [task-4](task-4-binary-and-envtest.md) | Binary, Dockerfile, offline end-to-end | luna | `cmd/backup-controller/`, `Dockerfile` | 3 |
| [task-5](task-5-install-assets.md) | Install assets and release | deepseek | `deploy/`, `chart/`, `.github/workflows/release.yaml` | 2 |
| [task-6](task-6-cluster-runbook.md) | Cluster runbook report | deepseek | `.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md` | 4, 5 |
| [task-7](task-7-docs-status.md) | Docs status close-out | deepseek | `README.md`, `docs/implementation-plan.md` | 4, 5 |
| [task-8](task-8-final-review.md) | Whole-branch review | luna | none (read-only report) | 0-7 |

## Tracks and waves

The build order here is genuinely a chain: nothing compiles before the module
exists, the callbacks need the API types, the binary needs the callbacks, and
the install render needs the CRD. Parallelism exists only around that chain.

| Wave | Workers | Meta | Why these together |
| --- | --- | --- | --- |
| 1 | task-1 | meta-w1 | Everything waits on the module and the flake |
| 2 | task-0, task-2 | meta-w2 | Disjoint: 0 writes one plan-dir findings file, 2 writes the API package. The CRD does not depend on the library contract, and the contract findings gate task 3, one wave later |
| 3 | task-3, task-5 | meta-w3 | Disjoint file sets: Go packages against YAML assets. 5 needs the generated CRD from wave 2 to copy into `deploy/crds/` and `chart/crds/` |
| 4 | task-4 | meta-w4 | Needs the callbacks from task 3 |
| 5 | task-6, task-7 | meta-w5 | Both read the finished tree; disjoint files |
| 6 | task-8 | meta-w6 | Reviews the whole tree once every other task has landed |

Every sequential edge in that table is a real dependency. Task 0 and task 2
share wave 2 because task 0 has to wait for task 1 to write the flake it enters
through.

## Pre-work: board

None. No external tracker is configured for this repository (no Plane, Jira,
Linear or GitHub Projects, and the repository has no commits or project yet),
which is the condition `catalyst-v2-status-board-keeping` attaches to a keeper.
Tracking lives in this plan directory, and the deviation is recorded here on the
record.

## Agent allocation

| Task | CLI | Model | Thinking | Rationale |
| --- | --- | --- | --- | --- |
| 0, 3, 4, 8 | omp | `opencode-go/gpt-5.6-luna` | high | Contract-defining: the library's real signatures, the callbacks built on them, the binary that reaches them, and the review of all three |
| 1, 2, 5, 6, 7 | omp | `opencode-go/deepseek-v4.1-flash` | max | Spec'd implementation with a reference implementation beside it, plus two chore-shaped docs tasks |
| every meta-agent | omp | `opencode-go/deepseek-v4.1-flash` | max | Monitoring and verification cycles |

Locked before dispatch. Re-route only when a delegate demonstrably misses the
bar after retries.

## Whole-change verification

Run once by the last wave's meta-agent, then audited by the orchestrator from
the hand-back (the orchestrator re-runs no gate):

```sh
cd /home/nixos/repos/backup-controller
nix develop -c go vet ./...
nix develop -c go test ./... -race
nix develop -c golangci-lint run
nix develop -c go build ./...
nix develop -c make verify        # controller-gen drift against config/crd
nix develop -c kustomize build deploy/ | grep -q 'kind: Deployment'
nix develop -c helm lint chart/
rev=$(nix eval --impure --raw --expr "(builtins.fromJSON (builtins.readFile ./flake.lock)).nodes.nixpkgs.locked.rev")
nix shell "github:NixOS/nixpkgs/${rev}#kubectl-validate" -c kubectl-validate .tmp/whole-change-render.yaml
nix develop -c hadolint Dockerfile
```

## Smoke test

Task 4's script is the epic's smoke test: it starts an envtest API server from
`~/.local/share/kubebuilder-envtest/k8s/1.37.0-linux-amd64`, applies this
repo's CRD plus a vendored ReplicationDestination CRD, creates a `VolumeRestore`
and a claim naming it, starts the built binary against that server, and asserts
from the API server itself:

| Observation | Expected |
| --- | --- |
| the binary's log | names the kind it watches as it comes up |
| `SIGTERM` | exit status 0, no panic |
| the copied repository Secret in the controller namespace | present, named from the claim UID |
| the prime claim | `prime-<claim uid>`, the app claim's storage class and size, no data source |
| the ReplicationDestination | `copyMethod: Direct`, `destinationPVC: prime-<uid>`, `trigger.manual: <uid>`, the VolumeRestore's `moverPodLabels` |

The restore completing (VolSync's mover writing data) is not reproducible
offline: no VolSync controller exists here to report
`status.lastManualSync`. That row, the claim rebind, the pool measurement and
the delete-and-refill cycle are task 6's runbook for the test cluster.

## Out of scope

- Milestone 6: every cluster command. Task 6 writes the runbook only.
- Milestone 7: the infrastructure repository, its terragrunt module, unit and
  Flux component change.
- Pushing to `ghcr.io`, tagging a release, running GitHub Actions.
- The `docker build` of the image: no container daemon is reachable.
- `v1beta1`, conversion webhooks, and any field beyond the four in `docs/api.md`.
