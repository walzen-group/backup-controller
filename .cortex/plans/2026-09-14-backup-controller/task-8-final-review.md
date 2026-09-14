# task-8: whole-branch review

## Context

Tasks 1 through 7 have landed in `/home/nixos/repos/backup-controller` and nothing
is committed. This task reads the whole tree against the design record and
reports what does not hold. It writes no product code: a finding is a finding,
and the orchestrator decides what happens next.

The design record is `docs/`: `overview.md` for the mechanism, `architecture.md`
for the object flow and the library's role, `api.md` for the resource,
`packaging.md` for the release and RBAC, `decisions.md` for the choices that were
settled, and `implementation-plan.md` for the order of work and the four
conditions under which the project would be wrong.

## Target

One file:
`.cortex/reports/2026-09-14-backup-controller-final-review.md`.

Non-goals: no edit to any file outside that report. Scratch renders and logs go
under `.tmp/`.

## Change

Read, then report. Every finding carries a `file:line` citation in this
repository or in `docs/`. Rank each finding `blocking`, `worth fixing`, or
`note`, and give the reason for the rank.

### What to check

| # | Check | Where the claim comes from |
| --- | --- | --- |
| 1 | The three provider callbacks do what `architecture.md`'s callback table says: create the destination on populate, read completion from the destination's status, delete it on cleanup | `docs/architecture.md` |
| 2 | The prime claim keeps the app claim's storage class, access modes, size and the scheduler's node, and no mutator hook changes them | `docs/decisions.md`'s prime-claim entry and `architecture.md`'s node section |
| 3 | The repository Secret copy is named from the claim UID, lives in the controller's namespace, and is deleted in cleanup | `docs/decisions.md`'s Secret entry |
| 4 | The `VolumeRestore` fields match `api.md`'s table one for one, and no field exists that the table does not list | `docs/api.md` |
| 5 | The status shape matches `api.md`: kstatus-compatible `Ready` condition plus `claims[]` with name, UID, phase and start time | `docs/api.md` |
| 6 | The ClusterRole grants nothing beyond `packaging.md`'s table, and every entry in the table is present | `docs/packaging.md` |
| 7 | The release workflow keeps the plain-semver guard, the digest grep and the chart packaging of a throwaway copy, and produces the four assets `packaging.md` names | `docs/packaging.md` |
| 8 | The rendered install carries the CRD, the Namespace and the workload in one asset, which is what `integration.md`'s single HTTP fetch needs | `docs/integration.md` |
| 9 | Every row of `implementation-plan.md`'s "Before writing code" table is confirmed in `library-contract-findings.md`, and no row was worked around quietly | `docs/implementation-plan.md` |
| 10 | Each of the four items in "What would make this project wrong" is either ruled out by the code or named as unproven | `docs/implementation-plan.md` |
| 11 | Test quality: for each test, does a plausible bug fail it? Name every test that asserts only wiring, a default, or a copied field | global constraint 11 |
| 12 | Scope: name anything in the tree that no task in `00-index.md` asked for, and anything a task asked for that is missing | `00-index.md`'s task table and the task docs |

Rebuild the evidence rather than trusting reports: run the whole-change
verification commands from `00-index.md` yourself, read the test files, and read
the generated CRD against the types. A claim in a task report that the tree does
not support is a finding, and the highest-ranked kind.

For check 9, read `library-contract-findings.md` first. Where it reports
`DIFFERENT`, confirm the code follows the finding rather than the design
document, and say so under the check that covers that code.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- Read-only with respect to the repository. The report is the only file written,
  and any command that writes goes to `.tmp/`.
- No cluster commands. envtest from the local assets and the flake's tools are
  what you have.
- Style follows `catalyst-v2-writing-docs`, humanizer pass included: the report
  leads with the answer, findings are ranked, and em and en dashes stay out.

## Acceptance

```sh
cd /home/nixos/repos/backup-controller
git status --short
nix develop -c go vet ./... && nix develop -c go test ./... -race && nix develop -c golangci-lint run && nix develop -c go build ./...
nix develop -c make verify
nix develop -c kustomize build deploy/ > /dev/null
nix develop -c helm lint chart/
```

Expected: every command exits 0, the report exists at the path above, and every
finding in it carries a citation. State the commands you ran with their output in
the report, and say which of the twelve checks produced no finding.

## Emission discipline

Spend at most 20 minutes reading before the first write, then write findings as
they are confirmed. Every 10 minutes of work must leave content on disk.

## Report to

`meta-w6`, one `A2A:` steer: the report path, the ranked findings with their
citations, which of the twelve checks produced no finding, and the commands run
with their results. Then stop.
