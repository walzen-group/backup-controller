# task-6: the cluster runbook

## Context

Milestone 6 of `docs/implementation-plan.md` is the canary run on the walzen
test cluster. By the user's decision of 2026-09-14 no cluster command runs in
this epic: the cluster is the admin's, and this task writes the report the admin
runs instead. Every claim this offline epic could not close belongs in it, with
what was proven in its place.

The deliverable is a report a person reads, so it follows
`catalyst-v2-writing-docs`: the answer leads, each procedure is a numbered step
with its command in its own fenced block, and each step states its expected
result on its own line. Keep the two style rules that bind hardest here: a
sentence states what a thing is and does, and em and en dashes stay out.

## Target

One file:
`.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md`.

Non-goals: no other file, and no cluster command of any kind. `kubectl` against a
discovered context is what this task exists to avoid.

## Change

Read first: `docs/implementation-plan.md` (milestone 6's table and the "What
would make this project wrong" list), `docs/integration.md` (the install and the
canary rehearsal), `docs/api.md` (the worked example: the three files that move a
claim to a `VolumeRestore`), and the whole-change verification section of
`00-index.md`.

The report carries these sections, in this order.

### What this runbook proves

Two or three sentences: a claim filled from a restic repository by VolSync's
mover, with no VolumeSnapshot and no clone, verified on the test cluster. Name
the magnitude question the design left open: the paragraph that sent everyone
here described a mechanism with no numbers, so the pool measurement is the
deliverable as much as the passing rows are.

### Before the first canary

The prerequisites and what each is for, as a table: the CRDs applied, VolSync
installed, the controller image pushed to ghcr, the controller Deployment
Available, and the repository Secret in the canary's namespace. State plainly
that no release exists yet, so the first cluster install applies `deploy/` from a
checkout or the release asset from the first tag, and name which file each path
arrives in.

### Install

Numbered steps with commands: apply the CRDs, apply the install, wait for the
rollout, confirm the controller is watching (`kubectl logs` for the startup line
task 4 logs). Each step states its expected result. For a step whose wrong result
has a cause worth naming, say what that result means.

### Move one canary to a VolumeRestore

The three files from `docs/api.md`'s worked example, with the canary's name
substituted, plus what to delete from the app's base. Name the two annotations
that disappear and what replaces their job: deleting the claim is the trigger,
and the scheduler's node is carried onto the prime claim.

### The observation rows

`docs/implementation-plan.md`'s milestone-6 table, all five rows, each as its own
numbered step with the command in a fenced block and the expected result on the
following line, plus what a failure in that row tells the operator. The rows:

| Check | Expected |
| --- | --- |
| `kubectl get pvc` in the app's namespace | the claim Bound, and no `volsync-*-dest` claim beside it |
| `kubectl get zfsvolume -A -o custom-columns='NAME:.metadata.name,SNAP:.spec.snapname'` | the app's volume with an empty `snapname` |
| `zfs list -t all -o name,used,refer,origin -r zfspv-pool` on the node | no clone and no snapshot for that volume |
| delete the claim | it is recreated, refilled, and the app comes back with its data |
| the mover pod | admitted by Kueue, like every other mover |

### The measurement

The pool command from row three, run twice: once on the canary moved to a
`VolumeRestore`, once on a volume still on the snapshot path. What to record
(`used`, `refer`, `origin` per dataset, and the snapshot's existence), and where
the numbers go, which is the infrastructure repository's backups documentation.
Say why the pair matters: a single volume proves nothing about drift, and the two
numbers together are what turns the mechanism claim into a magnitude.

### What would make this project wrong

The four items from `docs/implementation-plan.md`, each as its own subsection,
with what to watch for during the run and what to do when it appears: stop and
report rather than work around. The fourth (`a large restore outrunning the
library's own timeouts`) is the one the design calls most likely and the first to
test, so give it its own step: restore something in the fifty gigabyte range and
record the wall time.

### What this epic could not prove

A table of the gaps, each with the substitute evidence that exists:

| Gap | What was proven instead |
| --- | --- |
| the container image builds | a static `linux/amd64` binary build and hadolint; the build runs in CI's `image` job |
| a tag produces the four release assets | the render, the digest grep and the chart packaging run locally against a stand-in digest; publishing needs credentials and a tag |
| the restore completes and the claim rebinds | envtest showed the Secret copy, the prime claim and the ReplicationDestination; the mover and the rebind need VolSync |
| a large restore's wall time against the library's timeouts | nothing offline; the cluster run is the only place this is measurable |

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- One file, the report path above. No repository file changes.
- Every command in the report is copy-pasteable and names the namespace and tool
  it runs in. Do not invent a command you cannot trace to `docs/` or to a file in
  this repository.
- The report is self-contained: a reader who has not read the epic can run it.

## Acceptance

```sh
cd /home/nixos/repos/backup-controller
git status --short
wc -l .cortex/reports/2026-09-14-backup-controller-cluster-runbook.md
grep -c '```' .cortex/reports/2026-09-14-backup-controller-cluster-runbook.md
```

Expected: `git status --short` shows no repository file touched by this task;
every command from `docs/implementation-plan.md`'s milestone 6 and every row of
its two tables appears in the report, which you confirm by reading both
documents side by side and listing what you matched in the hand-back.

## Emission discipline

Spend at most 15 minutes reading the four documents, then write the report in
sections. Every 10 minutes of work must leave content on disk.

## Report to

`meta-w5`, one `A2A:` steer: the report path, its line count, the mapping from
each source table row to the section that carries it, and any row you could not
write because the source does not support it. Then stop.
