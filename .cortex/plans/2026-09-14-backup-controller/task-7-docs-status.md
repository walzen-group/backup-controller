# task-7: the docs status close-out

## Context

`README.md` opens with "Status: specification only. No code has been written.",
and `docs/implementation-plan.md` is written for a reader starting from an empty
repository. After tasks 1 through 6 the module, the API and its CRD, the
populator packages, the binary, the install assets and the release workflow all
exist. A reader arriving now is told the wrong thing about the repository in the
two places they look first.

This is a small, precise edit. `docs/` is the design record and the rest of it
stays as written: the mechanism, the decisions, the field tables and the failure
behavior are what the code was built from.

## Target

- `README.md`: the status paragraph only.
- `docs/implementation-plan.md`: a status block at the top, below the title.

Non-goals: every other file, including the rest of `README.md` and the other
pages under `docs/`. Do not rewrite the plan's milestones, the tables or the
rejected-alternatives text.

## Change

### `README.md`

Replace the status paragraph with what is true now. Name what exists (the module
and its flake, the `VolumeRestore` API with the generated CRD, the `volsync` and
`populator` packages with their fake-client tests, the binary bound to the
library's provider callbacks, the `deploy/` tree, the Helm chart and the release
workflow), and name what does not exist yet: no tag has been cut, so there is no
published image and no release asset, and the infrastructure repository in
`docs/integration.md` still describes work nobody has done.

Point at the runbook for the cluster proof:
`.cortex/reports/2026-09-14-backup-controller-cluster-runbook.md`. Keep the
paragraph short: what a reader needs is where the project stands, and the
documents table below it already routes them.

### `docs/implementation-plan.md`

Add a status block under the title, before the current "Written for an agent
starting from an empty repository" paragraph: the date, which milestones are
implemented (1 through 5), that milestone 6 is the admin's run with the runbook
path named, and that milestone 7 is untouched. Keep the existing paragraph: it
describes who the page was written for, which stays accurate.

Both edits follow `catalyst-v2-writing-docs`, and its mandatory humanizer pass
runs before the text is written. Two rules bind hardest here: a sentence states
what a thing is and does, and em and en dashes stay out.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- Every claim in the new text is checkable by a path that exists in the
  repository. Name those paths in the hand-back, one per claim.
- Two files. No third file changes, and no file under `.cortex/` is written.
- Do not describe work as done that no task in this plan did.

## Acceptance

```sh
cd /home/nixos/repos/backup-controller
git status --short
git diff --cached --stat README.md docs/implementation-plan.md
```

Expected: those two paths and no others. Read each new sentence against the tree
and confirm the path it describes exists; a sentence that survives that check
goes in the hand-back with its path.

## Emission discipline

This is a short task. Read the two files, write both edits, then run the
acceptance. Spend no more than 10 minutes before the first write.

## Report to

`meta-w5`, one `A2A:` steer: the two file paths, the diff stat, and each new
claim with the path that backs it. Then stop.
