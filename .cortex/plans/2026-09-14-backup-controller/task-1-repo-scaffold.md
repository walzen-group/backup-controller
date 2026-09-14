# task-1: the repository stands up

## Context

`/home/nixos/repos/backup-controller` holds `README.md`, `docs/` and nothing
else, with no commits yet. The design record describes a Kubernetes volume
populator controller built on kubernetes-csi/lib-volume-populator, and this task
is milestone 1 of `docs/implementation-plan.md`: the module, the pinned dev
shell, the CI workflow and the Makefile. Every later task enters the toolchain
through what this task writes.

The reference implementation is a local checkout at `/home/nixos/repos/kuport`,
a sibling project from the same organisation with the same release shape. Read
its `flake.nix`, `Makefile` and `.github/workflows/ci.yaml` and follow them.

## Target

Create, at the repository root:

- `go.mod`
- `flake.nix` and the `flake.lock` generated from it
- `.envrc`
- `.gitignore`
- `Makefile`
- `.github/workflows/ci.yaml`

Non-goals: `internal/`, `cmd/`, `config/`, `deploy/`, `chart/`, `Dockerfile`,
`README.md`, anything under `docs/`. Do not add a Go package. Tasks 2, 4 and 5
own the CI jobs they need and append them; this task writes the `check` job.

## Change

### `flake.nix`

Copy the shape of `/home/nixos/repos/kuport/flake.nix`: one `nixpkgs` input on
`github:NixOS/nixpkgs/nixos-unstable`, one `devShells.default` per system for
`x86_64-linux`, `aarch64-linux`, `x86_64-darwin`, `aarch64-darwin`.

The description names this project: fill a PersistentVolumeClaim from a restic
repository through VolSync's mover, leaving no ZFS clone behind.

Packages, all of which resolve in nixpkgs today (verified by the orchestrator on
2026-09-14 through `nix shell`):

| Package | Version observed | Used by |
| --- | --- | --- |
| `go_1_26` | go1.26.7 | every task |
| `gopls` | - | editors |
| `golangci-lint` | 2.13.2 | the lint gate |
| `kubernetes-controller-tools` | controller-tools 0.22.0, provides `controller-gen` | task 2's deepcopy and CRD generation |
| `kubectl` | v1.37.0 | task 5's render |
| `kustomize` | v5.8.1 | task 5 |
| `kubernetes-helm` | v4.2.4 | task 5 |
| `yq-go` | v4.53.3 | the release workflow's digest patch |
| `actionlint` | 1.7.12 | the workflow gate here and in task 5 |
| `hadolint` | 2.14.0 | task 4's Dockerfile |
| `setup-envtest` | - | task 2 and task 4's envtest suites |
| `git` | - | every task |

Carry kuport's comment over the `packages` list: everything every task calls
through `nix develop -c`, and nothing is fetched at shell entry.

Generate the lock file with `nix flake lock` from the repository root.

### `.envrc`

```
use flake
```

### `.gitignore`

`.direnv/`, `.tmp/`, `result`, `result-*`.

### `go.mod`

Module `github.com/walzen-group/backup-controller`, Go directive `1.26`. No
requirements: task 3 adds the library.

### `Makefile`

`NIX := nix develop -c`. Targets, all `.PHONY`:

| Target | Recipe |
| --- | --- |
| `build` | `$(NIX) go build ./...` |
| `test` | `$(NIX) go test ./... -race` |
| `vet` | `$(NIX) go vet ./...` |
| `lint` | `$(NIX) golangci-lint run` |
| `check` | `vet`, `test`, `lint`, `build` in that order |
| `generate` | `$(NIX) controller-gen object paths=./internal/api/...` |
| `manifests` | `$(NIX) controller-gen crd paths=./internal/api/... output:crd:dir=config/crd` |
| `verify` | regenerate the CRDs into `.tmp/crd` and `diff -ru config/crd .tmp/crd` |

`check` is what a person runs and what CI's check job mirrors. `generate`,
`manifests` and `verify` exist here so task 2 does not have to edit this file;
they have nothing to act on until the API package exists. Carry kuport's
rationale comment: there is no `go` or `controller-gen` on the host PATH.

### `.github/workflows/ci.yaml`

One job, `check`, copied from kuport's `ci.yaml` check job:

- `on: push: branches: ["**"]` and `pull_request`
- `permissions: contents: read`
- concurrency group `ci-${{ github.ref }}`, `cancel-in-progress: true`
- `actions/checkout@v7.0.1`, `cachix/install-nix-action@v31.11.1` with
  `experimental-features = nix-command flakes`,
  `DeterminateSystems/magic-nix-cache-action@v14`
- four steps: `nix develop -c go vet ./...`, `nix develop -c go test ./... -race`,
  `nix develop -c golangci-lint run`, `nix develop -c go build ./...`
- a fifth step after them: `nix develop -c actionlint`, whose bare form detects
  the project and lints every workflow file, so a workflow edit is checked in CI
  as well as locally. The directory form `nix develop -c actionlint
  .github/workflows/` exits 3 with `is a directory` on actionlint 1.7.12.

Carry kuport's header comment: every job runs the flake's toolchain through
`nix develop -c`, so CI and a developer's shell execute identical versions and
there is no per-tool setup action to drift from the flake.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- No Go package. Task 2 adds the first one.
- No CI job beyond `check`. Task 2 appends `generated` and `envtest`, task 5
  appends `chart` and `image`.

## Acceptance

Run from `/home/nixos/repos/backup-controller`:

```sh
nix develop -c go version        # expect: go version go1.26.7 linux/amd64, exit 0
nix develop -c go build ./...    # expect: exit 0
nix develop -c actionlint .github/workflows/ci.yaml   # expect: no output, exit 0
nix develop -c go vet ./...
nix develop -c go test ./... -race
nix develop -c golangci-lint run
nix flake lock                   # expect: exit 0
md5sum flake.lock                # expect: the same digest as before that call
```

`nix 2.34.8` has no `--no-update` flag: it answers `unrecognised flag
--no-update` and exits 1, so the literal flag this section first named is
**unsatisfiable on this host**. The property the gate wants is that the lock is
already resolved and stable, so run `nix flake lock` twice and compare the md5 of
`flake.lock`, or read `nix flake metadata` for the locked nixpkgs revision.

The last three fail on a module that holds no package. Measured by the
orchestrator on 2026-09-14 against a module containing only `go.mod`:

| Command | Exit | Output |
| --- | --- | --- |
| `go vet ./...` | 1 | `go: warning: "./..." matched no packages` / `no packages to vet` |
| `go test ./... -race` | 1 | `go: warning: "./..." matched no packages` / `no packages to test` |
| `golangci-lint run` | 5 | `context loading failed: no go files to analyze` |

Record those three as the milestone's known gap with this reason, and do not try
to close it by adding a package. They go green in task 2, and the report says so.
`nix develop -c make check` stops at the vet step, because make halts on the
first failing prerequisite and vet is first in the specified order. `make` itself
lives in the dev shell, so the working form is `nix develop -c make check`; the
milestone-1 local gate is the commands that pass above plus the flake entering
cleanly.

Do not run `nix flake check`: it evaluates every output and this flake has only a
devShell.

## Emission discipline

Spend at most 10 minutes reading the four kuport files named above, then write.
Every 10 minutes of work must leave a file on disk.

## Report to

`meta-w1`, one `A2A:` steer: files written, `git status --short`, the acceptance
commands with the exit codes and output above, the measured empty-module red
outputs recorded as the known gap, and any deviation. Then stop.
