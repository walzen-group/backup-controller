# task-5: the install assets and the release

## Context

Milestone 5 of `docs/implementation-plan.md`. The controller, its CRD and its
image all exist after tasks 2 and 4; this task writes what a cluster installs and
what a tag publishes. Two documents are the contract: `docs/packaging.md` for the
release's four assets, the repository layout, the RBAC table and the nine
workflow steps, and `docs/integration.md` for how the infrastructure repository
consumes the rendered manifest (one HTTP fetch, three kinds applied in order).

The reference implementation is `/home/nixos/repos/kuport`: its `deploy/`,
`chart/`, `.github/workflows/release.yaml` and `.github/workflows/ci.yaml` carry
the shape to copy, including the digest grep and the semver guard that
`docs/packaging.md` tells you to keep intact.

## Target

- `deploy/namespace.yaml`, `deploy/serviceaccount.yaml`, `deploy/rbac.yaml`,
  `deploy/deployment.yaml`, `deploy/kustomization.yaml`
- `deploy/crds/` (verbatim copies of `config/crd/*.yaml` plus its own
  `kustomization.yaml`)
- `chart/Chart.yaml`, `chart/values.yaml`, `chart/README.md`,
  `chart/templates/`, `chart/crds/` (the same verbatim copies)
- `.github/workflows/release.yaml`
- the `chart` and `image` jobs appended to `.github/workflows/ci.yaml`

Non-goals: Go code, `config/crd/` (task 2's, read-only here), `Dockerfile`
(task 4's, read-only here), `docs/`.

The Dockerfile arrives in task 4, one wave after this one. No gate in this task
needs the file: the release workflow and the `image` job reference it for the
image build, which happens on a runner with a container daemon. Write those
references against the path `Dockerfile` and the build argument `VERSION` exactly
as task 4 defines them, run `actionlint` over the result, and record in the report
that the file itself was not on disk to read.

## Change

### `deploy/`

| File | Contents |
| --- | --- |
| `namespace.yaml` | Namespace `backup-system` |
| `serviceaccount.yaml` | ServiceAccount `backup-controller` in `backup-system` |
| `rbac.yaml` | the ClusterRole and ClusterRoleBinding below |
| `deployment.yaml` | Deployment `backup-controller`, one replica, image `ghcr.io/walzen-group/backup-controller:v0.1.0`, `--namespace=backup-system`, the metrics port named `metrics`, `securityContext.runAsNonRoot: true`, `readOnlyRootFilesystem: true`, `allowPrivilegeEscalation: false`, all capabilities dropped, no ServiceAccount token automount beyond what the client needs |
| `kustomization.yaml` | the five resources, the `crds` directory, and an `images:` entry for `ghcr.io/walzen-group/backup-controller` with `newTag: v0.1.0` |

The ClusterRole is exactly the table in `docs/packaging.md`, and every grant
traces to a caller: the library for claims, volumes, storage classes and events;
the callbacks for destinations and Secrets; the controller for its own status.
Add nothing beyond it, because `docs/packaging.md` calls the secrets grant the
one line in this install worth arguing about:

| Resources | Verbs |
| --- | --- |
| `persistentvolumeclaims` | get, list, watch, create, patch, delete |
| `persistentvolumes` | get, list, watch, patch |
| `volumerestores.backup.wlz.li` | get, list, watch |
| `volumerestores.backup.wlz.li/status` | patch, update |
| `replicationdestinations.volsync.backube` | get, list, watch, create, delete |
| `secrets` | get, create, delete |
| `storageclasses.storage.k8s.io` | get, list, watch |
| `events` | create, patch |

Probes: finding (h) is DIFFERENT, and it settles this. The library registers only
the configured metrics path and exposes no health route, so the Deployment
carries no `livenessProbe` and no `readinessProbe`. Record that in the report with
the finding behind it. A probe against the metrics port would prove the process
listens and nothing about its state, and a real health handler is code neither
`docs/packaging.md` nor this plan asks for.

`deploy/crds/`: `cp config/crd/*.yaml deploy/crds/`, plus a `kustomization.yaml`
listing them. This copy is what makes the release asset one file containing the
whole install, which is what `docs/integration.md`'s single `data "http"` needs.
Carry kuport's comment: `kubectl kustomize` refuses a path outside the `deploy/`
tree, so the CRDs are verbatim copies with a `cp` line naming how to refresh them.

### `chart/`

Same objects templated, in kuport's shape: `_helpers.tpl` for the name and
labels, `serviceaccount.yaml`, `rbac.yaml`, `deployment.yaml`, `crds/` holding
the CRD copies, and `values.yaml` with:

| Key | Default | Meaning |
| --- | --- | --- |
| `image.repository` | `ghcr.io/walzen-group/backup-controller` | pull source |
| `image.tag` | `""`, meaning `v<appVersion>` | tag when no digest is set |
| `image.digest` | `""` | `sha256:...` pin, wins over the tag; the release bakes it in |
| `image.pullPolicy` | `IfNotPresent` | |
| `nameOverride`, `fullnameOverride` | `""` | |
| `rbac.create` | `true` | |
| `serviceAccount.create`, `serviceAccount.name` | `true`, `""` | |
| `resources` | requests 10m/64Mi, memory limit 128Mi | |
| `namespace` | `backup-system` | |

The image helper resolves digest, then tag, then `v<appVersion>`, matching
kuport's. `chart/README.md` states that `deploy/` is the primary install path and
that `deploy/` is right where the two disagree, plus the CRD note (Helm installs
`crds/` once and never upgrades it) and a values table.

### `.github/workflows/release.yaml`

Copy kuport's and rename. `docs/packaging.md`'s nine steps are the checklist, and
each one carries over with these names:

| Asset | Name |
| --- | --- |
| rendered install | `backup-controller-<version>.yaml` |
| CRD bundle | `crds-<version>.yaml` |
| chart tarball | `backup-controller-<version>.tgz` |
| OCI chart | `oci://ghcr.io/walzen-group/backup-controller` |

Keep intact: the `check` job mirroring `ci.yaml`'s four commands, the plain-semver
guard that rejects prerelease and build-metadata tags, the digest reported in the
job summary, the `kustomize edit set image` plus `kubectl kustomize` plus
`grep -q "@sha256:"` render step, the yq patch of a throwaway chart copy (the
checked-in chart is never modified), `helm package` with `--version` and
`--app-version` from the tag, `helm push` to ghcr, the CRD concatenation, and the
GitHub Release with all four assets. The image name and chart name change;
`docs/packaging.md`'s "Where the chart and `deploy/` disagree, `deploy/` is
right" line belongs in the chart README.

### `.github/workflows/ci.yaml`

Append two jobs from kuport's `ci.yaml`, leaving the existing jobs untouched:

- `chart`: render `deploy/` into `.tmp/`, grep the render for the CRD, the
  Namespace, `readOnlyRootFilesystem: true` and `runAsNonRoot: true`; `helm lint
  chart/`; `helm template` into `.tmp/`; then validate both renders offline with
  `kubectl-validate` resolved from the revision in `flake.lock`
  (`nix shell "github:NixOS/nixpkgs/${rev}#kubectl-validate"`). Carry kuport's
  comment on why `kubectl apply --dry-run=client` is unusable here.
- `image`: `docker/setup-buildx-action@v4.3.0` and
  `docker/build-push-action@v7.3.0` with `push: false`, `platforms: linux/amd64`,
  `build-args: VERSION=ci-${{ github.sha }}`, `tags: backup-controller:ci`, and
  the Actions cache.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- `config/crd/` and `Dockerfile` are read-only inputs. The CRD copies under
  `deploy/crds/` and `chart/crds/` are refreshed with `cp`, never generated here.
- No cluster command. The render, the Helm renders, `actionlint` and
  `kubectl-validate` are the gates that exist, and the release itself needs a tag
  and credentials it does not have here.
- Style follows `catalyst-v2-writing-docs` in the chart README and every comment:
  a sentence states what a thing is and does, em and en dashes stay out.

## Acceptance

From `/home/nixos/repos/backup-controller`:

```sh
nix develop -c actionlint .github/workflows/*.yaml
nix develop -c kustomize build deploy/ > .tmp/render-deploy.yaml
nix develop -c helm lint chart/
nix develop -c helm template backup-controller chart/ > .tmp/render-chart.yaml
nix develop -c bash -c 'set -euo pipefail
  grep -q "kind: CustomResourceDefinition" .tmp/render-deploy.yaml
  grep -q "kind: Namespace" .tmp/render-deploy.yaml
  grep -q "kind: Deployment" .tmp/render-deploy.yaml
  grep -q "readOnlyRootFilesystem: true" .tmp/render-deploy.yaml
  grep -q "runAsNonRoot: true" .tmp/render-deploy.yaml
  grep -q "secrets" .tmp/render-deploy.yaml'
# the release workflow's own render step, run locally against a stand-in digest:
rm -rf .tmp/release-deploy && cp -r deploy .tmp/release-deploy
nix develop -c bash -c 'cd .tmp/release-deploy && kustomize edit set image ghcr.io/walzen-group/backup-controller=ghcr.io/walzen-group/backup-controller@sha256:0000000000000000000000000000000000000000000000000000000000000000'
nix develop -c kubectl kustomize .tmp/release-deploy > .tmp/render-pinned.yaml
grep -q "@sha256:" .tmp/render-pinned.yaml
rev=$(nix eval --impure --raw --expr "(builtins.fromJSON (builtins.readFile ./flake.lock)).nodes.nixpkgs.locked.rev")
nix shell "github:NixOS/nixpkgs/${rev}#kubectl-validate" -c kubectl-validate .tmp/render-pinned.yaml
# and the chart's digest path:
rm -rf .tmp/release-chart && cp -r chart .tmp/release-chart
nix develop -c yq -i '.image.digest = "sha256:0000000000000000000000000000000000000000000000000000000000000000"' .tmp/release-chart/values.yaml
nix develop -c helm template backup-controller .tmp/release-chart | grep -q "@sha256:"
nix develop -c helm package .tmp/release-chart --version 0.0.1 --app-version 0.0.1 --destination .tmp
```

Expected: actionlint prints nothing; both renders carry the CRD, the Namespace
and the Deployment; the digest-pinned render and the digest-patched chart both
contain `@sha256:`; `helm lint` reports no failures; `helm package` writes a
`.tgz` into `.tmp/`; kubectl-validate accepts the render.

The `grep -q "@sha256:"` on the pinned render is the workflow's own guard
exercised locally. If it fails, the `images:` entry in `deploy/kustomization.yaml`
does not match the image name in `deployment.yaml`, which is the bug that guard
exists to catch.

Record as unobservable: publishing to ghcr, the four assets appearing on a
release, the OCI chart push, and the GitHub Actions run itself. Task 6's runbook
carries the cluster install, and `docs/packaging.md`'s claim that a `v0.0.1` tag
produces four assets is answered by the workflow's steps rather than by a run.

## Emission discipline

Spend at most 15 minutes reading `docs/packaging.md`, `docs/integration.md` and
kuport's install tree, then write. Copy kuport's files before adapting them: a
hand-rewritten workflow loses the guards. Every 10 minutes of work must leave a
file on disk.

## Report to

`meta-w3`, one `A2A:` steer: files written, `git status --short`, every
acceptance command with its output, the probe decision with task 0's finding
behind it, and the list of gaps recorded as unobservable. Then stop.
