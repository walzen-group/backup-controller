# Packaging and release

A release is what the infrastructure repository installs. This repository's
release pipeline is modelled on kuport's, which the walzen-group infrastructure
already consumes, so copy that shape rather than inventing one.
[walzen-group/kuport](https://github.com/walzen-group/kuport), file
`.github/workflows/release.yaml`, is the reference implementation.

## What a release has to attach

A tag `vX.Y.Z` produces four things, and the infrastructure repository reads the
first of them. The release page lists the first three; the fourth is a registry
push rather than an attached file:

| Asset | Contents | Consumed by |
| --- | --- | --- |
| `backup-controller-X.Y.Z.yaml` | the whole install rendered from `deploy/`, with the image pinned by digest and the CRD included | the terragrunt module, over HTTP |
| `crds-X.Y.Z.yaml` | the CRDs alone, which version on their own schedule | a cluster that applies schemas ahead of workloads |
| `backup-controller-X.Y.Z.tgz` | the Helm chart, with `image.digest` baked in | anyone installing with Helm |
| `oci://ghcr.io/walzen-group/backup-controller` | the same chart pushed as an OCI artifact | a Flux HelmRelease consumer |

The rendered manifest is the primary path. Where the chart and `deploy/`
disagree, `deploy/` is right, and the chart's README says so the way kuport's
does.

## Repository layout

```
backup-controller/
├── cmd/backup-controller/      the binary's main package
├── internal/
│   ├── populator/              the three provider callbacks
│   ├── volsync/                building and reading ReplicationDestination
│   └── api/v1alpha1/           the VolumeRestore types
├── config/
│   ├── crd/                    generated CRD YAML, one file per kind
│   └── samples/                a VolumeRestore and a claim that names it
├── deploy/                     the plain-manifest install path
│   ├── kustomization.yaml
│   ├── namespace.yaml
│   ├── rbac.yaml
│   ├── serviceaccount.yaml
│   ├── deployment.yaml
│   └── crds/                   copied from config/crd at release time
├── chart/
│   ├── Chart.yaml
│   ├── values.yaml
│   ├── crds/
│   ├── templates/
│   └── README.md
├── docs/
├── Dockerfile
├── Makefile
├── flake.nix                   the dev shell CI also uses
└── .github/workflows/          ci.yaml and release.yaml
```

## The release workflow, step by step

Copy kuport's and change the names. What each step is for:

1. **Check gate.** `go vet`, `go test -race`, `golangci-lint run`, `go build`,
   every command identical to `ci.yaml`. A tag that does not pass CI does not
   become a release.
2. **Reject a tag that is not plain semver.** kuport's workflow refuses
   prerelease and build-metadata suffixes because they overwrite the rolling
   minor tag and cannot produce a valid chart version. Keep the same guard.
3. **Build and push the image**, tagged `vX.Y.Z` and `X.Y`, to
   `ghcr.io/walzen-group/backup-controller`.
4. **Report the digest** in the job summary. The digest is the half of the pin
   that decides what a cluster runs.
5. **Render `deploy/` with the image pinned by digest.** `kustomize edit set
   image`, then `kubectl kustomize deploy/`, then a `grep -q "@sha256:"` that
   fails the run when the override was not consumed. That grep is what catches a
   rendered manifest still floating on a tag.
6. **Package the chart** with `--version` and `--app-version` from the tag, after
   patching `image.digest` into a throwaway copy of `chart/`. The checked-in
   chart is never modified.
7. **Push the chart to ghcr.io** as an OCI artifact.
8. **Bundle the CRDs** by concatenating `config/crd/*.yaml`.
9. **Create the GitHub Release** with the three file assets attached and the
   digest in the body. Step 7 already pushed the chart to the registry, so the
   release page itself lists three files.

## What the rendered manifest must contain

The terragrunt module splits the documents by kind and applies them in three
groups, so the render has to carry all of them:

| Kind | Why the module needs it in the same asset |
| --- | --- |
| CustomResourceDefinition | applied first and waited on for `Established`, so a VolumeRestore can be created in the same apply |
| Namespace | applied before the workload |
| everything else | ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment |

Do not split the install across two assets. One file is the whole install, which
is what makes the module a single `data "http"` and a single version string.

## Image

| | |
| --- | --- |
| Registry | `ghcr.io/walzen-group/backup-controller` |
| Platforms | `linux/amd64` is enough for this cluster; add arm64 only when a node needs it |
| Base | distroless or scratch. The binary needs no shell, no restic and no kubectl |
| User | non-root, no capabilities, read-only root filesystem |

## RBAC the controller needs

Write it out in `deploy/rbac.yaml` as a ClusterRole, and keep it to what the
library and the callbacks actually use:

| Resources | Verbs | For |
| --- | --- | --- |
| `persistentvolumeclaims` | get, list, watch, create, patch, delete | the app's claim and the prime claim |
| `persistentvolumes` | get, list, watch, patch | rebinding the volume to the app's claim |
| `volumerestores` (our group) | get, list, watch | reading the data source |
| `volumerestores/status` | patch, update | reporting conditions |
| `replicationdestinations.volsync.backube` | get, list, watch, create, delete | one per restore |
| `secrets` | get, create, delete | copying the repository Secret for the length of a restore |
| `storageclasses` | get, list, watch | reading the binding mode |
| `events` | create, patch | the recorder the library uses |

Narrow `secrets` if it can be narrowed. A ClusterRole that can read every Secret
in the cluster is the one line in this install worth arguing about, and
[decisions.md](decisions.md) records the alternative that avoids it.

## Versioning

Semver on the tag. The CRD's API version moves on its own: `v1alpha1` until the
shape has survived a cluster rebuild and a restore of something large, then
`v1beta1` with a conversion webhook only if a field has to change incompatibly.
Prefer adding an optional field to changing an existing one.
