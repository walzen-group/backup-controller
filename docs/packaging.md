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
│   ├── api/v1alpha1/           VolumeRestore, BackupRun, RestoreRun and the annotations
│   ├── populator/              the three provider callbacks
│   ├── volsync/                building and reading ReplicationDestination
│   ├── runs/                   the BackupRun and RestoreRun reconcilers and the scheduler
│   ├── bootstrap/              the Cluster webhook and the object store reads
│   └── restic/                 reading a repository's snapshots over S3
├── config/
│   ├── crd/                    generated CRD YAML, one file per kind
│   └── samples/                a VolumeRestore and a claim that names it
├── deploy/                     the plain-manifest install path
│   ├── kustomization.yaml
│   ├── namespace.yaml
│   ├── rbac.yaml
│   ├── serviceaccount.yaml
│   ├── deployment.yaml
│   ├── webhook.yaml            the webhook Service, Issuer, Certificate and MutatingWebhookConfiguration
│   └── crds/                   copied from config/crd by make manifests
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
| everything else | ServiceAccount, ClusterRole, ClusterRoleBinding, Deployment, and the webhook's Service, Issuer, Certificate and MutatingWebhookConfiguration |

Do not split the install across two assets. One file is the whole install, which
is what makes the module a single `data "http"` and a single version string.

## Image

| | |
| --- | --- |
| Registry | `ghcr.io/walzen-group/backup-controller` |
| Platforms | `linux/amd64` is enough for this cluster; add arm64 only when a node needs it |
| Base | scratch, with the public CA roots copied in from the build stage for the webhook's object store calls. The binary carries Go's zone database for `CRON_TZ=` schedules, and needs no shell, no restic and no kubectl |
| User | non-root, no capabilities, read-only root filesystem |

## RBAC the controller needs

`deploy/rbac.yaml` writes it as one ClusterRole, and every rule traces to a
caller in the controller:

| Resources | Verbs | For |
| --- | --- | --- |
| `persistentvolumeclaims` | get, list, watch, create, patch, delete | the app's claim, the prime claim, and an `into:` scratch claim |
| `persistentvolumes` | get, list, watch, patch | rebinding the volume to the app's claim, and reading a volume's node for its mover |
| `storageclasses` | get, list, watch | reading the binding mode |
| `pods` | get, list, watch | the library's pod informer, which it builds and waits on whether or not a populator pod is used, and a RestoreRun finding the pod that holds a claim |
| `volumerestores` (our group) | get, list, watch, create | reading the data source, and a RestoreRun writing the point-in-time one its scratch claim fills from |
| `volumerestores/status` | patch, update | reporting conditions |
| `backupruns` | get, list, watch, create, update, delete | the runs and their finalizer; create is the scheduler, delete the 30-day TTL |
| `restoreruns` | get, list, watch, update, delete | the runs and their finalizer |
| `backupruns/status`, `restoreruns/status` | patch, update | reporting phase, items and conditions |
| `replicationsources.volsync.backube` | get, list, watch, create, update, patch | writing each enabled claim's source and its manual trigger |
| `replicationdestinations.volsync.backube` | get, list, watch, create, delete | one per fill and per in-place restore |
| `namespaces` | get, list, watch | the schedule, timeout and prune interval annotations |
| `backups.postgresql.cnpg.io` | get, create | a base backup per enabled Cluster per run |
| `clusters.postgresql.cnpg.io` | get, list, delete | the webhook's shared-archive check, a database run, and a database restore deleting its Cluster |
| `objectstores.barmancloud.cnpg.io` | get | the webhook and the restore checks, reading where a Cluster archives |
| `deployments`, `statefulsets` | get, list, patch | quiesce |
| `kustomizations.kustomize.toolkit.fluxcd.io` | get, patch | suspending and resuming a quiesced workload's Kustomization |
| `workloads.kueue.x-k8s.io` | get, create, delete | admitting a run as one Workload |
| `workloads/status` | update | the PodsReady condition the run sets itself |
| `localqueues.kueue.x-k8s.io` | list | finding the namespace's queue |
| `secrets` | get, create, delete | copying the repository Secret for a fill, and the restic and object store reads |
| `events` | create, patch | the recorder the library uses |
| `events.events.k8s.io` | create, patch | an event on a BackupRun or RestoreRun at each new Ready reason |

Narrow `secrets` if it can be narrowed. A ClusterRole that can read every Secret
in the cluster is the one line in this install worth arguing about, and
[decisions.md](decisions.md) records the alternative that avoids it.

## What the install needs on the cluster

| Component | For |
| --- | --- |
| VolSync | the movers every volume backup and restore runs through |
| CloudNativePG and the Barman Cloud plugin | the databases the bootstrap webhook acts on |
| cert-manager | the webhook's serving certificate, issued and renewed with no admin step |

cert-manager is a hard requirement when the webhook is enabled. It issues the
certificate and injects the CA into the MutatingWebhookConfiguration through the
`cert-manager.io/inject-ca-from` annotation, so neither has an expiry date
anyone has to diary. A cluster without cert-manager installs with
`webhook.enabled: false` in the chart, and then a rebuilt cluster brings its
databases back empty; [restores.md](restores.md) says why.

## Versioning

Semver on the tag. The CRD's API version moves on its own: `v1alpha1` until the
shape has survived a cluster rebuild and a restore of something large, then
`v1beta1` with a conversion webhook only if a field has to change incompatibly.
Prefer adding an optional field to changing an existing one.
