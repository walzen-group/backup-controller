# Packaging and release

A release is what the infrastructure repository installs. This repository's
release pipeline is modelled on kuport's, which the walzen-group infrastructure
already consumes, so copy that shape.
[walzen-group/kuport](https://github.com/walzen-group/kuport), file
`.github/workflows/release.yaml`, is the reference implementation.
[releasing.md](releasing.md) gives the checks a maintainer runs on the local
e2e cluster before pushing a tag, and the tag and push commands.

## What a release has to attach

A tag `vX.Y.Z` produces three release files and one chart in the registry, and
the infrastructure repository reads the first of them. The release page lists
the three files, and the workflow pushes the chart to the registry:

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
│   ├── restorejob/             building, reading and stopping the restore Job
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
│   ├── admissionpolicy.yaml    the ValidatingAdmissionPolicy and binding on the restore Jobs
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

## Release workflow steps

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
| everything else | ServiceAccount, ClusterRole, ClusterRoleBinding, the ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding `backup-controller-restore-jobs`, Deployment, and the webhook's Service, Issuer, Certificate and MutatingWebhookConfiguration |

Do not split the install across two assets. One file is the whole install, which
is what makes the module a single `data "http"` and a single version string.

The Deployment in the render leaves out `--restore-image`, and the installer
appends it to the `controller` container's args ([Restore image](#restore-image)).

The CRDs are part of every install of a release, the upgrades included. A run
reads the installed CRD of its kind before it changes anything and ends with
reason CRDOutdated when the schema lacks a field the controller writes, because
the API server would drop that field from every write. Applying this file
server-side updates them; Helm upgrades no CRD on its own, and an install done
with `helm upgrade` alone leaves the CRDs at their first version.

## Image

| | |
| --- | --- |
| Registry | `ghcr.io/walzen-group/backup-controller` |
| Platforms | `linux/amd64` is enough for this cluster; add arm64 only when a node needs it |
| Base | scratch, with the public CA roots copied in from the build stage for the webhook's object store calls. The binary carries Go's zone database for `CRON_TZ=` schedules, and needs no shell, no restic and no kubectl |
| User | non-root, no capabilities, read-only root filesystem |

## Restore image

Every restore runs restic in a Job of the controller's own
([architecture.md](architecture.md#how-the-restore-job-is-built)), and the Job's image is
the controller's `--restore-image` flag. The flag has no default. Without it
the controller logs `refusing to start: --restore-image is required: pass the
image VolSync runs its restic mover in, pinned by digest, so restores run the
restic that wrote the backups` and exits, so the pod goes into
CrashLoopBackOff (cmd/backup-controller/flags.go:34).

Pass the image VolSync runs its restic mover in, pinned by digest. VolSync
0.16.0 runs its mover in its own image, `quay.io/backube/volsync:0.16.0`, which
carries restic 0.18.1, and its chart replaces that reference with the value
`restic.image` when set. Declaring the image once and handing it to both
VolSync and the controller keeps the restic that writes the backups the one
that restores them. The Job calls `restic` by name, so any image with restic
0.18.0 or later on its PATH works; the controller holds no restic or VolSync
version of its own.

| Install path | Where the flag comes from |
| --- | --- |
| the rendered manifest | nothing: deploy/deployment.yaml leaves the flag out, and the installer appends `--restore-image=<image>@sha256:<digest>` to the `controller` container's args. The walzen infrastructure repository does that in its backup-controller unit ([upgrading.md](upgrading.md#step-5-change-the-infra-units)) |
| the Helm chart | the value `restoreImage`, which is required: `helm template` fails while it is empty |
| the e2e setup | hack/e2e/backup-controller/backup-controller.sh appends the image hack/e2e/volsync/pins.json pins for VolSync |

## Admission policy on the restore Jobs

The ClusterRole grants create, patch and delete on `jobs` in every namespace,
because RestoreRuns run in the app's namespace. RBAC cannot narrow that grant
to the controller's own Jobs. The release therefore carries a
ValidatingAdmissionPolicy and its binding, both named
`backup-controller-restore-jobs` (deploy/admissionpolicy.yaml). The chart
renders the same objects as `<fullname>-restore-jobs` from
chart/templates/admissionpolicy.yaml, behind `admissionPolicy.create`, true by
default. They need Kubernetes 1.30 or later.

The policy matches only requests whose user is the controller's ServiceAccount:
`system:serviceaccount:backup-system:backup-controller` in deploy/, and in the
chart the release namespace and the chart's ServiceAccount. Other users' Jobs,
the Job controller and the garbage collector are never matched. For that user
the policy checks every create, update and delete of a batch/v1 Job, and
refuses with 403 Forbidden and the message of the first check that failed, in
the order of the table below. The API server reports no other failing check,
so a Job that fails several checks may need more than one fix:

| Check | Message |
| --- | --- |
| the Job carries `app.kubernetes.io/managed-by=backup-controller` and `app.kubernetes.io/component=restore` | `backup-controller may create, change and delete only Jobs labelled app.kubernetes.io/managed-by=backup-controller and app.kubernetes.io/component=restore` |
| an update changes a Job the controller created | `backup-controller may change only a Job it created` |
| the pod template carries `app.kubernetes.io/component=restore` | `a restore Job's pods carry app.kubernetes.io/component=restore` |
| `automountServiceAccountToken: false` | `a restore Job's pod mounts no ServiceAccount token` |
| no ServiceAccount but the namespace's `default` | `a restore Job's pod runs as its namespace's default ServiceAccount` |
| no host network, PID or IPC, no `nodeName`, no ephemeral containers, no resource claims | `a restore Job's pod uses no host namespace, names no node, and claims no device` |
| no `runtimeClassName`, no `priorityClassName` | `a restore Job's pod names no runtime class or priority class` |
| no `container.apparmor.security.beta.kubernetes.io/` annotation on the pod template | `a restore Job's pod template carries no AppArmor annotation` |
| volumes are only claims, emptyDir, and ephemeral claims without a data source | `a restore Job's pod mounts only claims, emptyDir and ephemeral claims without a data source` |
| the pod security context sets no sysctls, no SELinux options, and no Unconfined seccomp or AppArmor profile | `a restore Job's pod security context sets no sysctls, SELinux options or unconfined profile` |
| every container and init container drops ALL, adds at most CHOWN, DAC_OVERRIDE and FOWNER, is not privileged, forbids privilege escalation, has a read-only root filesystem, sets no SELinux options, no Unconfined profile and no procMount but Default, and uses no volume device and no host port | `a restore Job's containers are unprivileged: drop ALL, add at most CHOWN, DAC_OVERRIDE and FOWNER, no privilege escalation, read-only root, no host port or device` |

The pod security context comes from the VolumeRestore's or the RestoreRun's
`moverSecurityContext`, so a context that sets sysctls, SELinux options or an
unconfined profile is refused: a RestoreRun item fails with reason
RestoreJobRefused and the API server's answer, and a claim the populator fills
stays Pending with RestoreJobRefused on its VolumeRestore. A context of
`runAsUser`, `runAsGroup` and `fsGroup` passes.

The policy does not check the image, or the environment the Job reads from the
repository Secret. [decisions.md](decisions.md#narrow-the-jobs-grant-with-an-admission-policy)
records why, and what that leaves the ServiceAccount able to do.

The username in the policy's match condition is written for the namespace
backup-system and the ServiceAccount backup-controller. An install that
renames either has to change the policy too, or the policy matches no request
and admits every Job the ServiceAccount creates, the way RBAC alone would.
The chart builds the username from the release namespace and its
ServiceAccount value.

## RBAC the controller needs

`deploy/rbac.yaml` writes it as one ClusterRole, and every rule traces to a
caller in the controller:

| Resources | Verbs | For |
| --- | --- | --- |
| `persistentvolumeclaims` | get, list, watch, create, patch, delete | the app's claim, the prime claim, and an `into:` scratch claim; the populator records its restore Job's UID on the prime claim with a patch; the orphan reconciler watches claims being deleted, deletes a claim's prime claim, and patches the library's finalizer off a claim whose VolumeRestore is gone |
| `persistentvolumeclaims/finalizers` | update | each ReplicationSource names its claim as owner with `blockOwnerDeletion`, which the OwnerReferencesPermissionEnforcement admission plugin allows only to a writer that may update the owner's finalizers |
| `persistentvolumes` | get, list, watch, patch | rebinding the volume to the app's claim, and reading a volume's node for its mover |
| `storageclasses` | get, list, watch | reading the binding mode |
| `pods` | get, list, watch | the library's pod informer, which it builds and waits on whether or not a populator pod is used, a RestoreRun finding the pod that holds a claim, and the stop of a restore Job, which lists the Job's pods by their controller-uid label, and a claim's restore pods by `backup.wlz.li/restore-claim`, and waits while one may still write |
| `volumerestores` (our group) | get, list, watch, update | reading the data source, adding and removing the `backup.wlz.li/volume-populator` finalizer, and the orphan reconciler's check that a claim's VolumeRestore is gone and its watch for deleted ones |
| `volumerestores/status` | patch, update | reporting conditions |
| `backupruns` | get, list, watch, create, update, delete | the runs and their finalizer; create is the scheduler, delete the 30-day TTL |
| `restoreruns` | get, list, watch, update, delete | the runs and their finalizer |
| `backupruns/status`, `restoreruns/status` | patch, update | reporting phase, items and conditions |
| `backupruns/finalizers`, `restoreruns/finalizers` | update | the Workload, the scratch claim and the restore Job a run creates name the run as their controller with `blockOwnerDeletion`, which OwnerReferencesPermissionEnforcement allows only with this verb |
| `customresourcedefinitions` (apiextensions.k8s.io), named `backupruns.backup.wlz.li` and `restoreruns.backup.wlz.li` | get | a run reads the installed CRD of its kind before it changes anything, and ends with reason CRDOutdated when the schema lacks a field the controller writes, which the API server would drop |
| `replicationsources.volsync.backube` | get, list, watch, create, update, patch | writing each enabled claim's source and its manual trigger; v0.8.2 dropped delete, which v0.8.0 and v0.8.1 used after a failed mover |
| `leases` (coordination.k8s.io) | get, list, create, update, delete | the Lease a BackupRun or RestoreRun acquires on a claim and on its repository Secret right before it starts a mover or a restore Job, so a backup and a restore of either never run at once, and the Lease `backup-controller-quiesce` a run acquires in its namespace before it stops that namespace's workloads (internal/runs/lease.go). Runs live in every namespace, so the rule is cluster-wide, and `update` takes over the Lease of a run that has finished |
| `jobs` (batch) | get, list, create, patch, delete | the restore Job of a RestoreRun item or a populated claim: created suspended, read by name, resumed and suspended with a merge patch, and deleted with Foreground propagation; a backup lists the restore Jobs by label to wait for one on its claim or repository. Reads go through the uncached reader, so no watch. The [admission policy](#admission-policy-on-the-restore-jobs) narrows this grant to Jobs of the restore Job's shape |
| `namespaces` | get, list, watch | the schedule, timeout and prune interval annotations |
| `backups.postgresql.cnpg.io` | get, create | a base backup per enabled Cluster per run |
| `clusters.postgresql.cnpg.io` | get, list, delete | the webhook's shared-archive check, a database run, and a database restore deleting its Cluster |
| `objectstores.barmancloud.cnpg.io` | get, list | the webhook and the restore checks reading where a Cluster archives, and the webhook's shared-archive check listing every ObjectStore once per create. Without list, every Cluster create is refused with an HTTP 500 while any other Cluster archives |
| `deployments`, `statefulsets` | get, list | quiesce reads the workloads it stops; the controller has no write verb on them |
| `deployments/scale`, `statefulsets/scale` | get, update | quiesce sets the replica count through the scale subresource, which can change nothing else (internal/runs/scale.go) |
| `kustomizations.kustomize.toolkit.fluxcd.io` | get, patch | suspending and resuming a quiesced workload's Kustomization |
| `workloads.kueue.x-k8s.io` | get, create, delete | admitting a run as one Workload |
| `workloads/status` | update | the PodsReady condition the run sets itself |
| `localqueues.kueue.x-k8s.io` | list | finding the namespace's queue |
| `secrets` | get, create, delete | copying the repository Secret for a fill and deleting the copy afterwards, the orphan reconciler deleting the copy of a claim whose VolumeRestore is gone, and the restic and object store reads |
| `events` | create, patch | the recorder the library uses |
| `events.events.k8s.io` | create, patch | an event on a BackupRun or RestoreRun at each new Ready reason, and the orphan reconciler's WaitingForMover and DataSourceGone events on a claim |

Narrow `secrets` if it can be narrowed. A ClusterRole that can read every Secret
in the cluster is the one line in this install worth arguing about, and
[decisions.md](decisions.md) records the alternative that avoids it.

## What the install needs on the cluster

| Component | For |
| --- | --- |
| VolSync | the mover every volume backup runs through; restores run in the controller's own Job |
| Kubernetes 1.30 or later | ValidatingAdmissionPolicy, which the release applies with the workload |
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
