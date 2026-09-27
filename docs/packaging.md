# Packaging and release

A release is what the infrastructure repository installs. The release pipeline
of this repository uses the pipeline of kuport as its model. The walzen-group
infrastructure already uses the releases of kuport, so copy that shape.
[walzen-group/kuport](https://github.com/walzen-group/kuport), file
`.github/workflows/release.yaml`, is the reference implementation.
[releasing.md](releasing.md) gives the checks that a maintainer runs on the
local e2e cluster before a tag push. It also gives the tag and push commands.

## What a release has to attach

A tag `vX.Y.Z` makes three release files and one chart in the registry. The
infrastructure repository reads the first file. The release page lists the
three files, and the workflow pushes the chart to the registry:

| Asset | Contents | Consumed by |
| --- | --- | --- |
| `backup-controller-X.Y.Z.yaml` | the whole install rendered from `deploy/`, with the image pinned by digest and the CRD included | the terragrunt module, over HTTP |
| `crds-X.Y.Z.yaml` | the CRDs alone, which version on their own schedule | a cluster that applies schemas ahead of workloads |
| `backup-controller-X.Y.Z.tgz` | the Helm chart, with `image.digest` baked in | anyone installing with Helm |
| `oci://ghcr.io/walzen-group/backup-controller` | the same chart pushed as an OCI artifact | a Flux HelmRelease consumer |

The rendered manifest is the primary path. If the chart and `deploy/` do not
agree, `deploy/` is right. The README of the chart says this, as the README
of kuport does.

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

Copy the workflow of kuport and change the names. The purpose of each step is:

1. **Check gate.** `go vet`, `go test -race`, `golangci-lint run`, `go build`.
   Each command is identical to `ci.yaml`. A tag that does not pass CI does
   not become a release.
2. **Reject a tag that is not plain semver.** The workflow of kuport refuses
   prerelease and build-metadata suffixes. Such suffixes overwrite the rolling
   minor tag and cannot make a valid chart version. Keep the same guard.
3. **Build and push the image**, tagged `vX.Y.Z` and `X.Y`, to
   `ghcr.io/walzen-group/backup-controller`.
4. **Report the digest** in the job summary. The digest is the half of the pin
   that decides what a cluster runs.
5. **Render `deploy/` with the image pinned by digest.** Run `kustomize edit
   set image`, then `kubectl kustomize deploy/`. Then run a
   `grep -q "@sha256:"` that fails the run when the render did not use the
   override. That grep finds a rendered manifest that still uses only a tag.
6. **Package the chart** with `--version` and `--app-version` from the tag.
   Before that, patch `image.digest` into a temporary copy of `chart/`. The
   workflow never changes the chart in the repository.
7. **Push the chart to ghcr.io** as an OCI artifact.
8. **Bundle the CRDs**: concatenate `config/crd/*.yaml`.
9. **Create the GitHub Release** with the three file assets attached and the
   digest in the body. Step 7 already pushed the chart to the registry. Thus,
   the release page lists three files.

## What the rendered manifest must contain

The terragrunt module splits the documents by kind and applies them in three
groups. Thus, the render must contain all of them:

| Kind | Why the module needs it in the same asset |
| --- | --- |
| CustomResourceDefinition | the module applies it first and waits for `Established`. Thus, the same apply can create a VolumeRestore |
| Namespace | the module applies it before the workload |
| all other kinds | ServiceAccount, ClusterRole, ClusterRoleBinding, the ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding `backup-controller-restore-jobs`, Deployment, and the Service, Issuer, Certificate and MutatingWebhookConfiguration of the webhook |

Do not split the install across two assets. One file is the full install.
Thus, the module is a single `data "http"` and a single version string.

The Deployment in the render does not contain `--restore-image`. The installer
appends it to the args of the `controller` container
([Restore image](#restore-image)).

The CRDs are part of every install of a release. This includes upgrades.
Before a run changes anything, it reads the installed CRD of its kind. The run
ends with reason CRDOutdated when the schema does not have a field that the
controller writes, because the API server would drop that field from every
write. A server-side apply of this file updates the CRDs. Helm does not
upgrade a CRD on its own. If you install only with `helm upgrade`, the CRDs
stay at their first version.

## Image

| | |
| --- | --- |
| Registry | `ghcr.io/walzen-group/backup-controller` |
| Platforms | `linux/amd64` is sufficient for this cluster. Add arm64 only when a node needs it |
| Base | scratch, with the public CA roots copied in from the build stage. The webhook uses them for its object store calls. The binary contains the zone database of Go for `CRON_TZ=` schedules. It needs no shell, no restic and no kubectl |
| User | non-root, no capabilities, read-only root filesystem |

## Restore image

Every restore runs restic in a Job of the controller
([architecture.md](architecture.md#how-the-restore-job-is-built)). The image of
the Job is the `--restore-image` flag of the controller. The flag has no
default. Without it, the controller logs `refusing to start: --restore-image is required: pass the
image VolSync runs its restic mover in, pinned by digest, so restores run the
restic that wrote the backups` and exits. Then the pod goes into
CrashLoopBackOff (cmd/backup-controller/flags.go:34).

Give the image that VolSync runs its restic mover in, pinned by digest.
VolSync 0.16.0 runs its mover in its own image,
`quay.io/backube/volsync:0.16.0`, which contains restic 0.18.1. When the value
`restic.image` is set, the VolSync chart replaces that reference with it.
Declare the image once and give it to both VolSync and the controller. Then
the restic that writes the backups is also the restic that restores them. The
Job calls `restic` by name. Thus, any image with restic 0.18.0 or later on its
PATH works. The controller has no restic or VolSync version of its own.

| Install path | Where the flag comes from |
| --- | --- |
| the rendered manifest | nothing: deploy/deployment.yaml does not contain the flag. The installer appends `--restore-image=<image>@sha256:<digest>` to the args of the `controller` container. The walzen infrastructure repository does that in its backup-controller unit ([upgrading.md](upgrading.md#step-5-change-the-infra-units)) |
| the Helm chart | the value `restoreImage`, which is required: `helm template` fails while it is empty |
| the e2e setup | hack/e2e/backup-controller/backup-controller.sh appends the image that hack/e2e/volsync/pins.json pins for VolSync |

## Admission policy on the restore Jobs

The ClusterRole grants create, patch and delete on `jobs` in every namespace,
because RestoreRuns run in the namespace of the app. RBAC cannot limit that
grant to the Jobs of the controller. Thus, the release contains a
ValidatingAdmissionPolicy and its binding, both with the name
`backup-controller-restore-jobs` (deploy/admissionpolicy.yaml). The chart
renders the same objects as `<fullname>-restore-jobs` from
chart/templates/admissionpolicy.yaml. The value `admissionPolicy.create`
controls them, and it is true by default. They need Kubernetes 1.30 or later.

The policy matches only requests whose user is the ServiceAccount of the
controller. In deploy/, this user is
`system:serviceaccount:backup-system:backup-controller`. In the chart, the
user comes from the release namespace and the ServiceAccount of the chart. The
policy never matches the Jobs of other users, the Job controller or the
garbage collector.

For that user, the policy checks every create, update and delete of a
batch/v1 Job. It refuses with 403 Forbidden and the message of the first check
that failed, in the order of the table below. The API server reports no other
failed check. Thus, a Job that fails several checks may need more than one
fix:

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
every container and init container obeys these rules. It drops ALL and adds at most CHOWN, DAC_OVERRIDE and FOWNER. It is not privileged and forbids privilege escalation. It has a read-only root filesystem. It sets no SELinux options, no Unconfined profile and no procMount other than Default. It uses no volume device and no host port | `a restore Job's containers are unprivileged: drop ALL, add at most CHOWN, DAC_OVERRIDE and FOWNER, no privilege escalation, read-only root, no host port or device` |

The pod security context comes from the `moverSecurityContext` of the
VolumeRestore or the RestoreRun. The policy refuses a context that sets
sysctls, SELinux options or an unconfined profile. Then these results occur:

- A RestoreRun item fails with reason RestoreJobRefused and the answer of the
  API server.
- A claim that the populator fills stays Pending, with RestoreJobRefused on
  its VolumeRestore.

A context of `runAsUser`, `runAsGroup` and `fsGroup` passes.

The policy does not check the image. It also does not check the environment
that the Job reads from the repository Secret.
[decisions.md](decisions.md#narrow-the-jobs-grant-with-an-admission-policy)
records why, and what the ServiceAccount can still do.

The username in the match condition of the policy is for the namespace
backup-system and the ServiceAccount backup-controller. If an install renames
either, it must also change the policy. If not, the policy matches no request.
It then admits every Job that the ServiceAccount creates, as RBAC alone would.
The chart makes the username from the release namespace and its
ServiceAccount value.

## RBAC the controller needs

`deploy/rbac.yaml` writes it as one ClusterRole. Each rule has a caller in the
controller:

| Resources | Verbs | For |
| --- | --- | --- |
| `persistentvolumeclaims` | get, list, watch, create, patch, delete | the claim of the app, the prime claim, and an `into:` scratch claim. The populator records the UID of its restore Job on the prime claim with a patch. The orphan reconciler watches claims that are in deletion. It deletes the prime claim of a claim. It also patches the finalizer of the library off a claim whose VolumeRestore is gone |
| `persistentvolumeclaims/finalizers` | update | each ReplicationSource names its claim as owner with `blockOwnerDeletion`. The OwnerReferencesPermissionEnforcement admission plugin allows this only to a writer that may update the finalizers of the owner |
| `persistentvolumes` | get, list, watch, patch | bind the volume to the claim of the app again, and read the node of a volume for its mover |
| `storageclasses` | get, list, watch | read the binding mode |
| `pods` | get, list, watch | the pod informer of the library, which it builds and waits on in all cases, with or without a populator pod. A RestoreRun finds the pod that holds a claim. The stop of a restore Job lists the pods of the Job by their controller-uid label, and the restore pods of a claim by `backup.wlz.li/restore-claim`. It waits while one of them may still write |
| `volumerestores` (our group) | get, list, watch, update | read the data source, and add and remove the `backup.wlz.li/volume-populator` finalizer. The orphan reconciler checks that the VolumeRestore of a claim is gone, and it watches for deleted VolumeRestores |
| `volumerestores/status` | patch, update | report conditions |
| `backupruns` | get, list, watch, create, update, delete | the runs and their finalizer. The scheduler uses create. The 30-day TTL uses delete |
| `restoreruns` | get, list, watch, update, delete | the runs and their finalizer |
| `backupruns/status`, `restoreruns/status` | patch, update | report phase, items and conditions |
| `backupruns/finalizers`, `restoreruns/finalizers` | update | a run creates the Workload, the scratch claim and the restore Job. They name the run as their controller with `blockOwnerDeletion`. OwnerReferencesPermissionEnforcement allows this only with this verb |
| `customresourcedefinitions` (apiextensions.k8s.io), named `backupruns.backup.wlz.li` and `restoreruns.backup.wlz.li` | get | before a run changes anything, it reads the installed CRD of its kind. It ends with reason CRDOutdated when the schema does not have a field that the controller writes, because the API server would drop that field |
| `replicationsources.volsync.backube` | get, list, watch, create, update, patch | write the source of each enabled claim and its manual trigger. v0.8.2 dropped delete, which v0.8.0 and v0.8.1 used after a failed mover |
| `leases` (coordination.k8s.io) | get, list, create, update, delete | a BackupRun or RestoreRun acquires a Lease on a claim and on its repository Secret immediately before it starts a mover or a restore Job. Thus, a backup and a restore of the claim or the repository never run at the same time. A run also acquires the Lease `backup-controller-quiesce` in its namespace before it stops the workloads of that namespace (internal/runs/lease.go). Runs are in every namespace, so the rule is cluster-wide. `update` takes the Lease of a run that has finished |
| `jobs` (batch) | get, list, create, patch, delete | the restore Job of a RestoreRun item or a populated claim. The controller creates it suspended, reads it by name, resumes and suspends it with a merge patch, and deletes it with Foreground propagation. A backup lists the restore Jobs by label, to wait for one on its claim or repository. Reads go through the uncached reader, so there is no watch. The [admission policy](#admission-policy-on-the-restore-jobs) limits this grant to Jobs with the shape of the restore Job |
| `namespaces` | get, list, watch | the schedule, timeout and prune interval annotations |
| `backups.postgresql.cnpg.io` | get, create | a base backup per enabled Cluster per run |
| `clusters.postgresql.cnpg.io` | get, list, delete | the shared-archive check of the webhook, a database run, and a database restore that deletes its Cluster |
| `objectstores.barmancloud.cnpg.io` | get, list | the webhook and the restore checks read where a Cluster archives. The shared-archive check of the webhook lists every ObjectStore once per create. Without list, the webhook refuses every Cluster create with an HTTP 500 while any other Cluster archives |
| `deployments`, `statefulsets` | get, list | quiesce reads the workloads that it stops. The controller has no write verb on them |
| `deployments/scale`, `statefulsets/scale` | get, update | quiesce sets the replica count through the scale subresource, which can change nothing else (internal/runs/scale.go) |
| `kustomizations.kustomize.toolkit.fluxcd.io` | get, patch | suspend and resume the Kustomization of a quiesced workload |
| `workloads.kueue.x-k8s.io` | get, create, delete | admit a run as one Workload |
| `workloads/status` | update | the PodsReady condition that the run sets itself |
| `localqueues.kueue.x-k8s.io` | list | find the queue of the namespace |
| `secrets` | get, create, delete | copy the repository Secret for a fill and delete the copy after the fill. The orphan reconciler deletes the copy of a claim whose VolumeRestore is gone. The restic and object store reads also use it |
| `events` | create, patch | the recorder that the library uses |
| `events.events.k8s.io` | create, patch | an event on a BackupRun or RestoreRun at each new Ready reason, and the WaitingForMover and DataSourceGone events of the orphan reconciler on a claim |

Limit `secrets` if possible. A ClusterRole that can read every Secret in the
cluster is the one line in this install that needs discussion.
[decisions.md](decisions.md) records the alternative that avoids it.

## What the install needs on the cluster

| Component | For |
| --- | --- |
| VolSync | the mover that every volume backup runs through. Restores run in the Job of the controller |
| Kubernetes 1.30 or later | ValidatingAdmissionPolicy, which the release applies with the workload |
| CloudNativePG and the Barman Cloud plugin | the databases that the bootstrap webhook acts on |
| cert-manager | the serving certificate of the webhook. cert-manager issues and renews it with no admin step |

cert-manager is a hard requirement when the webhook is enabled. It issues the
certificate. It injects the CA into the MutatingWebhookConfiguration through
the `cert-manager.io/inject-ca-from` annotation. Thus, no person must record an
expiry date for either. A cluster without cert-manager installs with
`webhook.enabled: false` in the chart. Then a rebuilt cluster brings its
databases back empty. [restores.md](restores.md) says why.

## Versioning

Use semver on the tag. The API version of the CRD changes on its own schedule.
It stays `v1alpha1` until the shape has survived a cluster rebuild and a
restore of something large. It then becomes `v1beta1`, with a conversion
webhook only if a field must change incompatibly. Add an optional field in
preference to a change of a field that exists.
