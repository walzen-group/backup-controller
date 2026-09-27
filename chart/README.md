# backup-controller

A Helm chart for the backup controller. The chart installs the Deployment of the
controller, which does three things:

- It refills a claim from its restic repository with a restore Job of its own.
- It runs the namespace backups and restores.
- It reports each restore as conditions on the VolumeRestore.

The plain manifests under `deploy/` are the primary install path. Every release
attaches them with the image pinned by digest. This chart is for clusters that
install with Helm and want a templated image reference. Where this chart and
`deploy/` disagree, `deploy/` is right.

## Requirements

- VolSync, whose `replicationsources.volsync.backube` kind the controller
  writes for every backup.
- A restic image to restore with, pinned by digest, for the required value
  `restoreImage`: the image VolSync runs its restic mover in, so a restore runs
  the restic that wrote the backup.
- Kubernetes 1.30 or later, for the ValidatingAdmissionPolicy the chart
  installs on the controller's restore Jobs.
- A restic repository Secret in the namespace of each claim that restores. The
  controller copies that Secret into its own namespace for the length of a
  restore.
- A cluster-admin for the first install: the chart registers the CRDs.

## Install

1. Create the namespace the controller runs in.

   ```
   kubectl create namespace backup-system
   ```

   Expected result: `namespace/backup-system created`.

2. Install the release into that namespace, with the restic image.

   ```
   helm install backup-controller ./chart -n backup-system --set restoreImage=quay.io/backube/volsync:0.16.0@sha256:<digest>
   ```

   Expected result: `STATUS: deployed`. Without `restoreImage`, the render
   fails with `restoreImage is required: set it to the image VolSync runs its
   restic mover in, pinned by digest`.

3. Check the rollout.

   ```
   kubectl -n backup-system get deployment backup-controller
   ```

   Expected result: `READY` reads 1/1.

The chart passes the `namespace` value to the container as `--namespace`. In
that namespace, the controller creates the prime claim, the repository Secret
copy and the restore Job for each claim that the populator fills. The default,
`backup-system`, matches the install command above. A release in a different
namespace needs `--set namespace=<that namespace>`. If the value names a
namespace that does not exist, every restore fails.

The chart passes `restoreImage` as `--restore-image`. It is the image that every
restore Job runs restic in. The controller refuses to start without it.

With `admissionPolicy.create` true, the chart installs the
ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding
`<fullname>-restore-jobs`. They let the controller's ServiceAccount create and
change only Jobs of the restore Job's shape, and delete only its own labelled
Jobs. The ClusterRole grants create and delete on every Job in the cluster. This
policy narrows the grant. docs/packaging.md lists its checks.

The controller runs as a non-root user with a read-only root filesystem and no
capabilities, so a namespace with no pod security labels accepts the pod.

## Image reference

The helper resolves the image in this order: `image.digest`, then `image.tag`,
then `v<appVersion>`. A release publishes its images under a tag with the v
prefix (`v1.2.3`). Thus the fallback names a tag that a release publishes. The
release artifacts set the default of `image.digest` to the pushed digest. Thus
the pin travels with the chart version, and the tag stays for people to read.
The release also pushes the packaged chart to
`oci://ghcr.io/walzen-group/backup-controller`. A Flux-style consumer can thus
pull it by version with that digest intact.

## CRDs

The CRD is in `crds/`. It is not in `templates/`. Helm installs that directory
before the templates and never upgrades or deletes it. That is the correct
behaviour for a CRD. An upgrade that dropped the CRD would delete every
VolumeRestore in the cluster, whether it belongs to the release or not.

Thus Helm does not update the schema for you. If a release changes the CRD
schema, apply the new one by hand before or after the upgrade:

```
kubectl apply -f chart/crds/
```

`helm uninstall` deletes the release's objects. It leaves the CRD and all the
objects stored under it in place. Delete the CRD yourself only if you want to
delete every VolumeRestore in the cluster:

```
kubectl delete -f chart/crds/
```

## Values

| Key | Default | Meaning |
| --- | --- | --- |
| image.repository | ghcr.io/walzen-group/backup-controller | Repository to pull the controller image from |
| image.tag | "" (v<appVersion>) | Image tag when no digest is set |
| image.digest | "" | `sha256:...` pin. When set, it wins over the tag |
| image.pullPolicy | IfNotPresent | Pull policy for the controller container |
| nameOverride / fullnameOverride | "" | Name parts used by the object names |
| namespace | backup-system | Namespace the controller creates its prime claims, Secret copies and the populator's restore Jobs in, passed as --namespace |
| restoreImage | "" (required) | Image the restore Jobs run restic in, pinned by digest, passed as --restore-image. The render fails without it |
| webhook.enabled | true | Serve the bootstrap webhook and install its Service and MutatingWebhookConfiguration. With false, every Cluster bootstraps as written. Then a rebuilt cluster comes back with empty databases |
| webhook.port | 9443 | Port the webhook listens on inside the pod, passed as --webhook-port |
| webhook.certManager.create | true | Create the cert-manager Certificate that fills the Secret <fullname>-webhook-tls and injects the CA. With false, supply that Secret with tls.crt and tls.key, and set the caBundle yourself |
| admissionPolicy.create | true | Create the ValidatingAdmissionPolicy and binding that narrow the controller's Job grant to restore Jobs of its own shape. Needs Kubernetes 1.30 or later |
| rbac.create | true | Create the ClusterRole and ClusterRoleBinding for the controller |
| serviceAccount.create | true | Create the ServiceAccount |
| serviceAccount.name | "" (the fullname) | Use this ServiceAccount name, created or pre-existing |
| resources | 10m/256Mi requests, 512Mi memory limit | The chart sets no CPU limit on purpose. See values.yaml. The container gets GOMEMLIMIT set to its memory limit. Thus the Go runtime collects garbage before the heap reaches the limit |
