# backup-controller

A Helm chart for the backup controller: the Deployment that refills a claim from
its restic repository with a restore Job of its own, runs the namespace backups
and restores, and reports each restore as conditions on the VolumeRestore. The
plain manifests under `deploy/` are the primary install path, and every release
attaches them with the image pinned by digest. This chart exists for clusters
that install with Helm and want the image reference templated. Where this chart
and `deploy/` disagree, `deploy/` is right.

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

   Expected result: `STATUS: deployed`. Without `restoreImage` the render
   fails with `restoreImage is required: set it to the image VolSync runs its
   restic mover in, pinned by digest`.

3. Check the rollout.

   ```
   kubectl -n backup-system get deployment backup-controller
   ```

   Expected result: `READY` reads 1/1.

The chart passes the `namespace` value to the container as `--namespace`, which
is where the controller creates the prime claim, the repository Secret copy and
the restore Job for each claim the populator fills. The default,
`backup-system`, matches the install command above. A release installed into a
different namespace needs `--set namespace=<that namespace>`; a value that
names a namespace which does not exist fails every restore.

The chart passes `restoreImage` as `--restore-image`, the image every restore
Job runs restic in. The controller refuses to start without it.

With `admissionPolicy.create` true, the chart installs the
ValidatingAdmissionPolicy and ValidatingAdmissionPolicyBinding
`<fullname>-restore-jobs`, which let the controller's ServiceAccount create and
change only Jobs of the restore Job's shape, and delete only its own labelled
Jobs. The ClusterRole grants create and delete on every Job in the cluster, and
this policy is what narrows the grant; docs/packaging.md lists its checks.

The controller runs as a non-root user with a read-only root filesystem and no
capabilities, so a namespace with no pod security labels accepts the pod.

## Image reference

The helper resolves the image in this order: `image.digest`, then `image.tag`,
then `v<appVersion>`. Release images are published under a tag with the v
prefix (`v1.2.3`), so the fallback names a tag a release publishes. Release
artifacts default `image.digest` to the pushed digest, so the pin travels with
the chart version and the tag stays for people to read. The packaged chart is
also pushed to `oci://ghcr.io/walzen-group/backup-controller`, so a Flux-style
consumer can pull it by version with that digest intact.

## CRDs

The CRD lives in `crds/`, not in `templates/`. Helm installs that directory
before the templates and never upgrades or deletes it. That is the right
behaviour for a CRD: an upgrade that dropped the CRD would delete every
VolumeRestore in the cluster, release or not.

It also means Helm will not update the schema for you. When a release changes
the CRD schema, apply the new one by hand before or after the upgrade:

```
kubectl apply -f chart/crds/
```

`helm uninstall` removes the release's objects and leaves the CRD and
everything stored under it in place. Delete the CRD yourself only if you mean
to erase every VolumeRestore in the cluster:

```
kubectl delete -f chart/crds/
```

## Values

| Key | Default | Meaning |
| --- | --- | --- |
| image.repository | ghcr.io/walzen-group/backup-controller | Repository the controller image is pulled from |
| image.tag | "" (v<appVersion>) | Image tag when no digest is set |
| image.digest | "" | `sha256:...` pin; when set it wins over the tag |
| image.pullPolicy | IfNotPresent | Pull policy for the controller container |
| nameOverride / fullnameOverride | "" | Name parts used by the object names |
| namespace | backup-system | Namespace the controller creates its prime claims, Secret copies and the populator's restore Jobs in, passed as --namespace |
| restoreImage | "" (required) | Image the restore Jobs run restic in, pinned by digest, passed as --restore-image; the render fails without it |
| webhook.enabled | true | Serve the bootstrap webhook and install its Service and MutatingWebhookConfiguration; with false every Cluster bootstraps as written, so a rebuilt cluster comes back with empty databases |
| webhook.port | 9443 | Port the webhook listens on inside the pod, passed as --webhook-port |
| webhook.certManager.create | true | Create the cert-manager Certificate that fills the Secret <fullname>-webhook-tls and injects the CA; with false, supply that Secret with tls.crt and tls.key and set the caBundle yourself |
| admissionPolicy.create | true | Create the ValidatingAdmissionPolicy and binding that narrow the controller's Job grant to restore Jobs of its own shape; needs Kubernetes 1.30 or later |
| rbac.create | true | Create the ClusterRole and ClusterRoleBinding for the controller |
| serviceAccount.create | true | Create the ServiceAccount |
| serviceAccount.name | "" (the fullname) | Use this ServiceAccount name, created or pre-existing |
| resources | 10m/256Mi requests, 512Mi memory limit | CPU limit is deliberately absent; see values.yaml. The container gets GOMEMLIMIT set to its memory limit, so the Go runtime collects garbage before the heap reaches the limit |
