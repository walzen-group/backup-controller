# backup-controller

A Helm chart for the backup controller, which backs up a namespace's claims
and CloudNativePG databases and restores them (see the repository's README). The plain manifests under `deploy/` are the primary install
path, and every release attaches them with the image pinned by digest. This
chart exists for clusters that install with Helm and want the image reference
templated. Where this chart and `deploy/` disagree, `deploy/` is right.

## Requirements

- VolSync, CloudNativePG with the barman-cloud plugin, Kueue with a
  ClusterQueue that covers backup-controller.wlz.li/run, and cert-manager
  (docs/installing.md in the repository).
- VolSync's mover image for the restoreImage value; the controller restores
  volumes with the restic inside it.
- A cluster-admin for the first install: the chart registers three CRDs.

## Install

1. Create the namespace the controller runs in.

   ```
   kubectl create namespace backup-system
   ```

   Expected result: `namespace/backup-system created`.

2. Install the release into that namespace.

   ```
   helm install backup-controller ./chart -n backup-system --set restoreImage=quay.io/backube/volsync:0.16.0
   ```

   Expected result: `STATUS: deployed`.

3. Check the rollout.

   ```
   kubectl -n backup-system get deployment backup-controller
   ```

   Expected result: `READY` reads 2/2. One pod holds the Lease
   backup-controller and runs the controllers; both answer the webhook.

The chart passes the `namespace` value to the container as `--namespace`, which
is where the controller creates the prime claim, the repository Secret copy and
the restore Job for each claim the populator fills. The default, `backup-system`,
matches the install command above. A release installed into a different
namespace needs `--set namespace=<that namespace>`; a value that names a
namespace which does not exist fails every restore.

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
| namespace | backup-system | Namespace the controller creates its prime claim and destination in, passed as --namespace |
| replicas | 2 | Pods of the controller, each on its own node; one holds the Lease and runs the controllers, all answer the webhook |
| rbac.create | true | Create the ClusterRole and ClusterRoleBinding for the controller |
| serviceAccount.create | true | Create the ServiceAccount |
| serviceAccount.name | "" (the fullname) | Use this ServiceAccount name, created or pre-existing |
| resources | 10m/64Mi requests, 128Mi memory limit | CPU limit is deliberately absent; see values.yaml |
