# Test cluster

The end-to-end scenarios under test/e2e run on the kind cluster of Docker
Desktop, kubectl context docker-desktop. They drive the controller the way
a user does and check the data itself: the file in a claim, the rows in a
database. No part of Kubernetes, S3, restic, VolSync or CloudNativePG is
faked.

## Set up the cluster

1. Install the stack. Each step is safe to run again.

   ```
   hack/kind/up.sh
   ```

   Expected result: the last line reads `done: csi certmanager s3 cnpg volsync kueue flux cluster`.

2. Build the controller from this tree and deploy it.

   ```
   hack/kind/controller.sh deploy
   ```

   Expected result: the last line names the image it runs,
   `running backup-controller:dev-<id>`.

Run step 2 again after every change to the code. It builds a new image, and
the Deployment rolls to it.

## Run the scenarios

All of them:

```
make e2e
```

One of them:

```
go test -tags e2e -count=1 -timeout 60m -v ./test/e2e/ -run TestADeletedClusterComesBackWithItsData
```

A failed scenario prints the runs, Clusters, claims, pods and events of its
namespace and the end of the controller's log. Each scenario deletes its
namespace when it ends.

## What the cluster holds

| Component | Version | Why |
| --- | --- | --- |
| csi-driver-host-path, external-snapshotter | v1.18.0, v8.6.0 | claims, snapshots and clones for VolSync (prod runs zfs-localpv) |
| cert-manager | v1.21.2 | the certificate of the controller's webhook |
| RustFS | 1.0.0 | S3, namespace s3, buckets volsync and postgres |
| CloudNativePG, barman-cloud plugin | 1.30.0, v0.15.0 | the databases and their archives |
| VolSync | 0.16.0 | the backup movers, with restic 0.18.1 |
| Kueue | 0.19.5 | ClusterQueue backup, which admits 5 runs at once |
| Flux | v2.9.5 | applies each test app from the in-cluster registry |
| registry | 3.0.0 | holds the test apps as OCI artifacts |

hack/kind/versions.env pins these versions for the test cluster only; the
controller itself works with the versions the cluster serves.
