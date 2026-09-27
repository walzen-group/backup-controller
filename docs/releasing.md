# Releasing

A release starts when a person pushes a tag `vX.Y.Z`. GitHub's release
workflow then publishes the image, the rendered manifests, the chart and the
CRD bundle. [packaging.md](packaging.md) lists what it attaches and what each
of its steps does. GitHub CI runs only the fast tier (check, verify, envtest,
the chart renders and the image build). The e2e suites and the demo need a real
cluster with the real VolSync, CloudNativePG, Kueue and Flux. Thus they run on
the maintainer's machine, by hand, before every tag. No CI job, cron entry or
timer runs them on a schedule.

## Local e2e cluster

The e2e cluster is the Kubernetes cluster that Docker Desktop runs with its kind
provisioner. kubectl reaches it through the context docker-desktop, and every
script under hack/e2e passes `--context docker-desktop` on each call. The
cluster runs Kubernetes 1.36.4, with kindnet and the local-path StorageClass
standard.

You need to create that cluster once, by hand, in Docker Desktop's Kubernetes
settings. The scripts never create, reset or delete it. They only install components into
it and uninstall them. `make e2e-down` deletes every component and leaves the
cluster in place.

`make e2e-up` installs these components, in this order, each from
hack/e2e/<name>/<name>.sh with its versions in hack/e2e/<name>/pins.json:

| Component | What it installs |
| --- | --- |
| cert-manager | cert-manager 1.21.2 in namespace cert-manager |
| csi | the external-snapshotter CRDs, snapshot-controller and csi-driver-host-path in namespace e2e-csi, with StorageClass and VolumeSnapshotClass e2e-hostpath |
| rustfs | RustFS 1.0.0 in namespace e2e-s3, serving http://rustfs.e2e-s3.svc:9000 with the buckets volsync and postgres |
| volsync | the VolSync 0.16.0 manager, whose image is also the restic mover, in namespace volsync-system |
| kueue | Kueue 0.19.5 in namespace kueue-system, with ClusterQueue e2e |
| cnpg | the CloudNativePG 1.30.0 operator and the barman-cloud plugin 0.15.0 in namespace cnpg-system |
| flux | Flux 2.9.5 source-controller and kustomize-controller in namespace flux-system, plus the OCI registry e2e-registry |
| backup-controller | the controller built from this worktree, as image backup-controller:e2e, in namespace backup-system |

After it installs each component, e2e-up runs the check of that component. It
is the same check that `make e2e-check` runs.

The whole setup stays under 5 GB of storage. csi-driver-host-path runs with
`--capacity=e2e=5Gi` and `--max-volume-size=1073741824`. Thus it refuses a
claim on e2e-hostpath past 5Gi in total or past 1Gi on its own. RustFS keeps
its data on an emptyDir with a cap of 1Gi. The driver does not count snapshots
against the cap. It does not limit what a pod writes into a volume. Thus a test
that fills a volume can still exceed the cap.

### Fetch the pinned images and charts

Run this once, and again after any pins.json changes:

```
make e2e-fetch
```

e2e-fetch is the only e2e target that downloads. It does these steps:

1. It pulls every image by digest into Docker Desktop's engine.
2. It saves the charts and manifests under hack/e2e/<name>/cache or .tmp/e2e,
   and checks their sha256.
3. It builds the controller image from the repository's Dockerfile.

e2e-up, e2e and demo work from what it saved.

### Install the components

```
make e2e-up
```

Expected result: every `== check <name>` section ends without an error.

A cluster that already has the components installed needs only
`make e2e-check`.

## Before every tag

Run every step on the commit that you will tag, from a clean tree. Install the
e2e components as described above first. Every step must pass. If one step
fails, fix the cause, commit, and start again from step 1 on the new commit.

### Step 1: Run the fast tier

```
make check
```

```
make verify
```

```
make envtest
```

Expected result: all three exit 0. CI runs these gates on every push. The
release workflow runs the four Go commands of `make check` again before it
publishes.

### Step 2: Put the tagged code into the cluster

```
nix develop -c hack/e2e/backup-controller/backup-controller.sh rebuild
```

rebuild does these steps:

1. It builds the image from the working tree, uncommitted changes included.
2. It points the Deployment backup-controller in namespace backup-system at the
   image.
3. It waits for the rollout.

Each build gets a new tag, backup-controller:e2e-<image id>. Thus the kubelet
always runs the image that the script just built. Without this step, the e2e
suites test the code of the last build in the cluster.

Expected result: the rollout completes and the script exits 0.

### Step 3: Run the e2e suites

```
make e2e
```

Expected result: `ok` for every package with e2e tests. The run has a 60
minute limit.

### Step 4: Run the demo

```
make demo
```

The demo does these steps:

1. It backs up a small app on its schedule.
2. It changes the app's data.
3. It restores the backup in place.
4. It compares the data with the backed-up data.
5. It deletes what it created.

The demo runs for about five minutes.

Expected result: `--- PASS: TestDemo` and `ok`.

## After a dependency bump

A release can follow a change in versions.json or in any
hack/e2e/<name>/pins.json. That release first does the procedure in
[compatibility.md](compatibility.md#when-infra-bumps-a-dependency): new pins,
re-recorded fixtures, and a new read of each behaviour that the controller
relies on in the source of the new version. After the pins change, run
`make e2e-fetch` and `make e2e-up` again. Then run "Before every tag" from
step 1.

## Tag and publish

### Step 1: Tag the commit

```
git tag -a vX.Y.Z -m vX.Y.Z
```

The tag must be plain semver. The release workflow refuses a prerelease or
build-metadata suffix. Such a tag would overwrite the rolling X.Y image tag, and
it cannot become a chart version.

### Step 2: Push the tag

```
git push origin vX.Y.Z
```

The push starts .github/workflows/release.yaml. Its check job repeats the Go
gate. Its release job does these steps:

1. It builds and pushes the image.
2. It renders deploy/ pinned by digest.
3. It packages and pushes the chart.
4. It creates the GitHub Release.

[packaging.md](packaging.md#release-workflow-steps) describes each step.

Expected result: both jobs pass, and the job summary shows the image digest.

### Step 3: Record the release

Add the tag and its change to the Releases table in README.md. A release can
need something from the person who upgrades. Examples are RBAC to apply with
the image, a check to run first, or objects that the new code does not repair.
In that case, put the steps in a section for the release in
[upgrading.md](upgrading.md), and link the section from the table row.
