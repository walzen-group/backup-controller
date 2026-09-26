# Releasing

A release starts when a person pushes a tag `vX.Y.Z`. GitHub's release
workflow then publishes the image, the rendered manifests, the chart and the
CRD bundle; [packaging.md](packaging.md) lists what it attaches and what each
of its steps does. GitHub CI runs only the fast tier (check, verify, envtest,
the chart renders and the image build). The e2e suites and the demo need a real
cluster with the real VolSync, CloudNativePG, Kueue and Flux, so they run on
the maintainer's machine, by hand, before every tag. No CI job, cron entry or
timer runs them on a schedule.

## Local e2e cluster

The e2e cluster is the Kubernetes cluster Docker Desktop runs with its kind
provisioner. kubectl reaches it through the context docker-desktop, and every
script under hack/e2e passes `--context docker-desktop` on each call. The
cluster runs Kubernetes 1.36.4, with kindnet and the local-path StorageClass
standard.

You need to create that cluster once, by hand, in Docker Desktop's Kubernetes
settings. The scripts never create, reset or delete it: they install into it
and uninstall from it. `make e2e-down` removes every component and leaves the
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

After installing each component, e2e-up runs that component's check (the same
one `make e2e-check` runs).

The whole setup stays under 5 GB of storage. csi-driver-host-path runs with
`--capacity=e2e=5Gi` and `--max-volume-size=1073741824`, so it refuses a
claim on e2e-hostpath past 5Gi in total or past 1Gi on its own, and RustFS keeps
its data on an emptyDir capped at 1Gi. The driver does not count snapshots
against the cap and does not limit what a pod writes into a volume, so a test
that fills a volume can still exceed it.

### Fetch the pinned images and charts

Run this once, and again after any pins.json changes:

```
make e2e-fetch
```

e2e-fetch is the only e2e target that downloads. It pulls every image by
digest into Docker Desktop's engine, saves the charts and manifests under
hack/e2e/<name>/cache or .tmp/e2e with their sha256 checked, and builds the
controller image from the repository's Dockerfile. e2e-up, e2e and demo work
from what it saved.

### Install the components

```
make e2e-up
```

Expected result: every `== check <name>` section ends without an error.

A cluster that already has the components installed needs only
`make e2e-check`.

## Before every tag

Run every step on the commit you are about to tag, from a clean tree, with the
e2e components installed as described above. Every step must pass. When one
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

Expected result: all three exit 0. These are the gates CI runs on every push,
and the release workflow runs the four Go commands of `make check` again
before it publishes.

### Step 2: Put the tagged code into the cluster

```
nix develop -c hack/e2e/backup-controller/backup-controller.sh rebuild
```

rebuild builds the image from the working tree, uncommitted changes included,
points the Deployment
backup-controller in namespace backup-system at it and waits for the rollout.
Each build gets a new tag, backup-controller:e2e-<image id>, so the kubelet
always runs the image just built. Without this step the e2e suites test
whatever the cluster was last built from.

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

The demo backs up a small app on its schedule, changes the app's data,
restores the backup in place, compares the data with what was backed up, and
deletes what it created. It runs for about five minutes.

Expected result: `--- PASS: TestDemo` and `ok`.

## After a dependency bump

A release that follows a change in versions.json or in any
hack/e2e/<name>/pins.json first goes through
[compatibility.md](compatibility.md#when-infra-bumps-a-dependency): new pins,
re-recorded fixtures, and a re-read of each behaviour the controller relies on
in the new version's source. Run `make e2e-fetch` and `make e2e-up` again
after the pins move, then run "Before every tag" from step 1.

## Tag and publish

### Step 1: Tag the commit

```
git tag -a vX.Y.Z -m vX.Y.Z
```

The tag has to be plain semver. The release workflow refuses a prerelease or
build-metadata suffix, because such a tag would overwrite the rolling X.Y image
tag and cannot become a chart version.

### Step 2: Push the tag

```
git push origin vX.Y.Z
```

The push starts .github/workflows/release.yaml. Its check job repeats the Go
gate, and its release job builds and pushes the image, renders deploy/ pinned
by digest, packages and pushes the chart, and creates the GitHub Release;
[packaging.md](packaging.md#the-release-workflow-step-by-step) describes each
step.

Expected result: both jobs pass, and the job summary shows the image digest.

### Step 3: Record the release

Add the tag and its change to the Releases table in README.md. When the
release needs anything from the person upgrading, such as RBAC to apply with
the image, a check to run first, or objects the new code does not repair, the
steps go in a section for the release in [upgrading.md](upgrading.md), and the
table row links it.
