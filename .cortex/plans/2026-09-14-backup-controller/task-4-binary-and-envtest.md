# task-4: the binary, the image, and the offline end-to-end proof

## Context

Milestone 4 of `docs/implementation-plan.md`. `internal/populator` holds the
three callbacks from task 3; this task writes the command that binds them to
`lib-volume-populator`'s `RunControllerWithConfig`, the container image
definition, and the proof that the whole thing runs.

The environment decides how that proof looks. No Kubernetes cluster is reachable
(`kubectl get --raw /version` times out against the only configured context) and
no container daemon exists in this WSL distro (`docker info` reports the
integration is off). What is available is an envtest control plane already
unpacked at `~/.local/share/kubebuilder-envtest/k8s/1.37.0-linux-amd64/`, with
`kube-apiserver`, `etcd` and `kubectl` in it. So:

| Milestone-4 claim | How it is proven here |
| --- | --- |
| the image builds | a static `linux/amd64` binary build plus `hadolint`. The `docker build` itself runs in CI, and this gap is recorded |
| the binary starts against a kubeconfig | started as a subprocess against the envtest API server |
| it logs that it is watching the kind | the startup line is read from its output |
| it exits cleanly on SIGTERM | the subprocess exit status, with no panic in its output |

The envtest run goes past the milestone's own claims and observes the design's
core behavior, because that is now cheap: a claim naming a `VolumeRestore` should
produce the repository Secret copy, the prime claim, and the ReplicationDestination
with `copyMethod: Direct`.

Task 0's findings file names the library's real entry point, config fields and
namespace handling. Read
`.cortex/plans/2026-09-14-backup-controller/library-contract-findings.md` first.

## Target

- `cmd/backup-controller/main.go`
- `Dockerfile`
- `.tmp/envtest/main.go` and `.tmp/envtest/run.sh` (throwaway, never committed)
- `.tmp/backup-controller` (the built binary)

Non-goals: `deploy/`, `chart/`, `.github/workflows/release.yaml` (task 5),
`internal/api/`, `internal/volsync/`, `internal/populator/` (tasks 2 and 3),
`config/crd/` (task 2). Do not commit anything from `.tmp/`.

## Change

### `cmd/backup-controller/main.go`

Flags, with the library's real requirements from task 0's findings:

| Flag | Default | Purpose |
| --- | --- | --- |
| `--kubeconfig` | empty | path to a kubeconfig; empty means the ambient configuration |
| `--namespace` | `backup-system` | the namespace the prime claim and the destination live in |
| `--metrics-addr` | `:8080` | passed to the library |
| `--metrics-path` | `/metrics` | passed to the library |
| `--version` | `false` | print the version and exit |

Finding (i) is DIFFERENT: `ImageName` belongs to `PodConfig` alone, and
provider-function mode builds no pod, so there is no image flag. Do not add one,
and do not pass an image name to the library. Finding (h) is DIFFERENT as well:
the library registers only the configured metrics path, so the binary exposes no
health endpoint and this task claims none. Task 5's Deployment then carries no
liveness or readiness probe, and what the library actually serves is a startup
log line plus the Prometheus endpoint.

Wiring: build the library's config from the flags, set `ProviderFunctionConfig`
with task 3's three callbacks, and call `RunControllerWithConfig`. Before the
call, log one line naming the group, version and kind it watches, at info level,
so the startup claim is observable:

```
starting backup-controller: watching backup.wlz.li/v1alpha1 VolumeRestore
```

Signals: use `ctrl.SetupSignalHandler()` for the context; when it fires, log one
shutdown line and return from `main` with exit status 0. A panic on shutdown
fails this task.

### `Dockerfile`

Two stages, following `/home/nixos/repos/kuport/Dockerfile`:

1. Build stage `golang:1.26.8-alpine3.24@sha256:ce864e7223ac17b1775e6fd0b4c0db580c2eb50e7953a427916379e4b92a1628`
   (the pin kuport carries, dated 2026-09-06). `WORKDIR /src`, `COPY go.mod go.sum ./`,
   `RUN go mod download`, `COPY . .`, then `RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64
   go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/backup-controller
   ./cmd/backup-controller`, with `ARG VERSION=dev` declared before it. `main.go`
   carries `var version = "dev"`, and `--version` prints it, so the value CI passes
   through `build-args: VERSION=ci-<sha>` reaches the binary. Task 5's `image` job
   passes exactly that argument.
2. Final stage `scratch`, `USER 65532:65532` (non-root, from the
   `nonroot` user convention), `COPY --from=build /out/backup-controller /backup-controller`,
   `ENTRYPOINT ["/backup-controller"]`.

`docs/packaging.md` allows distroless or scratch. Carry kuport's reasoning in a
comment: the controller needs no shell, no restic and no kubectl, and it reaches
the API server with the mounted ServiceAccount token and cluster CA, so no CA
bundle is copied in. The Deployment sets `readOnlyRootFilesystem`, which the
image supports because the binary writes nothing.

### The envtest program

`.tmp/envtest/main.go`, package `main`, run from its own directory with
`go run .` inside the dev shell. It does this, in order:

1. Resolve the assets with `setup-envtest use 1.37.0 -p path` (this resolves to
   the already-installed `~/.local/share/kubebuilder-envtest/k8s/1.37.0-linux-amd64`
   without a download, verified by the orchestrator), and start a control plane
   through `sigs.k8s.io/controller-runtime/pkg/envtest`.
2. Install the CRDs: `config/crd/` from this repo, plus the VolSync
   ReplicationDestination CRD fetched from a pinned tag of
   `github.com/backube/volsync` (`config/crd/bases/volsync.backube_replicationdestinations.yaml`).
   Record the exact URL, the tag and the file's SHA-256 in the script and in the
   report. Without that CRD the controller cannot create its destination and the
   run proves nothing.
3. Create the namespace `backup-system`, a `StorageClass`, a repository `Secret`
   in the app namespace, a `VolumeRestore` naming it, and a
   `PersistentVolumeClaim` whose `dataSourceRef` names the `VolumeRestore`.
4. Start `.tmp/backup-controller` with `--kubeconfig` pointing at the envtest
   server and `--namespace backup-system`, capture its output, and wait for the
   startup line.
5. Assert, through the API server, the rows in the table below.
6. Send `SIGTERM`, wait for the process to exit, and record the exit status.
7. Print every observed object as YAML so the run is readable evidence, and
   write the whole transcript to `.tmp/envtest-output.log`.

Hypothesis to confirm or kill, from reading the design rather than the library:
the library may wait for the prime claim to be bound before it calls `Populate`,
and envtest runs no PersistentVolume controller. If `Populate` is not reached,
create a `PersistentVolume` and set the prime claim's `spec.volumeName` so the
claim reads as bound, then say in the report what the library actually waited
for. Mark it as a hypothesis until you have run it.

| Observation | Expected |
| --- | --- |
| the binary's output | the startup line naming `backup.wlz.li/v1alpha1 VolumeRestore` |
| exit status after `SIGTERM` | 0, and no panic in the output |
| a Secret in `backup-system` named `<claim uid>` | present, with the repository Secret's data |
| the prime claim | named `prime-<claim uid>`, the app claim's storage class and size, no `dataSource` |
| the ReplicationDestination | `copyMethod: Direct`, `destinationPVC: prime-<uid>`, `trigger.manual: <uid>`, `metadata.name: restore-<uid>` |
| the VolumeRestore status | `Ready` False with reason `Restoring`, one `claims[]` entry for the app claim with phase `Restoring` |

### `.tmp/envtest/run.sh`

One entry point that resolves the assets, builds the binary, runs the program,
and keeps `.tmp/envtest-output.log`. It is scratch: it stays out of git, and the
report quotes its output.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- The envtest assets are already on disk. Do not download a second control
  plane: `setup-envtest use 1.37.0 -p path` must resolve to the existing
  directory, and if it tries to fetch, stop and report.
- The only network fetch this task makes is the VolSync CRD from its pinned tag.
- Nothing under `.tmp/` is committed, and no repository file outside
  `cmd/backup-controller/`, `Dockerfile` and `go.mod`/`go.sum` is modified.

## Acceptance

From `/home/nixos/repos/backup-controller`:

```sh
nix develop -c go build ./...
nix develop -c go vet ./...
nix develop -c golangci-lint run
nix develop -c go test ./... -race
nix develop -c hadolint Dockerfile
nix develop -c bash -c 'CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath -ldflags "-s -w" -o .tmp/backup-controller ./cmd/backup-controller'
file .tmp/backup-controller
bash .tmp/envtest/run.sh
```

Expected: hadolint prints nothing; `file` reports an ELF 64-bit x86-64 static
executable; `run.sh` prints the observed objects and every row above matches,
with the transcript at `.tmp/envtest-output.log`.

Record these two gaps in the report, each with what was proven instead:

| Gap | Substitute evidence |
| --- | --- |
| `docker build` | the static binary build plus hadolint; the build runs in CI, in the `image` job task 5 appends to `ci.yaml` |
| the restore completing and the claim rebinding | out of reach offline: no VolSync controller exists to move data or to report `status.lastManualSync`. Task 6's runbook carries those rows for the cluster |

## Emission discipline

Spend at most 20 minutes reading task 0's findings and the library's entry point,
then write code. Get the binary starting before refining the assertions. Every 10
minutes of work must leave a file on disk.

## Report to

`meta-w4`, one `A2A:` steer: files written, `git status --short`, the log path
and the observed table, the SIGTERM exit status, the VolSync CRD URL with its
SHA-256, what the library waited for before calling `Populate`, and both recorded
gaps. Then stop.
