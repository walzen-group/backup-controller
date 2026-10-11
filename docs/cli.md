# kubectl backup

kubectl-backup is a kubectl plugin that creates a BackupRun or a RestoreRun from
one command line:

```sh
kubectl backup run before-the-move -n tracearr --all
```

```text
backuprun.backup.wlz.li/before-the-move created
```

The plugin builds the same object a manifest would declare and sends it to the
API server. Watch it afterwards the way you would watch one created from YAML:

```sh
kubectl -n tracearr get brun before-the-move
```

## Install

Every release attaches one binary per platform, named
kubectl-backup-VERSION-OS-ARCH. Put it on the PATH as kubectl-backup, and kubectl
runs it as `kubectl backup`:

```sh
curl -fsSL -o ~/.local/bin/kubectl-backup https://github.com/walzen-group/backup-controller/releases/download/v1.3.0/kubectl-backup-1.3.0-linux-amd64
chmod +x ~/.local/bin/kubectl-backup
kubectl backup version
```

Expected result: `1.3.0`.

With Go installed, `go install` builds the same binary from the tag:

```sh
go install github.com/walzen-group/backup-controller/cmd/kubectl-backup@v1.3.0
```

A binary from `go install` prints `dev` for its version, because the release
pipeline stamps the version at link time.

## Back up

```sh
kubectl backup run before-the-move -n tracearr --all
kubectl backup run before-the-upgrade -n immich --source immich-library
kubectl backup run before-the-upgrade -n immich --database immich-pg
```

| Flag | Sets | Default when omitted |
| --- | --- | --- |
| `--all` | spec.all | |
| `--source CLAIM` | spec.source | |
| `--database CLUSTER` | spec.database | |
| `--timeout 2h` | spec.timeout | the namespace's backup.wlz.li/timeout, else one hour |
| `--ttl 3600` | spec.ttlSecondsAfterFinished | the run is kept |

## Restore

```sh
kubectl backup restore undo-the-upgrade -n immich --database immich-pg
kubectl backup restore look-back -n immich --claim immich-library --into immich-library-old --as-of 2026-10-11T04:00:00Z
kubectl backup restore everything -n immich --all --sync-database-to-volume --pause deploy/immich-server
```

| Flag | Sets | Default when omitted |
| --- | --- | --- |
| `--all` | spec.all | |
| `--claim CLAIM` | spec.claim | |
| `--repository SECRET` | spec.repository | |
| `--database CLUSTER` | spec.database | |
| `--into CLAIM` | spec.into | the claim is restored in place |
| `--into-size 10Gi` | spec.intoSize | the source claim's size |
| `--as-of TIME` | spec.restoreAsOf, an RFC 3339 time | the newest backup |
| `--previous 1` | spec.previous | |
| `--sync-database-to-volume` | spec.syncDatabaseToVolume | |
| `--pause deploy/NAME` | one entry of spec.pauseDuringRestore; repeat the flag for more | the run waits for the workloads to stop |
| `--timeout 30m` | spec.timeout | 4h |
| `--ttl 86400` | spec.ttlSecondsAfterFinished | the run is kept |

`--pause` accepts the kinds the way kubectl spells them: deployment, deployments
or deploy, and statefulset, statefulsets or sts. moverSecurityContext has no
flag; a run that needs it is written as YAML.

## Flags of both commands

| Flag | Effect |
| --- | --- |
| `-n`, `--namespace` | the run's namespace; the kubeconfig context's namespace when omitted |
| `--kubeconfig` | the kubeconfig to read; KUBECONFIG or ~/.kube/config when omitted |
| `--context` | the kubeconfig context to use |
| `--dry-run` | sends the run as a server-side dry run and prints it as YAML |

`--dry-run` passes the object through the API server's validation and
defaulting, so the printed YAML is what the create would store, with the
defaulted fields filled in. The server stores nothing.

## Which flags go together

The plugin leaves the rules about combinations to the API server. A command
line that sets two of `--all`, `--source` and `--database` builds a BackupRun
the server rejects with the rule's own message, `set exactly one of source,
database and all`, and the plugin prints that message and exits 1. The
BackupRun and RestoreRun sections of [api.md](api.md) list the rules. The
plugin holds no copy of them, so a rule changed in the CRD applies to the
plugin without a new release of it.

The plugin itself refuses only what it cannot build: a missing or second NAME,
an unknown flag, a `--pause` value that is not KIND/NAME, and an `--into-size`
that is not a quantity:

```text
error: --pause "job/foo" names kind "job"; use deployment/NAME or statefulset/NAME
```
