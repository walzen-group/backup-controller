# task-2: the VolumeRestore API and its CRD

## Context

Milestone 2 of `docs/implementation-plan.md`. The repository has a module, a
flake and a Makefile from task 1 and no Go package yet. This task adds the
custom resource a claim names in `dataSourceRef`, the CRD controller-gen
generates from it, and the API-server-backed test that proves the CRD rejects a
malformed object. `docs/api.md` is the authority for every field: the resource
is namespaced, group `backup.wlz.li`, version `v1alpha1`, kind `VolumeRestore`,
short name `vrestore`.

The reference implementation is `/home/nixos/repos/kuport`; its
`internal/api/v1alpha1/` holds the shape for a groupversion file, a conditions
helper and kubebuilder markers, and its `config/crd/` and `config/samples/` hold
the generated and hand-written file names.

VolSync's own field types were read by the orchestrator from
`github.com/backube/volsync` on 2026-09-14 and the four passthrough fields exist
as `docs/api.md` claims: `spec.restic.restoreAsOf` is `*string` carrying
`+kubebuilder:validation:Format="date-time"`, `spec.restic.moverPodLabels` is
`map[string]string`, `spec.restic.moverSecurityContext` is
`*corev1.PodSecurityContext`, and `spec.destinationPVC` is `*string`. Mirror
those types so task 3's assignment compiles without a conversion.

## Target

- `internal/api/v1alpha1/groupversion_info.go`
- `internal/api/v1alpha1/volumerestore_types.go`
- `internal/api/v1alpha1/conditions.go`
- `internal/api/v1alpha1/zz_generated.deepcopy.go` (generated)
- `internal/api/v1alpha1/crd_validation_test.go` (build tag `envtest`)
- `config/crd/backup.wlz.li_volumerestores.yaml` (generated)
- `config/samples/backup.wlz.li_v1alpha1_volumerestore.yaml`
- `config/samples/claim.yaml`
- the `generated` and `envtest` jobs appended to `.github/workflows/ci.yaml`
- `go.mod` and `go.sum` (dependencies)

Non-goals: `internal/volsync/`, `internal/populator/`, `cmd/`, `Dockerfile`,
`deploy/`, `chart/`. Task 3 adds the library dependency; keep this task's
dependencies to what the types and the envtest test compile against.

## Change

### `groupversion_info.go`

`GroupVersion = schema.GroupVersion{Group: "backup.wlz.li", Version: "v1alpha1"}`,
a `SchemeBuilder`, `AddToScheme`, and the `// +kubebuilder:object:generate=true`
and `// +groupName=backup.wlz.li` markers. Follow kuport's file.

### `volumerestore_types.go`

`VolumeRestore` with `TypeMeta`, `ObjectMeta`, `Spec`, `Status`, and a
`VolumeRestoreList`. Markers on the type: `+kubebuilder:object:root=true`,
`+kubebuilder:subresource:status`,
`+kubebuilder:resource:scope=Namespaced,shortName=vrestore`, and a print column
so `kubectl get vrestore` answers the question `docs/api.md` poses:

```
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
```

`VolumeRestoreSpec`:

| Field | Go type | Markers |
| --- | --- | --- |
| `Repository` | `string` | `+kubebuilder:validation:Required`, `MinLength=1`, `MaxLength=253`, `Pattern` for a DNS-1123 subdomain |
| `RestoreAsOf` | `*string` | `+optional`, `+kubebuilder:validation:Format="date-time"`, matching VolSync's own marker |
| `MoverPodLabels` | `map[string]MoverPodLabelValue` | `+optional`, `+kubebuilder:validation:MaxProperties=8`, two `XValidation` CEL rules, below. `MoverPodLabelValue` is a named `string` carrying `+kubebuilder:validation:MaxLength=63`, which is the only route controller-gen gives to `additionalProperties.maxLength` |
| `MoverSecurityContext` | `*corev1.PodSecurityContext` | `+optional` |

DNS-1123 subdomain pattern for `Repository`:

```
^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$
```

The two CEL rules on `MoverPodLabels`, each with its own `message` naming the
field and the rule:

```
// +kubebuilder:validation:XValidation:rule="self.all(k, k.matches('^([a-z0-9]([-a-z0-9_.]*[a-z0-9])?/)?[a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?$'))",message="moverPodLabels keys must be a valid label key"
// +kubebuilder:validation:XValidation:rule="self.all(k, self[k].matches('^([a-zA-Z0-9]([-a-zA-Z0-9_.]*[a-zA-Z0-9])?)?$'))",message="moverPodLabels values must be a valid label value"
```

Those two patterns are the ones the Kubernetes documentation publishes for label
keys and values.

**The map must stay inside the API server's CEL cost budget, and this is the
binding constraint on this field.** A rule that ranges over an unbounded map
(`self.all(k, ...)`) has an unbounded estimated cost, and the API server refuses
to install a CRD whose rule exceeds the budget: the error names
`maxItems`/`maxProperties`/`maxLength` as the fix and the object never reaches
the cluster. Measured on this host by the wave-2 meta-agent: the two rules above
without a bound report over 100x the budget, and with `MaxProperties=16` still
6.0x. `MaxProperties=8` is the starting bound, and the design's own use of the
field is one or two labels for the backup queue.

If `MaxProperties=8` is still refused, spend the budget in this order and record
which step you stopped at:

1. Shorten the two expressions while keeping both meanings: cheaper regular
   expressions and no nested quantifier beyond the single `self.all`.
2. Reduce the bound further, to `4` or `2`, and record the bound in the report.
3. Keep only the key rule, drop the value rule, and record the deviation. VolSync
   already resolves the values it uses, and Kubernetes rejects an invalid label
   when the mover pod is created, so the value rule is the one to lose.

The gate that catches an over-budget rule is the CRD install itself: the envtest
suite fails to start when the API server refuses the CRD, and `kubectl-validate`
reports it against the types it embeds. Whichever bound you land on, the CRD
installs against a real API server and the six cases below still hold.

**Landed on 2026-09-14 by the task-2 worker, with the measurements.** The bound
alone did not rescue the rules: the two expressions measured over 100x the budget
unbounded, 6.0x at `MaxProperties=16` and 3.0x at `MaxProperties=8`, so the ladder
above stops at its first step. The cause is that controller-gen routes field
markers only to the field's own schema (`pkg/crd/schema.go:421-480`), which leaves
`additionalProperties.maxLength` reachable only through the value's type. The
landed shape is `MoverPodLabels map[string]MoverPodLabelValue` with `MaxLength=63`
on the named type and `MaxProperties=8`, both rule texts unchanged, and the JSON
representation unchanged. `VolumeRestoreSpec.MoverLabels()` returns the
`map[string]string` VolSync's own field takes, so task 3 needs no conversion loop.
The check that matters is unchanged: the CRD installs, and the six cases hold.

`VolumeRestoreStatus`:

```go
type VolumeRestoreStatus struct {
	Conditions []metav1.Condition    `json:"conditions,omitempty"`
	Claims     []ClaimRestoreStatus  `json:"claims,omitempty"`
}

type ClaimRestoreStatus struct {
	Name      string        `json:"name"`
	UID       types.UID     `json:"uid"`
	Phase     RestorePhase  `json:"phase"`
	StartedAt *metav1.Time  `json:"startedAt,omitempty"`
}

type RestorePhase string

const (
	RestorePhaseRestoring RestorePhase = "Restoring"
	RestorePhaseFailed    RestorePhase = "Failed"
)
```

Task 3 sets those phases and the `Ready` condition. Keep the constant names
exactly as written; task 3's spec refers to them.

### `conditions.go`

Carry kuport's shape: the condition type constant for `Ready`, and a helper that
sets or replaces a condition on a `*[]metav1.Condition` through
`meta.SetStatusCondition`. Task 3 calls it to report `Ready` False with reason
`Restoring` while a claim is filling, `Ready` False with reason `RestoreFailed`
when VolSync reports a failed mover, and `Ready` True once nothing is filling.

### Generated files

```sh
nix develop -c make generate
nix develop -c make manifests
nix develop -c make verify
```

`make verify` regenerates into `.tmp/crd` and diffs against `config/crd`; it must
exit 0. `zz_generated.deepcopy.go` is generated, never hand-edited.

### `config/samples/`

A `VolumeRestore` named `notes-data` with `repository: notes-restic` and a
`moverPodLabels` entry, plus a claim named `notes-data` whose `dataSourceRef`
names `apiGroup: backup.wlz.li`, `kind: VolumeRestore`, `name: notes-data`. Keep
them minimal, in the shape of kuport's samples.

### Dependencies

Pin `k8s.io/api`, `k8s.io/apimachinery`, `k8s.io/client-go` and
`sigs.k8s.io/controller-runtime` to the release train the library requires, so
task 3 does not have to move them. The version comes from
`.cortex/plans/2026-09-14-backup-controller/library-contract-findings.md`
(task 0, same wave) when that file exists; otherwise resolve it yourself:

```sh
nix develop -c go mod download github.com/kubernetes-csi/lib-volume-populator@latest
nix develop -c go env GOMODCACHE
```

and read the `k8s.io/*` versions out of that module's `go.mod` in the module
cache. Record the versions you pinned and where they came from.

### `crd_validation_test.go`

Build tag `envtest`. Start a control plane through
`sigs.k8s.io/controller-runtime/pkg/envtest` with `CRDDirectoryPaths` pointing at
`config/crd`, then create objects through a controller-runtime client. Assert on
the API server's answer, not on message text:

| Object | Expectation |
| --- | --- |
| valid VolumeRestore (`repository: notes-restic`) | created |
| `repository: Not_A_Name` | rejected, `apierrors.IsInvalid` |
| no `repository` | rejected, `apierrors.IsInvalid` |
| `restoreAsOf: "yesterday"` | rejected, `apierrors.IsInvalid` |
| `moverPodLabels: {"UPPER KEY": "v"}` | rejected, `apierrors.IsInvalid` |
| `moverPodLabels: {"app": "bad value!"}` | rejected, `apierrors.IsInvalid` |

The two label rows are why `docs/api.md` lists the label syntax rule: without
them the CEL rules are unverified text in a YAML file. Each rejection must be
the API server refusing the object; assert `IsInvalid` so a rejection for an
unrelated reason fails the test.

### `.github/workflows/ci.yaml`

Append two jobs from kuport's `ci.yaml`, keeping that file's existing comments
and its `check` job untouched:

- `generated`: checkout, nix, then `make verify`, with kuport's comment that this
  job must fail the build rather than warn.
- `envtest`: checkout, nix, then resolve the assets and run the tagged suite:
  `assets=$(setup-envtest use 1.37.0 -p path)` and
  `KUBEBUILDER_ASSETS="$assets" go test -tags envtest ./internal/api/...`, using
  a separate assignment for the asset path so a failed fetch aborts the step.

## Constraints

Global constraints 1-11 in `00-index.md`. On top of them:

- `docs/api.md`'s field table is the contract. Add no field it does not list, and
  add no field whose name does not match the ReplicationDestination field it
  reaches.
- `internal/api/v1alpha1` is the only Go package this task creates.
- Do not edit `Makefile`, `flake.nix` or the `check` job in `ci.yaml`.

## Acceptance

From `/home/nixos/repos/backup-controller`:

```sh
nix develop -c make generate
nix develop -c make manifests
nix develop -c make verify
nix develop -c go build ./...
nix develop -c go vet ./...
nix develop -c golangci-lint run
nix develop -c go test ./... -race
nix develop -c actionlint .github/workflows/ci.yaml
assets=$(nix shell nixpkgs#setup-envtest -c setup-envtest use 1.37.0 -p path)
KUBEBUILDER_ASSETS="$assets" nix develop -c go test -tags envtest ./internal/api/... -v
mkdir -p .tmp && cp config/crd/*.yaml .tmp/validate-m2-crd.yaml
rev=$(nix eval --impure --raw --expr "(builtins.fromJSON (builtins.readFile ./flake.lock)).nodes.nixpkgs.locked.rev")
nix shell "github:NixOS/nixpkgs/${rev}#kubectl-validate" -c kubectl-validate .tmp/validate-m2-crd.yaml
```

Expected: `make verify` prints no diff and exits 0; vet, lint, race tests and
the tagged suite exit 0 with the six cases above visible in the verbose output;
actionlint prints nothing; kubectl-validate reports the generated CRD as valid
against the API schemas it embeds. Feed kubectl-validate the CRD files only: it
does not know this group for instances, and the envtest suite covers those.

The acceptance criterion the design document states for this milestone is
"`kubectl apply --dry-run=server -f config/crd` on a cluster, then applying a
sample and reading it back". No cluster is reachable from this environment, so
the envtest suite is the substitute for both halves, and the runbook in task 6
carries the cluster commands. Record that substitution in the report.

## Emission discipline

Spend at most 15 minutes reading `docs/api.md`, kuport's `internal/api/v1alpha1/`
and kuport's `config/`, then write. Every 10 minutes of work must leave a file on
disk.

## Report to

`meta-w2`, one `A2A:` steer: files written, `git status --short`, the pinned
dependency versions with where they came from, the six envtest cases with the
observed API server verdicts, `make verify` output, and any deviation. Then stop.
