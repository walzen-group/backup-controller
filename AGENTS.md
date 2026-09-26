# Agent instructions

## Dependency versions

walzen upgrades the controller's dependencies over time: Kubernetes, Flux,
VolSync and its restic, Kueue, CloudNativePG, the barman-cloud plugin and
barman, cert-manager and RustFS. versions.json holds the versions the
controller is tested against. A change to any of them follows the procedure in
docs/compatibility.md: bump versions.json and the hack/e2e pins, re-record the
fixtures, run the e2e suites, and re-check every behaviour that doc lists for
the bumped dependency against the new version's source.

The controller follows the version the API server serves for every kind of
another project, and it fails loudly only on a real incompatibility; the rule
and the table of fields it reads are in docs/compatibility.md. After an infra
bump, run `make e2e` against the new pins.

## Releases

A release follows docs/releasing.md. Before every tag, on the commit being
tagged, `make check`, `make verify`, `make envtest`,
`hack/e2e/backup-controller/backup-controller.sh rebuild`, `make e2e` and
`make demo` all pass, the last three against the docker-desktop cluster after
`make e2e-up`. CI runs only the fast tier, and nothing runs the e2e tier on a
schedule. After a dependency bump, the docs/compatibility.md procedure comes
first.
