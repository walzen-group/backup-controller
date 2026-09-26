# Agent instructions

## Dependency versions

walzen upgrades the controller's dependencies over time: Kubernetes, Flux,
VolSync and its restic, Kueue, CloudNativePG, the barman-cloud plugin and
barman, cert-manager and RustFS. versions.json holds the versions the
controller is tested against. A change to any of them follows the procedure in
docs/compatibility.md: bump versions.json and the hack/e2e pins, re-record the
fixtures, run the e2e suites, and re-check every behaviour that doc lists for
the bumped dependency against the new version's source.

## Releases

A release requires `make e2e` passing locally, after `make e2e-up` against the
docker-desktop cluster.
