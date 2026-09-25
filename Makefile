# backup-controller: the module's gates and the generated-file targets. Every
# recipe enters the flake's devShell through `nix develop -c`; there is no go or
# controller-gen on the host PATH.

NIX := nix develop -c
CONTROLLER_GEN := $(NIX) controller-gen
# The envtest shell adds a kube-apiserver and an etcd from the store, at the
# Kubernetes version versions.json pins, and points KUBEBUILDER_ASSETS at them.
ENVTEST := nix develop .\#envtest -c
# The fixtures shell adds every real program a recorded fixture comes from.
FIXTURES := nix develop .\#fixtures -c

# config/crd is the generated original. deploy/ and chart/ each need their own
# copy, because kustomize and helm both read a directory of their own. Every
# generated file is copied, so a new kind needs no edit here.

.PHONY: build test vet lint check envtest generate manifests verify fixtures fixtures-restic fixtures-barman fixtures-crds

## build: compile every package.
build:
	$(NIX) go build ./...

## test: run the suite with the race detector.
test:
	$(NIX) go test ./... -race

## vet: run go vet over every package.
vet:
	$(NIX) go vet ./...

## lint: run golangci-lint.
lint:
	$(NIX) golangci-lint run

## check: the gate a person runs, and the one CI's check job mirrors.
check: vet test lint build

## envtest: run the suites tagged envtest against a real kube-apiserver and etcd, as CI's envtest job does.
envtest:
	$(ENVTEST) go test -tags envtest ./...

## fixtures: re-record every checked-in fixture that real programs produce. Run it after a pin in versions.json moves; check fails until it has.
fixtures: fixtures-restic fixtures-barman fixtures-crds

## fixtures-restic: re-record the restic repositories and restic's verdicts under internal/restic/testdata/recorded.
fixtures-restic:
	$(FIXTURES) hack/fixtures/restic.sh

## fixtures-barman: re-record the barman stores, barman's verdicts and RustFS's answers under internal/testinfra/barmanstore/recorded.
fixtures-barman:
	$(FIXTURES) hack/fixtures/barman-stores.sh

## fixtures-crds: copy the pinned third-party CRDs, and our CRDs at old tags, under internal/testinfra/crds.
fixtures-crds:
	$(FIXTURES) hack/fixtures/crds.sh

## generate: regenerate deepcopy functions into the API package.
generate:
	$(CONTROLLER_GEN) object paths=./internal/api/...

## manifests: regenerate CRD manifests into config/crd and copy them to deploy and chart.
manifests:
	$(CONTROLLER_GEN) crd paths=./internal/api/... output:crd:dir=config/crd
	rm -f deploy/crds/backup.wlz.li_*.yaml chart/crds/backup.wlz.li_*.yaml
	cp config/crd/backup.wlz.li_*.yaml deploy/crds/
	cp config/crd/backup.wlz.li_*.yaml chart/crds/

## verify: regenerate CRDs into .tmp/ and fail on any drift from config/crd or from either copy.
verify:
	rm -rf .tmp/crd
	mkdir -p .tmp/crd
	$(CONTROLLER_GEN) crd paths=./internal/api/... output:crd:dir=.tmp/crd
	diff -ru config/crd .tmp/crd
	diff -ru --exclude=kustomization.yaml config/crd deploy/crds
	diff -ru config/crd chart/crds
