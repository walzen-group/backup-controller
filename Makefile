# backup-controller: the module's gates and the generated-file targets. Every
# recipe enters the flake's devShell through `nix develop -c`; there is no go or
# controller-gen on the host PATH.

NIX := nix develop -c
CONTROLLER_GEN := $(NIX) controller-gen

# config/crd is the generated original. deploy/ and chart/ each need their own
# copy, because kustomize and helm both read a directory of their own.
CRD := backup.wlz.li_volumerestores.yaml

.PHONY: build test vet lint check generate manifests verify

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

## generate: regenerate deepcopy functions into the API package.
generate:
	$(CONTROLLER_GEN) object paths=./internal/api/...

## manifests: regenerate CRD manifests into config/crd and copy them to deploy and chart.
manifests:
	$(CONTROLLER_GEN) crd paths=./internal/api/... output:crd:dir=config/crd
	cp config/crd/$(CRD) deploy/crds/$(CRD)
	cp config/crd/$(CRD) chart/crds/$(CRD)

## verify: regenerate CRDs into .tmp/ and fail on any drift from config/crd or from either copy.
verify:
	rm -rf .tmp/crd
	mkdir -p .tmp/crd
	$(CONTROLLER_GEN) crd paths=./internal/api/... output:crd:dir=.tmp/crd
	diff -ru config/crd .tmp/crd
	diff -u config/crd/$(CRD) deploy/crds/$(CRD)
	diff -u config/crd/$(CRD) chart/crds/$(CRD)
