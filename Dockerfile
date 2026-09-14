# backup-controller container image.
#
# Two stages: build with the official Go image, ship a static binary on
# scratch. The controller needs no shell, no restic and no kubectl. It builds
# ReplicationDestination objects and lets VolSync's mover do the data work, so
# nothing in the image is executed except the binary itself.
#
# No CA bundle is copied in. The controller talks to the in-cluster API server
# with the ServiceAccount token and the cluster CA, both mounted by the
# kubelet. If it ever needs outbound TLS to something else, this is the line to
# revisit.
#
# The binary writes no files, so the Deployment's readOnlyRootFilesystem costs
# the image nothing.

# Pinned by tag and digest (Docker Hub, 2026-09-06). The build stage changes
# only when someone deliberately updates this pair.
FROM golang:1.26.8-alpine3.24@sha256:ce864e7223ac17b1775e6fd0b4c0db580c2eb50e7953a427916379e4b92a1628 AS build

WORKDIR /src

# Resolve module downloads before copying sources so edits to Go code do not
# invalidate the dependency layer.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# VERSION is stamped by CI: release.yaml passes the tag and ci.yaml passes
# ci-<sha>. Local builds without a version stay honest about being unstamped,
# and --version prints whatever landed here.
ARG VERSION=dev

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/backup-controller ./cmd/backup-controller

FROM scratch

# The controller needs no privilege on the node and the Deployment sets
# runAsNonRoot, so the image states a non-root uid and gid of its own.
USER 65532:65532

COPY --from=build /out/backup-controller /backup-controller

ENTRYPOINT ["/backup-controller"]
