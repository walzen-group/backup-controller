#!/usr/bin/env bash
# up.sh installs the test stack into the kind cluster of Docker Desktop
# (kubectl context docker-desktop). Each step is safe to run again.
#
# Usage: hack/kind/up.sh [step...]
# Steps, in order: csi certmanager s3 cnpg volsync kueue flux cluster.
# With no step given, it runs all of them.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=versions.env
source "$here/versions.env"
ctx=docker-desktop

k() { kubectl --context "$ctx" "$@"; }
h() { helm --kube-context "$ctx" "$@"; }
log() { printf '%s up.sh: %s\n' "$(date +%T)" "$*" >&2; }

# render replaces the image placeholders in a manifest with the pinned images.
render() {
  sed -e "s|RUSTFS_IMAGE|$RUSTFS_IMAGE|" -e "s|AWS_CLI_IMAGE|$AWS_CLI_IMAGE|" \
    -e "s|REGISTRY_IMAGE|$REGISTRY_IMAGE|" "$1"
}

# csi installs the VolumeSnapshot CRDs, the snapshot controller and the host
# path CSI driver into namespace csi.
csi() {
  local snap=https://raw.githubusercontent.com/kubernetes-csi/external-snapshotter/$SNAPSHOTTER_VERSION
  local hp=https://raw.githubusercontent.com/kubernetes-csi/csi-driver-host-path/$CSI_HOSTPATH_VERSION/deploy/kubernetes-1.34/hostpath
  k create namespace csi --dry-run=client -o yaml | k apply -f -
  for crd in volumesnapshotclasses volumesnapshotcontents volumesnapshots; do
    k apply -f "$snap/client/config/crd/snapshot.storage.k8s.io_$crd.yaml"
  done
  k wait --for condition=established --timeout=60s crd/volumesnapshots.snapshot.storage.k8s.io
  local urls=(
    "$snap/deploy/kubernetes/snapshot-controller/rbac-snapshot-controller.yaml"
    "$snap/deploy/kubernetes/snapshot-controller/setup-snapshot-controller.yaml"
    https://raw.githubusercontent.com/kubernetes-csi/external-attacher/v4.12.0/deploy/kubernetes/rbac.yaml
    https://raw.githubusercontent.com/kubernetes-csi/external-provisioner/v6.3.0/deploy/kubernetes/rbac.yaml
    https://raw.githubusercontent.com/kubernetes-csi/external-resizer/v2.2.1/deploy/kubernetes/rbac.yaml
    "$snap/deploy/kubernetes/csi-snapshotter/rbac-csi-snapshotter.yaml"
    https://raw.githubusercontent.com/kubernetes-csi/external-health-monitor/v0.18.0/deploy/kubernetes/external-health-monitor-controller/rbac.yaml
    "$hp/csi-hostpath-driverinfo.yaml"
    "$hp/csi-hostpath-plugin.yaml"
  )
  # The upstream manifests name namespace default or kube-system; everything
  # goes into csi. The v8.6.0 snapshot controller manifest still names the
  # v8.5.0 image; the CRDs and the sidecar are v8.6.0.
  for url in "${urls[@]}"; do
    curl -fsSL "$url" |
      sed -e 's/namespace: default\b/namespace: csi/' -e 's/namespace: kube-system\b/namespace: csi/' \
        -e 's|snapshot-controller:v8.5.0|snapshot-controller:v8.6.0|' |
      k apply -n csi -f -
  done
  k -n csi rollout status deployment/snapshot-controller --timeout=300s
  k -n csi rollout status statefulset/csi-hostpathplugin --timeout=300s
}

certmanager() {
  h upgrade --install cert-manager "$CERT_MANAGER_CHART" --version "$CERT_MANAGER_VERSION" \
    --namespace cert-manager --create-namespace --set crds.enabled=true --wait --timeout 10m
}

s3() {
  render "$here/manifests/rustfs.yaml" | k apply -f -
  k -n s3 rollout status deployment/rustfs --timeout=300s
  k -n s3 wait --for=condition=Complete job/buckets --timeout=300s
}

cnpg() {
  h upgrade --install cloudnative-pg "$CNPG_CHART" --version "$CNPG_VERSION" \
    --namespace cnpg-system --create-namespace --wait --timeout 10m
  h upgrade --install plugin-barman-cloud "$BARMAN_PLUGIN_CHART" --version "$BARMAN_PLUGIN_VERSION" \
    --namespace cnpg-system --wait --timeout 10m
}

volsync() {
  h repo add backube "$VOLSYNC_REPO" --force-update >/dev/null
  h upgrade --install volsync backube/volsync --version "$VOLSYNC_VERSION" \
    --namespace volsync-system --create-namespace --wait --timeout 10m
}

kueue() {
  h upgrade --install kueue "$KUEUE_CHART" --version "$KUEUE_VERSION" \
    --namespace kueue-system --create-namespace --wait --timeout 10m
}

# flux installs only the two controllers the tests use: source-controller
# reads the test apps from the registry, kustomize-controller applies them.
flux() {
  curl -fsSL "$FLUX_INSTALL" | k apply -f -
  for d in helm-controller notification-controller image-reflector-controller image-automation-controller; do
    k -n flux-system scale deployment "$d" --replicas=0 2>/dev/null || true
  done
  k -n flux-system rollout status deployment/source-controller --timeout=300s
  k -n flux-system rollout status deployment/kustomize-controller --timeout=300s
}

cluster() {
  render "$here/manifests/cluster.yaml" | k apply -f -
  k -n registry rollout status deployment/registry --timeout=300s
}

steps=("$@")
[[ ${#steps[@]} -gt 0 ]] || steps=(csi certmanager s3 cnpg volsync kueue flux cluster)
for step in "${steps[@]}"; do
  log "step $step"
  "$step"
done
log "done: ${steps[*]}"
