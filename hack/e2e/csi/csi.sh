#!/usr/bin/env bash
# csi.sh installs the external-snapshotter CRDs and snapshot-controller and csi-driver-host-path
# (clone and snapshot support) into namespace e2e-csi of the docker-desktop kind cluster, plus the
# StorageClass and VolumeSnapshotClass e2e-hostpath.
#
# Usage: hack/e2e/csi/csi.sh fetch|install|check|uninstall
#
# fetch downloads the manifests named in pins.json, checks each sha256 and pulls every image by
# digest into the local Docker engine. install works from what fetch saved. check provisions a
# claim, clones it, snapshots it, restores the snapshot and reads a file back from each copy, then
# deletes what it made. uninstall removes everything install created.
#
# Storage cap: the driver is started with --capacity=e2e=5Gi and --max-volume-size=1073741824 (1Gi), and the
# StorageClass passes kind=e2e, so the driver refuses to provision past 5Gi in total or 1Gi per
# claim. The driver does not count snapshots against the cap, does not check the total on volume
# expansion, and does not limit the bytes written into a volume (volumes are plain directories).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/pins.json"
cache="${E2E_CACHE:-${XDG_CACHE_HOME:-$HOME/.cache}/backup-controller-e2e}/csi"
ctx="docker-desktop"
ns="e2e-csi"
claim_size=100Mi
k() { kubectl --context "$ctx" "$@"; }

# image_ref prints the pinned tag@digest reference for an upstream image name.
image_ref() { jq -r --arg u "$1" '.images[] | select(.upstream == $u) | .ref' "$pins"; }

fetch() {
  mkdir -p "$cache"
  jq -r '.manifests[] | "\(.file) \(.url) \(.sha256)"' "$pins" | while read -r file url sum; do
    curl -fsSL "$url" -o "$cache/$file.tmp"
    echo "$sum  $cache/$file.tmp" | sha256sum -c --quiet - || { echo "sha256 mismatch: $url" >&2; exit 1; }
    mv "$cache/$file.tmp" "$cache/$file"
  done
  jq -r '.images[].ref' "$pins" | while read -r ref; do docker pull -q "$ref" >/dev/null; echo "pulled $ref"; done
}

# render prints one fetched manifest with the namespace moved to e2e-csi and images pinned.
render() {
  local f="$cache/$1"
  [[ -f $f ]] || { echo "missing $f; run fetch first" >&2; exit 1; }
  local script=(-e "s/namespace: default\b/namespace: $ns/" -e "s/namespace: kube-system\b/namespace: $ns/")
  while read -r up; do
    script+=(-e "s|image: $up\$|image: $(image_ref "$up")|")
  done < <(jq -r '.images[].upstream' "$pins")
  script+=(-e 's|^\( *\)# end hostpath args|\1- "--capacity=e2e=5Gi"\n\1- "--max-volume-size=1073741824"|')
  sed "${script[@]}" "$f"
}

classes() {
  cat <<EOF
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: e2e-hostpath
provisioner: hostpath.csi.k8s.io
parameters:
  kind: e2e
reclaimPolicy: Delete
volumeBindingMode: WaitForFirstConsumer
allowVolumeExpansion: true
---
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshotClass
metadata:
  name: e2e-hostpath
driver: hostpath.csi.k8s.io
deletionPolicy: Delete
EOF
}

install() {
  k create namespace "$ns" --dry-run=client -o yaml | k apply -f -
  for f in crd-volumesnapshotclasses.yaml crd-volumesnapshotcontents.yaml crd-volumesnapshots.yaml; do
    render "$f" | k apply -f -
  done
  k wait --for condition=established --timeout=60s \
    crd/volumesnapshotclasses.snapshot.storage.k8s.io crd/volumesnapshotcontents.snapshot.storage.k8s.io \
    crd/volumesnapshots.snapshot.storage.k8s.io
  for f in snapshot-controller-rbac.yaml snapshot-controller.yaml rbac-csi-attacher.yaml rbac-csi-provisioner.yaml \
    rbac-csi-resizer.yaml rbac-csi-snapshotter.yaml rbac-csi-health-monitor.yaml hostpath-driverinfo.yaml hostpath-plugin.yaml; do
    render "$f" | k apply -n "$ns" -f -
  done
  classes | k apply -f -
  k -n "$ns" rollout status deployment/snapshot-controller --timeout=180s
  k -n "$ns" rollout status statefulset/csi-hostpathplugin --timeout=180s
  k -n "$ns" wait --for=condition=Ready --timeout=180s pod/csi-hostpathplugin-0
}

uninstall() {
  k delete volumesnapshotclass e2e-hostpath --ignore-not-found
  k delete storageclass e2e-hostpath --ignore-not-found
  for f in hostpath-plugin.yaml hostpath-driverinfo.yaml rbac-csi-health-monitor.yaml rbac-csi-snapshotter.yaml \
    rbac-csi-resizer.yaml rbac-csi-provisioner.yaml rbac-csi-attacher.yaml snapshot-controller.yaml snapshot-controller-rbac.yaml \
    crd-volumesnapshots.yaml crd-volumesnapshotcontents.yaml crd-volumesnapshotclasses.yaml; do
    render "$f" | k delete -n "$ns" --ignore-not-found --wait=false -f -
  done
  k delete namespace "$ns" --ignore-not-found
}

# claim prints a 100Mi claim on e2e-hostpath; the optional second and third arguments name a
# dataSource kind and name.
claim() {
  cat <<EOF
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: $1, namespace: $ns}
spec:
  storageClassName: e2e-hostpath
  accessModes: [ReadWriteOnce]
  resources: {requests: {storage: $claim_size}}
EOF
  if [[ $# -eq 3 ]]; then
    local group=""
    [[ $2 == VolumeSnapshot ]] && group="apiGroup: snapshot.storage.k8s.io, "
    echo "  dataSource: {${group}kind: $2, name: $3}"
  fi
}

# pod prints a pod that mounts claim $2 at /data and runs shell command $3 once.
pod() {
  cat <<EOF
apiVersion: v1
kind: Pod
metadata: {name: $1, namespace: $ns}
spec:
  restartPolicy: Never
  containers:
    - name: c
      image: $(image_ref busybox:1.37.0)
      command: [sh, -c, "$3"]
      volumeMounts: [{name: d, mountPath: /data}]
  volumes: [{name: d, persistentVolumeClaim: {claimName: $2}}]
EOF
}

run_pod() {
  pod "$@" | k apply -f - >&2
  k -n "$ns" wait --for=jsonpath='{.status.phase}'=Succeeded --timeout=120s "pod/$1" >&2
  k -n "$ns" logs "pod/$1"
}

check_cleanup() {
  k -n "$ns" delete pod e2e-check-write e2e-check-clone e2e-check-restore e2e-check-big --ignore-not-found --wait=true
  k -n "$ns" delete pvc e2e-check-clone e2e-check-restore e2e-check-big --ignore-not-found --wait=true
  k -n "$ns" delete volumesnapshot e2e-check-snap --ignore-not-found --wait=true
  k -n "$ns" delete pvc e2e-check-src --ignore-not-found --wait=true
}

check() {
  trap check_cleanup EXIT
  check_cleanup
  local want="e2e-csi-check-$RANDOM"
  claim e2e-check-src | k apply -f -
  run_pod e2e-check-write e2e-check-src "echo $want > /data/f && sync"

  claim e2e-check-clone PersistentVolumeClaim e2e-check-src | k apply -f -
  local got
  got="$(run_pod e2e-check-clone e2e-check-clone 'cat /data/f')"
  [[ $got == "$want" ]] || { echo "clone read '$got', want '$want'" >&2; exit 1; }
  echo "clone: ok"

  cat <<EOF | k apply -f -
apiVersion: snapshot.storage.k8s.io/v1
kind: VolumeSnapshot
metadata: {name: e2e-check-snap, namespace: $ns}
spec:
  volumeSnapshotClassName: e2e-hostpath
  source: {persistentVolumeClaimName: e2e-check-src}
EOF
  k -n "$ns" wait --for=jsonpath='{.status.readyToUse}'=true --timeout=120s volumesnapshot/e2e-check-snap
  claim e2e-check-restore VolumeSnapshot e2e-check-snap | k apply -f -
  got="$(run_pod e2e-check-restore e2e-check-restore 'cat /data/f')"
  [[ $got == "$want" ]] || { echo "restore read '$got', want '$want'" >&2; exit 1; }
  echo "snapshot restore: ok"

  # The cap: a 2Gi claim is over --max-volume-size, so the driver must refuse it.
  claim_size=2Gi claim e2e-check-big | k apply -f -
  pod e2e-check-big e2e-check-big true | k apply -f -
  local i
  for i in $(seq 60); do
    k -n "$ns" get events --field-selector involvedObject.name=e2e-check-big,reason=ProvisioningFailed -o name | grep -q . && break
    sleep 1
  done
  k -n "$ns" get events --field-selector involvedObject.name=e2e-check-big,reason=ProvisioningFailed \
    -o jsonpath='{.items[*].message}' | grep -q 'exceeds maximum allowed' || { echo "2Gi claim was not refused" >&2; exit 1; }
  echo "size cap: ok"
  echo "check: ok"
}

case "${1:-}" in
  fetch) fetch ;;
  install) install ;;
  check) check ;;
  uninstall) uninstall ;;
  *) echo "usage: $0 fetch|install|check|uninstall" >&2; exit 2 ;;
esac
