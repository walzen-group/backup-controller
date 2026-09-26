#!/usr/bin/env bash
# volsync.sh installs the real VolSync manager (chart 0.16.0, prod's) into the
# docker-desktop e2e cluster, in namespace volsync-system.
#
# Usage: volsync.sh fetch|install|check|uninstall
#
# fetch pulls the chart into cache/ and checks its sha256, and pulls the manager
# image (which is also the restic mover image) into the local Docker engine by
# digest. install works from those alone. check waits for the manager to be
# Ready and for the ReplicationSource and ReplicationDestination CRDs to be
# served. uninstall removes the release, the namespace and the chart's CRDs.
# Needs helm, kubectl and jq (nix develop -c).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/pins.json"
cache="$here/cache"
ctx=docker-desktop
ns="$(jq -r .volsync.namespace "$pins")"
chart_repo="$(jq -r .volsync.chart.repository "$pins")"
chart_version="$(jq -r .volsync.chart.version "$pins")"
chart_file="$cache/$(jq -r .volsync.chart.file "$pins")"
chart_sha="$(jq -r .volsync.chart.sha256 "$pins")"
image="$(jq -r '.volsync.image | "\(.repository):\(.tag)@\(.digest)"' "$pins")"
image_by_digest="$(jq -r '.volsync.image | "\(.repository)@\(.digest)"' "$pins")"

verify_chart() {
  echo "$chart_sha  $chart_file" | sha256sum -c --quiet
}

fetch() {
  mkdir -p "$cache"
  if [[ ! -f "$chart_file" ]]; then
    helm pull volsync --repo "$chart_repo" --version "$chart_version" -d "$cache"
  fi
  verify_chart
  docker pull "$image_by_digest"
  docker tag "$image_by_digest" "$(jq -r '.volsync.image | "\(.repository):\(.tag)"' "$pins")"
  local restic
  restic="$(docker run --rm --entrypoint restic "$image_by_digest" version | awk '{print $2}')"
  if [[ "$restic" != "$(jq -r .volsync.restic "$pins")" ]]; then
    echo "restic in the mover image is $restic, pins.json says $(jq -r .volsync.restic "$pins")" >&2
    exit 1
  fi
}

install() {
  verify_chart
  # Every mover image points at the pinned digest; prod leaves them at the
  # chart's defaults, which name the same image by tag.
  helm --kube-context "$ctx" upgrade --install volsync "$chart_file" \
    --namespace "$ns" --create-namespace --wait --timeout 5m \
    --set image.image="$image" \
    --set restic.image="$image" \
    --set rclone.image="$image" \
    --set rsync.image="$image" \
    --set rsync-tls.image="$image" \
    --set syncthing.image="$image"
}

check() {
  kubectl --context "$ctx" -n "$ns" rollout status deploy/volsync --timeout=180s
  kubectl --context "$ctx" -n "$ns" wait pod -l app.kubernetes.io/name=volsync \
    --for=condition=Ready --timeout=120s
  local got
  got="$(kubectl --context "$ctx" -n "$ns" get deploy volsync \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="manager")].image}')"
  if [[ "$got" != "$image" ]]; then
    echo "manager image is $got, want $image" >&2
    exit 1
  fi
  for crd in replicationsources.volsync.backube replicationdestinations.volsync.backube; do
    kubectl --context "$ctx" wait crd "$crd" --for=condition=Established --timeout=60s
  done
  # Served: a list through the API returns without error.
  kubectl --context "$ctx" get replicationsources.volsync.backube -A >/dev/null
  kubectl --context "$ctx" get replicationdestinations.volsync.backube -A >/dev/null
  echo "volsync: manager Ready ($got), ReplicationSource and ReplicationDestination served"
}

uninstall() {
  helm --kube-context "$ctx" uninstall volsync --namespace "$ns" --wait --ignore-not-found || true
  kubectl --context "$ctx" delete namespace "$ns" --ignore-not-found --wait=true
  # manageCRDs puts the CRDs in the chart's templates, so helm uninstall
  # removes them; this catches any left behind.
  kubectl --context "$ctx" get crd -o name | grep '\.volsync\.backube$' \
    | xargs -r kubectl --context "$ctx" delete --ignore-not-found
}

case "${1:-}" in
  fetch|install|check|uninstall) "$1" ;;
  *) echo "usage: $0 fetch|install|check|uninstall" >&2; exit 2 ;;
esac
