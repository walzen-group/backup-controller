#!/usr/bin/env bash
# cnpg.sh installs the real CloudNativePG operator (chart 0.29.0, operator
# 1.30.0, prod's) and the barman-cloud plugin (chart 0.8.0, plugin 0.15.0,
# prod's) into the docker-desktop e2e cluster, both in namespace cnpg-system as
# prod has them.
#
# Usage: cnpg.sh fetch|install|check|uninstall
#
# fetch pulls both charts into cache/ and checks their sha256, and pulls the
# operator, plugin, sidecar and PostgreSQL images into the local Docker engine
# by digest. install works from those alone. It needs cert-manager (see
# ../cert-manager), which issues the plugin's gRPC certificates, and installs the
# operator first because the plugin registers with it. check waits for the
# operator and the plugin to be Ready and for the Cluster, Backup and ObjectStore
# CRDs to be served, then runs a one-instance Cluster on a 512Mi local-path
# volume (StorageClass standard, or CNPG_CHECK_STORAGE_CLASS; WAL on the same
# volume, no archiving) in a temporary namespace until
# it is healthy, and deletes it. uninstall removes both releases, the namespace
# and the charts' CRDs (the charts keep them on a helm uninstall). Needs helm,
# kubectl and jq (nix develop -c).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/pins.json"
cache="$here/cache"
ctx=docker-desktop
p() { jq -r ".cnpg$1" "$pins"; }
ns="$(p .namespace)"
charts=(operator plugin)
images=(.operator.image .plugin.image .plugin.sidecarImage .postgres)
k() { kubectl --context "$ctx" "$@"; }
# storage_class is the check Cluster's StorageClass. The docker-desktop kind
# cluster names its rancher.io/local-path class "standard" (the default).
storage_class="${CNPG_CHECK_STORAGE_CLASS:-standard}"

chart_file() { echo "$cache/$(p ".$1.chart.file")"; }

verify_charts() {
  local c
  for c in "${charts[@]}"; do
    echo "$(p ".$c.chart.sha256")  $(chart_file "$c")" | sha256sum -c --quiet
  done
}

# ref prints an image reference as repository:tag@digest.
ref() { p "$1 | \"\(.repository):\(.tag)@\(.digest)\""; }

crds() {
  k get crd -o name | grep -E '\.(postgresql|barmancloud)\.cnpg\.io$' || true
}

fetch() {
  mkdir -p "$cache"
  local c i by_digest
  for c in "${charts[@]}"; do
    if [[ ! -f "$(chart_file "$c")" ]]; then
      helm pull "$(p ".$c.chart.repository")" --version "$(p ".$c.chart.version")" -d "$cache"
    fi
  done
  verify_charts
  for i in "${images[@]}"; do
    by_digest="$(p "$i | \"\(.repository)@\(.digest)\"")"
    docker pull "$by_digest"
    docker tag "$by_digest" "$(p "$i | \"\(.repository):\(.tag)\"")"
  done
}

install() {
  verify_charts
  if ! k get crd certificates.cert-manager.io issuers.cert-manager.io >/dev/null 2>&1; then
    echo "cert-manager is not installed; run hack/e2e/cert-manager/cert-manager.sh install first" >&2
    exit 1
  fi
  # Prod installs both charts with their default values. The only values set
  # here pin the images: the charts render repository:tag, and a tag of
  # "<tag>@<digest>" makes that a pinned reference.
  helm --kube-context "$ctx" upgrade --install "$(p .operator.release)" "$(chart_file operator)" \
    --namespace "$ns" --create-namespace --wait --timeout 5m \
    --set image.repository="$(p .operator.image.repository)" \
    --set image.tag="$(p '.operator.image | "\(.tag)@\(.digest)"')"
  helm --kube-context "$ctx" upgrade --install "$(p .plugin.release)" "$(chart_file plugin)" \
    --namespace "$ns" --wait --timeout 5m \
    --set image.tag="$(p '.plugin.image | "\(.tag)@\(.digest)"')" \
    --set sidecarImage.tag="$(p '.plugin.sidecarImage | "\(.tag)@\(.digest)"')"
}

# served checks that an API group version serves each named resource.
served() {
  local gv="$1" r list
  shift
  list="$(k get --raw "/apis/$gv" | jq -r '.resources[].name')"
  for r in "$@"; do
    if ! grep -qx "$r" <<<"$list"; then
      echo "$gv does not serve $r" >&2
      return 1
    fi
  done
}

check() {
  k -n "$ns" wait deployment --all --for=condition=Available --timeout=3m
  served postgresql.cnpg.io/v1 clusters backups
  served barmancloud.cnpg.io/v1 objectstores
  k -n "$ns" get certificate -o name | while read -r c; do
    k -n "$ns" wait "$c" --for=condition=Ready --timeout=1m
  done

  tns="cnpg-check-$RANDOM"
  trap 'k delete namespace "$tns" --ignore-not-found --wait=false >/dev/null' EXIT
  k create namespace "$tns"
  # 512Mi holds data and WAL together; the small WAL sizes keep the volume's
  # real use well under that, since local-path does not enforce the size.
  k apply -n "$tns" -f - <<YAML
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: check
spec:
  instances: 1
  imageName: $(ref .postgres)
  storage:
    storageClass: $storage_class
    size: 512Mi
  postgresql:
    parameters:
      min_wal_size: 32MB
      max_wal_size: 64MB
      shared_buffers: 32MB
YAML
  k -n "$tns" wait cluster/check --for=condition=Ready --timeout=5m
  local phase
  phase="$(k -n "$tns" get cluster/check -o jsonpath='{.status.phase}')"
  if [[ "$phase" != "Cluster in healthy state" ]]; then
    echo "cluster phase is $phase" >&2
    exit 1
  fi
  k -n "$tns" delete cluster/check --wait --timeout=2m
  k delete namespace "$tns" --wait --timeout=2m
  trap - EXIT
  echo "cnpg: operator and plugin Ready, CRDs served, a one-instance Cluster became healthy"
}

uninstall() {
  helm --kube-context "$ctx" uninstall "$(p .plugin.release)" --namespace "$ns" --ignore-not-found --wait
  helm --kube-context "$ctx" uninstall "$(p .operator.release)" --namespace "$ns" --ignore-not-found --wait
  local c
  c="$(crds)"
  if [[ -n "$c" ]]; then
    # shellcheck disable=SC2086
    k delete $c --ignore-not-found
  fi
  k delete namespace "$ns" --ignore-not-found
}

case "${1:-}" in
  fetch | install | check | uninstall) "$1" ;;
  *)
    echo "usage: $0 fetch|install|check|uninstall" >&2
    exit 2
    ;;
esac
