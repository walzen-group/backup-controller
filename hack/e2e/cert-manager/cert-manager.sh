#!/usr/bin/env bash
# cert-manager.sh installs the real cert-manager (chart v1.21.2, prod's, CRDs
# from the chart as prod has them) into the docker-desktop e2e cluster, in
# namespace cert-manager.
#
# Usage: cert-manager.sh fetch|install|check|uninstall
#
# fetch pulls the chart into cache/ and checks its sha256, and pulls the four
# images the chart runs into the local Docker engine by digest. install works
# from those alone and points every image at its pinned digest. check has a
# self-signed Issuer issue a Certificate in a temporary namespace, waits for it
# to be Ready and for its Secret, then deletes the namespace. uninstall removes
# the release, the namespace and the chart's CRDs (the chart keeps them on a
# helm uninstall). Needs helm, kubectl and jq (nix develop -c).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/pins.json"
cache="$here/cache"
ctx=docker-desktop
p() { jq -r ".\"cert-manager\"$1" "$pins"; }
ns="$(p .namespace)"
release="$(p .release)"
chart_repo="$(p .chart.repository)"
chart_version="$(p .chart.version)"
chart_file="$cache/$(p .chart.file)"
chart_sha="$(p .chart.sha256)"
components=(controller webhook cainjector startupapicheck)
crds=(
  certificaterequests.cert-manager.io
  certificates.cert-manager.io
  challenges.acme.cert-manager.io
  clusterissuers.cert-manager.io
  issuers.cert-manager.io
  orders.acme.cert-manager.io
)

verify_chart() {
  echo "$chart_sha  $chart_file" | sha256sum -c --quiet
}

fetch() {
  mkdir -p "$cache"
  if [[ ! -f "$chart_file" ]]; then
    helm pull "$chart_repo" --version "$chart_version" -d "$cache"
  fi
  verify_chart
  local c by_digest
  for c in "${components[@]}"; do
    by_digest="$(p ".images.$c | \"\(.repository)@\(.digest)\"")"
    docker pull "$by_digest"
    docker tag "$by_digest" "$(p ".images.$c | \"\(.repository):\(.tag)\"")"
  done
}

install() {
  verify_chart
  # The chart's value for the controller image is image; the others sit under
  # their component's key.
  local c key args=()
  for c in "${components[@]}"; do
    key="$c.image"
    [[ "$c" == controller ]] && key=image
    args+=(--set "$key.repository=$(p ".images.$c.repository")")
    args+=(--set "$key.tag=$(p ".images.$c.tag")")
    args+=(--set "$key.digest=$(p ".images.$c.digest")")
  done
  helm --kube-context "$ctx" upgrade --install "$release" "$chart_file" \
    --namespace "$ns" --create-namespace --wait --timeout 5m \
    --set crds.enabled=true "${args[@]}"
}

check() {
  local tns
  tns="cert-manager-check-$RANDOM"
  trap 'kubectl --context "$ctx" delete namespace "$tns" --ignore-not-found --wait=false >/dev/null' EXIT
  kubectl --context "$ctx" get crd "${crds[@]}" >/dev/null
  kubectl --context "$ctx" -n "$ns" wait deployment --all --for=condition=Available --timeout=2m
  kubectl --context "$ctx" create namespace "$tns"
  kubectl --context "$ctx" apply -n "$tns" -f - <<YAML
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: selfsigned
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: check
spec:
  secretName: check-tls
  commonName: check.e2e.local
  dnsNames: [check.e2e.local]
  issuerRef:
    name: selfsigned
    kind: Issuer
YAML
  kubectl --context "$ctx" -n "$tns" wait certificate/check --for=condition=Ready --timeout=2m
  kubectl --context "$ctx" -n "$tns" get secret check-tls -o jsonpath='{.data.tls\.crt}' | base64 -d \
    | grep -q 'BEGIN CERTIFICATE'
  kubectl --context "$ctx" delete namespace "$tns" --wait --timeout=2m
  trap - EXIT
  echo "cert-manager: a self-signed Issuer issued a Certificate"
}

uninstall() {
  helm --kube-context "$ctx" uninstall "$release" --namespace "$ns" --ignore-not-found --wait
  kubectl --context "$ctx" delete crd "${crds[@]}" --ignore-not-found
  kubectl --context "$ctx" delete namespace "$ns" --ignore-not-found
}

case "${1:-}" in
  fetch | install | check | uninstall) "$1" ;;
  *)
    echo "usage: $0 fetch|install|check|uninstall" >&2
    exit 2
    ;;
esac
