#!/usr/bin/env bash
# kueue.sh installs the real Kueue controller (chart 0.19.5, prod's) into the
# docker-desktop e2e cluster, in namespace kueue-system, with ClusterQueue e2e.
#
# Usage: kueue.sh fetch|install|check|uninstall
#
# fetch pulls the chart into cache/ by digest and checks its sha256, and pulls
# the controller image into the local Docker engine by digest. install works
# from those alone: the chart with values.yaml (prod's controller settings),
# then queue.yaml (flavor default, ClusterQueue e2e). check creates a namespace
# labelled kueue-managed=true with a LocalQueue on e2e, runs a pod with the
# queue-name label, requires it to be admitted and to succeed, and deletes the
# namespace. uninstall removes the queue objects, the release, the namespace
# and the chart's CRDs. Needs helm, kubectl and jq (nix develop -c).
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
pins="$here/pins.json"
cache="$here/cache"
ctx=docker-desktop
ns="$(jq -r .kueue.namespace "$pins")"
queue="$(jq -r .kueue.clusterQueue "$pins")"
chart_repo="$(jq -r .kueue.chart.repository "$pins")"
chart_digest="$(jq -r .kueue.chart.digest "$pins")"
chart_file="$cache/$(jq -r .kueue.chart.file "$pins")"
chart_sha="$(jq -r .kueue.chart.sha256 "$pins")"
image_repo="$(jq -r .kueue.image.repository "$pins")"
image_tag="$(jq -r '.kueue.image | "\(.tag)@\(.digest)"' "$pins")"
image_by_digest="$(jq -r '.kueue.image | "\(.repository)@\(.digest)"' "$pins")"
test_image="$image_by_digest"

verify_chart() {
  echo "$chart_sha  $chart_file" | sha256sum -c --quiet
}

fetch() {
  mkdir -p "$cache"
  if [[ ! -f "$chart_file" ]]; then
    local tmp
    tmp="$(mktemp -d)"
    helm pull "$chart_repo@$chart_digest" -d "$tmp"
    mv "$tmp"/*.tgz "$chart_file"
    rmdir "$tmp"
  fi
  verify_chart
  docker pull "$image_by_digest"
  docker tag "$image_by_digest" "$image_repo:$(jq -r .kueue.image.tag "$pins")"
}

install() {
  verify_chart
  helm --kube-context "$ctx" upgrade --install kueue "$chart_file" \
    --namespace "$ns" --create-namespace --wait --timeout 10m \
    -f "$here/values.yaml" \
    --set controllerManager.manager.image.repository="$image_repo" \
    --set controllerManager.manager.image.tag="$image_tag"
  kubectl --context "$ctx" -n "$ns" rollout status deploy/kueue-controller-manager --timeout=300s
  # The webhook answers only once its certificate is in place; retry the apply
  # until it does.
  local i
  for i in $(seq 1 30); do
    if kubectl --context "$ctx" apply -f "$here/queue.yaml"; then
      return 0
    fi
    sleep 5
  done
  echo "kueue: queue.yaml was not accepted" >&2
  exit 1
}

check() {
  tns="kueue-e2e-check-$RANDOM"
  trap 'kubectl --context "$ctx" delete namespace "$tns" --ignore-not-found --wait=false >/dev/null' EXIT
  kubectl --context "$ctx" create namespace "$tns"
  kubectl --context "$ctx" label namespace "$tns" kueue-managed=true
  kubectl --context "$ctx" apply -f - <<YAML
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: e2e
  namespace: $tns
spec:
  clusterQueue: $queue
---
apiVersion: v1
kind: Pod
metadata:
  name: probe
  namespace: $tns
  labels:
    kueue.x-k8s.io/queue-name: e2e
spec:
  restartPolicy: Never
  containers:
  - name: probe
    image: $test_image
    imagePullPolicy: IfNotPresent
    command: ["/manager", "--help"]
YAML
  # Kueue gates the pod until its Workload is admitted.
  local gates
  gates="$(kubectl --context "$ctx" -n "$tns" get pod probe -o jsonpath='{.metadata.labels.kueue\.x-k8s\.io/managed}')"
  if [[ "$gates" != "true" ]]; then
    echo "kueue: the pod was not taken over by Kueue (label kueue.x-k8s.io/managed missing)" >&2
    exit 1
  fi
  local wl=""
  for _ in $(seq 1 60); do
    wl="$(kubectl --context "$ctx" -n "$tns" get workloads.kueue.x-k8s.io -o name | head -n1)"
    [[ -n "$wl" ]] && break
    sleep 1
  done
  [[ -n "$wl" ]] || { echo "kueue: no Workload for the pod" >&2; exit 1; }
  kubectl --context "$ctx" -n "$tns" wait "$wl" --for=condition=Admitted --timeout=120s
  kubectl --context "$ctx" -n "$tns" wait pod probe --for=jsonpath='{.status.phase}'=Succeeded --timeout=180s
  echo "kueue: pod admitted through ClusterQueue $queue and ran ($wl)"
}

uninstall() {
  kubectl --context "$ctx" delete -f "$here/queue.yaml" --ignore-not-found || true
  helm --kube-context "$ctx" uninstall kueue --namespace "$ns" --wait --ignore-not-found || true
  kubectl --context "$ctx" delete namespace "$ns" --ignore-not-found --wait=true
  kubectl --context "$ctx" get crd -o name | grep '\.kueue\.x-k8s\.io$' \
    | xargs -r kubectl --context "$ctx" delete --ignore-not-found
}

case "${1:-}" in
  fetch|install|check|uninstall) "$1" ;;
  *) echo "usage: $0 fetch|install|check|uninstall" >&2; exit 2 ;;
esac
