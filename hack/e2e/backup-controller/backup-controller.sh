#!/usr/bin/env bash
# backup-controller.sh runs the controller under test, built from this
# worktree's source, in the docker-desktop e2e cluster, in namespace
# backup-system.
#
# Usage: hack/e2e/backup-controller/backup-controller.sh fetch|install|check|rebuild|uninstall
#
#   fetch      builds the repo's Dockerfile into the local Docker engine as
#              backup-controller:e2e. The Dockerfile's build pulls its golang
#              base image by digest, runs apk add ca-certificates and go mod
#              download; nothing else is downloaded.
#   install    waits for cert-manager and Kueue (installed by their own
#              scripts), applies config/crd and deploy/ through overlay/ with
#              the image replaced by the local build, and creates the
#              LocalQueue the kueue-managed namespace needs. The Deployment
#              names the image as backup-controller:e2e-<image id>, so every
#              build is a tag the kubelet has not seen and pulls through
#              Docker Desktop's registry mirror from the local engine.
#   check      the Deployment is Ready, /healthz and /readyz answer, the
#              metrics listener on 8081 serves backup_controller_* series, and
#              the Cluster MutatingWebhookConfiguration has its caBundle.
#              Creates nothing, so there is nothing to clean up.
#   rebuild    fetch, then points the Deployment at the new image and waits
#              for the rollout. For iterating on fixes.
#   uninstall  deletes what install applied, the LocalQueue and the CRDs.
#
# The controller keeps no storage of its own. Every kubectl call passes
# --context docker-desktop. Run it from the flake's shell
# (nix develop -c hack/e2e/backup-controller/backup-controller.sh ...) for
# kubectl and jq.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../../.." && pwd)
pins="$here/pins.json"

context=docker-desktop
kc() { kubectl --context "$context" "$@"; }
pin() { jq -r ".\"backup-controller\".$1" "$pins"; }

ns=$(pin namespace)
image=$(pin image.name)
local_queue=$(pin kueue.localQueue)
cluster_queue=$(pin kueue.clusterQueue)
deploy=backup-controller
webhook_config=backup-controller-bootstrap
probe_ns=e2e-backup-controller-check

# wait_for polls a command every 5 seconds for up to 10 minutes. It prints
# once when it has to wait and says how long it waited.
#
# Parameters:
#   - what names the thing waited for, for the messages.
#   - the rest is the command that succeeds once it is ready.
wait_for() {
  local what=$1; shift
  local start=$SECONDS waited=0
  until "$@" >/dev/null 2>&1; do
    if (( SECONDS - start > 600 )); then
      echo "gave up after 10 minutes waiting for $what" >&2
      return 1
    fi
    if (( waited == 0 )); then echo "waiting for $what ..."; waited=1; fi
    sleep 5
  done
  if (( waited )); then echo "$what ready after $((SECONDS - start))s"; fi
}

cert_manager_ready() {
  kc wait crd certificates.cert-manager.io issuers.cert-manager.io \
    --for=condition=Established --timeout=5s &&
    kc -n cert-manager wait deploy/cert-manager-webhook deploy/cert-manager-cainjector deploy/cert-manager \
      --for=condition=Available --timeout=5s &&
    # The webhook answers: a server-side dry run of an Issuer goes through it.
    kc -n default apply --dry-run=server -f - <<'EOF'
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: e2e-backup-controller-probe
spec:
  selfSigned: {}
EOF
}

kueue_ready() {
  kc wait crd localqueues.kueue.x-k8s.io workloads.kueue.x-k8s.io \
    --for=condition=Established --timeout=5s &&
    kc -n kueue-system wait deploy/kueue-controller-manager --for=condition=Available --timeout=5s &&
    kc get clusterqueue "$cluster_queue"
}

# image_tag prints backup-controller:e2e-<first 12 hex digits of the image id>.
image_tag() {
  local id
  id=$(docker image inspect --format '{{.Id}}' "$image")
  id=${id#sha256:}
  echo "${image%%:*}:e2e-${id:0:12}"
}

fetch() {
  docker build -t "$image" --build-arg "VERSION=e2e-$(git -C "$repo" rev-parse --short HEAD)" "$repo"
  local tag
  tag=$(image_tag)
  docker tag "$image" "$tag"
  echo "built $image, tagged $tag"
}

# apply_controller renders overlay/ with the image named by its id and applies it.
apply_controller() {
  local tag
  tag=$(image_tag)
  docker tag "$image" "$tag"
  kc apply --server-side --field-manager=e2e-backup-controller -f "$repo/config/crd"
  kc kustomize "$here/overlay" | sed "s|image: $image\$|image: $tag|" |
    kc apply --server-side --field-manager=e2e-backup-controller -f -
}

install() {
  docker image inspect "$image" >/dev/null 2>&1 ||
    { echo "$image is not in the local Docker engine; run fetch first" >&2; exit 1; }
  wait_for "cert-manager (CRDs, deployments, webhook)" cert_manager_ready
  wait_for "Kueue (CRDs, controller, ClusterQueue $cluster_queue)" kueue_ready
  apply_controller
  # prod's terragrunt module writes this LocalQueue beside the namespace label
  # (docs/integration.md); a mover labelled for it is admitted through it.
  kc apply --server-side --field-manager=e2e-backup-controller -f - <<EOF
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: $local_queue
  namespace: $ns
spec:
  clusterQueue: $cluster_queue
EOF
  kc -n "$ns" rollout status "deploy/$deploy" --timeout=300s
}

rebuild() {
  fetch
  local tag
  tag=$(image_tag)
  kc -n "$ns" set image "deploy/$deploy" "controller=$tag"
  # set image is a no-op when the build changed nothing; restart anyway so
  # the pod is fresh.
  kc -n "$ns" rollout restart "deploy/$deploy"
  kc -n "$ns" rollout status "deploy/$deploy" --timeout=300s
}

fail() { echo "backup-controller check: $*" >&2; exit 1; }

check() {
  kc -n "$ns" rollout status "deploy/$deploy" --timeout=180s
  kc -n "$ns" wait "deploy/$deploy" --for=condition=Available --timeout=60s
  local pod want got
  pod=$(kc -n "$ns" get pod -l app.kubernetes.io/name=backup-controller \
    --field-selector=status.phase=Running -o jsonpath='{.items[0].metadata.name}')
  [[ -n "$pod" ]] || fail "no running controller pod"
  kc -n "$ns" wait "pod/$pod" --for=condition=Ready --timeout=60s

  # The pod runs the image built from this worktree.
  want=$(image_tag)
  got=$(kc -n "$ns" get pod "$pod" -o jsonpath='{.spec.containers[?(@.name=="controller")].image}')
  [[ "$got" == "$want" ]] || fail "pod runs $got, want $want"

  # The scratch image has no shell, so the endpoints are read through the
  # API server's pod proxy.
  local proxy="/api/v1/namespaces/$ns/pods/$pod"
  for path in healthz readyz; do
    kc get --raw "$proxy:8082/proxy/$path" >/dev/null || fail "/$path did not answer 200"
  done
  # The backup_controller_* series are labelled by namespace and exist only
  # for a namespace with a schedule, so the check makes one (its schedule
  # ticks once a year and it marks nothing enabled, so no run starts) and
  # removes it again.
  local series=0 i
  trap 'kc delete namespace "$probe_ns" --ignore-not-found --wait=true >/dev/null' EXIT
  kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $probe_ns
  annotations:
    backup.wlz.li/schedule: "0 5 1 1 *"
EOF
  for i in $(seq 30); do
    series=$(kc get --raw "$proxy:8081/proxy/metrics" |
      grep -c "^backup_controller_namespace_[a-z_]*{namespace=\"$probe_ns\"}" || true)
    (( series > 0 )) && break
    sleep 2
  done
  (( series > 0 )) || fail "the metrics listener on 8081 serves no backup_controller_* series for $probe_ns"

  local ca
  ca=$(kc get mutatingwebhookconfiguration "$webhook_config" \
    -o jsonpath='{range .webhooks[*]}{.clientConfig.caBundle}{"\n"}{end}')
  [[ -n "$ca" ]] && ! grep -qx '' <<<"$ca" || fail "$webhook_config has a webhook without a caBundle"

  kc -n "$ns" get localqueue "$local_queue" >/dev/null || fail "LocalQueue $local_queue is missing"
  echo "backup-controller: $pod Ready on $got, /healthz and /readyz answer, $series backup_controller_* series on 8081, caBundle set on $webhook_config"
}

uninstall() {
  kc -n "$ns" delete localqueue "$local_queue" --ignore-not-found 2>/dev/null || true
  kc kustomize "$here/overlay" | kc delete --ignore-not-found --wait=true -f -
  kc delete --ignore-not-found -f "$repo/config/crd"
  # The per-build tags; backup-controller:e2e stays so install works again
  # without a fetch.
  docker images --format '{{.Repository}}:{{.Tag}}' "${image%%:*}" | grep -- ':e2e-' |
    xargs -r docker rmi >/dev/null || true
}

case "${1:-}" in
  fetch|install|check|rebuild|uninstall) "$1" ;;
  *) echo "usage: $0 fetch|install|check|rebuild|uninstall" >&2; exit 2 ;;
esac
