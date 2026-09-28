#!/usr/bin/env bash
# controller.sh builds the controller image from this working tree and runs it
# in the kind cluster of Docker Desktop (kubectl context docker-desktop), so the
# e2e tests run against the code in the tree. Run hack/kind/up.sh first.
#
# Usage: hack/kind/controller.sh deploy|delete
#   deploy  builds the image, applies the CRDs and the deploy/ manifests with
#           the local image, and waits for the webhook CA and the rollout.
#   delete  removes what deploy applied, except the CRDs. The CRDs stay, so the
#           BackupRuns, RestoreRuns and VolumeRestores in the cluster stay too.
set -euo pipefail

here="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
root="$(cd "$here/../.." && pwd)"
ctx=docker-desktop
ns=backup-system
release_image=ghcr.io/walzen-group/backup-controller
webhook=backup-controller-bootstrap

k() { kubectl --context "$ctx" "$@"; }
log() { printf '%s controller.sh: %s\n' "$(date +%T)" "$*" >&2; }

# build builds the image and tags it with the first 12 hex digits of its image
# ID, so each new build gets a new tag. It prints the tag.
build() {
  local id
  id="$(docker build -q "$root")"
  id="${id#sha256:}"
  local image="backup-controller:dev-${id:0:12}"
  docker tag "$id" "$image"
  printf '%s\n' "$image"
}

# restore_image is the image of the controller's restore Jobs: VolSync's
# mover image, as prod's infra unit passes it in --restore-image.
restore_image=quay.io/backube/volsync:0.16.0

# render prints the deploy/ manifests without the CRDs, with the release image
# replaced by the image in $1, and --restore-image added to the controller's
# arguments. It reads only local files.
render() {
  kubectl kustomize "$root/deploy" |
    yq 'select(.kind != "CustomResourceDefinition")' |
    RESTORE_IMAGE="$restore_image" yq '(select(.kind == "Deployment") | .spec.template.spec.containers[0].args) += ["--restore-image=" + strenv(RESTORE_IMAGE)]' |
    sed -e "s|image: ${release_image}[:@].*|image: $1|"
}

# crds applies the CRDs from config/crd and waits until the API server serves
# them.
crds() {
  k apply --server-side -f "$root/config/crd"
  k wait --for condition=established --timeout=60s \
    crd/backupruns.backup.wlz.li crd/restoreruns.backup.wlz.li crd/volumerestores.backup.wlz.li
}

# wait_ca waits until cert-manager has written the CA into every entry of the
# MutatingWebhookConfiguration.
wait_ca() {
  local tries=60 bundles
  k -n "$ns" wait --for condition=Ready --timeout=120s certificate/backup-controller-webhook
  while ((tries-- > 0)); do
    # One [bundle] per entry, so an entry with no CA shows as [].
    bundles="$(k get mutatingwebhookconfiguration "$webhook" \
      -o jsonpath='{range .webhooks[*]}[{.clientConfig.caBundle}]{end}')"
    if [[ -n "$bundles" && "$bundles" != *'[]'* ]]; then
      return 0
    fi
    sleep 2
  done
  log "cert-manager did not set the caBundle of $webhook"
  return 1
}

# deploy builds the image, applies everything, and waits until the controller
# runs the new image and the webhook has its CA.
deploy() {
  local image
  image="$(build)"
  log "image $image"
  crds
  render "$image" | k apply --server-side -f -
  # The populator's restores wait for Kueue through a LocalQueue in the
  # controller namespace, as on prod, where the backup-controller unit
  # creates it.
  k apply --server-side -f - <<YAML
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: backups
  namespace: $ns
spec:
  clusterQueue: backup
YAML
  wait_ca
  k -n "$ns" rollout status deployment/backup-controller --timeout=300s
  log "running $image"
}

# delete removes the webhook configuration first, so that no Cluster creation
# waits for a webhook that is gone, and then the other objects of deploy/.
delete() {
  k delete --ignore-not-found mutatingwebhookconfiguration "$webhook"
  render "$release_image" | k delete --ignore-not-found --timeout=300s -f -
}

case "${1:-}" in
  deploy | delete) "$1" ;;
  *)
    printf 'usage: %s deploy|delete\n' "$0" >&2
    exit 2
    ;;
esac
