#!/usr/bin/env bash
# Installs Flux v2.9.5, the release prod runs, into the e2e cluster's
# flux-system namespace, with source-controller and kustomize-controller only.
# Run it inside the flake's dev shell (nix develop -c); it needs kubectl, yq,
# jq and docker.
#
# Subcommands:
#   fetch      downloads the release's install.yaml (checked against the sha256
#              in pins.json) into .tmp/e2e/flux and pulls every image in
#              pins.json, by tag and digest, into the local Docker engine
#   install    applies the two controllers, their CRDs, RBAC and network
#              policies from the saved install.yaml, with images by digest,
#              plus an in-cluster OCI registry (e2e-registry) that serves
#              artifacts to source-controller
#   check      proves a Kustomization applies, records its inventory, and
#              honours suspend and resume; cleans up what it created
#   uninstall  removes everything install applied
#
# Prod installs Flux through the flux-operator; this script applies the
# release's plain install manifests instead, filtered to the two controllers
# the controller under test needs.
#
# The source is an OCIRepository served by registry:3.0.0 in flux-system. A
# GitRepository would need a smart-HTTP or SSH git server image (go-git can't
# read the dumb HTTP protocol), and a Bucket needs an S3 server; the registry
# is one small pinned image with no configuration. The check pushes its
# artifact from inside the cluster with a Job running the release's own
# flux-cli image (flux push artifact), so nothing leaves the cluster at test
# time and the host needs no route to the registry.
set -euo pipefail

CTX=(--context docker-desktop)
NS=flux-system
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
CACHE="$ROOT/.tmp/e2e/flux"
PINS="$HERE/pins.json"
REGISTRY_SVC="e2e-registry.$NS.svc.cluster.local:5000"

k() { kubectl "${CTX[@]}" "$@"; }

pin() { jq -r ".flux$1" "$PINS"; }

# ref prints the image reference name:tag@digest for an entry in pins.json.
ref() {
	local e=".images[\"$1\"]"
	echo "$(pin "$e.image"):$(pin "$e.tag")@$(pin "$e.digest")"
}

fetch() {
	mkdir -p "$CACHE"
	local url sum
	url="$(pin .installManifest.url)"
	sum="$(pin .installManifest.sha256)"
	curl -fsSL -o "$CACHE/install.yaml.part" "$url"
	echo "$sum  $CACHE/install.yaml.part" | sha256sum -c -
	mv "$CACHE/install.yaml.part" "$CACHE/install.yaml"
	for img in $(jq -r '.flux.images | keys[]' "$PINS"); do
		docker pull -q "$(ref "$img")"
	done
}

# manifest prints install.yaml cut down to the namespace, the source and
# kustomize CRDs, the shared RBAC and network policies, and the objects of the
# two controllers, with their images pinned by digest.
manifest() {
	[[ -f "$CACHE/install.yaml" ]] || { echo "run fetch first" >&2; exit 1; }
	echo "$(pin .installManifest.sha256)  $CACHE/install.yaml" | sha256sum -c --quiet -
	yq 'select(
		(.kind == "Namespace" or .kind == "ResourceQuota" or .kind == "ClusterRole" or
		 .kind == "ClusterRoleBinding" or .kind == "NetworkPolicy") or
		(.kind == "CustomResourceDefinition" and
		 (.spec.group == "source.toolkit.fluxcd.io" or .spec.group == "kustomize.toolkit.fluxcd.io")) or
		(.metadata.labels["app.kubernetes.io/component"] == "source-controller" or
		 .metadata.labels["app.kubernetes.io/component"] == "kustomize-controller")
	)' "$CACHE/install.yaml" |
		sed -e "s|image: ghcr.io/fluxcd/source-controller:v1.9.5$|image: $(ref source-controller)|" \
			-e "s|image: ghcr.io/fluxcd/kustomize-controller:v1.9.5$|image: $(ref kustomize-controller)|"
}

registry() {
	cat <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: e2e-registry
  namespace: $NS
  labels: {app: e2e-registry}
spec:
  replicas: 1
  selector: {matchLabels: {app: e2e-registry}}
  template:
    metadata:
      labels: {app: e2e-registry}
    spec:
      containers:
        - name: registry
          image: $(ref registry)
          imagePullPolicy: IfNotPresent
          ports: [{containerPort: 5000}]
          readinessProbe: {httpGet: {path: /v2/, port: 5000}}
          volumeMounts: [{name: data, mountPath: /var/lib/registry}]
      volumes: [{name: data, emptyDir: {}}]
---
apiVersion: v1
kind: Service
metadata:
  name: e2e-registry
  namespace: $NS
spec:
  selector: {app: e2e-registry}
  ports: [{port: 5000, targetPort: 5000}]
EOF
}

install() {
	local m
	m="$(manifest)"
	if grep -E 'image: ghcr.io/fluxcd/' <<<"$m" | grep -vq '@sha256:'; then
		echo "an image is not pinned by digest" >&2
		exit 1
	fi
	k apply --server-side -f - <<<"$m"
	k -n "$NS" wait --for=condition=Established --timeout=60s \
		crd/ocirepositories.source.toolkit.fluxcd.io crd/kustomizations.kustomize.toolkit.fluxcd.io
	registry | k apply -f -
	k -n "$NS" rollout status --timeout=180s deploy/source-controller deploy/kustomize-controller deploy/e2e-registry
}

# wait_for polls a shell condition once a second until it holds or the
# timeout in seconds runs out.
wait_for() {
	local timeout="$1" what="$2"
	shift 2
	for ((i = 0; i < timeout; i++)); do
		if "$@" >/dev/null 2>&1; then return 0; fi
		sleep 1
	done
	echo "timed out waiting for $what" >&2
	return 1
}

replicas() { k -n "$CHECK_NS" get deploy flux-check -o jsonpath='{.spec.replicas}'; }
replicas_are() { [[ "$(replicas)" == "$1" ]]; }
# reconcile asks kustomize-controller to reconcile the check's Kustomization
# now and keeps the request's token in TOKEN.
reconcile() {
	TOKEN="$(date +%s%N)"
	k -n "$NS" annotate --overwrite kustomization "$CHECK" "reconcile.fluxcd.io/requestedAt=$TOKEN"
}
handled() { k -n "$NS" get kustomization "$CHECK" -o jsonpath='{.status.lastHandledReconcileAt}'; }
handled_token() { [[ "$(handled)" == "$TOKEN" ]]; }

cleanup_check() {
	set +e
	k -n "$NS" delete kustomization "$CHECK" --ignore-not-found --wait --timeout=60s
	k -n "$NS" delete ocirepository "$CHECK" --ignore-not-found --timeout=60s
	k -n "$NS" delete job "$CHECK-push" --ignore-not-found --cascade=foreground --timeout=60s
	k -n "$NS" delete configmap "$CHECK" --ignore-not-found
	k delete namespace "$CHECK_NS" --ignore-not-found --timeout=120s
}

check() {
	CHECK="flux-check-$(printf %x "$RANDOM$RANDOM")"
	CHECK_NS="$CHECK"
	trap cleanup_check EXIT
	k create namespace "$CHECK_NS"

	k -n "$NS" create configmap "$CHECK" --from-file=deploy.yaml=/dev/stdin <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flux-check
spec:
  replicas: 2
  selector: {matchLabels: {app: flux-check}}
  template:
    metadata:
      labels: {app: flux-check}
    spec:
      terminationGracePeriodSeconds: 0
      containers:
        - name: sleep
          image: $(ref busybox)
          imagePullPolicy: IfNotPresent
          command: [sleep, "86400"]
EOF

	k apply -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $CHECK-push
  namespace: $NS
spec:
  backoffLimit: 2
  template:
    spec:
      restartPolicy: Never
      containers:
        - name: push
          image: $(ref flux-cli)
          imagePullPolicy: IfNotPresent
          env: [{name: HOME, value: /tmp}]
          # flux writes a temporary file next to --path, so the manifests are
          # copied out of the read-only ConfigMap mount first.
          command: [sh, -c]
          args:
            - >-
              mkdir /tmp/m && cp /manifests/deploy.yaml /tmp/m/ &&
              flux push artifact "oci://$REGISTRY_SVC/flux-check:$CHECK"
              --path=/tmp/m --source=e2e
              --revision=check@sha1:0000000000000000000000000000000000000000
              --insecure-registry
          volumeMounts: [{name: m, mountPath: /manifests}, {name: tmp, mountPath: /tmp}]
      volumes: [{name: m, configMap: {name: $CHECK}}, {name: tmp, emptyDir: {}}]
EOF
	k -n "$NS" wait --for=condition=Complete --timeout=120s "job/$CHECK-push"

	k apply -f - <<EOF
apiVersion: source.toolkit.fluxcd.io/v1
kind: OCIRepository
metadata:
  name: $CHECK
  namespace: $NS
spec:
  interval: 1m
  url: oci://$REGISTRY_SVC/flux-check
  ref: {tag: $CHECK}
  insecure: true
---
apiVersion: kustomize.toolkit.fluxcd.io/v1
kind: Kustomization
metadata:
  name: $CHECK
  namespace: $NS
spec:
  interval: 1h
  prune: true
  wait: true
  timeout: 2m
  targetNamespace: $CHECK_NS
  sourceRef: {kind: OCIRepository, name: $CHECK}
EOF
	k -n "$NS" wait --for=condition=Ready --timeout=180s "kustomization/$CHECK"

	local want="${CHECK_NS}_flux-check_apps_Deployment"
	k -n "$NS" get kustomization "$CHECK" -o json |
		jq -e --arg id "$want" '.status.inventory.entries | map(.id) | index($id) != null' >/dev/null ||
		{ echo "inventory lacks $want" >&2; k -n "$NS" get kustomization "$CHECK" -o jsonpath='{.status.inventory}' >&2; exit 1; }
	echo "inventory lists $want"
	k -n "$CHECK_NS" rollout status --timeout=60s deploy/flux-check

	# Unsuspended, a manual scale-down is reverted on the next reconcile.
	k -n "$CHECK_NS" scale deploy/flux-check --replicas=0
	reconcile
	wait_for 60 "an unsuspended Kustomization to revert the scale-down" replicas_are 2
	# The status records the request only after the health checks, so wait
	# for it before suspending.
	wait_for 120 "the Kustomization to finish the requested reconcile" handled_token
	echo "unsuspended: scale-down reverted"
	# Suspended, a reconcile request leaves the workload alone. kustomize-controller
	# v1.9.5 still copies the request into status.lastHandledReconcileAt while
	# suspended, so that field is no sign of a reconcile.
	k -n "$NS" patch kustomization "$CHECK" --type=merge -p '{"spec":{"suspend":true}}'
	k -n "$CHECK_NS" scale deploy/flux-check --replicas=0
	reconcile
	sleep 15
	replicas_are 0 || { echo "a suspended Kustomization reverted the scale-down" >&2; exit 1; }
	echo "suspended: scale-down kept"

	# Resumed, the replicas come back.
	k -n "$NS" patch kustomization "$CHECK" --type=merge -p '{"spec":{"suspend":false}}'
	wait_for 60 "a resumed Kustomization to restore the replicas" replicas_are 2
	echo "resumed: replicas restored"
	echo "flux check passed"
}

uninstall() {
	set +e
	# Delete what the controllers own while they still run, so finalizers clear.
	for kind in kustomizations.kustomize.toolkit.fluxcd.io ocirepositories.source.toolkit.fluxcd.io \
		gitrepositories.source.toolkit.fluxcd.io buckets.source.toolkit.fluxcd.io \
		helmrepositories.source.toolkit.fluxcd.io helmcharts.source.toolkit.fluxcd.io; do
		k delete "$kind" --all -A --timeout=60s 2>/dev/null
	done
	registry | k delete --ignore-not-found -f -
	manifest | k delete --ignore-not-found --timeout=120s -f -
}

case "${1:-}" in
fetch | install | check | uninstall) "$1" ;;
*)
	echo "usage: $0 fetch|install|check|uninstall" >&2
	exit 2
	;;
esac
