#!/usr/bin/env bash
# rustfs.sh runs RustFS 1.0.0, the S3 server walzen prod stores its backups
# on, in the e2e cluster.
#
# It installs a single-node RustFS Deployment in namespace e2e-s3 with its
# data on an emptyDir capped at 1Gi, a Service rustfs on port 9000 (so the
# in-cluster endpoint is http://rustfs.e2e-s3.svc:9000, plain HTTP like
# prod's), fixed test credentials in the Secret rustfs-credentials, and the
# buckets volsync and postgres.
#
# Usage: hack/e2e/rustfs/rustfs.sh fetch|install|check|uninstall
#
#   fetch      pulls the RustFS and aws-cli images by tag and digest into the
#              local Docker engine and builds the host's aws CLI from the
#              flake's nixpkgs. The only step that downloads.
#   install    applies the namespace, Secret, Deployment and Service, waits
#              for RustFS, and creates the buckets from a pod in the cluster.
#   check      puts, lists and gets an object in each bucket from the host
#              (through a kubectl port-forward) and from a pod in the
#              cluster, then removes the objects and checks the buckets are
#              still there. Exits non-zero on any failure.
#   uninstall  deletes the namespace, which holds everything install made.
#
# Every kubectl call passes --context docker-desktop. Run it from the flake's
# shell (nix develop -c hack/e2e/rustfs/rustfs.sh ...) for kubectl and jq.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
repo=$(cd "$here/../../.." && pwd)
pins="$here/pins.json"

context=docker-desktop
kc() { kubectl --context "$context" "$@"; }
pin() { jq -r ".rustfs.$1" "$pins"; }

ns=$(pin namespace)
secret=$(pin credentialsSecret)
endpoint=$(pin endpoint)
mapfile -t buckets < <(jq -r '.rustfs.buckets[]' "$pins")
rustfs_image="$(pin image.repository):$(pin image.tag)@$(pin image.digest)"
client_image="$(pin s3client.repository):$(pin s3client.tag)@$(pin s3client.digest)"
host_client=$(pin hostClient.nixpkgs)

# Fixed test credentials. They only ever guard this throwaway e2e server.
access_key=e2e-rustfs
secret_key=e2e-rustfs-secret-key

log() { printf '[rustfs] %s\n' "$*" >&2; }

fetch() {
  local img
  for img in "$rustfs_image" "$client_image"; do
    log "pulling $img"
    docker pull "$img" >/dev/null
    # The engine files a pull by tag and digest under the digest only.
    docker image inspect "${img%:*@*}@${img#*@}" >/dev/null
  done
  log "building the host S3 client (nixpkgs#$host_client)"
  nix build --no-link --inputs-from "$repo" "nixpkgs#$host_client"
}

manifests() {
  cat <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $ns
---
apiVersion: v1
kind: Secret
metadata:
  name: $secret
  namespace: $ns
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: $access_key
  AWS_SECRET_ACCESS_KEY: $secret_key
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: rustfs
  namespace: $ns
  labels: {app: rustfs}
spec:
  replicas: 1
  strategy: {type: Recreate}
  selector:
    matchLabels: {app: rustfs}
  template:
    metadata:
      labels: {app: rustfs}
    spec:
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
      containers:
        - name: rustfs
          image: $rustfs_image
          imagePullPolicy: IfNotPresent
          env:
            - name: RUSTFS_VOLUMES
              value: /data
            - name: RUSTFS_ADDRESS
              value: 0.0.0.0:9000
            - name: RUSTFS_CONSOLE_ENABLE
              value: "false"
            - name: RUSTFS_ACCESS_KEY
              valueFrom: {secretKeyRef: {name: $secret, key: AWS_ACCESS_KEY_ID}}
            - name: RUSTFS_SECRET_KEY
              valueFrom: {secretKeyRef: {name: $secret, key: AWS_SECRET_ACCESS_KEY}}
          ports:
            - {name: s3, containerPort: 9000}
          readinessProbe:
            tcpSocket: {port: s3}
            periodSeconds: 2
          resources:
            requests: {cpu: 50m, memory: 128Mi}
            limits: {memory: 1Gi}
          volumeMounts:
            - {name: data, mountPath: /data}
            - {name: logs, mountPath: /logs}
      volumes:
        - name: data
          emptyDir: {sizeLimit: 1Gi}
        - name: logs
          emptyDir: {sizeLimit: 64Mi}
---
apiVersion: v1
kind: Service
metadata:
  name: rustfs
  namespace: $ns
spec:
  selector: {app: rustfs}
  ports:
    - {name: s3, port: 9000, targetPort: s3}
EOF
}

# in_cluster runs a shell script in a pod of the pinned aws-cli image, with
# the test credentials and endpoint in its environment, prints its log, and
# fails when the script fails.
#
# Parameters:
#   $1  the pod's name
#   $2  the script, run by /bin/sh
in_cluster() {
  local name=$1 script=$2 phase
  kc -n "$ns" delete pod "$name" --ignore-not-found --wait >/dev/null
  kc apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Pod
metadata:
  name: $name
  namespace: $ns
spec:
  restartPolicy: Never
  containers:
    - name: aws
      image: $client_image
      imagePullPolicy: IfNotPresent
      command: [/bin/sh, -ec]
      args:
        - |
          printf '[default]\ns3 =\n  addressing_style = path\n' > /tmp/aws-config
          $(printf '%s\n' "$script" | sed '2,$s/^/          /')
      envFrom:
        - secretRef: {name: $secret}
      env:
        - {name: AWS_CONFIG_FILE, value: /tmp/aws-config}
        - {name: AWS_ENDPOINT_URL, value: "$endpoint"}
        - {name: AWS_DEFAULT_REGION, value: us-east-1}
EOF
  for _ in $(seq 1 90); do
    phase=$(kc -n "$ns" get pod "$name" -o jsonpath='{.status.phase}')
    [[ $phase == Succeeded || $phase == Failed ]] && break
    sleep 2
  done
  kc -n "$ns" logs "$name" || true
  kc -n "$ns" delete pod "$name" --wait=false >/dev/null
  if [[ $phase != Succeeded ]]; then
    log "pod $name ended in phase '$phase'"
    return 1
  fi
}

install() {
  log "applying RustFS ($rustfs_image) to namespace $ns"
  manifests | kc apply -f - >/dev/null
  kc -n "$ns" rollout status deployment/rustfs --timeout=180s
  log "creating buckets: ${buckets[*]}"
  local script="" b
  for b in "${buckets[@]}"; do
    script+="aws s3api head-bucket --bucket $b 2>/dev/null || aws s3api create-bucket --bucket $b
"
  done
  script+="aws s3api list-buckets --query 'Buckets[].Name' --output text"
  in_cluster rustfs-make-buckets "$script"
}

# roundtrip_script prints the check's steps for one side as a shell script,
# so the same steps run on the host and in a pod: for each bucket it puts an
# object, lists it, gets it back and compares the bytes, deletes it, checks
# no test object is left, and checks the bucket is still there.
#
# Parameters:
#   $1  the side (host or cluster), which the object key starts with
roundtrip_script() {
  local side=$1 b key
  echo "set -e"
  echo "body='rustfs e2e check from the $side'"
  # shellcheck disable=SC2016 # $body expands in the printed script
  echo 'printf "%s" "$body" > /tmp/rustfs-check-put'
  for b in "${buckets[@]}"; do
    key="e2e-check/$side-object.txt"
    cat <<EOF
aws s3api put-object --bucket $b --key $key --body /tmp/rustfs-check-put >/dev/null
aws s3api list-objects-v2 --bucket $b --prefix e2e-check/$side- --query 'Contents[].Key' --output text | grep -qx '$key'
aws s3api get-object --bucket $b --key $key /tmp/rustfs-check-get >/dev/null
[ "\$(cat /tmp/rustfs-check-get)" = "\$body" ]
aws s3api delete-object --bucket $b --key $key >/dev/null
[ -z "\$(aws s3api list-objects-v2 --bucket $b --prefix e2e-check/ --query 'Contents[].Key' --output text | grep -v '^None\$' || true)" ]
aws s3api head-bucket --bucket $b
echo "$side: put, list, get and delete in $b ok"
EOF
  done
}

check() {
  local port pf_pid tmp
  tmp=$(mktemp -d)
  port=$(( 20000 + RANDOM % 20000 ))
  kc -n "$ns" port-forward svc/rustfs "$port:9000" >"$tmp/port-forward.log" 2>&1 &
  pf_pid=$!
  # shellcheck disable=SC2064
  trap "kill $pf_pid 2>/dev/null || true; rm -rf '$tmp'" EXIT
  for _ in $(seq 1 30); do
    curl -s -o /dev/null "http://127.0.0.1:$port/" && break
    sleep 1
  done

  log "host side through the port-forward on 127.0.0.1:$port"
  printf '[default]\ns3 =\n  addressing_style = path\n' >"$tmp/aws-config"
  (
    export AWS_CONFIG_FILE="$tmp/aws-config" AWS_SHARED_CREDENTIALS_FILE=/dev/null
    export AWS_ENDPOINT_URL="http://127.0.0.1:$port" AWS_DEFAULT_REGION=us-east-1
    export AWS_ACCESS_KEY_ID=$access_key AWS_SECRET_ACCESS_KEY=$secret_key
    cd "$tmp"
    nix shell --inputs-from "$repo" "nixpkgs#$host_client" -c bash -c "$(roundtrip_script host | sed "s#/tmp/#$tmp/#g")"
  )

  log "cluster side from a pod against $endpoint"
  in_cluster rustfs-check "$(roundtrip_script cluster)"
  log "check passed"
}

uninstall() {
  log "deleting namespace $ns"
  kc delete namespace "$ns" --ignore-not-found --wait=true --timeout=180s
}

case "${1:-}" in
  fetch | install | check | uninstall) "$1" ;;
  *)
    echo "usage: $0 fetch|install|check|uninstall" >&2
    exit 2
    ;;
esac
