#!/usr/bin/env bash
# empty-archive-repair.sh reproduces a CloudNativePG Cluster that started as
# initdb over a prefix already holding WAL, the state backup-controller v0.8.x
# admitted, and runs one of the two repairs docs/upgrading.md (v0.9.0, step 4)
# gives for it, on the docker-desktop e2e cluster.
#
# Usage: empty-archive-repair.sh server-name|empty-prefix
#
#   server-name   repairs by setting a new serverName in the Cluster's
#                 barman-cloud plugin parameters, so the old archive stays.
#   empty-prefix  repairs by deleting everything under the old prefix.
#
# Each run works in a namespace of its own (e2e-empty-archive-<random>) and
# under s3://postgres/<that namespace>/ on the e2e RustFS, and deletes both
# when it ends, whether it passes or fails. Steps:
#
#  1. A donor Cluster archives one real WAL segment to <prefix>/donor/ and is
#     deleted.
#  2. The Cluster app is created over the empty <prefix>/app/, so the
#     backup-controller webhook admits it. Right after the create, the donor's
#     segment is copied to <prefix>/app/wals/. The instance starts archiving
#     10 to 15 seconds later (initdb job, then the instance pod), so its first
#     barman-cloud-check-wal-archive already sees WAL and fails. The run checks
#     that it did: ContinuousArchiving False with reason
#     ContinuousArchivingFailing, "Expected empty archive" in the
#     plugin-barman-cloud container's log, the marker file still in PGDATA.
#  3. It writes 2000 rows in two transactions with a WAL switch after each,
#     so pg_wal holds three segments waiting to be archived.
#  4. The repair, then a wait for ContinuousArchiving True. The run checks the
#     marker is gone and segments 1 to 3 are in the new archive.
#  5. A BackupRun with database: app takes a base backup.
#  6. 100 more rows and a WAL switch; a Cluster verify recovers from the
#     archive and must hold all 2100 rows.
#
# Needs CloudNativePG and plugin-barman-cloud (../cnpg), RustFS (../rustfs) and
# backup-controller (../backup-controller) installed. Uses 512Mi claims on the
# StorageClass standard. One run takes about 3 minutes. Every kubectl call
# passes --context docker-desktop. Run it from the flake's shell for kubectl
# and jq.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
context=docker-desktop
pg_image=$(jq -r '.cnpg.postgres | "\(.repository):\(.tag)@\(.digest)"' "$here/pins.json")
s3_image=$(jq -r '.rustfs.s3client | "\(.repository):\(.tag)@\(.digest)"' "$here/../rustfs/pins.json")
endpoint=$(jq -r '.rustfs.endpoint' "$here/../rustfs/pins.json")
s3_ns=$(jq -r '.rustfs.namespace' "$here/../rustfs/pins.json")
s3_secret=$(jq -r '.rustfs.credentialsSecret' "$here/../rustfs/pins.json")
bucket=postgres
ns="e2e-empty-archive-$RANDOM"
prefix="s3://$bucket/$ns"
pgdata=/var/lib/postgresql/data/pgdata

k() { kubectl --context "$context" -n "$ns" "$@"; }
log() { printf '[empty-archive] %s\n' "$*" >&2; }
fail() { log "FAIL: $*"; exit 1; }
aws() { k exec s3 -- aws "$@"; }
psql() { k exec "$1-1" -c postgres -- psql -v ON_ERROR_STOP=1 -tA "${@:2}"; }

# until_ok polls a command every 3 seconds until it succeeds.
#
# Parameters:
#   - seconds is how long to try before failing the run.
#   - what names the wait, for the messages.
#   - the rest is the command.
until_ok() {
  local seconds=$1 what=$2 start=$SECONDS
  shift 2
  until "$@" >/dev/null 2>&1; do
    ((SECONDS - start < seconds)) || fail "no $what after ${seconds}s"
    sleep 3
  done
  log "$what after $((SECONDS - start))s"
}

# archiving prints the Cluster's ContinuousArchiving condition as
# "status reason: message".
archiving() {
  k get cluster "$1" -o json |
    jq -r '.status.conditions[]? | select(.type == "ContinuousArchiving") | "\(.status) \(.reason): \(.message)"'
}
archiving_is() { [[ "$(archiving "$1")" == "$2 "* ]]; }
has_object() { aws s3 ls "$1"; }

# cluster applies a one-instance Cluster that archives through the ObjectStore
# store.
#
# Parameters:
#   - name is the Cluster's name.
#   - the rest are YAML lines appended to spec (a bootstrap, externalClusters).
cluster() {
  local name=$1
  shift
  k apply -f - <<YAML
apiVersion: postgresql.cnpg.io/v1
kind: Cluster
metadata:
  name: $name
  annotations:
    backup.wlz.li/enabled: "true"
spec:
  instances: 1
  imageName: $pg_image
  storage: {storageClass: standard, size: 512Mi}
  postgresql:
    parameters: {min_wal_size: 32MB, max_wal_size: 64MB, shared_buffers: 32MB}
$(printf '%s\n' "$@")
YAML
}

archiver=(
  "  plugins:"
  "  - name: barman-cloud.cloudnative-pg.io"
  "    isWALArchiver: true"
  "    parameters: {barmanObjectName: store}"
)

cleanup() {
  log "cleaning up namespace $ns and $prefix/"
  k delete backupruns.backup.wlz.li --all --ignore-not-found >/dev/null 2>&1 || true
  k delete clusters.postgresql.cnpg.io --all --wait --timeout=3m >/dev/null 2>&1 || true
  aws s3 rm --recursive --quiet "$prefix/" >/dev/null 2>&1 || true
  kubectl --context "$context" delete namespace "$ns" --wait --timeout=3m >/dev/null 2>&1 || true
}

setup() {
  kubectl --context "$context" create namespace "$ns" >/dev/null
  trap cleanup EXIT
  kubectl --context "$context" -n "$s3_ns" get secret "$s3_secret" -o json |
    jq '{apiVersion, kind, type, data, metadata: {name: "s3"}}' | k apply -f - >/dev/null
  k apply -f - >/dev/null <<YAML
apiVersion: barmancloud.cnpg.io/v1
kind: ObjectStore
metadata:
  name: store
spec:
  configuration:
    destinationPath: $prefix/
    endpointURL: $endpoint
    s3Credentials:
      accessKeyId: {name: s3, key: AWS_ACCESS_KEY_ID}
      secretAccessKey: {name: s3, key: AWS_SECRET_ACCESS_KEY}
---
apiVersion: v1
kind: Pod
metadata:
  name: s3
spec:
  containers:
  - name: aws
    image: $s3_image
    imagePullPolicy: IfNotPresent
    command: [sleep, infinity]
    env:
    - {name: AWS_ACCESS_KEY_ID, valueFrom: {secretKeyRef: {name: s3, key: AWS_ACCESS_KEY_ID}}}
    - {name: AWS_SECRET_ACCESS_KEY, valueFrom: {secretKeyRef: {name: s3, key: AWS_SECRET_ACCESS_KEY}}}
    - {name: AWS_ENDPOINT_URL, value: $endpoint}
    - {name: AWS_DEFAULT_REGION, value: us-east-1}
YAML
  k wait pod/s3 --for=condition=Ready --timeout=2m >/dev/null
  log "namespace $ns, archive $prefix/"
}

reproduce() {
  log "donor: archiving one WAL segment to $prefix/donor/"
  cluster donor "${archiver[@]}" >/dev/null
  until_ok 300 "donor archiving" archiving_is donor True
  local seg=0000000100000000/000000010000000000000001
  until_ok 60 "donor segment 1 archived" has_object "$prefix/donor/wals/$seg"
  k delete cluster donor --wait --timeout=2m >/dev/null

  log "app: created over the empty $prefix/app/, then the donor's WAL copied in"
  cluster app "${archiver[@]}" >/dev/null
  aws s3 cp --quiet "$prefix/donor/wals/$seg" "$prefix/app/wals/$seg"
  until_ok 300 "app archiving False" archiving_is app False
  k wait cluster/app --for=condition=Ready --timeout=3m >/dev/null
  log "ContinuousArchiving: $(archiving app)"
  [[ "$(archiving app)" == "False ContinuousArchivingFailing: "* ]] || fail "unexpected condition"
  if [[ "$(archiving app)" == *'Expected empty archive'* ]]; then
    log "the condition's message names Expected empty archive"
  fi
  local sidecar line
  sidecar=$(k logs app-1 -c plugin-barman-cloud)
  line=$(grep -m1 'Expected empty archive' <<<"$sidecar" | jq -r .msg) ||
    fail "no Expected empty archive in the plugin-barman-cloud log"
  log "plugin-barman-cloud log: $line"
  if grep -q 'Expected empty archive' <<<"$(k logs app-1 -c postgres)"; then
    log "the postgres container's log names Expected empty archive too"
  else
    log "the postgres container's log does not name Expected empty archive"
  fi
  k exec app-1 -c postgres -- test -e "$pgdata/.check-empty-wal-archive" || fail "no marker file"

  psql app -c "create table t (id int primary key, note text)" \
    -c "insert into t select g, 'before repair' from generate_series(1, 1000) g" \
    -c "select pg_switch_wal()" \
    -c "insert into t select g, 'before repair' from generate_series(1001, 2000) g" \
    -c "select pg_switch_wal()" >/dev/null
  log "pg_wal/archive_status: $(k exec app-1 -c postgres -- ls "$pgdata/pg_wal/archive_status" | tr '\n' ' ')"
}

# check_repaired waits for archiving and checks the new archive at server.
check_repaired() {
  local server=$1 n
  until_ok 300 "app archiving True" archiving_is app True
  if k exec app-1 -c postgres -- test -e "$pgdata/.check-empty-wal-archive" 2>/dev/null; then
    fail "the marker file is still there"
  fi
  for n in 1 2 3; do
    has_object "$prefix/$server/wals/0000000100000000/00000001000000000000000$n" >/dev/null ||
      fail "segment $n is not under $prefix/$server/wals/"
  done
  log "segments 1 to 3 from pg_wal are in $prefix/$server/wals/"

  k apply -f - >/dev/null <<'YAML'
apiVersion: backup.wlz.li/v1alpha1
kind: BackupRun
metadata:
  name: after-repair
spec:
  database: app
YAML
  until_ok 300 "BackupRun finished" \
    bash -c "kubectl --context $context -n $ns get brun after-repair -o jsonpath='{.status.phase}' | grep -qE 'Succeeded|Failed'"
  [[ "$(k get brun after-repair -o jsonpath='{.status.phase}')" == Succeeded ]] ||
    fail "BackupRun: $(k get brun after-repair -o jsonpath='{.status.conditions[0].message}')"
  log "BackupRun Succeeded, base backup $(aws s3 ls "$prefix/$server/base/" | awk '{print $2}')"

  local seg
  seg=$(psql app -c "insert into t select g, 'after backup' from generate_series(2001, 2100) g" \
    -c "select pg_walfile_name(pg_current_wal_lsn())" -c "select pg_switch_wal()" | sed -n 2p)
  until_ok 120 "segment $seg archived" has_object "$prefix/$server/wals/0000000100000000/$seg"

  cluster verify \
    "  bootstrap:" "    recovery: {source: origin}" \
    "  externalClusters:" "  - name: origin" "    plugin:" \
    "      name: barman-cloud.cloudnative-pg.io" \
    "      parameters: {barmanObjectName: store, serverName: $server}" >/dev/null
  k wait cluster/verify --for=condition=Ready --timeout=6m >/dev/null
  n=$(psql verify -c "select count(*) from t")
  [[ "$n" == 2100 ]] || fail "the recovered database holds $n rows, want 2100"
  log "PASS: a Cluster recovered from $prefix/$server/ holds all 2100 rows"
}

server_name() {
  setup
  reproduce
  log "repair: serverName app-v2 in the Cluster's plugin parameters"
  k patch cluster app --type=json \
    -p '[{"op": "add", "path": "/spec/plugins/0/parameters/serverName", "value": "app-v2"}]' >/dev/null
  check_repaired app-v2
}

empty_prefix() {
  setup
  reproduce
  log "repair: delete everything under $prefix/app/"
  aws s3 rm --recursive --quiet "$prefix/app/"
  check_repaired app
}

case "${1:-}" in
  server-name) server_name ;;
  empty-prefix) empty_prefix ;;
  *)
    echo "usage: $0 server-name|empty-prefix" >&2
    exit 2
    ;;
esac
