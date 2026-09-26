#!/usr/bin/env bash
# Records the barman object stores under internal/testinfra/barmanstore/recorded
# with real barman-cloud, a real PostgreSQL and RustFS, the S3 server walzen
# prod stores its backups on. Run it through make fixtures-barman, which enters
# the flake's fixtures shell; it needs nothing that shell doesn't provide.
#
# Each store is written the way a CloudNativePG Cluster with the Barman Cloud
# plugin writes its archive: PostgreSQL archives every WAL segment with
# barman-cloud-wal-archive, and a base backup is barman-cloud-backup with the
# plugin's arguments (--user postgres --name <backup>), plus
# --immediate-checkpoint so the recorder doesn't wait for a spread checkpoint.
# Every store lies under s3://recorded/<store>/ with the server name app-pg:
#
#   empty           nothing at all
#   wal-only        WAL of a Cluster that never took a base backup
#   failed-base     one FAILED base backup, and WAL
#   started-base    one base backup left at STARTED (barman-cloud-backup killed
#                   with SIGKILL right after it wrote backup.info), and WAL
#   done-base       one DONE base backup, and WAL
#   many-failed     160 FAILED base backups and then one DONE, and WAL
#   two-servers     app-pg with WAL, and other-pg with a DONE base backup and
#                   no WAL, under one prefix
#   after-recovery  a DONE base backup, then a Cluster recovered from it that
#                   archives timeline 2 to the same server, with its .history
#
# A FAILED backup is made by refusing barman's data uploads with AccessDenied
# through the s3fault proxy, while backup.info goes through: barman then
# records the failure in backup.info, as it does when the store refuses it
# partway through a real backup.
#
# For each store it writes manifest.json (every object's key, size, time and
# ETag, and the full body of backup.info and .history files), verdicts.json
# (barman-cloud-backup-list --format json and barman-cloud-check-wal-archive
# per server), and transcript-<server>.json (the requests
# bootstrap.S3Prober makes against RustFS, with RustFS's answers). It also
# writes rustfs-behaviour.json, RustFS's answers to paging, delimiter, range
# and error requests, and provenance.json with the tool versions.
#
# With --transcripts-only it records the transcripts again and nothing else:
# it writes each store from its recorded manifest.json into a fresh RustFS
# (objects recorded without a body as zeros of their size), records the
# current bootstrap.S3Prober against it, and puts back the manifest's ETags
# and times in RustFS's answers, since RustFS sets its own on upload. Run it
# through make fixtures-transcripts after a change to the prober's requests.
set -euo pipefail

transcripts_only=false
case ${1:-} in
--transcripts-only) transcripts_only=true ;;
"") ;;
*) echo "usage: $0 [--transcripts-only]" >&2; exit 2 ;;
esac

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
out="$root/internal/testinfra/barmanstore/recorded"
versions="$root/versions.json"

want() { jq -r ".components[\"$1\"].version" "$versions"; }
barman_version=$(want barman)
rustfs_version=$(want rustfs)
postgres_version=$(want postgresql)
barman-cloud-backup --version | grep -qx "barman-cloud-backup $barman_version" ||
	{ echo "barman-cloud-backup is not $barman_version" >&2; exit 1; }
rustfs --version | head -1 | grep -qx "rustfs $rustfs_version" || { echo "rustfs is not $rustfs_version" >&2; exit 1; }
postgres --version | grep -q " $postgres_version\$" || { echo "postgres is not $postgres_version" >&2; exit 1; }

work=$(mktemp -d)
pids=()
datas=()
cleanup() {
	for d in "${datas[@]}"; do pg_ctl -D "$d" -m immediate stop >/dev/null 2>&1 || true; done
	for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done
	wait 2>/dev/null || true
	rm -rf "$work"
}
trap cleanup EXIT

# s3tool imports the package the recordings go into, which embeds them, so it
# is built before the old recordings are cleared.
mkdir -p "$out"
[[ -e $out/provenance.json ]] || echo '{}' >"$out/provenance.json"
(cd "$root" && go build -o "$work/s3tool" ./hack/fixtures/s3tool)
s3tool="$work/s3tool"

export AWS_ACCESS_KEY_ID=recorder-access-key
export AWS_SECRET_ACCESS_KEY=recorder-secret-key
export TZ=UTC
bucket=recorded

# free_port prints a TCP port nothing listens on.
free_port() {
	local port
	while :; do
		port=$((20000 + RANDOM % 30000))
		(exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null || { echo "$port"; return; }
	done
}

rustfs_port=$(free_port)
mkdir -p "$work/rustfs"
RUSTFS_ACCESS_KEY=$AWS_ACCESS_KEY_ID RUSTFS_SECRET_KEY=$AWS_SECRET_ACCESS_KEY \
	rustfs server --address "127.0.0.1:$rustfs_port" "$work/rustfs" >"$work/rustfs.log" 2>&1 &
pids+=($!)
endpoint="http://127.0.0.1:$rustfs_port"
for _ in $(seq 1 100); do
	curl -s -o /dev/null "$endpoint/" && break
	sleep 0.2
done
"$s3tool" mb "127.0.0.1:$rustfs_port" "$bucket"

# cloud runs a barman-cloud command against the store the way the plugin
# does: the aws-s3 provider and an endpoint URL.
cloud() {
	local cmd=$1 url=$2
	shift 2
	"barman-cloud-$cmd" --cloud-provider aws-s3 --endpoint-url "$url" "$@"
}

# pg_new initialises and starts a PostgreSQL whose archive_command sends WAL
# to one server of one store. With "none" as the store it doesn't archive.
# It prints the directory whose socket and data it uses.
pg_new() {
	local store=$1 server=$2 dir
	dir=$(mktemp -d "$work/pg.XXXX")
	initdb -D "$dir/data" -U postgres -A trust --no-sync >/dev/null
	{
		echo "listen_addresses = ''"
		echo "unix_socket_directories = '$dir'"
		echo "fsync = off"
		echo "wal_level = replica"
		if [[ $store == none ]]; then
			echo "archive_mode = off"
		else
			echo "archive_mode = on"
			echo "archive_command = 'barman-cloud-wal-archive --cloud-provider aws-s3 --endpoint-url $endpoint s3://$bucket/$store/ $server %p'"
		fi
	} >>"$dir/data/postgresql.conf"
	pg_ctl -D "$dir/data" -l "$dir/log" -w start >/dev/null
	datas+=("$dir/data")
	echo "$dir"
}

sql() {
	local dir=$1
	shift
	psql -h "$dir" -U postgres -d postgres -v ON_ERROR_STOP=1 -qAt -c "$*"
}

# wal writes some rows and switches to a new WAL segment, then waits until
# the archiver has sent the finished one.
wal() {
	local dir=$1 before
	before=$(sql "$dir" "select archived_count from pg_stat_archiver")
	sql "$dir" "set client_min_messages = warning; create table if not exists t (i int); insert into t select generate_series(1, 1000); select pg_switch_wal();" >/dev/null
	for _ in $(seq 1 300); do
		[[ $(sql "$dir" "select archived_count from pg_stat_archiver") -gt $before ]] && return
		sleep 0.1
	done
	echo "WAL archiving stalled in $dir" >&2
	tail "$dir/log" >&2
	exit 1
}

# backup runs barman-cloud-backup with the plugin's arguments against the
# given S3 URL (RustFS, or a proxy in front of it). It returns barman's exit
# code.
backup() {
	local dir=$1 url=$2 store=$3 server=$4 name=$5
	# barman names a backup by the second it starts, so two backups never
	# start in one second.
	local now
	now=$(date +%s)
	while [[ $(date +%s) == "$now" ]]; do sleep 0.05; done
	cloud backup "$url" --user postgres --host "$dir" --name "$name" --immediate-checkpoint \
		"s3://$bucket/$store/" "$server" >>"$work/backup.log" 2>&1
}

# refusing starts an s3fault proxy that refuses the data uploads of one
# server's base backups, and prints its URL.
refusing() {
	local store=$1 server=$2 port
	port=$(free_port)
	"$s3tool" proxy "127.0.0.1:$port" "$endpoint" "$bucket" "$store/$server/base/" >"$work/proxy-$store.log" 2>&1 &
	pids+=($!)
	for _ in $(seq 1 100); do
		curl -s -o /dev/null "http://127.0.0.1:$port/" && break
		sleep 0.1
	done
	echo "http://127.0.0.1:$port"
}

stop() { pg_ctl -D "$1/data" -m fast -w stop >/dev/null; }

record_wal_only() {
	local pg
	pg=$(pg_new wal-only app-pg)
	wal "$pg"; wal "$pg"; wal "$pg"
	stop "$pg"
}

record_failed_base() {
	local pg proxy
	pg=$(pg_new failed-base app-pg)
	wal "$pg"; wal "$pg"
	proxy=$(refusing failed-base app-pg)
	if backup "$pg" "$proxy" failed-base app-pg app-pg-backup-failed; then
		echo "the refused backup succeeded" >&2; exit 1
	fi
	wal "$pg"
	stop "$pg"
}

record_started_base() {
	local pg
	pg=$(pg_new started-base app-pg)
	wal "$pg"
	# Enough data that the upload is still running when barman is killed.
	pgbench -h "$pg" -U postgres -i -s 30 -q postgres >/dev/null 2>&1
	wal "$pg"
	setsid bash -c "exec barman-cloud-backup --cloud-provider aws-s3 --endpoint-url $endpoint --user postgres --host $pg \
		--name app-pg-backup-killed --immediate-checkpoint s3://$bucket/started-base/ app-pg" >>"$work/backup.log" 2>&1 &
	local barman=$!
	"$s3tool" wait-key "127.0.0.1:$rustfs_port" "$bucket" started-base/app-pg/base/ /backup.info
	kill -KILL -- "-$barman"
	wait "$barman" 2>/dev/null || true
	wal "$pg"
	stop "$pg"
}

record_done_base() {
	local pg
	pg=$(pg_new done-base app-pg)
	wal "$pg"; wal "$pg"
	backup "$pg" "$endpoint" done-base app-pg app-pg-backup-1
	wal "$pg"; wal "$pg"
	stop "$pg"
}

record_many_failed() {
	local pg proxy i
	pg=$(pg_new many-failed app-pg)
	wal "$pg"
	proxy=$(refusing many-failed app-pg)
	for i in $(seq 1 160); do
		if backup "$pg" "$proxy" many-failed app-pg "app-pg-backup-failed-$i"; then
			echo "refused backup $i succeeded" >&2; exit 1
		fi
	done
	backup "$pg" "$endpoint" many-failed app-pg app-pg-backup-done
	wal "$pg"
	stop "$pg"
}

record_two_servers() {
	local a b
	a=$(pg_new two-servers app-pg)
	wal "$a"; wal "$a"
	stop "$a"
	b=$(pg_new none other-pg)
	backup "$b" "$endpoint" two-servers other-pg other-pg-backup-1
	stop "$b"
}

record_after_recovery() {
	local pg id
	pg=$(pg_new after-recovery app-pg)
	wal "$pg"
	backup "$pg" "$endpoint" after-recovery app-pg app-pg-backup-1
	wal "$pg"; wal "$pg"
	stop "$pg"
	id=$(cloud backup-list "$endpoint" --format json "s3://$bucket/after-recovery/" app-pg | jq -r '.backups_list[0].backup_id')

	# The recovered Cluster: barman-cloud-restore of the base backup, WAL
	# replayed through barman-cloud-wal-restore to the end of the backup,
	# then promoted onto timeline 2 and archiving to the same server.
	local dir
	dir=$(mktemp -d "$work/pg.XXXX")
	cloud restore "$endpoint" "s3://$bucket/after-recovery/" app-pg "$id" "$dir/data" >>"$work/backup.log" 2>&1
	chmod 700 "$dir/data"
	{
		echo "unix_socket_directories = '$dir'"
		echo "restore_command = 'barman-cloud-wal-restore --cloud-provider aws-s3 --endpoint-url $endpoint s3://$bucket/after-recovery/ app-pg %f %p'"
		echo "recovery_target = 'immediate'"
		echo "recovery_target_action = 'promote'"
	} >>"$dir/data/postgresql.conf"
	touch "$dir/data/recovery.signal"
	pg_ctl -D "$dir/data" -l "$dir/log" -w start >/dev/null
	datas+=("$dir/data")
	for _ in $(seq 1 300); do
		[[ $(sql "$dir" "select pg_is_in_recovery()") == f ]] && break
		sleep 0.1
	done
	[[ $(sql "$dir" "select pg_walfile_name(pg_current_wal_lsn())") == 00000002* ]] ||
		{ echo "the recovered cluster is not on timeline 2" >&2; exit 1; }
	wal "$dir"; wal "$dir"
	"$s3tool" wait-key "127.0.0.1:$rustfs_port" "$bucket" after-recovery/app-pg/wals/ .history
	stop "$dir"
}

# transcripts writes provenance.json's transcripts entry: when the
# transcripts were recorded, the last commit that changed internal/bootstrap
# (the prober they record), and how.
transcripts() {
	local how=$1
	jq --arg recorded "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
		--arg commit "$(git -C "$root" log -1 --format=%h -- internal/bootstrap)" --arg how "$how" \
		'. + {transcripts: {recorded: $recorded, proberCommit: $commit, method: $how}}' \
		"$out/provenance.json" >"$work/provenance.json"
	mv "$work/provenance.json" "$out/provenance.json"
}

if $transcripts_only; then
	"$s3tool" transcripts "127.0.0.1:$rustfs_port" "$bucket" "$out"
	transcripts "--transcripts-only: each recorded manifest written into RustFS, bodiless objects as zeros, the manifest's ETags and times put back in RustFS's answers"
	echo "wrote the transcripts under $out"
	exit 0
fi

# record writes one store's files: manifest, verdicts and transcripts.
record() {
	local store=$1
	shift
	local servers=("$@") target="$out/$store"
	mkdir -p "$target"
	"$s3tool" manifest "127.0.0.1:$rustfs_port" "$bucket" "$store" "$target/manifest.json"
	echo '{}' >"$work/verdicts.json"
	local server
	for server in "${servers[@]}"; do
		local code
		cloud backup-list "$endpoint" --format json "s3://$bucket/$store/" "$server" >"$work/list.json"
		set +e
		cloud check-wal-archive "$endpoint" "s3://$bucket/$store/" "$server" >"$work/check.txt" 2>&1
		code=$?
		set -e
		jq --arg server "$server" --slurpfile list "$work/list.json" --argjson code "$code" --rawfile output "$work/check.txt" \
			'. + {($server): {backups: $list[0].backups_list, checkWalArchive: {exitCode: $code, output: $output}}}' \
			"$work/verdicts.json" >"$work/verdicts.next.json"
		mv "$work/verdicts.next.json" "$work/verdicts.json"
		"$s3tool" transcript "127.0.0.1:$rustfs_port" "$bucket" "$store" "$server" "$target/transcript-$server.json"
	done
	jq . "$work/verdicts.json" >"$target/verdicts.json"
}

find "$out" -mindepth 1 -maxdepth 1 ! -name provenance.json -exec rm -rf {} +

echo "store empty"; record empty app-pg
echo "store wal-only"; record_wal_only; record wal-only app-pg
echo "store failed-base"; record_failed_base; record failed-base app-pg
echo "store started-base"; record_started_base; record started-base app-pg
echo "store done-base"; record_done_base; record done-base app-pg
echo "store two-servers"; record_two_servers; record two-servers app-pg other-pg
echo "store after-recovery"; record_after_recovery; record after-recovery app-pg
echo "store many-failed"; record_many_failed; record many-failed app-pg
echo "rustfs behaviour"; "$s3tool" behaviour "127.0.0.1:$rustfs_port" "$bucket" "$out/rustfs-behaviour.json"

jq -n --arg generator hack/fixtures/barman-stores.sh --arg recorded "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--arg barman "$barman_version" --arg rustfs "$rustfs_version" --arg postgresql "$postgres_version" \
	'{generator: $generator, recorded: $recorded,
	  tools: {barman: $barman, rustfs: $rustfs, postgresql: $postgresql},
	  bucket: "recorded", server: "app-pg",
	  backupArguments: "barman-cloud-backup --cloud-provider aws-s3 --endpoint-url <rustfs> --user postgres --name <backup> --immediate-checkpoint s3://recorded/<store>/ <server>"}' \
	>"$out/provenance.json"
transcripts "recorded with the stores, from barman's own writes"
echo "wrote $out"
