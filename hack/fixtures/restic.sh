#!/usr/bin/env bash
# Records the restic repositories under internal/restic/testdata/recorded,
# with real restic and VolSync's real mover script, and what restic itself says
# about each one. The fast tests compare the controller's restic code with
# these recordings. Run it through make fixtures-restic, which enters the flake's
# fixtures shell; it needs nothing that shell doesn't provide.
#
# Every repository is written twice: with restic 0.18.1, the version inside
# the VolSync v0.16.0 mover image, and with restic 0.19.1, the version nixpkgs
# ships (versions.json names both). Each backup runs VolSync's own
# mover-restic/entry.sh, read from the Go module cache, inside a bubblewrap
# sandbox laid out like the mover container: the data at /data, the cache at
# /cache, the script as PID 1, a pod name as the hostname, and a uid with no
# passwd entry. The repository is a local directory; restic's layout and lock
# protocol are the same as on S3.
#
# For each version it writes:
#   timed/        three snapshots a little over a second apart, at the real,
#                 fractional times restic stamps
#   same-second/  two snapshots restic stamped within one second
#   killed-mover/ the timed repository plus the lock a mover leaves when it is
#                 killed during a backup: SIGTERM to PID 1 (ignored, as bash
#                 without a trap ignores it), then SIGKILL
# and next to each repository:
#   snapshots.json  restic snapshots --json
#   selection.json  the snapshot entry.sh restore selects for each RESTORE_AS_OF
#                   around every snapshot, with SELECT_PREVIOUS 0 to 2
#   rewrite.json    (timed) the snapshot restic rewrite --new-time followed by
#                   restic tag --add writes, the change Repository.Retime makes
#   locks.json      (killed-mover) every lock file, as restic cat lock prints it
#   verdicts.json   (killed-mover) the exit codes and messages of restic forget
#                   and restic unlock on the repository with that lock
# plus provenance.json with the tool versions and this script's name.
#
# There is no separately dated stale-lock repository: libfaketime can't move
# the clock of restic, a static Go binary that reads the time without libc,
# and the recorded lock is older than restic's 30 minutes by the time any test
# reads it. The tests date restic's own lock document back to a minute ago
# where they need a live one.
#
# The killed backup uploads pack files that no index references before it
# dies. They are left out of the checked-in copy to keep it small; restic's
# verdicts were recorded on the repository as the kill left it.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
out="$root/internal/restic/testdata/recorded"
versions="$root/versions.json"
password=backup
# The pod name a VolSync source mover runs under: volsync-src-<source>-<suffix>.
mover_host=volsync-src-notes-data-7xk2p
# A mover's uid, which has no passwd entry in the mover image.
mover_uid=65532

mover_version=$(jq -r '.components["restic-mover"].version' "$versions")
latest_version=$(jq -r '.components.restic.version' "$versions")
volsync_version=$(jq -r '.components.volsync.version' "$versions")

module_version=$(cd "$root" && go list -m -f '{{.Version}}' github.com/backube/volsync)
[[ $module_version == "v$volsync_version" ]] ||
	{ echo "go.mod has VolSync $module_version, versions.json says $volsync_version" >&2; exit 1; }
volsync_dir=$(cd "$root" && go list -m -f '{{.Dir}}' github.com/backube/volsync)
entry="$volsync_dir/mover-restic/entry.sh"
test -f "$entry" || { echo "no $entry" >&2; exit 1; }

work=$(mktemp -d)
trap 'chmod -R u+w "$work" 2>/dev/null; rm -rf "$work"' EXIT

# The module cache keeps files without the execute bit, which the image's
# copy has; the sandbox runs an executable copy of the same bytes.
install -m 0555 "$entry" "$work/entry.sh"
entry="$work/entry.sh"

# binary prints the path of the restic binary of one version, and fails when
# the binary on PATH reports another version.
binary() {
	local version=$1 path
	if [[ $version == "$mover_version" ]]; then path=$(command -v "restic-$version"); else path=$(command -v restic); fi
	"$path" version | grep -q "^restic $version " || { echo "$path is not restic $version" >&2; exit 1; }
	echo "$path"
}

# sandbox_bin makes a directory holding restic of one version under the name
# restic, which is what entry.sh calls.
sandbox_bin() {
	local version=$1 dir="$work/bin-$1"
	mkdir -p "$dir"
	ln -sf "$(binary "$version")" "$dir/restic"
	echo "$dir"
}

# mover runs entry.sh with the given operations in a sandbox shaped like the
# mover container, and returns its exit code.
#
# Parameters:
#   $1 restic version, $2 hostname (the pod name), $3 repository directory,
#   $4 data directory, $5 cache directory; then NAME=value pairs for the
#   container's environment, a --, and the operations (backup, restore).
# Extra bwrap options can be passed in the array bwrap_extra.
mover() {
	local version=$1 host=$2 repo=$3 data=$4 cache=$5
	shift 5
	local -a env=()
	while [[ $1 != -- ]]; do env+=(--setenv "${1%%=*}" "${1#*=}"); shift; done
	shift
	local bin
	bin=$(sandbox_bin "$version")
	local path="/sandbox-bin:$(dirname "$(command -v date)"):$(dirname "$(command -v grep)"):$(dirname "$(command -v awk)"):$(dirname "$(command -v bash)")"
	bwrap "${bwrap_extra[@]}" \
		--unshare-user --uid "$mover_uid" --gid "$mover_uid" \
		--unshare-pid --as-pid-1 --unshare-uts --hostname "$host" \
		--ro-bind /nix /nix --proc /proc --dev /dev --tmpfs /tmp \
		--ro-bind "$(command -v bash)" /bin/bash \
		--ro-bind "$bin" /sandbox-bin \
		--ro-bind "$entry" /mover-restic/entry.sh \
		--bind "$repo" /repo --bind "$data" /data --bind "$cache" /cache \
		--clearenv --setenv PATH "$path" --setenv TZ UTC --setenv HOME /tmp \
		--setenv RESTIC_REPOSITORY /repo --setenv RESTIC_PASSWORD "$password" \
		--setenv RESTIC_CACHE_DIR /cache --setenv DATA_DIR /data --setenv PRIVILEGED_MOVER 0 \
		"${env[@]}" \
		/mover-restic/entry.sh "$@"
}
bwrap_extra=()

# host_restic runs restic of one version on the host against a repository.
host_restic() {
	local version=$1 repo=$2
	shift 2
	RESTIC_REPOSITORY=$repo RESTIC_PASSWORD=$password TZ=UTC "$(binary "$version")" --no-cache "$@"
}

# backup writes the counter file and runs one mover backup.
backup() {
	local version=$1 repo=$2 data=$3 cache=$4 counter=$5
	echo "$counter" >"$data/counter"
	mover "$version" "$mover_host" "$repo" "$data" "$cache" FORGET_OPTIONS="--keep-last 10" -- backup >"$work/backup.log" 2>&1 ||
		{ cat "$work/backup.log" >&2; exit 1; }
}

# selection records what entry.sh restore picks from a repository, for
# RESTORE_AS_OF one second before, at and after each snapshot's whole second,
# and unset, each with SELECT_PREVIOUS 0, 1 and 2.
selection() {
	local version=$1 repo=$2 target=$3
	local -a moments=("")
	local t
	for t in $(host_restic "$version" "$repo" snapshots --json | jq -r '.[].time'); do
		local second
		second=$(date -u -d "$t" +%s)
		for delta in -1 0 1; do
			moments+=("$(date -u -d "@$((second + delta))" +%Y-%m-%dT%H:%M:%SZ)")
		done
	done
	local rows="[]" moment previous
	for moment in $(printf '%s\n' "${moments[@]}" | sort -u) ""; do
		for previous in 0 1 2; do
			local data="$work/restore-data" cache="$work/restore-cache"
			rm -rf "$data" "$cache"
			mkdir -p "$data" "$cache"
			local -a env=()
			[[ -n $moment ]] && env+=("RESTORE_AS_OF=$moment")
			env+=("SELECT_PREVIOUS=$previous")
			mover "$version" volsync-dst-notes-data-restore "$repo" "$data" "$cache" "${env[@]}" -- restore >"$work/restore.log" 2>&1 ||
				{ cat "$work/restore.log" >&2; exit 1; }
			local selected
			selected=$(sed -n 's/^Selected restic snapshot with id: //p' "$work/restore.log")
			if [[ -z $selected ]] && ! grep -q '^No eligible snapshots found' "$work/restore.log"; then
				cat "$work/restore.log" >&2
				exit 1
			fi
			rows=$(jq --arg m "$moment" --argjson p "$previous" --arg s "$selected" \
				'. + [{restoreAsOf: $m, selectPrevious: $p, selected: $s}]' <<<"$rows")
		done
	done
	jq . <<<"$rows" >"$target"
}

# checked_in copies a repository into the output tree without restic's local
# cache and without pack files no index references.
checked_in() {
	local repo=$1 target=$2 version=$3
	rm -rf "$target"
	mkdir -p "$target"
	cp -r "$repo/." "$target/"
	local indexed
	indexed=$(host_restic "$version" "$repo" list packs --no-lock 2>/dev/null | sort)
	local pack
	for pack in "$target"/data/*/*; do
		[[ -e $pack ]] || continue
		grep -qx "$(basename "$pack")" <<<"$indexed" || rm -f "$pack"
	done
	find "$target" -type d -empty -delete
	chmod -R u+rw,go-w "$target"
}

record_timed() {
	local version=$1 dir="$work/$1/timed"
	mkdir -p "$dir/repo" "$dir/data" "$dir/cache"
	for counter in 1 2 3; do
		backup "$version" "$dir/repo" "$dir/data" "$dir/cache" "$counter"
		sleep 1.3
	done
	local target="$out/restic-$version/timed"
	checked_in "$dir/repo" "$target/repo" "$version"
	host_restic "$version" "$dir/repo" snapshots --json | jq . >"$target/snapshots.json"
	selection "$version" "$dir/repo" "$target/selection.json"

	# restic's own form of a retime: the middle snapshot moved to a whole
	# second three seconds before it, then tagged quiesced.
	local copy="$work/$version/rewrite"
	rm -rf "$copy"
	cp -r "$dir/repo" "$copy"
	local id time new_time
	id=$(jq -r '.[1].id' "$target/snapshots.json")
	time=$(jq -r '.[1].time' "$target/snapshots.json")
	new_time=$(date -u -d "@$(($(date -u -d "$time" +%s) - 3))" '+%Y-%m-%d %H:%M:%S')
	host_restic "$version" "$copy" rewrite --forget --new-time "$new_time" "$id" >/dev/null 2>&1
	local rewritten
	rewritten=$(host_restic "$version" "$copy" snapshots --json | jq -r --arg old "$id" '.[] | select(.original == $old) | .id')
	host_restic "$version" "$copy" tag --add quiesced "$rewritten" >/dev/null 2>&1
	local tagged
	tagged=$(host_restic "$version" "$copy" snapshots --json | jq -r --arg old "$id" '.[] | select(.original == $old) | .id')
	jq -n --arg source "$id" --arg newTime "$(date -u -d "$new_time" +%Y-%m-%dT%H:%M:%SZ)" --arg tag quiesced \
		--arg id "$tagged" --argjson document "$(host_restic "$version" "$copy" cat snapshot "$tagged")" \
		'{source: $source, newTime: $newTime, tag: $tag, id: $id, document: $document}' >"$target/rewrite.json"
}

record_same_second() {
	local version=$1 dir="$work/$1/same-second" attempt
	for attempt in $(seq 1 40); do
		rm -rf "$dir"
		mkdir -p "$dir/repo" "$dir/data" "$dir/cache"
		echo 1 >"$dir/data/counter"
		bwrap_extra=()
		mover "$version" "$mover_host" "$dir/repo" "$dir/data" "$dir/cache" -- backup >/dev/null 2>&1
		# Two backups back to back, the way do_backup runs one, in one
		# sandbox so the second starts as the first ends.
		local bin
		bin=$(sandbox_bin "$version")
		echo 2 >"$dir/data/counter"
		bwrap --unshare-user --uid "$mover_uid" --gid "$mover_uid" --unshare-pid --unshare-uts --hostname "$mover_host" \
			--ro-bind /nix /nix --proc /proc --dev /dev --tmpfs /tmp --ro-bind "$bin" /sandbox-bin \
			--bind "$dir/repo" /repo --bind "$dir/data" /data --bind "$dir/cache" /cache \
			--clearenv --setenv PATH "/sandbox-bin:$(dirname "$(command -v bash)")" --setenv TZ UTC --setenv HOME /tmp \
			--setenv RESTIC_REPOSITORY /repo --setenv RESTIC_PASSWORD "$password" --setenv RESTIC_CACHE_DIR /cache \
			--chdir /data "$(command -v bash)" -c \
			'restic backup --host volsync --exclude=lost+found . >/dev/null && echo 3 >counter && restic backup --host volsync --exclude=lost+found . >/dev/null'
		local seconds
		seconds=$(host_restic "$version" "$dir/repo" snapshots --json | jq -r '.[1:] | map(.time | split(".")[0]) | unique | length')
		if [[ $seconds == 1 ]]; then
			local target="$out/restic-$version/same-second"
			checked_in "$dir/repo" "$target/repo" "$version"
			host_restic "$version" "$dir/repo" snapshots --json | jq . >"$target/snapshots.json"
			selection "$version" "$dir/repo" "$target/selection.json"
			return
		fi
	done
	echo "no two snapshots landed in one second after 40 attempts" >&2
	exit 1
}

# has_lock succeeds when the repository directory holds a lock file.
has_lock() {
	[[ -n $(ls -A "$1/locks" 2>/dev/null) ]]
}

record_killed_mover() {
	local version=$1 dir="$work/$1/killed-mover"
	rm -rf "$dir"
	mkdir -p "$dir/data" "$dir/cache"
	cp -r "$work/$version/timed/repo" "$dir/repo"
	# Enough data that the backup is still running when it is killed.
	head -c 268435456 /dev/urandom >"$dir/data/large"
	echo 4 >"$dir/data/counter"

	bwrap_extra=(--info-fd 3)
	mover "$version" "$mover_host" "$dir/repo" "$dir/data" "$dir/cache" -- backup 3>"$dir/info.json" >"$dir/mover.log" 2>&1 &
	local sandbox=$!
	# entry.sh runs restic cat config first, which holds a lock of its own for
	# a moment. The backup's lock is the one there once the backup started.
	local i
	for i in $(seq 1 2000); do
		grep -q '^=== Starting backup ===' "$dir/mover.log" && has_lock "$dir/repo" && break
		sleep 0.005
	done
	has_lock "$dir/repo" || { cat "$dir/mover.log" >&2; exit 1; }
	local child
	child=$(jq -r '."child-pid"' "$dir/info.json")
	# The kubelet's order: SIGTERM to the container's PID 1, which bash
	# without a trap ignores, then SIGKILL after the grace period.
	kill -TERM "$child" 2>/dev/null || true
	sleep 0.2
	kill -0 "$child" 2>/dev/null || { echo "PID 1 died on SIGTERM" >&2; exit 1; }
	kill -KILL "$child"
	wait "$sandbox" || true
	bwrap_extra=()

	local target="$out/restic-$version/killed-mover"
	checked_in "$dir/repo" "$target/repo" "$version"
	host_restic "$version" "$dir/repo" snapshots --json --no-lock | jq . >"$target/snapshots.json"
	local locks="[]" lock
	for lock in $(host_restic "$version" "$dir/repo" list locks --no-lock); do
		locks=$(jq --arg id "$lock" --argjson doc "$(host_restic "$version" "$dir/repo" cat lock "$lock" --no-lock)" \
			'. + [{id: $id, document: $doc}]' <<<"$locks")
	done
	jq . <<<"$locks" >"$target/locks.json"

	# restic's verdicts, on a copy each, while the lock is younger than
	# restic's 30 minutes.
	local copy="$work/$version/verdict" code output
	rm -rf "$copy" && cp -r "$dir/repo" "$copy"
	set +e
	output=$(host_restic "$version" "$copy" forget --host volsync --keep-last 1 --retry-lock 0s 2>&1)
	code=$?
	set -e
	local verdicts
	verdicts=$(jq -n --argjson code "$code" --arg output "$output" '{forget: {exitCode: $code, output: $output}}')
	output=$(host_restic "$version" "$copy" unlock 2>&1)
	verdicts=$(jq --arg output "$output" --argjson left "$(host_restic "$version" "$copy" list locks --no-lock | wc -l)" \
		'. + {unlock: {output: $output, locksLeft: $left}}' <<<"$verdicts")
	output=$(host_restic "$version" "$copy" unlock --remove-all 2>&1)
	verdicts=$(jq --arg output "$output" --argjson left "$(host_restic "$version" "$copy" list locks --no-lock | wc -l)" \
		'. + {unlockRemoveAll: {output: $output, locksLeft: $left}}' <<<"$verdicts")
	jq . <<<"$verdicts" >"$target/verdicts.json"
}

rm -rf "$out"
mkdir -p "$out"
for version in "$mover_version" "$latest_version"; do
	echo "restic $version: timed"
	record_timed "$version"
	echo "restic $version: same-second"
	record_same_second "$version"
	echo "restic $version: killed-mover"
	record_killed_mover "$version"
done

jq -n --arg generator "hack/fixtures/restic.sh" --arg recorded "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
	--arg mover "$mover_version" --arg latest "$latest_version" --arg volsync "$volsync_version" \
	--arg bwrapSeen "$(bwrap --version | awk '{print $2}')" \
	'{generator: $generator, recorded: $recorded,
	  tools: {"restic-mover": $mover, restic: $latest, volsync: $volsync, bubblewrap: $bwrapSeen},
	  password: "backup", moverHost: "volsync-src-notes-data-7xk2p", moverUID: 65532}' >"$out/provenance.json"
echo "wrote $out"
