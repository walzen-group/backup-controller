#!/usr/bin/env bash
# Records the repository under testdata/same-time/restic-<version>, with real
# restic of the version inside the VolSync mover image (versions.json,
# restic-mover). The conformance tests in internal/restic read it to check two
# things against restic itself: that Snapshots orders two snapshots with the
# same time by ID, and that MoverWritten accepts only snapshots written the way
# VolSync's backup mover writes them.
#
# Run it from the repository root in the flake's fixtures shell:
#
#	nix develop .#fixtures -c internal/restic/testdata/same-time/record.sh
#
# Every restic command runs in a bubblewrap sandbox where the data
# directories sit at /data, /extra and /srv, so the snapshots record those
# absolute paths, as a mover's backup of /data does. It writes:
#
#	repo/           the repository (version 2, password "backup")
#	snapshots.json  restic snapshots --json
#	kinds.json      the snapshot IDs by how each was written:
#	                  mover        host volsync, `restic backup .` run in /data
#	                  otherHost    host other, `restic backup .` run in /data
#	                  twoPaths     host volsync, `restic backup /data /extra`
#	                  otherPath    host volsync, `restic backup .` run in /srv
#	                  retimed      restic rewrite --new-time of the first mover
#	                               snapshot, which keeps its original
#
# The two mover snapshots carry the same --time, so restic stamps them with
# identical times.
set -euo pipefail

here=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
root=$(cd "$here/../../../.." && pwd)
version=$(jq -r '.components["restic-mover"].version' "$root/versions.json")
restic=$(command -v "restic-$version")
"$restic" version | grep -q "^restic $version " || { echo "$restic is not restic $version" >&2; exit 1; }

work=$(mktemp -d)
trap 'chmod -R u+w "$work" 2>/dev/null; rm -rf "$work"' EXIT
mkdir -p "$work/repo" "$work/data" "$work/extra" "$work/srv"

# run runs restic in the sandbox with the working directory given as $1.
run() {
	local dir=$1
	shift
	bwrap --unshare-user --unshare-pid --unshare-uts --hostname recorder \
		--ro-bind /nix /nix --proc /proc --dev /dev --tmpfs /tmp \
		--bind "$work/repo" /repo --bind "$work/data" /data \
		--bind "$work/extra" /extra --bind "$work/srv" /srv \
		--chdir "$dir" --clearenv --setenv TZ UTC --setenv HOME /tmp \
		--setenv RESTIC_REPOSITORY /repo --setenv RESTIC_PASSWORD backup \
		"$restic" --no-cache "$@"
}

# backup runs one backup and prints the ID of the snapshot it saved.
backup() {
	run "$@" --json | jq -r 'select(.message_type == "summary") | .snapshot_id'
}

run / init --repository-version 2 >/dev/null
echo 1 >"$work/data/counter"
mover1=$(backup /data backup --host volsync --time "2026-09-26 10:00:00" .)
echo 2 >"$work/data/counter"
mover2=$(backup /data backup --host volsync --time "2026-09-26 10:00:00" .)
other_host=$(backup /data backup --host other --time "2026-09-26 10:01:00" .)
echo extra >"$work/extra/file"
two_paths=$(backup / backup --host volsync --time "2026-09-26 10:02:00" /data /extra)
echo srv >"$work/srv/file"
other_path=$(backup /srv backup --host volsync --time "2026-09-26 10:03:00" .)
run / rewrite --new-time "2026-09-26 09:00:00" "$mover1" >/dev/null 2>&1

target="$here/restic-$version"
rm -rf "$target"
mkdir -p "$target"
cp -r "$work/repo" "$target/repo"
chmod -R u+rw,go-w "$target"
run / snapshots --json | jq . >"$target/snapshots.json"
retimed=$(jq -r --arg old "$mover1" '.[] | select(.original == $old) | .id' "$target/snapshots.json")
jq -n --arg m1 "$mover1" --arg m2 "$mover2" --arg oh "$other_host" --arg tp "$two_paths" \
	--arg op "$other_path" --arg rt "$retimed" \
	'{mover: [$m1, $m2], otherHost: [$oh], twoPaths: [$tp], otherPath: [$op], retimed: [$rt]}' >"$target/kinds.json"
echo "wrote $target"
