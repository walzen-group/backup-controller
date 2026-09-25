#!/usr/bin/env bash
# Copies the CustomResourceDefinitions the cluster tests install into
# internal/testinfra/crds, each from its real source at the version walzen prod
# runs (versions.json), and writes provenance.json with where each file came
# from and its sha256. The tests never download anything: they read these
# checked-in copies. Run it through make fixtures-crds after a pin moves.
#
# Sources:
#   cloudnative-pg/       Cluster and Backup, from the cloudnative-pg repository
#                         at the release tag
#   plugin-barman-cloud/  ObjectStore, from the plugin-barman-cloud repository
#                         at the release tag
#   flux/                 Kustomization, from the kustomize-controller release
#                         that the flux2 release tag names in
#                         manifests/bases/kustomize-controller
#   kueue/                Workload and LocalQueue, from the kueue repository at
#                         the release tag
#   volsync/              ReplicationSource and ReplicationDestination
#                         (config/crd/bases) and the VolumeSnapshot CRDs VolSync
#                         tests against (hack/crds), from the Go module in the
#                         module cache
#   backup-controller/    this repository's BackupRun, RestoreRun and
#                         VolumeRestore at the tags v0.7.2 and v0.8.1, the
#                         CRDs an upgrade finds installed
#
# A download that fails stops the script; nothing is half written, since every
# file is fetched into a scratch directory first and moved into place at the end.
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
versions="$root/versions.json"
target="$root/internal/testinfra/crds"
want() { jq -r ".components[\"$1\"].version" "$versions"; }

cnpg=$(want cloudnative-pg)
barman_plugin=$(want plugin-barman-cloud)
flux=$(want flux)
kueue=$(want kueue)
volsync=$(want volsync)
own_tags=(v0.7.2 v0.8.1)

work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
out="$work/crds"
mkdir -p "$out"
entries="$work/entries.jsonl"
: >"$entries"

# record DEST COMPONENT VERSION SOURCE: notes a file already written to
# $out/DEST in provenance.
record() {
	local sum
	sum=$(sha256sum "$out/$1" | cut -d' ' -f1)
	jq -cn --arg path "$1" --arg component "$2" --arg version "$3" --arg source "$4" --arg sha256 "$sum" \
		'{path: $path, component: $component, version: $version, source: $source, sha256: $sha256}' >>"$entries"
}

# fetch DEST COMPONENT VERSION URL [SOURCE]: downloads URL to $out/DEST and
# records SOURCE, or URL when SOURCE is not given, as where it came from.
fetch() {
	mkdir -p "$(dirname "$out/$1")"
	if ! curl -fsSL --retry 3 -o "$out/$1" "$4"; then
		echo "crds.sh: could not download $4" >&2
		exit 1
	fi
	record "$1" "$2" "$3" "${5:-$4}"
}

# CloudNativePG.
for kind in clusters backups; do
	fetch "cloudnative-pg/postgresql.cnpg.io_$kind.yaml" cloudnative-pg "$cnpg" \
		"https://raw.githubusercontent.com/cloudnative-pg/cloudnative-pg/v$cnpg/config/crd/bases/postgresql.cnpg.io_$kind.yaml"
done

# plugin-barman-cloud.
fetch plugin-barman-cloud/barmancloud.cnpg.io_objectstores.yaml plugin-barman-cloud "$barman_plugin" \
	"https://raw.githubusercontent.com/cloudnative-pg/plugin-barman-cloud/v$barman_plugin/config/crd/bases/barmancloud.cnpg.io_objectstores.yaml"

# Flux: the flux2 release names the kustomize-controller release it ships.
flux_kustomization="https://raw.githubusercontent.com/fluxcd/flux2/v$flux/manifests/bases/kustomize-controller/kustomization.yaml"
kc_url=$(curl -fsSL --retry 3 "$flux_kustomization" | grep -o 'https://github.com/fluxcd/kustomize-controller/releases/download/v[^/]*/kustomize-controller.crds.yaml')
if [ -z "$kc_url" ]; then
	echo "crds.sh: $flux_kustomization names no kustomize-controller CRD release" >&2
	exit 1
fi
fetch flux/kustomize-controller.crds.yaml flux "$flux" "$kc_url" "$kc_url (named by $flux_kustomization)"

# Kueue.
for kind in workloads localqueues; do
	fetch "kueue/kueue.x-k8s.io_$kind.yaml" kueue "$kueue" \
		"https://raw.githubusercontent.com/kubernetes-sigs/kueue/v$kueue/config/components/crd/bases/kueue.x-k8s.io_$kind.yaml"
done

# VolSync, from the module go.mod requires. The module's version has to be
# the one prod runs, or the copies would come from the wrong release.
gomod=$(cd "$root" && go list -m -f '{{.Version}}' github.com/backube/volsync)
if [ "$gomod" != "v$volsync" ]; then
	echo "crds.sh: go.mod requires github.com/backube/volsync $gomod, versions.json pins $volsync" >&2
	exit 1
fi
(cd "$root" && go mod download github.com/backube/volsync)
modcache=$(cd "$root" && go env GOMODCACHE)
module="$modcache/github.com/backube/volsync@v$volsync"
mkdir -p "$out/volsync"
for f in "$module"/config/crd/bases/*.yaml "$module"/hack/crds/*.yaml; do
	name=$(basename "$f")
	cp "$f" "$out/volsync/$name"
	chmod 644 "$out/volsync/$name"
	record "volsync/$name" volsync "$volsync" "go module github.com/backube/volsync@v$volsync ${f#"$module"/}"
done

# This repository's own CRDs at the tags an upgrade starts from.
for tag in "${own_tags[@]}"; do
	mkdir -p "$out/backup-controller/$tag"
	for kind in backupruns restoreruns volumerestores; do
		path="config/crd/backup.wlz.li_$kind.yaml"
		git -C "$root" show "$tag:$path" >"$out/backup-controller/$tag/backup.wlz.li_$kind.yaml"
		record "backup-controller/$tag/backup.wlz.li_$kind.yaml" backup-controller "${tag#v}" "git show $tag:$path"
	done
done

jq -s --arg generator hack/fixtures/crds.sh '{generator: $generator, files: .}' "$entries" >"$out/provenance.json"

# Replace the checked-in copies, keeping the Go test that sits beside them.
find "$target" -mindepth 1 -maxdepth 1 ! -name '*.go' -exec rm -rf {} +
cp -r "$out"/. "$target"/
echo "crds.sh: wrote $(jq '.files | length' "$out/provenance.json") CRD files to ${target#"$root"/}"
