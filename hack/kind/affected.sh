#!/usr/bin/env bash
# affected.sh prints the -run pattern of the end-to-end scenarios that a change
# touches, from the table in hack/kind/README.md.
#
# Usage: hack/kind/affected.sh [BASE]
#   BASE is the git revision to compare the working tree with; the newest
#   version tag when omitted.
#
# The pattern goes to stdout, the chosen scenarios and any warning to stderr.
# It prints nothing when the change selects no scenario.
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$root"
base="${1:-$(git tag --list 'v*' --sort=-v:refname | head -1)}"

# The table rows between the markers, as "key<TAB>scenarios", where key is a
# path prefix or path:function.
rows="$(awk '/<!-- affected:start -->/{on=1; next} /<!-- affected:end -->/{on=0} on && /^\| / && !/^\| (Path|---)/' hack/kind/README.md |
  awk -F'|' '{gsub(/^ +| +$/, "", $2); gsub(/^ +| +$/, "", $3); print $2 "\t" $3}')"
smoke="$(printf '%s\n' "$rows" | awk -F'\t' '$1 == "smoke" {print $2}')"

selected=""
add() {
  local scenarios="$1"
  [[ "$scenarios" == none ]] && return
  [[ "$scenarios" == smoke ]] && scenarios="$smoke"
  selected="$selected, $scenarios"
}

# row prints the scenarios of the row whose key equals $1, or nothing.
row() { printf '%s\n' "$rows" | awk -F'\t' -v key="$1" '$1 == key {print $2}'; }

# prefix_rows prints the scenarios of every path row that $1 starts with.
prefix_rows() {
  printf '%s\n' "$rows" | awk -F'\t' -v file="$1" '$1 != "smoke" && $1 !~ /:/ && index(file, $1) == 1 {print $2}'
}

# changed_funcs prints the functions whose code changed in Go file $1, one per
# line, and "-" for a changed line outside any function other than the import
# block. Blank and comment-only lines don't count, and neither does the
# declaration line of a Test function, whose only change can be its name. A
# changed line belongs to the function the hunk starts in, or to a function
# the hunk adds before it.
# A new file counts as a change of every function in it.
changed_funcs() {
  local file="$1"
  if [[ -z "$(git ls-files "$file")" ]]; then
    grep -oE '^func (\([^)]*\) )?[A-Za-z0-9_]+' "$file" | awk '{print $NF}' | sed 's/.*)//'
    return
  fi
  local tests=0
  [[ "$file" == test/e2e/*_test.go ]] && tests=1
  git diff -U0 "$base" -- "$file" | awk -v tests="$tests" '
    /^@@/ {
      header = $0; sub(/^@@[^@]*@@ ?/, "", header)
      if (header ~ /^func /) {
        sub(/^func (\([^)]*\) )?/, "", header); sub(/[^A-Za-z0-9_].*/, "", header); current = header
      } else if (header ~ /^import/) { current = "import" } else { current = "-" }
      next
    }
    /^[+-]func / {
      name = $0; sub(/^[+-]func (\([^)]*\) )?/, "", name); sub(/[^A-Za-z0-9_].*/, "", name)
      # A Test function takes only t, so a changed declaration line of one is
      # a rename; its body lines decide whether it changed.
      if (!(tests && name ~ /^Test/)) print name
      current = name; next
    }
    # A blank or comment-only line changes no behaviour.
    /^[+-][ \t]*(\/\/.*)?$/ { next }
    /^[+-]/ && current != "" { print current }
  ' | grep -v '^import$' | sort -u
}

# callers prints the Test functions in test/e2e that call one of the named
# functions, directly or through other functions of the package.
callers() {
  local names=" $* " grew=1
  while [[ "$grew" == 1 ]]; do
    grew=0
    for f in $(awk '
        /^func / { fn = $0; sub(/^func (\([^)]*\) )?/, "", fn); sub(/[^A-Za-z0-9_].*/, "", fn); next }
        fn != "" { print fn "\t" $0 }
      ' test/e2e/*_test.go | awk -F'\t' -v names="$names" '
        { n = split(names, list, " "); for (i = 1; i <= n; i++) if (list[i] != "" && index($2, list[i] "(") > 0) print $1 }
      ' | sort -u); do
      if [[ "$names" != *" $f "* ]]; then names="$names$f "; grew=1; fi
    done
  done
  # The load scenario runs only for the rows that name it (see the README).
  printf '%s\n' $names | grep '^Test' | grep -vx TestSeventyNamespacesBackUpAtOneTick || true
}

changed="$( { git diff --name-only "$base"; git ls-files --others --exclude-standard; } | sort -u)"

while IFS= read -r file; do
  [[ -z "$file" ]] && continue
  case "$file" in
    docs/*|*.md) continue ;;
  esac

  # A scenario file selects its changed Test functions and the Test
  # functions that call its changed helpers.
  if [[ "$file" == test/e2e/*_test.go ]]; then
    [[ -f "$file" ]] || continue
    funcs="$(changed_funcs "$file" | grep -v '^-$' || true)"
    tests="$(printf '%s\n' $funcs | grep '^Test' || true)"
    helpers="$(printf '%s\n' $funcs | grep -v '^Test' || true)"
    [[ -n "$helpers" ]] && tests="$tests $(callers $helpers)"
    # Names a change removed or renamed away no longer exist.
    tests="$(for t in $tests; do grep -qE "^func $t\(" test/e2e/*_test.go && echo "$t"; done | sort -u | paste -sd, -)"
    if [[ -n "$tests" ]]; then add "$tests"; else add "$(prefix_rows "$file" | paste -sd, -)"; fi
    continue
  fi

  file_rows="$(prefix_rows "$file")"
  if [[ "$file" == *.go && -f "$file" ]]; then
    fallback=0
    while IFS= read -r fn; do
      [[ -z "$fn" ]] && continue
      scenarios="$(row "$file:$fn")"
      if [[ -n "$scenarios" ]]; then add "$scenarios"; else fallback=1; fi
    done <<< "$(changed_funcs "$file")"
    [[ "$fallback" == 0 ]] && continue
  fi
  if [[ -z "$file_rows" ]]; then
    echo "warning: no row matches $file; running the smoke scenarios" >&2
    add smoke
    continue
  fi
  while IFS= read -r scenarios; do add "$scenarios"; done <<< "$file_rows"
done <<< "$changed"

list="$(printf '%s\n' "$selected" | tr ',' '\n' | sed 's/^ *//; s/ *$//' | grep -v '^$' | sort -u)"
if [[ -z "$list" ]]; then
  echo "the change selects no scenario" >&2
  exit 0
fi
printf '%s\n' "$list" | sed 's/^/  /' >&2
printf '^(%s)$\n' "$(printf '%s\n' "$list" | paste -sd'|' -)"
