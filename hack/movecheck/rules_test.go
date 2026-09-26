package main

import (
	"bytes"
	"slices"
	"strings"
	"testing"
)

// hashesOf writes files into a new directory and returns movecheck's hash
// for each key found there.
func hashesOf(t *testing.T, files map[string]string) map[string]string {
	t.Helper()
	return fingerprints(t, writePkg(t, files))
}

// changedKeys returns the keys whose hash differs between the package
// written from before and the one written from after, sorted.
func changedKeys(t *testing.T, before, after map[string]string) []string {
	t.Helper()
	got := changed(hashesOf(t, before), hashesOf(t, after))
	slices.Sort(got)
	return got
}

func TestAnIotaConstWithAnUnresolvedTypeIsHashedWithItsGroup(t *testing.T) {
	for name, typ := range map[string]struct{ decl, use string }{
		"imported type":        {decl: "", use: "util.Kind"},
		"local imported alias": {decl: "type K util.Base\n\n", use: "K"},
	} {
		t.Run(name, func(t *testing.T) {
			head := "package fixture\n\nimport \"example.com/util\"\n\n" + typ.decl
			grouped := head + "const (\n\tA = 1\n\tB " + typ.use + " = iota\n)\n"
			split := head + "const (\n\tA = 1\n)\n\nconst B " + typ.use + " = iota\n"
			got := changedKeys(t, map[string]string{"a.go": grouped}, map[string]string{"a.go": split})
			if !slices.Contains(got, "const B") {
				t.Errorf("changed = %v, want const B: its value goes from 1 to 0", got)
			}
		})
	}
}

func TestAShadowedIotaIsNotPlaceDependent(t *testing.T) {
	head := "package fixture\n\nconst iota = 7\n\n"
	grouped := head + "const (\n\tA = 1\n\tB = iota\n)\n"
	split := head + "const (\n\tA = 1\n)\n\nconst B = iota\n"
	if got := changedKeys(t, map[string]string{"a.go": grouped}, map[string]string{"a.go": split}); len(got) != 0 {
		t.Errorf("changed = %v, want none: iota here is the package's constant", got)
	}
}

func TestARawStringKeepsItsIndentation(t *testing.T) {
	before := map[string]string{"a.go": "package fixture\n\nvar Script = `\n  echo a\n`\n"}
	after := map[string]string{"a.go": "package fixture\n\nvar Script = `\n      echo a\n`\n"}
	if got := changedKeys(t, before, after); !slices.Equal(got, []string{"var Script"}) {
		t.Errorf("changed = %v, want [var Script]: the string's value changed", got)
	}
}

func TestARawStringInAGroupMovesOutUnchanged(t *testing.T) {
	before := map[string]string{"a.go": "package fixture\n\nvar (\n\tScript = `\n  echo a\n`\n\tOther = 1\n)\n"}
	after := map[string]string{"a.go": "package fixture\n\nvar Script = `\n  echo a\n`\n\nvar (\n\tOther = 1\n)\n"}
	if got := changedKeys(t, before, after); len(got) != 0 {
		t.Errorf("changed = %v, want none: only the group's indentation moved", got)
	}
}

func TestAGroupDocIsHashedIntoEachSpec(t *testing.T) {
	before := map[string]string{"a.go": "package fixture\n\n// Limits are in bytes.\nconst (\n\tA = 1\n\tB = 2\n)\n"}
	after := map[string]string{"a.go": "package fixture\n\n// Limits are in megabytes.\nconst (\n\tA = 1\n\tB = 2\n)\n"}
	if got := changedKeys(t, before, after); !slices.Equal(got, []string{"const A", "const B"}) {
		t.Errorf("changed = %v, want [const A const B]: the doc of their group changed", got)
	}
}

func TestAGroupOfOneHashesLikeTheSpecWithTheSameDoc(t *testing.T) {
	before := map[string]string{"a.go": "package fixture\n\n// A is one.\nconst (\n\tA = 1\n)\n"}
	after := map[string]string{"a.go": "package fixture\n\n// A is one.\nconst A = 1\n"}
	if got := changedKeys(t, before, after); len(got) != 0 {
		t.Errorf("changed = %v, want none: the doc and the spec are the same", got)
	}
}

// compareWith runs movecheck with args and returns its status, standard
// output and standard error.
func compareWith(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	status := run(args, &out, &errOut)
	return status, out.String(), errOut.String()
}

func TestADeclarationOnlyInTheNewDirectoryFailsTheComparison(t *testing.T) {
	oldDir := writePkg(t, map[string]string{"a.go": "package fixture\n\nfunc A() int { return 1 }\n\nfunc B() int { return 2 }\n"})
	newDir := writePkg(t, map[string]string{"a.go": "package fixture\n\nfunc A() int { return 1 }\n\nfunc C() int { return 2 }\n"})

	status, out, errOut := compareWith(oldDir, newDir)
	if status != 1 || !strings.Contains(out, "only in new: func C") {
		t.Errorf("status = %d, stdout %q, stderr %q; want 1 naming func C: B went missing or was renamed without a map entry", status, out, errOut)
	}

	status, out, errOut = compareWith("-new", "C", oldDir, newDir)
	if status != 0 {
		t.Errorf("with -new C: status = %d, stdout %q, stderr %q; want 0", status, out, errOut)
	}
}

func TestAnUnusedNewEntryFailsTheComparison(t *testing.T) {
	dir := writePkg(t, map[string]string{"a.go": "package fixture\n\nfunc A() int { return 1 }\n"})
	status, out, errOut := compareWith("-new", "Missing", dir, dir)
	if status != 1 || !strings.Contains(out, "unused -new entry: Missing") {
		t.Errorf("status = %d, stdout %q, stderr %q; want 1 naming Missing", status, out, errOut)
	}
}

func TestTheNewListNeedsTwoDirectories(t *testing.T) {
	dir := writePkg(t, map[string]string{"a.go": "package fixture\n\nfunc A() int { return 1 }\n"})
	if status, _, errOut := compareWith("-new", "A", dir); status != 2 {
		t.Errorf("status = %d, stderr %q; want 2", status, errOut)
	}
}

func TestAFailedGoListIsNamedWhenANameDoesNotResolve(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	dir := writePkg(t, map[string]string{"a.go": "package fixture\n\nimport \"math/rand/v2\"\n\nfunc F() int { return rand.IntN(3) }\n"})
	status, _, errOut := compareWith(dir)
	if status != 2 || !strings.Contains(errOut, "go list") {
		t.Errorf("status = %d, stderr %q; want 2 saying go list failed", status, errOut)
	}
}

func TestAValueTheCheckerSkippedIsResolvedByScope(t *testing.T) {
	body := "\n\n// H builds a map from imported names.\nfunc H() any { return map[string]string{util.K: util.V} }\n"
	one := hashesOf(t, map[string]string{"a.go": "package fixture\n\nimport \"example.com/one/util\"" + body})
	two := hashesOf(t, map[string]string{"a.go": "package fixture\n\nimport \"example.com/two/util\"" + body})
	if got := changed(one, two); !slices.Equal(got, []string{"func H"}) {
		t.Errorf("changed = %v, want [func H]: util.V names another path, and the checker skips a value whose key has no type", got)
	}
}

func TestRepeatedNamesAreNumbered(t *testing.T) {
	got := hashesOf(t, map[string]string{
		"a.go": "package fixture\n\nfunc init() {}\n\nvar _ = 1\n",
		"b.go": "package fixture\n\nfunc init() {}\n\nvar _ = 2\n",
	})
	for _, key := range []string{"func init", "func init#2", "var _", "var _#2"} {
		if _, ok := got[key]; !ok {
			t.Errorf("keys = %v, want %s", got, key)
		}
	}
}

func TestAMethodIsKeyedByItsReceiverType(t *testing.T) {
	got := hashesOf(t, map[string]string{"a.go": "package fixture\n\ntype T int\n\ntype U int\n\nfunc (T) M() {}\n\nfunc (*U) M() {}\n"})
	for _, key := range []string{"func T.M", "func U.M"} {
		if _, ok := got[key]; !ok {
			t.Errorf("keys = %v, want %s", got, key)
		}
	}
}

func TestGoListNamesAPackageWhosePathEndsInAVersion(t *testing.T) {
	got := hashesOf(t, map[string]string{"a.go": "package fixture\n\nimport \"math/rand/v2\"\n\nfunc F() int { return rand.IntN(3) }\n"})
	if _, ok := got["func F"]; !ok {
		t.Errorf("keys = %v, want func F", got)
	}
}

func TestAFieldNamedLikeARenamedIdentifierKeepsItsName(t *testing.T) {
	viaField := "package runs\n\nimport \"example.com/util\"\n\n// viaField reads a field of an imported type.\nfunc viaField(s *util.State) int { return s.getCluster }\n"
	oldDir := writePkg(t, map[string]string{"a.go": viaField, "b.go": "package runs\n\n// getCluster returns one.\nfunc getCluster() int { return 1 }\n"})
	newDir := writePkg(t, map[string]string{"a.go": viaField})
	status, out, errOut := compareWith("-rename", "getCluster=cnpg.GetCluster", oldDir, newDir)
	if status != 0 {
		t.Errorf("status = %d, stdout %q, stderr %q; want 0: s.getCluster is a field and keeps its name", status, out, errOut)
	}
}
