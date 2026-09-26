package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// before is the fixture package every single-directory case starts from:
// a function with an imported package behind an alias, a plain function, a
// const group without iota, a var group whose spec spans lines, an iota
// group, a type and its method.
var before = map[string]string{
	"a.go": `package fixture

import util "example.com/one/util"

// F calls the imported package.
func F() int { return util.Do() }

// G is moved or changed by the cases.
func G() int { return 2 }

const (
	// X is one.
	X = 1
	Y = 2 // Y is two.
)

var (
	// V spans lines.
	V = []int{
		1,
	}
)

const (
	A = iota
	B
	C
)

// T is a type with a method.
type T struct{ n int }

// M returns n.
func (t *T) M() int { return t.n }
`,
}

// writePkg writes files into a new directory and returns it.
func writePkg(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, src := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(src), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// fingerprints runs movecheck on dir and returns each key's hash.
func fingerprints(t *testing.T, dir string) map[string]string {
	t.Helper()
	var out, errOut bytes.Buffer
	if status := run([]string{dir}, &out, &errOut); status != 0 {
		t.Fatalf("movecheck %s exited %d: %s", dir, status, errOut.String())
	}
	hashes := map[string]string{}
	for line := range strings.SplitSeq(strings.TrimSpace(out.String()), "\n") {
		i := strings.LastIndex(line, " ")
		hashes[line[:i]] = line[i+1:]
	}
	return hashes
}

// changed returns the keys whose hash differs between a and b, and the keys
// found on one side only.
func changed(a, b map[string]string) []string {
	var keys []string
	for k, h := range a {
		if b[k] != h {
			keys = append(keys, k)
		}
	}
	for k := range b {
		if _, ok := a[k]; !ok {
			keys = append(keys, k)
		}
	}
	return keys
}

// edited returns before's a.go with each old string replaced by its new one.
func edited(t *testing.T, pairs ...string) string {
	t.Helper()
	src := before["a.go"]
	for i := 0; i < len(pairs); i += 2 {
		if !strings.Contains(src, pairs[i]) {
			t.Fatalf("fixture has no %q", pairs[i])
		}
		src = strings.Replace(src, pairs[i], pairs[i+1], 1)
	}
	return src
}

func TestAMoveBetweenFilesKeepsEveryHash(t *testing.T) {
	a := edited(t,
		"// G is moved or changed by the cases.\nfunc G() int { return 2 }\n", "",
		"\t// X is one.\n\tX = 1\n", "",
		"\tY = 2 // Y is two.\n", "",
		"\t// V spans lines.\n\tV = []int{\n\t\t1,\n\t}\n", "",
		"// T is a type with a method.\ntype T struct{ n int }\n\n// M returns n.\nfunc (t *T) M() int { return t.n }\n", "")
	b := `package fixture

// M returns n.
func (t *T) M() int { return t.n }

// G is moved or changed by the cases.
func G() int { return 2 }

// X is one.
const X = 1

const Y = 2 // Y is two.

// V spans lines.
var V = []int{
	1,
}

// T is a type with a method.
type T struct{ n int }
`
	got := changed(fingerprints(t, writePkg(t, before)), fingerprints(t, writePkg(t, map[string]string{"a.go": a, "b.go": b})))
	if len(got) != 0 {
		t.Errorf("a move between files changed %v, want no hash changed", got)
	}
}

func TestAChangedBodyChangesTheHash(t *testing.T) {
	after := edited(t, "func G() int { return 2 }", "func G() int { return 3 }")
	got := changed(fingerprints(t, writePkg(t, before)), fingerprints(t, writePkg(t, map[string]string{"a.go": after})))
	if len(got) != 1 || got[0] != "func G" {
		t.Errorf("changed = %v, want [func G]", got)
	}
}

func TestAChangedCommentChangesTheHash(t *testing.T) {
	for comment, key := range map[string]string{"// X is one.": "const X", "// Y is two.": "const Y"} {
		after := edited(t, comment, comment+" Changed.")
		got := changed(fingerprints(t, writePkg(t, before)), fingerprints(t, writePkg(t, map[string]string{"a.go": after})))
		if len(got) != 1 || got[0] != key {
			t.Errorf("changed = %v after editing %q, want [%s]", got, comment, key)
		}
	}
}

func TestAnImportBehindTheSameNameChangesTheHash(t *testing.T) {
	for name, imp := range map[string]string{
		"aliased":   `import util "example.com/two/util"`,
		"unaliased": `import "example.com/two/util"`,
	} {
		t.Run(name, func(t *testing.T) {
			a := edited(t,
				"import util \"example.com/one/util\"\n", "",
				"// F calls the imported package.\nfunc F() int { return util.Do() }\n", "")
			b := "package fixture\n\n" + imp + "\n\n// F calls the imported package.\nfunc F() int { return util.Do() }\n"
			got := changed(fingerprints(t, writePkg(t, before)), fingerprints(t, writePkg(t, map[string]string{"a.go": a, "b.go": b})))
			if len(got) != 1 || got[0] != "func F" {
				t.Errorf("changed = %v, want [func F]: the file imports another path under the name util", got)
			}
		})
	}
}

func TestAnAliasChangeAloneKeepsTheHash(t *testing.T) {
	after := edited(t,
		`import util "example.com/one/util"`, `import "example.com/one/util"`)
	got := changed(fingerprints(t, writePkg(t, before)), fingerprints(t, writePkg(t, map[string]string{"a.go": after})))
	if len(got) != 0 {
		t.Errorf("changed = %v, want none: util names the same path", got)
	}
}

func TestSplittingAnIotaGroupChangesItsHashes(t *testing.T) {
	a := edited(t, "\tB\n\tC\n)", "\tB\n)")
	b := "package fixture\n\nconst (\n\tC = iota\n)\n"
	got := changed(fingerprints(t, writePkg(t, before)), fingerprints(t, writePkg(t, map[string]string{"a.go": a, "b.go": b})))
	want := map[string]bool{"const A": true, "const B": true, "const C": true}
	if len(got) != len(want) {
		t.Fatalf("changed = %v, want the three iota constants", got)
	}
	for _, k := range got {
		if !want[k] {
			t.Errorf("changed = %v, want only the iota constants", got)
		}
	}
}

// oldRuns is the old side of the rename cases: a package runs that declares
// getCluster, calls it, and has a local of the same name.
var oldRuns = map[string]string{"a.go": `package runs

// getCluster returns one.
func getCluster() int { return 1 }

// user calls getCluster.
func user() int { return getCluster() }

// local shadows getCluster.
func local() int {
	getCluster := 2
	return getCluster
}
`}

// compareDirs runs movecheck on two directories with a rename map and
// returns its status and standard output.
func compareDirs(t *testing.T, renames, oldDir, newDir string) (int, string) {
	t.Helper()
	var out, errOut bytes.Buffer
	status := run([]string{"-rename", renames, oldDir, newDir}, &out, &errOut)
	if status == 2 {
		t.Fatalf("movecheck exited 2: %s", errOut.String())
	}
	return status, out.String()
}

func TestTheRenameMapComparesAMoveIntoAnotherPackage(t *testing.T) {
	cnpg := writePkg(t, map[string]string{"cluster.go": `package cnpg

// getCluster returns one.
func GetCluster() int { return 1 }
`})
	status, out := compareDirs(t, "getCluster=cnpg.GetCluster", writePkg(t, oldRuns), cnpg)
	if status != 0 {
		t.Errorf("status = %d (%s), want 0: GetCluster moved unchanged", status, out)
	}

	changedBody := writePkg(t, map[string]string{"cluster.go": `package cnpg

// getCluster returns one.
func GetCluster() int { return 2 }
`})
	status, out = compareDirs(t, "getCluster=cnpg.GetCluster", writePkg(t, oldRuns), changedBody)
	if status != 1 || !strings.Contains(out, "changed: func GetCluster") {
		t.Errorf("status = %d, output %q, want 1 naming func GetCluster", status, out)
	}
}

func TestTheRenameMapSpellsAQualifiedReference(t *testing.T) {
	newRuns := writePkg(t, map[string]string{"a.go": `package runs

import "example.com/internal/cnpg"

// user calls getCluster.
func user() int { return cnpg.GetCluster() }

// local shadows getCluster.
func local() int {
	getCluster := 2
	return getCluster
}
`})
	status, out := compareDirs(t, "getCluster=cnpg.GetCluster", writePkg(t, oldRuns), newRuns)
	if status != 0 {
		t.Errorf("status = %d (%s), want 0: user calls the moved function, and the local keeps its name", status, out)
	}
}

func TestAnUnresolvedSelectorIsAnError(t *testing.T) {
	dir := writePkg(t, map[string]string{"a.go": "package fixture\n\nfunc F() int { return nowhere.Do() }\n"})
	var out, errOut bytes.Buffer
	if status := run([]string{dir}, &out, &errOut); status != 2 || !strings.Contains(errOut.String(), "nowhere resolves to no declaration or import") {
		t.Errorf("status = %d, stderr %q, want 2 naming nowhere", status, errOut.String())
	}
}
