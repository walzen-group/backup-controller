package runs

import (
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// apiPackage is the package that declares ItemReason and every value an
// item may record in it.
const apiPackage = "github.com/walzen-group/backup-controller/internal/api/v1alpha1"

// reasonRuleScope holds the packages TestEveryItemReasonIsAnAPIConstant
// type-checks, as directories relative to this package. Only their
// non-test files are read.
var reasonRuleScope = []string{".", "../populator"}

// reasonRuleEntry names the values one top-level declaration may give a
// reason although they are no API constant, and why. Every other value in
// the declaration is checked as anywhere else.
type reasonRuleEntry struct {
	// file is the base name of the file that holds the declaration.
	file string
	// decl is the declaration: a function, or a method as Type.name.
	decl string
	// forwarded are the values let through, each written as the source
	// has it, such as failure.reason.
	forwarded []string
	// what says where the reason it passes on comes from, which is always
	// a reason a checked site already set.
	what string
}

// reasonForwardingAllowlist lets failure.go pass on a reason its caller
// chose: refuse stores the reason of a checked call, and the item
// failures copy a *refusalError's reason into the item. The constant
// reasons these declarations set of their own are checked. A new entry
// needs a reviewer's eye like any rule exception.
var reasonForwardingAllowlist = []reasonRuleEntry{
	{file: "failure.go", decl: "refuse", forwarded: []string{"reason"},
		what: "stores its reason parameter, which the rule checks at every call"},
	{file: "failure.go", decl: "asItemFailure", forwarded: []string{"refused.reason"},
		what: "copies the reason of the *refusalError it found"},
	{file: "failure.go", decl: "failBackupItem", forwarded: []string{"failure.reason"},
		what: "copies the reason asItemFailure returned"},
	{file: "failure.go", decl: "failRestoreItem", forwarded: []string{"failure.reason"},
		what: "copies the reason asItemFailure returned"},
}

// reasonListUse is one value of an entry, as applyReasonList reports it
// when the value let nothing through.
type reasonListUse struct {
	file, decl string
	// value is the forwarded value.
	value string
}

// Every reason an item records is a constant the API package declares, so
// no item carries a reason that docs/api.md does not list and no decision
// has to guess what a reason means. The rule checks the first argument of
// each call to refuse, and each value assigned to a field or a variable of
// type ItemReason: by =, :=, an operator assignment, a var declaration or a
// struct literal. The value passes only when it names a *types.Const of
// type ItemReason from the API package. failure.go's forwarding is let
// through by declaration and value, so any other value in those
// declarations is checked, and a listed value that matches nothing fails
// too, so the list stays exact.
func TestEveryItemReasonIsAnAPIConstant(t *testing.T) {
	findings := scanReasonRule(t, reasonRuleScope...)
	left, unused := applyReasonList(findings, reasonForwardingAllowlist)
	for _, f := range left {
		t.Errorf("%s", f)
	}
	for _, u := range unused {
		t.Errorf("the list entry %s %s lets %s through, which matches nothing; delete it", u.file, u.decl, u.value)
	}
}

// The rule catches each shape of a reason that is not an API constant in
// its fixture: an untyped string passed to refuse or assigned, a
// conversion, a parameter or variable, a constant of another type, an
// operator assignment, a var declaration and struct literals with and
// without keys. A listed value in a listed declaration is let through, a
// literal beside it in the same declaration is still reported, and a
// listed value that matches nothing is reported.
func TestTheReasonRuleCatchesEveryShape(t *testing.T) {
	findings := scanReasonRule(t, "./testdata/reasonrule")
	left, unused := applyReasonList(findings, []reasonRuleEntry{
		{file: "fixture.go", decl: "allowed", forwarded: []string{"f.reason", "g.reason"}, what: "a reason the fixture's failure carries"},
		{file: "fixture.go", decl: "stale", forwarded: []string{"f.reason"}, what: "nothing"},
	})
	got := map[int]bool{}
	for _, f := range left {
		got[f.line] = true
	}
	want := fixtureWants(t, "testdata/reasonrule/fixture.go")
	for line := range want {
		if !got[line] {
			t.Errorf("fixture line %d: the rule found nothing, want a reason finding", line)
		}
	}
	for _, f := range left {
		if _, ok := want[f.line]; !ok {
			t.Errorf("fixture line %d: %s found where none is wanted", f.line, f.text)
		}
	}
	wantUnused := []reasonListUse{
		{file: "fixture.go", decl: "allowed", value: "g.reason"},
		{file: "fixture.go", decl: "stale", value: "f.reason"},
	}
	if !slices.Equal(unused, wantUnused) {
		t.Errorf("unused values = %+v, want %+v", unused, wantUnused)
	}
}

// reasonFinding is one reason the rule found that is not an API constant.
type reasonFinding struct {
	file, decl string
	line       int
	// value is the reason's expression as the source has it.
	value string
	// text says what was found.
	text string
}

// String renders the finding for a test failure: where it is, in which
// declaration, and what was found.
func (f reasonFinding) String() string {
	return fmt.Sprintf("%s:%d in %s: %s", f.file, f.line, f.decl, f.text)
}

// applyReasonList drops the findings the list lets through.
//
// Parameters:
//   - findings are the rule's findings, before any list applies.
//   - list names, per declaration, the values let through.
//
// It returns the findings left, and each listed value that let nothing
// through, in the list's order.
//
// A finding is let through only when its declaration is listed and its
// value is one of that entry's forwarded values; any other value in a
// listed declaration stays a finding.
func applyReasonList(findings []reasonFinding, list []reasonRuleEntry) (left []reasonFinding, unused []reasonListUse) {
	used := map[reasonListUse]bool{}
	for _, f := range findings {
		use := reasonListUse{file: filepath.Base(f.file), decl: f.decl, value: f.value}
		listed := slices.ContainsFunc(list, func(e reasonRuleEntry) bool {
			return e.file == use.file && e.decl == use.decl && slices.Contains(e.forwarded, use.value)
		})
		if !listed {
			left = append(left, f)
			continue
		}
		used[use] = true
	}
	for _, e := range list {
		for _, value := range e.forwarded {
			if use := (reasonListUse{file: e.file, decl: e.decl, value: value}); !used[use] {
				unused = append(unused, use)
			}
		}
	}
	return left, unused
}

// scanReasonRule type-checks the non-test files of each directory and
// returns every reason in them that is not an API constant, before any
// list applies.
//
// Parameters:
//   - dirs are the package directories, relative to this package, each
//     written as ., or with a leading ./ or ../, so go list reads it as a
//     directory.
func scanReasonRule(t *testing.T, dirs ...string) []reasonFinding {
	t.Helper()
	fset := token.NewFileSet()
	config := &types.Config{Importer: importer.ForCompiler(fset, "gc", exportLookup(t, dirs))}
	var findings []reasonFinding
	for _, dir := range dirs {
		files := parseNonTestFiles(t, fset, dir)
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
		pkg, err := config.Check(dir, fset, files, info)
		if err != nil {
			t.Fatalf("type-check %s: %v", dir, err)
		}
		scan := reasonScan{fset: fset, info: info, refuse: pkg.Scope().Lookup("refuse")}
		for _, file := range files {
			for _, decl := range file.Decls {
				for _, named := range declNames(decl) {
					findings = append(findings, scan.decl(named)...)
				}
			}
		}
	}
	return findings
}

// exportLookup returns the lookup the gc importer reads export data
// through. It asks go list, once, for the export data file of every package
// the directories import, directly or not, so the type-check needs no
// package outside the standard library.
func exportLookup(t *testing.T, dirs []string) func(string) (io.ReadCloser, error) {
	t.Helper()
	args := append([]string{"list", "-export", "-deps", "-f", "{{.ImportPath}} {{.Export}}"}, dirs...)
	out, err := exec.CommandContext(t.Context(), "go", args...).Output()
	if err != nil {
		t.Fatalf("go list the export data of %v: %v", dirs, err)
	}
	exports := map[string]string{}
	for line := range strings.Lines(string(out)) {
		if path, file, ok := strings.Cut(strings.TrimSpace(line), " "); ok {
			exports[path] = file
		}
	}
	return func(path string) (io.ReadCloser, error) {
		file, ok := exports[path]
		if !ok {
			return nil, fmt.Errorf("go list gave no export data for %s", path)
		}
		return os.Open(file)
	}
}

// parseNonTestFiles parses the non-test Go files of one directory.
func parseNonTestFiles(t *testing.T, fset *token.FileSet, dir string) []*ast.File {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		t.Fatal(err)
	}
	var files []*ast.File
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	if len(files) == 0 {
		t.Fatalf("the directory %s holds no Go file", dir)
	}
	return files
}

// reasonScan is what the rule knows of one package while it scans it.
type reasonScan struct {
	fset *token.FileSet
	info *types.Info
	// refuse is the package's refuse function, or nil when it has none.
	refuse types.Object
}

// decl returns the reasons in one top-level declaration that are not API
// constants.
func (s reasonScan) decl(named namedNode) []reasonFinding {
	var findings []reasonFinding
	check := func(value ast.Expr, what string) {
		if !s.isAPIReason(value) {
			pos := s.fset.Position(value.Pos())
			findings = append(findings, reasonFinding{file: pos.Filename, decl: named.name, line: pos.Line,
				value: types.ExprString(value), text: what + " is not an ItemReason constant of " + apiPackage})
		}
	}
	ast.Inspect(named.node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.CallExpr:
			if id, ok := ast.Unparen(n.Fun).(*ast.Ident); ok && s.refuse != nil && s.info.Uses[id] == s.refuse && len(n.Args) > 0 {
				check(n.Args[0], "the reason passed to refuse")
			}
		case *ast.AssignStmt:
			s.checkAssign(n, check)
		case *ast.ValueSpec:
			for i, value := range n.Values {
				if i < len(n.Names) && isItemReason(s.info.TypeOf(n.Names[i])) {
					check(value, "the value declared for "+n.Names[i].Name)
				}
			}
		case *ast.CompositeLit:
			s.checkLiteral(n, check)
		}
		return true
	})
	return findings
}

// checkAssign checks each value an assignment gives a field or a variable
// of type ItemReason. An operator assignment, and an assignment from a
// call's results, never assigns a constant, so the rule reports either.
func (s reasonScan) checkAssign(a *ast.AssignStmt, check func(ast.Expr, string)) {
	for i, lhs := range a.Lhs {
		if !isItemReason(s.info.TypeOf(lhs)) {
			continue
		}
		switch {
		case a.Tok != token.ASSIGN && a.Tok != token.DEFINE:
			// The left side is a variable or a field, never a constant, so
			// the check always reports it.
			check(lhs, "an operator assignment to a reason")
		case len(a.Rhs) != len(a.Lhs):
			check(a.Rhs[0], "a reason taken from a call's results")
		default:
			check(a.Rhs[i], "the value assigned to a reason")
		}
	}
}

// checkLiteral checks each value a struct literal gives a field of type
// ItemReason, with or without keys.
func (s reasonScan) checkLiteral(lit *ast.CompositeLit, check func(ast.Expr, string)) {
	st, ok := s.info.TypeOf(lit).Underlying().(*types.Struct)
	if !ok {
		return
	}
	for i, elt := range lit.Elts {
		field, value := types.Object(nil), elt
		if kv, keyed := elt.(*ast.KeyValueExpr); keyed {
			if id, isIdent := kv.Key.(*ast.Ident); isIdent {
				field = s.info.Uses[id]
			}
			value = kv.Value
		} else if i < st.NumFields() {
			field = st.Field(i)
		}
		if field != nil && isItemReason(field.Type()) {
			check(value, "the value of the field "+field.Name())
		}
	}
}

// isAPIReason reports whether an expression names a constant of type
// ItemReason that the API package declares.
func (s reasonScan) isAPIReason(e ast.Expr) bool {
	var id *ast.Ident
	switch e := ast.Unparen(e).(type) {
	case *ast.Ident:
		id = e
	case *ast.SelectorExpr:
		id = e.Sel
	default:
		return false
	}
	c, ok := s.info.Uses[id].(*types.Const)
	return ok && c.Pkg() != nil && c.Pkg().Path() == apiPackage && isItemReason(c.Type())
}

// isItemReason reports whether a type is the API package's ItemReason.
func isItemReason(t types.Type) bool {
	named, ok := t.(*types.Named)
	if !ok {
		return false
	}
	obj := named.Obj()
	return obj.Pkg() != nil && obj.Pkg().Path() == apiPackage && obj.Name() == "ItemReason"
}
