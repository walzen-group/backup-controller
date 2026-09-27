package runs

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// ruleScope holds the directories, relative to this package, whose non-test
// files the rules read. internal/restorejob renders the termination message
// restic leaves and must never decide on it. internal/quiesce, internal/cnpg
// and internal/kueue hold code that moved out of internal/runs.
var ruleScope = []string{".", "../populator", "../restorejob", "../quiesce", "../cnpg", "../kueue"}

// textMatcherAllowlist names, as <package>.<declaration>, the declarations
// that may use a text matcher of strings, bytes or regexp, and why. A new
// entry needs a reviewer's eye like any rule exception.
var textMatcherAllowlist = map[string]string{
	"runs.RestoreRunReconciler.inPlaceClaimLost": "strings.HasPrefix and TrimPrefix on a Lease name, to find the claim Leases by the prefix the controller gives them",
	"runs.leaseItems":          "strings.Split on the Lease's items annotation, the comma-separated item names acquireLeases wrote",
	"quiesce.parseInventoryID": "strings.Cut and strings.LastIndex on a Kustomization inventory entry id, <namespace>_<name>_<group>_<kind> as fluxcd/cli-utils ObjMetadata.String writes it",
	"quiesce.Apply":            "strings.Cut on a status.suspendedKustomizations entry, the namespace/name key the controller built",
	"quiesce.Applied":          "strings.Cut on a status.suspendedKustomizations entry, the namespace/name key the controller built",
	"quiesce.resume":           "strings.Cut on a status.suspendedKustomizations entry, the namespace/name key the controller built",
	"runs.resticSpan":          "the regexp of the span syntax restic's --keep-within takes",
	"runs.retention":           "resticSpan.MatchString on the claim's retain-within annotation, a setting a person declared",
	"runs.lastLines":           "strings.Split and TrimSpace on a mover log, to show its last lines in a message; no decision reads the result",
}

// messageComparisonAllowlist names the declarations that may compare a
// message, a log or an error string, and why.
var messageComparisonAllowlist = map[string]string{
	"runs.startErrorNote": "adds status.items[].lastStartError to a failing item's message only when the item has one; no decision reads the note",
}

// textMatchers are the functions of packages strings and bytes that match,
// compare, cut, split or trim by content.
var textMatchers = []string{
	"HasPrefix", "HasSuffix", "Contains", "ContainsAny", "ContainsRune", "ContainsFunc", "Cut", "CutPrefix",
	"CutSuffix", "Trim", "TrimPrefix", "TrimSuffix", "TrimSpace", "TrimLeft", "TrimRight", "TrimFunc", "Index",
	"IndexAny", "IndexByte", "IndexFunc", "IndexRune", "LastIndex", "LastIndexAny", "LastIndexByte",
	"LastIndexFunc", "EqualFold", "Count", "Compare", "Equal", "Split", "SplitN", "SplitAfter", "SplitAfterN",
	"SplitSeq", "SplitAfterSeq", "Fields", "FieldsFunc", "FieldsSeq", "FieldsFuncSeq",
}

// No decision in the scope reads text a component wrote for a person: the
// controller's own messages, VolSync's mover logs, or an error's string.
// The test fails on two shapes outside the allowlists:
//   - A call of a text matcher of strings or bytes, or any use of regexp.
//   - A comparison, a switch or a map key on a .Message, .Logs or
//     .LastStartError field or on an Error() string, also through an index,
//     a slice, len, string() or a concatenation.
//
// An allowlist entry that lets nothing through fails too, so the lists stay
// exact. The rule reads syntax without types, so a reviewer still checks a
// message passed to a helper.
//
// Every item reason must also be an API constant: the test fails on a
// conversion to ItemReason, since that is the only way to give an item a
// reason docs/api.md does not list.
func TestNoDecisionReadsAMessage(t *testing.T) {
	t.Parallel()
	used := map[string]bool{}
	for _, dir := range ruleScope {
		files := parseRuleScope(t, dir)
		regexps := regexpVars(files)
		for _, file := range files {
			for _, f := range scanRules(file.ast, file.pkg, regexps) {
				allow := textMatcherAllowlist
				if f.comparison {
					allow = messageComparisonAllowlist
				}
				if _, ok := allow[f.decl]; ok {
					used[f.decl] = true
					continue
				}
				t.Errorf("%s:%d in %s: %s", file.path, file.fset.Position(f.pos).Line, f.decl, f.text)
			}
		}
	}
	for decl := range textMatcherAllowlist {
		if !used[decl] {
			t.Errorf("the allowlist entry %s matches nothing; delete it", decl)
		}
	}
	for decl := range messageComparisonAllowlist {
		if !used[decl] {
			t.Errorf("the allowlist entry %s matches nothing; delete it", decl)
		}
	}
}

// ruleFile is one parsed non-test file of the scope.
type ruleFile struct {
	path, pkg string
	fset      *token.FileSet
	ast       *ast.File
}

// parseRuleScope parses the non-test Go files of one directory.
func parseRuleScope(t *testing.T, dir string) []ruleFile {
	t.Helper()
	abs, err := filepath.Abs(dir)
	if err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("the scope %s holds no Go file: %v", dir, err)
	}
	var files []ruleFile
	for _, path := range paths {
		if strings.HasSuffix(path, "_test.go") {
			continue
		}
		fset := token.NewFileSet()
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, ruleFile{path: path, pkg: filepath.Base(abs), fset: fset, ast: file})
	}
	return files
}

// ruleFinding is one text match or message comparison.
type ruleFinding struct {
	// decl is the top-level declaration, as <package>.<name>.
	decl string
	pos  token.Pos
	// comparison is true for a message comparison, false for a text matcher
	// or an ItemReason conversion.
	comparison bool
	text       string
}

// scanRules returns every finding of one file, each named after its
// top-level declaration.
func scanRules(file *ast.File, pkg string, regexps map[string]bool) []ruleFinding {
	matchers := map[string]bool{}
	regexpName := ""
	for _, spec := range file.Imports {
		path, _ := strconv.Unquote(spec.Path.Value)
		name := path
		if spec.Name != nil {
			name = spec.Name.Name
		}
		switch path {
		case "strings", "bytes":
			matchers[name] = true
		case "regexp":
			regexpName = name
		}
	}
	var findings []ruleFinding
	for _, decl := range file.Decls {
		name := pkg + "." + declName(decl)
		add := func(n ast.Node, comparison bool, text string) {
			findings = append(findings, ruleFinding{decl: name, pos: n.Pos(), comparison: comparison, text: text})
		}
		ast.Inspect(decl, func(n ast.Node) bool {
			switch n := n.(type) {
			case *ast.SelectorExpr:
				if id, ok := n.X.(*ast.Ident); ok {
					if (matchers[id.Name] && slices.Contains(textMatchers, n.Sel.Name)) || id.Name == regexpName || regexps[id.Name] {
						add(n, false, "a text matcher "+id.Name+"."+n.Sel.Name)
					}
				}
			case *ast.CallExpr:
				if sel, ok := n.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "ItemReason" {
					add(n, false, "a conversion to ItemReason; use a constant of the API package")
				}
			case *ast.BinaryExpr:
				if n.Op.Precedence() == token.EQL.Precedence() && (isMessage(n.X) || isMessage(n.Y)) {
					add(n, true, "a comparison of a message")
				}
			case *ast.SwitchStmt:
				if n.Tag != nil && isMessage(n.Tag) {
					add(n, true, "a switch on a message")
				}
			case *ast.IndexExpr:
				if isMessage(n.Index) {
					add(n, true, "a lookup keyed by a message")
				}
			}
			return true
		})
	}
	return findings
}

// declName returns the name of a top-level declaration: a function, a
// method as Type.name, or the first name of a var, const or type.
func declName(decl ast.Decl) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if d.Recv != nil && len(d.Recv.List) == 1 {
			return fmt.Sprintf("%s.%s", receiverType(d.Recv.List[0].Type), d.Name.Name)
		}
		return d.Name.Name
	case *ast.GenDecl:
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.ValueSpec:
				return s.Names[0].Name
			case *ast.TypeSpec:
				return s.Name.Name
			}
		}
	}
	return "?"
}

// receiverType returns the name of a method's receiver type, without a
// pointer or type parameters.
func receiverType(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return receiverType(t.X)
	case *ast.IndexExpr:
		return receiverType(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}

// isMessage reports whether an expression is a message, a log or an error
// string, or is built from one.
func isMessage(e ast.Expr) bool {
	switch e := ast.Unparen(e).(type) {
	case *ast.SelectorExpr:
		return e.Sel.Name == "Message" || e.Sel.Name == "Logs" || e.Sel.Name == "LastStartError"
	case *ast.IndexExpr:
		return isMessage(e.X)
	case *ast.SliceExpr:
		return isMessage(e.X)
	case *ast.BinaryExpr:
		return e.Op == token.ADD && (isMessage(e.X) || isMessage(e.Y))
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Error" && len(e.Args) == 0 {
			return true
		}
		if id, ok := e.Fun.(*ast.Ident); ok && (id.Name == "string" || id.Name == "len") && len(e.Args) == 1 {
			return isMessage(e.Args[0])
		}
	}
	return false
}

// regexpVars returns the package-level vars of a directory that hold a value
// built by package regexp. A method called on one is a text matcher.
func regexpVars(files []ruleFile) map[string]bool {
	names := map[string]bool{}
	for _, file := range files {
		for _, decl := range file.ast.Decls {
			gen, ok := decl.(*ast.GenDecl)
			if !ok || gen.Tok != token.VAR {
				continue
			}
			for _, spec := range gen.Specs {
				value, ok := spec.(*ast.ValueSpec)
				if ok && len(value.Values) == 1 && strings.Contains(types.ExprString(value.Values[0]), "regexp.") {
					names[value.Names[0].Name] = true
				}
			}
		}
	}
	return names
}
