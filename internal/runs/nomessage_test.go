package runs

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// messageRuleScope is the code TestNoDecisionReadsAMessage reads: the
// non-test Go files of each directory, or only the named files when a scope
// lists them. It covers every non-test file of internal/runs and
// internal/populator, and internal/restorejob, whose Read renders the
// termination message restic leaves and must never decide on it. It also
// covers internal/quiesce, internal/cnpg and internal/kueue, which hold code
// that moved out of internal/runs.
var messageRuleScope = []messageRuleDir{
	{dir: "."},
	{dir: "../populator"},
	{dir: "../restorejob"},
	{dir: "../quiesce"},
	{dir: "../cnpg"},
	{dir: "../kueue"},
}

// messageRuleDir is one directory the rule reads.
type messageRuleDir struct {
	// dir is the directory, relative to this package.
	dir string
	// files names the files to read in it. Empty means every non-test Go
	// file.
	files []string
}

// messageRuleEntry names one top-level declaration the rule lets through,
// and why.
type messageRuleEntry struct {
	// pkg is the directory's base name, such as runs or populator.
	pkg string
	// decl is the declaration: a function, a method as Type.name, or a
	// package-level var.
	decl string
	// what says what the declaration matches or compares, which is never
	// text a component wrote for a person.
	what string
}

// textMatcherAllowlist lets a declaration use a text matcher (part 1 of
// the rule). A new entry needs a reviewer's eye like any rule exception.
var textMatcherAllowlist = []messageRuleEntry{
	{pkg: "runs", decl: "RestoreRunReconciler.inPlaceClaimLost",
		what: "strings.HasPrefix and TrimPrefix on a Lease name, to find the claim Leases by the prefix the controller gives them"},
	{pkg: "runs", decl: "fieldName",
		what: "strings.Cut on a Go struct field's json tag, to get the field name the CRD schema must declare"},
	{pkg: "runs", decl: "leaseItems",
		what: "strings.Split on the Lease's items annotation, the comma-separated item names acquireLeases wrote"},
	{pkg: "quiesce", decl: "OtherNamespaces",
		what: "strings.Split on a Kustomization inventory entry id, <namespace>_<name>_<group>_<kind> as kustomize-controller records it"},
	{pkg: "quiesce", decl: "inventoryIDs",
		what: "strings.Count on a Kustomization inventory entry id, to check its four-part format"},
	{pkg: "quiesce", decl: "Apply",
		what: "strings.Cut on a status.suspendedKustomizations entry, the namespace/name key the controller built"},
	{pkg: "quiesce", decl: "Applied",
		what: "strings.Cut on a status.suspendedKustomizations entry, the namespace/name key the controller built"},
	{pkg: "quiesce", decl: "resume",
		what: "strings.Cut on a status.suspendedKustomizations entry, the namespace/name key the controller built"},
	{pkg: "runs", decl: "resticSpan",
		what: "the regexp of the span syntax restic's --keep-within takes"},
	{pkg: "runs", decl: "retention",
		what: "resticSpan.MatchString on the claim's retain-within annotation, a setting a person declared"},
	{pkg: "runs", decl: "lastLines",
		what: "strings.Split and TrimSpace on a mover log, to show its last lines in a message; no decision reads the result"},
}

// messageComparisonAllowlist lets a declaration compare a message, a log or
// an error string (part 2 of the rule). A new entry needs a reviewer's eye
// like any rule exception.
var messageComparisonAllowlist = []messageRuleEntry{
	{pkg: "runs", decl: "startErrorNote",
		what: "adds status.items[].lastStartError to a failing item's message only when the item has one; no decision reads the note"},
}

// No decision in the scope reads text a component wrote for a person: the
// controller's own messages, VolSync's mover logs, or an error's string.
// Part 1 fails on any text matcher outside the allowlist, since a message
// read through a local looks like any other string. Part 2 fails on a
// comparison (==, !=, <, >, <=, >=, a switch) of a .Message or .Logs
// field, an Error() string, an index, slice, len or concatenation of one, a
// local assigned from any of those, or the key or value of a range over
// one, and on a lookup keyed by any of them. An entry on a list that
// matches nothing fails too, so the lists stay exact.
//
// The rule reads each declaration on its own and without types, so two
// ways around it stay open, and a reviewer still checks for them:
//   - A helper in a file the scope does not read yet, or in another
//     package, can match a message the scope passes to it. The scope
//     shrinks this as later steps widen it; a helper in another package,
//     such as one in internal/restorejob, is never seen.
//   - A helper in the scope that compares its own string parameters, as in
//     sameText(a, b string) bool { return a == b }, is not seen, because a
//     parameter is never taken for a message.
func TestNoDecisionReadsAMessage(t *testing.T) {
	var findings []messageFinding
	for _, scope := range messageRuleScope {
		findings = append(findings, scanMessageRule(t, scope)...)
	}
	lists := messageRuleLists{
		matchers:    textMatcherAllowlist,
		comparisons: messageComparisonAllowlist,
	}
	left, unused := lists.apply(findings)
	for _, f := range left {
		t.Errorf("%s", f)
	}
	for _, e := range unused {
		t.Errorf("the list entry %s.%s (%s) matches nothing; delete it", e.pkg, e.decl, e.what)
	}
}

// The rule catches each shape of a message read in its fixture: a matcher
// on a field, on a local and in a package-level var, a split, a Compare, a
// bytes.Equal, a regexp method, a comparison of a field (lastStartError
// included), of an Error() string, of an index, a slice or a concatenation
// of one and of locals assigned from them, the len of one, an order
// comparison, a range over one, a lookup keyed by one, and a switch on a
// message. A declaration on the allowlist is let through, and an entry that
// matches nothing is reported.
func TestTheMessageRuleCatchesEveryShape(t *testing.T) {
	findings := scanMessageRule(t, messageRuleDir{dir: "testdata/nomessage"})
	lists := messageRuleLists{
		matchers: []messageRuleEntry{
			{pkg: "nomessage", decl: "allowed", what: "a key the fixture built"},
			{pkg: "nomessage", decl: "stale", what: "nothing"},
		},
	}
	left, unused := lists.apply(findings)
	got := map[int]string{}
	for _, f := range left {
		got[f.line] = f.rule
	}
	want := fixtureWants(t, "testdata/nomessage/fixture.go")
	for line, rule := range want {
		if got[line] != rule {
			t.Errorf("fixture line %d: rule found %q, want %q", line, got[line], rule)
		}
	}
	for line, rule := range got {
		if _, ok := want[line]; !ok {
			t.Errorf("fixture line %d: rule %q found where none is wanted", line, rule)
		}
	}
	if len(unused) != 1 || unused[0].decl != "stale" {
		t.Errorf("unused entries = %+v, want only stale", unused)
	}
}

// messageFinding is one message read the rule found.
type messageFinding struct {
	pkg, decl string
	file      string
	line      int
	// rule is ruleMatcher or ruleComparison.
	rule string
	// text says what was found.
	text string
}

// The two parts of the rule, as a finding names them.
const (
	ruleMatcher    = "matcher"
	ruleComparison = "comparison"
)

func (f messageFinding) String() string {
	return fmt.Sprintf("%s:%d in %s: %s (%s)", f.file, f.line, f.decl, f.text, f.rule)
}

// messageRuleLists holds the entries each part of the rule lets through.
type messageRuleLists struct {
	matchers, comparisons []messageRuleEntry
}

// apply drops the findings a list lets through. It returns the findings
// left and the entries that let nothing through.
func (l messageRuleLists) apply(findings []messageFinding) (left []messageFinding, unused []messageRuleEntry) {
	used := map[messageRuleEntry]bool{}
	for _, f := range findings {
		list := l.matchers
		if f.rule == ruleComparison {
			list = l.comparisons
		}
		i := slices.IndexFunc(list, func(e messageRuleEntry) bool { return e.pkg == f.pkg && e.decl == f.decl })
		if i < 0 {
			left = append(left, f)
			continue
		}
		used[list[i]] = true
	}
	for _, e := range slices.Concat(l.matchers, l.comparisons) {
		if !used[e] {
			unused = append(unused, e)
		}
	}
	return left, unused
}

// scanMessageRule parses the files of one scope and returns every message
// read in them, before any list applies.
func scanMessageRule(t *testing.T, scope messageRuleDir) []messageFinding {
	t.Helper()
	paths := make([]string, 0, len(scope.files))
	for _, name := range scope.files {
		paths = append(paths, filepath.Join(scope.dir, name))
	}
	if len(paths) == 0 {
		all, err := filepath.Glob(filepath.Join(scope.dir, "*.go"))
		if err != nil {
			t.Fatal(err)
		}
		for _, path := range all {
			if !strings.HasSuffix(path, "_test.go") {
				paths = append(paths, path)
			}
		}
	}
	if len(paths) == 0 {
		t.Fatalf("the scope %s holds no Go file", scope.dir)
	}
	abs, err := filepath.Abs(scope.dir)
	if err != nil {
		t.Fatal(err)
	}
	fset := token.NewFileSet()
	var files []*ast.File
	for _, path := range paths {
		file, err := parser.ParseFile(fset, path, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		files = append(files, file)
	}
	pkg := messagePackage{fset: fset, name: filepath.Base(abs), regexps: regexpNames(files)}
	var findings []messageFinding
	for _, file := range files {
		findings = append(findings, pkg.scanFile(file)...)
	}
	return findings
}

// messagePackage is what the rule knows of one package while it scans it.
type messagePackage struct {
	fset *token.FileSet
	name string
	// regexps are the names assigned a value built by package regexp
	// anywhere in the package. A method called on one is a matcher. The
	// rule goes by name alone, so a shadowed name counts as well.
	regexps map[string]bool
}

// textMatchers are the functions of packages strings and bytes the rule
// forbids: every one that matches, compares, cuts, splits or trims by
// content.
var textMatchers = map[string]bool{
	"HasPrefix": true, "HasSuffix": true, "Contains": true, "ContainsAny": true, "ContainsRune": true,
	"ContainsFunc": true, "Cut": true, "CutPrefix": true, "CutSuffix": true, "Trim": true, "TrimPrefix": true,
	"TrimSuffix": true, "TrimSpace": true, "TrimLeft": true, "TrimRight": true, "TrimFunc": true,
	"TrimLeftFunc": true, "TrimRightFunc": true, "Index": true, "IndexAny": true, "IndexByte": true,
	"IndexFunc": true, "IndexRune": true, "LastIndex": true, "LastIndexAny": true, "LastIndexByte": true,
	"LastIndexFunc": true, "EqualFold": true, "Count": true, "Compare": true, "Equal": true, "Split": true,
	"SplitN": true, "SplitAfter": true, "SplitAfterN": true, "SplitSeq": true, "SplitAfterSeq": true, "Fields": true,
	"FieldsFunc": true, "FieldsSeq": true, "FieldsFuncSeq": true,
}

// regexpNames returns every name assigned from an expression that uses
// package regexp, in any file of the package.
func regexpNames(files []*ast.File) map[string]bool {
	names := map[string]bool{}
	for _, file := range files {
		local := importName(file, "regexp")
		if local == "" {
			continue
		}
		ast.Inspect(file, func(n ast.Node) bool {
			lhs, rhs := assignment(n)
			for i, value := range rhs {
				if i < len(lhs) && usesPackage(value, local) {
					if id, ok := lhs[i].(*ast.Ident); ok {
						names[id.Name] = true
					}
				}
			}
			return true
		})
	}
	return names
}

// assignment returns the two sides of an assignment or a var spec, and
// nothing for any other node.
func assignment(n ast.Node) (lhs, rhs []ast.Expr) {
	switch n := n.(type) {
	case *ast.AssignStmt:
		return n.Lhs, n.Rhs
	case *ast.ValueSpec:
		for _, name := range n.Names {
			lhs = append(lhs, name)
		}
		return lhs, n.Values
	}
	return nil, nil
}

// usesPackage reports whether an expression refers to the package imported
// as local.
func usesPackage(e ast.Expr, local string) bool {
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		if sel, ok := n.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok && id.Name == local {
				found = true
			}
		}
		return !found
	})
	return found
}

// importName returns the name a file refers to an import path by, or ""
// when the file does not import it.
func importName(file *ast.File, path string) string {
	for _, spec := range file.Imports {
		if p, err := strconv.Unquote(spec.Path.Value); err != nil || p != path {
			continue
		}
		if spec.Name != nil {
			return spec.Name.Name
		}
		return path
	}
	return ""
}

// scanFile returns the message reads of one file, each named after its
// top-level declaration. A dot import of strings, bytes or regexp is a
// finding of its own, since the rule could not see its calls.
func (p messagePackage) scanFile(file *ast.File) []messageFinding {
	var findings []messageFinding
	matchers := map[string]string{}
	for _, path := range []string{"strings", "bytes", "regexp"} {
		switch name := importName(file, path); name {
		case "", "_":
		case ".":
			findings = append(findings, p.finding(file, "import", ruleMatcher, "a dot import of "+path))
		default:
			matchers[name] = path
		}
	}
	for _, decl := range file.Decls {
		for _, named := range declNames(decl) {
			findings = append(findings, p.scanDecl(named, matchers)...)
		}
	}
	return findings
}

// scanDecl returns the message reads in one top-level declaration.
//
// Parameters:
//   - named is the declaration and its key.
//   - matchers maps the file's names for strings, bytes and regexp to the
//     import path.
func (p messagePackage) scanDecl(named namedNode, matchers map[string]string) []messageFinding {
	var findings []messageFinding
	add := func(n ast.Node, rule, text string) {
		findings = append(findings, p.finding(n, named.name, rule, text))
	}
	tainted := taintedLocals(named.node)
	message := func(e ast.Expr) bool { return isMessage(e, tainted) }
	ast.Inspect(named.node, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.SelectorExpr:
			p.checkMatcher(n, matchers, add)
		case *ast.BinaryExpr:
			if comparisonOps[n.Op] && (message(n.X) || message(n.Y)) {
				add(n, ruleComparison, "a comparison of a message, a log or an error string")
			}
		case *ast.IndexExpr:
			if message(n.Index) {
				add(n, ruleComparison, "a lookup keyed by a message, a log or an error string")
			}
		case *ast.SwitchStmt:
			checkSwitch(n, message, add)
		}
		return true
	})
	return findings
}

// comparisonOps are the operators that compare two values: equality and
// order. Either one applied to a message decides on its text.
var comparisonOps = map[token.Token]bool{
	token.EQL: true, token.NEQ: true, token.LSS: true, token.GTR: true, token.LEQ: true, token.GEQ: true,
}

// checkMatcher reports a selector that names a text matcher: a matching
// function of strings or bytes, any function of regexp, or a method of a
// name assigned from regexp.
func (p messagePackage) checkMatcher(sel *ast.SelectorExpr, matchers map[string]string, add func(ast.Node, string, string)) {
	id, ok := sel.X.(*ast.Ident)
	if !ok {
		return
	}
	switch path := matchers[id.Name]; {
	case path == "regexp":
		add(sel, ruleMatcher, "regexp."+sel.Sel.Name)
	case path != "" && textMatchers[sel.Sel.Name]:
		add(sel, ruleMatcher, path+"."+sel.Sel.Name)
	case path == "" && p.regexps[id.Name]:
		add(sel, ruleMatcher, "the regexp method "+id.Name+"."+sel.Sel.Name)
	}
}

// checkSwitch reports a switch whose tag, or one of whose case values, is a
// message.
func checkSwitch(s *ast.SwitchStmt, message func(ast.Expr) bool, add func(ast.Node, string, string)) {
	if s.Tag == nil {
		return
	}
	values := []ast.Expr{s.Tag}
	for _, stmt := range s.Body.List {
		if clause, ok := stmt.(*ast.CaseClause); ok {
			values = append(values, clause.List...)
		}
	}
	for _, v := range values {
		if message(v) {
			add(s, ruleComparison, "a switch on a message, a log or an error string")
			return
		}
	}
}

// taintedLocals returns the names in a declaration that are assigned a
// message, through :=, =, += or var, directly or from another such name,
// and the key and value of a range over a message, repeated until no name
// is added.
func taintedLocals(node ast.Node) map[string]bool {
	tainted := map[string]bool{}
	for changed := true; changed; {
		changed = false
		taint := func(e ast.Expr) {
			if id, ok := e.(*ast.Ident); ok && id.Name != "_" && !tainted[id.Name] {
				tainted[id.Name], changed = true, true
			}
		}
		ast.Inspect(node, func(n ast.Node) bool {
			if loop, ok := n.(*ast.RangeStmt); ok && isMessage(loop.X, tainted) {
				taint(loop.Key)
				taint(loop.Value)
				return true
			}
			lhs, rhs := assignment(n)
			if len(lhs) != len(rhs) {
				return true
			}
			for i, value := range rhs {
				if isMessage(value, tainted) {
					taint(lhs[i])
				}
			}
			return true
		})
	}
	return tainted
}

// isMessage reports whether an expression is text a component wrote for a
// person: a field .Message, .Logs or .LastStartError (an error string
// stored), an Error() call, a string conversion of one, a tainted local, or
// anything cut from one: an index, a slice, its len, or a + concatenation
// with a message on either side.
func isMessage(e ast.Expr, tainted map[string]bool) bool {
	switch e := ast.Unparen(e).(type) {
	case *ast.SelectorExpr:
		return e.Sel.Name == "Message" || e.Sel.Name == "Logs" || e.Sel.Name == "LastStartError"
	case *ast.IndexExpr:
		return isMessage(e.X, tainted)
	case *ast.SliceExpr:
		return isMessage(e.X, tainted)
	case *ast.BinaryExpr:
		return e.Op == token.ADD && (isMessage(e.X, tainted) || isMessage(e.Y, tainted))
	case *ast.CallExpr:
		if sel, ok := e.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Error" && len(e.Args) == 0 {
			return true
		}
		if id, ok := e.Fun.(*ast.Ident); ok && (id.Name == "string" || id.Name == "len") && len(e.Args) == 1 {
			return isMessage(e.Args[0], tainted)
		}
	case *ast.Ident:
		return tainted[e.Name]
	}
	return false
}

// finding builds a finding at a node's position.
func (p messagePackage) finding(n ast.Node, decl, rule, text string) messageFinding {
	pos := p.fset.Position(n.Pos())
	return messageFinding{pkg: p.name, decl: decl, file: pos.Filename, line: pos.Line, rule: rule, text: text}
}
