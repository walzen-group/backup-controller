package runs

import (
	"go/ast"
	"os"
	"strings"
	"testing"
)

// The helpers here serve both rule tests, TestNoDecisionReadsAMessage and
// TestEveryItemReasonIsAnAPIConstant: each keys its lists by top-level
// declaration and checks its own fixture's marked lines.

// fixtureWants reads the fixture's "// want <rule>" comments and returns
// the rule each marked line must break, by line number.
func fixtureWants(t *testing.T, path string) map[int]string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int]string{}
	for i, line := range strings.Split(string(raw), "\n") {
		if _, rule, ok := strings.Cut(line, "// want "); ok {
			want[i+1] = strings.TrimSpace(rule)
		}
	}
	if len(want) == 0 {
		t.Fatalf("%s marks no line", path)
	}
	return want
}

// namedNode is a top-level declaration, or one spec of it, with the key the
// lists use.
type namedNode struct {
	name string
	node ast.Node
}

// declNames splits a top-level declaration into what the lists name: a
// function, a method as Type.name, or each spec of a var, const or type
// declaration by its first name.
func declNames(decl ast.Decl) []namedNode {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		name := d.Name.Name
		if d.Recv != nil && len(d.Recv.List) == 1 {
			name = receiverType(d.Recv.List[0].Type) + "." + name
		}
		return []namedNode{{name, d}}
	case *ast.GenDecl:
		var nodes []namedNode
		for _, spec := range d.Specs {
			switch s := spec.(type) {
			case *ast.ValueSpec:
				nodes = append(nodes, namedNode{s.Names[0].Name, s})
			case *ast.TypeSpec:
				nodes = append(nodes, namedNode{s.Name.Name, s})
			}
		}
		return nodes
	}
	return nil
}

// receiverType returns the name of a method's receiver type, without a
// pointer or type parameters.
func receiverType(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.StarExpr:
		return receiverType(t.X)
	case *ast.IndexExpr:
		return receiverType(t.X)
	case *ast.IndexListExpr:
		return receiverType(t.X)
	case *ast.Ident:
		return t.Name
	}
	return "?"
}
