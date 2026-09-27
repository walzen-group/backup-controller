package main

import (
	"cmp"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
)

// edit replaces a range of a file's source bytes before a text is hashed.
type edit struct {
	// start and end are the byte offsets of the range [start, end).
	start, end int
	// text replaces the range.
	text string
}

// text returns a range of the file's source with its references
// normalised.
//
// Parameters:
//   - node is the syntax whose references are normalised; it lies inside
//     the range.
//   - start and end delimit the range [start, end), which also covers the
//     doc comment before the node.
//
// It returns the text, or the error of a reference that resolves to
// nothing. Each imported package name in the node is replaced with its
// import path, each renamed identifier is respelled (see edits), and the
// rename map is applied to the comments in the range (see commentEdits).
func (h hasher) text(node ast.Node, start, end token.Pos) (string, error) {
	edits, err := h.edits(node)
	if err != nil {
		return "", err
	}
	edits = append(edits, h.commentEdits(start, end)...)
	return h.splice(start, end, edits), nil
}

// specBody returns the text of one spec without the indentation its group
// adds.
//
// Parameters:
//   - spec is a spec of a general declaration.
//
// It returns the text from the spec's start to its end, normalised as text
// does, with the leading blanks of every line after the first removed,
// except on a line that starts inside a raw string literal, whose bytes are
// the string's value. It returns the error of a reference that resolves to
// nothing.
func (h hasher) specBody(spec ast.Spec) (string, error) {
	edits, err := h.edits(spec)
	if err != nil {
		return "", err
	}
	edits = append(edits, h.indentEdits(spec)...)
	edits = append(edits, h.commentEdits(spec.Pos(), spec.End())...)
	return h.splice(spec.Pos(), spec.End(), edits), nil
}

// indentEdits returns the edits that remove the indentation from the lines
// of a spec.
//
// Parameters:
//   - spec is a spec of a general declaration.
//
// It returns one edit per line after the first that starts with blanks
// outside a raw string literal. The edits never overlap the ones edits
// returns, which replace identifiers.
func (h hasher) indentEdits(spec ast.Spec) []edit {
	lo, hi := h.offset(spec.Pos()), h.offset(spec.End())
	var raw [][2]int
	ast.Inspect(spec, func(n ast.Node) bool {
		if lit, ok := n.(*ast.BasicLit); ok && lit.Kind == token.STRING && strings.HasPrefix(lit.Value, "`") {
			raw = append(raw, [2]int{h.offset(lit.Pos()), h.offset(lit.End())})
		}
		return true
	})
	inRaw := func(at int) bool {
		return slices.ContainsFunc(raw, func(r [2]int) bool { return r[0] < at && at < r[1] })
	}
	var edits []edit
	for at := lo; at < hi; at++ {
		if h.file.src[at] != '\n' || inRaw(at+1) {
			continue
		}
		end := at + 1
		for end < hi && (h.file.src[end] == ' ' || h.file.src[end] == '\t') {
			end++
		}
		if end > at+1 {
			edits = append(edits, edit{start: at + 1, end: end})
		}
	}
	return edits
}

// splice returns a range of the file's source with edits applied.
//
// Parameters:
//   - start and end delimit the range [start, end).
//   - edits are non-overlapping replacements inside the range, in any
//     order.
func (h hasher) splice(start, end token.Pos, edits []edit) string {
	slices.SortFunc(edits, func(a, b edit) int { return cmp.Compare(a.start, b.start) })
	var b strings.Builder
	at := h.offset(start)
	for _, e := range edits {
		b.Write(h.file.src[at:e.start])
		b.WriteString(e.text)
		at = e.end
	}
	b.Write(h.file.src[at:h.offset(end)])
	return b.String()
}

// offset returns the byte offset of a position in the hasher's file.
//
// Parameters:
//   - pos is a position inside the file.
func (h hasher) offset(pos token.Pos) int {
	return int(pos) - h.pkg.fset.File(pos).Base()
}

// edits collects the replacements that normalise the references in a
// node.
//
// Parameters:
//   - node is the syntax to walk.
//
// It returns an edit per package name on the left of a selector, which
// becomes "⟨import path⟩", and per package-level identifier the rename map
// names, which becomes its new spelling. It returns the error of the first
// selector whose left side resolves to nothing.
func (h hasher) edits(node ast.Node) ([]edit, error) {
	var edits []edit
	var failure error
	selected := map[*ast.Ident]bool{}
	ast.Inspect(node, func(n ast.Node) bool {
		if failure != nil {
			return false
		}
		switch n := n.(type) {
		case *ast.SelectorExpr:
			selected[n.Sel] = true
			e, ok, err := h.selectorEdit(n)
			if ok {
				edits = append(edits, e)
			}
			failure = err
		case *ast.Ident:
			if e, ok := h.renamed(n, selected[n]); ok {
				edits = append(edits, e)
			}
		}
		return failure == nil
	})
	return edits, failure
}

// selectorEdit returns the edit for the left side of a selector.
//
// Parameters:
//   - sel is the selector expression.
//
// It returns the edit and true when the left side is an identifier that
// names an imported package, false when it is anything else, and an error
// when it is an identifier that resolves to nothing.
func (h hasher) selectorEdit(sel *ast.SelectorExpr) (edit, bool, error) {
	x, ok := sel.X.(*ast.Ident)
	if !ok {
		return edit{}, false, nil
	}
	return h.qualifier(x)
}

// object returns what an identifier refers to.
//
// Parameters:
//   - id is an identifier of the hasher's file.
//
// It returns the checker's object for it, or the object of that name in
// the scope around it, or nil when nothing of that name is in scope.
//
// The checker skips an expression whose operand has no valid type, which
// an empty import leaves behind (the value in a map literal whose key is a
// name from an import, a composite literal of an imported type), so an
// identifier there has no recorded object. The scope around it holds the parameters,
// the locals declared before it, the file's imports and the package's
// declarations.
func (h hasher) object(id *ast.Ident) types.Object {
	if obj := h.pkg.info.Uses[id]; obj != nil {
		return obj
	}
	if obj := h.pkg.info.Defs[id]; obj != nil {
		return obj
	}
	scope := h.pkg.types.Scope().Innermost(id.Pos())
	if scope == nil {
		return nil
	}
	_, obj := scope.LookupParent(id.Name, id.Pos())
	return obj
}

// qualifier returns the edit for the identifier on the left of a selector.
//
// Parameters:
//   - x is the identifier.
//
// It returns the edit that replaces it with "⟨import path⟩" and true when
// it names an imported package, false for any other object, and an error
// when it resolves to nothing. The error says when go list failed, since
// the name of an import then falls back to its path's last element.
func (h hasher) qualifier(x *ast.Ident) (edit, bool, error) {
	switch obj := h.object(x).(type) {
	case *types.PkgName:
		return h.span(x, "⟨"+obj.Imported().Path()+"⟩"), true, nil
	case nil:
		err := fmt.Errorf("%s: %s resolves to no declaration or import", h.pkg.fset.Position(x.Pos()), x.Name)
		if h.pkg.listErr != nil {
			err = fmt.Errorf("%w; imports are named after their path's last element because %w", err, h.pkg.listErr)
		}
		return edit{}, false, err
	default:
		return edit{}, false, nil
	}
}

// renamed returns the edit that respells an identifier the rename map
// names.
//
// Parameters:
//   - id is an identifier of the hasher's file.
//   - selected is true when the identifier is the right side of a selector:
//     a field or method name, which keeps its name like a parameter or a
//     local does.
//
// It returns the edit and true when the identifier refers to a
// package-level declaration the map names, and false otherwise. The new
// spelling is New when the identifier moves into the target package or
// stays in its own, "⟨path⟩.New" when the target reaches it through an
// import, and "⟨pkg?⟩.New", which matches no new declaration, when the
// target does not import its package.
func (h hasher) renamed(id *ast.Ident, selected bool) (edit, bool) {
	r, ok := h.renames[id.Name]
	if !ok || selected {
		return edit{}, false
	}
	if obj := h.object(id); obj == nil || obj.Parent() != h.pkg.types.Scope() {
		return edit{}, false
	}
	if r.pkg == "" || h.into == nil || r.pkg == h.into.name {
		return h.span(id, r.name), true
	}
	if p, ok := h.into.imports[r.pkg]; ok {
		return h.span(id, "⟨"+p+"⟩."+r.name), true
	}
	return h.span(id, "⟨"+r.pkg+"?⟩."+r.name), true
}

// span returns the edit that replaces an identifier's bytes.
//
// Parameters:
//   - id is the identifier.
//   - text replaces it.
func (h hasher) span(id *ast.Ident, text string) edit {
	return edit{start: h.offset(id.Pos()), end: h.offset(id.End()), text: text}
}
