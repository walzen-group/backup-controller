package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"slices"
	"strings"
)

// decl is one top-level declaration's fingerprint.
type decl struct {
	// key is the declaration's line key, "<kind> <name>".
	key string
	// hash is the hex sha256 of the declaration's normalised text.
	hash string
}

// hasher fingerprints the declarations of one file.
type hasher struct {
	// pkg is the package the file belongs to.
	pkg *pkg
	// file is the file whose declarations are hashed.
	file pkgFile
	// renames is the rename map applied to the old side, nil otherwise.
	renames map[string]rename
	// into is the new side the rename map spells against, nil without a
	// rename map.
	into *target
}

// scan fingerprints the declarations of one package directory.
//
// Parameters:
//   - dir is the package directory.
//   - renames is the rename map to apply, nil for none. Each package-level
//     identifier it names is respelled the way the new code spells it
//     before the text is hashed, and the declaration of that identifier is
//     keyed under its new name.
//   - into is the new side the rename map spells against, nil for none.
//
// It returns the declarations sorted by key, or the error load reports or
// the first reference that resolves to nothing.
func scan(dir string, renames map[string]rename, into *target) ([]decl, error) {
	p, err := load(dir)
	if err != nil {
		return nil, err
	}
	var decls []decl
	for _, f := range p.files {
		h := hasher{pkg: p, file: f, renames: renames, into: into}
		for _, d := range f.ast.Decls {
			found, err := h.decl(d)
			if err != nil {
				return nil, err
			}
			decls = append(decls, found...)
		}
	}
	numberDuplicates(decls)
	slices.SortFunc(decls, func(a, b decl) int { return strings.Compare(a.key, b.key) })
	return decls, nil
}

// numberDuplicates makes every key unique.
//
// Parameters:
//   - decls are the declarations in the order the files and the
//     declarations within them come; their keys are changed in place.
//
// The second and later declarations under one key (several init functions,
// several blank variables) get a "#n" suffix, n counting from 2.
func numberDuplicates(decls []decl) {
	seen := map[string]int{}
	for i := range decls {
		seen[decls[i].key]++
		if n := seen[decls[i].key]; n > 1 {
			decls[i].key = fmt.Sprintf("%s#%d", decls[i].key, n)
		}
	}
}

// specNames returns the names a spec declares.
//
// Parameters:
//   - spec is one spec of a general declaration.
//
// It returns the type name of a TypeSpec, the names of a ValueSpec, and
// none for an ImportSpec.
func specNames(spec ast.Spec) []*ast.Ident {
	switch s := spec.(type) {
	case *ast.TypeSpec:
		return []*ast.Ident{s.Name}
	case *ast.ValueSpec:
		return s.Names
	default:
		return nil
	}
}

// decl fingerprints one top-level declaration.
//
// Parameters:
//   - d is a declaration of the hasher's file.
//
// It returns one fingerprint for a function, one per spec of a const, var
// or type declaration and none for an import, or the error of the first
// reference that resolves to nothing.
func (h hasher) decl(d ast.Decl) ([]decl, error) {
	switch d := d.(type) {
	case *ast.FuncDecl:
		return h.funcDecl(d)
	case *ast.GenDecl:
		return h.genDecl(d)
	default:
		return nil, fmt.Errorf("%s: unexpected declaration %T", h.pkg.fset.Position(d.Pos()), d)
	}
}

// funcDecl fingerprints a function or method.
//
// Parameters:
//   - d is the function declaration.
//
// It returns one fingerprint, keyed "func <name>" or "func <receiver
// type>.<name>", over the text from the doc comment to the closing brace,
// or the error of a reference that resolves to nothing.
func (h hasher) funcDecl(d *ast.FuncDecl) ([]decl, error) {
	start := d.Pos()
	if d.Doc != nil {
		start = d.Doc.Pos()
	}
	text, err := h.text(d, start, d.End())
	if err != nil {
		return nil, err
	}
	name := h.spell(d.Name.Name)
	if d.Recv != nil && len(d.Recv.List) > 0 {
		name = h.spell(receiverName(d.Recv.List[0].Type)) + "." + d.Name.Name
	}
	return []decl{{key: "func " + name, hash: digest(text)}}, nil
}

// spell returns the name a declaration is keyed under.
//
// Parameters:
//   - name is the declared name.
//
// It returns the rename map's new name when the map names the identifier,
// and the name itself otherwise.
func (h hasher) spell(name string) string {
	if r, ok := h.renames[name]; ok {
		return r.name
	}
	return name
}

// receiverName returns the type name of a method receiver.
//
// Parameters:
//   - expr is the receiver's type expression.
//
// It returns the name without a pointer and without type parameters, or
// the Go type of the expression when it holds no name.
func receiverName(expr ast.Expr) string {
	for {
		switch e := expr.(type) {
		case *ast.StarExpr:
			expr = e.X
		case *ast.ParenExpr:
			expr = e.X
		case *ast.IndexExpr:
			expr = e.X
		case *ast.IndexListExpr:
			expr = e.X
		case *ast.Ident:
			return e.Name
		default:
			return fmt.Sprintf("%T", expr)
		}
	}
}

// genDecl fingerprints each spec of a const, var or type declaration.
//
// Parameters:
//   - d is the general declaration.
//
// It returns one fingerprint per spec, keyed "<kind> <names>" with the
// names joined by commas, none for an import declaration, or the error of
// a reference that resolves to nothing. A const spec whose value depends on
// its place is hashed with its whole group (see placeDependent).
func (h hasher) genDecl(d *ast.GenDecl) ([]decl, error) {
	if d.Tok == token.IMPORT {
		return nil, nil
	}
	decls := make([]decl, 0, len(d.Specs))
	for _, spec := range d.Specs {
		var text string
		var err error
		if d.Tok == token.CONST && h.placeDependent(spec) {
			text, err = h.groupText(d)
		} else {
			text, err = h.specText(d, spec)
		}
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(specNames(spec)))
		for _, id := range specNames(spec) {
			names = append(names, h.spell(id.Name))
		}
		decls = append(decls, decl{key: d.Tok.String() + " " + strings.Join(names, ","), hash: digest(text)})
	}
	return decls, nil
}

// groupText returns the text of a whole declaration with its doc comment.
//
// Parameters:
//   - d is the general declaration.
//
// It returns the text, or the error of a reference that resolves to
// nothing.
func (h hasher) groupText(d *ast.GenDecl) (string, error) {
	start := d.Pos()
	if d.Doc != nil {
		start = d.Doc.Pos()
	}
	return h.text(d, start, d.End())
}

// specText returns the text of one spec on its own.
//
// Parameters:
//   - d is the general declaration the spec belongs to.
//   - spec is the spec.
//
// It returns the declaration's doc comment, the spec's own doc comment, the
// spec without the indentation its group adds, and its line comment; or
// the error of a reference that resolves to nothing.
//
// The parser gives the doc comment of a declaration without parentheses to
// the declaration, so "// Doc\nconst X = 1" and "// Doc\nconst (\n\tX =
// 1\n)" hash alike, and so does a spec whose doc moved from the group onto
// itself when the spec left the group.
func (h hasher) specText(d *ast.GenDecl, spec ast.Spec) (string, error) {
	var doc, comment *ast.CommentGroup
	switch s := spec.(type) {
	case *ast.TypeSpec:
		doc, comment = s.Doc, s.Comment
	case *ast.ValueSpec:
		doc, comment = s.Doc, s.Comment
	}
	body, err := h.specBody(spec)
	if err != nil {
		return "", err
	}
	var b strings.Builder
	for _, group := range []*ast.CommentGroup{d.Doc, doc} {
		if group == nil || (group == doc && doc == d.Doc) {
			continue
		}
		for _, c := range group.List {
			b.WriteString(c.Text + "\n")
		}
	}
	b.WriteString(body + "\n")
	if comment != nil {
		for _, c := range comment.List {
			b.WriteString(c.Text + "\n")
		}
	}
	return b.String(), nil
}

// placeDependent reports whether a const spec's value depends on its place
// in its group.
//
// Parameters:
//   - spec is a spec of a const declaration.
//
// It returns true when the spec omits its expression, or when an
// identifier named iota in its expression resolves to the universe's iota
// or to nothing.
//
// The test reads the syntax, and it fails closed: the checker does not
// evaluate the value of a const whose type it cannot resolve, such as one
// from an empty import, so it records no use of iota there. Only an iota
// that resolves to a declaration of the package or of the function around
// it counts as an ordinary name.
func (h hasher) placeDependent(spec ast.Spec) bool {
	s, ok := spec.(*ast.ValueSpec)
	if !ok {
		return false
	}
	if len(s.Values) == 0 {
		return true
	}
	universeIota := types.Universe.Lookup("iota")
	uses := false
	for _, v := range s.Values {
		ast.Inspect(v, func(n ast.Node) bool {
			if id, ok := n.(*ast.Ident); ok && id.Name == "iota" {
				obj := h.object(id)
				uses = uses || obj == nil || obj == universeIota
			}
			return !uses
		})
	}
	return uses
}

// digest returns the hex sha256 of a text.
//
// Parameters:
//   - text is the normalised text of a declaration.
func digest(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}
