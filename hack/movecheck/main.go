// Command movecheck fingerprints every top-level declaration of a Go package
// directory, so a step that only moves code can show that it changed none.
//
// Usage:
//
//	movecheck DIR
//	movecheck [-rename old=pkg.New,...] OLDDIR NEWDIR
//
// With one directory it prints one line per declaration, "<kind> <name>
// <sha256>", sorted. The kind is func, type, const or var; the name of a
// method is "<receiver type>.<name>". The file a declaration sits in is not
// part of its line, so the output before and after a move between the files
// of one package is identical when nothing else changed.
//
// With two directories it compares the declarations found in both, after
// applying the rename map to the old side, and prints each one whose hash
// differs. A step that moves declarations into another package uses it: the
// old directory holds the package before the move, the new one the package
// that received them. Declarations found on one side only are counted on
// standard error and not compared.
//
// The hash of a declaration covers:
//   - its source text, with its doc comment and the comments inside it;
//   - the import path of every package a selector in it names: the package
//     name is replaced by the path it stands for, so a declaration whose
//     text is unchanged but whose file imports another path under the same
//     name hashes differently;
//   - for a const, var or type group, each spec on its own with its doc and
//     line comment, so a group split across files changes no hash. A const
//     spec whose value depends on its place in the group (it uses iota, or
//     omits its expression) is hashed with the text of the whole group,
//     since splitting the group changes its value.
//
// Names are resolved with go/types over the package's files. Imported
// packages are not loaded: each import stands for an empty package under
// the name `go list`, run in the directory, reports for it (the path's last
// element when go list cannot resolve it, as for a fixture's made-up path).
// The type errors that leaves are ignored. An identifier the checker
// skipped because of them is looked up by name in the scope around it (see
// hasher.object). A selector whose left side resolves to nothing is an
// error, so the tool never hashes a reference it could not resolve.
//
// Not covered: the order in which package variables are initialised across
// files and the order of init functions, both of which follow the file
// order; build constraints (every non-test .go file is read); test files,
// which are skipped.
//
// The exit status is 0 when the run succeeds and, with two directories, every
// compared declaration hashes the same; 1 when a compared declaration
// differs; 2 on a usage or parse error.
package main

import (
	"cmp"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// decl is one top-level declaration: its line key ("<kind> <name>") and the
// hash of its normalised text.
type decl struct {
	key  string
	hash string
}

// rename is one entry of the rename map: an old package-level identifier
// and what it becomes. pkg is the name of the package the identifier moves
// to, empty when it stays in the package it is declared in.
type rename struct {
	pkg  string
	name string
}

// target describes the package the old side is compared against, which the
// rename map needs to spell a renamed identifier the way the new code does.
type target struct {
	// name is the new directory's package name. A rename into this package
	// is spelled unqualified.
	name string
	// imports maps each package name the new directory imports to its path.
	// A rename into another package is spelled with that path.
	imports map[string]string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses the arguments, fingerprints the directories they name, and
// prints the result.
//
// Parameters:
//   - args are the command-line arguments without the program name.
//   - stdout receives the declaration lines or the differences.
//   - stderr receives errors and the counts of one-sided declarations.
//
// It returns the exit status described in the package comment.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("movecheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	renameFlag := flags.String("rename", "", "comma-separated old=pkg.New or old=New entries applied to the old side")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	renames, err := parseRenames(*renameFlag)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	switch flags.NArg() {
	case 1:
		return printOne(flags.Arg(0), renames, stdout, stderr)
	case 2:
		return compare(flags.Arg(0), flags.Arg(1), renames, stdout, stderr)
	default:
		_, _ = fmt.Fprintln(stderr, "usage: movecheck DIR | movecheck [-rename old=pkg.New,...] OLDDIR NEWDIR")
		return 2
	}
}

// printOne prints the sorted declaration lines of dir. A rename map without
// a second directory has nothing to spell a package-qualified name against,
// so it is refused.
func printOne(dir string, renames map[string]rename, stdout, stderr io.Writer) int {
	if len(renames) > 0 {
		_, _ = fmt.Fprintln(stderr, "-rename needs an old and a new directory")
		return 2
	}
	decls, err := scan(dir, nil, nil)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	for _, d := range decls {
		_, _ = fmt.Fprintf(stdout, "%s %s\n", d.key, d.hash)
	}
	return 0
}

// compare hashes oldDir with the rename map applied and newDir as it is,
// and prints each declaration present in both whose hash differs.
func compare(oldDir, newDir string, renames map[string]rename, stdout, stderr io.Writer) int {
	into, err := readTarget(newDir)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	oldDecls, err := scan(oldDir, renames, into)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	newDecls, err := scan(newDir, nil, nil)
	if err != nil {
		_, _ = fmt.Fprintln(stderr, err)
		return 2
	}
	newHash := make(map[string]string, len(newDecls))
	for _, d := range newDecls {
		newHash[d.key] = d.hash
	}
	status, same, oldOnly := 0, 0, 0
	for _, d := range oldDecls {
		h, ok := newHash[d.key]
		delete(newHash, d.key)
		switch {
		case !ok:
			oldOnly++
		case h == d.hash:
			same++
		default:
			_, _ = fmt.Fprintf(stdout, "changed: %s\n", d.key)
			status = 1
		}
	}
	_, _ = fmt.Fprintf(stderr, "%d identical, %d only in %s, %d only in %s\n", same, oldOnly, oldDir, len(newHash), newDir)
	return status
}

// parseRenames reads the -rename value: comma-separated entries of the form
// old=pkg.New (the identifier moves to package pkg as New) or old=New (it
// is renamed within its package). An empty value is an empty map.
func parseRenames(value string) (map[string]rename, error) {
	renames := map[string]rename{}
	if value == "" {
		return renames, nil
	}
	for entry := range strings.SplitSeq(value, ",") {
		old, to, ok := strings.Cut(entry, "=")
		if !ok || !token.IsIdentifier(old) {
			return nil, fmt.Errorf("rename %q: want old=pkg.New or old=New", entry)
		}
		pkg, name, qualified := strings.Cut(to, ".")
		if !qualified {
			pkg, name = "", to
		}
		if !token.IsIdentifier(name) || (qualified && !token.IsIdentifier(pkg)) {
			return nil, fmt.Errorf("rename %q: want old=pkg.New or old=New", entry)
		}
		if _, dup := renames[old]; dup {
			return nil, fmt.Errorf("rename %q: %s is renamed twice", entry, old)
		}
		renames[old] = rename{pkg: pkg, name: name}
	}
	return renames, nil
}

// pkgFile is one parsed file of the package: its path, syntax tree and
// source.
type pkgFile struct {
	name string
	ast  *ast.File
	src  []byte
}

// pkg is one parsed and type-checked package directory.
type pkg struct {
	fset  *token.FileSet
	files []pkgFile
	// declared maps an unaliased import path to the name its package
	// declares (see listNames).
	declared map[string]string
	types    *types.Package
	info     *types.Info
}

// load parses every non-test .go file of dir, in file name order, and
// type-checks them against empty imported packages (see the package
// comment).
func load(dir string) (*pkg, error) {
	p := &pkg{fset: token.NewFileSet()}
	if err := p.parse(dir); err != nil {
		return nil, err
	}
	p.declared = listNames(dir, p.files)
	p.check()
	return p, nil
}

// parse reads and parses the non-test .go files of dir.
func (p *pkg) parse(dir string) error {
	names, err := filepath.Glob(filepath.Join(dir, "*.go"))
	if err != nil {
		return err
	}
	slices.Sort(names)
	for _, name := range names {
		if strings.HasSuffix(name, "_test.go") {
			continue
		}
		src, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		f, err := parser.ParseFile(p.fset, name, src, parser.ParseComments|parser.SkipObjectResolution)
		if err != nil {
			return err
		}
		p.files = append(p.files, pkgFile{name: name, ast: f, src: src})
	}
	if len(p.files) == 0 {
		return fmt.Errorf("%s: no Go files", dir)
	}
	return nil
}

// fileImports maps each package name f imports to its path: the alias when
// an import has one, otherwise the name the package declares (see
// listNames), otherwise the path's last element. Blank and dot imports name
// nothing.
func (p *pkg) fileImports(f pkgFile) map[string]string {
	names := map[string]string{}
	for _, spec := range f.ast.Imports {
		importPath := strings.Trim(spec.Path.Value, "`\"")
		switch {
		case spec.Name != nil && (spec.Name.Name == "_" || spec.Name.Name == "."):
		case spec.Name != nil:
			names[spec.Name.Name] = importPath
		case p.declared[importPath] != "":
			names[p.declared[importPath]] = importPath
		default:
			names[path.Base(importPath)] = importPath
		}
	}
	return names
}

// packageImports maps each package name the files of dir import to its
// path. One name standing for two paths in different files is an error,
// since a renamed identifier could not be spelled against it.
func (p *pkg) packageImports(dir string) (map[string]string, error) {
	all := map[string]string{}
	for _, f := range p.files {
		for name, importPath := range p.fileImports(f) {
			if other, ok := all[name]; ok && other != importPath {
				return nil, fmt.Errorf("%s: package name %s stands for both %s and %s", dir, name, other, importPath)
			}
			all[name] = importPath
		}
	}
	return all, nil
}

// listNames asks `go list`, run in dir, for the package name each unaliased
// import of files declares. A path go list cannot resolve is left out.
func listNames(dir string, files []pkgFile) map[string]string {
	var paths []string
	for _, f := range files {
		for _, spec := range f.ast.Imports {
			if spec.Name == nil {
				paths = append(paths, strings.Trim(spec.Path.Value, "`\""))
			}
		}
	}
	declared := map[string]string{}
	if len(paths) == 0 {
		return declared
	}
	cmd := exec.Command("go", append([]string{"list", "-e", "-f", "{{.ImportPath}} {{.Name}}"}, paths...)...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		return declared
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		if importPath, name, ok := strings.Cut(line, " "); ok && name != "" {
			declared[importPath] = name
		}
	}
	return declared
}

// emptyImporter hands the type checker an empty, complete package for each
// import, named as declared says, so names resolve without loading
// dependencies.
type emptyImporter struct {
	declared map[string]string
	loaded   map[string]*types.Package
}

// Import returns the empty package for importPath, the same one on every
// call.
func (e emptyImporter) Import(importPath string) (*types.Package, error) {
	if p, ok := e.loaded[importPath]; ok {
		return p, nil
	}
	name := e.declared[importPath]
	if name == "" {
		name = path.Base(importPath)
	}
	p := types.NewPackage(importPath, name)
	p.MarkComplete()
	e.loaded[importPath] = p
	return p, nil
}

// check type-checks the files and records every identifier's object. The
// type errors empty imports cause are expected and ignored.
func (p *pkg) check() {
	asts := make([]*ast.File, 0, len(p.files))
	for _, f := range p.files {
		asts = append(asts, f.ast)
	}
	conf := types.Config{
		Importer: emptyImporter{declared: p.declared, loaded: map[string]*types.Package{}},
		Error:    func(error) {},
	}
	p.info = &types.Info{Defs: map[*ast.Ident]types.Object{}, Uses: map[*ast.Ident]types.Object{}}
	p.types, _ = conf.Check(p.files[0].ast.Name.Name, p.fset, asts, p.info)
}

// readTarget reads the package name of dir and the package names its files
// import, for spelling renamed identifiers.
func readTarget(dir string) (*target, error) {
	p, err := load(dir)
	if err != nil {
		return nil, err
	}
	imports, err := p.packageImports(dir)
	if err != nil {
		return nil, err
	}
	return &target{name: p.files[0].ast.Name.Name, imports: imports}, nil
}

// scan fingerprints the declarations of dir. With a rename map and a
// target, each package-level identifier the map names is respelled first,
// the way the new code spells it. It returns the declarations sorted by key.
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

// numberDuplicates gives each repeated key (several init functions, several
// blank variables) a "#n" suffix in the order the files and declarations
// come, so every key is unique.
func numberDuplicates(decls []decl) {
	seen := map[string]int{}
	for i := range decls {
		seen[decls[i].key]++
		if n := seen[decls[i].key]; n > 1 {
			decls[i].key = fmt.Sprintf("%s#%d", decls[i].key, n)
		}
	}
}

// specNames returns the names a spec declares: the type name of a TypeSpec,
// the names of a ValueSpec, and none for an ImportSpec.
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

// hasher fingerprints the declarations of one file.
type hasher struct {
	pkg     *pkg
	file    pkgFile
	renames map[string]rename
	into    *target
}

// edit replaces the source bytes [start, end) with text.
type edit struct {
	start, end int
	text       string
}

// decl returns the fingerprints of one top-level declaration: one for a
// function, one per spec of a const, var or type declaration, none for an
// import.
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

// funcDecl fingerprints a function or method with its doc comment.
func (h hasher) funcDecl(d *ast.FuncDecl) ([]decl, error) {
	start := d.Pos()
	if d.Doc != nil {
		start = d.Doc.Pos()
	}
	sum, err := h.hash(d, start, d.End())
	if err != nil {
		return nil, err
	}
	name := h.spell(d.Name.Name)
	if d.Recv != nil && len(d.Recv.List) > 0 {
		name = h.spell(receiverName(d.Recv.List[0].Type)) + "." + d.Name.Name
	}
	return []decl{{key: "func " + name, hash: sum}}, nil
}

// spell returns the name a declaration of the old side is keyed under: the
// rename map's new name when it names the identifier, and name otherwise.
func (h hasher) spell(name string) string {
	if r, ok := h.renames[name]; ok {
		return r.name
	}
	return name
}

// receiverName returns the type name of a method receiver, without a
// pointer and without type parameters.
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

// genDecl fingerprints each spec of a const, var or type declaration, and
// skips an import declaration.
func (h hasher) genDecl(d *ast.GenDecl) ([]decl, error) {
	if d.Tok == token.IMPORT {
		return nil, nil
	}
	decls := make([]decl, 0, len(d.Specs))
	for _, spec := range d.Specs {
		node := ast.Node(spec)
		start, end := specRange(d, spec)
		if d.Tok == token.CONST && h.placeDependent(spec) {
			node, start, end = d, d.Pos(), d.End()
			if d.Doc != nil {
				start = d.Doc.Pos()
			}
		}
		sum, err := h.hash(node, start, end)
		if err != nil {
			return nil, err
		}
		names := make([]string, 0, len(specNames(spec)))
		for _, id := range specNames(spec) {
			names = append(names, h.spell(id.Name))
		}
		decls = append(decls, decl{key: d.Tok.String() + " " + strings.Join(names, ","), hash: sum})
	}
	return decls, nil
}

// specRange returns the source range of one spec with its doc and line
// comment. The doc of a declaration without parentheses is the declaration's
// own doc, so "const X = 1" and the same spec inside a group hash alike.
func specRange(d *ast.GenDecl, spec ast.Spec) (token.Pos, token.Pos) {
	start, end := spec.Pos(), spec.End()
	var doc, comment *ast.CommentGroup
	switch s := spec.(type) {
	case *ast.TypeSpec:
		doc, comment = s.Doc, s.Comment
	case *ast.ValueSpec:
		doc, comment = s.Doc, s.Comment
	}
	if doc == nil && !d.Lparen.IsValid() {
		doc = d.Doc
	}
	if doc != nil {
		start = doc.Pos()
	}
	if comment != nil {
		end = comment.End()
	}
	return start, end
}

// placeDependent reports whether a const spec's value depends on its place
// in its group: it omits its expression, or its expression uses iota.
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
			if id, ok := n.(*ast.Ident); ok && h.pkg.info.Uses[id] == universeIota {
				uses = true
			}
			return !uses
		})
	}
	return uses
}

// hash returns the sha256 of the source range [start, end) after replacing
// each imported package name in node with its import path and respelling
// each renamed identifier.
func (h hasher) hash(node ast.Node, start, end token.Pos) (string, error) {
	edits, err := h.edits(node)
	if err != nil {
		return "", err
	}
	base := h.pkg.fset.File(start).Base()
	lo, hi := int(start)-base, int(end)-base
	slices.SortFunc(edits, func(a, b edit) int { return cmp.Compare(a.start, b.start) })
	var b strings.Builder
	at := lo
	for _, e := range edits {
		b.Write(h.file.src[at:e.start])
		b.WriteString(e.text)
		at = e.end
	}
	b.Write(h.file.src[at:hi])
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:]), nil
}

// edits collects the replacements for node: each package name of a
// selector becomes "⟨import path⟩", and each package-level identifier the
// rename map names becomes its new spelling.
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
			if x, ok := n.X.(*ast.Ident); ok {
				e, handled, err := h.qualifier(x)
				if err != nil {
					failure = err
					return false
				}
				if handled {
					edits = append(edits, e)
				}
			}
		case *ast.Ident:
			if e, ok := h.renamed(n, selected[n]); ok {
				edits = append(edits, e)
			}
		}
		return true
	})
	return edits, failure
}

// object returns what id refers to. The checker skips an expression whose
// operand has no valid type, which an empty import leaves behind (a type
// assertion on a value of an imported type, a composite literal of one), so
// an identifier there has no recorded object. It is looked up by name in
// the scope around it, which holds the parameters, the locals declared
// before it, the file's imports and the package's declarations. It returns
// nil when nothing of that name is in scope.
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

// qualifier returns the edit for x, the left side of a selector, when x
// names an imported package. It reports handled false for any other
// object, and an error when x resolves to nothing.
func (h hasher) qualifier(x *ast.Ident) (edit, bool, error) {
	switch obj := h.object(x).(type) {
	case *types.PkgName:
		return h.span(x, "⟨"+obj.Imported().Path()+"⟩"), true, nil
	case nil:
		return edit{}, false, fmt.Errorf("%s: %s resolves to no declaration or import", h.pkg.fset.Position(x.Pos()), x.Name)
	default:
		return edit{}, false, nil
	}
}

// renamed returns the edit for id when id refers to a package-level
// declaration the rename map names: New when it moves into the target
// package or stays in its own, and "⟨path⟩.New" when the target reaches it
// through an import. selected is true when id is the right side of a
// selector, a field or method name, which keeps its name like a parameter
// or local does. A package the target does not import is spelled
// "⟨pkg?⟩", which matches no new declaration.
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

// span returns the edit that replaces id's bytes with text.
func (h hasher) span(id *ast.Ident, text string) edit {
	base := h.pkg.fset.File(id.Pos()).Base()
	return edit{start: int(id.Pos()) - base, end: int(id.End()) - base, text: text}
}
