package main

import (
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"slices"
	"strings"
)

// pkgFile is one parsed file of a package.
type pkgFile struct {
	// name is the file's path.
	name string
	// ast is the file's syntax tree, with comments.
	ast *ast.File
	// src is the file's source, which the hash reads its text from.
	src []byte
}

// pkg is one parsed and type-checked package directory.
type pkg struct {
	fset  *token.FileSet
	files []pkgFile
	// declared maps an unaliased import path to the name its package
	// declares (see listNames).
	declared map[string]string
	// listErr is why go list failed, nil when it ran. A name it could not
	// supply falls back to the last element of the import path.
	listErr error
	types   *types.Package
	info    *types.Info
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

// load reads and type-checks one package directory.
//
// Parameters:
//   - dir is the package directory.
//
// It returns the package, or an error when a file cannot be read or parsed
// or the directory holds no Go file.
//
// It parses every non-test .go file in file name order, asks go list for
// the names of the imported packages, and type-checks the files against
// empty imported packages (see the package comment).
func load(dir string) (*pkg, error) {
	p := &pkg{fset: token.NewFileSet()}
	if err := p.parse(dir); err != nil {
		return nil, err
	}
	p.declared, p.listErr = listNames(dir, p.files)
	p.check()
	return p, nil
}

// parse reads and parses the non-test .go files of a directory into the
// package.
//
// Parameters:
//   - dir is the package directory.
//
// It returns an error when a file cannot be read or parsed, or when there
// is no file to read.
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

// fileImports maps each package name a file imports to its path.
//
// Parameters:
//   - f is one of the package's files.
//
// It returns the map. A name is the alias when an import has one, otherwise
// the name the package declares (see listNames), otherwise the path's last
// element. Blank and dot imports name nothing and are left out.
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

// packageImports maps each package name the package's files import to its
// path.
//
// Parameters:
//   - dir is the package directory, named in the error.
//
// It returns the map, or an error when one name stands for two paths in
// different files, since a renamed identifier could not be spelled against
// that name.
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

// listNames asks go list for the package name each unaliased import
// declares.
//
// Parameters:
//   - dir is the package directory, where go list runs so that the
//     directory's module resolves the paths.
//   - files are the package's parsed files, whose imports are listed.
//
// It returns the map from import path to declared name, and the reason go
// list failed, nil when it ran. A path go list cannot resolve is left out
// of the map, and so is every path when go list failed; its name then falls
// back to the path's last element, and the error lets a later failure to
// resolve that name say why.
func listNames(dir string, files []pkgFile) (map[string]string, error) {
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
		return declared, nil
	}
	cmd := exec.Command("go", append([]string{"list", "-e", "-f", "{{.ImportPath}} {{.Name}}"}, paths...)...)
	cmd.Dir = dir
	out, err := cmd.Output()
	if err != nil {
		var exit *exec.ExitError
		if errors.As(err, &exit) && len(exit.Stderr) > 0 {
			err = fmt.Errorf("%w: %s", err, strings.TrimSpace(string(exit.Stderr)))
		}
		return declared, fmt.Errorf("go list in %s: %w", dir, err)
	}
	for line := range strings.SplitSeq(string(out), "\n") {
		if importPath, name, ok := strings.Cut(line, " "); ok && name != "" {
			declared[importPath] = name
		}
	}
	return declared, nil
}

// emptyImporter hands the type checker an empty, complete package for each
// import, so names resolve without loading dependencies.
type emptyImporter struct {
	// declared maps an import path to the name its package declares.
	declared map[string]string
	// loaded holds the package already handed out for each path.
	loaded map[string]*types.Package
}

// Import returns the empty package for an import path.
//
// Parameters:
//   - importPath is the path an import declaration names.
//
// It returns the same package on every call for one path, named as
// declared says or after the path's last element, and never an error.
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

// check type-checks the package's files and records every identifier's
// object in the package's info.
//
// The type errors the empty imports cause are expected and ignored; the
// checker still returns the package and its scopes.
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

// readTarget reads what the rename map needs to know about the new side.
//
// Parameters:
//   - dir is the new package directory.
//
// It returns the directory's package name and the package names its files
// import, or the error load or packageImports reports.
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
