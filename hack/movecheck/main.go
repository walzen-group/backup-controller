// Command movecheck fingerprints every top-level declaration of a Go package
// directory, so a step that only moves code can show that it changed none.
//
// Usage:
//
//	movecheck DIR
//	movecheck [-rename old=pkg.New,...] [-new Name,...] OLDDIR NEWDIR
//
// With one directory it prints one line per declaration, "<kind> <name>
// <sha256>", sorted. The kind is func, type, const or var; the name of a
// method is "<receiver type>.<name>". The file a declaration sits in is not
// part of its line, so the output before and after a move between the files
// of one package is identical when nothing else changed.
//
// With two directories it compares the declarations found in both, after
// applying the rename map to the old side, and prints each one whose hash
// differs. The rename map applies to the code and to the comments of the
// old side (see renameWords). So a doc comment that names the declaration
// it documents does not make a moved declaration differ.
//
// A step that moves declarations into another package uses it: the old
// directory holds the package before the move, the new one the package
// that received them. Declarations found only on the old side are counted
// on standard error, since the old side of a move out of a package holds
// much that stays there. Every declaration found only on the new side is
// printed and fails the run, because it either came from the old side under
// a name the rename map misses or is new; the -new flag lists the names of
// the declarations the step adds on purpose. A -new entry that names no
// declaration found only on the new side also fails the run.
//
// The hash of a declaration covers:
//   - its source text, with its doc comment and the comments inside it;
//   - the import path of every package a selector in it names: the package
//     name is replaced by the path it stands for, so a declaration whose
//     text is unchanged but whose file imports another path under the same
//     name hashes differently;
//   - for a const, var or type declaration, each spec on its own with the
//     doc comment of its parenthesised group, its own doc and line comment,
//     and without the indentation the group adds, so a group split across
//     files, or a spec taken out of its group with the group's doc, changes
//     no hash. The lines inside a raw string literal keep every byte. A
//     const spec whose value depends on its place in the group is hashed
//     with the text of the whole group, since splitting the group changes
//     its value: it omits its expression, or its expression holds an
//     identifier iota that does not resolve to a declaration of the package
//     or of the function around it. That test reads the syntax, because the
//     checker does not evaluate the value of a const whose type comes from
//     an import.
//
// Names are resolved with go/types over the package's files. Imported
// packages are not loaded: each import stands for an empty package under
// the name `go list`, run in the directory, reports for it (the path's last
// element when go list cannot resolve it, as for a fixture's made-up path).
// The type errors that leaves are ignored. An identifier the checker
// skipped because of them is looked up by name in the scope around it (see
// hasher.object). A selector whose left side resolves to nothing is an
// error, so the tool never hashes a reference it could not resolve; when go
// list itself failed, the error says so.
//
// Not covered: the order in which package variables are initialised across
// files and the order of init functions, both of which follow the file
// order; build constraints (every non-test .go file is read); test files,
// which are skipped; comments between the specs of a group that belong to
// no spec; the paths of dot imports.
//
// The exit status is 0 when the run succeeds and, with two directories, every
// compared declaration hashes the same and every declaration found only on
// the new side is listed in -new; 1 when a compared declaration differs, a
// declaration is found only on the new side without being listed, or a
// -new entry names none; 2 on a usage or parse error.
//
// Each directory must be inside a Go module: a go.mod in the directory or
// above it. go list reads it to name the imports. For the old side, check
// out the old commit whole, for example with git worktree add. A copy of
// the package directory alone gives the wrong names, and a selector on an
// import then resolves to no declaration.
package main

import (
	"errors"
	"flag"
	"fmt"
	"go/token"
	"io"
	"os"
	"strings"
)

// usage is the synopsis printed with a usage error.
const usage = "usage: movecheck DIR | movecheck [-rename old=pkg.New,...] [-new Name,...] OLDDIR NEWDIR\n" +
	"each directory must be inside a Go module (a go.mod in it or above it), also the old side: check out the old commit whole, for example with git worktree add"

// errReported stands for an error the flag package has already printed.
var errReported = errors.New("flags: reported")

// rename is one entry of the rename map: an old package-level identifier
// and what it becomes.
type rename struct {
	// pkg is the name of the package the identifier moves to, empty when it
	// stays in the package it is declared in.
	pkg string
	// name is the identifier's new name.
	name string
}

// options holds the parsed command line.
type options struct {
	// renames maps each old identifier the -rename flag names to what it
	// becomes.
	renames map[string]rename
	// added holds the names the -new flag lists: declarations the step adds
	// to the new directory on purpose.
	added map[string]bool
	// dirs are the one or two directories to read.
	dirs []string
}

// main runs the command and exits with its status.
func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run parses the arguments, fingerprints the directories they name, and
// prints the result.
//
// Parameters:
//   - args are the command-line arguments without the program name.
//   - stdout receives the declaration lines or the differences.
//   - stderr receives errors and the count of declarations found on one
//     side.
//
// It returns the exit status described in the package comment.
func run(args []string, stdout, stderr io.Writer) int {
	opts, err := parseArgs(args, stderr)
	if err != nil {
		if !errors.Is(err, errReported) {
			_, _ = fmt.Fprintln(stderr, err)
		}
		return 2
	}
	if len(opts.dirs) == 1 {
		return printOne(opts.dirs[0], stdout, stderr)
	}
	return compare(opts, stdout, stderr)
}

// parseArgs reads the flags and directories of the command line.
//
// Parameters:
//   - args are the command-line arguments without the program name.
//   - stderr receives the flag package's own messages, such as the list of
//     flags after an unknown one.
//
// It returns the parsed options, or an error for a malformed flag value, a
// directory count other than one or two, or a flag given with one
// directory, where there is no new side for it to apply to.
func parseArgs(args []string, stderr io.Writer) (options, error) {
	flags := flag.NewFlagSet("movecheck", flag.ContinueOnError)
	flags.SetOutput(stderr)
	renameFlag := flags.String("rename", "", "comma-separated old=pkg.New or old=New entries applied to the old side")
	newFlag := flags.String("new", "", "comma-separated names of the declarations the new directory adds on purpose")
	if err := flags.Parse(args); err != nil {
		return options{}, errReported
	}
	renames, err := parseRenames(*renameFlag)
	if err != nil {
		return options{}, err
	}
	added, err := parseNames(*newFlag)
	if err != nil {
		return options{}, err
	}
	opts := options{renames: renames, added: added, dirs: flags.Args()}
	switch {
	case len(opts.dirs) == 1 && len(renames)+len(added) > 0:
		return options{}, fmt.Errorf("-rename and -new need an old and a new directory")
	case len(opts.dirs) != 1 && len(opts.dirs) != 2:
		return options{}, fmt.Errorf("%s", usage)
	}
	return opts, nil
}

// parseRenames reads the value of the -rename flag.
//
// Parameters:
//   - value is the flag's text: comma-separated entries of the form
//     old=pkg.New, for an identifier that moves to package pkg as New, or
//     old=New, for one renamed within its package.
//
// It returns the map from each old identifier to what it becomes, empty for
// an empty value, or an error naming the first malformed entry or an
// identifier renamed twice.
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

// parseNames reads the value of the -new flag.
//
// Parameters:
//   - value is the flag's text: comma-separated declaration names as the
//     key of a line spells them after its kind, such as Result, T.Method or
//     init#2.
//
// It returns the set of names, empty for an empty value, or an error for an
// empty or repeated entry.
func parseNames(value string) (map[string]bool, error) {
	names := map[string]bool{}
	if value == "" {
		return names, nil
	}
	for name := range strings.SplitSeq(value, ",") {
		if name == "" || names[name] {
			return nil, fmt.Errorf("-new %q: every entry must be a distinct, non-empty name", value)
		}
		names[name] = true
	}
	return names, nil
}

// printOne prints the sorted declaration lines of one directory.
//
// Parameters:
//   - dir is the package directory to fingerprint.
//   - stdout receives one "<key> <hash>" line per declaration.
//   - stderr receives the error when the directory cannot be read.
//
// It returns 0, or 2 when the directory cannot be read, parsed or
// resolved.
func printOne(dir string, stdout, stderr io.Writer) int {
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
