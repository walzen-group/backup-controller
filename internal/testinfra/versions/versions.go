// Package versions reads versions.json, the one list of the versions walzen
// prod runs and of the tools the tests take from the flake. The tests use it
// to check that a recorded fixture came from the pinned tools, so a bump of a
// pin that forgets to regenerate the fixtures fails a test instead of passing
// on recordings of the old version.
//
// It is test support: only tests import it.
package versions

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
)

// Component is one entry of versions.json.
type Component struct {
	// Version is the version, without a leading v.
	Version string `json:"version"`
	// Prod says where walzen prod pins it, when prod runs it.
	Prod string `json:"prod"`
	// Tests says where the tests take it from.
	Tests string `json:"tests"`
	// Chart is the version of the Helm chart prod installs it with, when that
	// differs from Version.
	Chart string `json:"chart"`
	// E2E says which hack/e2e component pins it for the e2e cluster.
	E2E string `json:"e2e"`
}

// File is versions.json as a whole.
type File struct {
	Components map[string]Component `json:"components"`
}

// Path returns the absolute path of versions.json at the module's root. It
// finds the root from this source file's own path, so it works from any
// package's test directory.
func Path() string {
	_, self, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(self), "..", "..", "..", "versions.json")
}

// Read reads and decodes versions.json. It returns an error when the file
// can't be read or isn't the expected JSON.
func Read() (File, error) {
	raw, err := os.ReadFile(Path())
	if err != nil {
		return File{}, err
	}
	var f File
	if err := json.Unmarshal(raw, &f); err != nil {
		return File{}, fmt.Errorf("decode %s: %w", Path(), err)
	}
	return f, nil
}

// Of returns the version versions.json gives the named component, and fails
// the test when the file can't be read or has no such component.
func Of(t testing.TB, name string) string {
	t.Helper()
	f, err := Read()
	if err != nil {
		t.Fatalf("read versions.json: %v", err)
	}
	c, ok := f.Components[name]
	if !ok || c.Version == "" {
		t.Fatalf("versions.json has no version for %s", name)
	}
	return c.Version
}
