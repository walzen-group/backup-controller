// Package crds holds checked-in copies of the CustomResourceDefinitions the
// cluster tests install: CloudNativePG, plugin-barman-cloud, Flux, Kueue and
// VolSync at the versions walzen prod runs, and this repository's own CRDs at
// older tags. hack/fixtures/crds.sh writes them, with provenance.json saying
// where each came from.
package crds

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
)

// provenance is provenance.json as hack/fixtures/crds.sh writes it.
type provenance struct {
	Generator string `json:"generator"`
	Files     []struct {
		Path      string `json:"path"`
		Component string `json:"component"`
		Version   string `json:"version"`
		Source    string `json:"source"`
		SHA256    string `json:"sha256"`
	} `json:"files"`
}

// TestProvenance checks that every file provenance.json lists exists with the
// recorded sha256, that no CRD file sits here unlisted, and that each
// third-party copy has the version versions.json pins. A pin bump without a
// rerun of make fixtures-crds fails here.
func TestProvenance(t *testing.T) {
	raw, err := os.ReadFile("provenance.json")
	if err != nil {
		t.Fatal(err)
	}
	var p provenance
	if err := json.Unmarshal(raw, &p); err != nil {
		t.Fatalf("decode provenance.json: %v", err)
	}
	if len(p.Files) == 0 {
		t.Fatal("provenance.json lists no files")
	}

	pins, err := versions.Read()
	if err != nil {
		t.Fatal(err)
	}

	listed := map[string]bool{}
	for _, f := range p.Files {
		listed[filepath.FromSlash(f.Path)] = true
		data, err := os.ReadFile(filepath.FromSlash(f.Path))
		if err != nil {
			t.Errorf("%s: %v", f.Path, err)
			continue
		}
		sum := sha256.Sum256(data)
		if got := hex.EncodeToString(sum[:]); got != f.SHA256 {
			t.Errorf("%s: sha256 %s, provenance.json records %s", f.Path, got, f.SHA256)
		}
		if f.Component == "backup-controller" {
			continue
		}
		pin, ok := pins.Components[f.Component]
		if !ok {
			t.Errorf("%s: component %q is not in versions.json", f.Path, f.Component)
		} else if pin.Version != f.Version {
			t.Errorf("%s: copied from %s %s, versions.json pins %s; run make fixtures-crds", f.Path, f.Component, f.Version, pin.Version)
		}
	}

	err = filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || filepath.Ext(path) != ".yaml" {
			return err
		}
		if !listed[path] {
			t.Errorf("%s is not listed in provenance.json", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
