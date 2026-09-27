package strictclient

import (
	"path/filepath"
	"slices"
	"sync"
	"testing"
)

// TestCRDSetsAreParsedOncePerFileList checks that concurrent loads of one
// ordered file list share a single parsed set, and that a different order is
// a different set, since a later file's kind replaces an earlier one's.
func TestCRDSetsAreParsedOncePerFileList(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "crds", "backup-controller", "v0.8.1", "*.yaml"))
	if err != nil || len(files) < 2 {
		t.Fatalf("need at least two pinned CRDs: %v", err)
	}
	sets := make([]*crdSet, 8)
	var wg sync.WaitGroup
	for i := range sets {
		wg.Go(func() {
			set, err := cachedCRDs(files)
			if err != nil {
				t.Error(err)
			}
			sets[i] = set
		})
	}
	wg.Wait()
	for _, s := range sets[1:] {
		if s != sets[0] {
			t.Fatal("concurrent loads of one file list returned different sets")
		}
	}
	if len(sets[0].schemas) == 0 {
		t.Fatal("the shared set has no schemas")
	}
	reversed := slices.Clone(files)
	slices.Reverse(reversed)
	other, err := cachedCRDs(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if other == sets[0] {
		t.Fatal("a reordered file list shares the set of the original order")
	}
}

// TestACRDFileIsParsedOnceAndReadCRDsGivesCopies checks that two file lists
// that hold the same file share one parse of it, and that ReadCRDs gives a
// new deep copy on each call, so a test that changes its copy does not
// change the CRDs of the next test.
func TestACRDFileIsParsedOnceAndReadCRDsGivesCopies(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("..", "crds", "backup-controller", "v0.8.1", "*.yaml"))
	if err != nil || len(files) < 2 {
		t.Fatalf("need at least two pinned CRDs: %v", err)
	}
	if _, err := cachedCRDs(files); err != nil {
		t.Fatal(err)
	}
	first := cachedFile(files[0])
	if _, err := cachedCRDs(files[:1]); err != nil {
		t.Fatal(err)
	}
	if again := cachedFile(files[0]); again != first {
		t.Fatal("a second file list parsed a file that the first list parsed before")
	}

	a, err := ReadCRDs(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 || len(a[0].Spec.Versions) == 0 {
		t.Fatalf("ReadCRDs(%s) gave %d CRDs, want one with versions", files[0], len(a))
	}
	name := a[0].Spec.Names.Kind
	a[0].Spec.Names.Kind = "Changed"
	a[0].Spec.Versions[0].Name = "changed"
	b, err := ReadCRDs(files[0])
	if err != nil {
		t.Fatal(err)
	}
	if b[0].Spec.Names.Kind != name || b[0].Spec.Versions[0].Name == "changed" {
		t.Fatal("a change to the CRDs that ReadCRDs gave shows in a later call")
	}
}
