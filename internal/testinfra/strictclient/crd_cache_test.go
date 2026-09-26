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
