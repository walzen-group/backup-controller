package restic

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestKeyDerivationsRunOneAtATime checks that a keyDeriver runs its
// derivations one after another when several Opens ask at once.
//
// scrypt with restic's parameters (N=32768, r=8) holds 32 MiB for each
// derivation. The BackupRun and RestoreRun reconcilers and the populator
// each open repositories, and three derivations at once, on top of the
// process's resting memory, passed the controller's 128Mi limit: the e2e run
// saw the controller OOMKilled.
func TestKeyDerivationsRunOneAtATime(t *testing.T) {
	var running, most atomic.Int32
	watch := func(_, _ []byte, _, _, _, keyLen int) ([]byte, error) {
		now := running.Add(1)
		defer running.Add(-1)
		for {
			seen := most.Load()
			if now <= seen || most.CompareAndSwap(seen, now) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		return make([]byte, keyLen), nil
	}
	d := newKeyDeriver(watch)

	var wg sync.WaitGroup
	for i := range 4 {
		wg.Go(func() {
			file := keyFile{KDF: "scrypt", N: 2, R: 1, P: 1, Salt: []byte{byte(i)}}
			if _, err := d.derive("backup", file); err != nil {
				t.Errorf("derive: %v", err)
			}
		})
	}
	wg.Wait()

	if got := most.Load(); got != 1 {
		t.Errorf("%d derivations ran at once, want 1", got)
	}
}

// TestAKeyFileIsDerivedOnce checks that a keyDeriver runs scrypt once for a
// password and a key file, and gives the key it derived to every later Open
// of the same key file. A key file with another salt, another N, r or p, or
// another password gets its own derivation.
//
// The e2e restore tests opened their repositories 37 times, and each Open
// allocated scrypt's 32 MiB again. The large spans left the heap at 181 MB
// though it held about 60 MB.
func TestAKeyFileIsDerivedOnce(t *testing.T) {
	var calls atomic.Int32
	count := func(password, salt []byte, _, _, _, keyLen int) ([]byte, error) {
		calls.Add(1)
		out := make([]byte, keyLen)
		copy(out, append(append([]byte{}, password...), salt...))
		return out, nil
	}
	d := newKeyDeriver(count)
	file := keyFile{KDF: "scrypt", N: 2, R: 1, P: 1, Salt: []byte("salt")}

	first, err := d.derive("backup", file)
	if err != nil {
		t.Fatalf("derive: %v", err)
	}
	for range 3 {
		again, err := d.derive("backup", file)
		if err != nil {
			t.Fatalf("derive again: %v", err)
		}
		if again != first {
			t.Error("a second derivation of the same key file gave another key")
		}
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("four Opens of one key file ran scrypt %d times, want 1", got)
	}

	other := file
	other.Salt = []byte("other")
	if _, err := d.derive("backup", other); err != nil {
		t.Fatalf("derive another salt: %v", err)
	}
	if _, err := d.derive("changed", file); err != nil {
		t.Fatalf("derive another password: %v", err)
	}
	if got := calls.Load(); got != 3 {
		t.Errorf("another salt and another password ran scrypt %d times in all, want 3", got)
	}

	for name, cost := range map[string]func(*keyFile){
		"N": func(f *keyFile) { f.N = 4 },
		"r": func(f *keyFile) { f.R = 2 },
		"p": func(f *keyFile) { f.P = 2 },
	} {
		before := calls.Load()
		changed := file
		cost(&changed)
		if _, err := d.derive("backup", changed); err != nil {
			t.Fatalf("derive another %s: %v", name, err)
		}
		if calls.Load() == before {
			t.Errorf("the same salt with another %s gave the kept key, want its own derivation", name)
		}
	}
}
