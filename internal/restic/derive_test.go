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
	for range 4 {
		wg.Go(func() {
			if _, err := d.derive("backup", keyFile{KDF: "scrypt", N: 2, R: 1, P: 1}); err != nil {
				t.Errorf("derive: %v", err)
			}
		})
	}
	wg.Wait()

	if got := most.Load(); got != 1 {
		t.Errorf("%d derivations ran at once, want 1", got)
	}
}

// TestTheControllersDerivationsShareOneGate checks that the keyDeriver every
// Open goes through admits one derivation at a time.
func TestTheControllersDerivationsShareOneGate(t *testing.T) {
	if got := cap(derivations.gate); got != 1 {
		t.Errorf("the controller's key derivations admit %d at a time, want 1", got)
	}
}
