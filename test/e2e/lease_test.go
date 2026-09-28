//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/walzen-group/backup-controller/internal/lease"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// apiClient returns a client of the test cluster's API server that knows the
// core and coordination kinds. The test fails when the kubeconfig has no
// context docker-desktop.
func apiClient(t *testing.T) client.Client {
	t.Helper()
	config, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(),
		&clientcmd.ConfigOverrides{CurrentContext: kubeContext},
	).ClientConfig()
	if err != nil {
		t.Fatalf("load the kubeconfig context %s: %v", kubeContext, err)
	}
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, coordinationv1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.New(config, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatalf("create a client for %s: %v", kubeContext, err)
	}
	return c
}

// TestOneOfManyRunsTakesALease lets 20 runs take the same Lease at the same
// moment, against the real API server. Exactly one of them holds it. Once
// the holder no longer needs it, exactly one of the others takes it over;
// while the holder needs it, none does. After the holder releases it, the
// Lease is gone.
func TestOneOfManyRunsTakesALease(t *testing.T) {
	t.Parallel()
	a := newApp(t, "lease")
	c := apiClient(t)

	var mu sync.Mutex
	finished := map[types.UID]bool{}
	leases := &lease.Leases{Client: c, Reader: c, Alive: func(_ context.Context, _ string, h lease.Holder) (bool, error) {
		mu.Lock()
		defer mu.Unlock()
		return !finished[h.UID], nil
	}}
	holders := make([]lease.Holder, 20)
	for i := range holders {
		holders[i] = lease.Holder{Kind: "BackupRun", Name: fmt.Sprintf("run-%d", i), UID: types.UID(fmt.Sprintf("uid-%d", i))}
	}

	// race lets every holder that the skip function allows take the Lease at
	// once, and returns the holders that got it.
	race := func(skip func(lease.Holder) bool) []lease.Holder {
		var wg sync.WaitGroup
		var won []lease.Holder
		var wonMu sync.Mutex
		for _, h := range holders {
			if skip(h) {
				continue
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				held, _, err := leases.Take(t.Context(), a.ns, lease.ClaimPrefix+"data", h)
				if err != nil {
					t.Errorf("%s: %v", h, err)
					return
				}
				if held {
					wonMu.Lock()
					won = append(won, h)
					wonMu.Unlock()
				}
			}()
		}
		wg.Wait()
		return won
	}

	first := race(func(lease.Holder) bool { return false })
	if len(first) != 1 {
		t.Fatalf("%d runs hold the Lease after the first race, want 1: %v", len(first), first)
	}
	if again := race(func(h lease.Holder) bool { return h == first[0] }); len(again) != 0 {
		t.Fatalf("runs %v took the Lease while %s still needs it", again, first[0])
	}

	mu.Lock()
	finished[first[0].UID] = true
	mu.Unlock()
	second := race(func(h lease.Holder) bool { return h == first[0] })
	if len(second) != 1 {
		t.Fatalf("%d runs took over the Lease of the finished %s, want 1: %v", len(second), first[0], second)
	}

	if err := leases.Release(t.Context(), a.ns, lease.ClaimPrefix+"data", first[0]); err != nil {
		t.Fatal(err)
	}
	if err := getJSON(a.ns, "lease", lease.ClaimPrefix+"data", &coordinationv1.Lease{}); err != nil {
		t.Fatalf("the finished %s released the Lease that %s holds now: %v", first[0], second[0], err)
	}
	if err := leases.Release(t.Context(), a.ns, lease.ClaimPrefix+"data", second[0]); err != nil {
		t.Fatal(err)
	}
	if err := getJSON(a.ns, "lease", lease.ClaimPrefix+"data", &coordinationv1.Lease{}); err == nil {
		t.Fatalf("the Lease is still there after %s released it", second[0])
	}
}
