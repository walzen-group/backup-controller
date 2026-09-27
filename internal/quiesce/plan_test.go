package quiesce

import (
	"context"
	"slices"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// Two targets that one Kustomization applies give that Kustomization's key
// one time. The run records the keys in status.suspendedKustomizations, and
// it suspends and resumes each key it records.
func TestAKustomizationOfTwoTargetsIsListedOnce(t *testing.T) {
	ctx := context.Background()
	web := labeled()
	web.Name = "notes-web"
	c, mapper := newTestClient(t, interceptor.Funcs{}, labeled(), web, kustomizationOf([]any{
		map[string]any{"id": testNS + "_notes_apps_Deployment", "v": "v1"},
		map[string]any{"id": testNS + "_notes-web_apps_Deployment", "v": "v1"},
	}))
	targets, err := Named(ctx, c, testNS, []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: "notes"}, {Kind: "Deployment", Name: "notes-web"}})
	if err != nil {
		t.Fatal(err)
	}

	stop, suspend, err := Plan(ctx, c, mapper, testNS, targets)
	if err != nil {
		t.Fatal(err)
	}
	if len(stop) != 2 {
		t.Errorf("stop = %+v, want both Deployments", stop)
	}
	if !slices.Equal(suspend, []string{"flux-system/notes"}) {
		t.Errorf("suspend = %q, want the key flux-system/notes one time", suspend)
	}
}
