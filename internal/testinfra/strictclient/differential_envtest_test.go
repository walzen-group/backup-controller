//go:build envtest

// The envtest differential suite runs the custom resource rules of the strict
// client against envtest's kube-apiserver 1.36.3, with this repository's
// BackupRun CRD at v0.7.2 and at v0.8.1 from internal/testinfra/crds, and
// compares what a client observes. Each version gets its own control plane,
// which is stopped at the end, so the CRDs go with it.
//
//	nix develop .#envtest -c go test -tags envtest ./internal/testinfra/strictclient/
package strictclient_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
)

func TestDifferentialAgainstEnvtest(t *testing.T) {
	for _, version := range []string{"v0.7.2", "v0.8.1"} {
		t.Run(version, func(t *testing.T) {
			dir := filepath.Join("..", "crds", "backup-controller", version)
			env := &envtest.Environment{CRDDirectoryPaths: []string{dir}, ErrorIfCRDPathMissing: true}
			cfg, err := env.Start()
			if err != nil {
				t.Fatalf("start envtest: %v", err)
			}
			t.Cleanup(func() { _ = env.Stop() })
			scheme := runtime.NewScheme()
			for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, backupv1alpha1.AddToScheme} {
				if err := add(scheme); err != nil {
					t.Fatal(err)
				}
			}
			real, err := client.New(cfg, client.Options{Scheme: scheme})
			if err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			strict := strictclient.Build(fake.NewClientBuilder(), scheme, strictclient.Options{Clock: time.Now, CRDs: files})

			want := envtestOps(t, strict)
			got := envtestOps(t, real)
			for k, w := range want {
				if got[k] != w {
					t.Errorf("%s: strict client %q, envtest %q", k, w, got[k])
				}
			}
			for k := range got {
				if _, ok := want[k]; !ok {
					t.Errorf("%s: only envtest observed %q", k, got[k])
				}
			}
		})
	}
}

// envtestOps runs the CRD operations with c and returns what it observed.
func envtestOps(t *testing.T, c client.Client) map[string]string {
	t.Helper()
	ctx := context.Background()
	o := map[string]string{}
	newRun := func(name string) *backupv1alpha1.BackupRun {
		return &backupv1alpha1.BackupRun{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: name},
			Spec:       backupv1alpha1.BackupRunSpec{Source: "data"},
		}
	}

	// Pruning and the status split: a create's status is dropped, a status
	// write keeps only what the installed schema declares, and a plain
	// update does not touch status.
	r := newRun("pruned")
	r.Status.Phase = "Running"
	if err := c.Create(ctx, r); err != nil {
		t.Fatal(err)
	}
	o["create status phase"] = string(r.Status.Phase)
	o["create generation"] = fmt.Sprint(r.Generation)
	r.Status.Phase = "Running"
	r.Status.RestartPending = true
	if err := c.Status().Update(ctx, r); err != nil {
		t.Fatal(err)
	}
	stored := newRun("pruned")
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatal(err)
	}
	o["status write restartPending"] = fmt.Sprint(stored.Status.RestartPending)
	o["status write phase"] = string(stored.Status.Phase)
	o["status write generation"] = fmt.Sprint(stored.Generation)

	stored.Status.Phase = "Failed"
	stored.Labels = map[string]string{"k": "v"}
	if err := c.Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatal(err)
	}
	o["plain update status phase"] = string(stored.Status.Phase)
	o["plain update generation"] = fmt.Sprint(stored.Generation)

	patch := client.RawPatch(types.MergePatchType, []byte(`{"status":{"restartPending":false}}`))
	if err := c.Status().Patch(ctx, stored, patch); err != nil {
		t.Fatal(err)
	}
	patch = client.RawPatch(types.MergePatchType, []byte(`{"status":{"restartPending":true}}`))
	if err := c.Status().Patch(ctx, stored, patch); err != nil {
		t.Fatal(err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatal(err)
	}
	o["status patch restartPending"] = fmt.Sprint(stored.Status.RestartPending)

	// An update and a status update carrying another uid.
	stale := stored.DeepCopy()
	stale.UID = "00000000-0000-0000-0000-000000000000"
	o["update with mismatched uid"] = envtestReason(c.Update(ctx, stale))
	stale = stored.DeepCopy()
	stale.UID = "00000000-0000-0000-0000-000000000000"
	o["status update with mismatched uid"] = envtestReason(c.Status().Update(ctx, stale))

	if err := c.Delete(ctx, stored); err != nil {
		t.Fatal(err)
	}
	return o
}

func envtestReason(err error) string {
	if err == nil {
		return "ok"
	}
	return string(apierrors.ReasonForError(err))
}
