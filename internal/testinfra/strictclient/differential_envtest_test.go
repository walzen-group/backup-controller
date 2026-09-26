//go:build envtest

// The envtest differential suite runs the custom resource rules of the strict
// client against envtest's kube-apiserver 1.36.3, with this repository's
// BackupRun CRD at v0.7.2 and at v0.8.1 from internal/testinfra/crds and
// VolSync's ReplicationSource CRD from the same folder (written as an
// unstructured object), and compares what a client observes, the RestoreRun
// defaults included. Each version gets its own control plane,
// which is stopped at the end, so the CRDs go with it.
//
//	nix develop .#envtest -c go test -tags envtest ./internal/testinfra/strictclient/
package strictclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
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
			env := &envtest.Environment{CRDDirectoryPaths: []string{dir, replicationSourceCRD}, ErrorIfCRDPathMissing: true}
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
			scheme.AddKnownTypeWithName(replicationSourceGVK, &unstructured.Unstructured{})
			scheme.AddKnownTypeWithName(replicationSourceGVK.GroupVersion().WithKind("ReplicationSourceList"), &unstructured.UnstructuredList{})
			real, err := client.New(cfg, client.Options{Scheme: scheme})
			if err != nil {
				t.Fatal(err)
			}
			files, err := filepath.Glob(filepath.Join(dir, "*.yaml"))
			if err != nil {
				t.Fatal(err)
			}
			files = append(files, replicationSourceCRD)
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

// replicationSourceCRD is a third-party CRD whose spec.external.parameters is
// a free-form string map, so pruning has to keep keys its schema does not
// name there while it drops unknown fields elsewhere. None of the pinned
// third-party CRDs uses x-kubernetes-preserve-unknown-fields.
var replicationSourceCRD = filepath.Join("..", "crds", "volsync", "volsync.backube_replicationsources.yaml")

var replicationSourceGVK = schema.GroupVersionKind{Group: "volsync.backube", Version: "v1alpha1", Kind: "ReplicationSource"}

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

	// A spec change raises the generation, and a status write that also
	// changes labels keeps the stored labels.
	stored.Spec.Source = "other"
	if err := c.Update(ctx, stored); err != nil {
		t.Fatal(err)
	}
	o["spec update generation"] = fmt.Sprint(stored.Generation)
	stored.Labels = map[string]string{"k": "changed", "extra": "x"}
	stored.Status.Phase = "Running"
	o["status update with labels"] = envtestReason(c.Status().Update(ctx, stored))
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatal(err)
	}
	o["status update with labels: labels"] = fmt.Sprint(stored.Labels)
	o["status update with labels: phase"] = string(stored.Status.Phase)
	o["status update with labels: generation"] = fmt.Sprint(stored.Generation)

	// A patch and a status patch carrying another uid.
	uid := stored.UID
	stalePatch := client.RawPatch(types.MergePatchType,
		[]byte(`{"metadata":{"uid":"00000000-0000-0000-0000-000000000000"},"spec":{"source":"patched"}}`))
	o["patch with stale uid"] = envtestReason(c.Patch(ctx, stored.DeepCopy(), stalePatch))
	stalePatch = client.RawPatch(types.MergePatchType,
		[]byte(`{"metadata":{"uid":"00000000-0000-0000-0000-000000000000"},"status":{"phase":"Failed"}}`))
	o["status patch with stale uid"] = envtestReason(c.Status().Patch(ctx, stored.DeepCopy(), stalePatch))
	if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
		t.Fatal(err)
	}
	o["after stale uid patches: uid kept"] = fmt.Sprint(stored.UID == uid)
	o["after stale uid patches: source"] = stored.Spec.Source
	o["after stale uid patches: phase"] = string(stored.Status.Phase)

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
	restoreRunDefaultOps(t, c, o)
	replicationSourceOps(t, c, o)
	return o
}

// restoreRunDefaultOps writes RestoreRuns with c and adds to o what the CRD's
// default for spec.timeout (4h in both pinned versions) left in the object:
// on a create without it, on a create that sets it, and after an update
// that clears it.
func restoreRunDefaultOps(t *testing.T, c client.Client, o map[string]string) {
	t.Helper()
	ctx := context.Background()
	timeout := func(r *backupv1alpha1.RestoreRun) string {
		if r.Spec.Timeout == nil {
			return "<nil>"
		}
		return r.Spec.Timeout.Duration.String()
	}
	for _, tc := range []struct {
		name    string
		timeout *metav1.Duration
	}{
		{name: "defaulted", timeout: nil},
		{name: "explicit", timeout: &metav1.Duration{Duration: 30 * time.Minute}},
	} {
		r := &backupv1alpha1.RestoreRun{
			ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: tc.name},
			Spec:       backupv1alpha1.RestoreRunSpec{Claim: "data", Timeout: tc.timeout},
		}
		if err := c.Create(ctx, r); err != nil {
			t.Fatal(err)
		}
		o["RestoreRun "+tc.name+" create: returned timeout"] = timeout(r)
		stored := &backupv1alpha1.RestoreRun{ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: tc.name}}
		if err := c.Get(ctx, client.ObjectKeyFromObject(stored), stored); err != nil {
			t.Fatal(err)
		}
		o["RestoreRun "+tc.name+" create: stored timeout"] = timeout(stored)
		stored.Spec.Timeout = nil
		if err := c.Update(ctx, stored); err != nil {
			t.Fatal(err)
		}
		o["RestoreRun "+tc.name+" update clearing timeout: returned timeout"] = timeout(stored)
		o["RestoreRun "+tc.name+" update clearing timeout: generation"] = fmt.Sprint(stored.Generation)
		if err := c.Delete(ctx, stored); err != nil {
			t.Fatal(err)
		}
	}
}

// replicationSourceOps writes an unstructured VolSync ReplicationSource with
// c and adds what it observed to o: which fields pruning kept on create and
// on an update, and the generations.
func replicationSourceOps(t *testing.T, c client.Client, o map[string]string) {
	t.Helper()
	ctx := context.Background()
	rs := &unstructured.Unstructured{}
	rs.SetGroupVersionKind(replicationSourceGVK)
	rs.SetNamespace("default")
	rs.SetName("third-party")
	rs.Object["spec"] = map[string]any{
		"sourcePVC":    "data",
		"unknownField": "dropped",
		"external": map[string]any{
			"provider":    "example.com/mover",
			"parameters":  map[string]any{"anyKey": "kept", "other": "kept too"},
			"notInSchema": "dropped",
		},
	}
	rs.Object["status"] = map[string]any{"lastSyncDuration": "1s"}
	if err := c.Create(ctx, rs); err != nil {
		t.Fatal(err)
	}
	read := func(label string) {
		got := &unstructured.Unstructured{}
		got.SetGroupVersionKind(replicationSourceGVK)
		if err := c.Get(ctx, client.ObjectKeyFromObject(rs), got); err != nil {
			t.Fatal(err)
		}
		spec, err := json.Marshal(got.Object["spec"])
		if err != nil {
			t.Fatal(err)
		}
		status, err := json.Marshal(got.Object["status"])
		if err != nil {
			t.Fatal(err)
		}
		o["ReplicationSource "+label+" spec"] = string(spec)
		o["ReplicationSource "+label+" status"] = string(status)
		o["ReplicationSource "+label+" generation"] = fmt.Sprint(got.GetGeneration())
	}
	read("create")
	if err := unstructured.SetNestedField(rs.Object, "new", "spec", "external", "parameters", "added"); err != nil {
		t.Fatal(err)
	}
	if err := unstructured.SetNestedField(rs.Object, "dropped", "spec", "external", "alsoUnknown"); err != nil {
		t.Fatal(err)
	}
	if err := c.Update(ctx, rs); err != nil {
		t.Fatal(err)
	}
	read("update")
	if err := c.Delete(ctx, rs); err != nil {
		t.Fatal(err)
	}
}

func envtestReason(err error) string {
	if err == nil {
		return "ok"
	}
	return string(apierrors.ReasonForError(err))
}
