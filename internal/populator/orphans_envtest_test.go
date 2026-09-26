//go:build envtest

// The orphan envtest reproduces, against envtest's kube-apiserver and the real
// lib-volume-populator v3.3.0, a claim that stays Terminating because its
// VolumeRestore was deleted while the prime claim was still Pending, and shows
// that OrphanReconciler clears it. envtest runs no provisioner, so the prime
// stays Pending for good, which is exactly the failing provisioning case.
//
//	nix develop .#envtest -c go test -tags envtest ./internal/populator/
package populator

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

// TestEnvtestAClaimWhoseVolumeRestoreIsGoneIsReleased runs the library alone
// first, shows that a claim deleted after its VolumeRestore keeps
// ClaimFinalizer and its prime, then starts a manager with OrphanReconciler
// and shows the claim goes and the prime is deleted.
func TestEnvtestAClaimWhoseVolumeRestoreIsGoneIsReleased(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	env := &envtest.Environment{
		CRDDirectoryPaths: []string{
			filepath.Join("..", "..", "config", "crd"),
			filepath.Join("..", "testinfra", "crds", "volsync", "volsync.backube_replicationdestinations.yaml"),
		},
		ErrorIfCRDPathMissing: true,
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, storagev1.AddToScheme, volsyncv1alpha1.AddToScheme, backupv1alpha1.AddToScheme} {
		if err := add(scheme); err != nil {
			t.Fatal(err)
		}
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}

	for _, ns := range []string{orphanNamespace, orphanControllerNS} {
		if err := c.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: ns}}); err != nil {
			t.Fatalf("create namespace %s: %v", ns, err)
		}
	}
	immediate := storagev1.VolumeBindingImmediate
	class := &storagev1.StorageClass{
		ObjectMeta:        metav1.ObjectMeta{Name: "unserved"},
		Provisioner:       "unserved.example.invalid",
		VolumeBindingMode: &immediate,
	}
	if err := c.Create(ctx, class); err != nil {
		t.Fatalf("create StorageClass: %v", err)
	}

	// The library reads a kubeconfig file, so envtest's goes to a temp file.
	kubeconfig := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(kubeconfig, env.KubeConfig, 0o600); err != nil {
		t.Fatal(err)
	}
	stop := make(chan struct{})
	libraryDone := make(chan struct{})
	go func() {
		defer close(libraryDone)
		callbacks := New(newFakeOperations(), orphanControllerNS, nil)
		populatormachinery.RunControllerWithConfig(populatormachinery.VolumePopulatorConfig{
			Kubeconfig: kubeconfig,
			Namespace:  orphanControllerNS,
			Prefix:     Prefix,
			Gk:         schema.GroupKind{Group: backupv1alpha1.GroupVersion.Group, Kind: "VolumeRestore"},
			Gvr:        backupv1alpha1.GroupVersion.WithResource("volumerestores"),
			StopCh:     stop,
			ProviderFunctionConfig: &populatormachinery.ProviderFunctionConfig{
				PopulateFn:         callbacks.Populate,
				PopulateCompleteFn: callbacks.Complete,
				PopulateCleanupFn:  callbacks.Cleanup,
			},
		})
	}()
	t.Cleanup(func() {
		close(stop)
		select {
		case <-libraryDone:
		case <-time.After(10 * time.Second):
			t.Log("the populator library did not stop within 10 s")
		}
	})

	// A VolumeRestore as a person or GitOps creates it: without the
	// populator's finalizer.
	vr := &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: orphanVolumeRestore, Namespace: orphanNamespace},
		Spec:       backupv1alpha1.VolumeRestoreSpec{Repository: "repo-secret"},
	}
	if err := c.Create(ctx, vr); err != nil {
		t.Fatalf("create VolumeRestore: %v", err)
	}
	group := backupv1alpha1.GroupVersion.Group
	className := class.Name
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: orphanNamespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &className,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
			DataSourceRef:    &corev1.TypedObjectReference{APIGroup: &group, Kind: "VolumeRestore", Name: orphanVolumeRestore},
		},
	}
	if err := c.Create(ctx, claim); err != nil {
		t.Fatalf("create claim: %v", err)
	}
	claimKey := client.ObjectKeyFromObject(claim)
	primeKey := types.NamespacedName{Namespace: orphanControllerNS, Name: PrimeClaimName(claim.UID)}

	// The library creates the prime and adds its finalizer to the claim. This
	// pins both library names, PrimeClaimName and ClaimFinalizer.
	eventually(t, 30*time.Second, "the prime exists and the claim carries "+ClaimFinalizer, func() bool {
		prime := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, primeKey, prime); err != nil {
			return false
		}
		got := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, claimKey, got); err != nil {
			t.Fatalf("get claim: %v", err)
		}
		return controllerutil.ContainsFinalizer(got, ClaimFinalizer)
	})

	// The VolumeRestore carries no finalizer, so it goes at once.
	if err := c.Delete(ctx, vr); err != nil {
		t.Fatalf("delete VolumeRestore: %v", err)
	}
	if err := c.Get(ctx, client.ObjectKeyFromObject(vr), &backupv1alpha1.VolumeRestore{}); !apierrors.IsNotFound(err) {
		t.Fatalf("VolumeRestore after delete: got %v, want NotFound", err)
	}
	if err := c.Delete(ctx, claim); err != nil {
		t.Fatalf("delete claim: %v", err)
	}

	// Without the reconciler the library returns on the data source's
	// NotFound before its cleanup, so the claim keeps its finalizer and the
	// prime stays.
	time.Sleep(20 * time.Second)
	stuck := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, claimKey, stuck); err != nil {
		t.Fatalf("get claim: %v", err)
	}
	if stuck.DeletionTimestamp == nil || !controllerutil.ContainsFinalizer(stuck, ClaimFinalizer) {
		t.Fatalf("without the reconciler: claim deletion %v, finalizers %v; want Terminating with %s", stuck.DeletionTimestamp, stuck.Finalizers, ClaimFinalizer)
	}
	prime := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, primeKey, prime); err != nil || prime.DeletionTimestamp != nil {
		t.Fatalf("without the reconciler: prime get %v, deletion %v; want it in place", err, prime.DeletionTimestamp)
	}

	// Start the reconciler on a stuck claim, as a new process would.
	manager, err := ctrl.NewManager(cfg, ctrl.Options{Scheme: scheme, Metrics: metricsserver.Options{BindAddress: "0"}})
	if err != nil {
		t.Fatalf("build manager: %v", err)
	}
	orphans := &OrphanReconciler{
		Client:    manager.GetClient(),
		Reader:    manager.GetAPIReader(),
		Recorder:  manager.GetEventRecorder("backup-controller"),
		Namespace: orphanControllerNS,
	}
	if err := orphans.SetupWithManager(manager); err != nil {
		t.Fatalf("register OrphanReconciler: %v", err)
	}
	managerDone := make(chan error, 1)
	go func() { managerDone <- manager.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		<-managerDone
	})

	eventually(t, 30*time.Second, "the claim loses "+ClaimFinalizer+" and the prime is deleted", func() bool {
		got := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, claimKey, got); err != nil {
			t.Fatalf("get claim: %v", err)
		}
		if controllerutil.ContainsFinalizer(got, ClaimFinalizer) {
			return false
		}
		p := &corev1.PersistentVolumeClaim{}
		err := c.Get(ctx, primeKey, p)
		return apierrors.IsNotFound(err) || (err == nil && p.DeletionTimestamp != nil)
	})

	// envtest runs no pvc-protection controller. No pod uses the claim, so
	// that controller would release it; the test does it in its place.
	eventually(t, 30*time.Second, "the claim is gone", func() bool {
		got := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, claimKey, got); apierrors.IsNotFound(err) {
			return true
		} else if err != nil {
			t.Fatalf("get claim: %v", err)
		}
		base := got.DeepCopy()
		controllerutil.RemoveFinalizer(got, pvcProtection)
		if err := c.Patch(ctx, got, client.MergeFrom(base)); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
			t.Fatalf("remove %s: %v", pvcProtection, err)
		}
		return false
	})
}

// eventually polls done every 250 ms until it returns true, and fails the
// test with what when timeout passes first.
func eventually(t *testing.T, timeout time.Duration, what string, done func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !done() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting until %s", timeout, what)
		}
		time.Sleep(250 * time.Millisecond)
	}
}
