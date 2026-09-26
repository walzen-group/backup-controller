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
	internalvolsync "github.com/walzen-group/backup-controller/internal/volsync"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/config"
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

	env, cfg, scheme, c, className := startOrphanEnvtest(ctx, t)

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
		// The destination is looked for only a MoverPoll after its delete;
		// a short one keeps the test inside its budget.
		MoverPoll: 500 * time.Millisecond,
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

// TestEnvtestARunningMoverHoldsTheOrphanCleanup builds a claim whose
// VolumeRestore is gone, with its prime, Secret copy and ReplicationDestination
// in the controller namespace and a Running pod of the destination's mover
// Job. It shows that OrphanReconciler deletes only the ReplicationDestination
// and records a Normal WaitingForMover event while the pod is there, and that
// the cleanup completes once the pod is deleted.
//
// OrphanReconciler counts every pod its label selector lists as a mover still
// there, whatever the phase, so only the pod's deletion releases the cleanup.
// The claim is created with ClaimFinalizer directly; the library plays no
// part in this path, and TestEnvtestAClaimWhoseVolumeRestoreIsGoneIsReleased
// pins the library's names.
func TestEnvtestARunningMoverHoldsTheOrphanCleanup(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, cfg, scheme, c, className := startOrphanEnvtest(ctx, t)

	// The claim names a VolumeRestore that does not exist, as one deleted
	// after the library took the claim on.
	group := backupv1alpha1.GroupVersion.Group
	claim := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "data", Namespace: orphanNamespace, Finalizers: []string{ClaimFinalizer}},
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
	destination := DestinationName(claim.UID)
	rdKey := types.NamespacedName{Namespace: orphanControllerNS, Name: destination}
	primeKey := types.NamespacedName{Namespace: orphanControllerNS, Name: PrimeClaimName(claim.UID)}
	secretKey := types.NamespacedName{Namespace: orphanControllerNS, Name: internalvolsync.SecretCopyName(claim.UID)}
	podKey := types.NamespacedName{Namespace: orphanControllerNS, Name: internalvolsync.MoverJobName(destination) + "-abcde"}

	prime := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: primeKey.Name, Namespace: primeKey.Namespace},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &className,
			Resources:        corev1.VolumeResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")}},
		},
	}
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: secretKey.Name, Namespace: secretKey.Namespace},
		StringData: map[string]string{"RESTIC_REPOSITORY": "s3:http://example.invalid/bucket"},
	}
	rd := &volsyncv1alpha1.ReplicationDestination{ObjectMeta: metav1.ObjectMeta{Name: rdKey.Name, Namespace: rdKey.Namespace}}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podKey.Name,
			Namespace: podKey.Namespace,
			Labels:    map[string]string{"job-name": internalvolsync.MoverJobName(destination)},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "restic", Image: "quay.io/backube/volsync:0.13.0"}},
		},
	}
	for _, obj := range []client.Object{prime, secret, rd, pod} {
		if err := c.Create(ctx, obj); err != nil {
			t.Fatalf("create %T %s: %v", obj, obj.GetName(), err)
		}
	}
	pod.Status.Phase = corev1.PodRunning
	if err := c.Status().Update(ctx, pod); err != nil {
		t.Fatalf("set the mover pod Running: %v", err)
	}
	if err := c.Delete(ctx, claim); err != nil {
		t.Fatalf("delete claim: %v", err)
	}

	// controller-runtime keeps controller names process wide, and the other
	// envtest in this package registers the same one.
	skipNameValidation := true
	manager, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:     scheme,
		Metrics:    metricsserver.Options{BindAddress: "0"},
		Controller: config.Controller{SkipNameValidation: &skipNameValidation},
	})
	if err != nil {
		t.Fatalf("build manager: %v", err)
	}
	orphans := &OrphanReconciler{
		Client:    manager.GetClient(),
		Reader:    manager.GetAPIReader(),
		Recorder:  manager.GetEventRecorder("backup-controller"),
		Namespace: orphanControllerNS,
		MoverPoll: 500 * time.Millisecond,
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

	eventually(t, 30*time.Second, "the ReplicationDestination is gone and a Normal WaitingForMover event is on the claim", func() bool {
		if err := c.Get(ctx, rdKey, &volsyncv1alpha1.ReplicationDestination{}); !apierrors.IsNotFound(err) {
			return false
		}
		events := &corev1.EventList{}
		if err := c.List(ctx, events, client.InNamespace(orphanNamespace)); err != nil {
			t.Fatalf("list events: %v", err)
		}
		for _, e := range events.Items {
			if e.InvolvedObject.UID == claim.UID && e.Reason == "WaitingForMover" && e.Type == corev1.EventTypeNormal {
				return true
			}
		}
		return false
	})

	// Several passes run while the pod is there; none goes past the pod.
	time.Sleep(2 * time.Second)
	for _, key := range []types.NamespacedName{primeKey, secretKey} {
		var obj client.Object = &corev1.PersistentVolumeClaim{}
		if key == secretKey {
			obj = &corev1.Secret{}
		}
		if err := c.Get(ctx, key, obj); err != nil || obj.GetDeletionTimestamp() != nil {
			t.Fatalf("while the mover runs: %T %s get %v, deletion %v; want it in place", obj, key, err, obj.GetDeletionTimestamp())
		}
	}
	held := &corev1.PersistentVolumeClaim{}
	if err := c.Get(ctx, claimKey, held); err != nil {
		t.Fatalf("get claim: %v", err)
	}
	if !controllerutil.ContainsFinalizer(held, ClaimFinalizer) {
		t.Fatalf("while the mover runs: claim finalizers %v; want %s", held.Finalizers, ClaimFinalizer)
	}
	if err := c.Get(ctx, podKey, &corev1.Pod{}); err != nil {
		t.Fatalf("while the mover runs: get mover pod: %v", err)
	}

	// The pod is unscheduled, so the apiserver removes it at once.
	if err := c.Delete(ctx, pod, client.GracePeriodSeconds(0)); err != nil {
		t.Fatalf("delete the mover pod: %v", err)
	}
	eventually(t, 30*time.Second, "the Secret copy and the prime are deleted and the claim loses "+ClaimFinalizer, func() bool {
		got := &corev1.PersistentVolumeClaim{}
		if err := c.Get(ctx, claimKey, got); err != nil && !apierrors.IsNotFound(err) {
			t.Fatalf("get claim: %v", err)
		} else if err == nil && controllerutil.ContainsFinalizer(got, ClaimFinalizer) {
			return false
		}
		if err := c.Get(ctx, secretKey, &corev1.Secret{}); !apierrors.IsNotFound(err) {
			return false
		}
		p := &corev1.PersistentVolumeClaim{}
		err := c.Get(ctx, primeKey, p)
		return apierrors.IsNotFound(err) || (err == nil && p.DeletionTimestamp != nil)
	})
}

// startOrphanEnvtest starts envtest's kube-apiserver with the repo's CRDs and
// the recorded VolSync ReplicationDestination CRD, and creates the claim
// namespace, the controller namespace and a StorageClass nothing serves. The
// apiserver stops when the test ends.
//
// It returns the environment, its rest config, a scheme with the core,
// batch, storage, VolSync and backup types, a direct client, and the
// StorageClass's name. It fails the test when a step fails.
func startOrphanEnvtest(ctx context.Context, t *testing.T) (*envtest.Environment, *rest.Config, *runtime.Scheme, client.Client, string) {
	t.Helper()
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
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, storagev1.AddToScheme, volsyncv1alpha1.AddToScheme, backupv1alpha1.AddToScheme} {
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
	return env, cfg, scheme, c, class.Name
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
