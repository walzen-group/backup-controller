//go:build envtest

// The refusal envtest applies deploy/'s namespace, ServiceAccount, ClusterRole
// and admission policy to envtest's kube-apiserver and runs Populate as the
// controller's ServiceAccount, by impersonation, for a VolumeRestore whose
// moverSecurityContext the policy refuses.
//
//	nix develop .#envtest -c go test -tags envtest ./internal/populator/ -run Refused
package populator

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// policyOperations are the callbacks' operations for the refusal envtest:
// the Jobs, their pods, the namespace and the claims go to the API server as
// the controller's ServiceAccount, and the Secrets and the VolumeRestore stay
// in fakeOperations' memory, where the test reads the status writes.
type policyOperations struct {
	*fakeOperations
	api Operations
}

func (p policyOperations) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	return p.api.GetNamespace(ctx, name)
}

func (p policyOperations) GetClaim(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	return p.api.GetClaim(ctx, namespace, name)
}

func (p policyOperations) GetVolume(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	return p.api.GetVolume(ctx, name)
}

func (p policyOperations) PatchClaim(ctx context.Context, claim *corev1.PersistentVolumeClaim, patch client.Patch) error {
	return p.api.PatchClaim(ctx, claim, patch)
}

// applyDeployManifest creates every object of one of deploy/'s files as the
// administrator.
func applyDeployManifest(ctx context.Context, t *testing.T, admin client.Client, name string) {
	t.Helper()
	path := filepath.Join("..", "..", "deploy", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	for {
		u := &unstructured.Unstructured{}
		err := decoder.Decode(&u.Object)
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if len(u.Object) == 0 {
			continue
		}
		if err := admin.Create(ctx, u); err != nil {
			t.Fatalf("create %s %s from %s: %v", u.GetKind(), u.GetName(), path, err)
		}
	}
}

// startPolicyEnvtest starts envtest with deploy/'s objects and returns a
// client that writes as the controller's ServiceAccount, once the policy
// refuses a Job whose pod sets sysctls. Until the API server has compiled a
// new policy it admits everything, so the wait sends that Job as a dry run.
func startPolicyEnvtest(ctx context.Context, t *testing.T) client.Client {
	t.Helper()
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	admin, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"namespace.yaml", "serviceaccount.yaml", "rbac.yaml", "admissionpolicy.yaml"} {
		applyDeployManifest(ctx, t, admin, name)
	}
	// The namespace of the VolumeRestore, where the populator reads the
	// privileged-movers annotation.
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appNS}}); err != nil {
		t.Fatal(err)
	}
	impersonated := rest.CopyConfig(cfg)
	impersonated.Impersonate = rest.ImpersonationConfig{
		UserName: "system:serviceaccount:backup-system:backup-controller",
		Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:backup-system", "system:authenticated"},
	}
	controller, err := client.New(impersonated, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	probe, err := restorejob.Build(restorejob.Spec{
		Name: "policy-probe", Namespace: controllerNS,
		Origin:     restorejob.Origin{Kind: restorejob.OriginClaim, UID: "probe"},
		Owner:      metav1.OwnerReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "probe", UID: "probe"},
		SnapshotID: monday.ID, Claim: "probe", Repository: "probe", Image: testImage,
		SecurityContext: sysctlContext(),
	})
	if err != nil {
		t.Fatal(err)
	}
	eventually(t, 30*time.Second, "the policy refuses a Job whose pod sets sysctls", func() bool {
		return apierrors.IsForbidden(controller.Create(ctx, probe.DeepCopy(), client.DryRunAll))
	})
	return controller
}

// sysctlContext is a pod security context that sets a sysctl, which the
// admission policy refuses on a restore Job.
func sysctlContext() *corev1.PodSecurityContext {
	return &corev1.PodSecurityContext{Sysctls: []corev1.Sysctl{{Name: "net.ipv4.ip_local_port_range", Value: "1024 65535"}}}
}

// TestEnvtestARefusedMoverSecurityContextShowsOnReady runs
// Populate for a VolumeRestore whose moverSecurityContext sets a sysctl.
// The admission policy refuses the restore Job, so Populate creates nothing,
// returns an error, and puts reason RestoreJobRefused with the policy's
// message on Ready.
func TestEnvtestARefusedMoverSecurityContextShowsOnReady(t *testing.T) {
	ctx := context.Background()
	controller := startPolicyEnvtest(ctx, t)
	ops := policyOperations{fakeOperations: &fakeOperations{Jobs: NewJobs(controller, controller), secrets: map[string]*corev1.Secret{}}, api: NewOperations(controller)}
	addRepository(ops.fakeOperations)
	p := paramsWith(func(vr *backupv1alpha1.VolumeRestore) { vr.Spec.MoverSecurityContext = sysctlContext() })

	if err := New(ops, controllerNS, testImage, fixedSnapshots{monday}).Populate(ctx, p); err == nil {
		t.Fatal("Populate() returned no error for a refused Job")
	}
	phase, reason, message := lastReady(t, ops.fakeOperations)
	if phase != backupv1alpha1.RestorePhaseFailed || reason != backupv1alpha1.ReasonRestoreJobRefused || !strings.Contains(message, "sets no sysctls") {
		t.Fatalf("entry %s, Ready %s %q; want Failed and RestoreJobRefused with the policy's message", phase, reason, message)
	}
	jobs := &batchv1.JobList{}
	if err := controller.List(ctx, jobs, client.InNamespace(controllerNS)); err != nil {
		t.Fatal(err)
	}
	if len(jobs.Items) != 0 {
		t.Fatalf("Jobs = %d, want none created", len(jobs.Items))
	}
}
