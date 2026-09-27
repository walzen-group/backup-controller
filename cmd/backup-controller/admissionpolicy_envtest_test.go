//go:build envtest

// The admission policy envtest applies deploy/'s namespace, ServiceAccount,
// ClusterRole and admission policy to envtest's kube-apiserver (the
// Kubernetes version versions.json pins) and writes as the controller's
// ServiceAccount, by impersonation, to check what the policy admits and what
// the shipped ClusterRole lets the ServiceAccount do to workloads.
//
//	nix develop .#envtest -c go test -tags envtest ./cmd/backup-controller/ -run Envtest
package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilyaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	"github.com/walzen-group/backup-controller/internal/restorejob"
)

// controllerUser is the username the API server gives the controller's
// ServiceAccount as deploy/ installs it, the user the policy matches.
const controllerUser = "system:serviceaccount:backup-system:backup-controller"

// policyName is the name of the policy and its binding in deploy/.
const policyName = "backup-controller-restore-jobs"

// The messages of the policy's validations, as deploy/admissionpolicy.yaml
// writes them. A refusal is asserted by its message, so a case that a
// different validation refuses fails.
const (
	msgLabelled      = "backup-controller may create, change and delete only Jobs labelled"
	msgPodLabel      = "a restore Job's pods carry app.kubernetes.io/component=restore"
	msgNoToken       = "a restore Job's pod mounts no ServiceAccount token"
	msgServiceAcct   = "a restore Job's pod runs as its namespace's default ServiceAccount"
	msgNoHost        = "a restore Job's pod uses no host namespace, names no node, and claims no device"
	msgVolumes       = "a restore Job's pod mounts only claims, emptyDir and ephemeral claims without a data source"
	msgPodSecurity   = "a restore Job's pod security context sets no sysctls, SELinux options or unconfined profile"
	msgUnprivileged  = "a restore Job's containers are unprivileged"
	policyRefusalFmt = "ValidatingAdmissionPolicy '%s'"
)

// The namespaces the test restores in: app is a plain namespace, and
// app-privileged carries VolSync's privileged-movers annotation.
const (
	nsController = "backup-system"
	nsApp        = "app"
	nsPrivileged = "app-privileged"
)

// policyHarness holds the envtest API server with deploy/'s objects applied
// and two clients: admin as envtest's administrator, and controller as the
// controller's ServiceAccount.
type policyHarness struct {
	admin      client.Client
	controller client.Client
	jobs       restorejob.API
	serial     int
}

// startPolicyHarness starts envtest, applies deploy/'s namespace,
// ServiceAccount, ClusterRole and admission policy, and waits until the
// policy refuses a Job it must refuse.
//
// Parameters:
//   - t stops the API server when the test ends, and fails the test when any
//     step of the setup fails, when the API server reports a type-checking
//     warning on the policy's expressions, or when the policy never takes
//     effect.
//
// It returns the harness. Its controller client writes as the controller's
// ServiceAccount, by impersonation, with the strict field validation of
// clientOptions, the way the controller's own client writes.
func startPolicyHarness(t *testing.T) *policyHarness {
	t.Helper()
	ctx := context.Background()
	env := &envtest.Environment{}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, batchv1.AddToScheme, appsv1.AddToScheme,
		autoscalingv1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	admin, err := client.New(cfg, client.Options{Scheme: s})
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{"namespace.yaml", "serviceaccount.yaml", "rbac.yaml", "admissionpolicy.yaml"} {
		applyManifest(ctx, t, admin, filepath.Join("..", "..", "deploy", file))
	}
	for _, ns := range []*corev1.Namespace{
		{ObjectMeta: metav1.ObjectMeta{Name: nsApp}},
		{ObjectMeta: metav1.ObjectMeta{Name: nsPrivileged, Annotations: map[string]string{restorejob.AnnotationPrivilegedMovers: "true"}}},
	} {
		if err := admin.Create(ctx, ns); err != nil {
			t.Fatal(err)
		}
	}

	impersonated := rest.CopyConfig(cfg)
	impersonated.Impersonate = rest.ImpersonationConfig{
		UserName: controllerUser,
		Groups:   []string{"system:serviceaccounts", "system:serviceaccounts:backup-system", "system:authenticated"},
	}
	controller, err := client.New(impersonated, clientOptions(s))
	if err != nil {
		t.Fatal(err)
	}
	h := &policyHarness{admin: admin, controller: controller, jobs: restorejob.NewAPI(controller, controller)}
	h.waitForPolicy(ctx, t)
	return h
}

// applyManifest creates every object of a multi-document YAML file as the
// administrator.
//
// Parameters:
//   - t fails the test when the file cannot be read or parsed, when it holds
//     no object, or when a create fails.
//   - admin is the administrator's client.
//   - path is the manifest's path relative to this package.
func applyManifest(ctx context.Context, t *testing.T, admin client.Client, path string) {
	t.Helper()
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	decoder := utilyaml.NewYAMLOrJSONDecoder(bytes.NewReader(content), 4096)
	created := 0
	for {
		u := &unstructured.Unstructured{}
		err := decoder.Decode(&u.Object)
		if errors.Is(err, io.EOF) {
			break
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
		created++
	}
	if created == 0 {
		t.Fatalf("%s holds no object", path)
	}
}

// waitForPolicy waits until the policy refuses a Job with a hostPath volume.
//
// Parameters:
//   - t records an error when the policy still admits the Job after 30
//     seconds. It is an error and not a fatal failure, so the cases after it
//     still run and show what an absent policy admits.
//
// The API server compiles a new policy and binding a moment after they are
// created, and until then admits everything. Each attempt that is admitted
// is deleted again by the administrator.
//
// The policy's status.typeChecking stays empty here: the controller that
// type-checks the expressions against the Job schema runs in
// kube-controller-manager, which envtest does not start. A validation that
// does not compile or evaluate denies every Job with an error of its own
// under failurePolicy Fail, so the admitted Jobs and the message each refusal
// is asserted by stand in for that check.
func (h *policyHarness) waitForPolicy(ctx context.Context, t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		job := h.build(t, nsApp, false)
		job.Spec.Template.Spec.Volumes = append(job.Spec.Template.Spec.Volumes, hostPathVolume())
		err := h.jobs.CreateJob(ctx, job)
		if apierrors.IsForbidden(err) {
			return
		}
		if err == nil {
			_ = h.admin.Delete(ctx, job)
		}
		if time.Now().After(deadline) {
			t.Errorf("the controller's ServiceAccount could still create a Job with a hostPath volume after 30 seconds (last answer: %v)", err)
			return
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// build returns the restore Job restorejob.Build makes for a RestoreRun's
// item in a namespace, with a name of its own.
//
// Parameters:
//   - t fails the test when Build refuses the Spec.
//   - namespace is the Job's namespace, one of the test's namespaces.
//   - privileged is what the caller sets from the namespace's
//     privileged-movers annotation.
//
// A privileged Job runs as root with the three ownership capabilities; the
// other one runs under a non-root moverSecurityContext, the values prod
// sets.
func (h *policyHarness) build(t *testing.T, namespace string, privileged bool) *batchv1.Job {
	t.Helper()
	h.serial++
	spec := restorejob.Spec{
		Name:       fmt.Sprintf("restore-%d", h.serial),
		Namespace:  namespace,
		Origin:     restorejob.Origin{Kind: restorejob.OriginRestoreRun, UID: types.UID(fmt.Sprintf("run-uid-%d", h.serial))},
		Owner:      metav1.OwnerReference{APIVersion: "backup.wlz.li/v1alpha1", Kind: "RestoreRun", Name: "restore", UID: types.UID(fmt.Sprintf("run-uid-%d", h.serial)), Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)},
		SnapshotID: strings.Repeat("0123456789abcdef", 4),
		Claim:      "data",
		Repository: "data-restic",
		Image:      pinnedRestoreImage,
		Delete:     true,
		Privileged: privileged,
	}
	if !privileged {
		spec.SecurityContext = &corev1.PodSecurityContext{RunAsUser: ptr.To[int64](1000), RunAsGroup: ptr.To[int64](1000), FSGroup: ptr.To[int64](1000)}
	}
	job, err := restorejob.Build(spec)
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return job
}

// buildForClaim returns the restore Job restorejob.Build makes for the
// populator: in the controller's namespace, for a claim, owned by the prime
// claim, without --delete.
//
// Parameters:
//   - t fails the test when Build refuses the Spec.
func (h *policyHarness) buildForClaim(t *testing.T) *batchv1.Job {
	t.Helper()
	h.serial++
	uid := types.UID(fmt.Sprintf("claim-uid-%d", h.serial))
	job, err := restorejob.Build(restorejob.Spec{
		Name:          fmt.Sprintf("restore-claim-%d", h.serial),
		Namespace:     nsController,
		Origin:        restorejob.Origin{Kind: restorejob.OriginClaim, UID: uid},
		Owner:         metav1.OwnerReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "prime-" + string(uid), UID: uid},
		SnapshotID:    strings.Repeat("fedcba9876543210", 4),
		Claim:         "prime-" + string(uid),
		Repository:    "prime-" + string(uid) + "-restic",
		Image:         pinnedRestoreImage,
		CacheCapacity: ptr.To(resource.MustParse("2Gi")),
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	return job
}

// hostPathVolume returns a volume that mounts the node's root.
func hostPathVolume() corev1.Volume {
	return corev1.Volume{Name: "host", VolumeSource: corev1.VolumeSource{HostPath: &corev1.HostPathVolumeSource{Path: "/"}}}
}

// wantRefused fails the test unless err is the policy's 403 with the given
// message.
//
// Parameters:
//   - t records the failure.
//   - what names the write in the failure.
//   - err is the write's error.
//   - message is the text of the validation that must refuse it.
func wantRefused(t *testing.T, what string, err error, message string) {
	t.Helper()
	switch {
	case err == nil:
		t.Errorf("%s was admitted, want the policy to refuse it with %q", what, message)
	case !apierrors.IsForbidden(err) || !strings.Contains(err.Error(), fmt.Sprintf(policyRefusalFmt, policyName)):
		t.Errorf("%s = %v, want the policy's 403", what, err)
	case !strings.Contains(err.Error(), message):
		t.Errorf("%s = %v, want the message %q", what, err, message)
	}
}

// The controller's ServiceAccount runs a restore Job through its whole life
// under the policy: restorejob.Build's Job for a RestoreRun in a privileged
// and in a plain namespace, and for the populator, is created suspended,
// resumed, suspended again and deleted with Foreground propagation, each
// through restorejob's own API. Every step is admitted. The pod template's
// containers cannot change, and a change that drops the pod's restore label
// from a suspended Job is refused.
func TestEnvtestThePolicyAdmitsTheRestoreJob(t *testing.T) {
	ctx := context.Background()
	h := startPolicyHarness(t)

	for name, job := range map[string]*batchv1.Job{
		"a RestoreRun's Job in a privileged namespace": h.build(t, nsPrivileged, true),
		"a RestoreRun's Job in a plain namespace":      h.build(t, nsApp, false),
		"the populator's Job":                          h.buildForClaim(t),
	} {
		t.Run(name, func(t *testing.T) {
			if err := h.jobs.CreateJob(ctx, job); err != nil {
				t.Fatalf("create: %v", err)
			}
			if err := h.jobs.ResumeJob(ctx, job); err != nil {
				t.Errorf("resume: %v", err)
			}
			if err := h.jobs.SuspendJob(ctx, job); err != nil {
				t.Errorf("suspend: %v", err)
			}
			if err := h.jobs.DeleteJob(ctx, job); err != nil {
				t.Errorf("foreground delete: %v", err)
			}
		})
	}

	t.Run("a change to a suspended Job's pod template", func(t *testing.T) {
		job := h.build(t, nsApp, false)
		if err := h.jobs.CreateJob(ctx, job); err != nil {
			t.Fatalf("create: %v", err)
		}
		// The API server keeps a Job's containers immutable (validation.go of
		// the batch API), before any admission policy runs.
		image := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"template":{"spec":{"containers":[{"name":"restore","image":"busybox"}]}}}}`))
		if err := h.controller.Patch(ctx, job.DeepCopy(), image); !apierrors.IsInvalid(err) {
			t.Errorf("a patch of the restore container's image = %v, want the API server's 422", err)
		}
		// The pod template's labels may change while the Job is suspended, so
		// the policy checks them on every update.
		label := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"template":{"metadata":{"labels":{"app.kubernetes.io/component":"other"}}}}}`))
		wantRefused(t, "a patch of the pod's component label", h.controller.Patch(ctx, job.DeepCopy(), label), msgPodLabel)
	})
}

// Each Job here is restorejob.Build's Job with one change that would give its
// pod host access, an API token or privileges. The controller's ServiceAccount
// may not create any of them, and the refusal carries the message of the
// validation the change breaks.
func TestEnvtestThePolicyRefusesJobsOutOfShape(t *testing.T) {
	ctx := context.Background()
	h := startPolicyHarness(t)

	restore := func(job *batchv1.Job) *corev1.SecurityContext {
		return job.Spec.Template.Spec.Containers[0].SecurityContext
	}
	cases := []struct {
		name    string
		change  func(*batchv1.Job)
		message string
	}{
		{"no managed-by label", func(j *batchv1.Job) { delete(j.Labels, restorejob.LabelManagedBy) }, msgLabelled},
		{"a hostPath volume", func(j *batchv1.Job) {
			j.Spec.Template.Spec.Volumes = append(j.Spec.Template.Spec.Volumes, hostPathVolume())
		}, msgVolumes},
		{"a Secret volume", func(j *batchv1.Job) {
			j.Spec.Template.Spec.Volumes = append(j.Spec.Template.Spec.Volumes, corev1.Volume{Name: "s", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: "data-restic"}}})
		}, msgVolumes},
		{"a projected ServiceAccount token", func(j *batchv1.Job) {
			j.Spec.Template.Spec.Volumes = append(j.Spec.Template.Spec.Volumes, corev1.Volume{Name: "token", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{
				Sources: []corev1.VolumeProjection{{ServiceAccountToken: &corev1.ServiceAccountTokenProjection{Path: "token"}}},
			}}})
		}, msgVolumes},
		{"an ephemeral volume with a data source", func(j *batchv1.Job) {
			for _, v := range j.Spec.Template.Spec.Volumes {
				if v.Ephemeral != nil {
					v.Ephemeral.VolumeClaimTemplate.Spec.DataSource = &corev1.TypedLocalObjectReference{Kind: "PersistentVolumeClaim", Name: "someone-elses"}
				}
			}
		}, msgVolumes},
		{"hostNetwork", func(j *batchv1.Job) { j.Spec.Template.Spec.HostNetwork = true }, msgNoHost},
		{"hostPID", func(j *batchv1.Job) { j.Spec.Template.Spec.HostPID = true }, msgNoHost},
		{"a node name", func(j *batchv1.Job) { j.Spec.Template.Spec.NodeName = "node-1" }, msgNoHost},
		{"automountServiceAccountToken unset", func(j *batchv1.Job) { j.Spec.Template.Spec.AutomountServiceAccountToken = nil }, msgNoToken},
		{"automountServiceAccountToken true", func(j *batchv1.Job) { j.Spec.Template.Spec.AutomountServiceAccountToken = ptr.To(true) }, msgNoToken},
		{"the controller's ServiceAccount on the pod", func(j *batchv1.Job) { j.Spec.Template.Spec.ServiceAccountName = "backup-controller" }, msgServiceAcct},
		{"pod sysctls", func(j *batchv1.Job) {
			j.Spec.Template.Spec.SecurityContext = &corev1.PodSecurityContext{
				Sysctls: []corev1.Sysctl{{Name: "net.ipv4.ip_unprivileged_port_start", Value: "0"}},
			}
		}, msgPodSecurity},
		{"privileged", func(j *batchv1.Job) { restore(j).Privileged = ptr.To(true); restore(j).AllowPrivilegeEscalation = nil }, msgUnprivileged},
		{"allowPrivilegeEscalation unset", func(j *batchv1.Job) { restore(j).AllowPrivilegeEscalation = nil }, msgUnprivileged},
		{"capability SYS_ADMIN", func(j *batchv1.Job) {
			restore(j).Capabilities.Add = append(restore(j).Capabilities.Add, "SYS_ADMIN")
		}, msgUnprivileged},
		{"capability SYS_ADMIN on the init container", func(j *batchv1.Job) {
			sc := j.Spec.Template.Spec.InitContainers[0].SecurityContext
			sc.Capabilities.Add = append(sc.Capabilities.Add, "SYS_ADMIN")
		}, msgUnprivileged},
		{"a host port", func(j *batchv1.Job) {
			j.Spec.Template.Spec.Containers[0].Ports = []corev1.ContainerPort{{ContainerPort: 8080, HostPort: 8080}}
		}, msgUnprivileged},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			job := h.build(t, nsPrivileged, true)
			c.change(job)
			err := h.jobs.CreateJob(ctx, job)
			wantRefused(t, "create", err, c.message)
			if err == nil {
				_ = h.admin.Delete(ctx, job)
			}
		})
	}
}

// The policy matches only the controller's ServiceAccount, and only its Jobs.
// The administrator may create a Job with a hostPath volume. The controller's
// ServiceAccount may neither suspend nor delete a Job it did not create,
// which carries no restore labels.
func TestEnvtestThePolicyLeavesOtherUsersJobsAlone(t *testing.T) {
	ctx := context.Background()
	h := startPolicyHarness(t)

	theirs := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{Name: "theirs", Namespace: nsApp},
		Spec: batchv1.JobSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers:    []corev1.Container{{Name: "c", Image: "busybox", VolumeMounts: []corev1.VolumeMount{{Name: "host", MountPath: "/host"}}}},
			Volumes:       []corev1.Volume{hostPathVolume()},
		}}},
	}
	if err := h.admin.Create(ctx, theirs); err != nil {
		t.Fatalf("the administrator's hostPath Job = %v, want it admitted", err)
	}
	suspend := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"suspend":true}}`))
	wantRefused(t, "the ServiceAccount's suspend of another user's Job", h.controller.Patch(ctx, theirs.DeepCopy(), suspend), msgLabelled)
	wantRefused(t, "the ServiceAccount's delete of another user's Job", h.jobs.DeleteJob(ctx, theirs), msgLabelled)
}

// Under the shipped ClusterRole the controller's ServiceAccount changes a
// Deployment's replica count through the scale subresource, as quiesce
// does, and may not patch the Deployment itself, so it cannot change a
// workload's pod template.
func TestEnvtestTheControllerChangesWorkloadsOnlyThroughScale(t *testing.T) {
	ctx := context.Background()
	h := startPolicyHarness(t)

	labels := map[string]string{"app": "web"}
	deployment := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: "web", Namespace: nsApp},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To[int32](2),
			Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "web", Image: "nginx"}}},
			},
		},
	}
	if err := h.admin.Create(ctx, deployment); err != nil {
		t.Fatal(err)
	}

	scale := &autoscalingv1.Scale{}
	if err := h.controller.SubResource("scale").Get(ctx, deployment, scale); err != nil {
		t.Fatalf("the ServiceAccount's get of deployments/scale: %v", err)
	}
	scale.Spec.Replicas = 0
	if err := h.controller.SubResource("scale").Update(ctx, deployment, client.WithSubResourceBody(scale)); err != nil {
		t.Errorf("the ServiceAccount's update of deployments/scale: %v", err)
	}
	stored := &appsv1.Deployment{}
	if err := h.admin.Get(ctx, client.ObjectKeyFromObject(deployment), stored); err != nil {
		t.Fatal(err)
	}
	if got := ptr.Deref(stored.Spec.Replicas, -1); got != 0 {
		t.Errorf("replicas after the scale update = %d, want 0", got)
	}

	template := client.RawPatch(types.MergePatchType, []byte(`{"spec":{"template":{"spec":{"containers":[{"name":"web","image":"attacker"}]}}}}`))
	if err := h.controller.Patch(ctx, deployment.DeepCopy(), template); !apierrors.IsForbidden(err) {
		t.Errorf("the ServiceAccount's patch of the Deployment's template = %v, want RBAC's 403", err)
	}
}
