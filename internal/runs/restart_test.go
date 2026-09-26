package runs

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// The API groups of Flux's kustomize-controller and of Kueue.
const (
	fluxGroup  = "kustomize.toolkit.fluxcd.io"
	kueueGroup = "kueue.x-k8s.io"
)

// servedBackupReconciler returns a BackupRunReconciler over a strict client
// that holds the given objects and fails every call for a kind the API
// server does not serve, as a real client does (see servingOnly). The groups
// in removed are served by no CRD from the start. It also returns the strict
// client underneath, which the test's own reads and writes go through, and
// its mapper, through which a test removes a group later.
func servedBackupReconciler(t *testing.T, removed []string, objects ...client.Object) (*BackupRunReconciler, client.Client, *servedKinds) {
	t.Helper()
	c := newClient(t, objects...)
	kinds := c.RESTMapper().(*servedKinds)
	kinds.remove(removed...)
	serving := servingOnly(c)
	return &BackupRunReconciler{Client: serving, Reader: serving, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: func() time.Time { return frozen }}, c, kinds
}

// tryStep reconciles the BackupRun before-upgrade once and returns the
// reconcile's error.
func tryStep(r *BackupRunReconciler) error {
	_, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "before-upgrade"}})
	return err
}

// removeFlux stands in for deleting Flux's CRDs from the cluster: the
// Kustomization goes with its CRD, and the API server serves the kind no
// more.
func removeFlux(t *testing.T, c client.Client, kinds *servedKinds) {
	t.Helper()
	if err := c.Delete(context.Background(), kustomization(true)); err != nil {
		t.Fatalf("delete the Kustomization: %v", err)
	}
	kinds.remove(fluxGroup)
}

// A namespace run that suspended the app's Kustomization, on a cluster from
// which Flux's CRDs have since been removed, gives the app its replicas back
// once the clone is cut and finishes. No version of Kustomization is served,
// so there is nothing left to resume.
func TestARunGivesTheAppBackAfterFluxIsRemoved(t *testing.T) {
	r, c, kinds := servedBackupReconciler(t, nil, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start
	if replicas := replicasOf(t, c); replicas != 0 {
		t.Fatalf("replicas = %d after the quiesce pass, want 0", replicas)
	}

	removeFlux(t, c, kinds)
	cutClone(t, c)
	if err := tryStep(r); err != nil {
		t.Fatalf("restart pass: %v", err)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Fatalf("replicas = %d after the restart pass, want 2 back", replicas)
	}

	complete(t, c, "snapshot 6e473100 saved")
	if err := tryStep(r); err != nil {
		t.Fatalf("collect pass: %v", err)
	}
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}

// A namespace run deleted while it holds the app down, after Flux's CRDs were
// removed, gives the app back and lets its deletion complete.
func TestADeletedRunGivesTheAppBackAfterFluxIsRemoved(t *testing.T) {
	r, c, kinds := servedBackupReconciler(t, nil, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce

	removeFlux(t, c, kinds)
	if err := c.Delete(context.Background(), readBackupRun(t, c)); err != nil {
		t.Fatalf("delete the run: %v", err)
	}
	if err := tryStep(r); err != nil {
		t.Fatalf("finalize pass: %v", err)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Fatalf("replicas = %d after the run was deleted, want 2 back", replicas)
	}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "before-upgrade"}, &backupv1alpha1.BackupRun{}); !apierrors.IsNotFound(err) {
		t.Fatalf("get the run after its finalize pass: %v, want it gone", err)
	}
}

// On a cluster without Flux, a Deployment that still carries
// kustomize-controller's labels is stopped with nothing suspended, and the
// run goes on.
func TestANamespaceIsQuiescedWithoutFlux(t *testing.T) {
	r, c, _ := servedBackupReconciler(t, []string{fluxGroup}, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment())
	step(t, r) // plan
	step(t, r) // admit, no queue
	if err := tryStep(r); err != nil {
		t.Fatalf("quiesce pass: %v", err)
	}
	run := readBackupRun(t, c)
	if run.Status.QuiescedAt == nil || len(run.Status.SuspendedKustomizations) != 0 {
		t.Fatalf("quiescedAt = %v, suspended = %v, want the app stopped with nothing suspended", run.Status.QuiescedAt, run.Status.SuspendedKustomizations)
	}
	if replicas := replicasOf(t, c); replicas != 0 {
		t.Fatalf("replicas = %d after the quiesce pass, want 0", replicas)
	}
}

// On a cluster without Kueue's CRDs a run starts without admission and has
// no Workload to delete, so a volume run finishes.
func TestASourceRunFinishesWithoutKueue(t *testing.T) {
	r, c, _ := servedBackupReconciler(t, []string{kueueGroup}, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository())
	step(t, r) // plan
	step(t, r) // admit: no Kueue, so it starts at once
	step(t, r) // start
	complete(t, c, "snapshot 6e473100 saved")
	if err := tryStep(r); err != nil {
		t.Fatalf("collect pass: %v", err)
	}
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Fatalf("phase = %q (%s), want Succeeded", run.Status.Phase, readyMessage(run.Status.Conditions))
	}
}

// A restart the API server keeps refusing keeps the run unfinished. The run
// says why on its Ready condition, with reason RestartFailed, a message that
// names the Deployment and the replicas it is owed, and one Warning event. It
// retries on every pass, and the first pass the API server accepts gives the
// app back and finishes the run.
func TestARestartThatKeepsFailingIsReportedAndRetried(t *testing.T) {
	r, c, _ := servedBackupReconciler(t, nil, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	refuse := false
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && refuse {
				if data, _ := patch.Data(obj); !strings.Contains(string(data), `"replicas":0`) {
					return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, obj.GetName(), errors.New("a policy refuses the change"))
				}
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start

	refuse = true
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	r.Now = func() time.Time { return frozen.Add(2 * time.Hour) } // past the one-hour timeout
	for pass := 1; pass <= 2; pass++ {
		if err := tryStep(r); err == nil {
			t.Fatalf("pass %d: the reconcile returned no error, want the refused restart retried", pass)
		}
		run := readBackupRun(t, c)
		if run.Status.Phase.Finished() || run.Status.RestartedAt != nil {
			t.Fatalf("pass %d: phase = %q, restartedAt = %v, want the run unfinished and the restart still owed", pass, run.Status.Phase, run.Status.RestartedAt)
		}
		if reason := readyReason(run.Status.Conditions); reason != backupv1alpha1.ReasonRestartFailed {
			t.Fatalf("pass %d: reason = %q, want %s", pass, reason, backupv1alpha1.ReasonRestartFailed)
		}
		message := readyMessage(run.Status.Conditions)
		for _, want := range []string{"Deployment " + appN, "2 replicas", "a policy refuses the change", Finalizer} {
			if !strings.Contains(message, want) {
				t.Errorf("pass %d: message %q does not name %q", pass, message, want)
			}
		}
	}
	got := recorded(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Warning "+backupv1alpha1.ReasonRestartFailed+" ") {
		t.Fatalf("events = %q, want one Warning %s", got, backupv1alpha1.ReasonRestartFailed)
	}
	if replicas := replicasOf(t, c); replicas != 0 {
		t.Fatalf("replicas = %d while the API server refuses the restart, want 0", replicas)
	}

	refuse = false
	if err := tryStep(r); err != nil {
		t.Fatalf("pass after the refusal ended: %v", err)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Fatalf("replicas = %d once the API server accepts the restart, want 2 back", replicas)
	}
	k, _ := getUnstructured(t, c, KustomizationGVK, "flux-system", appN)
	if suspended, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend"); suspended {
		t.Error("the Kustomization the run suspended was not resumed")
	}
	run := readBackupRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonFailed {
		t.Fatalf("phase = %q, reason = %q, want the timed-out run Failed", run.Status.Phase, readyReason(run.Status.Conditions))
	}
}

// restartWorkloads resumes a Kustomization at the version the API server
// serves, here v2 only, and does not assume v1.
func TestRestartResumesAKustomizationAtTheServedVersion(t *testing.T) {
	v2 := schema.GroupVersionKind{Group: fluxGroup, Version: "v2", Kind: "Kustomization"}
	s := runtime.NewScheme()
	if err := appsv1.AddToScheme(s); err != nil {
		t.Fatal(err)
	}
	s.AddKnownTypeWithName(v2, &unstructured.Unstructured{})
	s.AddKnownTypeWithName(v2.GroupVersion().WithKind("KustomizationList"), &unstructured.UnstructuredList{})
	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{v2.GroupVersion()})
	mapper.Add(v2, meta.RESTScopeNamespace)

	k := kustomization(true)
	k.SetGroupVersionKind(v2)
	inner := strictclient.Build(fake.NewClientBuilder().WithObjects(k).WithRESTMapper(mapper), s, strictclient.Options{Clock: func() time.Time { return frozen }})
	c := servingOnly(inner)

	if err := restartWorkloads(context.Background(), c, ns, nil, []string{"flux-system/" + appN}); err != nil {
		t.Fatalf("restartWorkloads: %v", err)
	}
	got, ok := getUnstructured(t, c, v2, "flux-system", appN)
	if !ok {
		t.Fatal("the v2 Kustomization is gone")
	}
	if suspended, _, _ := unstructured.NestedBool(got.Object, "spec", "suspend"); suspended {
		t.Error("the Kustomization served only at v2 was not resumed")
	}
}

// failingMapper is a RESTMapper whose every lookup fails with err, as the
// manager's mapper does when a discovery call fails.
type failingMapper struct {
	meta.RESTMapper
	err error
}

// RESTMapping returns the mapper's error.
func (m failingMapper) RESTMapping(schema.GroupKind, ...string) (*meta.RESTMapping, error) {
	return nil, m.err
}

// Only a lookup that finds no served version of a kind counts as the kind
// being gone. A failed discovery call, including one that failed for only
// some versions of the group, is an error the caller retries, so nothing is
// skipped on it.
func TestOnlyAKindNoVersionOfWhichIsServedIsGone(t *testing.T) {
	gk := KustomizationGVK.GroupKind()
	partial := apiutil.ErrResourceDiscoveryFailed{
		{Group: fluxGroup, Version: "v1"}: &meta.NoResourceMatchError{PartialResource: schema.GroupVersionResource{Group: fluxGroup, Version: "v1"}},
	}
	for _, tc := range []struct {
		name string
		err  error
		gone bool
	}{
		{"no version served", &meta.NoKindMatchError{GroupKind: gk}, true},
		{"discovery failed", errors.New("failed to get server groups: connection refused"), false},
		{"discovery failed for some versions", &partial, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := servedKind(failingMapper{err: tc.err}, gk)
			if err == nil || apierrors.IsNotFound(err) != tc.gone {
				t.Fatalf("servedKind error = %v, want gone = %t", err, tc.gone)
			}
			c := servingOnly(strictclient.Build(fake.NewClientBuilder().WithRESTMapper(failingMapper{err: tc.err}), runtime.NewScheme(), strictclient.Options{Clock: func() time.Time { return frozen }}))
			if err := deleteWorkload(context.Background(), c, ns, runUID); (err == nil) != tc.gone {
				t.Errorf("deleteWorkload error = %v, want an error only when the lookup failed", err)
			}
		})
	}
}

// A run that has given back everything it stopped, and then cannot release
// its Leases or delete its Kueue Workload, says which of the two failed and
// gives advice for that. It never tells a person to scale workloads or
// resume Kustomizations, which the run has already done.
func TestAReleaseFailureNamesWhatFailed(t *testing.T) {
	refused := errors.New("a policy refuses the delete")
	for name, tc := range map[string]struct {
		refuse func(client.Object) bool
		names  []string
	}{
		"Lease": {
			refuse: func(obj client.Object) bool { _, ok := obj.(*coordinationv1.Lease); return ok },
			names:  []string{"Lease", labelLeaseHolderUID},
		},
		"Workload": {
			refuse: func(obj client.Object) bool {
				u, ok := obj.(*unstructured.Unstructured)
				return ok && u.GroupVersionKind().GroupKind() == WorkloadGVK.GroupKind()
			},
			names: []string{"Workload " + workloadName(runUID)},
		},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
				claim(), volume(), volumeRestore(), repository())
			r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if tc.refuse(obj) {
						return apierrors.NewForbidden(schema.GroupResource{Resource: name}, obj.GetName(), refused)
					}
					return cl.Delete(ctx, obj, opts...)
				},
			})
			step(t, r) // plan
			step(t, r) // admit, no queue
			step(t, r) // start: the run takes the Leases
			complete(t, c, "snapshot 6e473100 saved")
			if err := tryStep(r); err == nil {
				t.Fatal("the collect pass returned no error, want the refused delete retried")
			}

			run := readBackupRun(t, c)
			if run.Status.Phase.Finished() || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonRestartFailed {
				t.Fatalf("phase = %q, reason = %q, want the run unfinished with %s", run.Status.Phase, readyReason(run.Status.Conditions), backupv1alpha1.ReasonRestartFailed)
			}
			message := readyMessage(run.Status.Conditions)
			for _, want := range append(tc.names, refused.Error()) {
				if !strings.Contains(message, want) {
					t.Errorf("message %q does not name %q", message, want)
				}
			}
			for _, wrong := range []string{"status.quiesced", "replicas", "put back what the run changed"} {
				if strings.Contains(message, wrong) {
					t.Errorf("message %q says %q, which is advice for another failure", message, wrong)
				}
			}
		})
	}
}

// A restart that fails in the pass that gives the app back after the clones
// are cut, long before the run's timeout, is reported at once: Ready turns
// False with reason RestartFailed and the message finish writes, with one
// Warning event. Each later pass tries again, and the first one the API
// server accepts gives the app back and goes on backing up.
func TestARestartThatFailsBeforeTheTimeoutIsReported(t *testing.T) {
	r, c, _ := servedBackupReconciler(t, nil, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	refuse := false
	r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if _, ok := obj.(*appsv1.Deployment); ok && refuse {
				if data, _ := patch.Data(obj); !strings.Contains(string(data), `"replicas":0`) {
					return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, obj.GetName(), errors.New("a policy refuses the change"))
				}
			}
			return cl.Patch(ctx, obj, patch, opts...)
		},
	})
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // quiesce
	step(t, r) // start
	cutClone(t, c)

	refuse = true
	recorder := events.NewFakeRecorder(10)
	r.Recorder = recorder
	for pass := 1; pass <= 2; pass++ {
		if err := tryStep(r); err == nil {
			t.Fatalf("pass %d: the reconcile returned no error, want the refused restart retried", pass)
		}
		run := readBackupRun(t, c)
		if !run.Status.RestartPending || run.Status.Phase.Finished() {
			t.Fatalf("pass %d: restartPending = %t, phase = %q, want the restart still owed", pass, run.Status.RestartPending, run.Status.Phase)
		}
		if reason := readyReason(run.Status.Conditions); reason != backupv1alpha1.ReasonRestartFailed {
			t.Fatalf("pass %d: reason = %q, want %s", pass, reason, backupv1alpha1.ReasonRestartFailed)
		}
		message := readyMessage(run.Status.Conditions)
		for _, want := range []string{"Deployment " + appN, "2 replicas", "a policy refuses the change", Finalizer} {
			if !strings.Contains(message, want) {
				t.Errorf("pass %d: message %q does not name %q", pass, message, want)
			}
		}
	}
	got := recorded(recorder)
	if len(got) != 1 || !strings.HasPrefix(got[0], "Warning "+backupv1alpha1.ReasonRestartFailed+" ") {
		t.Fatalf("events = %q, want one Warning %s", got, backupv1alpha1.ReasonRestartFailed)
	}

	refuse = false
	if err := tryStep(r); err != nil {
		t.Fatalf("pass after the refusal ended: %v", err)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Fatalf("replicas = %d once the API server accepts the restart, want 2 back", replicas)
	}
	run := readBackupRun(t, c)
	if run.Status.RestartPending || readyReason(run.Status.Conditions) == backupv1alpha1.ReasonRestartFailed {
		t.Errorf("restartPending = %t, reason = %q after the restart went through, want the run backing up again",
			run.Status.RestartPending, readyReason(run.Status.Conditions))
	}
}

// staleFluxReconciler returns a BackupRunReconciler whose client looks
// versions up through controller-runtime's lazy RESTMapper over a discovery
// endpoint that serves Kustomization at v1 (see fluxServedAt), and the
// endpoint, so the test can switch Flux to v2 mid-run. It also returns the
// strict client underneath, which stores the Kustomization at v1.
func staleFluxReconciler(t *testing.T, aggregated bool, objects ...client.Object) (*BackupRunReconciler, client.Client, *fluxDiscovery) {
	t.Helper()
	c := newClient(t, objects...)
	d := newFluxDiscovery(t, "v1", aggregated)
	served := fluxServedAt(c, d, d.mapper(t))
	return &BackupRunReconciler{Client: served, Reader: served, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: func() time.Time { return frozen }}, c, d
}

// A Flux upgrade that stops serving the Kustomization version the controller
// looked up and cached, while a run holds the app down, never leaves the
// Kustomization suspended. The API server answers the resume at the cached
// version with a 404 for the resource, which is not a deleted
// Kustomization: the pass reports RestartFailed and the mapper looks the
// version up again, and the next pass resumes the Kustomization at the new
// version.
func TestAKustomizationVersionFluxStopsServingIsNotTakenForGone(t *testing.T) {
	for name, aggregated := range map[string]bool{"aggregated discovery": true, "legacy discovery": false} {
		t.Run(name, func(t *testing.T) {
			r, c, d := staleFluxReconciler(t, aggregated, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
			step(t, r) // plan
			step(t, r) // admit, no queue
			step(t, r) // quiesce: the mapper caches v1
			step(t, r) // start
			if !suspended(t, c) {
				t.Fatal("the quiesce pass did not suspend the Kustomization")
			}

			d.serve("v2")
			cutClone(t, c)
			if err := tryStep(r); err == nil {
				t.Fatal("the restart pass returned no error, want the resume at the unserved version retried")
			}
			run := readBackupRun(t, c)
			if reason := readyReason(run.Status.Conditions); reason != backupv1alpha1.ReasonRestartFailed || !run.Status.RestartPending {
				t.Fatalf("reason = %q, restartPending = %t after the failed resume, want %s and the restart still owed",
					reason, run.Status.RestartPending, backupv1alpha1.ReasonRestartFailed)
			}
			if message := readyMessage(run.Status.Conditions); !strings.Contains(message, "Kustomization flux-system/"+appN) {
				t.Errorf("message %q does not name the Kustomization", message)
			}

			if err := tryStep(r); err != nil {
				t.Fatalf("pass after the mapper looked v2 up: %v", err)
			}
			if suspended(t, c) {
				t.Fatal("the Kustomization stayed suspended after Flux moved to v2")
			}
			if run := readBackupRun(t, c); run.Status.RestartPending || readyReason(run.Status.Conditions) == backupv1alpha1.ReasonRestartFailed {
				t.Errorf("restartPending = %t, reason = %q, want the restart done", run.Status.RestartPending, readyReason(run.Status.Conditions))
			}
			if replicas := replicasOf(t, c); replicas != 2 {
				t.Errorf("replicas = %d, want 2 back", replicas)
			}
		})
	}
}

// A Flux upgrade that stops serving the cached Kustomization version before
// a run stops the app does not make the run stop the app with its
// Kustomization left running, which would scale the app back up mid-backup.
// The quiesce pass fails and stops nothing; the next one suspends the
// Kustomization at the new version.
func TestAQuiesceAfterFluxMovesVersionStillSuspends(t *testing.T) {
	r, c, d := staleFluxReconciler(t, true, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	if _, err := r.RESTMapper().RESTMapping(KustomizationGVK.GroupKind()); err != nil {
		t.Fatal(err)
	}
	d.serve("v2")
	step(t, r) // plan
	step(t, r) // admit, no queue
	if err := tryStep(r); err == nil {
		t.Fatal("the quiesce pass returned no error, want the read at the unserved version retried")
	}
	if run := readBackupRun(t, c); len(run.Status.Quiesced) != 0 || replicasOf(t, c) != 2 {
		t.Fatalf("quiesced = %v, replicas = %d after the failed read, want nothing stopped", run.Status.Quiesced, replicasOf(t, c))
	}
	step(t, r) // quiesce at v2
	run := readBackupRun(t, c)
	if len(run.Status.SuspendedKustomizations) != 1 || !suspended(t, c) {
		t.Fatalf("suspended = %v, want the Kustomization suspended at the new version", run.Status.SuspendedKustomizations)
	}
}

// A run whose restart the API server refuses still releases its Leases, so
// a restore of the claim need not wait for the app to come back; otherMover
// still keeps a restore off a claim whose mover runs. When the Lease delete
// fails as well, the Ready message names both failures.
func TestARefusedRestartStillReleasesTheLeases(t *testing.T) {
	for name, refuseLeases := range map[string]bool{"Leases deleted": false, "Lease delete refused too": true} {
		t.Run(name, func(t *testing.T) {
			r, c, _ := servedBackupReconciler(t, nil, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
			refuse := false
			r.Client = interceptor.NewClient(r.Client.(client.WithWatch), interceptor.Funcs{
				Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
					if _, ok := obj.(*appsv1.Deployment); ok && refuse {
						if data, _ := patch.Data(obj); !strings.Contains(string(data), `"replicas":0`) {
							return apierrors.NewForbidden(schema.GroupResource{Group: "apps", Resource: "deployments"}, obj.GetName(), errors.New("a policy refuses the change"))
						}
					}
					return cl.Patch(ctx, obj, patch, opts...)
				},
				Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					if _, ok := obj.(*coordinationv1.Lease); ok && refuse && refuseLeases {
						return apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, obj.GetName(), errors.New("a policy refuses the delete"))
					}
					return cl.Delete(ctx, obj, opts...)
				},
			})
			step(t, r) // plan
			step(t, r) // admit, no queue
			step(t, r) // quiesce
			step(t, r) // start: the run takes the Leases
			claimLease, repoLease := leaseNames(t, c)
			if leaseHolderOf(t, c, claimLease) != string(runUID) || leaseHolderOf(t, c, repoLease) != string(runUID) {
				t.Fatal("the run does not hold the claim and repository Leases after the start pass")
			}

			refuse = true
			r.Now = func() time.Time { return frozen.Add(2 * time.Hour) } // past the one-hour timeout
			if err := tryStep(r); err == nil {
				t.Fatal("the reconcile returned no error, want the refused restart retried")
			}
			run := readBackupRun(t, c)
			if run.Status.Phase.Finished() || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonRestartFailed {
				t.Fatalf("phase = %q, reason = %q, want the run unfinished with %s", run.Status.Phase, readyReason(run.Status.Conditions), backupv1alpha1.ReasonRestartFailed)
			}
			message := readyMessage(run.Status.Conditions)
			if !strings.Contains(message, "a policy refuses the change") {
				t.Errorf("message %q does not name the refused restart", message)
			}
			if refuseLeases {
				if !strings.Contains(message, "a policy refuses the delete") || !strings.Contains(message, labelLeaseHolderUID) {
					t.Errorf("message %q does not name the refused Lease delete as well", message)
				}
				return
			}
			if holder := leaseHolderOf(t, c, claimLease) + leaseHolderOf(t, c, repoLease); holder != "" {
				t.Errorf("a Lease is still held by %q after the refused restart, want both released", holder)
			}
		})
	}
}
