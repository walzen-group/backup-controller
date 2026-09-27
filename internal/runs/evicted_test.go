package runs

import (
	"context"
	"errors"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/kueue"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// setWorkloadConditions replaces the conditions of the run's Workload, as
// Kueue does when it evicts the Workload or clears its admission.
func setWorkloadConditions(t *testing.T, c client.Client, conditions ...map[string]any) {
	t.Helper()
	workload, ok := getUnstructured(t, c, kueue.WorkloadGVK, ns, kueue.WorkloadName(runUID))
	if !ok {
		t.Fatal("the run has no Workload")
	}
	list := make([]any, 0, len(conditions))
	for _, condition := range conditions {
		condition["lastTransitionTime"] = "2026-09-24T12:05:00Z"
		list = append(list, condition)
	}
	_ = unstructured.SetNestedSlice(workload.Object, list, "status", "conditions")
	if err := c.Status().Update(context.Background(), workload); err != nil {
		t.Fatal(err)
	}
}

// runningQuiescedRun starts a namespace run through Kueue and stops the app,
// so the run holds the app down when the test changes its Workload.
func runningQuiescedRun(t *testing.T) (*BackupRunReconciler, client.Client) {
	t.Helper()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false), localQueueObject())
	step(t, r) // plan
	step(t, r) // admit: creates the Workload and waits
	admitAll(t, c)
	step(t, r) // admitted: Running
	step(t, r) // quiesce
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 0 {
		t.Fatalf("replicas = %d after the quiesce pass, want 0", *d.Spec.Replicas)
	}
	return r, c
}

// A Running run whose Workload Kueue evicts, or whose admission Kueue
// clears, gives the app back, deletes the Workload so Kueue frees the quota,
// and ends Failed with reason Evicted on the run and on each unfinished item.
func TestAnEvictedRunGivesTheAppBackAndEnds(t *testing.T) {
	cases := []struct {
		name       string
		conditions []map[string]any
	}{
		{"Evicted is True", []map[string]any{
			{"type": "Admitted", "status": "True", "reason": "Admitted", "message": ""},
			{"type": "QuotaReserved", "status": "True", "reason": "QuotaReserved", "message": ""},
			{"type": "Evicted", "status": "True", "reason": "Preempted", "message": "Preempted to accommodate a workload"},
		}},
		{"Admitted is False", []map[string]any{
			{"type": "Admitted", "status": "False", "reason": "NoReservation", "message": ""},
			{"type": "QuotaReserved", "status": "False", "reason": "Pending", "message": ""},
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, c := runningQuiescedRun(t)
			setWorkloadConditions(t, c, tc.conditions...)
			step(t, r)

			run := readBackupRun(t, c)
			ready := apimeta.FindStatusCondition(run.Status.Conditions, backupv1alpha1.ConditionReady)
			if run.Status.Phase != backupv1alpha1.RunPhaseFailed || ready == nil || ready.Reason != backupv1alpha1.ReasonEvicted {
				t.Fatalf("phase = %q, Ready = %+v, want Failed with reason Evicted", run.Status.Phase, ready)
			}
			for _, item := range run.Status.Items {
				if item.Phase == backupv1alpha1.ItemFailed && item.Reason != backupv1alpha1.ItemReasonEvicted {
					t.Errorf("item %s reason = %q, want Evicted", item.Name, item.Reason)
				}
			}
			d := &appsv1.Deployment{}
			get(t, c, ns, appN, d)
			if *d.Spec.Replicas != 2 {
				t.Errorf("replicas = %d after the eviction, want 2 back", *d.Spec.Replicas)
			}
			if _, ok := getUnstructured(t, c, kueue.WorkloadGVK, ns, kueue.WorkloadName(runUID)); ok {
				t.Error("the Workload is still there, and Kueue keeps its quota reserved")
			}
		})
	}
}

// A Running run whose Workload the controller cannot read goes on with its
// pass, so the max-quiesce limit and the timeout still give the app back.
func TestARunWhoseWorkloadReadFailsGoesOn(t *testing.T) {
	r, c := runningQuiescedRun(t)
	r.Client = interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if obj.GetObjectKind().GroupVersionKind().Kind == kueue.WorkloadGVK.Kind {
				return apierrors.NewForbidden(schema.GroupResource{Group: kueue.WorkloadGVK.Group, Resource: "workloads"}, key.Name, errors.New("RBAC lost"))
			}
			return cl.Get(ctx, key, obj, opts...)
		},
	})
	step(t, r)
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseRunning {
		t.Fatalf("phase = %q, want Running", run.Status.Phase)
	}
}
