package runs

import (
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/quiesce"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// sharedKustomizationName is the Kustomization in flux-system that applies
// the notes and the wiki Deployment, so one Kustomization serves two
// namespaces.
const sharedKustomizationName = "apps"

// wikiNS names the second namespace, whose Deployment the shared
// Kustomization applies as well.
const wikiNS = "wiki"

// deploymentApplying returns a Deployment named name in namespace, marked for
// quiesce with two replicas and labeled as applied by the shared
// Kustomization.
func deploymentApplying(namespace, name string) *appsv1.Deployment {
	d := deployment()
	d.Namespace, d.Name = namespace, name
	d.Labels = map[string]string{quiesce.FluxNameLabel: sharedKustomizationName, quiesce.FluxNamespaceLabel: "flux-system"}
	return d
}

// sharedKustomization returns the Kustomization in flux-system that applies
// both Deployments, its inventory listing each, with spec.suspend set to the
// value given in suspended.
func sharedKustomization(suspended bool) *unstructured.Unstructured {
	k := kustomization(suspended)
	k.SetName(sharedKustomizationName)
	_ = unstructured.SetNestedSlice(k.Object, []any{
		map[string]any{"id": ns + "_" + appN + "_apps_Deployment", "v": "v1"},
		map[string]any{"id": wikiNS + "_wiki_apps_Deployment", "v": "v1"},
	}, "status", "inventory", "entries")
	return k
}

// sharedSuspended reports whether the shared Kustomization is suspended.
func sharedSuspended(t *testing.T, c client.Client) bool {
	t.Helper()
	k, ok := getUnstructured(t, c, quiesce.KustomizationGVK, "flux-system", sharedKustomizationName)
	if !ok {
		t.Fatal("the shared Kustomization is gone")
	}
	s, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend")
	return s
}

// sharedRefusal is what the Ready message of a run that refuses the shared
// Kustomization holds.
var sharedRefusal = []string{
	"Kustomization flux-system/" + sharedKustomizationName + " applies workloads in namespaces " + ns + " and " + wikiNS,
	"which would leave the other namespace's workloads unmanaged by Flux, so it refuses",
	"Give each namespace its own Kustomization",
}

// expectSharedRefusal checks that a run ended Failed with reason Invalid and
// the refusal of the shared Kustomization, and that nothing was stopped: the
// notes app is at 2, the Kustomization is as suspended says it was, and no
// quiesce Lease is left in the namespace.
func expectSharedRefusal(t *testing.T, c client.Client, phase backupv1alpha1.RunPhase, conditions []metav1.Condition, suspended bool) {
	t.Helper()
	if phase != backupv1alpha1.RunPhaseFailed || readyReason(conditions) != backupv1alpha1.ReasonInvalid {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed, Invalid", phase, readyReason(conditions), readyMessage(conditions))
	}
	for _, want := range sharedRefusal {
		if !strings.Contains(readyMessage(conditions), want) {
			t.Errorf("message = %q, want it to hold %q", readyMessage(conditions), want)
		}
	}
	if got := replicasOf(t, c); got != 2 {
		t.Errorf("replicas = %d, want the notes app left at 2", got)
	}
	if sharedSuspended(t, c) != suspended {
		t.Errorf("the shared Kustomization's suspend = %t, want it left at %t", !suspended, suspended)
	}
	if lease := quiesceLeaseIn(t, c, ns); lease != nil {
		t.Errorf("quiesce Lease = %+v, want it released", lease)
	}
}

// A namespace BackupRun whose app a Kustomization applies together with a
// workload of another namespace ends Invalid before it stops anything, and
// names the Kustomization and both namespaces. Suspending the Kustomization
// would leave the other namespace's workloads unmanaged by Flux, and a run
// there could resume it while this one holds its app at 0. That holds for a
// Kustomization another run has already suspended too. Before, the run took
// a Lease per Kustomization and waited for any other run holding one.
func TestANamespaceBackupRefusesAKustomizationSharedAcrossNamespaces(t *testing.T) {
	t.Parallel()
	for name, suspended := range map[string]bool{"running": false, "already suspended": true} {
		t.Run(name, func(t *testing.T) {
			r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
				deploymentApplying(ns, appN), deploymentApplying(wikiNS, "wiki"), sharedKustomization(suspended),
				claim(), volume(), volumeRestore(), repository())
			step(t, r) // plan
			step(t, r) // admit, no queue
			step(t, r) // quiesce

			run := readBackupRun(t, c)
			expectSharedRefusal(t, c, run.Status.Phase, run.Status.Conditions, suspended)
			if len(run.Status.Quiesced) != 0 || len(run.Status.SuspendedKustomizations) != 0 {
				t.Errorf("quiesced = %v, suspended = %v; want no plan", run.Status.Quiesced, run.Status.SuspendedKustomizations)
			}
		})
	}
}
