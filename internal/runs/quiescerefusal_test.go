package runs

import (
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// kustomizationWithEntries returns the app's Kustomization with the given
// status.inventory.entries. A nil list removes status.inventory.entries.
func kustomizationWithEntries(entries []any) *unstructured.Unstructured {
	k := kustomization(false)
	if entries == nil {
		unstructured.RemoveNestedField(k.Object, "status", "inventory", "entries")
		return k
	}
	_ = unstructured.SetNestedSlice(k.Object, entries, "status", "inventory", "entries")
	return k
}

// quiesceRefusalCase is one refusal from the quiesce package at one caller
// in internal/runs.
type quiesceRefusalCase struct {
	// objects are the objects in the cluster beside the run.
	objects []client.Object
	// want is a part of the Ready message that names the refusal.
	want string
}

// Each refusal of the quiesce package ends the run in the pass that meets
// it, with the reason that caller ends with: Invalid where the run plans or
// stops the workloads, and Failed where work reads spec.quiesce again after
// the stop. A caller that misses one would retry the error until the run's
// timeout.
func TestEveryQuiesceRefusalEndsTheRun(t *testing.T) {
	t.Parallel()
	crossNamespace := []client.Object{deploymentApplying(ns, appN), deploymentApplying(wikiNS, "wiki"), sharedKustomization(false)}
	inventoryMissing := []client.Object{deployment(), kustomizationWithEntries(nil)}
	entryMalformed := []client.Object{deployment(), kustomizationWithEntries([]any{map[string]any{"id": "notes-deployment", "v": "v1"}})}
	const crossText, missingText, malformedText = "applies workloads in namespaces", "has no list at status.inventory.entries", "whose id is not"

	t.Run("BackupRun quiesce", func(t *testing.T) {
		for name, tc := range map[string]quiesceRefusalCase{
			"CrossNamespaceError":    {objects: crossNamespace, want: crossText},
			"InventoryError missing": {objects: inventoryMissing, want: missingText},
			"InventoryError entry":   {objects: entryMalformed, want: malformedText},
		} {
			t.Run(name, func(t *testing.T) {
				objects := append([]client.Object{backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
					claim(), volume(), volumeRestore(), repository()}, tc.objects...)
				r, c := backupReconciler(t, objects...)
				step(t, r) // plan
				step(t, r) // admit, no queue
				step(t, r) // quiesce
				run := readBackupRun(t, c)
				expectQuiesceRefusal(t, run.Status.Phase, run.Status.Conditions, backupv1alpha1.ReasonInvalid, tc.want)
			})
		}
	})

	t.Run("RestoreRun plan", func(t *testing.T) {
		for name, ref := range map[string]backupv1alpha1.WorkloadRef{
			"SpecError kind":     {Kind: "DaemonSet", Name: appN},
			"SpecError workload": {Kind: "StatefulSet", Name: "notes-worker"},
		} {
			t.Run(name, func(t *testing.T) {
				run := quiescedRestore()
				run.Spec.Quiesce = append(run.Spec.Quiesce, ref)
				r, c := restoreReconciler(t, prober{saturday}, run,
					claim(), volumeRestore(), repository(), cluster(), objectStore(), storeSecret(),
					deployment(), kustomization(false), writerPod())
				restoreStep(t, r)
				got := readRestoreRun(t, c)
				expectQuiesceRefusal(t, got.Status.Phase, got.Status.Conditions, backupv1alpha1.ReasonInvalid, "spec.quiesce lists "+ref.Kind+" "+ref.Name)
			})
		}
	})

	t.Run("RestoreRun quiesce", func(t *testing.T) {
		for name, tc := range map[string]quiesceRefusalCase{
			"SpecError":              {objects: nil, want: "spec.quiesce lists Deployment " + appN + ", which this namespace does not hold"},
			"CrossNamespaceError":    {objects: crossNamespace, want: crossText},
			"InventoryError missing": {objects: inventoryMissing, want: missingText},
			"InventoryError entry":   {objects: entryMalformed, want: malformedText},
		} {
			t.Run(name, func(t *testing.T) {
				objects := append([]client.Object{quiescedRestoreOf(), claim(), volumeRestore(), repository()}, tc.objects...)
				r, c := restoreReconciler(t, nil, objects...)
				restoreStep(t, r)
				got := readRestoreRun(t, c)
				expectQuiesceRefusal(t, got.Status.Phase, got.Status.Conditions, backupv1alpha1.ReasonInvalid, tc.want)
			})
		}
	})

	t.Run("RestoreRun work after the stop", func(t *testing.T) {
		run := checkedRestore(func(r *backupv1alpha1.RestoreRun) {
			r.Spec.Claim = claimN
			r.Spec.Quiesce = []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: appN}}
			r.Status.QuiescedAt = atFrozen(0)
			r.Status.Quiesced = []backupv1alpha1.QuiescedWorkload{{Kind: "Deployment", Name: appN, Replicas: 2}}
		})
		r, c := restoreReconciler(t, nil, run, claim(), volumeRestore(), repository())
		restoreStep(t, r)
		got := readRestoreRun(t, c)
		expectQuiesceRefusal(t, got.Status.Phase, got.Status.Conditions, backupv1alpha1.ReasonFailed,
			"spec.quiesce lists Deployment "+appN+", which this namespace does not hold")
	})
}

// expectQuiesceRefusal checks that a run ended Failed with the given reason
// and a Ready message that holds want.
func expectQuiesceRefusal(t *testing.T, phase backupv1alpha1.RunPhase, conditions []metav1.Condition, reason, want string) {
	t.Helper()
	if phase != backupv1alpha1.RunPhaseFailed || readyReason(conditions) != reason {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed, %s", phase, readyReason(conditions), readyMessage(conditions), reason)
	}
	if !strings.Contains(readyMessage(conditions), want) {
		t.Errorf("message = %q, want it to hold %q", readyMessage(conditions), want)
	}
}
