package runs

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"github.com/walzen-group/backup-controller/internal/kueue"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// The tests in this file stand in for a release of another project that
// moves or renames a field the controller reads, and check that the run
// fails loudly, naming the field, where reading the field as unset would
// make it act on a wrong answer.

// quiesceRun steps a spec.all BackupRun through plan, admission and
// the quiesce pass, ignoring the errors of each pass, and returns it.
func quiesceRun(t *testing.T, r *BackupRunReconciler) {
	t.Helper()
	for range 3 {
		_ = tryStep(r)
	}
}

// A Kustomization that applies the app, going by the app's
// kustomize-controller labels, but carries no status.inventory.entries is
// refused before anything stops: kustomize-controller records every object
// it applies there, so a missing list means a Flux release moved it, and
// reading it as "not listed" would stop the app with Flux still free to
// scale it back up mid-backup.
func TestAKustomizationWithoutItsInventoryIsRefused(t *testing.T) {
	k := kustomization(false)
	unstructured.RemoveNestedField(k.Object, "status")
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), k)
	quiesceRun(t, r)

	run := readBackupRun(t, c)
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Errorf("replicas = %d, want the app left running", replicas)
	}
	if message := readyMessage(run.Status.Conditions); !strings.Contains(message, "status.inventory.entries") {
		t.Errorf("Ready = %s: %q, want it to name status.inventory.entries", readyReason(run.Status.Conditions), message)
	}
}

// An inventory entry whose id is not "<namespace>_<name>_<group>_<kind>" is
// refused the same way.
func TestAnInventoryEntryThatDoesNotParseIsRefused(t *testing.T) {
	k := kustomization(false)
	_ = unstructured.SetNestedSlice(k.Object, []any{map[string]any{"id": "notes/notes/apps/Deployment", "v": "v1"}}, "status", "inventory", "entries")
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), k)
	quiesceRun(t, r)

	run := readBackupRun(t, c)
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Errorf("replicas = %d, want the app left running", replicas)
	}
	if message := readyMessage(run.Status.Conditions); !strings.Contains(message, "status.inventory.entries") || !strings.Contains(message, "notes/notes/apps/Deployment") {
		t.Errorf("Ready = %s: %q, want it to name the field and the entry", readyReason(run.Status.Conditions), message)
	}
}

// kustomizationCRDWithoutSuspend writes the pinned Kustomization CRD without
// spec.suspend, as after a Flux release that renames the field, and returns
// newClient's CRD files with it in place of the pinned one.
func kustomizationCRDWithoutSuspend(t *testing.T) []string {
	t.Helper()
	pinned := crdDir + "flux/kustomize-controller.crds.yaml"
	crd := readCRD(t, pinned)
	versions, _, _ := unstructured.NestedSlice(crd.Object, "spec", "versions")
	for _, v := range versions {
		unstructured.RemoveNestedField(v.(map[string]any), "schema", "openAPIV3Schema", "properties", "spec", "properties", "suspend")
	}
	if err := unstructured.SetNestedSlice(crd.Object, versions, "spec", "versions"); err != nil {
		t.Fatal(err)
	}
	data, err := yaml.Marshal(crd.Object)
	if err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(t.TempDir(), "kustomizations.yaml")
	if err := os.WriteFile(out, data, 0o600); err != nil {
		t.Fatal(err)
	}
	files := make([]string, 0, len(crds))
	for _, path := range crds {
		if path == pinned {
			path = out
		}
		files = append(files, path)
	}
	return files
}

// A suspend the API server drops, because its schema no longer has
// spec.suspend, is an error that names the field: the run reads the
// Kustomization back from the patch and never stops the app behind a
// Kustomization that is still reconciling.
func TestASuspendTheAPIServerDropsIsAnError(t *testing.T) {
	c := newClientWithCRDs(t, kustomizationCRDWithoutSuspend(t), backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), deployment(), kustomization(false))
	r := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	step(t, r) // plan
	step(t, r) // admit, no queue
	err := tryStep(r)
	run := readBackupRun(t, c)
	if err == nil && !strings.Contains(readyMessage(run.Status.Conditions), "spec.suspend") {
		t.Errorf("the quiesce pass = %v, Ready = %s: %q; want an error naming spec.suspend", err, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	if err != nil && !strings.Contains(err.Error(), "spec.suspend") {
		t.Errorf("the quiesce pass = %v, want an error naming spec.suspend", err)
	}
	if replicas := replicasOf(t, c); replicas != 2 {
		t.Errorf("replicas = %d, want the app left running", replicas)
	}
}

// A run that Kueue never admits, as when a Kueue release renames the
// Admitted condition or the queueName field, does not wait forever holding
// the namespace's schedule: once its timeout has passed since its creation
// it fails, naming Kueue and its Workload, and gives the quota back. Its
// items record the reason TimedOut.
func TestAQueuedRunThatKueueNeverAdmitsFails(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Source = claimN }),
		claim(), volume(), volumeRestore(), repository(), localQueueObject())
	step(t, r) // plan
	step(t, r) // admit: the Workload waits
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Fatalf("phase = %q, want Queued", run.Status.Phase)
	}
	step(t, r) // still waiting within the timeout
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Fatalf("phase = %q within the timeout, want Queued", run.Status.Phase)
	}

	r.Now = func() time.Time { return frozen.Add(2 * time.Hour) } // the run's timeout is an hour
	step(t, r)

	run := readBackupRun(t, c)
	message := readyMessage(run.Status.Conditions)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || !strings.Contains(message, "Kueue") || !strings.Contains(message, kueue.WorkloadName(runUID)) {
		t.Fatalf("phase = %q, Ready = %q; want Failed naming Kueue and the Workload %s", run.Status.Phase, message, kueue.WorkloadName(runUID))
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonTimedOut {
		t.Errorf("item = %+v, want Failed with reason TimedOut, as every other timeout gives", item)
	}
	if _, ok := getUnstructured(t, c, kueue.WorkloadGVK, ns, kueue.WorkloadName(runUID)); ok {
		t.Error("the Workload outlived the failed run")
	}
}

// A Backup whose status.phase is none of the phases CloudNativePG 1.30 sets,
// as after a release that adds a harmless phase, leaves its item Running:
// the run waits, naming status.phase and the value in its Ready message and
// in the item's message, and goes on once CloudNativePG reports completed,
// which makes the item Succeeded and clears its message. A run that reaches
// its timeout in such a phase names it in the item's message.
func TestABackupInAPhaseTheControllerDoesNotKnowWaitsNamingIt(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), cluster())
	step(t, r)
	step(t, r)
	step(t, r)
	setBackupPhase(t, c, "uploading")
	step(t, r)

	run := readBackupRun(t, c)
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemRunning {
		t.Fatalf("item = %+v, want it still Running", item)
	}
	if !strings.Contains(item.Message, "status.phase") || !strings.Contains(item.Message, `"uploading"`) {
		t.Errorf("item message = %q, want it to name status.phase and \"uploading\"", item.Message)
	}
	if message := readyMessage(run.Status.Conditions); !strings.Contains(message, "status.phase") || !strings.Contains(message, `"uploading"`) {
		t.Errorf("Ready message = %q, want it to name status.phase and \"uploading\"", message)
	}

	setBackupPhase(t, c, "completed")
	step(t, r)
	if item := readBackupRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemSucceeded || item.Message != "" {
		t.Errorf("item = %+v, want it Succeeded with no message once the Backup completed", item)
	}
}

// A run past its timeout while its Backup is in a phase the controller
// doesn't know fails the item with a message that names the phase.
func TestABackupTimedOutInAnUnknownPhaseNamesIt(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), cluster())
	step(t, r)
	step(t, r)
	step(t, r)
	setBackupPhase(t, c, "uploading")
	step(t, r)

	r.Now = func() time.Time { return frozen.Add(48 * time.Hour) }
	step(t, r)

	run := readBackupRun(t, c)
	item := run.Status.Items[0]
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || item.Phase != backupv1alpha1.ItemFailed {
		t.Fatalf("phase = %q, item = %+v; want the run and the item Failed", run.Status.Phase, item)
	}
	if !strings.Contains(item.Message, "status.phase") || !strings.Contains(item.Message, `"uploading"`) {
		t.Errorf("item message = %q, want it to name status.phase and \"uploading\"", item.Message)
	}
}

// A run that times out while its Backup waits in a phase the controller
// doesn't know and a volume waits for another run names the phase once in
// the database item's message: the SourceBusy message names only the
// waits, and the timeout adds the item's own note to its message.
func TestATimedOutDatabaseItemNamesItsPhaseOnce(t *testing.T) {
	r, c := backupReconciler(t, backupRun(), cluster(),
		claim(), volume(), volumeRestore(), repository(), busySource(TriggerFor(otherRunUID)), otherRun())
	step(t, r) // plan
	step(t, r) // admit, no queue
	step(t, r) // the Backup is created; the volume waits for manual-notes
	setBackupPhase(t, c, "uploading")
	step(t, r)
	if reason := readyReason(readBackupRun(t, c).Status.Conditions); reason != backupv1alpha1.ReasonSourceBusy {
		t.Fatalf("reason = %q, want SourceBusy while the volume waits", reason)
	}

	r.Now = func() time.Time { return frozen.Add(48 * time.Hour) }
	step(t, r)

	for _, item := range readBackupRun(t, c).Status.Items {
		if item.Kind != backupv1alpha1.ItemKindCluster {
			continue
		}
		if n := strings.Count(item.Message, "CloudNativePG reports status.phase"); item.Phase != backupv1alpha1.ItemFailed || n != 1 {
			t.Errorf("database item = %+v names the phase %d times, want it Failed naming it once", item, n)
		}
	}
}

// setBackupPhase sets status.phase of the run's CloudNativePG Backup of the
// Cluster pgN, as CloudNativePG does.
func setBackupPhase(t *testing.T, c client.Client, phase string) {
	t.Helper()
	backup, ok := getUnstructured(t, c, cnpg.BackupGVK, ns, cnpg.BackupName(pgN, runUID))
	if !ok {
		t.Fatal("no Backup was created")
	}
	_ = unstructured.SetNestedField(backup.Object, phase, "status", "phase")
	if err := c.Status().Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
}

// CloudNativePG 1.30.0 checks a Backup in the phase "invalid backup
// definition" again on each pass and resets the phase when the definition
// becomes valid (api/v1/backup_funcs.go:336-342). Only "failed" and
// "completed" are terminal (internal/controller/backup_controller.go:155-158).
// Thus the item waits and does not fail.
func TestAnInvalidBackupDefinitionKeepsTheItemWaiting(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.Database = pgN }), cluster())
	step(t, r)
	step(t, r)
	step(t, r)
	backup, _ := getUnstructured(t, c, cnpg.BackupGVK, ns, cnpg.BackupName(pgN, runUID))
	_ = unstructured.SetNestedField(backup.Object, "invalid backup definition", "status", "phase")
	_ = unstructured.SetNestedField(backup.Object, "no plugin configured", "status", "error")
	if err := c.Status().Update(context.Background(), backup); err != nil {
		t.Fatal(err)
	}
	step(t, r)

	run := readBackupRun(t, c)
	if item := run.Status.Items[0]; item.Phase == backupv1alpha1.ItemFailed {
		t.Errorf("item = %+v, want it to wait while CloudNativePG checks the Backup again", item)
	}
	if run.Status.Phase != backupv1alpha1.RunPhaseRunning {
		t.Errorf("run phase = %q, want Running", run.Status.Phase)
	}
	if item := run.Status.Items[0]; !strings.Contains(item.Message, "no plugin configured") {
		t.Errorf("item message = %q, want it to carry status.error while the item waits", item.Message)
	}

	r.Now = func() time.Time { return frozen.Add(48 * time.Hour) }
	step(t, r)
	item := readBackupRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "no plugin configured") {
		t.Errorf("item = %+v, want it Failed at the timeout with status.error in its message", item)
	}
}
