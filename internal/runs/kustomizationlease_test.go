package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// sharedKustomizationName is the Kustomization in flux-system that applies
// the notes and the wiki Deployment, so one Kustomization serves two
// namespaces.
const sharedKustomizationName = "apps"

// wikiNS and wikiClaimName name the second namespace and its claim, whose
// Deployment the shared Kustomization applies as well.
const (
	wikiNS        = "wiki"
	wikiClaimName = "wiki-data"
)

// wikiRunUID is the UID of the BackupRun that quiesces the wiki namespace.
const wikiRunUID = types.UID("7c1f3b90-0000-4000-8000-000000000003")

// wikiBackup returns the BackupRun scheduled-wiki with spec.all set, the run
// that quiesces the wiki namespace.
func wikiBackup(mutate ...func(*backupv1alpha1.BackupRun)) *backupv1alpha1.BackupRun {
	run := &backupv1alpha1.BackupRun{
		ObjectMeta: metav1.ObjectMeta{Name: "scheduled-wiki", Namespace: wikiNS, UID: wikiRunUID, Generation: 1},
		Spec:       backupv1alpha1.BackupRunSpec{All: true, Timeout: &metav1.Duration{Duration: time.Hour}},
	}
	for _, m := range mutate {
		m(run)
	}
	return run
}

// wikiClaim returns the wiki namespace's claim and the objects it needs to be
// backed up, built like claim, volume, volumeRestore and repository.
func wikiClaim() []client.Object {
	pvc := claim()
	pvc.Namespace, pvc.Name, pvc.UID = wikiNS, wikiClaimName, "wiki-claim-uid"
	pvc.Spec.VolumeName, pvc.Spec.DataSourceRef.Name = "pvc-"+wikiClaimName, wikiClaimName
	pv := volume()
	pv.Name = "pvc-" + wikiClaimName
	vr := volumeRestore()
	vr.Namespace, vr.Name, vr.Spec.Repository = wikiNS, wikiClaimName, "wiki-restic-data"
	secret := repository()
	secret.Namespace, secret.Name = wikiNS, "wiki-restic-data"
	return []client.Object{pvc, pv, vr, secret}
}

// deploymentApplying returns a Deployment named name in namespace, marked for
// quiesce with two replicas and labeled as applied by the shared
// Kustomization.
func deploymentApplying(namespace, name string) *appsv1.Deployment {
	d := deployment()
	d.Namespace, d.Name = namespace, name
	d.Labels = map[string]string{fluxNameLabel: sharedKustomizationName, fluxNamespaceLabel: "flux-system"}
	return d
}

// sharedKustomization returns the Kustomization in flux-system that applies
// both Deployments, its inventory listing each.
func sharedKustomization(suspended bool) *unstructured.Unstructured {
	k := kustomization(suspended)
	k.SetName(sharedKustomizationName)
	_ = unstructured.SetNestedSlice(k.Object, []any{
		map[string]any{"id": ns + "_" + appN + "_apps_Deployment", "v": "v1"},
		map[string]any{"id": wikiNS + "_wiki_apps_Deployment", "v": "v1"},
	}, "status", "inventory", "entries")
	return k
}

// wikiClone stands in for VolSync cutting the wiki claim's clone, five
// seconds after the frozen clock's time, which is after the run stopped the
// wiki app.
func wikiClone(t *testing.T, c client.Client) {
	t.Helper()
	defer atServerTime(t, c, frozen.Add(5*time.Second))()
	clone := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "volsync-" + wikiClaimName + "-src", Namespace: wikiNS,
			Finalizers: []string{"kubernetes.io/pvc-protection"}},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
	if err := c.Create(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
	if err := c.Status().Update(context.Background(), clone); err != nil {
		t.Fatal(err)
	}
}

// sharedKustomizationLeaseIn returns the Lease that guards the shared
// Kustomization, or nil.
func sharedKustomizationLeaseIn(t *testing.T, c client.Client) *coordinationv1.Lease {
	t.Helper()
	key := &unstructured.Unstructured{}
	key.SetGroupVersionKind(KustomizationGVK)
	get(t, c, "flux-system", sharedKustomizationName, key)
	lease := &coordinationv1.Lease{}
	err := c.Get(context.Background(), types.NamespacedName{
		Namespace: "flux-system", Name: kustomizationLeaseName(key.GetUID()),
	}, lease)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatalf("get the Kustomization Lease: %v", err)
	}
	return lease
}

// A Kustomization that applies workloads of two namespaces is held by one run
// at a time: the second run waits for the first rather than stopping its own
// workloads under a Kustomization the first will resume, which would let Flux
// scale them back up.
func TestAKustomizationSharedAcrossNamespacesIsHeldByOneRun(t *testing.T) {
	objects := append([]client.Object{
		wikiBackup(), quiescedRestoreOf(),
		deploymentApplying(ns, appN), deploymentApplying(wikiNS, "wiki"), sharedKustomization(false),
		claim(), volume(), volumeRestore(), repository(),
	}, wikiClaim()...)
	c := newClient(t, objects...)
	br := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: frozenNow}
	rr := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: frozenNow}

	// The wiki backup quiesces: it suspends the shared Kustomization and
	// stops the wiki app.
	stepIn(t, br, wikiNS, "scheduled-wiki") // plan
	stepIn(t, br, wikiNS, "scheduled-wiki") // admit, no queue
	stepIn(t, br, wikiNS, "scheduled-wiki") // quiesce
	stepIn(t, br, wikiNS, "scheduled-wiki") // start
	if !sharedSuspended(t, c) {
		t.Fatal("the wiki run did not suspend the shared Kustomization")
	}
	if held := sharedKustomizationLeaseIn(t, c); held == nil || holderUID(held) != string(wikiRunUID) {
		t.Errorf("the Kustomization Lease = %+v; want the wiki run holding it", held)
	}

	// The wiki run still owes its app, so the notes restore waits for its
	// Lease rather than suspending the Kustomization behind its back.
	restoreStep(t, rr) // quiesce: waits for the Kustomization Lease
	restore := readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 0 {
		t.Fatalf("the restore recorded %+v and suspended %v; want no plan while the wiki run holds the Kustomization",
			restore.Status.Quiesced, restore.Status.SuspendedKustomizations)
	}
	message := readyMessage(restore.Status.Conditions)
	if readyReason(restore.Status.Conditions) != backupv1alpha1.ReasonSourceBusy ||
		!strings.Contains(message, "BackupRun wiki/scheduled-wiki") ||
		!strings.Contains(message, "Kustomization flux-system/"+sharedKustomizationName) ||
		!strings.Contains(message, "Deployment "+appN) {
		t.Fatalf("reason = %q, message = %q; want SourceBusy naming the wiki run, the Kustomization and the Deployment",
			readyReason(restore.Status.Conditions), message)
	}
	if got := replicasOf(t, c); got != 2 {
		t.Fatalf("replicas = %d while the restore waits, want the notes app untouched", got)
	}

	// The wiki run gives its app back, which resumes the shared Kustomization
	// and makes its Lease stale; the restore then takes the Lease over and
	// suspends the Kustomization itself.
	wikiClone(t, c)
	stepIn(t, br, wikiNS, "scheduled-wiki") // restart
	if sharedSuspended(t, c) {
		t.Fatal("the wiki run did not resume the shared Kustomization after its restart")
	}
	restoreStep(t, rr)
	restore = readRestoreRun(t, c)
	if len(restore.Status.Quiesced) != 1 || restore.Status.Quiesced[0].Replicas != 2 {
		t.Fatalf("the restore recorded %+v, want Deployment %s at 2", restore.Status.Quiesced, appN)
	}
	if len(restore.Status.SuspendedKustomizations) != 1 || restore.Status.SuspendedKustomizations[0] != "flux-system/"+sharedKustomizationName {
		t.Fatalf("the restore suspended %v, want the shared Kustomization", restore.Status.SuspendedKustomizations)
	}
	if !sharedSuspended(t, c) {
		t.Error("the restore did not suspend the shared Kustomization")
	}
	if got := replicasOf(t, c); got != 0 {
		t.Errorf("replicas = %d after the restore stopped the notes app, want 0", got)
	}
	if held := sharedKustomizationLeaseIn(t, c); held == nil || holderUID(held) != string(restoreUID) {
		t.Errorf("the Kustomization Lease = %+v; want the restore holding it once the wiki run's restart made it stale", held)
	}
}

// sharedSuspended reports whether the shared Kustomization is suspended.
func sharedSuspended(t *testing.T, c client.Client) bool {
	t.Helper()
	k, ok := getUnstructured(t, c, KustomizationGVK, "flux-system", sharedKustomizationName)
	if !ok {
		t.Fatal("the shared Kustomization is gone")
	}
	s, _, _ := unstructured.NestedBool(k.Object, "spec", "suspend")
	return s
}
