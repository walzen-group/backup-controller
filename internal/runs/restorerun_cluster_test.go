package runs

import (
	"context"
	"strings"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// These tests cover how a database item tells the Cluster it deleted, its own
// recovery and any other Cluster of the same name apart. Each returning
// Cluster is created the way the bootstrap webhook admits it: opted out with
// no annotation, archiving nowhere with no annotation, or recovered with
// backup.wlz.li/restore-run naming the run. The strict client gives every
// created Cluster a fresh UID, as the API server does.

// databaseRestore returns the reconciler and client for a restore of the
// database pgN alone, planned, with the Cluster deleted and the item Deleted.
func databaseRestore(t *testing.T) (*RestoreRunReconciler, client.Client) {
	t.Helper()
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }), cluster(), objectStore(), storeSecret())
	restoreStep(t, r) // plan
	restoreStep(t, r) // delete
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemDeleted || item.ClusterUID != "old-cluster-uid" {
		t.Fatalf("item = %+v, want Deleted with the old Cluster's UID", item)
	}
	if _, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN); ok {
		t.Fatal("the Cluster was not deleted")
	}
	return r, c
}

// createCluster creates a Cluster of the name pgN, as Flux or tofu would after
// the webhook admitted it, and returns it with the UID the client gave it.
func createCluster(t *testing.T, c client.Client, mutate ...func(*unstructured.Unstructured)) *unstructured.Unstructured {
	t.Helper()
	created := cluster(mutate...)
	created.SetUID("")
	if err := c.Create(context.Background(), created); err != nil {
		t.Fatal(err)
	}
	return created
}

// recoveredBy is a mutate function for cluster that adds the annotation the
// webhook writes on a Cluster it recovers for the named run.
func recoveredBy(run string) func(*unstructured.Unstructured) {
	return func(u *unstructured.Unstructured) {
		annotations := u.GetAnnotations()
		annotations[backupv1alpha1.AnnotationRestoreRun] = run
		u.SetAnnotations(annotations)
	}
}

// archivingNowhere is a mutate function for cluster that removes the
// barman-cloud plugin, so the Cluster archives nowhere.
func archivingNowhere(u *unstructured.Unstructured) {
	unstructured.RemoveNestedField(u.Object, "spec", "plugins")
}

// markHealthy writes the phase CloudNativePG reports for a Cluster that is up.
func markHealthy(t *testing.T, c client.Client, u *unstructured.Unstructured) {
	t.Helper()
	_ = unstructured.SetNestedField(u.Object, cnpg.HealthyPhase, "status", "phase")
	if err := c.Status().Update(context.Background(), u); err != nil {
		t.Fatal(err)
	}
}

// stepRestore reconciles the named RestoreRun once. An error fails the test.
func stepRestore(t *testing.T, r *RestoreRunReconciler, name string) {
	t.Helper()
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: name}}); err != nil {
		t.Fatalf("reconcile %s: %v", name, err)
	}
}

// clusterUID returns the UID of the Cluster pgN, or an empty string when it
// is gone.
func clusterUID(t *testing.T, c client.Client) types.UID {
	t.Helper()
	u, ok := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN)
	if !ok {
		return ""
	}
	return u.GetUID()
}

// A Cluster created again without the barman-cloud plugin archives nowhere.
// The webhook admits it untouched, so it is not the run's recovery. The run
// fails the item saying so and leaves the Cluster alone, instead of deleting
// it each time Flux creates it again until the timeout.
func TestAClusterCreatedAgainWithoutArchivingIsNotDeleted(t *testing.T) {
	t.Parallel()
	r, c := databaseRestore(t)
	recreated := createCluster(t, c, archivingNowhere)
	restoreStep(t, r)
	restoreStep(t, r)

	run := readRestoreRun(t, c)
	item := run.Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "archives nowhere") ||
		!strings.Contains(item.Message, string(recreated.GetUID())) {
		t.Errorf("item = %+v, want Failed naming the Cluster's UID and that it archives nowhere", item)
	}
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Errorf("phase = %q, want Failed", run.Status.Phase)
	}
	if uid := clusterUID(t, c); uid != recreated.GetUID() {
		t.Errorf("Cluster UID = %q, want the Cluster that archives nowhere (%s) left alone", uid, recreated.GetUID())
	}
}

// A Cluster the webhook recovered for the run is replaced during the
// recovery, or is being deleted while it reports itself healthy. The
// replacement carries no annotation naming the run, since the webhook
// recovers only for a run whose item says Deleted. The run fails instead of
// reporting Succeeded once either Cluster is healthy.
func TestARecoveredClusterReplacedDuringRecoveryFailsTheRun(t *testing.T) {
	t.Parallel()
	recovering := func(t *testing.T) (*RestoreRunReconciler, client.Client, *unstructured.Unstructured) {
		t.Helper()
		r, c := databaseRestore(t)
		recovered := createCluster(t, c, recoveredBy("back-to-monday"))
		restoreStep(t, r)
		if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemRecovering {
			t.Fatalf("item = %+v, want Recovering", item)
		}
		return r, c, recovered
	}

	t.Run("replaced", func(t *testing.T) {
		r, c, recovered := recovering(t)
		if err := c.Delete(context.Background(), recovered); err != nil {
			t.Fatal(err)
		}
		replacement := createCluster(t, c)
		markHealthy(t, c, replacement)
		restoreStep(t, r)
		restoreStep(t, r)

		run := readRestoreRun(t, c)
		if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "replaced") {
			t.Errorf("item = %+v, want Failed saying the recovered Cluster was replaced", item)
		}
		if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
			t.Errorf("phase = %q, want Failed", run.Status.Phase)
		}
		if uid := clusterUID(t, c); uid != replacement.GetUID() {
			t.Errorf("Cluster UID = %q, want the replacement left alone", uid)
		}
	})

	t.Run("being deleted", func(t *testing.T) {
		r, c, recovered := recovering(t)
		recovered.SetFinalizers([]string{"example.com/hold"})
		if err := c.Update(context.Background(), recovered); err != nil {
			t.Fatal(err)
		}
		if err := c.Delete(context.Background(), recovered); err != nil {
			t.Fatal(err)
		}
		deleting, _ := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN)
		if deleting.GetDeletionTimestamp() == nil {
			t.Fatal("the Cluster is not being deleted")
		}
		markHealthy(t, c, deleting)
		restoreStep(t, r)
		restoreStep(t, r)

		run := readRestoreRun(t, c)
		if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "deleted") {
			t.Errorf("item = %+v, want Failed saying the recovered Cluster was deleted", item)
		}
	})
}

// A second run for a Cluster that another unfinished run is restoring is
// refused at its checks with reason Invalid, naming the first run, and
// deletes nothing. The first run deletes the Cluster, the webhook recovers
// it for that run, and the first run succeeds with its recovery untouched.
func TestTwoRunsWaitingOnOneClusterDoNotDeleteEachOthersRecovery(t *testing.T) {
	t.Parallel()
	second := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Name, r.UID, r.Spec.Database = "second", "9b7d4e21-0000-4000-8000-00000000000a", pgN
	})
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }), second,
		cluster(), objectStore(), storeSecret())
	stepRestore(t, r, "back-to-monday") // plan
	stepRestore(t, r, "second")         // plan: refused

	run := &backupv1alpha1.RestoreRun{}
	get(t, c, ns, "second", run)
	want := "RestoreRun back-to-monday is restoring Cluster " + pgN
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonInvalid ||
		!strings.HasPrefix(readyMessage(run.Status.Conditions), want) {
		t.Fatalf("second run: phase = %q, reason = %q, message = %q; want Failed, Invalid, starting %q",
			run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions), want)
	}
	if uid := clusterUID(t, c); uid != "old-cluster-uid" {
		t.Fatalf("Cluster UID = %q after the refusal, want the old Cluster untouched", uid)
	}

	stepRestore(t, r, "back-to-monday") // delete
	recovered := createCluster(t, c, recoveredBy("back-to-monday"))
	stepRestore(t, r, "second")
	stepRestore(t, r, "back-to-monday")
	if uid := clusterUID(t, c); uid != recovered.GetUID() {
		t.Fatalf("Cluster UID = %q, want back-to-monday's recovery (%s) left alone", uid, recovered.GetUID())
	}
	markHealthy(t, c, recovered)
	stepRestore(t, r, "back-to-monday")
	if first := readRestoreRun(t, c); first.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
		t.Errorf("first run phase = %q, items = %+v; want Succeeded", first.Status.Phase, first.Status.Items)
	}
}

// Two runs that planned in the same instant both passed the check at their
// plan, since neither had written its items yet. So a run checks again right
// before it deletes the Cluster: back-to-monday finds second holding the
// Cluster Pending, fails its item naming second, and deletes nothing. Before,
// it deleted the Cluster, and second's later delete would have reached
// back-to-monday's recovery.
func TestARunChecksForAnotherRunAgainBeforeItDeletesTheCluster(t *testing.T) {
	t.Parallel()
	planned := func(r *backupv1alpha1.RestoreRun) {
		r.Spec.Database = pgN
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemPending, BaseBackup: saturday.ID}}
	}
	second := restoreRun(planned, func(r *backupv1alpha1.RestoreRun) {
		r.Name, r.UID = "second", "9b7d4e21-0000-4000-8000-00000000000a"
	})
	r, c := restoreReconciler(t, prober{saturday}, restoreRun(planned), second, cluster(), objectStore(), storeSecret())

	stepRestore(t, r, "back-to-monday")

	if uid := clusterUID(t, c); uid != "old-cluster-uid" {
		t.Fatalf("Cluster UID = %q, want the Cluster left alone while second holds it", uid)
	}
	item := readRestoreRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "RestoreRun second is restoring Cluster "+pgN) {
		t.Errorf("item = %+v, want Failed naming RestoreRun second", item)
	}
}

// deletedDatabaseRun is a mutate function for restoreRun that makes it a
// database restore of pgN, started at frozen, whose item is Deleted: the run
// deleted the old Cluster and waits for its owner to create it again.
func deletedDatabaseRun(r *backupv1alpha1.RestoreRun) {
	started := metav1.NewTime(frozen)
	r.Spec.Database = pgN
	r.Finalizers = []string{Finalizer}
	r.Status.Phase = backupv1alpha1.RunPhaseWaiting
	r.Status.StartedAt = &started
	r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemDeleted,
		ClusterUID: "old-cluster-uid", BaseBackup: saturday.ID}}
}

// endedBeforeRecreate is what the message of a Deleted Cluster item says
// once its run has ended: the webhook recovers the next creation of the
// Cluster without the run.
const endedBeforeRecreate = "recovers it to the end of its archive, or to the time in its own backup.wlz.li/restore-as-of annotation"

// A run that times out while it waits for its deleted Cluster to be created
// again says, on the item, what the webhook does with that Cluster now: once
// the run has ended, no run waits for the Cluster, so its next creation
// recovers to the end of its archive or to its own annotation.
func TestATimedOutRunSaysHowItsDeletedClusterComesBack(t *testing.T) {
	t.Parallel()
	r, c := restoreReconciler(t, prober{saturday}, restoreRun(deletedDatabaseRun), objectStore(), storeSecret())
	r.Now = func() time.Time { return frozen.Add(5 * time.Hour) }

	restoreStep(t, r)

	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
		t.Fatalf("phase = %q, want Failed", run.Status.Phase)
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, endedBeforeRecreate) {
		t.Errorf("item = %+v, want Failed saying %q", item, endedBeforeRecreate)
	}
	if !strings.Contains(readyMessage(run.Status.Conditions), endedBeforeRecreate) {
		t.Errorf("ready message = %q, want it to say %q", readyMessage(run.Status.Conditions), endedBeforeRecreate)
	}
	if item := run.Status.Items[0]; !item.ClusterLeftDeleted || item.Reason != backupv1alpha1.ItemReasonTimedOut {
		t.Errorf("clusterLeftDeleted = %t, reason = %q; want true and TimedOut", item.ClusterLeftDeleted, item.Reason)
	}
}
