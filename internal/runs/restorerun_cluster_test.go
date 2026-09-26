package runs

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
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
	if _, ok := getUnstructured(t, c, ClusterGVK, ns, pgN); ok {
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
	_ = unstructured.SetNestedField(u.Object, healthyPhase, "status", "phase")
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
	u, ok := getUnstructured(t, c, ClusterGVK, ns, pgN)
	if !ok {
		return ""
	}
	return u.GetUID()
}

// refuseFirstClusterDelete returns a client over c whose first delete of a
// Cluster fails the way an API server outage fails it.
func refuseFirstClusterDelete(c client.Client) client.Client {
	refused := false
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if u, ok := obj.(*unstructured.Unstructured); ok && u.GetKind() == ClusterGVK.Kind && !refused {
				refused = true
				return apierrors.NewServiceUnavailable("etcd leader changed")
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
}

// A Cluster created again after the run deleted the old one, carrying
// backup.wlz.li/bootstrap: initdb or declaring its own bootstrap, started
// empty or from its owner's bootstrap, and nothing was restored. The item
// fails naming what the Cluster carries, the run ends Failed, and the run
// leaves the new Cluster alone. Without the webhook a declared bootstrap
// reaches this point too, since nothing refuses it while the run waits.
func TestAClusterCreatedAgainOptedOutFailsTheRun(t *testing.T) {
	for name, tc := range map[string]struct {
		mutate func(*unstructured.Unstructured)
		names  string
	}{
		"opted out":     {optedOut, bootstrap.OptOutAnnotation + ": " + bootstrap.OptOutValue},
		"pg_basebackup": {declaring("pg_basebackup", map[string]any{"source": "legacy-db"}), "spec.bootstrap.pg_basebackup"},
	} {
		t.Run(name, func(t *testing.T) {
			r, c := databaseRestore(t)
			recreated := createCluster(t, c, tc.mutate)
			restoreStep(t, r)
			restoreStep(t, r)

			run := readRestoreRun(t, c)
			item := run.Status.Items[0]
			if item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, tc.names) || !strings.Contains(item.Message, "nothing was restored") {
				t.Errorf("item = %+v, want Failed naming %s and saying nothing was restored", item, tc.names)
			}
			if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
				t.Errorf("phase = %q (%s), want Failed", run.Status.Phase, readyMessage(run.Status.Conditions))
			}
			if uid := clusterUID(t, c); uid != recreated.GetUID() {
				t.Errorf("Cluster UID = %q, want the Cluster created again (%s) left alone", uid, recreated.GetUID())
			}
		})
	}
}

// The old Cluster, whose delete failed, gains backup.wlz.li/bootstrap: initdb
// before the run tries again. It is the Cluster the run never deleted, so the
// item is Skipped and the Cluster stays.
func TestAnOldClusterThatOptsOutBeforeTheDeleteIsSkipped(t *testing.T) {
	r, c := restoreReconciler(t, prober{saturday},
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }), cluster(), objectStore(), storeSecret())
	restoreStep(t, r) // plan
	r.Client = refuseFirstClusterDelete(c)
	if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
		t.Fatal("the pass whose delete failed succeeded, want the error returned")
	}
	if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemDeleted || item.ClusterUID != "old-cluster-uid" {
		t.Fatalf("item = %+v, want Deleted with the old Cluster's UID", item)
	}

	old, _ := getUnstructured(t, c, ClusterGVK, ns, pgN)
	optedOut(old)
	if err := c.Update(context.Background(), old); err != nil {
		t.Fatal(err)
	}
	restoreStep(t, r)

	item := readRestoreRun(t, c).Status.Items[0]
	if item.Phase != backupv1alpha1.ItemSkipped || !strings.Contains(item.Message, bootstrap.OptOutAnnotation) {
		t.Errorf("item = %+v, want Skipped naming %s", item, bootstrap.OptOutAnnotation)
	}
	if uid := clusterUID(t, c); uid != "old-cluster-uid" {
		t.Errorf("Cluster UID = %q, want the old Cluster left alone", uid)
	}
}

// A Cluster created again without the barman-cloud plugin archives nowhere.
// The webhook admits it untouched, so it is not the run's recovery. The run
// fails the item saying so and leaves the Cluster alone, instead of deleting
// it each time Flux creates it again until the timeout.
func TestAClusterCreatedAgainWithoutArchivingIsNotDeleted(t *testing.T) {
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

// An item marked Deleted without a clusterUID can't tell the old Cluster from
// a new one. A run that found no Cluster at its start leaves such an item,
// and a test stands in for the rest by clearing the field. A live Cluster
// that is not the run's recovery fails the item and stays, whichever it is.
func TestARunWithoutAClusterUIDDeletesNothing(t *testing.T) {
	forget := func(t *testing.T, c client.Client) {
		t.Helper()
		run := readRestoreRun(t, c)
		run.Status.Items[0].ClusterUID = ""
		if err := c.Status().Update(context.Background(), run); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("the old Cluster the delete missed", func(t *testing.T) {
		r, c := restoreReconciler(t, prober{saturday},
			restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }), cluster(), objectStore(), storeSecret())
		restoreStep(t, r) // plan
		r.Client = refuseFirstClusterDelete(c)
		if _, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Namespace: ns, Name: "back-to-monday"}}); err == nil {
			t.Fatal("the pass whose delete failed succeeded, want the error returned")
		}
		forget(t, c)
		restoreStep(t, r)
		restoreStep(t, r)

		run := readRestoreRun(t, c)
		if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "no clusterUID") {
			t.Errorf("item = %+v, want Failed saying the item holds no clusterUID", item)
		}
		if run.Status.Phase != backupv1alpha1.RunPhaseFailed {
			t.Errorf("phase = %q, want Failed", run.Status.Phase)
		}
		if uid := clusterUID(t, c); uid != "old-cluster-uid" {
			t.Errorf("Cluster UID = %q, want the Cluster left alone", uid)
		}
	})

	t.Run("a new Cluster", func(t *testing.T) {
		r, c := databaseRestore(t)
		forget(t, c)
		recreated := createCluster(t, c)
		restoreStep(t, r)
		restoreStep(t, r)

		if item := readRestoreRun(t, c).Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed {
			t.Errorf("item = %+v, want Failed", item)
		}
		if uid := clusterUID(t, c); uid != recreated.GetUID() {
			t.Errorf("Cluster UID = %q, want the Cluster left alone", uid)
		}
	})

	t.Run("its own recovery", func(t *testing.T) {
		r, c := databaseRestore(t)
		forget(t, c)
		recovered := createCluster(t, c, recoveredBy("back-to-monday"))
		restoreStep(t, r)
		markHealthy(t, c, recovered)
		restoreStep(t, r)

		if run := readRestoreRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseSucceeded {
			t.Errorf("phase = %q, items = %+v; want Succeeded", run.Status.Phase, run.Status.Items)
		}
	})
}

// A Cluster the webhook recovered for the run is replaced during the
// recovery, or is being deleted while it reports itself healthy. The
// replacement carries no annotation naming the run, since the webhook
// recovers only for a run whose item says Deleted. The run fails instead of
// reporting Succeeded once either Cluster is healthy.
func TestARecoveredClusterReplacedDuringRecoveryFailsTheRun(t *testing.T) {
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
		deleting, _ := getUnstructured(t, c, ClusterGVK, ns, pgN)
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

// A Deleted item that finds the Cluster recovered for another run fails,
// naming that run, and leaves the recovery alone. The refusal at the checks
// keeps two runs from both deleting one Cluster; this is what the run does
// should they get there all the same.
func TestADeletedItemLeavesAnotherRunsRecoveryAlone(t *testing.T) {
	second := restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.Name, r.UID, r.Spec.Database = "second", "9b7d4e21-0000-4000-8000-00000000000a", pgN
		r.Finalizers = []string{Finalizer}
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "Cluster", Name: pgN, Phase: backupv1alpha1.ItemDeleted}}
	})
	r, c := restoreReconciler(t, prober{saturday}, second, objectStore(), storeSecret())
	recovered := createCluster(t, c, recoveredBy("back-to-monday"))
	stepRestore(t, r, "second")
	stepRestore(t, r, "second")

	run := &backupv1alpha1.RestoreRun{}
	get(t, c, ns, "second", run)
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || !strings.Contains(item.Message, "RestoreRun back-to-monday recovered it") {
		t.Errorf("second run's item = %+v (%s), want Failed naming back-to-monday", item, readyMessage(run.Status.Conditions))
	}
	if uid := clusterUID(t, c); uid != recovered.GetUID() {
		t.Errorf("Cluster UID = %q, want back-to-monday's recovery (%s) left alone", uid, recovered.GetUID())
	}
}
