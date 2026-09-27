package runs

import (
	"context"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/cnpg"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// A database item records why it ended. The message texts stay as they
// were; each case gives the text the item's message holds.
//
//   - A Cluster the run must leave alone records ClusterLeftAlone and is
//     Skipped: at the plan of a namespace restore, at a Pending item, and as
//     the old Cluster a Deleted item finds still there.
//   - A Cluster that came back without the run's recovery, and a recovered
//     Cluster that is deleted or replaced, record ClusterNotRecovered.
//   - An item that abort fails records RunEnded.
func TestADatabaseRestoreRecordsItsReason(t *testing.T) {
	tests := []struct {
		name string
		// run reconciles a run until its Cluster item has ended, and
		// returns the client that holds it.
		run    func(t *testing.T) client.Client
		phase  backupv1alpha1.ItemPhase
		reason backupv1alpha1.ItemReason
		text   string
	}{
		{name: "left alone at the plan", run: func(t *testing.T) client.Client {
			r, c := restoreReconciler(t, prober{saturday},
				restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.All = true }),
				claim(), volumeRestore(), repository(), cluster(optedOut), objectStore(), storeSecret())
			restoreStep(t, r) // plan
			return c
		}, phase: backupv1alpha1.ItemSkipped, reason: backupv1alpha1.ItemReasonClusterLeftAlone, text: bootstrap.OptOutAnnotation},
		{name: "left alone at a Pending item", run: func(t *testing.T) client.Client {
			r, c := restoreReconciler(t, prober{saturday},
				restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Database = pgN }), cluster(), objectStore(), storeSecret())
			restoreStep(t, r) // plan
			old, _ := getUnstructured(t, c, cnpg.ClusterGVK, ns, pgN)
			declaring("pg_basebackup", map[string]any{"source": "legacy-db"})(old)
			if err := c.Update(context.Background(), old); err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r)
			return c
		}, phase: backupv1alpha1.ItemSkipped, reason: backupv1alpha1.ItemReasonClusterLeftAlone, text: "spec.bootstrap.pg_basebackup"},
		{name: "created again opted out", run: func(t *testing.T) client.Client {
			r, c := databaseRestore(t)
			createCluster(t, c, optedOut)
			restoreStep(t, r)
			return c
		}, phase: backupv1alpha1.ItemFailed, reason: backupv1alpha1.ItemReasonClusterNotRecovered, text: "nothing was restored"},
		{name: "the recovered Cluster deleted", run: func(t *testing.T) client.Client {
			r, c := databaseRestore(t)
			recovered := createCluster(t, c, recoveredBy("back-to-monday"))
			restoreStep(t, r) // Recovering
			if err := c.Delete(context.Background(), recovered); err != nil {
				t.Fatal(err)
			}
			restoreStep(t, r)
			return c
		}, phase: backupv1alpha1.ItemFailed, reason: backupv1alpha1.ItemReasonClusterNotRecovered, text: "the recovered Cluster was deleted"},
		{name: "the recovered Cluster replaced", run: func(t *testing.T) client.Client {
			r, c := databaseRestore(t)
			recovered := createCluster(t, c, recoveredBy("back-to-monday"))
			restoreStep(t, r) // Recovering
			if err := c.Delete(context.Background(), recovered); err != nil {
				t.Fatal(err)
			}
			createCluster(t, c)
			restoreStep(t, r)
			return c
		}, phase: backupv1alpha1.ItemFailed, reason: backupv1alpha1.ItemReasonClusterNotRecovered, text: "replaced by one this run did not recover"},
		{name: "aborted while Deleted", run: func(t *testing.T) client.Client {
			r, c := databaseRestore(t)
			if _, err := r.abort(context.Background(), readRestoreRun(t, c), backupv1alpha1.ReasonFailed, "Deployment notes was deleted"); err != nil {
				t.Fatal(err)
			}
			return c
		}, phase: backupv1alpha1.ItemFailed, reason: backupv1alpha1.ItemReasonRunEnded, text: "Deployment notes was deleted"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			run := readRestoreRun(t, tt.run(t))
			var item *backupv1alpha1.RestoreItem
			for i := range run.Status.Items {
				if run.Status.Items[i].Kind == backupv1alpha1.ItemKindCluster {
					item = &run.Status.Items[i]
				}
			}
			if item == nil {
				t.Fatalf("items = %+v, want a Cluster item", run.Status.Items)
			}
			if item.Phase != tt.phase || item.Reason != tt.reason || !strings.Contains(item.Message, tt.text) {
				t.Errorf("item = %+v; want %s with reason %s and a message that holds %q", item, tt.phase, tt.reason, tt.text)
			}
		})
	}
}
