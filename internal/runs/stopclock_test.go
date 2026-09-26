package runs

import (
	"context"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// stoppedItemRun returns the RestoreRun back-to-monday with one finished
// in-place item that still names the ReplicationDestination it deleted.
func stoppedItemRun(uid types.UID) *backupv1alpha1.RestoreRun {
	return restoreRun(func(r *backupv1alpha1.RestoreRun) {
		r.UID = uid
		r.Spec.Claim = claimN
		r.Status.Phase = backupv1alpha1.RunPhaseRunning
		r.Status.Items = []backupv1alpha1.RestoreItem{{Kind: "PersistentVolumeClaim", Name: claimN,
			Phase: backupv1alpha1.ItemSucceeded, Destination: destinationName(restoreUID, 0)}}
	})
}

// A look for a stopped mover that starts before pollInterval has passed since
// the delete never counts the mover gone, however long the look lasts: the
// time of the pass is taken before the look. The clock here moves past
// pollInterval while the pods are listed.
func TestAMoverLookThatStartsEarlyNeverCountsTheMoverGone(t *testing.T) {
	for name, look := range map[string]func(*RestoreRunReconciler, *backupv1alpha1.RestoreRun) (moverList, error){
		"removeDestinations": func(r *RestoreRunReconciler, run *backupv1alpha1.RestoreRun) (moverList, error) {
			return r.removeDestinations(context.Background(), run, anyItem)
		},
		"stoppedMovers": func(r *RestoreRunReconciler, run *backupv1alpha1.RestoreRun) (moverList, error) {
			return r.stoppedMovers(context.Background(), run, anyItem)
		},
	} {
		t.Run(name, func(t *testing.T) {
			run := stoppedItemRun(restoreUID)
			c := newClient(t)
			now := frozen
			slow := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
					if _, ok := list.(*corev1.PodList); ok {
						now = now.Add(pollInterval)
					}
					return cl.List(ctx, list, opts...)
				},
			})
			r := &RestoreRunReconciler{Client: c, Reader: slow, Now: func() time.Time { return now }}
			r.stops.Deleted(stopKey(run, run.Status.Items[0].Destination), frozen)
			now = frozen.Add(pollInterval / 2)

			left, err := look(r, run)
			if err != nil {
				t.Fatal(err)
			}
			if len(left) != 1 {
				t.Errorf("movers left = %+v, want the mover still counted while the look started before pollInterval passed", left)
			}
		})
	}
}

// Two runs whose destinations share a name, as the destinations of two runs
// whose UIDs share their first eight characters do, keep separate records of
// when each deleted its destination: one run's old delete never lets the
// other count its mover gone.
func TestTwoRunsWithOneDestinationNameKeepSeparateStops(t *testing.T) {
	first := stoppedItemRun(restoreUID)
	second := stoppedItemRun(types.UID(string(restoreUID)[:8] + "-ffff-4000-8000-000000000009"))
	c := newClient(t)
	r := &RestoreRunReconciler{Client: c, Reader: c, Now: func() time.Time { return frozen.Add(time.Minute) }}
	r.stops.Deleted(stopKey(first, first.Status.Items[0].Destination), frozen)

	left, err := r.stoppedMovers(context.Background(), second, anyItem)
	if err != nil {
		t.Fatal(err)
	}
	if len(left) != 1 {
		t.Errorf("movers left = %+v, want the second run's mover still counted: it has no delete of its own on record", left)
	}
}
