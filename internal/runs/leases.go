package runs

import (
	"context"
	"fmt"
	"slices"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/lease"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// RunAlive returns the lease.Liveness of the controller's runs: a BackupRun
// or RestoreRun holder needs its Leases while it exists with the holder's
// UID and has not finished.
//
// Parameters:
//   - reader reads the runs from the API server.
//
// A run that is gone, that has another UID (a new run with an old run's
// name), or that has finished no longer needs its Leases. A holder of
// another kind needs none either, since this controller writes only these
// two kinds. The function returns an error when the read fails for another
// reason, so no Lease is taken over on a guess.
func RunAlive(reader client.Reader) lease.Liveness {
	return func(ctx context.Context, namespace string, holder lease.Holder) (bool, error) {
		key := types.NamespacedName{Namespace: namespace, Name: holder.Name}
		var uid types.UID
		var phase backupv1alpha1.RunPhase
		switch holder.Kind {
		case "BackupRun":
			run := &backupv1alpha1.BackupRun{}
			if err := reader.Get(ctx, key, run); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			uid, phase = run.UID, run.Status.Phase
		case "RestoreRun":
			run := &backupv1alpha1.RestoreRun{}
			if err := reader.Get(ctx, key, run); err != nil {
				return false, client.IgnoreNotFound(err)
			}
			uid, phase = run.UID, run.Status.Phase
		default:
			return false, nil
		}
		return uid == holder.UID && !phase.Finished(), nil
	}
}

// holding takes every Lease a run needs before it acts.
//
// Parameters:
//   - leases takes the Leases.
//   - namespace is the run's namespace.
//   - holder is the run.
//   - names are the Leases the run needs, in the order it takes them.
//   - recorded is the run's status.leases. holding adds each name before it
//     takes the Lease, so a run that stops after the take still releases it.
//   - record writes the run's status when holding added a name.
//
// It returns an empty string when the run holds every Lease. It returns a
// message naming the first Lease another run holds, and that run, when the
// run has to wait. It returns an error when a status write or a Lease call
// fails.
func holding(ctx context.Context, leases *lease.Leases, namespace string, holder lease.Holder, names []string, recorded *[]string, record func() error) (string, error) {
	added := false
	for _, name := range names {
		if !slices.Contains(*recorded, name) {
			*recorded = append(*recorded, name)
			added = true
		}
	}
	if added {
		if err := record(); err != nil {
			return "", err
		}
	}
	for _, name := range names {
		held, other, err := leases.Take(ctx, namespace, name, holder)
		if err != nil {
			return "", err
		}
		if !held {
			if other == "" {
				return fmt.Sprintf("waiting to take Lease %s", name), nil
			}
			return fmt.Sprintf("Lease %s is held by %s", name, other), nil
		}
	}
	return "", nil
}

// releaseAll deletes every Lease a run recorded that it still holds.
//
// Parameters:
//   - leases releases the Leases.
//   - namespace is the run's namespace.
//   - holder is the run.
//   - recorded is the run's status.leases.
//
// A Lease another run took over, or one that is gone, is left alone (see
// lease.Leases.Release). It returns the first error.
func releaseAll(ctx context.Context, leases *lease.Leases, namespace string, holder lease.Holder, recorded []string) error {
	for _, name := range recorded {
		if err := leases.Release(ctx, namespace, name, holder); err != nil && !apierrors.IsNotFound(err) {
			return err
		}
	}
	return nil
}
