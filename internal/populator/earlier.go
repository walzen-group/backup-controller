package populator

import (
	"context"
	"errors"
	"fmt"

	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
)

// errJobOfEarlierPrime is a claim whose restore Job was built for an earlier
// prime claim, one the library has since created again under the same name.
// That Job's volume went with its prime, so nothing it did counts for the
// prime the library has now.
var errJobOfEarlierPrime = errors.New("its restore Job filled an earlier prime claim; it is stopped, and a new one is created once it is gone")

// ownedByPrime reports whether a restore Job was built for a prime claim,
// by the owner reference createJob gives every Job.
//
// Parameters:
//   - job is the claim's Job, as just read.
//   - prime is the prime claim the library passes now.
//
// The library creates a prime claim again under the same name, with a new
// UID, when the old one is gone before the claim is filled. The owner
// reference carries the UID of the prime the Job was built for, so a Job of
// the earlier prime has none that names the prime of now.
func ownedByPrime(job *batchv1.Job, prime *corev1.PersistentVolumeClaim) bool {
	for _, owner := range job.OwnerReferences {
		if owner.UID == prime.UID {
			return true
		}
	}
	return false
}

// stopEarlierJob stops a restore Job that was built for an earlier prime
// claim, so that a Job for the prime of now can take its name.
//
// Parameters:
//   - r is the claim's restore.
//   - job is the Job of the earlier prime, as Populate just read it, not
//     being deleted.
//
// It always returns an error, so the library requeues the claim and does
// not call Complete in this sync: a *stoppingError while a pod of the Job
// may still write, the error of the stop, or errJobOfEarlierPrime once the
// stop has deleted the Job.
//
// restorejob.Stop suspends the Job when it is not finished, waits for its
// pods to end and deletes it by its UID, so a Job created under the name
// since the read is left alone. The Job is never recorded on the prime of
// now and never resumed, and a later sync creates the new Job once this
// one is gone.
func (c *Callbacks) stopEarlierJob(ctx context.Context, r restore, job *batchv1.Job) error {
	state, err := restorejob.Stop(ctx, c.operations, restorejob.RefOf(job))
	switch {
	case err != nil:
		return claimError(r.claim, fmt.Errorf("%w; stopping it: %w", errJobOfEarlierPrime, err))
	case !state.Stopped:
		return claimError(r.claim, &stoppingError{state: state})
	default:
		return claimError(r.claim, errJobOfEarlierPrime)
	}
}
