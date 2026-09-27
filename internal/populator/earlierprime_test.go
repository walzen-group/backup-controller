package populator

import (
	"context"
	"errors"
	"testing"

	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
)

// earlierPrimeUID is the UID of a prime claim of notes that is gone: the
// library created the prime claim again under the same name since.
const earlierPrimeUID types.UID = "prime-uid-OLD"

// createEarlierJob creates the restore Job of notes as Populate built it for
// the earlier prime claim, owned by that prime, in the state the Job
// controller leaves it in right after the create.
func (f *fakeOperations) createEarlierJob(t *testing.T) *batchv1.Job {
	t.Helper()
	job, err := restorejob.Build(restorejob.Spec{
		Name: JobName("claim-123"), Namespace: controllerNS,
		Origin:     restorejob.Origin{Kind: restorejob.OriginClaim, UID: "claim-123"},
		Owner:      metav1.OwnerReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: PrimeClaimName("claim-123"), UID: earlierPrimeUID},
		SnapshotID: monday.ID, Claim: PrimeClaimName("claim-123"), Repository: "claim-123", Image: testImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.cluster.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return f.setJob(t, "claim-123", func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })
}

// TestAFinishedJobOfAnEarlierPrimeIsNeverTrusted checks that a Job with
// Complete=True that filled an earlier prime claim of the claim never
// completes the claim: its volume went with that prime, and the library
// would bind the new, empty prime. Populate stops the Job and creates a new
// one for the prime it has now only once the old one is gone.
func TestAFinishedJobOfAnEarlierPrimeIsNeverTrusted(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	old := ops.createEarlierJob(t)
	ops.setJob(t, "claim-123", complete)
	callbacks := newCallbacks(ops, monday)

	if done, err := callbacks.Complete(ctx, params()); err != nil || done {
		t.Fatalf("Complete() = %t, %v; want false for the Job of an earlier prime", done, err)
	}
	if done, err := librarySync(ctx, callbacks, params()); err == nil || done {
		t.Fatalf("sync = %t, %v; want an error and not done", done, err)
	}
	if got := ops.job(t, "claim-123"); got != nil && got.DeletionTimestamp == nil {
		t.Fatalf("the Job of the earlier prime is %+v, want it stopped", got)
	}
	if _, err := librarySync(ctx, callbacks, params()); err != nil {
		t.Fatalf("sync once the old Job is gone: %v", err)
	}
	replaced := ops.job(t, "claim-123")
	if replaced == nil || replaced.UID == old.UID || len(replaced.OwnerReferences) != 1 || replaced.OwnerReferences[0].UID != "prime-uid-123" {
		t.Fatalf("Job = %+v, want a new one owned by prime-uid-123", replaced)
	}
}

// TestASuspendedJobOfAnEarlierPrimeIsNeverResumed checks that a Job built
// for an earlier prime claim, still suspended, is never recorded on the prime
// claim the library has now and never resumed: it would write that prime
// until the garbage collector killed it mid-restore.
func TestASuspendedJobOfAnEarlierPrimeIsNeverResumed(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	old := ops.createEarlierJob(t)
	callbacks := newCallbacks(ops, monday)

	for sync := range 2 {
		_ = callbacks.Populate(ctx, params())
		if got := ops.prime(t, "claim-123").Annotations[AnnotationJobUID]; got == string(old.UID) {
			t.Fatalf("sync %d recorded the Job of an earlier prime on the prime claim", sync)
		}
		if job := ops.job(t, "claim-123"); job != nil && job.UID == old.UID && !ptr.Deref(job.Spec.Suspend, false) {
			t.Fatalf("sync %d resumed the Job of an earlier prime", sync)
		}
	}
}

// TestAStalePrimeNeverRecordsOrResumesAJob checks that Populate records and
// resumes no Job while the library's cache still passes an earlier prime
// claim and the API server already holds a new one under the same name. The
// Job was built for the earlier prime, so ownedByPrime accepts it against
// the cached prime; the fresh read shows the prime of now, and Populate
// returns an error until the cache catches up.
func TestAStalePrimeNeverRecordsOrResumesAJob(t *testing.T) {
	ctx := context.Background()
	ops := populatedOperations(t)
	old := ops.createEarlierJob(t)
	stale := params()
	stale.PvcPrime.UID = earlierPrimeUID
	callbacks := newCallbacks(ops, monday)

	for sync := range 2 {
		if err := callbacks.Populate(ctx, stale); !errors.Is(err, errStalePrime) {
			t.Fatalf("sync %d: Populate() = %v, want errStalePrime", sync, err)
		}
		if got := ops.prime(t, "claim-123").Annotations[AnnotationJobUID]; got == string(old.UID) {
			t.Fatalf("sync %d recorded the Job of the cached prime on the prime of now", sync)
		}
		if job := ops.job(t, "claim-123"); !ptr.Deref(job.Spec.Suspend, false) {
			t.Fatalf("sync %d resumed the Job of the cached prime", sync)
		}
	}
}
