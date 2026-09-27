package populator

import (
	"context"
	"slices"
	"testing"
	"time"

	populatormachinery "github.com/kubernetes-csi/lib-volume-populator/v3/populator-machinery"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/restorejob"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// The controller namespace, the app namespace and the image of the
// callbacks' tests.
const (
	controllerNS = "backup-system"
	appNS        = "apps"
	testImage    = "quay.io/backube/volsync:0.16.0"
)

// serverTime is the API server's clock in the callbacks' tests.
var serverTime = time.Date(2026, 9, 26, 7, 0, 0, 0, time.UTC)

// The two claims of the callbacks' tests: notes (claim-123) and photos
// (claim-456), in apps, each with its prime claim in backup-system bound to
// its own volume.
var testClaims = []struct {
	name     string
	uid      types.UID
	primeUID types.UID
	volume   string
}{
	{"notes", "claim-123", "prime-uid-123", "pv-123"},
	{"photos", "claim-456", "prime-uid-456", "pv-456"},
}

// testScheme returns a scheme with the core, batch and backup.wlz.li types.
func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{corev1.AddToScheme, batchv1.AddToScheme, backupv1alpha1.AddToScheme} {
		if err := add(s); err != nil {
			t.Fatal(err)
		}
	}
	return s
}

// newCluster returns a strict fake API server with the garbage collector on,
// holding the two namespaces, the claims notes and photos, their prime claims
// and the volumes the prime claims are bound to, and the objects given.
func newCluster(t *testing.T, objects ...client.Object) *strictclient.Client {
	t.Helper()
	seed := []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: controllerNS}},
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: appNS}},
	}
	for _, c := range testClaims {
		seed = append(seed,
			&corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{Name: c.name, Namespace: appNS, UID: c.uid}},
			&corev1.PersistentVolumeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: PrimeClaimName(c.uid), Namespace: controllerNS, UID: c.primeUID},
				Spec:       corev1.PersistentVolumeClaimSpec{VolumeName: c.volume},
			},
			&corev1.PersistentVolume{
				ObjectMeta: metav1.ObjectMeta{Name: c.volume},
				Spec: corev1.PersistentVolumeSpec{ClaimRef: &corev1.ObjectReference{
					Kind: "PersistentVolumeClaim", APIVersion: "v1", Namespace: controllerNS, Name: PrimeClaimName(c.uid), UID: c.primeUID,
				}},
			},
		)
	}
	return strictclient.Build(fake.NewClientBuilder().WithObjects(append(seed, objects...)...), testScheme(t), strictclient.Options{
		Clock:          func() time.Time { return serverTime },
		CRDs:           []string{"../testinfra/crds/backup-controller/v0.8.1/backup.wlz.li_volumerestores.yaml"},
		GarbageCollect: true,
	})
}

// fakeOperations is the callbacks' Operations for tests. The restore Jobs,
// their pods, the namespaces and the claims live in a strict fake API
// server; the Secrets live in a map, and every Secret create and delete,
// status write and VolumeRestore update is recorded so the tests can check
// them. When volumeRestoreGone is true, writes to the VolumeRestore fail with
// NotFound, as they do once it has been deleted.
type fakeOperations struct {
	Jobs
	cluster           *strictclient.Client
	secrets           map[string]*corev1.Secret
	createdSec        []*corev1.Secret
	deletedSec        []string
	statuses          []*backupv1alpha1.VolumeRestore
	updates           []*backupv1alpha1.VolumeRestore
	volumeRestoreGone bool
	// jobCreates counts the restore Job creates sent, those the API server
	// refused included.
	jobCreates int
	// refuseCreate and refuseResume, when set, are the API server's answer
	// to every restore Job create and every resume, as an admission policy
	// or a missing grant gives it.
	refuseCreate error
	refuseResume error
	// beforeResume, when set, runs before each resume is sent, so a test
	// can change the cluster between Populate's reads and the resume.
	beforeResume func(context.Context) error
}

var _ Operations = (*fakeOperations)(nil)

// newFakeOperations returns a fakeOperations over newCluster that holds no
// Secret.
func newFakeOperations(t *testing.T) *fakeOperations {
	t.Helper()
	return fakeOperationsOn(newCluster(t))
}

// fakeOperationsOn returns a fakeOperations over the cluster given.
func fakeOperationsOn(c *strictclient.Client) *fakeOperations {
	return &fakeOperations{Jobs: NewJobs(c, c), cluster: c, secrets: make(map[string]*corev1.Secret)}
}

// CreateJob counts the create and sends it to the cluster.
func (f *fakeOperations) CreateJob(ctx context.Context, job *batchv1.Job) error {
	f.jobCreates++
	if f.refuseCreate != nil {
		return f.refuseCreate
	}
	return f.Jobs.CreateJob(ctx, job)
}

// ResumeJob runs beforeResume and sends the resume to the cluster, unless
// refuseResume is set.
func (f *fakeOperations) ResumeJob(ctx context.Context, job *batchv1.Job) error {
	if f.beforeResume != nil {
		if err := f.beforeResume(ctx); err != nil {
			return err
		}
	}
	if f.refuseResume != nil {
		return f.refuseResume
	}
	return f.Jobs.ResumeJob(ctx, job)
}

func (f *fakeOperations) GetVolume(ctx context.Context, name string) (*corev1.PersistentVolume, error) {
	volume := &corev1.PersistentVolume{}
	return volume, f.cluster.Get(ctx, client.ObjectKey{Name: name}, volume)
}

func (f *fakeOperations) GetNamespace(ctx context.Context, name string) (*corev1.Namespace, error) {
	ns := &corev1.Namespace{}
	return ns, f.cluster.Get(ctx, client.ObjectKey{Name: name}, ns)
}

func (f *fakeOperations) GetClaim(ctx context.Context, namespace, name string) (*corev1.PersistentVolumeClaim, error) {
	claim := &corev1.PersistentVolumeClaim{}
	return claim, f.cluster.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, claim)
}

func (f *fakeOperations) PatchClaim(ctx context.Context, claim *corev1.PersistentVolumeClaim, patch client.Patch) error {
	return f.cluster.Patch(ctx, claim, patch)
}

func (f *fakeOperations) GetSecret(_ context.Context, namespace, name string) (*corev1.Secret, error) {
	if secret, ok := f.secrets[namespacedName(namespace, name)]; ok {
		return secret.DeepCopy(), nil
	}
	return nil, apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
}

func (f *fakeOperations) CreateSecret(_ context.Context, secret *corev1.Secret) error {
	key := namespacedName(secret.Namespace, secret.Name)
	if _, ok := f.secrets[key]; ok {
		return apierrors.NewAlreadyExists(schema.GroupResource{Resource: "secrets"}, secret.Name)
	}
	f.secrets[key] = secret.DeepCopy()
	f.createdSec = append(f.createdSec, secret.DeepCopy())
	return nil
}

func (f *fakeOperations) DeleteSecret(_ context.Context, namespace, name string) error {
	key := namespacedName(namespace, name)
	if _, ok := f.secrets[key]; !ok {
		return apierrors.NewNotFound(schema.GroupResource{Resource: "secrets"}, name)
	}
	delete(f.secrets, key)
	f.deletedSec = append(f.deletedSec, key)
	return nil
}

func (f *fakeOperations) SetStatus(_ context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if f.volumeRestoreGone {
		return apierrors.NewNotFound(schema.GroupResource{Group: "backup.wlz.li", Resource: "volumerestores"}, vr.Name)
	}
	f.statuses = append(f.statuses, vr.DeepCopy())
	return nil
}

func (f *fakeOperations) UpdateVolumeRestore(_ context.Context, vr *backupv1alpha1.VolumeRestore) error {
	if f.volumeRestoreGone {
		return apierrors.NewNotFound(schema.GroupResource{Group: "backup.wlz.li", Resource: "volumerestores"}, vr.Name)
	}
	f.updates = append(f.updates, vr.DeepCopy())
	return nil
}

// job reads a claim's restore Job, or returns nil when there is none.
func (f *fakeOperations) job(t *testing.T, claimUID types.UID) *batchv1.Job {
	t.Helper()
	job := &batchv1.Job{}
	err := f.cluster.Get(context.Background(), client.ObjectKey{Namespace: controllerNS, Name: JobName(claimUID)}, job)
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return job
}

// prime reads a claim's prime claim.
func (f *fakeOperations) prime(t *testing.T, claimUID types.UID) *corev1.PersistentVolumeClaim {
	t.Helper()
	prime := &corev1.PersistentVolumeClaim{}
	if err := f.cluster.Get(context.Background(), client.ObjectKey{Namespace: controllerNS, Name: PrimeClaimName(claimUID)}, prime); err != nil {
		t.Fatal(err)
	}
	return prime
}

// setJob changes a claim's restore Job's status as the Job controller would,
// and returns the Job as stored.
func (f *fakeOperations) setJob(t *testing.T, claimUID types.UID, change func(*batchv1.Job)) *batchv1.Job {
	t.Helper()
	job := f.job(t, claimUID)
	change(job)
	if err := f.cluster.Status().Update(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return f.job(t, claimUID)
}

// createJob creates a restore Job for claim notes as Populate builds it, in
// the state the Job controller leaves it in right after the create: suspended,
// with Suspended=True.
func (f *fakeOperations) createJob(t *testing.T) *batchv1.Job {
	t.Helper()
	return f.createJobFor(t, 0)
}

// createJobFor creates the restore Job of the claim testClaims[i], as
// createJob does for notes.
func (f *fakeOperations) createJobFor(t *testing.T, i int) *batchv1.Job {
	t.Helper()
	c := testClaims[i]
	job, err := restorejob.Build(restorejob.Spec{
		Name: JobName(c.uid), Namespace: controllerNS,
		Origin:     restorejob.Origin{Kind: restorejob.OriginClaim, UID: c.uid},
		Owner:      metav1.OwnerReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: PrimeClaimName(c.uid), UID: c.primeUID},
		SnapshotID: monday.ID, Claim: PrimeClaimName(c.uid), Repository: string(c.uid), Image: testImage,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := f.cluster.Create(context.Background(), job); err != nil {
		t.Fatal(err)
	}
	return f.setJob(t, c.uid, func(j *batchv1.Job) { withCondition(j, batchv1.JobSuspended) })
}

// runJob resumes a claim's Job as its creator does and marks it resumed as
// the Job controller does, and returns it.
func (f *fakeOperations) runJob(t *testing.T, claimUID types.UID) *batchv1.Job {
	t.Helper()
	if err := f.ResumeJob(context.Background(), f.job(t, claimUID)); err != nil {
		t.Fatal(err)
	}
	return f.setJob(t, claimUID, func(j *batchv1.Job) {
		j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobSuspended, Status: corev1.ConditionFalse, Reason: "JobResumed"}}
	})
}

// createPod creates a pod of a Job as the Job controller does, on the node
// given (none for an unscheduled pod), in the phase given.
func (f *fakeOperations) createPod(t *testing.T, job *batchv1.Job, name, node string, phase corev1.PodPhase) *corev1.Pod {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: job.Namespace,
			Labels: map[string]string{batchv1.ControllerUidLabel: string(job.UID), batchv1.JobNameLabel: job.Name},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "Job", Name: job.Name, UID: job.UID,
				Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true)}},
		},
		Spec: corev1.PodSpec{NodeName: node, Containers: []corev1.Container{{Name: "restore", Image: testImage}}},
	}
	for key, value := range job.Spec.Template.Labels {
		pod.Labels[key] = value
	}
	if err := f.cluster.Create(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
	f.setPod(t, pod, func(p *corev1.Pod) { p.Status.Phase = phase })
	return pod
}

// setPod changes a pod's status as its kubelet would.
func (f *fakeOperations) setPod(t *testing.T, pod *corev1.Pod, change func(*corev1.Pod)) {
	t.Helper()
	if err := f.cluster.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	change(pod)
	if err := f.cluster.Status().Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
}

// withCondition sets a Job condition True.
func withCondition(job *batchv1.Job, typ batchv1.JobConditionType) {
	job.Status.Conditions = append(job.Status.Conditions, batchv1.JobCondition{Type: typ, Status: corev1.ConditionTrue, Reason: string(typ)})
}

// failed marks a Job as the Job controller marks one that met its backoff
// limit.
func failed(job *batchv1.Job) {
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{Type: batchv1.JobFailureTarget, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded"},
		batchv1.JobCondition{Type: batchv1.JobFailed, Status: corev1.ConditionTrue, Reason: "BackoffLimitExceeded", Message: "Job has reached the specified backoff limit"})
}

// complete marks a Job as the Job controller marks one whose pod succeeded.
func complete(job *batchv1.Job) {
	job.Status.Conditions = append(job.Status.Conditions,
		batchv1.JobCondition{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
		batchv1.JobCondition{Type: batchv1.JobComplete, Status: corev1.ConditionTrue})
}

// fixedSnapshots is a SnapshotLister that returns the same snapshots for
// every Secret.
type fixedSnapshots []restic.Snapshot

func (f fixedSnapshots) Snapshots(context.Context, *corev1.Secret) ([]restic.Snapshot, error) {
	return f, nil
}

// moverSnapshot returns a snapshot with the layout VolSync's backup mover
// gives one, the full ID given and the time given.
func moverSnapshot(id string, at time.Time) restic.Snapshot {
	return restic.Snapshot{ID: id, Time: at, Hostname: "volsync", Paths: []string{"/data"}}
}

// monday and tuesday are mover snapshots taken at 05:00 UTC on Monday 21
// and Tuesday 22 September 2026.
var (
	monday  = moverSnapshot("6e4731000000000000000000000000000000000000000000000000000000aaaa", time.Date(2026, 9, 21, 5, 0, 0, 0, time.UTC))
	tuesday = moverSnapshot("7f5842000000000000000000000000000000000000000000000000000000bbbb", time.Date(2026, 9, 22, 5, 0, 0, 0, time.UTC))
)

// newCallbacks returns the callbacks over ops for backup-system, with a
// repository that holds the snapshots given.
func newCallbacks(ops Operations, snapshots ...restic.Snapshot) *Callbacks {
	return New(ops, controllerNS, testImage, fixedSnapshots(snapshots))
}

// librarySync runs one sync of the populator library for a bound prime
// claim: Populate, then Complete only when Populate returned nil, as the
// library does (lib-volume-populator v3.3.0 populator-machinery
// controller.go:809-827).
func librarySync(ctx context.Context, callbacks *Callbacks, params populatormachinery.PopulatorParams) (bool, error) {
	if err := callbacks.Populate(ctx, params); err != nil {
		return false, err
	}
	return callbacks.Complete(ctx, params)
}

// jobTracking is the finalizer the Job controller puts on every pod it
// creates and removes once it has counted the pod's end. A pod that carries
// it stays after a delete, as a pod does on a real cluster until then.
const jobTracking = "batch.kubernetes.io/job-tracking"

// addTracking puts jobTracking on a pod.
func (f *fakeOperations) addTracking(t *testing.T, pod *corev1.Pod) {
	t.Helper()
	if err := f.cluster.Get(context.Background(), client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = append(pod.Finalizers, jobTracking)
	if err := f.cluster.Update(context.Background(), pod); err != nil {
		t.Fatal(err)
	}
}

// removeTracking takes jobTracking off a pod, which deletes a pod being
// deleted, and then does what the garbage collector does next: it removes
// the foregroundDeletion finalizer of the claim's Job once no pod of it is
// left, which deletes the Job.
func (f *fakeOperations) removeTracking(t *testing.T, pod *corev1.Pod, claimUID types.UID) {
	t.Helper()
	ctx := context.Background()
	if err := f.cluster.Get(ctx, client.ObjectKeyFromObject(pod), pod); err != nil {
		t.Fatal(err)
	}
	pod.Finalizers = nil
	if err := f.cluster.Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
	job := f.job(t, claimUID)
	if job == nil {
		return
	}
	job.Finalizers = slices.DeleteFunc(job.Finalizers, func(s string) bool { return s == metav1.FinalizerDeleteDependents })
	if err := f.cluster.Update(ctx, job); err != nil {
		t.Fatal(err)
	}
}
