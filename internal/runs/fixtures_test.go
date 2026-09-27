package runs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restic"
	"github.com/walzen-group/backup-controller/internal/testinfra/strictclient"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/yaml"
)

// frozen is the time the tests' clocks stand at. A test reaches a deadline by
// setting the clock to a later time, so no test sleeps.
var frozen = time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

// The names of the test namespace and of the objects in it, and the UID that
// every test run gets.
const (
	ns     = "notes"
	claimN = "notes-data"
	repoN  = "notes-restic-data"
	pgN    = "notes-pg"
	appN   = "notes"
	runUID = types.UID("3f2a1c7e-0000-4000-8000-000000000001")
)

// unstructuredKinds are the kinds the package reads as unstructured objects.
// scheme registers each one, and its list kind, so the fake client can store
// them.
var unstructuredKinds = []schema.GroupVersionKind{
	ClusterGVK, BackupGVK, KustomizationGVK, WorkloadGVK,
	{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "LocalQueue"},
	{Group: "kueue.x-k8s.io", Version: "v1beta1", Kind: "LocalQueue"},
	{Group: "kueue.x-k8s.io", Version: "v1beta1", Kind: "Workload"},
	bootstrap.ObjectStoreGVK, crdGVK,
	// The CloudNativePG and barman-cloud kinds at the version the version
	// tests serve them at alone (see crdsServedAtNext).
	{Group: "postgresql.cnpg.io", Version: "v2", Kind: "Cluster"},
	{Group: "postgresql.cnpg.io", Version: "v2", Kind: "Backup"},
	{Group: "barmancloud.cnpg.io", Version: "v2", Kind: "ObjectStore"},
}

// scheme returns a scheme that holds the typed kinds the package uses and the
// kinds in unstructuredKinds.
func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, batchv1.AddToScheme, volsyncv1alpha1.AddToScheme, backupv1alpha1.AddToScheme, coordinationv1.AddToScheme,
	} {
		if err := add(s); err != nil {
			t.Fatalf("register types: %v", err)
		}
	}
	for _, gvk := range unstructuredKinds {
		s.AddKnownTypeWithName(gvk, &unstructured.Unstructured{})
		s.AddKnownTypeWithName(gvk.GroupVersion().WithKind(gvk.Kind+"List"), &unstructured.UnstructuredList{})
	}
	return s
}

// crdDir is the folder of pinned third-party CustomResourceDefinitions the
// strict client prunes against.
const crdDir = "../testinfra/crds/"

// ownCRDDir holds this repository's own CRDs as the current code generates
// them. The tests prune against these, so a field added to the API is kept in
// the tests the moment make manifests writes it. The copies of older releases
// under crdDir/backup-controller serve the upgrade tests only.
const ownCRDDir = "../../config/crd/"

// crds are the CRDs of the kinds the package reads or writes: this
// repository's own from config/crd, and the pinned VolSync
// ReplicationSource and ReplicationDestination, CloudNativePG Cluster and
// Backup, Kueue Workload and LocalQueue, Flux Kustomization and the barman
// cloud plugin's ObjectStore.
var crds = []string{
	ownCRDDir + "backup.wlz.li_backupruns.yaml",
	ownCRDDir + "backup.wlz.li_restoreruns.yaml",
	ownCRDDir + "backup.wlz.li_volumerestores.yaml",
	crdDir + "volsync/volsync.backube_replicationsources.yaml",
	crdDir + "volsync/volsync.backube_replicationdestinations.yaml",
	crdDir + "cloudnative-pg/postgresql.cnpg.io_clusters.yaml",
	crdDir + "cloudnative-pg/postgresql.cnpg.io_backups.yaml",
	crdDir + "kueue/kueue.x-k8s.io_workloads.yaml",
	crdDir + "kueue/kueue.x-k8s.io_localqueues.yaml",
	crdDir + "flux/kustomize-controller.crds.yaml",
	crdDir + "plugin-barman-cloud/barmancloud.cnpg.io_objectstores.yaml",
}

// newClient builds a strict fake client that holds the given objects. It sets
// the server-owned metadata as kube-apiserver does, prunes custom resources
// against the pinned CRDs in crds, and gives every kind the package uses the
// status subresource its CRD declares. It also holds the Namespace notes
// unless the objects hold one (see withNamespace). The server clock stands
// at frozen.
func newClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	return newClientWithCRDs(t, crds, objects...)
}

// newClientWithCRDs builds a strict fake client as newClient does, pruning
// against the CRD files in crdFiles. The client also holds this project's
// CustomResourceDefinitions from those files as objects, so a run's schema
// check reads the same CRDs the client prunes against. Its RESTMapper is a
// *servedKinds that serves the scheme's built-in kinds and the versions the
// CRD files serve, as the API server's discovery would.
func newClientWithCRDs(t *testing.T, crdFiles []string, objects ...client.Object) client.Client {
	t.Helper()
	now := frozen
	objects = append(withNamespace(objects), ownCRDObjects(t, crdFiles)...)
	s := scheme(t)
	c := strictclient.Build(fake.NewClientBuilder().WithObjects(objects...).WithRESTMapper(newServedKinds(t, s, crdFiles)), s, strictclient.Options{
		Clock: func() time.Time { return now },
		CRDs:  crdFiles,
	})
	serverClocks[c] = &now
	t.Cleanup(func() { delete(serverClocks, c) })
	return c
}

// serverClocks holds the API server clock of each client newClient built
// that a test has not finished with. The clock starts at frozen.
var serverClocks = map[client.Client]*time.Time{}

// atServerTime sets the API server clock of c, a client newClient built, to
// at, so the objects created until the returned function runs get at as their
// creationTimestamp. The returned function sets the clock back to what it
// was.
func atServerTime(t *testing.T, c client.Client, at time.Time) func() {
	t.Helper()
	now, ok := serverClocks[c]
	if !ok {
		t.Fatal("the client was not built by newClient")
	}
	was := *now
	*now = at
	return func() { *now = was }
}

// snapshots is a restic.Lister that returns a fixed list of snapshots.
type snapshots []restic.Snapshot

// Snapshots returns the fixed list, whatever repository it is asked about.
func (s snapshots) Snapshots(context.Context, *corev1.Secret) ([]restic.Snapshot, error) {
	return s, nil
}

// sunday and monday are two snapshots a VolSync mover took a day apart, each
// of /data from host volsync at 05:00:02 UTC.
var (
	sunday = restic.Snapshot{ID: fullID("2edf5bab"), Time: time.Date(2026, 9, 20, 5, 0, 2, 0, time.UTC), Hostname: "volsync", Paths: []string{"/data"}}
	monday = restic.Snapshot{ID: fullID("6e473100"), Time: time.Date(2026, 9, 21, 5, 0, 2, 0, time.UTC), Hostname: "volsync", Paths: []string{"/data"}}
)

// retimeCall holds the arguments of one call to retimer.Retime.
type retimeCall struct {
	id  string
	at  time.Time
	tag string
}

// retimer is a restic.Retimer that records each call. It answers with the
// snapshot rewritten under an ID that starts with c0ffee00, or with the error
// in err while err is set.
type retimer struct {
	calls []retimeCall
	err   error
}

// Retime records the call and returns the rewritten snapshot, which carries
// the requested time and tag, or returns r.err when it is set.
func (r *retimer) Retime(_ context.Context, _ *corev1.Secret, id string, at time.Time, tag string) (restic.Snapshot, error) {
	r.calls = append(r.calls, retimeCall{id: id, at: at, tag: tag})
	if r.err != nil {
		return restic.Snapshot{}, r.err
	}
	return restic.Snapshot{ID: fullID("c0ffee00"), Time: at, Tags: []string{tag}, Original: id}, nil
}

// prober answers the questions about a Cluster's base backups from a fixed
// list, with no object store behind it.
type prober []bootstrap.BaseBackup

// BaseBackups returns the fixed list.
func (p prober) BaseBackups(context.Context, bootstrap.Location) ([]bootstrap.BaseBackup, error) {
	return p, nil
}

// saturday is a base backup that ended at 03:00:40 UTC on 19 September 2026.
var saturday = bootstrap.BaseBackup{ID: "20260919T030000", End: time.Date(2026, 9, 19, 3, 0, 40, 0, time.UTC)}

// enabled returns the annotations that mark a claim for backup and keep its
// newest 10 snapshots.
func enabled() map[string]string {
	return map[string]string{backupv1alpha1.AnnotationEnabled: "true", backupv1alpha1.AnnotationRetainLast: "10"}
}

// claim returns a dynamic claim that is marked for backup, filled from the
// VolumeRestore of the same name, and bound to a zfs-localpv volume.
func claim() *corev1.PersistentVolumeClaim {
	class := "zfs"
	group := backupv1alpha1.GroupVersion.Group
	return &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: claimN, Namespace: ns, UID: "claim-uid", Annotations: enabled()},
		Spec: corev1.PersistentVolumeClaimSpec{
			AccessModes:      []corev1.PersistentVolumeAccessMode{corev1.ReadWriteOnce},
			StorageClassName: &class,
			VolumeName:       "pvc-notes-data",
			Resources: corev1.VolumeResourceRequirements{
				Requests: corev1.ResourceList{corev1.ResourceStorage: resource.MustParse("1Gi")},
			},
			DataSourceRef: &corev1.TypedObjectReference{APIGroup: &group, Kind: "VolumeRestore", Name: claimN},
		},
		Status: corev1.PersistentVolumeClaimStatus{Phase: corev1.ClaimBound},
	}
}

// volume returns the claim's PersistentVolume, with the node affinity that
// zfs-localpv writes on every volume it provisions.
func volume() *corev1.PersistentVolume {
	return &corev1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "pvc-notes-data"},
		Spec: corev1.PersistentVolumeSpec{
			NodeAffinity: &corev1.VolumeNodeAffinity{Required: &corev1.NodeSelector{
				NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key: "openebs.io/nodeid", Operator: corev1.NodeSelectorOpIn, Values: []string{"worker-1"},
				}}}},
			}},
		},
	}
}

// volumeRestore returns the claim's VolumeRestore. It names the repository
// Secret, a cache class, and the Kueue queue label for the mover.
func volumeRestore() *backupv1alpha1.VolumeRestore {
	class := "zfs-ephemeral"
	return &backupv1alpha1.VolumeRestore{
		ObjectMeta: metav1.ObjectMeta{Name: claimN, Namespace: ns},
		Spec: backupv1alpha1.VolumeRestoreSpec{
			Repository:            repoN,
			CacheStorageClassName: &class,
			MoverPodLabels:        map[string]backupv1alpha1.MoverPodLabelValue{"kueue.x-k8s.io/queue-name": "backups"},
		},
	}
}

// repository returns the claim's restic repository Secret.
func repository() *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: repoN, Namespace: ns}}
}

// cluster returns a Cluster that is marked for backup and archives its WAL
// through the barman-cloud plugin. Its UID is old-cluster-uid, and a test
// that stands in for the Cluster being created again gives the new one
// another UID. Each function passed in mutate changes the
// Cluster before it is returned.
func cluster(mutate ...func(*unstructured.Unstructured)) *unstructured.Unstructured {
	c := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{
			"plugins": []any{map[string]any{
				"name":          bootstrap.PluginName,
				"isWALArchiver": true,
				"parameters":    map[string]any{"barmanObjectName": pgN + "-store"},
			}},
		},
	}}
	c.SetGroupVersionKind(ClusterGVK)
	c.SetNamespace(ns)
	c.SetName(pgN)
	c.SetUID("old-cluster-uid")
	c.SetAnnotations(map[string]string{backupv1alpha1.AnnotationEnabled: "true"})
	for _, m := range mutate {
		m(c)
	}
	return c
}

// objectStore returns the barman-cloud ObjectStore the Cluster archives to.
// bootstrap.ResolveLocation reads it and storeSecret to find where the
// Cluster's backups are.
func objectStore() *unstructured.Unstructured {
	s := &unstructured.Unstructured{}
	s.SetGroupVersionKind(bootstrap.ObjectStoreGVK)
	s.SetNamespace(ns)
	s.SetName(pgN + "-store")
	_ = unstructured.SetNestedMap(s.Object, map[string]any{
		"destinationPath": "s3://cnpg/" + ns,
		"endpointURL":     "http://store.example:10172",
		"s3Credentials": map[string]any{
			"accessKeyId":     map[string]any{"name": pgN + "-backup", "key": "ACCESS_KEY_ID"},
			"secretAccessKey": map[string]any{"name": pgN + "-backup", "key": "ACCESS_SECRET_KEY"},
		},
	}, "spec", "configuration")
	return s
}

// storeSecret returns the Secret that holds the ObjectStore's credentials.
func storeSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: pgN + "-backup", Namespace: ns},
		Data:       map[string][]byte{"ACCESS_KEY_ID": []byte("key"), "ACCESS_SECRET_KEY": []byte("secret")},
	}
}

// deployment returns the app's Deployment with two replicas. It is marked for
// quiesce and carries the labels of the Flux Kustomization that applies it.
func deployment() *appsv1.Deployment {
	replicas := int32(2)
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name: appN, Namespace: ns,
			Annotations: map[string]string{backupv1alpha1.AnnotationQuiesce: "true"},
			Labels:      map[string]string{fluxNameLabel: appN, fluxNamespaceLabel: "flux-system"},
		},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": appN}},
		},
	}
}

// kustomization returns the Flux Kustomization that applies the app, with
// spec.suspend set to the value given in suspended. Its inventory lists the
// app's Deployment, the way kustomize-controller records each object it
// applied.
func kustomization(suspended bool) *unstructured.Unstructured {
	k := &unstructured.Unstructured{Object: map[string]any{
		"spec": map[string]any{"suspend": suspended},
		"status": map[string]any{"inventory": map[string]any{"entries": []any{
			map[string]any{"id": ns + "_" + appN + "_apps_Deployment", "v": "v1"},
		}}},
	}}
	k.SetGroupVersionKind(KustomizationGVK)
	k.SetNamespace("flux-system")
	k.SetName(appN)
	return k
}

// localQueueObject returns a Kueue LocalQueue named backups in the test
// namespace.
func localQueueObject() *unstructured.Unstructured {
	q := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"clusterQueue": "backups"}}}
	q.SetGroupVersionKind(schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "LocalQueue"})
	q.SetNamespace(ns)
	q.SetName("backups")
	return q
}

// get reads one object into object and fails the test when it can't.
func get(t *testing.T, c client.Client, namespace, name string, object client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, object); err != nil {
		t.Fatalf("get %s/%s: %v", namespace, name, err)
	}
}

// getUnstructured reads one object of the given kind as unstructured. It
// returns false when the read fails, so a test can check that an object is
// absent.
func getUnstructured(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, bool) {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, u)
	return u, err == nil
}

// loseStatusWriteAt returns a client over c that fails one status write of a
// BackupRun or RestoreRun with a conflict: the first one made while the app's
// Deployment stands at the replica count given in replicas. With 0 it stands
// in for a quiesce pass whose status write is lost after the app was
// stopped, and with 2 for a pass that loses it after the app was started
// again. A conflict and a controller crash both lose the write.
func loseStatusWriteAt(c client.Client, replicas int32) client.Client {
	lost := false
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			_, backup := obj.(*backupv1alpha1.BackupRun)
			_, restore := obj.(*backupv1alpha1.RestoreRun)
			if (backup || restore) && !lost {
				d := &appsv1.Deployment{}
				if err := cl.Get(ctx, types.NamespacedName{Namespace: ns, Name: appN}, d); err == nil && d.Spec.Replicas != nil && *d.Spec.Replicas == replicas {
					lost = true
					return apierrors.NewConflict(backupv1alpha1.GroupVersion.WithResource("runs").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
				}
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}

// deploymentScale reports whether a subresource update is a write to a
// Deployment's scale subresource, the way quiesce and restart set the app's
// replica count, and returns the count it sets.
//
// Parameters:
//   - sub is the subresource the update names.
//   - obj is the object whose subresource is written.
//   - opts are the update's options, which carry the Scale as its body.
func deploymentScale(sub string, obj client.Object, opts []client.SubResourceUpdateOption) (int32, bool) {
	if _, ok := obj.(*appsv1.Deployment); !ok || sub != "scale" {
		return 0, false
	}
	o := &client.SubResourceUpdateOptions{}
	o.ApplyOptions(opts)
	scale, ok := o.SubResourceBody.(*autoscalingv1.Scale)
	if !ok {
		return 0, false
	}
	return scale.Spec.Replicas, true
}

// ownCRDObjects reads this project's CustomResourceDefinitions, the files in
// files whose name starts with backup.wlz.li_, as unstructured objects a fake
// client can hold.
func ownCRDObjects(t *testing.T, files []string) []client.Object {
	t.Helper()
	var objects []client.Object
	for _, path := range files {
		if !strings.HasPrefix(filepath.Base(path), backupv1alpha1.GroupVersion.Group+"_") {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		crd := &unstructured.Unstructured{}
		if err := yaml.Unmarshal(data, &crd.Object); err != nil {
			t.Fatalf("decode %s: %v", path, err)
		}
		objects = append(objects, crd)
	}
	return objects
}

// fullID returns a full, 64-character snapshot ID that starts with short
// and is padded with zeros, the form a restore Job restores by.
func fullID(short string) string {
	return short + strings.Repeat("0", 64-len(short))
}

// recordedSnapshots caches the snapshot lists read from the recorded
// repositories, keyed by directory, so each repository's key is derived once.
var recordedSnapshots sync.Map

// recordedRepository returns the snapshots of one repository that
// hack/fixtures/restic.sh recorded with the restic version VolSync's mover
// ships, read through the restic package as the controller reads a real
// repository.
//
// Parameters:
//   - kind names the fixture, such as same-second.
//
// It fails the test when the repository can't be opened or listed.
func recordedRepository(t *testing.T, kind string) snapshots {
	t.Helper()
	dir := filepath.Join("..", "restic", "testdata", "recorded", "restic-"+versions.Of(t, "restic-mover"), kind, "repo")
	if list, ok := recordedSnapshots.Load(dir); ok {
		return list.(snapshots)
	}
	repo, err := restic.Open(context.Background(), restic.DirStore(dir), "backup")
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	list, err := repo.Snapshots(context.Background())
	if err != nil {
		t.Fatalf("list %s: %v", dir, err)
	}
	recordedSnapshots.Store(dir, snapshots(list))
	return list
}

// moverSnapshot returns a snapshot the way a VolSync mover writes one: host
// volsync and the one path /data, at the given time, with the given tags.
func moverSnapshot(short string, at time.Time, tags ...string) restic.Snapshot {
	return restic.Snapshot{ID: fullID(short), Time: at, Hostname: "volsync", Paths: []string{"/data"}, Tags: tags}
}

// expectRefused checks that the run back-to-monday ended at its checks as
// Failed with reason NoBackupInReach, that the item's message holds every
// string in want, and that the run created no mover and no claim named
// scratch.
func expectRefused(t *testing.T, r *RestoreRunReconciler, want ...string) {
	t.Helper()
	c := r.Client
	run := readRestoreRun(t, c)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || readyReason(run.Status.Conditions) != backupv1alpha1.ReasonNoBackupInReach {
		t.Fatalf("phase = %q, reason = %q (%s); want Failed, NoBackupInReach", run.Status.Phase, readyReason(run.Status.Conditions), readyMessage(run.Status.Conditions))
	}
	message := readyMessage(run.Status.Conditions)
	if len(run.Status.Items) > 0 {
		message = run.Status.Items[0].Message
	}
	for _, w := range want {
		if !strings.Contains(message, w) {
			t.Errorf("message = %q, want it to hold %q", message, w)
		}
	}
	if names := movers(t, c); len(names) != 0 {
		t.Errorf("movers = %v, want none", names)
	}
	scratch := &corev1.PersistentVolumeClaim{}
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: ns, Name: "scratch"}, scratch); !apierrors.IsNotFound(err) {
		t.Errorf("claim scratch: %v, want it never created", err)
	}
}

// loseFailedRunWrite returns a client over c that fails, with a conflict,
// the first status write that ends a RestoreRun Failed: finish's write,
// which comes after it stopped the run's restore Jobs.
func loseFailedRunWrite(c client.Client) client.Client {
	lost := false
	return interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
		SubResourceUpdate: func(ctx context.Context, cl client.Client, sub string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if run, ok := obj.(*backupv1alpha1.RestoreRun); ok && !lost && run.Status.Phase == backupv1alpha1.RunPhaseFailed {
				lost = true
				return apierrors.NewConflict(backupv1alpha1.GroupVersion.WithResource("restoreruns").GroupResource(), obj.GetName(), errors.New("the object has been modified"))
			}
			return cl.SubResource(sub).Update(ctx, obj, opts...)
		},
	})
}
