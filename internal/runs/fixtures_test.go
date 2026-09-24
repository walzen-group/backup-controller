package runs

import (
	"context"
	"testing"
	"time"

	volsyncv1alpha1 "github.com/backube/volsync/api/v1alpha1"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/bootstrap"
	"github.com/walzen-group/backup-controller/internal/restic"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// frozen is the clock every test runs on, so a deadline is a subtraction rather
// than a sleep.
var frozen = time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)

const (
	ns     = "notes"
	claimN = "notes-data"
	repoN  = "notes-restic-data"
	pgN    = "notes-pg"
	appN   = "notes"
	runUID = types.UID("3f2a1c7e-0000-4000-8000-000000000001")
)

// The kinds the package reads as unstructured, registered by GVK so the fake
// client can store them.
var unstructuredKinds = []schema.GroupVersionKind{
	ClusterGVK, BackupGVK, KustomizationGVK, WorkloadGVK,
	{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "LocalQueue"},
	bootstrap.ObjectStoreGVK,
}

func scheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme, appsv1.AddToScheme, volsyncv1alpha1.AddToScheme, backupv1alpha1.AddToScheme,
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

// newClient builds a fake client holding objects, with the status subresource
// on every kind the package writes status to.
func newClient(t *testing.T, objects ...client.Object) client.Client {
	t.Helper()
	workload := &unstructured.Unstructured{}
	workload.SetGroupVersionKind(WorkloadGVK)
	return fake.NewClientBuilder().
		WithScheme(scheme(t)).
		WithObjects(objects...).
		WithStatusSubresource(&backupv1alpha1.BackupRun{}, &backupv1alpha1.RestoreRun{},
			&volsyncv1alpha1.ReplicationSource{}, &volsyncv1alpha1.ReplicationDestination{}, workload).
		Build()
}

// snapshots is a restic.Lister holding a fixed list.
type snapshots []restic.Snapshot

func (s snapshots) Snapshots(context.Context, *corev1.Secret) ([]restic.Snapshot, error) {
	return s, nil
}

var (
	sunday = restic.Snapshot{ID: "2edf5bab" + "00000000", Time: time.Date(2026, 9, 20, 5, 0, 2, 0, time.UTC)}
	monday = restic.Snapshot{ID: "6e473100" + "00000000", Time: time.Date(2026, 9, 21, 5, 0, 2, 0, time.UTC)}
)

// prober answers the base backup questions without an object store.
type prober []bootstrap.BaseBackup

func (p prober) HasBaseBackup(context.Context, bootstrap.Location) (bool, error) {
	return len(p) > 0, nil
}

func (p prober) BaseBackups(context.Context, bootstrap.Location) ([]bootstrap.BaseBackup, error) {
	return p, nil
}

var saturday = bootstrap.BaseBackup{ID: "20260919T030000", End: time.Date(2026, 9, 19, 3, 0, 40, 0, time.UTC)}

func enabled() map[string]string {
	return map[string]string{backupv1alpha1.AnnotationEnabled: "true", backupv1alpha1.AnnotationRetainLast: "10"}
}

// claim is a backed-up dynamic claim, bound to a zfs-localpv volume.
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

// volume is the claim's PersistentVolume, with the node affinity zfs-localpv
// writes on every volume it provisions.
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

func repository() *corev1.Secret {
	return &corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: repoN, Namespace: ns}}
}

// cluster is a Cluster archiving through the plugin, marked enabled.
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
	c.SetAnnotations(map[string]string{backupv1alpha1.AnnotationEnabled: "true"})
	for _, m := range mutate {
		m(c)
	}
	return c
}

// objectStore and storeSecret are what ResolveLocation reads for the Cluster.
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

func storeSecret() *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: pgN + "-backup", Namespace: ns},
		Data:       map[string][]byte{"ACCESS_KEY_ID": []byte("key"), "ACCESS_SECRET_KEY": []byte("secret")},
	}
}

// deployment is the app, marked for quiesce and applied by Flux.
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

func kustomization(suspended bool) *unstructured.Unstructured {
	k := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"suspend": suspended}}}
	k.SetGroupVersionKind(KustomizationGVK)
	k.SetNamespace("flux-system")
	k.SetName(appN)
	return k
}

func localQueueObject() *unstructured.Unstructured {
	q := &unstructured.Unstructured{Object: map[string]any{"spec": map[string]any{"clusterQueue": "backups"}}}
	q.SetGroupVersionKind(schema.GroupVersionKind{Group: "kueue.x-k8s.io", Version: "v1beta2", Kind: "LocalQueue"})
	q.SetNamespace(ns)
	q.SetName("backups")
	return q
}

func get(t *testing.T, c client.Client, namespace, name string, object client.Object) {
	t.Helper()
	if err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, object); err != nil {
		t.Fatalf("get %s/%s: %v", namespace, name, err)
	}
}

func getUnstructured(t *testing.T, c client.Client, gvk schema.GroupVersionKind, namespace, name string) (*unstructured.Unstructured, bool) {
	t.Helper()
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(gvk)
	err := c.Get(context.Background(), types.NamespacedName{Namespace: namespace, Name: name}, u)
	return u, err == nil
}

func readyReason(conditions []metav1.Condition) string {
	for _, c := range conditions {
		if c.Type == backupv1alpha1.ConditionReady {
			return c.Reason
		}
	}
	return ""
}
