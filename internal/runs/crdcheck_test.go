package runs

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"
)

// oldCRDDir holds the CRDs of v0.7.2, the release before status.restartPending
// and the RestoreRun items' clusterUID and snapshotTime.
const oldCRDDir = crdDir + "backup-controller/v0.7.2/"

// withOldCRD returns crds with this project's CRD file named file replaced by
// the v0.7.2 copy.
func withOldCRD(file string) []string {
	files := append([]string(nil), crds...)
	for i, f := range files {
		if f == ownCRDDir+file {
			files[i] = oldCRDDir + file
		}
	}
	return files
}

// crdObjectCache holds the CRD files that readCRD decoded so far, keyed by
// the path as the test gives it. Each file is then decoded one time per test
// binary. An entry does not change after readCRD stores it.
var crdObjectCache sync.Map // string -> *unstructured.Unstructured

// readCRD decodes the CRD file at path.
//
// It returns a deep copy of the decoded object, so the caller can change it.
// The first call for a path decodes the file, and later calls copy the
// cached object. A file that can't be read or decoded fails the test.
func readCRD(t *testing.T, path string) *unstructured.Unstructured {
	t.Helper()
	if crd, ok := crdObjectCache.Load(path); ok {
		return crd.(*unstructured.Unstructured).DeepCopy()
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	crd := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(data, &crd.Object); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	cached, _ := crdObjectCache.LoadOrStore(path, crd)
	return cached.(*unstructured.Unstructured).DeepCopy()
}

// v081CRDDir holds the CRDs of v0.8.1, the release before the run's ending and
// the items' typed state (snapshotID, job, reason, lastStartError and
// clusterLeftDeleted).
const v081CRDDir = crdDir + "backup-controller/v0.8.1/"

// The CRDs make manifests generates declare every field of their Go types,
// and each released CRD lacks exactly the fields the releases after it added.
// This is also the guard against the generated CRDs drifting from the types.
func TestSchemaGapsNamesTheFieldsAnOldCRDLacks(t *testing.T) {
	t.Parallel()
	v09Backup := []string{"status.ending", "status.items[].lastStartError", "status.items[].noSnapshotListedAt", "status.items[].reason", "status.items[].snapshotID", "status.resumedAt"}
	v09Restore := []string{"status.ending", "status.items[].clusterLeftDeleted", "status.items[].job", "status.items[].jobUID", "status.items[].reason", "status.items[].snapshotID", "status.resumedAt"}
	for _, tc := range []struct {
		file   string
		sample any
		v072   []string
		v081   []string
	}{
		{
			"backup.wlz.li_backupruns.yaml", backupv1alpha1.BackupRun{},
			[]string{"status.ending", "status.items[].lastStartError", "status.items[].noSnapshotListedAt", "status.items[].reason", "status.items[].snapshotID", "status.restartPending", "status.resumedAt"},
			v09Backup,
		},
		{
			"backup.wlz.li_restoreruns.yaml", backupv1alpha1.RestoreRun{},
			[]string{"status.ending", "status.items[].clusterLeftDeleted", "status.items[].clusterUID", "status.items[].job", "status.items[].jobUID", "status.items[].reason", "status.items[].snapshotID", "status.items[].snapshotTime", "status.resumedAt"},
			v09Restore,
		},
		{"backup.wlz.li_volumerestores.yaml", backupv1alpha1.VolumeRestore{}, nil, nil},
	} {
		for _, crd := range []struct {
			dir  string
			want []string
		}{{ownCRDDir, nil}, {oldCRDDir, tc.v072}, {v081CRDDir, tc.v081}} {
			gaps, err := schemaGaps(readCRD(t, crd.dir+tc.file), tc.sample)
			if err != nil || !reflect.DeepEqual(gaps, crd.want) {
				t.Errorf("%s%s: gaps = %v, %v; want %v", crd.dir, tc.file, gaps, err, crd.want)
			}
		}
	}
}

// A quiesced run under the v0.7.2 BackupRun CRD, which drops
// status.restartPending, ends CRDOutdated at plan, and its app keeps running.
func TestABackupRunUnderAnOldCRDStopsNothing(t *testing.T) {
	t.Parallel()
	c := newClientWithCRDs(t, withOldCRD("backup.wlz.li_backupruns.yaml"),
		backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false))
	r := &BackupRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Retimer: &retimer{}, Now: func() time.Time { return frozen }}

	for range 4 {
		step(t, r)
	}
	run := readBackupRun(t, c)
	ready := meta.FindStatusCondition(run.Status.Conditions, backupv1alpha1.ConditionReady)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || ready == nil || ready.Reason != backupv1alpha1.ReasonCRDOutdated {
		t.Fatalf("phase = %q, Ready = %+v; want Failed with reason CRDOutdated", run.Status.Phase, ready)
	}
	for _, want := range []string{"status.restartPending", "Apply the CRDs of this release"} {
		if !strings.Contains(ready.Message, want) {
			t.Errorf("message %q does not contain %q", ready.Message, want)
		}
	}
	d := &appsv1.Deployment{}
	get(t, c, ns, appN, d)
	if *d.Spec.Replicas != 2 {
		t.Errorf("replicas = %d, want the app left running at 2", *d.Spec.Replicas)
	}
	if len(run.Status.Items) != 0 {
		t.Errorf("items = %+v; a refused run plans nothing", run.Status.Items)
	}
}

// A controller that may not read the CRD refuses the run and names the
// permission, since an unchecked run could leave workloads stopped.
func TestABackupRunWithoutCRDAccessFailsClosed(t *testing.T) {
	t.Parallel()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false))
	r.Reader = forbidCRDs{c}
	step(t, r)
	step(t, r)
	run := readBackupRun(t, c)
	ready := meta.FindStatusCondition(run.Status.Conditions, backupv1alpha1.ConditionReady)
	if ready == nil || ready.Reason != backupv1alpha1.ReasonCRDOutdated || !strings.Contains(ready.Message, "customresourcedefinitions") {
		t.Fatalf("Ready = %+v, want reason CRDOutdated naming the permission", ready)
	}
}

// An item that the CRD check fails records the reason CRDOutdated, so that
// an alert on items[].reason sees why the item ended.
func TestAnItemTheCRDCheckFailsRecordsCRDOutdated(t *testing.T) {
	t.Parallel()
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) {
		b.Spec.Source = claimN
		b.Status.Items = []backupv1alpha1.BackupItem{{Kind: backupv1alpha1.ItemKindSource, Name: claimN, Phase: backupv1alpha1.ItemPending}}
	}), claim(), volume(), volumeRestore(), repository())
	r.Reader = forbidCRDs{c}
	step(t, r)

	run := readBackupRun(t, c)
	if len(run.Status.Items) != 1 {
		t.Fatalf("items = %+v, want the one item", run.Status.Items)
	}
	if item := run.Status.Items[0]; item.Phase != backupv1alpha1.ItemFailed || item.Reason != backupv1alpha1.ItemReasonCRDOutdated {
		t.Errorf("item = %+v, want Failed with reason CRDOutdated", item)
	}
}

// forbidCRDs is a Reader that refuses every read of a
// CustomResourceDefinition as RBAC would, and passes any other read on.
type forbidCRDs struct{ client.Reader }

// Get returns Forbidden for a CustomResourceDefinition.
func (f forbidCRDs) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if u, ok := obj.(*unstructured.Unstructured); ok && u.GroupVersionKind() == crdGVK {
		return apierrors.NewForbidden(schema.GroupResource{Group: "apiextensions.k8s.io", Resource: "customresourcedefinitions"}, key.Name, errors.New("RBAC: access denied"))
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}
