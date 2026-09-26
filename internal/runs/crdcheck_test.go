package runs

import (
	"context"
	"errors"
	"os"
	"reflect"
	"strings"
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

// readCRD decodes the CRD file at path.
func readCRD(t *testing.T, path string) *unstructured.Unstructured {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	crd := &unstructured.Unstructured{}
	if err := yaml.Unmarshal(data, &crd.Object); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return crd
}

// The CRDs make manifests generates declare every field of their Go types,
// and the v0.7.2 CRDs lack exactly the fields v0.8 and v0.9 added. This is
// also the guard against the generated CRDs drifting from the types.
func TestSchemaGapsNamesTheFieldsAnOldCRDLacks(t *testing.T) {
	for _, tc := range []struct {
		file   string
		sample any
		old    []string
	}{
		{"backup.wlz.li_backupruns.yaml", backupv1alpha1.BackupRun{}, []string{"status.restartPending"}},
		{"backup.wlz.li_restoreruns.yaml", backupv1alpha1.RestoreRun{}, []string{"status.items[].clusterUID", "status.items[].snapshotTime"}},
		{"backup.wlz.li_volumerestores.yaml", backupv1alpha1.VolumeRestore{}, nil},
	} {
		gaps, err := schemaGaps(readCRD(t, ownCRDDir+tc.file), tc.sample)
		if err != nil || len(gaps) != 0 {
			t.Errorf("%s: gaps = %v, %v; want none", tc.file, gaps, err)
		}
		gaps, err = schemaGaps(readCRD(t, oldCRDDir+tc.file), tc.sample)
		if err != nil || !reflect.DeepEqual(gaps, tc.old) {
			t.Errorf("v0.7.2 %s: gaps = %v, %v; want %v", tc.file, gaps, err, tc.old)
		}
	}
}

// A quiesced run under the v0.7.2 BackupRun CRD, which drops
// status.restartPending, ends CRDOutdated at plan, and its app keeps running.
func TestABackupRunUnderAnOldCRDStopsNothing(t *testing.T) {
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

// Under the CRDs of this release the check passes and the run goes on.
func TestABackupRunUnderTheCurrentCRDProceeds(t *testing.T) {
	r, c := backupReconciler(t, backupRun(func(b *backupv1alpha1.BackupRun) { b.Spec.All = true }),
		claim(), volume(), volumeRestore(), repository(), cluster(), deployment(), kustomization(false))
	step(t, r) // plan
	if run := readBackupRun(t, c); run.Status.Phase != backupv1alpha1.RunPhaseQueued {
		t.Fatalf("phase = %q (%s), want Queued", run.Status.Phase, readyReason(run.Status.Conditions))
	}
}

// A RestoreRun under the v0.7.2 RestoreRun CRD, which drops the items'
// clusterUID and snapshotTime, ends CRDOutdated at plan.
func TestARestoreRunUnderAnOldCRDEndsBeforePlanning(t *testing.T) {
	c := newClientWithCRDs(t, withOldCRD("backup.wlz.li_restoreruns.yaml"),
		restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Spec.Claim = claimN }, asOf("2026-09-21T04:00:00Z")),
		claim(), volumeRestore(), repository())
	r := &RestoreRunReconciler{Client: c, Reader: c, Snapshots: snapshots{sunday, monday}, Now: func() time.Time { return frozen }}
	restoreStep(t, r)
	run := &backupv1alpha1.RestoreRun{}
	get(t, c, ns, "back-to-monday", run)
	ready := meta.FindStatusCondition(run.Status.Conditions, backupv1alpha1.ConditionReady)
	if run.Status.Phase != backupv1alpha1.RunPhaseFailed || ready == nil || ready.Reason != backupv1alpha1.ReasonCRDOutdated {
		t.Fatalf("phase = %q, Ready = %+v; want Failed with reason CRDOutdated", run.Status.Phase, ready)
	}
	if !strings.Contains(ready.Message, "status.items[].clusterUID") {
		t.Errorf("message %q does not name status.items[].clusterUID", ready.Message)
	}
	if len(run.Status.Items) != 0 {
		t.Errorf("items = %+v; a refused run plans nothing", run.Status.Items)
	}
}

// A controller that may not read the CRD refuses the run and names the
// permission, since an unchecked run could leave workloads stopped.
func TestABackupRunWithoutCRDAccessFailsClosed(t *testing.T) {
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
