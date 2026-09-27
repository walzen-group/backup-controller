package runs

import (
	"context"
	"errors"
	"strings"
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// readStop gives an error, and keeps the copy of the run as it was, when the
// stored run is another object or when the read fails. The caller then
// changes no workload. If it took the stored restart of another object, a
// run could start the workloads again under a stop it does not own.
func TestReadStopRefusesAnotherObjectAndAFailedRead(t *testing.T) {
	t.Parallel()
	restarted := metav1.NewTime(frozen)
	runs := map[string]func() client.Object{
		"BackupRun": func() client.Object {
			return backupRun(func(b *backupv1alpha1.BackupRun) { b.Status.RestartedAt = &restarted })
		},
		"RestoreRun": func() client.Object {
			return restoreRun(func(r *backupv1alpha1.RestoreRun) { r.Status.RestartedAt = &restarted })
		},
	}
	for kind, stored := range runs {
		t.Run(kind+"/another object", func(t *testing.T) {
			c := newClient(t, stored())
			old := readOld(t, c, stored())
			old.SetUID(types.UID("3f2a1c7e-0000-4000-8000-00000000000f"))

			_, err := readStop(context.Background(), c, old)
			if err == nil || !strings.Contains(err.Error(), "is now another object") {
				t.Errorf("err = %v, want the other object refused", err)
			}
			if restartedAt(old) != nil {
				t.Error("the copy took the restart of another object")
			}
		})
		t.Run(kind+"/read fails", func(t *testing.T) {
			c := newClient(t, stored())
			old := readOld(t, c, stored())
			unavailable := apierrors.NewServiceUnavailable("etcd is not ready")
			failing := interceptor.NewClient(c.(client.WithWatch), interceptor.Funcs{
				Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
					return unavailable
				},
			})

			_, err := readStop(context.Background(), failing, old)
			if !errors.Is(err, unavailable) {
				t.Errorf("err = %v, want the failed read returned", err)
			}
			if restartedAt(old) != nil {
				t.Error("the copy changed although the read failed")
			}
		})
	}
}

// readOld reads the stored run and returns it as a pass that read it before
// the restart would hold it: with an older resourceVersion and no
// status.restartedAt.
func readOld(t *testing.T, c client.Client, run client.Object) client.Object {
	t.Helper()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(run), run); err != nil {
		t.Fatal(err)
	}
	run.SetResourceVersion("1")
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		r.Status.RestartedAt = nil
	case *backupv1alpha1.RestoreRun:
		r.Status.RestartedAt = nil
	}
	return run
}

// restartedAt returns status.restartedAt of a BackupRun or a RestoreRun.
func restartedAt(run client.Object) *metav1.Time {
	switch r := run.(type) {
	case *backupv1alpha1.BackupRun:
		return r.Status.RestartedAt
	case *backupv1alpha1.RestoreRun:
		return r.Status.RestartedAt
	}
	return nil
}
