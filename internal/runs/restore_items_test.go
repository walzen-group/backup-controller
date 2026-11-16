package runs

import (
	"testing"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/utils/ptr"
)

// TestLimitDownloadUsesTheFirstLimitSet checks the order in which a restore
// Job of a RestoreRun gets its download limit: the run, then the VolumeRestore
// of the source claim, then the controller's --restore-limit-download.
// Without it, a run could not lift a slow limit for an urgent restore, and a
// VolumeRestore could not set its own.
//
// Parameters:
//   - t is the test handle that the go test runner gives.
//
// It has no return value. It reports an error for each case where limitDownload
// returns another limit than the first one set.
//
// A limit of 0 counts as set, so a run with 0 gives a Job without
// --limit-download even when the VolumeRestore sets 20480. A run from
// spec.repository has no VolumeRestore, which the nil case covers.
func TestLimitDownloadUsesTheFirstLimitSet(t *testing.T) {
	run := func(limit *int32) *backupv1alpha1.RestoreRun {
		return &backupv1alpha1.RestoreRun{Spec: backupv1alpha1.RestoreRunSpec{LimitDownloadKiBps: limit}}
	}
	vr := func(limit *int32) *backupv1alpha1.VolumeRestore {
		return &backupv1alpha1.VolumeRestore{Spec: backupv1alpha1.VolumeRestoreSpec{LimitDownloadKiBps: limit}}
	}
	for _, tc := range []struct {
		name string
		run  *backupv1alpha1.RestoreRun
		vr   *backupv1alpha1.VolumeRestore
		want int32
	}{
		{"the run's limit before the VolumeRestore's", run(ptr.To[int32](1024)), vr(ptr.To[int32](20480)), 1024},
		{"the run's 0 lifts the VolumeRestore's limit", run(ptr.To[int32](0)), vr(ptr.To[int32](20480)), 0},
		{"the VolumeRestore's limit before the controller's", run(nil), vr(ptr.To[int32](20480)), 20480},
		{"the VolumeRestore's 0 lifts the controller's limit", run(nil), vr(ptr.To[int32](0)), 0},
		{"the controller's limit when neither sets one", run(nil), vr(nil), 512},
		{"the controller's limit without a VolumeRestore", run(nil), nil, 512},
	} {
		if got := limitDownload(tc.run, tc.vr, 512); got != tc.want {
			t.Errorf("%s: limit %d, want %d", tc.name, got, tc.want)
		}
	}
}
