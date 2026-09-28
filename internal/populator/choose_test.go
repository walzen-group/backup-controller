package populator

import (
	"errors"
	"testing"
	"time"

	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"github.com/walzen-group/backup-controller/internal/restic"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// TestChoose checks the snapshot choice: the newest snapshot at or before a
// pin, else the paused snapshot at the namespace's moment, else the newest
// snapshot, and no snapshot for an empty repository.
func TestChoose(t *testing.T) {
	at := func(h int) time.Time { return time.Date(2026, 9, 28, h, 0, 0, 0, time.UTC) }
	paused := restic.Snapshot{ID: "paused03", Time: at(3), Tags: []string{restic.PausedTag}}
	early := restic.Snapshot{ID: "live0001", Time: at(1)}
	late := restic.Snapshot{ID: "live0005", Time: at(5)}
	all := []restic.Snapshot{late, early, paused}
	pinAt := func(h int) *pin {
		return &pin{at: at(h), value: at(h).Format(time.RFC3339), source: "spec.restoreAsOf"}
	}
	moment := func(h int) *time.Time { m := at(h); return &m }
	cases := []struct {
		name      string
		snapshots []restic.Snapshot
		pinned    *pin
		moment    *time.Time
		want      string
		found     bool
		reach     bool
	}{
		{"a pin gives the newest snapshot at or before it", all, pinAt(4), moment(3), "paused03", true, false},
		{"a pin at the time of a snapshot gives that snapshot", all, pinAt(5), nil, "live0005", true, false},
		{"a pin before every snapshot is out of reach", all, pinAt(0), nil, "", false, true},
		{"a pin on an empty repository is out of reach", nil, pinAt(4), nil, "", false, true},
		{"without a pin the paused moment wins over a newer snapshot", all, nil, moment(3), "paused03", true, false},
		{"a moment this repository does not hold falls back to the newest", all, nil, moment(2), "live0005", true, false},
		{"without a pin or moment the newest snapshot wins", all, nil, nil, "live0005", true, false},
		{"an empty repository binds empty", nil, nil, moment(3), "", false, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, found, err := choose(c.snapshots, c.pinned, c.moment)
			var reach *outOfReachError
			if errors.As(err, &reach) != c.reach {
				t.Fatalf("choose error = %v, want out of reach %t", err, c.reach)
			}
			if !c.reach && err != nil {
				t.Fatalf("choose error = %v, want none", err)
			}
			if got.ID != c.want || found != c.found {
				t.Errorf("choose = %q, %t; want %q, %t", got.ID, found, c.want, c.found)
			}
		})
	}
}

// TestPinOf checks that the claim's annotation comes before the
// VolumeRestore's restoreAsOf, and that a value that is not an RFC 3339 time
// is out of reach.
func TestPinOf(t *testing.T) {
	spec := "2026-09-28T03:00:00Z"
	annotated := "2026-09-28T01:00:00Z"
	withSpec := &backupv1alpha1.VolumeRestore{Spec: backupv1alpha1.VolumeRestoreSpec{RestoreAsOf: &spec}}
	claim := func(value string) *corev1.PersistentVolumeClaim {
		if value == "" {
			return &corev1.PersistentVolumeClaim{}
		}
		return &corev1.PersistentVolumeClaim{ObjectMeta: metav1.ObjectMeta{
			Annotations: map[string]string{backupv1alpha1.AnnotationRestoreAsOf: value},
		}}
	}
	cases := []struct {
		name   string
		vr     *backupv1alpha1.VolumeRestore
		claim  *corev1.PersistentVolumeClaim
		want   string
		pinned bool
		reach  bool
	}{
		{"the annotation comes first", withSpec, claim(annotated), annotated, true, false},
		{"restoreAsOf pins a claim without the annotation", withSpec, claim(""), spec, true, false},
		{"no setting is no pin", &backupv1alpha1.VolumeRestore{}, claim(""), "", false, false},
		{"a value that is not a time is out of reach", withSpec, claim("yesterday"), "", false, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, pinned, err := pinOf(c.vr, c.claim)
			var reach *outOfReachError
			if errors.As(err, &reach) != c.reach {
				t.Fatalf("pinOf error = %v, want out of reach %t", err, c.reach)
			}
			if pinned != c.pinned || got.value != c.want {
				t.Errorf("pinOf = %q, %t; want %q, %t", got.value, pinned, c.want, c.pinned)
			}
		})
	}
}
