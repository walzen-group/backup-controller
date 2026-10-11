package cli

import (
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	backupv1alpha1 "github.com/walzen-group/backup-controller/internal/api/v1alpha1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func parseBackup(t *testing.T, args ...string) *backupv1alpha1.BackupRun {
	t.Helper()
	obj, _, err := parse(args, "run", io.Discard, func(fs *pflag.FlagSet) builder { return backupFlags(fs) })
	if err != nil {
		t.Fatalf("parse %q: %v", args, err)
	}
	return obj.(*backupv1alpha1.BackupRun)
}

func parseRestore(t *testing.T, args ...string) *backupv1alpha1.RestoreRun {
	t.Helper()
	obj, _, err := parse(args, "restore", io.Discard, func(fs *pflag.FlagSet) builder { return restoreFlags(fs) })
	if err != nil {
		t.Fatalf("parse %q: %v", args, err)
	}
	return obj.(*backupv1alpha1.RestoreRun)
}

// TestBackupRunFromFlags checks that the name, the namespace and the target
// land in the BackupRun with flags on either side of the name.
func TestBackupRunFromFlags(t *testing.T) {
	for _, args := range [][]string{
		{"before-the-move", "-n", "tracearr", "--all"},
		{"-n", "tracearr", "--all", "before-the-move"},
		{"--namespace=tracearr", "before-the-move", "--all"},
	} {
		run := parseBackup(t, args...)
		if run.Name != "before-the-move" || run.Namespace != "tracearr" || !run.Spec.All {
			t.Errorf("%q built name %q, namespace %q, all %v; want before-the-move, tracearr, true", args, run.Name, run.Namespace, run.Spec.All)
		}
		if run.Kind != "BackupRun" || run.APIVersion != "backup.wlz.li/v1alpha1" {
			t.Errorf("%q built %s %s; want backup.wlz.li/v1alpha1 BackupRun", args, run.APIVersion, run.Kind)
		}
	}
}

// TestBackupRunOptionalFields checks that --timeout and --ttl set their fields
// and that omitting them leaves the fields nil, so the run's defaults apply.
func TestBackupRunOptionalFields(t *testing.T) {
	run := parseBackup(t, "x", "--source", "immich-library", "--timeout", "2h", "--ttl", "0")
	if run.Spec.Source != "immich-library" {
		t.Errorf("source %q, want immich-library", run.Spec.Source)
	}
	if run.Spec.Timeout == nil || run.Spec.Timeout.Duration != 2*time.Hour {
		t.Errorf("timeout %v, want 2h", run.Spec.Timeout)
	}
	if run.Spec.TTLSecondsAfterFinished == nil || *run.Spec.TTLSecondsAfterFinished != 0 {
		t.Errorf("ttl %v, want a pointer to 0", run.Spec.TTLSecondsAfterFinished)
	}

	run = parseBackup(t, "x", "--database", "immich-pg")
	if run.Spec.Database != "immich-pg" || run.Spec.Timeout != nil || run.Spec.TTLSecondsAfterFinished != nil {
		t.Errorf("built database %q, timeout %v, ttl %v; want immich-pg, nil, nil", run.Spec.Database, run.Spec.Timeout, run.Spec.TTLSecondsAfterFinished)
	}
}

// TestRestoreRunFromFlags checks every RestoreRun flag against the field it
// sets.
func TestRestoreRunFromFlags(t *testing.T) {
	run := parseRestore(t, "look-back", "-n", "immich",
		"--claim", "immich-library", "--into", "immich-library-old", "--into-size", "10Gi",
		"--as-of", "2026-10-11T04:00:00Z", "--previous", "1",
		"--timeout", "30m", "--ttl", "86400")

	s := run.Spec
	if run.Name != "look-back" || run.Namespace != "immich" {
		t.Errorf("name %q, namespace %q; want look-back, immich", run.Name, run.Namespace)
	}
	if s.Claim != "immich-library" || s.Into != "immich-library-old" {
		t.Errorf("claim %q, into %q; want immich-library, immich-library-old", s.Claim, s.Into)
	}
	if s.IntoSize == nil || s.IntoSize.Cmp(resource.MustParse("10Gi")) != 0 {
		t.Errorf("intoSize %v, want 10Gi", s.IntoSize)
	}
	if s.RestoreAsOf == nil || *s.RestoreAsOf != "2026-10-11T04:00:00Z" {
		t.Errorf("restoreAsOf %v, want 2026-10-11T04:00:00Z", s.RestoreAsOf)
	}
	if s.Previous == nil || *s.Previous != 1 {
		t.Errorf("previous %v, want 1", s.Previous)
	}
	if s.Timeout == nil || s.Timeout.Duration != 30*time.Minute {
		t.Errorf("timeout %v, want 30m", s.Timeout)
	}
	if s.TTLSecondsAfterFinished == nil || *s.TTLSecondsAfterFinished != 86400 {
		t.Errorf("ttl %v, want 86400", s.TTLSecondsAfterFinished)
	}

	run = parseRestore(t, "everything", "--all", "--sync-database-to-volume", "--pause", "deploy/immich-server", "--pause", "sts/immich-ml")
	want := []backupv1alpha1.WorkloadRef{{Kind: "Deployment", Name: "immich-server"}, {Kind: "StatefulSet", Name: "immich-ml"}}
	if !run.Spec.All || !run.Spec.SyncDatabaseToVolume {
		t.Errorf("all %v, syncDatabaseToVolume %v; want true, true", run.Spec.All, run.Spec.SyncDatabaseToVolume)
	}
	if len(run.Spec.PauseDuringRestore) != 2 || run.Spec.PauseDuringRestore[0] != want[0] || run.Spec.PauseDuringRestore[1] != want[1] {
		t.Errorf("pauseDuringRestore %v, want %v", run.Spec.PauseDuringRestore, want)
	}
	if run.Spec.Previous != nil || run.Spec.RestoreAsOf != nil || run.Spec.IntoSize != nil {
		t.Errorf("previous %v, restoreAsOf %v, intoSize %v; want all nil when the flags are omitted", run.Spec.Previous, run.Spec.RestoreAsOf, run.Spec.IntoSize)
	}
}

// TestRejectedCommandLines checks the command lines the plugin refuses before
// it contacts a cluster, and the message each one prints.
func TestRejectedCommandLines(t *testing.T) {
	cases := []struct {
		command string
		args    []string
		want    string
	}{
		{"run", []string{"--all"}, "needs exactly one NAME, got 0"},
		{"run", []string{"a", "b", "--all"}, "needs exactly one NAME, got 2"},
		{"run", []string{"a", "--every"}, "unknown flag: --every"},
		{"restore", []string{"a", "--all", "--pause", "tracearr"}, `--pause "tracearr" is not KIND/NAME`},
		{"restore", []string{"a", "--all", "--pause", "job/tracearr"}, `names kind "job"; use deployment/NAME or statefulset/NAME`},
		{"restore", []string{"a", "--claim", "c", "--into", "d", "--into-size", "lots"}, `--into-size "lots" is not a quantity`},
	}
	for _, tc := range cases {
		flags := func(fs *pflag.FlagSet) builder { return backupFlags(fs) }
		if tc.command == "restore" {
			flags = func(fs *pflag.FlagSet) builder { return restoreFlags(fs) }
		}
		var stderr strings.Builder
		_, _, err := parse(tc.args, tc.command, &stderr, flags)
		if err == nil {
			t.Errorf("%s %q: parsed; want an error containing %q", tc.command, tc.args, tc.want)
			continue
		}
		got := stderr.String()
		if !errors.Is(err, errUsage) {
			got = err.Error()
		}
		if !strings.Contains(got, tc.want) {
			t.Errorf("%s %q: printed %q; want it to contain %q", tc.command, tc.args, got, tc.want)
		}
	}
}
