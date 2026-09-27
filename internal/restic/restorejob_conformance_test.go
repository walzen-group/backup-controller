//go:build conformance

package restic

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/restorejob"
	"github.com/walzen-group/backup-controller/internal/testinfra/versions"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// The tests in this file run real restic, of the version inside the VolSync
// mover image (versions.json, restic-mover), with the argument lists
// restorejob.Build gives the restore Job's two containers, against a copy of
// the recorded timed fixture. They check that the restore puts back exactly
// the snapshot it names by full ID, that --delete removes what the snapshot
// does not hold, and that each exit code the Job can end with means what
// restorejob.ExitMeaning says. make conformance runs them with restic from
// the flake on PATH.
//
// Only two argument values change: --target names a temporary directory in
// place of the container's /data, and the locked case shortens --retry-lock
// so the test ends in seconds. Every other argument is Build's.

// resticRunTimeout bounds one restic run, so a hung restic fails the test.
const resticRunTimeout = 2 * time.Minute

// resticBinary returns the path of restic-<version> for the version the
// mover image runs, and fails the test when PATH has none or it reports
// another version.
func resticBinary(t *testing.T) string {
	t.Helper()
	version := versions.Of(t, "restic-mover")
	bin, err := exec.LookPath("restic-" + version)
	if err != nil {
		t.Fatalf("restic-%s is not on PATH; run the test through make conformance: %v", version, err)
	}
	out, err := exec.Command(bin, "version").Output()
	if err != nil || !strings.HasPrefix(string(out), "restic "+version+" ") {
		t.Fatalf("%s version = %q, %v; want restic %s", bin, out, err, version)
	}
	return bin
}

// jobArgs returns the arguments Build gives the unlock init container and
// the restore container of a Job that restores one snapshot with --delete.
//
// Parameters:
//   - id is the full ID of the snapshot the Job restores.
//
// It fails the test when Build refuses the spec or a container does not run
// restic itself as its command.
func jobArgs(t *testing.T, id string) (unlock, restore []string) {
	t.Helper()
	job, err := restorejob.Build(restorejob.Spec{
		Name:       "restore-claim-123",
		Namespace:  "backup-system",
		Origin:     restorejob.Origin{Kind: restorejob.OriginClaim, UID: "claim-123"},
		Owner:      metav1.OwnerReference{APIVersion: "v1", Kind: "PersistentVolumeClaim", Name: "prime-claim-123", UID: "prime-123"},
		SnapshotID: id,
		Delete:     true,
		Claim:      "prime-claim-123",
		Repository: "restic-config",
		Image:      "restic:pinned",
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	pod := job.Spec.Template.Spec
	if len(pod.InitContainers) != 1 || len(pod.Containers) != 1 {
		t.Fatalf("the Job's pod has %d init containers and %d containers, want 1 and 1", len(pod.InitContainers), len(pod.Containers))
	}
	for _, c := range slices.Concat(pod.InitContainers, pod.Containers) {
		if !slices.Equal(c.Command, []string{"restic"}) {
			t.Fatalf("container %s runs %q, want restic itself", c.Name, c.Command)
		}
	}
	return pod.InitContainers[0].Args, pod.Containers[0].Args
}

// withValue returns a copy of restic's arguments with the value that follows
// a flag replaced, and fails the test when the flag is not there.
//
// Parameters:
//   - args are the Job's arguments.
//   - flag is the flag whose value changes, such as --target.
//   - value is its new value.
func withValue(t *testing.T, args []string, flag, value string) []string {
	t.Helper()
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		t.Fatalf("the Job's arguments %q have no %s with a value", args, flag)
	}
	out := slices.Clone(args)
	out[i+1] = value
	return out
}

// withoutFlag returns a copy of restic's arguments without a flag and its
// value, and fails the test when the flag is not there.
func withoutFlag(t *testing.T, args []string, flag string) []string {
	t.Helper()
	i := slices.Index(args, flag)
	if i < 0 || i+1 >= len(args) {
		t.Fatalf("the Job's arguments %q have no %s with a value", args, flag)
	}
	return slices.Delete(slices.Clone(args), i, i+2)
}

// resticEnv is the environment one restic run gets, as the Job's containers
// get it from the repository Secret and from RESTIC_CACHE_DIR.
type resticEnv struct {
	// repository is RESTIC_REPOSITORY, a local directory.
	repository string
	// password is RESTIC_PASSWORD.
	password string
	// cache is RESTIC_CACHE_DIR.
	cache string
}

// run runs restic with the given arguments and returns its exit code and its
// combined output. It fails the test when restic can't be started or runs
// longer than resticRunTimeout.
func (e resticEnv) run(t *testing.T, bin string, args ...string) (int, string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), resticRunTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Env = []string{
		"RESTIC_REPOSITORY=" + e.repository,
		"RESTIC_PASSWORD=" + e.password,
		"RESTIC_CACHE_DIR=" + e.cache,
		"HOME=" + e.cache,
		"TZ=UTC",
	}
	out, err := cmd.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("restic %q ran longer than %s:\n%s", args, resticRunTimeout, out)
	}
	var exit *exec.ExitError
	switch {
	case errors.As(err, &exit):
		return exit.ExitCode(), string(out)
	case err != nil:
		t.Fatalf("start restic %q: %v", args, err)
	}
	return 0, string(out)
}

// timedRepository copies the recorded timed fixture of the mover's restic
// into a temporary directory and returns its store, the Repository opened on
// it, the environment that points restic at it, and the recorded snapshots,
// oldest first.
func timedRepository(t *testing.T) (DirStore, *Repository, resticEnv, []recordedSnapshot) {
	t.Helper()
	name := "restic-" + versions.Of(t, "restic-mover")
	var fixture recordedFixture
	for _, f := range fixtures(t, "timed") {
		if strings.HasPrefix(f.name, name+"/") {
			fixture = f
		}
	}
	if fixture.dir == "" {
		t.Fatalf("no recorded timed fixture of %s", name)
	}
	store, repo := fixture.writable(t)
	snapshots := fixture.snapshots(t)
	if len(snapshots) < 2 {
		t.Fatalf("%s holds %d snapshots, want at least 2", fixture.name, len(snapshots))
	}
	return store, repo, resticEnv{repository: string(store), password: "backup", cache: t.TempDir()}, snapshots
}

// TestTheRestoreJobRestoresTheSnapshotItNames checks that the Job's unlock
// and restore arguments, run by real restic, put back exactly the snapshot
// the Job names by full ID. The target first holds a file no snapshot has,
// then the older snapshot's data: after each restore the target holds what
// restic dump reads from that snapshot and nothing else, so --delete removed
// the stray file, and restoring the newer snapshot over the older one
// replaced its data.
func TestTheRestoreJobRestoresTheSnapshotItNames(t *testing.T) {
	bin := resticBinary(t)
	_, _, env, snapshots := timedRepository(t)
	target := t.TempDir()
	if err := os.WriteFile(filepath.Join(target, "stray"), []byte("not in any snapshot\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	for _, s := range []recordedSnapshot{snapshots[0], snapshots[len(snapshots)-1]} {
		unlock, restore := jobArgs(t, s.ID)
		if code, out := env.run(t, bin, unlock...); code != 0 {
			t.Fatalf("unlock exited %d:\n%s", code, out)
		}
		code, out := env.run(t, bin, withValue(t, restore, "--target", target)...)
		if code != 0 || restorejob.ExitMeaning(int32(code)) != "success" {
			t.Fatalf("restore %s exited %d (%s):\n%s", s.ShortID, code, restorejob.ExitMeaning(int32(code)), out)
		}
		entries, err := os.ReadDir(target)
		if err != nil {
			t.Fatal(err)
		}
		var names []string
		for _, e := range entries {
			names = append(names, e.Name())
		}
		if !slices.Equal(names, []string{"counter"}) {
			t.Fatalf("after restoring %s the target holds %q, want only the snapshot's counter", s.ShortID, names)
		}
		code, want := env.run(t, bin, "dump", s.ID, "/counter")
		if code != 0 {
			t.Fatalf("restic dump %s exited %d:\n%s", s.ShortID, code, want)
		}
		got, err := os.ReadFile(filepath.Join(target, "counter"))
		if err != nil || string(got) != want {
			t.Fatalf("after restoring %s the counter holds %q (%v), want %q from restic dump", s.ShortID, got, err, want)
		}
	}
}

// TestTheRestoreJobsExitCodesMeanWhatExitMeaningSays checks, with real
// restic, each exit code the restore container ends with in a case the Job
// can meet, and that restorejob.ExitMeaning names that case: a snapshot ID
// the repository does not hold (1), no repository at the address (10), a
// wrong password (12), and a live exclusive lock (11), both with a short
// --retry-lock and with none. The lock is a fresh one of a mover pod on another
// host, which the unlock init container leaves in place.
func TestTheRestoreJobsExitCodesMeanWhatExitMeaningSays(t *testing.T) {
	bin := resticBinary(t)
	missing := strings.Repeat("0", 64)

	for name, test := range map[string]struct {
		id      string
		env     func(resticEnv) resticEnv
		args    func(*testing.T, []string) []string
		lock    bool
		code    int
		meaning string
	}{
		"missing snapshot": {id: missing, code: 1, meaning: "failure"},
		"no repository": {
			env:  func(e resticEnv) resticEnv { e.repository = filepath.Join(e.cache, "none"); return e },
			code: 10, meaning: "no repository",
		},
		"wrong password": {
			env:  func(e resticEnv) resticEnv { e.password = "wrong"; return e },
			code: 12, meaning: "wrong password",
		},
		"locked with a short retry": {
			lock: true,
			args: func(t *testing.T, a []string) []string { return withValue(t, a, "--retry-lock", "1s") },
			code: 11, meaning: "the repository is locked",
		},
		"locked with no retry": {
			lock: true,
			args: func(t *testing.T, a []string) []string { return withoutFlag(t, a, "--retry-lock") },
			code: 11, meaning: "the repository is locked",
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, repo, env, snapshots := timedRepository(t)
			id := test.id
			if id == "" {
				id = snapshots[len(snapshots)-1].ID
			}
			unlock, restore := jobArgs(t, id)
			restore = withValue(t, restore, "--target", t.TempDir())
			if test.args != nil {
				restore = test.args(t, restore)
			}
			if test.env != nil {
				env = test.env(env)
			}
			if test.lock {
				writeMoverLock(t, repo, 0)
				if code, out := env.run(t, bin, unlock...); code != 0 || len(lockFiles(t, store)) != 1 {
					t.Fatalf("unlock exited %d and left %d locks, want 0 and the live lock:\n%s", code, len(lockFiles(t, store)), out)
				}
			}

			code, out := env.run(t, bin, restore...)
			if code != test.code {
				t.Fatalf("restore exited %d, want %d:\n%s", code, test.code, out)
			}
			if got := restorejob.ExitMeaning(int32(code)); got != test.meaning {
				t.Errorf("ExitMeaning(%d) = %q, want %q", code, got, test.meaning)
			}
		})
	}
}

// TestTheUnlockInitContainerRemovesAStaleLock checks that the Job's unlock
// arguments, run by real restic, remove an exclusive lock written 31 minutes
// ago by a mover pod that is gone, so the restore after it does not wait for
// a lock nobody holds.
func TestTheUnlockInitContainerRemovesAStaleLock(t *testing.T) {
	bin := resticBinary(t)
	store, repo, env, snapshots := timedRepository(t)
	writeMoverLock(t, repo, 31*time.Minute)

	unlock, _ := jobArgs(t, snapshots[0].ID)
	if code, out := env.run(t, bin, unlock...); code != 0 {
		t.Fatalf("unlock exited %d:\n%s", code, out)
	}
	if left := lockFiles(t, store); len(left) != 0 {
		t.Errorf("unlock left the locks %q, want the stale one removed", left)
	}
}

// writeMoverLock writes an exclusive lock into a repository, as a VolSync
// mover pod holds one while it prunes.
//
// Parameters:
//   - repo is the repository to lock.
//   - age is how long ago the lock was taken. A lock younger than 30 minutes
//     is live for restic; an older one is stale.
//
// The lock names the mover pod as its host and PID 1. restic looks at a
// lock's process only when the lock names restic's own host; with restic
// 0.18.1, a lock that named this test's host and PID ended the test binary
// with SIGHUP. A lock of another host keeps restic away from this process,
// and it is the case a restore Job meets.
func writeMoverLock(t *testing.T, repo *Repository, age time.Duration) {
	t.Helper()
	document, err := json.Marshal(lockJSON{
		Time: time.Now().Add(-age), Exclusive: true, Hostname: "volsync-src-notes-data-7xk2p", PID: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.save(context.Background(), "locks", document); err != nil {
		t.Fatalf("write the lock: %v", err)
	}
}
