//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/walzen-group/backup-controller/internal/restorejob"
	batchv1 "k8s.io/api/batch/v1"
)

// limitedManifests returns the objects of volumeManifests with
// limitDownloadKiBps: 64 on the VolumeRestore data, so every restore Job of
// the claim data reads from the repository at most at 64 KiB/s.
func limitedManifests() string {
	return strings.Replace(volumeManifests(), "  cacheCapacity: 100Mi\n", "  cacheCapacity: 100Mi\n  limitDownloadKiBps: 64\n", 1)
}

// writeBlob writes 4 MiB of random data into the file /data/blob of the
// Deployment app, and flushes it to the volume.
//
// It returns the SHA-256 sum of the file, as sha256sum prints it. Random
// data does not compress, so restic stores and reads about 4 MiB for it.
func (a *app) writeBlob() string {
	a.t.Helper()
	out := kubectl(a.t, "", "-n", a.ns, "exec", "deploy/app", "--", "sh", "-c",
		"head -c 4194304 /dev/urandom > /data/blob && sync && sha256sum /data/blob")
	sum, _, _ := strings.Cut(strings.TrimSpace(out), " ")
	return sum
}

// waitLimitedJob waits for the restore Job of one restore and checks that
// restic in it keeps to a download limit of 64 KiB/s.
//
// Parameters:
//   - namespace is where the Job runs: the app's namespace for a RestoreRun,
//     and backup-system for the populator.
//   - owner is the value of the label backup.wlz.li/restore on the Job: the
//     RestoreRun's name, or the app's namespace for the populator.
//
// It fails the test when the restic arguments of the Job do not end with
// --limit-download 64. It also fails the test when the Job finishes, or is
// gone, less than 30 seconds after its start. At 64 KiB/s, restic needs at
// least 63 seconds for the 4 MiB of writeBlob, so a Job that finishes sooner
// read faster than the limit. A RestoreRun and the populator delete a Job
// only after it finished.
func (a *app) waitLimitedJob(namespace, owner string) {
	a.t.Helper()
	var started time.Time
	waitFor(a.t, "the restore Job of "+owner+" to run for 30 seconds", 10*time.Minute, func() (bool, string) {
		out, err := run(a.t.Context(), "", "-n", namespace, "get", "jobs", "-l", restorejob.Label+"="+owner, "-o", "json")
		if err != nil {
			return false, firstLine(err.Error())
		}
		var jobs batchv1.JobList
		if err := json.Unmarshal([]byte(out), &jobs); err != nil {
			return false, err.Error()
		}
		if len(jobs.Items) == 0 {
			if !started.IsZero() {
				a.t.Fatalf("the restore Job of %s is gone %s after its start; at 64 KiB/s it runs for at least 63 seconds\n%s", owner, time.Since(started).Round(time.Second), a.describe())
			}
			return false, "no restore Job yet"
		}
		job := jobs.Items[0]
		args := job.Spec.Template.Spec.Containers[0].Args
		if !slices.Equal(args[len(args)-2:], []string{"--limit-download", "64"}) {
			a.t.Fatalf("restore Job %s runs restic with %q; want the arguments to end with --limit-download 64", job.Name, args)
		}
		if job.Status.StartTime == nil {
			return false, "the restore Job has not started"
		}
		started = job.Status.StartTime.Time
		if restorejob.Result(&job) != restorejob.Running {
			a.t.Fatalf("restore Job %s finished %s after its start; at 64 KiB/s it runs for at least 63 seconds\n%s", job.Name, time.Since(started).Round(time.Second), a.describe())
		}
		return time.Since(started) >= 30*time.Second, fmt.Sprintf("restore Job %s runs", job.Name)
	}, a.describe)
}

// TestARestoreReadsNoFasterThanItsDownloadLimit backs up 4 MiB of random
// data from a claim whose VolumeRestore sets limitDownloadKiBps: 64. It then
// restores the claim twice: into a new claim with a RestoreRun without a
// limit, and in place through the populator after a rebuild of the
// namespace. Both restore Jobs give restic --limit-download 64 and run
// longer than 30 seconds, and both claims contain the file of the backup.
func TestARestoreReadsNoFasterThanItsDownloadLimit(t *testing.T) {
	t.Parallel()
	a := newApp(t, "limit")
	a.publish(limitedManifests())

	a.write("limited")
	sum := a.writeBlob()
	backedUp := a.backup("base", "source: data")
	mustSucceed(t, "BackupRun", "base", backedUp.Status.Phase, backedUp.Status.Conditions, a.describe)

	// The RestoreRun has no limit, so its Job gets the limit of the
	// VolumeRestore of the source claim data.
	spec := "claim: data\ninto: copy"
	apply(t, fmt.Sprintf("apiVersion: backup.wlz.li/v1alpha1\nkind: RestoreRun\nmetadata:\n  name: copy\n  namespace: %s\nspec:\n%s", a.ns, indent(spec, 2)))
	a.waitLimitedJob(a.ns, "copy")
	// restore applies the same RestoreRun again, which leaves it unchanged, and
	// waits until it ends.
	restored := a.restore("copy", spec)
	mustSucceed(t, "RestoreRun", "copy", restored.Status.Phase, restored.Status.Conditions, a.describe)

	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: read-copy
  namespace: %s
spec:
  restartPolicy: Never
  containers:
    - name: read
      image: %s
      command: [sha256sum, /copy/blob]
      volumeMounts: [{name: copy, mountPath: /copy}]
  volumes:
    - name: copy
      persistentVolumeClaim: {claimName: copy}
`, a.ns, appImage))
	waitFor(t, "the pod read-copy to read the restored file", 5*time.Minute, func() (bool, string) {
		var pod struct {
			Status struct{ Phase string } `json:"status"`
		}
		if err := getJSON(a.ns, "pod", "read-copy", &pod); err != nil {
			return false, err.Error()
		}
		return pod.Status.Phase == "Succeeded", pod.Status.Phase
	}, a.describe)
	if got, _, _ := strings.Cut(strings.TrimSpace(a.logs(a.ns, "pod/read-copy")), " "); got != sum {
		t.Errorf("claim copy has a blob with SHA-256 %s, want %s", got, sum)
	}

	// The populator has no RestoreRun, so its Job gets the limit of the
	// VolumeRestore data.
	a.deleteClaim()
	a.reconcile()
	a.waitLimitedJob("backup-system", a.ns)
	waitFor(t, "the app to read the blob from the filled claim", 10*time.Minute, func() (bool, string) {
		out, err := run(t.Context(), "", "-n", a.ns, "exec", "deploy/app", "--", "sha256sum", "/data/blob")
		if err != nil {
			return false, firstLine(err.Error())
		}
		got, _, _ := strings.Cut(strings.TrimSpace(out), " ")
		return got == sum, got
	}, a.describe)
}
