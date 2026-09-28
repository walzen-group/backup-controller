//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// resticImage is the VolSync mover image, which holds the restic that
// writes the repositories (restic 0.18.1 in VolSync 0.16.0). The tests read
// a repository with the same restic.
const resticImage = "quay.io/backube/volsync:0.16.0"

// snapshot is one restic snapshot as restic snapshots --json prints it.
type snapshot struct {
	ID    string    `json:"id"`
	Time  time.Time `json:"time"`
	Tags  []string  `json:"tags"`
	Paths []string  `json:"paths"`
}

// snapshots lists the snapshots of the app's restic repository.
//
// It runs restic snapshots --json in a pod in the app's namespace, with the
// Secret restic as its environment, so it reads the repository the way the
// movers do. It returns the snapshots oldest first. The test fails when the
// pod does not succeed within three minutes or its output is not restic's
// JSON.
func (a *app) snapshots() []snapshot {
	a.t.Helper()
	name := "restic-snapshots-" + suffix()
	apply(a.t, fmt.Sprintf(`apiVersion: v1
kind: Pod
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  restartPolicy: Never
  containers:
    - name: restic
      image: %[3]s
      command: [restic, snapshots, --json, --no-lock]
      envFrom:
        - secretRef: {name: restic}
`, a.ns, name, resticImage))
	defer func() {
		_, _ = run(a.t.Context(), "", "-n", a.ns, "delete", "pod", name, "--wait=false")
	}()
	waitFor(a.t, "restic to list the snapshots", 3*time.Minute, func() (bool, string) {
		var pod struct {
			Status struct{ Phase string } `json:"status"`
		}
		if err := getJSON(a.ns, "pod", name, &pod); err != nil {
			return false, err.Error()
		}
		if pod.Status.Phase == "Failed" {
			a.t.Fatalf("restic snapshots failed:\n%s", a.logs(a.ns, "pod/"+name))
		}
		return pod.Status.Phase == "Succeeded", pod.Status.Phase
	}, a.describe)
	var list []snapshot
	if err := json.Unmarshal([]byte(a.logs(a.ns, "pod/"+name)), &list); err != nil {
		a.t.Fatalf("decode the output of restic snapshots: %v", err)
	}
	return list
}

// hasTag reports whether a snapshot carries a tag.
func (s snapshot) hasTag(tag string) bool {
	for _, t := range s.Tags {
		if t == tag {
			return true
		}
	}
	return false
}

// newest returns the last snapshot of a list that restic printed oldest
// first, and fails the test when the list is empty.
func newest(t *testing.T, list []snapshot) snapshot {
	t.Helper()
	if len(list) == 0 {
		t.Fatal("the repository holds no snapshot")
	}
	return list[len(list)-1]
}
