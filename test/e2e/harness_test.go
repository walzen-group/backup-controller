//go:build e2e

// Package e2e runs the scenarios of the backup controller on the kind cluster
// of Docker Desktop (kubectl context docker-desktop). hack/kind/up.sh installs
// the stack and hack/kind/controller.sh deploys the controller from this
// tree. The tests fake nothing: they drive the controller as a user does and
// check the data itself, the files in a claim and the rows in a database.
package e2e

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const (
	kubeContext = "docker-desktop"

	// s3Endpoint is RustFS inside the cluster, and accessKey and secretKey
	// are its credentials (hack/kind/manifests/rustfs.yaml).
	s3Endpoint = "http://rustfs.s3.svc:9000"
	accessKey  = "test-access-key"
	secretKey  = "test-secret-key-0123456789"

	// postgresImage is the PostgreSQL image of every test Cluster
	// (hack/kind/versions.env).
	postgresImage = "ghcr.io/cloudnative-pg/postgresql:18.6"

	// appImage runs the test apps and the check pods.
	appImage = "docker.io/library/busybox:1.37.0"

	// storageClass provisions every test claim (hack/kind/manifests/cluster.yaml).
	storageClass = "csi-hostpath"
)

// run runs kubectl against the test cluster.
//
// Parameters:
//   - ctx ends the command when it is cancelled or its deadline passes.
//   - stdin is written to the command's standard input, for example a
//     manifest for kubectl apply -f -. An empty string gives no input.
//   - args are the kubectl arguments. The context flag for the test cluster
//     is added in front of them.
//
// It returns the command's standard output. When kubectl exits with an error,
// it also returns an error that names the arguments and carries kubectl's
// standard error, so a failure message shows why the command failed.
func run(ctx context.Context, stdin string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", append([]string{"--context", kubeContext}, args...)...)
	cmd.Stdin = strings.NewReader(stdin)
	var out, errOut bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// kubectl runs a kubectl command that the test cannot go on without.
//
// Parameters:
//   - t is the test that fails when the command fails.
//   - stdin and args are passed to run.
//
// It returns the command's standard output. The command gets six minutes,
// one more than the longest --timeout a scenario passes to a waiting
// command; when it fails or runs out of time, the test fails with kubectl's
// error.
func kubectl(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Minute)
	defer cancel()
	out, err := run(ctx, stdin, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// apply creates or updates the objects of a YAML manifest.
//
// Parameters:
//   - t is the test that fails when kubectl refuses the manifest.
//   - manifest holds one or more YAML documents.
func apply(t *testing.T, manifest string) {
	t.Helper()
	kubectl(t, manifest, "apply", "-f", "-")
}

// getJSON reads one object from the test cluster.
//
// Parameters:
//   - namespace, kind and name identify the object, as kubectl get takes
//     them.
//   - into receives the decoded JSON of the object.
//
// It returns the error of kubectl, for example when the object does not
// exist, or the error of the JSON decoding. The read gets 30 seconds.
func getJSON(namespace, kind, name string, into any) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := run(ctx, "", "-n", namespace, "get", kind, name, "-o", "json")
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(out), into)
}

// waitFor waits until a condition on the test cluster holds.
//
// Parameters:
//   - t is the test that fails when the condition does not hold in time.
//   - what names the condition in the log and in the failure message.
//   - timeout is how long waitFor waits.
//   - check reports whether the condition holds, and a short description of
//     the current state, for example the phase of a run.
//   - describe returns the state of the cluster for the failure message.
//
// waitFor calls check every two seconds. It logs each new state that check
// reports, so the test log shows how the wait went. When the timeout passes
// first, the test fails with the last state and what describe returns.
func waitFor(t *testing.T, what string, timeout time.Duration, check func() (bool, string), describe func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	state := ""
	for {
		done, now := check()
		if done {
			return
		}
		if now != state {
			t.Logf("waiting for %s: %s", what, now)
			state = now
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s did not happen within %s; last state: %s\n%s", what, timeout, state, describe())
		}
		time.Sleep(2 * time.Second)
	}
}

// suffix returns six random hexadecimal characters. The tests add them to
// namespace names, so the objects of one test run never meet those of
// another.
func suffix() string {
	b := make([]byte, 3)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// indent moves a block of text to the right.
//
// Parameters:
//   - text is the block, for example a manifest to embed in another one.
//   - spaces is the number of spaces put in front of each line.
//
// It returns the indented block without a trailing newline.
func indent(text string, spaces int) string {
	pad := strings.Repeat(" ", spaces)
	return pad + strings.ReplaceAll(strings.TrimRight(text, "\n"), "\n", "\n"+pad)
}

// jsonInto decodes the JSON in text into into, and returns the decoding
// error.
func jsonInto(text string, into any) error {
	return json.Unmarshal([]byte(text), into)
}
