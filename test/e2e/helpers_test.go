//go:build e2e

// Package e2e holds the end-to-end scenarios that run against the local
// docker-desktop cluster, with the real VolSync, Kueue, RustFS and the
// controller built from this tree installed by hack/e2e.
package e2e

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	// kubeContext is the only context any e2e helper talks to.
	kubeContext = "docker-desktop"

	// controllerNamespace is where hack/e2e/backup-controller installs the
	// controller.
	controllerNamespace = "backup-system"

	// rustfsNamespace, rustfsService and rustfsSecret name the RustFS install
	// of hack/e2e/rustfs.
	rustfsNamespace = "e2e-s3"
	rustfsService   = "rustfs"
	rustfsSecret    = "rustfs-credentials"

	// rustfsInCluster is the endpoint the movers reach RustFS at.
	rustfsInCluster = "http://rustfs.e2e-s3.svc:9000"

	// volsyncBucket is the bucket the volume repositories live in.
	volsyncBucket = "volsync"

	// resticMoverVersion is the restic release VolSync 0.16.0's mover ships.
	resticMoverVersion = "0.18.1"
)

// kubectl runs kubectl against the docker-desktop context.
//
// Parameters:
//   - stdin: written to kubectl's standard input when not empty
//   - args: kubectl's arguments, without --context
//
// Returns standard output, and an error carrying standard error when kubectl
// exits non-zero.
func kubectl(ctx context.Context, stdin string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "kubectl", append([]string{"--context", kubeContext}, args...)...)
	if stdin != "" {
		cmd.Stdin = strings.NewReader(stdin)
	}
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("kubectl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// mustKubectl runs kubectl like kubectl does and fails the test on an error.
func mustKubectl(t *testing.T, stdin string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	out, err := kubectl(ctx, stdin, args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

// apply applies YAML documents with kubectl apply and fails the test on an
// error.
func apply(t *testing.T, manifest string) {
	t.Helper()
	mustKubectl(t, manifest, "apply", "-f", "-")
}

// testNamespace is a namespace set up the way the infrastructure repository
// sets up an app namespace for backups.
type testNamespace struct {
	// Name is the namespace's name.
	Name string
	// Queue is the LocalQueue in it, pointing at the ClusterQueue e2e.
	Queue string
}

// newTestNamespace creates a namespace carrying what prod writes on one:
// kueue-managed=true, a backup.wlz.li/schedule far from any tick the test
// could meet, and a LocalQueue named backup on the ClusterQueue e2e. The
// namespace is deleted when the test ends, and the cleanup waits for it to go.
//
// Parameters:
//   - prefix: the start of the namespace's name; a timestamp follows it
func newTestNamespace(t *testing.T, prefix string) *testNamespace {
	t.Helper()
	ns := &testNamespace{Name: fmt.Sprintf("%s-%d", prefix, time.Now().Unix()%1000000), Queue: "backup"}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer cancel()
		if _, err := kubectl(ctx, "", "delete", "namespace", ns.Name, "--wait=true", "--ignore-not-found"); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	apply(t, fmt.Sprintf(`apiVersion: v1
kind: Namespace
metadata:
  name: %[1]s
  labels:
    kueue-managed: "true"
    e2e.backup.wlz.li/test: "true"
  annotations:
    backup.wlz.li/schedule: "0 3 29 2 *"
---
apiVersion: kueue.x-k8s.io/v1beta2
kind: LocalQueue
metadata:
  name: %[2]s
  namespace: %[1]s
spec:
  clusterQueue: e2e
`, ns.Name, ns.Queue))
	return ns
}

// rustfsCredentials returns the access and secret key of the RustFS install.
func rustfsCredentials(t *testing.T) (accessKey, secretKey string) {
	t.Helper()
	out := mustKubectl(t, "", "-n", rustfsNamespace, "get", "secret", rustfsSecret, "-o", "jsonpath={.data}")
	var data map[string]string
	if err := json.Unmarshal([]byte(out), &data); err != nil {
		t.Fatalf("read Secret %s/%s: %v", rustfsNamespace, rustfsSecret, err)
	}
	decode := func(key string) string {
		b, err := base64.StdEncoding.DecodeString(data[key])
		if err != nil || len(b) == 0 {
			t.Fatalf("Secret %s/%s has no usable %s", rustfsNamespace, rustfsSecret, key)
		}
		return string(b)
	}
	return decode("AWS_ACCESS_KEY_ID"), decode("AWS_SECRET_ACCESS_KEY")
}

// resticRepo is a restic repository under a prefix of the volsync bucket.
type resticRepo struct {
	// Prefix is the key prefix in the bucket, without slashes at either end.
	Prefix string
	// Password is the repository password.
	Password string
	// AccessKey and SecretKey are the RustFS keys.
	AccessKey, SecretKey string
}

// newResticRepo names a repository under prefix and deletes every object under
// that prefix when the test ends, through a port-forward to RustFS.
func newResticRepo(t *testing.T, prefix string) *resticRepo {
	t.Helper()
	access, secret := rustfsCredentials(t)
	repo := &resticRepo{Prefix: prefix, Password: "e2e-restic-password", AccessKey: access, SecretKey: secret}
	t.Cleanup(func() {
		endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
		if err := deletePrefix(endpoint, access, secret, volsyncBucket, prefix+"/"); err != nil {
			t.Errorf("cleanup: delete s3://%s/%s/: %v", volsyncBucket, prefix, err)
		}
	})
	return repo
}

// secretManifest returns the restic repository Secret the movers read, the
// shape modules/cluster/volsync/opentofu/repository writes.
func (r *resticRepo) secretManifest(namespace, name string) string {
	return fmt.Sprintf(`apiVersion: v1
kind: Secret
metadata:
  name: %s
  namespace: %s
stringData:
  RESTIC_REPOSITORY: s3:%s/%s/%s
  RESTIC_PASSWORD: %s
  AWS_ACCESS_KEY_ID: %s
  AWS_SECRET_ACCESS_KEY: %s
`, name, namespace, rustfsInCluster, volsyncBucket, r.Prefix, r.Password, r.AccessKey, r.SecretKey)
}

// resticSnapshot is the part of restic's snapshots --json output the tests
// read.
type resticSnapshot struct {
	ID       string    `json:"id"`
	ShortID  string    `json:"short_id"`
	Time     time.Time `json:"time"`
	Paths    []string  `json:"paths"`
	Tags     []string  `json:"tags"`
	Hostname string    `json:"hostname"`
}

// snapshots lists the repository's snapshots with the restic release VolSync's
// mover ships, against RustFS through a port-forward.
func (r *resticRepo) snapshots(t *testing.T) []resticSnapshot {
	t.Helper()
	endpoint := portForward(t, rustfsNamespace, "svc/"+rustfsService, 9000)
	out, err := r.restic(t, endpoint, "snapshots", "--json", "--no-lock")
	if err != nil {
		t.Fatalf("restic snapshots: %v", err)
	}
	var snaps []resticSnapshot
	if err := json.Unmarshal([]byte(out), &snaps); err != nil {
		t.Fatalf("read restic snapshots --json: %v\n%s", err, out)
	}
	return snaps
}

// restic runs the mover's restic release against the repository.
//
// Parameters:
//   - endpoint: host:port RustFS answers at on this machine
//   - args: restic's arguments
//
// Returns standard output, and an error carrying standard error.
func (r *resticRepo) restic(t *testing.T, endpoint string, args ...string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, resticBinary(t), args...)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("RESTIC_REPOSITORY=s3:http://%s/%s/%s", endpoint, volsyncBucket, r.Prefix),
		"RESTIC_PASSWORD="+r.Password,
		"AWS_ACCESS_KEY_ID="+r.AccessKey,
		"AWS_SECRET_ACCESS_KEY="+r.SecretKey,
		"RESTIC_CACHE_DIR="+t.TempDir(),
	)
	var out, errOut bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	if err := cmd.Run(); err != nil {
		return out.String(), fmt.Errorf("%s %s: %w: %s", cmd.Path, strings.Join(args, " "), err, strings.TrimSpace(errOut.String()))
	}
	return out.String(), nil
}

// resticBinary returns the path of restic-0.18.1, the release VolSync's mover
// ships. It looks on PATH first (the fixtures shell has it) and otherwise
// builds the flake's restic-mover package, which comes from the Nix store
// when it was built before.
func resticBinary(t *testing.T) string {
	t.Helper()
	name := "restic-" + resticMoverVersion
	if p, err := exec.LookPath(name); err == nil {
		return p
	}
	out, err := exec.Command("nix", "build", "--no-link", "--print-out-paths", "../..#restic-mover").Output()
	if err != nil {
		t.Fatalf("%s is not on PATH and nix build .#restic-mover failed: %v", name, err)
	}
	return strings.TrimSpace(string(out)) + "/bin/" + name
}

// forwardLine is what kubectl port-forward prints once it listens.
var forwardLine = regexp.MustCompile(`Forwarding from 127\.0\.0\.1:(\d+) ->`)

// portForward starts kubectl port-forward to a port of a pod or service on a
// free local port, and stops it when the test ends.
//
// Parameters:
//   - namespace, target: where to forward to, such as svc/rustfs
//   - port: the remote port
//
// Returns host:port on 127.0.0.1.
func portForward(t *testing.T, namespace, target string, port int) string {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, "kubectl", "--context", kubeContext, "-n", namespace,
		"port-forward", "--address", "127.0.0.1", target, fmt.Sprintf(":%d", port))
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	var errOut bytes.Buffer
	cmd.Stderr = &errOut
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start port-forward to %s/%s: %v", namespace, target, err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
	})
	found := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			if m := forwardLine.FindStringSubmatch(sc.Text()); m != nil {
				found <- "127.0.0.1:" + m[1]
				break
			}
		}
		for sc.Scan() {
		}
	}()
	select {
	case addr := <-found:
		return addr
	case <-time.After(30 * time.Second):
		t.Fatalf("port-forward to %s/%s did not start in 30s: %s", namespace, target, errOut.String())
	}
	return ""
}

// deletePrefix deletes every object under prefix in bucket.
func deletePrefix(endpoint, accessKey, secretKey, bucket, prefix string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	client, err := minio.New(endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(accessKey, secretKey, ""),
		BucketLookup: minio.BucketLookupPath,
		Region:       "us-east-1",
	})
	if err != nil {
		return err
	}
	objects := make(chan minio.ObjectInfo)
	go func() {
		defer close(objects)
		for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
			if obj.Err != nil {
				return
			}
			objects <- obj
		}
	}()
	for res := range client.RemoveObjects(ctx, bucket, objects, minio.RemoveObjectsOptions{}) {
		if res.Err != nil {
			return fmt.Errorf("remove %s: %w", res.ObjectName, res.Err)
		}
	}
	left := 0
	for obj := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true}) {
		if obj.Err != nil {
			return obj.Err
		}
		left++
	}
	if left > 0 {
		return fmt.Errorf("%d objects are left under %s", left, prefix)
	}
	return nil
}

// waitFor polls cond until it reports done, the timeout passes, or cond
// returns an error. On a timeout or an error it fails the test with the last
// state cond described and the evidence evidence collects.
//
// Parameters:
//   - what: what is awaited, for the failure message
//   - cond: returns whether the wait is over and a one-line state
//   - evidence: collects what explains a failure; may be nil
func waitFor(t *testing.T, what string, timeout, interval time.Duration, cond func() (bool, string, error), evidence func() string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	last := ""
	for {
		done, state, err := cond()
		if state != last {
			t.Logf("%s: %s", what, state)
			last = state
		}
		if err == nil && done {
			return
		}
		if err != nil || time.Now().After(deadline) {
			msg := fmt.Sprintf("timed out after %s", timeout)
			if err != nil {
				msg = err.Error()
			}
			ev := ""
			if evidence != nil {
				ev = evidence()
			}
			t.Fatalf("waiting for %s: %s; last state: %s\n%s", what, msg, last, ev)
		}
		time.Sleep(interval)
	}
}

// collect runs kubectl commands and joins their output under headings, for a
// failure message. A command that fails contributes its error.
func collect(cmds ...[]string) string {
	var b strings.Builder
	for _, args := range cmds {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		out, err := kubectl(ctx, "", args...)
		cancel()
		fmt.Fprintf(&b, "\n===== kubectl %s\n%s", strings.Join(args, " "), out)
		if err != nil {
			fmt.Fprintf(&b, "(error: %v)\n", err)
		}
	}
	return b.String()
}

// runEvidence gathers what explains a run's outcome in namespace: the runs,
// VolSync's sources and destinations, the claims and pods, the namespace's
// events, the mover logs and the controller's recent log.
func runEvidence(namespace string) string {
	return collect(
		[]string{"-n", namespace, "get", "backupruns,restoreruns", "-o", "yaml"},
		[]string{"-n", namespace, "get", "replicationsources,replicationdestinations", "-o", "yaml"},
		[]string{"-n", namespace, "get", "pvc,pods,jobs", "-o", "wide"},
		[]string{"-n", namespace, "get", "events", "--sort-by=.lastTimestamp"},
		[]string{"-n", namespace, "logs", "-l", "app.kubernetes.io/created-by=volsync", "--all-containers", "--tail=200", "--prefix"},
		[]string{"-n", controllerNamespace, "logs", "deploy/backup-controller", "--tail=300"},
		[]string{"get", "workloads.kueue.x-k8s.io", "-n", namespace, "-o", "wide"},
	)
}

// getJSON reads one object of kind named name in namespace into into. An
// empty name reads the list of every object of kind.
func getJSON(namespace, kind, name string, into any) error {
	args := []string{"get", kind}
	if name != "" {
		args = append(args, name)
	}
	out, err := kubectlQuick(namespace, append(args, "-o", "json")...)
	if err != nil {
		return err
	}
	return json.Unmarshal([]byte(out), into)
}
