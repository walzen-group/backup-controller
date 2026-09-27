package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

// pinnedRestoreImage is VolSync's mover image as hack/e2e/volsync/pins.json
// pins it, used here only as a realistic value to render with.
const pinnedRestoreImage = "quay.io/backube/volsync:0.16.0@sha256:0d03a6aad57569eba2c0eaa0848cf4a908d9744b372ff2224bb291a320f36d76"

// renderChartIn runs helm template over the chart for a release in a
// namespace and returns what it printed.
//
// Parameters:
//   - t fails the test when helm is not on the PATH or the render fails. The
//     tests run in the flake's shell, which carries helm, and a missing helm
//     must not pass as a chart that renders.
//   - namespace is the release's namespace, where the chart puts the
//     controller's ServiceAccount.
//   - set holds helm's --set assignments, one per entry.
func renderChartIn(t *testing.T, namespace string, set ...string) string {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Fatalf("helm is not on the PATH; run the tests in the flake's shell: %v", err)
	}
	args := []string{"template", "backup-controller", filepath.Join("..", "..", "chart"), "--namespace", namespace}
	for _, value := range set {
		args = append(args, "--set", value)
	}
	output, err := exec.Command(helm, args...).Output()
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, output)
	}
	return string(output)
}

// controllerDeployment returns the Deployment with the controller container
// in a multi-document YAML render, and that container.
//
// Parameters:
//   - t fails the test when the render holds no such Deployment.
//   - render is the output of helm template or the content of a manifest.
func controllerDeployment(t *testing.T, render string) (*appsv1.Deployment, corev1.Container) {
	t.Helper()
	for _, document := range strings.Split(render, "\n---") {
		deployment := &appsv1.Deployment{}
		if err := yaml.Unmarshal([]byte(document), deployment); err != nil || deployment.Kind != "Deployment" {
			continue
		}
		for _, container := range deployment.Spec.Template.Spec.Containers {
			if container.Name == "controller" {
				return deployment, container
			}
		}
	}
	t.Fatalf("no Deployment with a controller container in:\n%s", render)
	return nil, corev1.Container{}
}

// TestTheDeploymentShape checks the controller's Deployment in deploy/ and in
// the chart's render:
//
//   - Both replace the pod with the Recreate strategy. The controller runs
//     without leader election. Under the default RollingUpdate, a rollout
//     starts the new pod before it stops the old one, and for that while two
//     schedulers write BackupRuns and two reconcilers work on the same runs.
//   - Both set GOMEMLIMIT from the container's own memory limit through the
//     downward API, in bytes. Without it the Go runtime collects only when the
//     heap doubles. A repository Open holds 32 MiB for scrypt, and the garbage
//     it left let the heap grow past the former 128Mi limit until the kubelet
//     OOMKilled the controller during the e2e suite.
//   - The chart passes the restoreImage value as --restore-image, unchanged
//     and exactly once, and --pause only when the pause value is true.
func TestTheDeploymentShape(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read deploy/deployment.yaml: %v", err)
	}
	render := renderChartIn(t, "backup-system", "restoreImage="+pinnedRestoreImage)
	for source, manifest := range map[string]string{"deploy/deployment.yaml": string(content), "the chart": render} {
		deployment, container := controllerDeployment(t, manifest)
		if deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
			t.Errorf("%s has strategy %q, want Recreate", source, deployment.Spec.Strategy.Type)
		}
		if _, ok := container.Resources.Limits[corev1.ResourceMemory]; !ok {
			t.Errorf("%s sets no memory limit for GOMEMLIMIT to follow", source)
		}
		i := slices.IndexFunc(container.Env, func(env corev1.EnvVar) bool { return env.Name == "GOMEMLIMIT" })
		if i < 0 {
			t.Errorf("%s sets no GOMEMLIMIT on the controller container", source)
			continue
		}
		got := container.Env[i]
		if got.Value != "" || got.ValueFrom == nil || got.ValueFrom.ResourceFieldRef == nil ||
			got.ValueFrom.ResourceFieldRef.ContainerName != "controller" ||
			got.ValueFrom.ResourceFieldRef.Resource != "limits.memory" ||
			got.ValueFrom.ResourceFieldRef.Divisor.Value() != 1 {
			t.Errorf("%s sets GOMEMLIMIT to %+v, want the controller's limits.memory in bytes", source, got)
		}
	}

	_, container := controllerDeployment(t, render)
	var images []string
	for _, arg := range container.Args {
		if strings.HasPrefix(arg, "--restore-image") {
			images = append(images, arg)
		}
	}
	if len(images) != 1 || images[0] != "--restore-image="+pinnedRestoreImage {
		t.Errorf("the controller's --restore-image args are %q, want one with %s", images, pinnedRestoreImage)
	}
	for value, want := range map[string]bool{"true": true, "false": false} {
		_, container := controllerDeployment(t, renderChartIn(t, "backup-system", "restoreImage="+pinnedRestoreImage, "pause="+value))
		if got := slices.Contains(container.Args, "--pause"); got != want {
			t.Errorf("pause=%s: --pause in the args = %t, want %t", value, got, want)
		}
	}
}
