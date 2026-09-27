package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

// TestOneControllerRunsAtATime checks that the Deployment in deploy/ and the
// chart's Deployment replace the pod with the Recreate strategy.
//
// The controller runs without leader election. Under the default
// RollingUpdate, a rollout starts the new pod before it stops the old one,
// and for that while two schedulers write BackupRuns and two reconcilers
// work on the same runs.
func TestOneControllerRunsAtATime(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read deploy/deployment.yaml: %v", err)
	}
	deployment := &appsv1.Deployment{}
	if err := yaml.Unmarshal(content, deployment); err != nil {
		t.Fatalf("parse deploy/deployment.yaml: %v", err)
	}
	if deployment.Spec.Strategy.Type != appsv1.RecreateDeploymentStrategyType {
		t.Errorf("deploy/deployment.yaml has strategy %q, want Recreate", deployment.Spec.Strategy.Type)
	}

	chart, err := os.ReadFile(filepath.Join("..", "..", "chart", "templates", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read the chart's deployment: %v", err)
	}
	if !strings.Contains(string(chart), "type: Recreate") {
		t.Error("the chart's Deployment does not use the Recreate strategy")
	}
}

// pinnedRestoreImage is VolSync's mover image as hack/e2e/volsync/pins.json
// pins it, used here only as a realistic value to render with.
const pinnedRestoreImage = "quay.io/backube/volsync:0.16.0@sha256:0d03a6aad57569eba2c0eaa0848cf4a908d9744b372ff2224bb291a320f36d76"

// renderChart runs helm template over the chart and returns what it printed
// and the error it ended with.
//
// Parameters:
//   - t fails the test when helm is not on the PATH. The tests run in the
//     flake's shell, which carries helm, and a missing helm must not pass as
//     a chart that renders.
//   - set holds helm's --set assignments, one per entry.
func renderChart(t *testing.T, set ...string) (string, error) {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Fatalf("helm is not on the PATH; run the tests in the flake's shell: %v", err)
	}
	args := []string{"template", "backup-controller", filepath.Join("..", "..", "chart")}
	for _, value := range set {
		args = append(args, "--set", value)
	}
	output, err := exec.Command(helm, args...).CombinedOutput()
	return string(output), err
}

// controllerContainer returns the controller container of the Deployment
// in a multi-document YAML render.
//
// Parameters:
//   - t fails the test when the render holds no such Deployment or container.
//   - render is the output of helm template or the content of a manifest.
func controllerContainer(t *testing.T, render string) corev1.Container {
	t.Helper()
	for _, document := range strings.Split(render, "\n---") {
		deployment := &appsv1.Deployment{}
		if err := yaml.Unmarshal([]byte(document), deployment); err != nil || deployment.Kind != "Deployment" {
			continue
		}
		for _, container := range deployment.Spec.Template.Spec.Containers {
			if container.Name == "controller" {
				return container
			}
		}
	}
	t.Fatalf("no Deployment with a controller container in:\n%s", render)
	return corev1.Container{}
}

// controllerArgs returns the args of the controller container of the
// Deployment in a multi-document YAML render (see controllerContainer).
func controllerArgs(t *testing.T, render string) []string {
	t.Helper()
	return controllerContainer(t, render).Args
}

// TestTheChartRefusesToRenderWithoutARestoreImage checks that helm template
// fails when the restoreImage value is not set, and that its error names the
// value.
//
// The controller refuses to start without --restore-image, and has no
// default image of its own. A chart that rendered without the value would
// install a pod that exits at once; failing the render says what is missing
// before anything is installed.
func TestTheChartRefusesToRenderWithoutARestoreImage(t *testing.T) {
	output, err := renderChart(t)
	if err == nil {
		t.Fatalf("helm template rendered without restoreImage:\n%s", output)
	}
	if !strings.Contains(output, "restoreImage") {
		t.Errorf("helm's error does not name restoreImage:\n%s", output)
	}
}

// TestTheChartPassesTheRestoreImage checks that the chart passes the
// restoreImage value to the controller container as --restore-image,
// unchanged and exactly once.
func TestTheChartPassesTheRestoreImage(t *testing.T) {
	output, err := renderChart(t, "restoreImage="+pinnedRestoreImage)
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, output)
	}
	if got := restoreImageArgs(controllerArgs(t, output)); len(got) != 1 || got[0] != "--restore-image="+pinnedRestoreImage {
		t.Errorf("the controller's --restore-image args are %q, want one with %s", got, pinnedRestoreImage)
	}
}

// TestTheReleaseManifestsLeaveTheRestoreImageToTheInstaller checks that the
// controller container in deploy/deployment.yaml has an args list and no
// --restore-image in it.
//
// deploy/ is the release's plain manifest, and it names no restic image,
// because only the installer knows which image VolSync backs up with. The
// installer appends the flag to the controller's args (the infra module
// does, and so does the e2e setup), so a flag here would reach the
// controller twice, and a missing args list would leave nothing to append
// to.
func TestTheReleaseManifestsLeaveTheRestoreImageToTheInstaller(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read deploy/deployment.yaml: %v", err)
	}
	args := controllerArgs(t, string(content))
	if len(args) == 0 {
		t.Error("the controller container in deploy/deployment.yaml has no args to append --restore-image to")
	}
	if got := restoreImageArgs(args); len(got) != 0 {
		t.Errorf("deploy/deployment.yaml passes %q; the installer appends --restore-image", got)
	}
}

// restoreImageArgs returns the entries of args that set --restore-image.
func restoreImageArgs(args []string) []string {
	var found []string
	for _, arg := range args {
		if strings.HasPrefix(arg, "--restore-image") {
			found = append(found, arg)
		}
	}
	return found
}

// TestTheGoMemoryLimitIsTheContainersLimit checks that the controller
// container in deploy/deployment.yaml and in the chart's render sets
// GOMEMLIMIT from its own memory limit through the downward API, in bytes,
// and that both set it the same way.
//
// Without GOMEMLIMIT the Go runtime collects only when the heap doubles
// since the last collection. A repository Open holds 32 MiB for scrypt, and
// the garbage it leaves let the heap grow past the container's former 128Mi limit
// until the kubelet OOMKilled the controller during the e2e suite. With the
// limit known, the runtime collects before the heap reaches it.
func TestTheGoMemoryLimitIsTheContainersLimit(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read deploy/deployment.yaml: %v", err)
	}
	render, err := renderChart(t, "restoreImage="+pinnedRestoreImage)
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, render)
	}
	want := corev1.EnvVar{
		Name: "GOMEMLIMIT",
		ValueFrom: &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{
			ContainerName: "controller",
			Resource:      "limits.memory",
			Divisor:       resource.MustParse("1"),
		}},
	}
	for source, manifest := range map[string]string{"deploy/deployment.yaml": string(content), "the chart": render} {
		container := controllerContainer(t, manifest)
		got := goMemLimit(container.Env)
		if got == nil {
			t.Errorf("%s sets no GOMEMLIMIT on the controller container", source)
			continue
		}
		if got.Value != "" || got.ValueFrom == nil || got.ValueFrom.ResourceFieldRef == nil ||
			got.ValueFrom.ResourceFieldRef.ContainerName != want.ValueFrom.ResourceFieldRef.ContainerName ||
			got.ValueFrom.ResourceFieldRef.Resource != want.ValueFrom.ResourceFieldRef.Resource ||
			got.ValueFrom.ResourceFieldRef.Divisor.Cmp(want.ValueFrom.ResourceFieldRef.Divisor) != 0 {
			t.Errorf("%s sets GOMEMLIMIT to %+v, want the controller's limits.memory in bytes", source, got)
		}
		if _, ok := container.Resources.Limits[corev1.ResourceMemory]; !ok {
			t.Errorf("%s sets no memory limit for GOMEMLIMIT to follow", source)
		}
	}
}

// goMemLimit returns the GOMEMLIMIT entry of a container's env, or nil when
// there is none.
func goMemLimit(env []corev1.EnvVar) *corev1.EnvVar {
	for i := range env {
		if env[i].Name == "GOMEMLIMIT" {
			return &env[i]
		}
	}
	return nil
}

// controllerMemoryRequest and controllerMemoryLimit are the memory request
// and the memory limit of the controller container in deploy/ and in the
// chart's default values.
//
// The process rests at about 50Mi, and each restic key derivation holds 32
// MiB more while it runs. 128Mi left too little room: the e2e suite saw the
// controller OOMKilled. The request is the same as the old limit, so that the
// scheduler keeps that memory for the controller.
const (
	controllerMemoryRequest = "256Mi"
	controllerMemoryLimit   = "512Mi"
)

// TestTheControllersMemoryLimit checks that the controller container in
// deploy/deployment.yaml and in the chart's default render has the memory
// request controllerMemoryRequest and the memory limit controllerMemoryLimit,
// so both install the same controller.
func TestTheControllersMemoryLimit(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read deploy/deployment.yaml: %v", err)
	}
	render, err := renderChart(t, "restoreImage="+pinnedRestoreImage)
	if err != nil {
		t.Fatalf("helm template: %v\n%s", err, render)
	}
	wantRequest := resource.MustParse(controllerMemoryRequest)
	wantLimit := resource.MustParse(controllerMemoryLimit)
	for source, manifest := range map[string]string{"deploy/deployment.yaml": string(content), "the chart": render} {
		resources := controllerContainer(t, manifest).Resources
		got, ok := resources.Requests[corev1.ResourceMemory]
		if !ok || got.Cmp(wantRequest) != 0 {
			t.Errorf("%s requests %s of memory for the controller, want %s", source, got.String(), controllerMemoryRequest)
		}
		got, ok = resources.Limits[corev1.ResourceMemory]
		if !ok || got.Cmp(wantLimit) != 0 {
			t.Errorf("%s limits the controller's memory to %s, want %s", source, got.String(), controllerMemoryLimit)
		}
	}
}
