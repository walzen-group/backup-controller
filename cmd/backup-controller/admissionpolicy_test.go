package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"sigs.k8s.io/yaml"
)

// The kinds of the admission policy's two objects.
const (
	kindPolicy  = "ValidatingAdmissionPolicy"
	kindBinding = "ValidatingAdmissionPolicyBinding"
)

// chartOnlyLabels are the labels the chart's labels helper adds beyond the
// two deploy/ writes. They say which chart and version rendered an object and
// decide nothing about it.
var chartOnlyLabels = []string{"helm.sh/chart", "app.kubernetes.io/version", "app.kubernetes.io/managed-by"}

// renderChartIn runs helm template over the chart for a release in a
// namespace and returns what it printed.
//
// Parameters:
//   - t fails the test when helm is not on the PATH or the render fails.
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

// documentsOfKind returns the documents of a multi-document YAML text whose
// kind is the given one, parsed into maps.
//
// Parameters:
//   - t fails the test when a document does not parse.
//   - text is a render or the content of a manifest.
//   - kind is the kind to keep.
func documentsOfKind(t *testing.T, text, kind string) []map[string]any {
	t.Helper()
	var found []map[string]any
	for _, document := range strings.Split(text, "\n---") {
		object := map[string]any{}
		if err := yaml.Unmarshal([]byte(document), &object); err != nil {
			t.Fatalf("parse a document: %v\n%s", err, document)
		}
		if object["kind"] == kind {
			found = append(found, object)
		}
	}
	return found
}

// withoutChartLabels removes chartOnlyLabels from an object's metadata.
func withoutChartLabels(object map[string]any) map[string]any {
	metadata, _ := object["metadata"].(map[string]any)
	labels, _ := metadata["labels"].(map[string]any)
	for _, key := range chartOnlyLabels {
		delete(labels, key)
	}
	return object
}

// readDeployPolicy returns the content of deploy/admissionpolicy.yaml.
func readDeployPolicy(t *testing.T) string {
	t.Helper()
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "admissionpolicy.yaml"))
	if err != nil {
		t.Fatalf("read deploy/admissionpolicy.yaml: %v", err)
	}
	return string(content)
}

// TestTheChartRendersTheReleasesAdmissionPolicy checks that the chart,
// installed as deploy/ installs the controller (release backup-controller in
// backup-system), renders the admission policy and its binding exactly as
// deploy/admissionpolicy.yaml writes them, apart from the chart's own labels.
//
// The policy is written twice, once for each way to install, and the two
// must not drift: a validation that one of them lacks would admit Jobs the
// other refuses.
func TestTheChartRendersTheReleasesAdmissionPolicy(t *testing.T) {
	render := renderChartIn(t, "backup-system", "restoreImage="+pinnedRestoreImage)
	deploy := readDeployPolicy(t)
	for _, kind := range []string{kindPolicy, kindBinding} {
		want := documentsOfKind(t, deploy, kind)
		got := documentsOfKind(t, render, kind)
		if len(want) != 1 || len(got) != 1 {
			t.Fatalf("%s: deploy/ has %d and the chart renders %d, want one each", kind, len(want), len(got))
		}
		if !reflect.DeepEqual(withoutChartLabels(got[0]), want[0]) {
			gotYAML, _ := yaml.Marshal(got[0])
			wantYAML, _ := yaml.Marshal(want[0])
			t.Errorf("%s: the chart renders\n%s\ndeploy/ has\n%s", kind, gotYAML, wantYAML)
		}
	}
}

// TestTheChartsPolicyMatchesItsServiceAccount checks that the chart's policy
// matches the ServiceAccount the chart runs the controller as, in the
// release's namespace, and that the binding names the chart's policy.
//
// The policy matches on the ServiceAccount's username. A username that names
// another account or namespace matches nothing, and the controller's Jobs
// would then be admitted unchecked.
func TestTheChartsPolicyMatchesItsServiceAccount(t *testing.T) {
	render := renderChartIn(t, "ops", "restoreImage="+pinnedRestoreImage, "serviceAccount.name=restorer", "fullnameOverride=backups")
	policies := documentsOfKind(t, render, kindPolicy)
	bindings := documentsOfKind(t, render, kindBinding)
	if len(policies) != 1 || len(bindings) != 1 {
		t.Fatalf("the chart renders %d policies and %d bindings, want one each", len(policies), len(bindings))
	}
	policyYAML, _ := yaml.Marshal(policies[0])
	if want := "request.userInfo.username == 'system:serviceaccount:ops:restorer'"; !strings.Contains(string(policyYAML), want) {
		t.Errorf("the policy does not match %q:\n%s", want, policyYAML)
	}
	name := policies[0]["metadata"].(map[string]any)["name"]
	if name != "backups-restore-jobs" {
		t.Errorf("the policy is named %v, want backups-restore-jobs", name)
	}
	if got := bindings[0]["spec"].(map[string]any)["policyName"]; got != name {
		t.Errorf("the binding names the policy %v, want %v", got, name)
	}
}
