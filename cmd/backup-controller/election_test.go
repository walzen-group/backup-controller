package main

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/yaml"
)

// TestTheManagerCompetesForTheLeaseWhenAsked checks the options --leader-elect
// gives the manager: the Lease backup-controller in the controller's
// namespace, released on a clean shutdown so the other replica takes over at
// once.
func TestTheManagerCompetesForTheLeaseWhenAsked(t *testing.T) {
	options := managerOptions(runtime.NewScheme(), ":8081", Election{Enabled: true, Namespace: "backup-system"})

	if !options.LeaderElection {
		t.Errorf("LeaderElection is false; two replicas would both run every controller")
	}
	if options.LeaderElectionID != "backup-controller" {
		t.Errorf("LeaderElectionID is %q, want %q", options.LeaderElectionID, "backup-controller")
	}
	if options.LeaderElectionNamespace != "backup-system" {
		t.Errorf("LeaderElectionNamespace is %q, want %q", options.LeaderElectionNamespace, "backup-system")
	}
	if !options.LeaderElectionReleaseOnCancel {
		t.Errorf("LeaderElectionReleaseOnCancel is false; the other replica would wait out the lease after every rollout")
	}
}

// TestTheManagerRunsAloneWithoutTheFlag checks that a process started without
// --leader-elect takes no Lease, which is how a single replica and the tests
// run it.
func TestTheManagerRunsAloneWithoutTheFlag(t *testing.T) {
	options := managerOptions(runtime.NewScheme(), ":8081", Election{Namespace: "backup-system"})

	if options.LeaderElection {
		t.Errorf("LeaderElection is true without --leader-elect")
	}
}

// TestTheDeploymentRunsTwoElectedReplicas checks that deploy/ and the chart
// both run two replicas and pass --leader-elect. Two replicas without the flag
// would both drive the same run, and the flag without a second replica leaves
// the webhook unanswered whenever the one pod is gone.
func TestTheDeploymentRunsTwoElectedReplicas(t *testing.T) {
	content, err := os.ReadFile(filepath.Join("..", "..", "deploy", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read deploy/deployment.yaml: %v", err)
	}
	deployment := &appsv1.Deployment{}
	if err := yaml.Unmarshal(content, deployment); err != nil {
		t.Fatalf("parse deploy/deployment.yaml: %v", err)
	}
	if replicas := deployment.Spec.Replicas; replicas == nil || *replicas != 2 {
		got := "no"
		if replicas != nil {
			got = strconv.Itoa(int(*replicas))
		}
		t.Errorf("deploy/deployment.yaml runs %s replicas, want 2", got)
	}
	var args []string
	for _, c := range deployment.Spec.Template.Spec.Containers {
		if c.Name == "controller" {
			args = c.Args
		}
	}
	if !slices.Contains(args, "--leader-elect") {
		t.Errorf("deploy/deployment.yaml passes %v, want --leader-elect among them", args)
	}

	chart, err := os.ReadFile(filepath.Join("..", "..", "chart", "templates", "deployment.yaml"))
	if err != nil {
		t.Fatalf("read the chart's deployment: %v", err)
	}
	for _, want := range []string{"replicas: 2", "- --leader-elect"} {
		if !strings.Contains(string(chart), want) {
			t.Errorf("the chart's deployment has no %q", want)
		}
	}
}
