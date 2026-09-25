package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
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
