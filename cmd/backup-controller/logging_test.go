package main

import (
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"
)

// TestConfigureLoggingMakesControllerRuntimeLog checks that after
// configureLogging runs, controller-runtime's logger is enabled.
//
// v0.2.2 ran on the cluster with no logger set. The BackupRun and RestoreRun
// reconcilers discarded every line they wrote, and controller-runtime reported
// it once, after thirty seconds, with a stack trace:
//
//	[controller-runtime] log.SetLogger(...) was never called; logs will not be
//	displayed.
//
// Until SetLogger is called, controller-runtime's delegating sink drops every
// line, so Enabled reports false. Every defect found in this controller so far
// was found by reading a log, which is why a silent reconciler gets a test.
func TestConfigureLoggingMakesControllerRuntimeLog(t *testing.T) {
	configureLogging()

	if !ctrl.Log.Enabled() {
		t.Error("controller-runtime's logger is disabled, so the reconcilers write nothing anywhere")
	}
}
