package main

import (
	"testing"

	ctrl "sigs.k8s.io/controller-runtime"
)

// v0.2.2 ran on the cluster with no logger, so the BackupRun and RestoreRun
// reconcilers discarded every line they produced and controller-runtime said so
// once, after thirty seconds, with a stack trace:
//
//	[controller-runtime] log.SetLogger(...) was never called; logs will not be
//	displayed.
//
// Until SetLogger is called, controller-runtime's delegating sink drops
// everything, so Enabled reports false. Every defect found in this controller
// so far was found by reading a log, which is what makes a silent reconciler
// worth a test.
func TestConfigureLoggingMakesControllerRuntimeLog(t *testing.T) {
	configureLogging()

	if !ctrl.Log.Enabled() {
		t.Error("controller-runtime's logger is disabled, so the reconcilers write nothing anywhere")
	}
}
