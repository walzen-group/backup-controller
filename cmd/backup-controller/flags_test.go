package main

import (
	"flag"
	"testing"

	// This import is here for its side effect. pkg/client/config registers a
	// --kubeconfig flag in its init function, so the default FlagSet already
	// has one before main runs. This package imports controller-runtime
	// through runs.go as well, so the collision these tests guard against is
	// real here.
	_ "sigs.k8s.io/controller-runtime/pkg/client/config"
)

// TestKubeconfigFlagReusesOneSomethingElseRegistered checks that
// kubeconfigFlag reads a --kubeconfig flag another package already registered,
// both before and after the command line is parsed. v0.2.0 crashlooped on the
// cluster with "flag redefined: kubeconfig" as soon as controller-runtime came
// into the binary. Every gate passed, because nothing in the test path called
// main and nothing registered the flag twice.
func TestKubeconfigFlagReusesOneSomethingElseRegistered(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	fs.String("kubeconfig", "/from/elsewhere", "registered by a dependency")

	read := kubeconfigFlag(fs)

	if got := read(); got != "/from/elsewhere" {
		t.Errorf("value = %q, want the existing flag's", got)
	}
	if err := fs.Parse([]string{"--kubeconfig=/passed/in"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := read(); got != "/passed/in" {
		t.Errorf("value after parsing = %q, want what the command line set", got)
	}
}

// TestKubeconfigFlagRegistersItsOwnWhenNothingHas checks that kubeconfigFlag
// registers the flag on a FlagSet that lacks one. The flag defaults to empty,
// and it reads the value the command line passes.
func TestKubeconfigFlagRegistersItsOwnWhenNothingHas(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)

	read := kubeconfigFlag(fs)

	if got := read(); got != "" {
		t.Errorf("default = %q, want empty, meaning the ambient configuration", got)
	}
	if err := fs.Parse([]string{"--kubeconfig=/passed/in"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := read(); got != "/passed/in" {
		t.Errorf("value after parsing = %q, want what the command line set", got)
	}
}

// TestKubeconfigFlagOnTheDefaultFlagSetDoesNotPanic checks that kubeconfigFlag
// works on the default FlagSet, which main uses and where the collision
// happened. It must not panic, however many dependencies have already put a
// kubeconfig flag there.
func TestKubeconfigFlagOnTheDefaultFlagSetDoesNotPanic(t *testing.T) {
	read := kubeconfigFlag(flag.CommandLine)
	if read == nil {
		t.Fatal("kubeconfigFlag returned nothing for the default FlagSet")
	}
}

// TestRestoreJobsRefusesAWrongLimit checks that the controller stops at
// startup for a --restore-limit-download below 0 or above the int32 of
// limitDownloadKiBps. Otherwise the value would go to restic as a negative or
// wrapped --limit-download in every restore Job.
//
// Parameters:
//   - t is the test handle that the go test runner gives.
//
// It has no return value. It reports an error when restoreJobs accepts -1, 1<<31
// or an empty image, or refuses 0 or 20480.
func TestRestoreJobsRefusesAWrongLimit(t *testing.T) {
	const image = "quay.io/backube/volsync:0.17.0"
	for _, limit := range []int{-1, 1 << 31} {
		if _, err := restoreJobs(image, limit); err == nil {
			t.Errorf("restoreJobs accepted --restore-limit-download %d, want an error", limit)
		}
	}
	if _, err := restoreJobs("", 0); err == nil {
		t.Error("restoreJobs accepted an empty --restore-image, want an error")
	}
	for _, limit := range []int{0, 20480} {
		got, err := restoreJobs(image, limit)
		if err != nil || got.LimitDownloadKiBps != int32(limit) || got.Image != image {
			t.Errorf("restoreJobs(%d) = %+v, %v; want the image and the limit %d", limit, got, err, limit)
		}
	}
}
