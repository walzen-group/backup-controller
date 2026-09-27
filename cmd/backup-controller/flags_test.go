package main

import (
	"errors"
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

// TestTheRunOptionsCarryTheRestoreImage checks that the run controllers'
// options carry the --restore-image the command line sets, and that building
// them fails when it sets none. The RestoreRun reconciler gets its image
// from these options, so a command line without one never reaches it.
func TestTheRunOptionsCarryTheRestoreImage(t *testing.T) {
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	read := runFlags(fs)
	if _, err := read(); !errors.Is(err, errNoRestoreImage) {
		t.Errorf("options without --restore-image: error %v, want errNoRestoreImage", err)
	}

	if err := fs.Parse([]string{"--restore-image=restic.example/restic:1", "--namespace=elsewhere"}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	options, err := read()
	if err != nil {
		t.Fatalf("options: %v", err)
	}
	if options.RestoreImage != "restic.example/restic:1" || options.Namespace != "elsewhere" {
		t.Errorf("options = %+v, want the image and namespace the command line set", options)
	}
}

// --pause parses into RunOptions.Paused, and the controller runs without the
// pause when the command line leaves the flag out.
func TestThePauseFlagSetsRunOptionsPaused(t *testing.T) {
	for name, tc := range map[string]struct {
		args []string
		want bool
	}{
		"with --pause":    {args: []string{"--pause"}, want: true},
		"without --pause": {args: nil, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			fs := flag.NewFlagSet("t", flag.ContinueOnError)
			options := runFlags(fs)
			if err := fs.Parse(append([]string{"--restore-image=" + pinnedRestoreImage}, tc.args...)); err != nil {
				t.Fatalf("parse: %v", err)
			}
			got, err := options()
			if err != nil || got.Paused != tc.want {
				t.Errorf("Paused = %t, %v; want %t", got.Paused, err, tc.want)
			}
		})
	}
}
