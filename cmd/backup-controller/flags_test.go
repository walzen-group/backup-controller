package main

import (
	"errors"
	"flag"
	"strings"
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

// TestARestoreImageIsRequired checks that reading --restore-image fails when
// the command line leaves it out or sets it empty, and that the error names
// the flag.
//
// The controller carries no image of its own: the restore Job has to run the
// restic that VolSync backs up with, and only the installer knows which
// image that is. A default here would restore with a restic nobody chose.
func TestARestoreImageIsRequired(t *testing.T) {
	for name, args := range map[string][]string{
		"left out":   nil,
		"empty":      {"--restore-image="},
		"only space": {"--restore-image= "},
	} {
		t.Run(name, func(t *testing.T) {
			fs := flag.NewFlagSet("t", flag.ContinueOnError)
			read := restoreImageFlag(fs)
			if err := fs.Parse(args); err != nil {
				t.Fatalf("parse: %v", err)
			}
			image, err := read()
			if !errors.Is(err, errNoRestoreImage) {
				t.Fatalf("read = %q, %v; want errNoRestoreImage", image, err)
			}
			if !strings.Contains(err.Error(), "--restore-image") {
				t.Errorf("the error %q does not name the flag", err)
			}
		})
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
