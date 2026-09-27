package main

import (
	"errors"
	"flag"
	"os"
	"os/exec"
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

// runMainEnv names the environment variable that makes
// TestTheControllerRefusesToStartWithoutARestoreImage run main in the child
// process it starts.
const runMainEnv = "BACKUP_CONTROLLER_TEST_RUN_MAIN"

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

// TestTheRestoreImageIsWhatTheCommandLineSets checks that reading
// --restore-image returns the value the command line passes, unchanged.
func TestTheRestoreImageIsWhatTheCommandLineSets(t *testing.T) {
	const want = "quay.io/backube/volsync:0.16.0@sha256:0d03a6aad57569eba2c0eaa0848cf4a908d9744b372ff2224bb291a320f36d76"
	fs := flag.NewFlagSet("t", flag.ContinueOnError)
	read := restoreImageFlag(fs)
	if err := fs.Parse([]string{"--restore-image=" + want}); err != nil {
		t.Fatalf("parse: %v", err)
	}
	got, err := read()
	if err != nil || got != want {
		t.Errorf("read = %q, %v; want %q", got, err, want)
	}
}

// TestTheControllerRefusesToStartWithoutARestoreImage runs main in a child
// process without --restore-image and checks that it exits with status 1
// and says which flag is missing, before it builds any client.
//
// The child is this test binary, started again with runMainEnv set, so the
// test runs the real main with the real default FlagSet. The kubeconfig it
// passes names a file that does not exist, so a main that went on past the
// check would fail later with a different message.
func TestTheControllerRefusesToStartWithoutARestoreImage(t *testing.T) {
	if os.Getenv(runMainEnv) == "1" {
		os.Args = []string{"backup-controller", "--kubeconfig=/nonexistent/kubeconfig"}
		main()
		return
	}

	child := exec.Command(os.Args[0], "-test.run=^TestTheControllerRefusesToStartWithoutARestoreImage$")
	child.Env = append(os.Environ(), runMainEnv+"=1")
	output, err := child.CombinedOutput()

	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 1 {
		t.Fatalf("main ended with %v, want exit status 1; output:\n%s", err, output)
	}
	if !strings.Contains(string(output), errNoRestoreImage.Error()) {
		t.Errorf("main's output does not say that --restore-image is missing:\n%s", output)
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
