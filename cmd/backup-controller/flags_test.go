package main

import (
	"flag"
	"testing"

	// Imported for its side effect, which is the point of this test file:
	// pkg/client/config registers a --kubeconfig flag in its init, so the
	// default FlagSet carries one before main runs. This package imports
	// controller-runtime through runs.go, so the collision is real here.
	_ "sigs.k8s.io/controller-runtime/pkg/client/config"
)

// v0.2.0 crashlooped on the cluster with "flag redefined: kubeconfig" the
// moment controller-runtime came into the binary. Every gate passed, because
// nothing in the test path calls main and nothing registered flags twice.
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

// The default FlagSet is the one main uses, and it is where the collision
// happened. Registering against it must not panic however many dependencies
// have already put a kubeconfig flag there.
func TestKubeconfigFlagOnTheDefaultFlagSetDoesNotPanic(t *testing.T) {
	read := kubeconfigFlag(flag.CommandLine)
	if read == nil {
		t.Fatal("kubeconfigFlag returned nothing for the default FlagSet")
	}
}
