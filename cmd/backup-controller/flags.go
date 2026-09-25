package main

import "flag"

// kubeconfigFlag returns a function that reads the --kubeconfig flag from fs.
// It registers the flag only when fs doesn't already have one, and otherwise
// reads the existing flag. main passes flag.CommandLine.
//
// controller-runtime's pkg/client/config registers a --kubeconfig flag on the
// default FlagSet in its init function, so any binary that imports
// controller-runtime has one before main runs. Registering a second one panics
// at startup with "flag redefined: kubeconfig". No unit test catches that,
// because none of them calls main, and v0.2.0 shipped that way and crashlooped
// on the cluster.
//
// Reading whichever flag exists keeps the command line the same no matter who
// registered it, and keeps working if the dependency stops registering one.
func kubeconfigFlag(fs *flag.FlagSet) func() string {
	const name = "kubeconfig"
	if existing := fs.Lookup(name); existing != nil {
		return existing.Value.String
	}
	value := fs.String(name, "", "path to a kubeconfig; empty means the ambient configuration")
	return func() string { return *value }
}
