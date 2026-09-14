package main

import "flag"

// kubeconfigFlag returns a reader for the --kubeconfig flag, registering it
// only if nothing already has.
//
// controller-runtime's pkg/client/config registers a --kubeconfig flag in its
// init, so importing controller-runtime at all puts one on the default FlagSet
// before main runs. Declaring a second panics the process at startup with
// "flag redefined: kubeconfig", which no unit test catches because none of them
// calls main: v0.2.0 shipped that way and crashlooped on the cluster.
//
// Reusing whichever flag exists keeps the binary's interface the same whoever
// registered it, and keeps working if that dependency stops registering one.
func kubeconfigFlag(fs *flag.FlagSet) func() string {
	const name = "kubeconfig"
	if existing := fs.Lookup(name); existing != nil {
		return existing.Value.String
	}
	value := fs.String(name, "", "path to a kubeconfig; empty means the ambient configuration")
	return func() string { return *value }
}
