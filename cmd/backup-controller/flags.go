package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
)

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

// restoreJobs checks the restore Job flags and returns them as RestoreJobs.
// main calls it before it starts anything, so a wrong flag stops the process
// at once.
//
// Parameters:
//   - image is the --restore-image flag. Every volume restore runs a Job
//     with this image, so it must not be empty.
//   - limit is the --restore-limit-download flag in KiB/s. It must be 0 or
//     more and fit the int32 of limitDownloadKiBps.
//
// It returns the settings, or an error that gives the flag and the value it
// expects. Without this check, an empty image fails each restore only once
// it starts, and a negative limit goes to restic.
func restoreJobs(image string, limit int) (RestoreJobs, error) {
	if image == "" {
		return RestoreJobs{}, errors.New("--restore-image is required: pass VolSync's mover image, which holds the restic that wrote the repositories")
	}
	if limit < 0 || limit > math.MaxInt32 {
		return RestoreJobs{}, fmt.Errorf("--restore-limit-download is %d: pass a rate in KiB/s from 0 to %d, where 0 means no limit", limit, math.MaxInt32)
	}
	return RestoreJobs{Image: image, LimitDownloadKiBps: int32(limit)}, nil
}
