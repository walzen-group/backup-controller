// Command kubectl-backup is a kubectl plugin that creates BackupRuns and
// RestoreRuns from the command line. With the binary on the PATH, kubectl runs
// it as "kubectl backup". internal/cli holds the commands.
package main

import (
	"os"

	"github.com/walzen-group/backup-controller/internal/cli"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	os.Exit(cli.Main(os.Args[1:], version, os.Stdout, os.Stderr))
}
